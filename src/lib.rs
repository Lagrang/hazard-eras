#![feature(stdsimd)]

mod list;
mod stamped_ptr;

use list::LockFreeList;
use std::borrow::Borrow;
use std::cell::{RefCell, UnsafeCell};
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::option::Option::Some;
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};
use std::thread_local;

thread_local! {
    // thread local map contains pointer to internals of inside hazard eras structure(address
    // of this structure is key of the hashmap)
    static HAZARD_TID: RefCell<HashMap<usize, (usize, usize)>> = RefCell::new(HashMap::new());
}

#[derive(Clone, Copy, Debug)]
#[repr(transparent)]
struct Era(u64);

impl PartialOrd<u64> for Era {
    fn partial_cmp(&self, other: &u64) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(other)
    }
}

impl PartialEq<u64> for Era {
    fn eq(&self, other: &u64) -> bool {
        self.0 == *other
    }
}

const ERA_NOT_SET: u64 = 0;

pub trait HazardPointer<'a, T, M> {
    fn load(&'a self, ord: Ordering) -> Option<PointerValue<'a, T, M>>;
}

impl<'a, T> HazardPointer<'a, T, ()> for AtomicPtr<T> {
    fn load(&'a self, ord: Ordering) -> Option<PointerValue<'a, T, ()>> {
        let ptr = self.load(ord);
        if ptr.is_null() {
            None
        } else {
            Some(unsafe {
                PointerValue {
                    value: &*ptr,
                    metadata: (),
                }
            })
        }
    }
}

pub struct PointerValue<'a, T, M> {
    value: &'a T,
    metadata: M,
}

impl<'a, T, M> Deref for PointerValue<'a, T, M> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.value
    }
}

impl<'a, T, M> AsRef<T> for PointerValue<'a, T, M> {
    fn as_ref(&self) -> &T {
        self.value
    }
}

impl<'a, T, M> Borrow<T> for PointerValue<'a, T, M> {
    fn borrow(&self) -> &T {
        self.value
    }
}

pub struct HazardObject<T> {
    object: T,
    create_era: Era,
}

impl<T> HazardObject<T> {
    fn new(val: T, eras: &HazardEras) -> Self {
        HazardObject {
            object: val,
            create_era: eras.current_era(),
        }
    }

    pub fn get(&self) -> &T {
        &self.object
    }
}

pub struct Guard<'e> {
    eras: &'e HazardEras,
    active_pointers: Option<UnsafeCell<HashSet<*const HazardPtrReadState>>>,
}

impl<'e> Guard<'e> {
    pub fn read_object<'a: 'e, T: 'a, M>(
        &'e self,
        ptr: &'a impl HazardPointer<'a, T, M>,
    ) -> Option<PointerValue<'e, T, M>> {
        if let Some(read_ptrs) = &self.active_pointers {
            let (p, read_state) = self.eras.read(ptr);
            unsafe { &mut *read_ptrs.get() }.insert(read_state);
            p
        } else {
            self.eras.read_unprotected(ptr)
        }
    }

    pub fn create_object<T>(&self, val: T) -> *mut HazardObject<T> {
        Box::into_raw(Box::new(HazardObject::new(val, self.eras)))
    }

    pub fn retire<T>(&self, retired_object: &HazardObject<T>) {
        self.eras
            .retire(retired_object as *const HazardObject<T> as *mut HazardObject<T>);
    }

    pub fn new_guard(&self) -> Guard {
        self.eras.new_guard()
    }

    pub fn unprotected(&self) -> Guard {
        self.eras.unprotected()
    }
}

impl<'e> Drop for Guard<'e> {
    fn drop(&mut self) {
        self.eras.drop_guard(self);
    }
}

pub struct HazardEras {
    clock: AtomicU64,
    thread_eras: LockFreeList<LockFreeList<HazardPtrReadState>>,
    retired: LockFreeList<LockFreeList<Retired>>,
}

impl HazardEras {
    pub fn new() -> Self {
        Self {
            clock: AtomicU64::new(ERA_NOT_SET),
            thread_eras: LockFreeList::new(),
            retired: LockFreeList::new(),
        }
    }

    pub fn new_guard(&self) -> Guard {
        Guard {
            eras: self,
            active_pointers: Some(UnsafeCell::new(HashSet::new())),
        }
    }

    pub const fn unprotected(&self) -> Guard {
        Guard {
            eras: self,
            active_pointers: None,
        }
    }

    fn current_era(&self) -> Era {
        Era(self.clock.load(Ordering::Relaxed))
    }

    /// Add the pointer to thread-local retired list and try to free memory used by retired
    /// pointers(retired by caller thread).
    fn retire<T>(&self, obj: *mut HazardObject<T>) {
        let cur_era = self.clock.load(Ordering::SeqCst);
        let retired_tls = self.get_retired();
        let obj_addr = obj as usize;
        unsafe {
            retired_tls.add(
                Retired {
                    create_era: (*obj).create_era,
                    delete_era: Era(cur_era),
                    drop_fn: Box::new(move || {
                        drop(Box::from_raw(obj_addr as *mut HazardObject<T>))
                    }),
                },
                &self.new_guard(),
            );
        }

        // if some other thread already increase era number, we can skip expensive atomic store
        if self.clock.load(Ordering::SeqCst) == cur_era {
            self.clock.fetch_add(1, Ordering::SeqCst);
        }

        'r: for retired in retired_tls.iter() {
            for read_list in self.thread_eras.iter() {
                for e in read_list.iter() {
                    let era = e.era.load(Ordering::Relaxed);
                    if retired.create_era >= era && retired.delete_era <= era {
                        // era of some thread, which reads hazard pointer, overlaps with
                        // lifetime of retired object. Skip retirement until this thread will
                        // release reference to hazard object.
                        break 'r;
                    }
                }
            }

            // hazard object not used by any thread, drop it
            (&retired.drop_fn)();
        }
    }

    /// Read value from passed pointer and register it as 'currently in use'.
    fn read<'a, 'p: 'a, T: 'p, M>(
        &'a self,
        ptr: &'p impl HazardPointer<'p, T, M>,
    ) -> (Option<PointerValue<'a, T, M>>, *const HazardPtrReadState) {
        // each read from pointer create new entry inside HE.
        // Otherwise, we can't ensure that HE will not release memory while it still used by
        // caller thread. If we will reuse same `HazardPtrReadState` entry, we can't differentiate
        // several reads of same pointer by caller thread and we will release memory which can
        // still be used by this thread.
        let create_era = self.clock.load(Ordering::SeqCst);
        let state = self.get_read_eras().add(
            HazardPtrReadState {
                era: AtomicU64::new(create_era),
            },
            &self.new_guard(),
        );

        let mut read_era = state.era.load(Ordering::SeqCst);
        loop {
            let p = ptr.load(Ordering::SeqCst);
            let cur_era = self.clock.load(Ordering::SeqCst);
            if cur_era == read_era {
                // era not changed and this indicate us that no pointers were retired while we
                // read pointer value. We can safely use value of pointer.
                return (p, state);
            }
            // era changed and pointer value can be already retired, read pointer value again
            state.era.store(cur_era, Ordering::SeqCst);
            read_era = cur_era;
        }
    }

    /// Read value from passed pointer and **do not** register it as 'currently in use'.
    fn read_unprotected<'a, 'p: 'a, T: 'p, M>(
        &'a self,
        ptr: &'p impl HazardPointer<'p, T, M>,
    ) -> Option<PointerValue<'a, T, M>> {
        let mut read_era = self.clock.load(Ordering::SeqCst);
        loop {
            let p = ptr.load(Ordering::SeqCst);
            let cur_era = self.clock.load(Ordering::SeqCst);
            if cur_era == read_era {
                // era not changed and this indicate us that no pointers were retired while we
                // read pointer value. We can safely use value of pointer.
                return p;
            }
            read_era = cur_era;
        }
    }

    /// Remove 'read state' for each pointer registered in passed guard.
    /// This method called when thread completes reading bunch of pointers and finish processing
    /// them. Now, old values, related to these pointers, can be retired.
    fn drop_guard(&self, guard: &Guard) {
        unsafe {
            if let Some(read_ptrs) = &guard.active_pointers {
                // use 'unprotected' guard to prevent recursive drop of guard
                // unprotected guard do not track pointers which were read through it and hence
                // it will not require any state inside HE structure.
                // It's save to use unprotected guard here, because we remove from thread local
                // structure which can be modified only by this thread. Other threads only read
                // from it. Thus, we do not need to track references which will be read during
                // remove operation. All pointers read by remove method will be retired only when
                // other threads will complete there scan operations.
                let unprotected = &guard.unprotected();
                for p in (&*read_ptrs.get()).iter() {
                    let res = self.get_read_eras().remove(
                        |state| state as *const HazardPtrReadState == *p,
                        unprotected,
                    );
                    debug_assert!(res);
                }
            }
        }
    }

    /// Return list of pointers which were read by caller thread.
    fn get_read_eras(&self) -> &LockFreeList<HazardPtrReadState> {
        self.get_thread_state().0
    }

    /// Return list of pointers which were retired by caller thread.
    fn get_retired(&self) -> &LockFreeList<Retired> {
        self.get_thread_state().1
    }

    fn get_thread_state(&self) -> (&LockFreeList<HazardPtrReadState>, &LockFreeList<Retired>) {
        HAZARD_TID.with(|map| {
            let addr = self as *const HazardEras as usize;
            let (read_list, retired_list) = map.borrow().get(&addr).copied().unwrap_or_else(|| {
                let guard = self.new_guard();
                let read_list = self.thread_eras.add(LockFreeList::new(), &guard);
                let retire_list = self.retired.add(LockFreeList::new(), &guard);
                map.borrow_mut().insert(
                    addr,
                    (
                        read_list as *const LockFreeList<HazardPtrReadState> as usize,
                        retire_list as *const LockFreeList<Retired> as usize,
                    ),
                );
                (
                    read_list as *const LockFreeList<HazardPtrReadState> as usize,
                    retire_list as *const LockFreeList<Retired> as usize,
                )
            });
            unsafe {
                (
                    &*(read_list as *const LockFreeList<HazardPtrReadState>),
                    &*(retired_list as *const LockFreeList<Retired>),
                )
            }
        })
    }
}

impl Drop for HazardEras {
    fn drop(&mut self) {
        for ptrs in self.retired.iter() {
            for retired in ptrs.iter() {
                (&retired.drop_fn)();
            }
        }
    }
}

struct Retired {
    drop_fn: Box<dyn Fn()>,
    create_era: Era,
    delete_era: Era,
}

#[repr(transparent)]
struct HazardPtrReadState {
    era: AtomicU64,
}

impl Eq for HazardPtrReadState {}
impl Ord for HazardPtrReadState {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl PartialEq for HazardPtrReadState {
    fn eq(&self, other: &Self) -> bool {
        self.era.load(Ordering::Relaxed) == other.era.load(Ordering::Relaxed)
    }
}

impl PartialOrd for HazardPtrReadState {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(
            self.era
                .load(Ordering::Relaxed)
                .cmp(&other.era.load(Ordering::Relaxed)),
        )
    }
}

impl Hash for HazardPtrReadState {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.era.load(Ordering::Relaxed));
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
