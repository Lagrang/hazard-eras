use crate::{HazardPointer, PointerValue};
use std::arch::x86_64::cmpxchg16b;
use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::ptr;
use std::sync::atomic::Ordering;

#[repr(align(16))]
pub struct StampedPointer<T> {
    // TODO: add heap based implementation when cmpxchg16b is not supported
    ptr: UnsafeCell<u128>,
    phantom: PhantomData<T>,
}

unsafe impl<T> Send for StampedPointer<T> {}
unsafe impl<T> Sync for StampedPointer<T> {}

#[derive(Debug, Copy, Clone)]
pub struct VersionedPtr<T> {
    pointer: *mut T,
    version: u64,
}

impl<T> Eq for VersionedPtr<T> {}

impl<T> PartialEq for VersionedPtr<T> {
    fn eq(&self, other: &Self) -> bool {
        self.version == other.version && self.pointer == other.pointer
    }
}

impl<T> Default for StampedPointer<T> {
    /// Return pointer to null and stamped to 0 version.
    fn default() -> Self {
        Self::new(ptr::null_mut())
    }
}

impl<T> StampedPointer<T> {
    pub fn new(ptr: *mut T) -> Self {
        Self {
            ptr: UnsafeCell::new(u128::from(ptr as u64)),
            phantom: PhantomData {},
        }
    }

    pub fn load(&self, ord: Ordering) -> VersionedPtr<T> {
        let null = ptr::null() as *const T as u128;
        let cur_val = unsafe { cmpxchg16b(self.ptr.get(), null, null, ord, ord) };
        VersionedPtr::from(cur_val)
    }

    pub fn compare_exchange(
        &self,
        current: VersionedPtr<T>,
        new: VersionedPtr<T>,
        success_ord: Ordering,
        fail_ord: Ordering,
    ) -> Result<VersionedPtr<T>, VersionedPtr<T>> {
        let cur = current.into();
        let prev_val =
            unsafe { cmpxchg16b(self.ptr.get(), cur, new.into(), success_ord, fail_ord) };

        if prev_val == cur {
            Ok(VersionedPtr::from(cur))
        } else {
            Err(VersionedPtr::from(prev_val))
        }
    }
}

impl<T> VersionedPtr<T> {
    pub fn new(ptr: *mut T, version: u64) -> Self {
        Self {
            pointer: ptr,
            version,
        }
    }

    pub fn update(&self, new_ptr: *mut T) -> VersionedPtr<T> {
        VersionedPtr::new(new_ptr, self.version.wrapping_add(1))
    }

    pub fn pointer(&self) -> *mut T {
        self.pointer
    }

    pub fn version(&self) -> u64 {
        self.version
    }
}

impl<T> From<u128> for VersionedPtr<T> {
    fn from(val: u128) -> Self {
        VersionedPtr::new(val as u64 as *mut T, (val >> 64) as u64)
    }
}

impl<T> From<VersionedPtr<T>> for u128 {
    fn from(val: VersionedPtr<T>) -> Self {
        val.pointer as u128 | ((val.version as u128) << 64)
    }
}

impl<T> From<*mut T> for VersionedPtr<T> {
    fn from(ptr: *mut T) -> Self {
        VersionedPtr::new(ptr, 0)
    }
}

impl<T> From<VersionedPtr<T>> for *mut T {
    fn from(ptr: VersionedPtr<T>) -> Self {
        ptr.pointer
    }
}

impl<'a, T> HazardPointer<'a, T, u64> for StampedPointer<T> {
    fn load(&'a self, ord: Ordering) -> Option<PointerValue<'a, T, u64>> {
        let ptr = self.load(ord);
        if ptr.pointer.is_null() {
            None
        } else {
            Some(PointerValue {
                value: unsafe { &*ptr.pointer },
                metadata: ptr.version,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::stamped_ptr::{StampedPointer, VersionedPtr};
    use crate::HazardPointer;
    use std::mem::align_of_val;
    use std::ptr;
    use std::sync::atomic::Ordering;

    #[test]
    fn compare_exchange_of_null() {
        let sp = StampedPointer::default();
        let cur = sp.load(Ordering::Relaxed);
        assert_eq!(cur, VersionedPtr::new(ptr::null_mut(), 0));

        let mut new_val = Box::new(10);
        assert!(
            matches!(sp.compare_exchange(cur, VersionedPtr::new(new_val.as_mut(), 1),
                Ordering::AcqRel,
                Ordering::Acquire),
                Ok(prev) if prev==cur
            )
        );

        let cur = sp.load(Ordering::Relaxed);
        assert_eq!(cur, VersionedPtr::new(new_val.as_mut() as *mut i32, 1));
    }

    #[test]
    fn compare_exchange_of_valid_ptr() {
        let mut val = Box::new(10);
        let sp = StampedPointer::new(val.as_mut());
        let cur = sp.load(Ordering::Relaxed);
        assert_eq!(cur, VersionedPtr::new(val.as_mut() as *mut i32, 0));

        let mut new_val = Box::new(11);
        assert!(
            matches!(sp.compare_exchange(cur, VersionedPtr::new(new_val.as_mut(), 1),
                Ordering::AcqRel,
                Ordering::Acquire),
                Ok(prev) if prev==cur
            )
        );

        let cur = sp.load(Ordering::Relaxed);
        assert_eq!(cur, VersionedPtr::new(new_val.as_mut() as *mut i32, 1));
    }

    #[test]
    fn compare_exchange_of_hazard_ptr() {
        let mut val = Box::new(10);
        let sp = StampedPointer::new(val.as_mut());
        let cur = HazardPointer::load(&sp, Ordering::Relaxed);
        assert!(matches!(cur, Some(v) if v.value == val.as_ref() && v.metadata == 0));

        let mut new_val = Box::new(11);
        assert!(
            matches!(sp.compare_exchange(VersionedPtr::new(val.as_mut(), 0), VersionedPtr::new
                (new_val.as_mut(), 1),
                Ordering::SeqCst,
                Ordering::SeqCst),
                Ok(prev) if prev==VersionedPtr::new(val.as_mut(), 0)
            )
        );

        let cur = sp.load(Ordering::Relaxed);
        assert_eq!(cur, VersionedPtr::new(new_val.as_mut() as *mut i32, 1));
    }
}
