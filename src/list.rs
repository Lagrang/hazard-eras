use crate::stamped_ptr::{StampedPointer, VersionedPtr};
use crate::Guard;
use crate::HazardObject;
use crate::PointerValue;
use std::ops::RangeInclusive;
use std::option::Option::Some;
use std::ptr;
use std::sync::atomic::{fence, Ordering};

pub struct LockFreeList<T> {
    head: StampedPointer<HazardObject<Node<T>>>,
}

struct Node<T> {
    value: T,
    next: StampedPointer<HazardObject<Node<T>>>,
}

const LIVE_NODE: u64 = 0;
const REMOVED_NODE: u64 = 1;

unsafe impl<T> Send for LockFreeList<T> {}
unsafe impl<T> Sync for LockFreeList<T> {}

impl<T> LockFreeList<T> {
    pub fn new() -> Self {
        Self {
            head: StampedPointer::default(),
        }
    }

    pub fn add<'a>(&'a self, val: T, guard: &Guard) -> &'a T {
        let new_node = guard.create_object(Node {
            value: val,
            next: StampedPointer::default(),
        });

        loop {
            let head_stamped_ptr: Option<PointerValue<HazardObject<Node<T>>, u64>> =
                guard.read_object(&self.head);
            let (head_ptr, head_version) = head_stamped_ptr.map_or_else(
                || (ptr::null_mut(), LIVE_NODE),
                |p| {
                    (
                        p.value as *const HazardObject<Node<T>> as *mut HazardObject<Node<T>>,
                        p.metadata,
                    )
                },
            );
            unsafe {
                (*new_node).object.next = StampedPointer::new(head_ptr);
            }

            if self
                .head
                .compare_exchange(
                    VersionedPtr::new(head_ptr, head_version),
                    VersionedPtr::new(new_node, LIVE_NODE),
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
            {
                return &unsafe { &*new_node }.get().value;
            }
        }
    }

    /// Remove all values between passed range. If one of range bounds not found, remove will
    /// return `None`.
    ///
    /// This method cannot be concurrently executed by different threads. It can be safely
    /// concurrently executed only with list iterators.
    // pub fn remove_range<'g>(
    //     &'g self,
    //     range: RangeInclusive<&T>,
    //     guard: &'g Guard,
    // ) -> Result<(), ()> {
    //     // reverse order of range because we insert to the head of list but caller assumes that
    //     // we insert 'in order'(e.g., to the tail). Caller passes the range and expects that
    //     // earlier inserted elements will be found in list before later one.
    //     let search_res = self
    //         .search(&|val| ptr::eq(val, *range.end()), guard)
    //         .and_then(|left| {
    //             self.search(&|val| ptr::eq(val, *range.start()), guard)
    //                 .map(|right| (left, right))
    //         });
    //
    //     if let Some((left, right)) = search_res {
    //         let next_snap = &right.found_node.next_snapshot;
    //         let next_ptr = if !next_snap.pointer().is_null() {
    //             let right_node = unsafe { &*next_snap.pointer() }.get();
    //             let right_ptr = right_node.next.load(Ordering::SeqCst);
    //             if right_ptr.version() == REMOVED_NODE {
    //                 // next node was removed
    //                 return Err(());
    //             }
    //             VersionedPtr::new(next_snap.pointer(), LIVE_NODE)
    //         } else {
    //             // there is no next node, e.g. we trying to remove last node in list
    //             VersionedPtr::new(ptr::null_mut(), LIVE_NODE)
    //         };
    //
    //         let removed_marker =
    //             VersionedPtr::new(left.found_node.next_snapshot.pointer(), REMOVED_NODE);
    //         let (mut cur, mut next) = (
    //             left.found_node.node_ptr.value,
    //             left.found_node.next_snapshot.pointer(),
    //         );
    //         // mark found node as removed
    //         if left
    //             .found_node
    //             .node_ptr
    //             .get()
    //             .next
    //             .compare_exchange(
    //                 left.found_node.next_snapshot,
    //                 removed_marker,
    //                 Ordering::SeqCst,
    //                 Ordering::SeqCst,
    //             )
    //             .is_ok()
    //         {
    //             let result = if let Some(prev) = left.prev {
    //                 // update 'next' pointer of node which is precedes found node, to unlink
    //                 // removed node from list
    //                 prev.node_ptr.get().next.compare_exchange(
    //                     prev.next_snapshot,
    //                     next_ptr,
    //                     Ordering::SeqCst,
    //                     Ordering::SeqCst,
    //                 )
    //             } else {
    //                 // found node is a list head, update it to unlink removed node
    //                 self.head.compare_exchange(
    //                     VersionedPtr::new(
    //                         left.found_node.node_ptr.value as *const HazardObject<Node<T>>
    //                             as *mut HazardObject<Node<T>>,
    //                         LIVE_NODE,
    //                     ),
    //                     next_ptr,
    //                     Ordering::SeqCst,
    //                     Ordering::SeqCst,
    //                 )
    //             };
    //
    //             if result.is_ok() {
    //                 // nodes unlinked from list and can be retired
    //                 let until = right.found_node.next_snapshot.pointer();
    //                 loop {
    //                     guard.retire(cur);
    //                     if next == until {
    //                         break;
    //                     }
    //                     cur = unsafe { &*(next as *const HazardObject<Node<T>>) };
    //                     next = cur.object.next.load(Ordering::Relaxed).pointer();
    //                 }
    //             } else {
    //                 // someone change pointer in 'previous' node
    //                 panic!(
    //                     "Range remove cannot be executed concurrently with other remove methods"
    //                 );
    //             }
    //             return Ok(());
    //         }
    //         // node already marked as removed by other thread
    //         panic!("Range remove cannot be executed concurrently with other remove methods");
    //     } else {
    //         Err(())
    //     }
    // }
    //

    pub fn remove<'g>(&'g self, predicate: impl Fn(&T) -> bool, guard: &'g Guard) -> Option<&'g T> {
        loop {
            if let Some(search_res) = self.search(&predicate, guard) {
                let next_snap = &search_res.found_node.next_snapshot;
                let next_ptr = if !next_snap.pointer().is_null() {
                    let right_node = unsafe { &*next_snap.pointer() }.get();
                    let right_ptr = right_node.next.load(Ordering::SeqCst);
                    if right_ptr.version() == REMOVED_NODE {
                        // next node was removed, retry
                        continue;
                    }
                    VersionedPtr::new(next_snap.pointer(), LIVE_NODE)
                } else {
                    // there is no next node, e.g. we trying to remove last node in list
                    VersionedPtr::new(ptr::null_mut(), LIVE_NODE)
                };

                let removed_marker =
                    VersionedPtr::new(search_res.found_node.next_snapshot.pointer(), REMOVED_NODE);
                // mark found node as removed
                if search_res
                    .found_node
                    .node_ptr
                    .get()
                    .next
                    .compare_exchange(
                        search_res.found_node.next_snapshot,
                        removed_marker,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_ok()
                {
                    let result = if let Some(prev) = search_res.prev {
                        // update 'next' pointer of node which is precedes found node, to unlink
                        // removed node from list
                        prev.node_ptr.get().next.compare_exchange(
                            prev.next_snapshot,
                            next_ptr,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        )
                    } else {
                        // found node is a list head, update it to unlink removed node
                        let ptr1 = self.head.load(Ordering::SeqCst);
                        self.head.compare_exchange(
                            VersionedPtr::new(
                                search_res.found_node.node_ptr.value as *const HazardObject<Node<T>>
                                    as *mut HazardObject<Node<T>>,
                                LIVE_NODE,
                            ),
                            next_ptr,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        )
                    };

                    if result.is_ok() {
                        // node unlinked from list and can be retired
                        guard.retire(search_res.found_node.node_ptr.value);
                    } else {
                        // someone change pointer in 'previous' node, we can't retire node
                        // safely, because list can still contain reference to removed node.
                        // We run search again to help to remove node.
                        self.search(&predicate, guard);
                    }
                    return Some(&search_res.found_node.node_ptr.value.object.value);
                }
                // node already marked as removed by other thread, retry
            } else {
                return None;
            }
        }
    }

    pub fn find<'s: 'g, 'g>(
        &'s self,
        predicate: impl Fn(&T) -> bool,
        guard: &'g Guard<'g>,
    ) -> Option<&'g T> {
        self.search(&predicate, guard)
            .map(|s| &s.found_node.node_ptr.value.object.value)
    }

    fn search<'s: 'g, 'g>(
        &'s self,
        predicate: &impl Fn(&T) -> bool,
        guard: &'g Guard<'g>,
    ) -> Option<Search<'g, T>> {
        let mut prev: Option<NodeSnap<T>> = None;
        let mut cur_ptr = &self.head;
        loop {
            if let Some(cur_node_ptr) = guard.read_object(cur_ptr) {
                let cur_node = cur_node_ptr.value;
                let cur_node_version = cur_node_ptr.metadata;
                let next = cur_node.get().next.load(Ordering::SeqCst);

                if let Some(p) = &prev {
                    // restart search if previous node already points to some other next node
                    // which is not equals to current
                    if p.next_snapshot
                        != VersionedPtr::new(
                            cur_node as *const HazardObject<Node<T>> as *mut HazardObject<Node<T>>,
                            cur_node_version,
                        )
                    {
                        prev = None;
                        cur_ptr = &self.head;
                        continue;
                    }
                }

                if next.version() == REMOVED_NODE {
                    // current node marked to remove
                    let result = if let Some(pred) = prev {
                        pred.node_ptr.get().next.compare_exchange(
                            pred.next_snapshot,
                            VersionedPtr::new(next.pointer(), LIVE_NODE),
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        )
                    } else {
                        // current node is head of list, update head pointer to next node
                        self.head.compare_exchange(
                            VersionedPtr::new(
                                cur_node as *const HazardObject<Node<T>>
                                    as *mut HazardObject<Node<T>>,
                                LIVE_NODE,
                            ),
                            VersionedPtr::new(next.pointer(), LIVE_NODE),
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        )
                    };
                    if result.is_ok() {
                        // node marked as removed and can be retired
                        guard.retire(cur_node);
                    }
                    // list changed, restart
                    prev = None;
                    cur_ptr = &self.head;
                } else if predicate(&cur_node.get().value) {
                    // found node which contains required element
                    return Some(Search {
                        found_node: NodeSnap {
                            node_ptr: cur_node_ptr,
                            next_snapshot: next,
                        },
                        prev,
                    });
                } else {
                    prev = Some(NodeSnap {
                        node_ptr: cur_node_ptr,
                        next_snapshot: next,
                    });
                    cur_ptr = &cur_node.get().next;
                }
            } else {
                return None;
            }
        }
    }

    pub fn iter<'g>(&'g self, guard: &'g Guard) -> impl Iterator<Item = &'g T> {
        fence(Ordering::SeqCst);
        guard
            .read_object(&self.head)
            .map(|h| Iter::new(&h.value.object, guard))
            .unwrap_or_else(|| Iter::empty(guard))
    }
}

struct Search<'a, T> {
    prev: Option<NodeSnap<'a, T>>,
    found_node: NodeSnap<'a, T>,
}

struct NodeSnap<'a, T> {
    node_ptr: PointerValue<'a, HazardObject<Node<T>>, u64>,
    next_snapshot: VersionedPtr<HazardObject<Node<T>>>,
}

struct Iter<'a, T> {
    next: Option<&'a Node<T>>,
    guard: &'a Guard<'a>,
}

impl<'a, T> Iter<'a, T> {
    fn new(head: &'a Node<T>, guard: &'a Guard) -> Self {
        Self {
            next: Some(head),
            guard,
        }
    }

    fn empty(guard: &'a Guard) -> Self {
        Self { next: None, guard }
    }
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(node) = self.next {
            self.next = self.guard.read_object(&node.next).map(|n| &n.value.object);
            Some(&node.value)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{HazardEras, LockFreeList};
    use rand::prelude::SliceRandom;
    use rand::thread_rng;

    #[test]
    fn insert_and_iter() {
        let he = HazardEras::new();
        let list = LockFreeList::new();
        for i in 0..100 {
            assert_eq!(*list.add(i, &he.new_guard()), i);
            let res_list: Vec<i32> = list.iter(&he.new_guard()).copied().collect();
            let expected: Vec<i32> = (0..=i).rev().collect();
            assert_eq!(res_list, expected);
        }
    }

    #[test]
    fn remove_all() {
        let he = HazardEras::new();
        let list = LockFreeList::new();
        for i in 0..100 {
            list.add(i, &he.new_guard());
        }

        let mut count = 0;
        while list.remove(|_| true, &he.new_guard()).is_some() {
            count += 1;
        }

        assert_eq!(count, 100);
        assert!(list.iter(&he.new_guard()).next().is_none());
        assert!(list.remove(|_| true, &he.new_guard()).is_none());
    }

    #[test]
    fn remove_list_of_lists() {
        let he = HazardEras::new();
        let list = LockFreeList::new();
        for i in 0..100 {
            let inner_list = LockFreeList::new();
            inner_list.add(i, &he.new_guard());
            list.add(inner_list, &he.new_guard());
        }

        let mut count = 0;
        while list.remove(|_| true, &he.new_guard()).is_some() {
            count += 1;
        }

        assert_eq!(count, 100);
        assert!(list.iter(&he.new_guard()).next().is_none());
        assert!(list.remove(|_| true, &he.new_guard()).is_none());
    }

    #[test]
    fn remove_using_insert_order() {
        let he = HazardEras::new();
        let list = LockFreeList::new();
        for i in 0..100 {
            list.add(i, &he.new_guard());
        }

        for i in 0..100 {
            assert!(list.remove(|v| *v == i, &he.new_guard()).is_some());
            let res_list: Vec<i32> = list.iter(&he.new_guard()).copied().collect();
            let expected: Vec<i32> = (i + 1..100).rev().collect();
            assert_eq!(res_list, expected);
        }

        assert!(list.iter(&he.new_guard()).next().is_none());
        assert!(list.remove(|_| true, &he.new_guard()).is_none());
    }

    #[test]
    fn remove_using_reversed_insert_order() {
        let he = HazardEras::new();
        let list = LockFreeList::new();
        for i in 0..100 {
            list.add(i, &he.new_guard());
        }

        for i in (0..100).rev() {
            assert!(list.remove(|v| *v == i, &he.new_guard()).is_some());
            let res_list: Vec<i32> = list.iter(&he.new_guard()).copied().collect();
            let expected: Vec<i32> = if i == 0 {
                vec![]
            } else {
                (0..=i - 1).rev().collect()
            };
            assert_eq!(res_list, expected);
        }

        assert!(list.iter(&he.new_guard()).next().is_none());
        assert!(list.remove(|_| true, &he.new_guard()).is_none());
    }

    #[test]
    fn remove_using_random_order() {
        let he = HazardEras::new();
        let list = LockFreeList::new();
        for i in 0..100 {
            list.add(i, &he.new_guard());
        }

        let mut removal_list: Vec<i32> = (0..100).collect();
        removal_list.shuffle(&mut thread_rng());
        for i in &removal_list {
            assert!(list.remove(|v| *v == *i, &he.new_guard()).is_some());
            assert!(!list.iter(&he.new_guard()).any(|v| *v == *i));
        }

        assert!(list.iter(&he.new_guard()).next().is_none());
        assert!(list.remove(|_| true, &he.new_guard()).is_none());
    }

    // #[test]
    // fn range_remove() {
    //     let he = HazardEras::new();
    //     let list = LockFreeList::new();
    //     let zero = list.add(0, &he.new_guard());
    //     let one = list.add(1, &he.new_guard());
    //     let two = list.add(2, &he.new_guard());
    //     let third = list.add(3, &he.new_guard());
    //     let fourth = list.add(4, &he.new_guard());
    //
    //     assert!(list.remove_range(third..=fourth, &he.new_guard()).is_ok());
    //     let res_list: Vec<i32> = list.iter(&he.new_guard()).copied().collect();
    //     let expected: Vec<i32> = (0..3).rev().collect();
    //     assert_eq!(res_list, expected);
    //
    //     assert!(list.remove_range(zero..=one, &he.new_guard()).is_ok());
    //     let res_list: Vec<i32> = list.iter(&he.new_guard()).copied().collect();
    //     let expected: Vec<i32> = (2..=2).rev().collect();
    //     assert_eq!(res_list, expected);
    //
    //     assert!(list.remove_range(two..=two, &he.new_guard()).is_ok());
    //     assert!(list.iter(&he.new_guard()).next().is_none());
    // }
}
