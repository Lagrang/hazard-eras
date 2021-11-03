use crate::stamped_ptr::{StampedPointer, VersionedPtr};
use crate::{Guard, HazardObject, PointerValue};
use std::option::Option::Some;
use std::sync::atomic::Ordering;

pub struct LockFreeList<T> {
    head: StampedPointer<HazardObject<Node<T>>>,
}

struct Node<T> {
    value: T,
    next: StampedPointer<HazardObject<Node<T>>>,
}

const LIVE_NODE: u64 = 0;
const REMOVED_NODE: u64 = 1;

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
            let head_ptr = self.head.load(Ordering::SeqCst);
            if !head_ptr.pointer().is_null() {
                unsafe {
                    (*new_node).object.next = StampedPointer::new(head_ptr.pointer());
                }
            }

            if self
                .head
                .compare_exchange(
                    head_ptr,
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

    pub fn remove(&self, predicate: impl Fn(&T) -> bool, guard: &Guard) -> bool {
        loop {
            if let Some(res) = self.search(&predicate, guard) {
                let right_ptr = (unsafe { &*res.found_node.next_snapshot.pointer() })
                    .get()
                    .next
                    .load(Ordering::SeqCst);
                if right_ptr.version() == REMOVED_NODE {
                    continue;
                }
                // mark found node as removed
                let removed_ptr =
                    VersionedPtr::new(res.found_node.next_snapshot.pointer(), REMOVED_NODE);
                if res
                    .found_node
                    .node_ptr
                    .get()
                    .next
                    .compare_exchange(
                        res.found_node.next_snapshot,
                        removed_ptr,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_ok()
                {
                    if let Some(prev) = res.left {
                        if prev
                            .node_ptr
                            .get()
                            .next
                            .compare_exchange(
                                prev.next_snapshot,
                                right_ptr,
                                Ordering::SeqCst,
                                Ordering::SeqCst,
                            )
                            .is_ok()
                        {
                            // node unlinked from list and can be retired
                            guard.retire(res.found_node.node_ptr.value);
                        } else {
                            // someone change pointer in 'previous' node, we can't retire node
                            // safely, because list can still contain reference to removed node.
                            // We run search again to help to remove node.
                            self.search(&predicate, guard);
                        }
                        return true;
                    }
                }
                // node already marked as removed by other thread, retry
            } else {
                return false;
            }
        }
    }

    fn search<'s: 'g, 'g>(
        &'s self,
        predicate: &impl Fn(&T) -> bool,
        guard: &'g Guard<'g>,
    ) -> Option<Search<'g, T>> {
        let mut prev: Option<NodeSnap<T>> = None;
        let mut cur_ptr = &self.head;
        loop {
            if let Some(cur_val) = guard.read_object(cur_ptr) {
                let cur_node = cur_val.value;
                let next = cur_node.get().next.load(Ordering::SeqCst);
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
                        self.head.compare_exchange(
                            VersionedPtr::new(
                                cur_node as *const HazardObject<Node<T>>
                                    as *mut HazardObject<Node<T>>,
                                cur_val.metadata,
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
                    // TODO: optimize case when several sequential nodes were removed
                    // list changed, restart
                    prev = None;
                    cur_ptr = &self.head;
                } else if predicate(&cur_node.get().value) {
                    // found node which contains required element
                    return Some(Search {
                        found_node: NodeSnap {
                            node_ptr: cur_val,
                            next_snapshot: next,
                        },
                        left: prev,
                    });
                } else {
                    prev = Some(NodeSnap {
                        node_ptr: cur_val,
                        next_snapshot: next,
                    });
                    cur_ptr = &cur_node.get().next;
                }
            } else {
                return None;
            }
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> {
        // TODO:
        std::iter::empty()
    }
}

struct Search<'a, T> {
    left: Option<NodeSnap<'a, T>>,
    found_node: NodeSnap<'a, T>,
}

struct NodeSnap<'a, T> {
    node_ptr: PointerValue<'a, HazardObject<Node<T>>, u64>,
    next_snapshot: VersionedPtr<HazardObject<Node<T>>>,
}
