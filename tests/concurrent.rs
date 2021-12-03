use std::time::Instant;

use rand::prelude::*;
use rand::seq::SliceRandom;

use crate::history_verifier::{History, Ops};
use crossbeam_utils::thread;
use hazard_eras::list::LockFreeList;
use hazard_eras::{Guard, HazardEras};
use std::fmt::Debug;
use std::hash::Hash;
use std::mem;

mod history_verifier;

pub trait DataStructure<K, V> {
    fn insert(&self, key: K, value: V, guard: &Guard) -> bool;
    fn upsert<'g>(&'g self, key: K, value: V, guard: &'g Guard) -> Option<&'g V>;
    fn is_upsert_supported(&self) -> bool;
    fn delete<'g>(&'g self, key: &K, guard: &'g Guard) -> Option<&'g V>;
    fn get<'g>(&'g self, key: &K, guard: &'g Guard) -> Option<&'g V>;
    fn iter<'g>(&'g self, guard: &'g Guard) -> Box<dyn Iterator<Item = (&'g K, &'g V)> + 'g>;
    fn double_ended_iter<'g>(
        &'g self,
        guard: &'g Guard,
    ) -> Box<dyn DoubleEndedIterator<Item = (&'g K, &'g V)> + 'g>;
    fn is_double_ended_iter_supported(&self) -> bool;
}

impl<K: Ord, V> DataStructure<K, V> for LockFreeList<(K, V)> {
    fn insert(&self, key: K, value: V, guard: &Guard) -> bool {
        self.add((key, value), guard);
        true
    }

    fn upsert<'g>(&'g self, _: K, _: V, _: &'g Guard) -> Option<&'g V> {
        panic!("Not supported");
    }

    fn is_upsert_supported(&self) -> bool {
        false
    }

    fn delete<'g>(&'g self, key: &K, guard: &'g Guard) -> Option<&'g V> {
        self.remove(|(k, _)| k == key, guard).map(|(_, v)| v)
    }

    fn get<'g>(&'g self, key: &K, guard: &'g Guard) -> Option<&'g V> {
        self.find(|(k, _)| k == key, guard).map(|(_, v)| v)
    }

    fn iter<'g>(&'g self, guard: &'g Guard) -> Box<dyn Iterator<Item = (&'g K, &'g V)> + 'g> {
        Box::new(self.iter(guard).map(|(k, v)| (k, v)))
    }

    fn double_ended_iter<'g>(
        &'g self,
        _: &'g Guard,
    ) -> Box<dyn DoubleEndedIterator<Item = (&'g K, &'g V)> + 'g> {
        panic!()
    }

    fn is_double_ended_iter_supported<'g>(&self) -> bool {
        false
    }
}

fn test<F, TreeSupplier, K, V>(data_structure_creator: TreeSupplier, test: F)
where
    F: Fn(Box<dyn DataStructure<K, V> + Send + Sync>, usize, usize),
    K: Clone + Ord + Hash + Debug,
    V: Send + Sync,
    TreeSupplier: Fn() -> Box<dyn DataStructure<K, V> + Send + Sync>,
{
    let cpus = num_cpus::get();
    let per_thread_changes = 5000;
    for mult in 1..=3 {
        let threads = cpus * mult;
        let structure = data_structure_creator();
        test(structure, threads, per_thread_changes);
    }
}

#[test]
fn insert_of_non_overlaping_keys_and_search() {
    let he_val = HazardEras::new();
    let he = &he_val;
    test(
        || Box::new(LockFreeList::new()),
        |ds, threads, shard_size: usize| {
            let ds = ds.as_ref();
            thread::scope(|scope| {
                for id in 0..threads {
                    scope.spawn(move |_| {
                        let thread_id = id;
                        let base = shard_size * thread_id;
                        let mut keys: Vec<usize> = (base..base + shard_size).collect();
                        keys.shuffle(&mut thread_rng());
                        for i in keys {
                            let guard = he.new_guard();
                            let key = i.to_string();
                            let value = thread_rng().gen::<usize>();
                            if !ds.is_upsert_supported() || thread_rng().gen_bool(0.5) {
                                ds.insert(key.clone(), value, &guard);
                            } else {
                                ds.upsert(key.clone(), value, &guard);
                            }

                            let found_val = ds
                                .get(&key, &guard)
                                .unwrap_or_else(|| panic!("{:?} not found", &key));
                            assert_eq!(found_val, &value);
                        }
                    });
                }
            })
            .unwrap();
        },
    );
}

#[test]
fn upsert_of_overlaping_keys() {
    let he = &HazardEras::new();
    test(
        || Box::new(LockFreeList::new()),
        |ds, threads, thread_changes_count| {
            let ds = ds.as_ref();
            let mut per_thread_elem_set = Vec::new();
            for _ in 0..threads {
                let mut elems = Vec::with_capacity(thread_changes_count);
                (0..thread_changes_count).for_each(|i| elems.push(i));
                elems.shuffle(&mut thread_rng());
                per_thread_elem_set.push(elems);
            }

            let history = thread::scope(|scope| {
                let mut handles = Vec::new();
                for elems in &per_thread_elem_set {
                    handles.push(scope.spawn(move |_| {
                        let mut ops = Ops::new();
                        for i in elems {
                            let guard = he.new_guard();
                            let key = i.to_string();
                            let value = thread_rng().gen::<usize>();
                            let start = Instant::now();
                            ds.upsert(key.clone(), value, &guard);
                            ops.insert(key, value, start);
                        }
                        ops
                    }));
                }

                let ops = handles
                    .drain(..)
                    .map(|h| h.join().unwrap())
                    .fold(Ops::new(), |ops1, ops2| ops1.merge(ops2));

                History::based_on(ops)
            })
            .unwrap();

            let guard = he.new_guard();
            history.run_check(|key| ds.get(key, &guard));
        },
    );
}

#[test]
fn add_and_delete() {
    let he = &HazardEras::new();
    let max_val = 50;
    test(
        || Box::new(LockFreeList::new()),
        |ds, threads, thread_changes| {
            let ds = ds.as_ref();
            let mut per_thread_elem_set = Vec::with_capacity(threads);
            for _ in 0..threads {
                // each thread upsert and delete small range of keys
                // to increase contention on same values between threads
                let mut indexes = Vec::with_capacity(thread_changes);
                (0..thread_changes).for_each(|_| indexes.push(thread_rng().gen_range(1..max_val)));
                per_thread_elem_set.push(indexes);
            }

            let history = thread::scope(|scope| {
                let mut handles = Vec::new();
                for elems in &per_thread_elem_set {
                    handles.push(scope.spawn(move |_| {
                        let mut ops = Ops::new();
                        for i in elems {
                            let guard = he.new_guard();
                            let key = i.to_string();
                            let value = thread_rng().gen_range(1..100000);
                            let start = Instant::now();
                            if thread_rng().gen_bool(0.4) {
                                ds.upsert(key.clone(), value, &guard);
                                ops.insert(key, value, start);
                            } else if thread_rng().gen_bool(0.3)
                                && ds.delete(&key, &he.new_guard()).is_some()
                            {
                                ops.delete(key.clone(), start);
                            } else if ds.insert(key.clone(), value, &he.new_guard()) {
                                ops.insert(key.clone(), value, start);
                            }
                        }
                        ops
                    }));
                }

                let ops = handles
                    .drain(..)
                    .map(|h| h.join().unwrap())
                    .fold(Ops::new(), |ops1, ops2| ops1.merge(ops2));

                History::based_on(ops)
            })
            .unwrap();

            let guard = he.new_guard();
            history.run_check(|key| ds.get(key, &guard));
        },
    );
}

#[test]
fn key_search() {
    let he = &HazardEras::new();
    test(
        || Box::new(LockFreeList::new()),
        |ds, threads, changes| {
            let ds = ds.as_ref();
            thread::scope(|scope| {
                let mut keys: Vec<usize> = (0..changes * threads).collect();
                keys.shuffle(&mut thread_rng());
                for i in keys {
                    let guard = he.new_guard();
                    let key = i.to_string();
                    let value = i;
                    ds.insert(key, value, &guard);
                }

                for thread_id in 0..threads {
                    scope.spawn(move |_| {
                        let base = changes * thread_id;
                        for i in base..base + changes {
                            let key = i.to_string();
                            let guard = he.new_guard();
                            let found_val = ds
                                .get(&key, &guard)
                                .unwrap_or_else(|| panic!("{:?} not found", &key));
                            assert_eq!(*found_val, i);
                        }
                    });
                }
            })
            .unwrap();
        },
    );
}

#[test]
fn overlapped_inserts_and_deletes() {
    let he = &HazardEras::new();
    test(
        || Box::new(LockFreeList::new()),
        |ds, threads, changes| {
            let ds = ds.as_ref();
            let min = 0;
            let max = (threads - 1) * changes + changes;
            let mid = max / 2;
            for thread_id in 0..threads {
                let guard = he.new_guard();
                let mut keys: Vec<usize> =
                    (thread_id * changes..thread_id * changes + changes).collect();
                keys.shuffle(&mut thread_rng());
                for i in keys {
                    ds.insert(i.to_string(), i.to_string(), &guard);
                }
            }

            thread::scope(|scope| {
                scope.spawn(move |_| {
                    for i in min..max {
                        let guard = he.new_guard();
                        ds.insert(i.to_string(), i.to_string(), &guard);
                    }

                    for i in min..max {
                        let guard = he.new_guard();
                        ds.delete(&i.to_string(), &guard);
                    }
                });

                scope.spawn(move |_| {
                    for i in (min..max).rev() {
                        let guard = he.new_guard();
                        ds.insert(i.to_string(), i.to_string(), &guard);
                    }

                    for i in (min..max).rev() {
                        let guard = he.new_guard();
                        ds.delete(&i.to_string(), &guard);
                    }
                });

                scope.spawn(move |_| {
                    for i in min..mid {
                        let guard = he.new_guard();
                        ds.insert(i.to_string(), i.to_string(), &guard);
                    }
                    for i in min..mid {
                        let guard = he.new_guard();
                        ds.delete(&i.to_string(), &guard);
                    }
                });

                scope.spawn(move |_| {
                    for i in (mid..max).rev() {
                        let guard = he.new_guard();
                        ds.insert(i.to_string(), i.to_string(), &guard);
                    }
                    for i in (mid..max).rev() {
                        let guard = he.new_guard();
                        ds.delete(&i.to_string(), &guard);
                    }
                });

                scope.spawn(move |_| {
                    for i in (min..mid).rev() {
                        let guard = he.new_guard();
                        ds.insert(i.to_string(), i.to_string(), &guard);
                    }
                    for i in (min..mid).rev() {
                        let guard = he.new_guard();
                        ds.delete(&i.to_string(), &guard);
                    }
                });

                scope.spawn(move |_| {
                    for i in mid..max {
                        let guard = he.new_guard();
                        ds.insert(i.to_string(), i.to_string(), &guard);
                    }
                    for i in mid..max {
                        let guard = he.new_guard();
                        ds.delete(&i.to_string(), &guard);
                    }
                });
            })
            .unwrap();

            assert!(ds.iter(&he.new_guard()).next().is_none());
        },
    );
}

#[test]
fn liveness() {
    let he = &HazardEras::new();
    test(
        || Box::new(LockFreeList::new()),
        |ds, threads, shard_size: usize| {
            let ds = ds.as_ref();
            thread::scope(|scope| {
                for id in 0..threads {
                    scope.spawn(move |_| {
                        let thread_id = id;
                        let base = shard_size * thread_id;
                        let mut keys: Vec<usize> = (base..base + shard_size).collect();
                        keys.shuffle(&mut thread_rng());
                        for i in keys {
                            let guard = he.new_guard();
                            let key = i.to_string();
                            let value = thread_rng().gen::<usize>();
                            if thread_rng().gen_bool(0.5) {
                                ds.insert(key.clone(), value, &guard);
                            } else {
                                ds.upsert(key.clone(), value, &guard);
                            }
                        }
                    });

                    scope.spawn(move |_| {
                        let thread_id = id;
                        let base = shard_size * thread_id;
                        let mut keys: Vec<usize> = (base..base + shard_size).collect();
                        keys.shuffle(&mut thread_rng());
                        for i in keys {
                            let guard = he.new_guard();
                            let key = i.to_string();
                            ds.delete(&key, &guard);
                        }
                    });

                    scope.spawn(move |_| {
                        let thread_id = id;
                        let base = shard_size * thread_id;
                        let mut keys: Vec<usize> = (base..base + shard_size).collect();
                        keys.shuffle(&mut thread_rng());
                        for i in keys {
                            let guard = he.new_guard();
                            let key = i.to_string();
                            ds.get(&key, &guard);
                        }
                    });

                    // scope.spawn(move |_| {
                    //     let thread_id = id;
                    //     let base = shard_size * thread_id;
                    //     let keys: Vec<usize> = (base..base + shard_size).collect();
                    //     for _ in 0..keys.len() {
                    //         let guard = he.new_guard();
                    //         let i1 = thread_rng().gen_range(0..keys.len());
                    //         let i2 = thread_rng().gen_range(0..keys.len());
                    //         let start = keys.get(std::cmp::min(i1, i2)).unwrap().to_string();
                    //         let end = keys.get(std::cmp::max(i1, i2)).unwrap().to_string();
                    //         tree.range(start..end, &guard);
                    //     }
                    // });
                }
            })
            .unwrap();
        },
    );
}
