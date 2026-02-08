mod scratch;

use crossbeam::sync::{Parker, Unparker};
use dashmap::DashMap;
use fxhash::FxBuildHasher;
use std::{cell::RefCell, collections::VecDeque, hash::Hash, rc::Rc, thread::ThreadId};

type FxDashMap<K, V> = DashMap<K, V, FxBuildHasher>;

trait Database
where
    Self: Sized,
{
    type Query: Clone + Eq + std::fmt::Debug;

    fn dispatch<D>(d: D, q: Self::Query) -> D::Result
    where
        D: Dispatch<Self>;
}

struct Context<DB: Database> {
    query_dependencies: RefCell<Vec<DB::Query>>,
    stealable: RefCell<Vec<Stealable<DB::Query>>>,
    thieves: RefCell<VecDeque<(ThreadId, Unparker)>>,
    database: DB,
    thread_dependencies: DashMap<ThreadId, ThreadId>,
}

struct Stealable<Q> {
    query: Q,
}

struct Thievery<'a, DB: Database> {
    context: &'a Context<DB>,
}

impl<DB: Database> Dispatch<DB> for Thievery<'_, DB> {
    type Result = ();

    fn dispatch<Q: Query<DB>>(self, query: Q) -> Self::Result {
        let map = Q::sub_map(&self.context.database);
        let waiters = match map.entry(query.clone()) {
            dashmap::Entry::Occupied(_) => return,
            dashmap::Entry::Vacant(vacant_entry) => {
                let waiters = Rc::new(RefCell::new(Vec::new()));
                let tid = std::thread::current().id();
                vacant_entry.insert(Entry::InProgress {
                    thread_id: tid,
                    waiters: waiters.clone(),
                });
                waiters
            }
        };

        let (result, dependencies) = self.context.rule(&query);
        map.insert(
            query,
            Entry::Complete {
                result,
                dependencies,
            },
        );
        for (waiting_thread_id, waiter) in waiters.borrow().iter() {
            self.context.thread_dependencies.remove(waiting_thread_id);
            waiter.unpark();
        }
    }
}

trait Query<DB: Database>
where
    Self: Clone + Eq + Hash + Into<DB::Query>,
{
    type Result: Clone;

    fn rule(qc: &Context<DB>, query: &Self) -> Self::Result;
    fn sub_map(db: &DB) -> &FxDashMap<Self, Entry<Self::Result, DB::Query>>;
}

trait Dispatch<DB: Database> {
    type Result;
    fn dispatch<Q: Query<DB>>(self, query: Q) -> Self::Result;
}

#[derive(Clone)]
pub enum Entry<Result, Query> {
    InProgress {
        thread_id: ThreadId,
        waiters: Rc<RefCell<Vec<(ThreadId, Unparker)>>>,
    },
    Complete {
        result: Result,
        dependencies: Vec<Query>,
    },
}

enum TryFetch<Result, Query> {
    Stole(Stealable<Query>),
    WaitFor(Parker),
    Complete(Result),
}

impl<DB: Database> Context<DB> {
    fn deadlock_check(&self, other_tid: ThreadId) {
        let my_tid = std::thread::current().id();
        self.thread_dependencies.insert(my_tid, other_tid);
        let mut current = other_tid;
        while let Some(next) = self.thread_dependencies.get(&current).map(|entry| *entry) {
            if next == my_tid {
                panic!("cyclic query detected");
            }
            current = next;
        }
    }

    fn rule<Q: Query<DB>>(&self, query: &Q) -> (Q::Result, Vec<DB::Query>) {
        let mut saved_dependencies = self.query_dependencies.take();
        let result = Q::rule(self, query);
        saved_dependencies.push(query.clone().into());
        let query_dependencies = self.query_dependencies.replace(saved_dependencies);
        (result, query_dependencies)
    }

    fn steal(&self, stealable: Stealable<DB::Query>) {
        DB::dispatch(Thievery { context: self }, stealable.query);
    }

    fn try_fetch<Q: Query<DB>>(&self, query: Q) -> TryFetch<Q::Result, DB::Query> {
        let map = Q::sub_map(&self.database);
        let waiters = match map.entry(query.clone()) {
            dashmap::Entry::Occupied(mut occupied_entry) => match occupied_entry.get() {
                Entry::InProgress { .. } => {
                    let Entry::InProgress { thread_id, waiters } = occupied_entry.get_mut() else {
                        unreachable!()
                    };
                    if let Some(stealable) = self.stealable.borrow_mut().pop() {
                        return TryFetch::Stole(stealable);
                    }
                    let parker = Parker::new();
                    let unparker = parker.unparker();
                    waiters
                        .borrow_mut()
                        .push((std::thread::current().id(), unparker.clone()));
                    self.thieves
                        .borrow_mut()
                        .push_back((std::thread::current().id(), unparker.clone()));
                    self.deadlock_check(*thread_id);
                    return TryFetch::WaitFor(parker);
                }
                Entry::Complete { result, .. } => {
                    return TryFetch::Complete(result.clone());
                }
            },
            dashmap::Entry::Vacant(vacant_entry) => {
                let waiters = Rc::new(RefCell::new(Vec::new()));
                let tid = std::thread::current().id();
                vacant_entry.insert(Entry::InProgress {
                    thread_id: tid,
                    waiters: waiters.clone(),
                });
                waiters
            }
        };

        let (result, dependencies) = self.rule(&query);
        map.insert(
            query,
            Entry::Complete {
                result: result.clone(),
                dependencies,
            },
        );
        for (waiting_thread_id, waiter) in waiters.borrow().iter() {
            self.thread_dependencies.remove(waiting_thread_id);
            waiter.unpark();
        }
        TryFetch::Complete(result)
    }

    pub fn fetch<Q: Query<DB>>(&self, query: &Q) -> Q::Result {
        loop {
            match self.try_fetch(query.clone()) {
                TryFetch::Stole(stealable) => self.steal(stealable),
                TryFetch::WaitFor(parker) => parker.park(),
                TryFetch::Complete(result) => return result,
            }
        }
    }
}
