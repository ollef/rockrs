mod deadlock_detector;
mod scratch;

use crossbeam::deque::{self, Stealer};
use dashmap::DashMap;
use deadlock_detector::DeadlockDetector;
use event_listener::{Event, Listener, listener};
use fxhash::FxBuildHasher;
use std::{
    cell::RefCell,
    hash::Hash,
    pin::pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Poll, Waker},
    thread::Thread,
};

#[derive(PartialEq, Eq, Clone, Copy)]
pub struct WorkerId(usize);

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

struct GlobalContext<DB: Database> {
    database: DB,
    workers: Vec<Worker<DB>>,
    deadlock_detector: DeadlockDetector,
}

struct Worker<DB: Database> {
    thread: Thread,
    is_idle: AtomicBool,
    stealer: Stealer<DB::Query>,
}

struct Context<DB: Database> {
    id: WorkerId,
    global: Arc<GlobalContext<DB>>,
    stealable: deque::Worker<DB::Query>,
    waker: Waker,
    query_dependencies: RefCell<Vec<DB::Query>>,
}

struct Stealable<Q> {
    query: Q,
}

struct Theft<'a, DB: Database> {
    context: &'a Context<DB>,
}

impl<DB: Database> Dispatch<DB> for Theft<'_, DB> {
    type Result = ();

    fn dispatch<Q: Query<DB>>(self, query: Q) -> Self::Result {
        self.context.try_claim_and_execute(query, |_| ());
    }
}

trait Query<DB: Database>
where
    Self: Clone + Eq + Hash + Into<DB::Query>,
{
    type Result: Clone;

    fn storage(db: &DB) -> &FxDashMap<Self, Entry<Self::Result, DB::Query>>;
    fn rule(qc: &Context<DB>, query: &Self) -> Self::Result;
}

trait Dispatch<DB: Database> {
    type Result;
    fn dispatch<Q: Query<DB>>(self, query: Q) -> Self::Result;
}

pub enum Entry<Result, Query> {
    Queued,
    InProgress {
        worker: WorkerId,
        event: Arc<Event>,
    },
    Completed {
        result: Result,
        dependencies: Vec<Query>,
    },
}

impl<DB: Database> Context<DB> {
    pub fn fetch<Q: Query<DB>>(&self, query: &Q) -> Q::Result {
        self.query_dependencies
            .borrow_mut()
            .push(query.clone().into());
        let storage = Q::storage(&self.global.database);
        loop {
            match storage.get(query) {
                Some(entry) => match entry.value() {
                    Entry::Queued => {
                        drop(entry);
                        if let Some(result) =
                            self.try_claim_and_execute(query.clone(), Clone::clone)
                        {
                            return result;
                        }
                    }
                    Entry::InProgress { worker, event } => {
                        let worker = *worker;
                        let event = event.clone();
                        listener!(event => listener);
                        drop(entry);
                        if let Err(()) = self.global.deadlock_detector.add_wait(self.id, worker) {
                            panic!("Deadlock detected");
                        }

                        let _guard = deadlock_detector::WaitGuard {
                            detector: &self.global.deadlock_detector,
                            me: self.id,
                            other: worker,
                        };

                        self.wait(listener);

                        continue;
                    }
                    Entry::Completed { result, .. } => return result.clone(),
                },
                None => {
                    if let Some(result) = self.try_claim_and_execute(query.clone(), Clone::clone) {
                        return result;
                    }
                }
            }
        }
    }

    pub fn prefetch<Q: Query<DB>>(&self, query: &Q) {
        if self.stealable.len() > 128 {
            return;
        }

        if let dashmap::Entry::Vacant(entry) =
            Q::storage(&self.global.database).entry(query.clone())
        {
            entry.insert(Entry::Queued);
            self.stealable.push(query.clone().into());

            for worker in &self.global.workers {
                if worker
                    .is_idle
                    .compare_exchange(true, false, Ordering::Acquire, Ordering::Relaxed)
                    .is_ok()
                {
                    worker.thread.unpark();
                    break;
                }
            }
        }
    }

    fn rule<Q: Query<DB>>(&self, query: &Q) -> (Q::Result, Vec<DB::Query>) {
        let saved_dependencies = self.query_dependencies.take();
        let result = Q::rule(self, query);
        let query_dependencies = self.query_dependencies.replace(saved_dependencies);
        (result, query_dependencies)
    }

    fn try_claim_and_execute<Q: Query<DB>, T>(
        &self,
        query: Q,
        f: impl FnOnce(&Q::Result) -> T,
    ) -> Option<T> {
        let storage = Q::storage(&self.global.database);
        match storage.entry(query.clone()) {
            dashmap::Entry::Occupied(mut occupied) => match occupied.get() {
                Entry::Queued => {
                    occupied.insert(Entry::InProgress {
                        worker: self.id,
                        event: Arc::new(Event::new()),
                    });
                }
                Entry::InProgress { .. } | Entry::Completed { .. } => return None,
            },
            dashmap::Entry::Vacant(vacant_entry) => {
                vacant_entry.insert(Entry::InProgress {
                    worker: self.id,
                    event: Arc::new(Event::new()),
                });
            }
        }
        let (query_result, dependencies) =
            stacker::maybe_grow(64 * 1024, 1024 * 1024, || self.rule(&query));
        let result = f(&query_result);
        let old_entry = storage.insert(
            query,
            Entry::Completed {
                result: query_result,
                dependencies,
            },
        );
        let event = match old_entry {
            Some(Entry::InProgress { event, .. }) => event,
            _ => unreachable!(),
        };
        event.notify(usize::MAX);
        Some(result)
    }

    fn wait(&self, listener: impl Listener) {
        let mut listener = pin!(listener);
        let mut context = std::task::Context::from_waker(&self.waker);
        loop {
            if let Poll::Ready(()) = listener.as_mut().poll(&mut context) {
                break;
            }

            if let Some(query) = self.find_work() {
                DB::dispatch(Theft { context: self }, query);
                continue;
            }

            self.global.workers[self.id.0]
                .is_idle
                .store(true, Ordering::Release);
            std::thread::park();
            self.global.workers[self.id.0]
                .is_idle
                .store(false, Ordering::Release);
        }
    }

    fn find_work(&self) -> Option<DB::Query> {
        self.stealable.pop().or_else(|| {
            for worker in self.global.workers[self.id.0 + 1..self.global.workers.len()]
                .iter()
                .chain(&self.global.workers[0..self.id.0])
            {
                loop {
                    match worker.stealer.steal_batch_and_pop(&self.stealable) {
                        crossbeam::deque::Steal::Success(query) => return Some(query),
                        crossbeam::deque::Steal::Empty => break,
                        crossbeam::deque::Steal::Retry => continue,
                    }
                }
            }
            None
        })
    }
}
