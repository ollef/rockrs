mod deadlock_detector;
mod scratch;

use crossbeam::deque::{self, Injector, Steal, Stealer};
use dashmap::DashMap;
use deadlock_detector::DeadlockDetector;
use event_listener::{Event, Listener, listener};
use fxhash::{FxBuildHasher, FxHashSet};
use std::{
    cell::RefCell,
    fmt::Debug,
    hash::Hash,
    pin::pin,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    task::{Poll, Waker},
    thread::{JoinHandle, Thread},
};

#[derive(PartialEq, Eq, Clone, Copy)]
pub struct WorkerId(usize);

type FxDashMap<K, V> = DashMap<K, V, FxBuildHasher>;

trait Database
where
    Self: Sized,
{
    type Query: Clone + Eq + Debug + Hash;

    fn dispatch<D>(d: D, q: Self::Query) -> D::Result
    where
        D: Dispatch<Self>;
}

struct GlobalContext<DB: Database> {
    database: DB,
    workers: Vec<Worker<DB>>,
    deadlock_detector: DeadlockDetector,
    injector: Injector<DB::Query>,
    shutdown: AtomicBool,
}

struct Worker<DB: Database> {
    thread: OnceLock<Thread>,
    is_idle: AtomicBool,
    stealer: Stealer<DB::Query>,
}

struct Context<DB: Database> {
    id: WorkerId,
    global: Arc<GlobalContext<DB>>,
    stealable: deque::Worker<DB::Query>,
    waker: Waker,
    current_query: RefCell<Option<DB::Query>>,
    query_dependencies: RefCell<Vec<DB::Query>>,
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
    Queued {
        event: Arc<Event>,
    },
    InProgress {
        worker: WorkerId,
        event: Arc<Event>,
    },
    Completed {
        result: Result,
        dependencies: Vec<Query>,
        reverse_dependencies: Mutex<FxHashSet<Query>>,
    },
    Poisoned,
}

struct InProgressGuard<'a, DB: Database, Q: Query<DB>> {
    storage: &'a FxDashMap<Q, Entry<Q::Result, DB::Query>>,
    query: Option<Q>,
}

impl<'a, DB: Database, Q: Query<DB>> Drop for InProgressGuard<'a, DB, Q> {
    fn drop(&mut self) {
        if let Some(query) = self.query.take() {
            let Some(Entry::InProgress { event, .. }) = self.storage.insert(query, Entry::Poisoned)
            else {
                unreachable!();
            };
            event.notify(usize::MAX);
        }
    }
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
                    Entry::Queued { .. } => {
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
                    Entry::Completed {
                        result,
                        reverse_dependencies,
                        ..
                    } => {
                        if let Some(current_query) = self.current_query.borrow().as_ref() {
                            reverse_dependencies
                                .lock()
                                .unwrap()
                                .insert(current_query.clone());
                        }
                        return result.clone();
                    }
                    Entry::Poisoned => {
                        drop(entry);
                        panic!("Query panicked during execution")
                    }
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
            entry.insert(Entry::Queued {
                event: Arc::new(Event::new()),
            });
            self.stealable.push(query.clone().into());
            self.global.try_wake_idle_thread();
        }
    }

    fn try_claim_and_execute<Q: Query<DB>, T>(
        &self,
        query: Q,
        f: impl FnOnce(&Q::Result) -> T,
    ) -> Option<T> {
        let storage = Q::storage(&self.global.database);
        match storage.entry(query.clone()) {
            dashmap::Entry::Occupied(mut occupied) => match occupied.get() {
                Entry::Queued { .. } => {
                    let Entry::Queued { event } =
                        std::mem::replace(occupied.get_mut(), Entry::Poisoned)
                    else {
                        unreachable!()
                    };
                    occupied.insert(Entry::InProgress {
                        worker: self.id,
                        event,
                    });
                }
                Entry::InProgress { .. } | Entry::Completed { .. } | Entry::Poisoned => {
                    return None;
                }
            },
            dashmap::Entry::Vacant(vacant_entry) => {
                vacant_entry.insert(Entry::InProgress {
                    worker: self.id,
                    event: Arc::new(Event::new()),
                });
            }
        }
        let mut panic_guard = InProgressGuard {
            storage,
            query: Some(query.clone()),
        };

        let (query_result, dependencies) = {
            let saved_dependencies = self.query_dependencies.take();
            let saved_current_query = self.current_query.replace(Some(query.clone().into()));
            let query_result =
                stacker::maybe_grow(64 * 1024, 1024 * 1024, || Q::rule(self, &query));
            self.current_query.replace(saved_current_query);
            let dependencies = self.query_dependencies.replace(saved_dependencies);
            (query_result, dependencies)
        };
        let result = f(&query_result);
        panic_guard.query = None;
        let mut reverse_dependencies = FxHashSet::default();
        if let Some(current_query) = self.current_query.borrow().as_ref() {
            reverse_dependencies.insert(current_query.clone());
        }
        let old_entry = storage.insert(
            query,
            Entry::Completed {
                result: query_result,
                dependencies,
                reverse_dependencies: Mutex::new(reverse_dependencies),
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
            loop {
                match self.global.injector.steal_batch_and_pop(&self.stealable) {
                    Steal::Success(query) => return Some(query),
                    Steal::Empty => break,
                    Steal::Retry => continue,
                }
            }

            for worker in self.global.workers[self.id.0 + 1..self.global.workers.len()]
                .iter()
                .chain(&self.global.workers[0..self.id.0])
            {
                loop {
                    match worker.stealer.steal_batch_and_pop(&self.stealable) {
                        Steal::Success(query) => return Some(query),
                        Steal::Empty => break,
                        Steal::Retry => continue,
                    }
                }
            }
            None
        })
    }
}

impl<DB: Database> GlobalContext<DB> {
    pub(crate) fn try_wake_idle_thread(&self) {
        for worker in &self.workers {
            if let Some(thread) = worker.thread.get()
                && worker
                    .is_idle
                    .compare_exchange(true, false, Ordering::Acquire, Ordering::Relaxed)
                    .is_ok()
            {
                thread.unpark();
                break;
            }
        }
    }
}

struct Engine<DB: Database> {
    global: Arc<GlobalContext<DB>>,
    handles: Vec<JoinHandle<()>>,
}

impl<DB: Database + Send + Sync + 'static> Engine<DB>
where
    DB::Query: Send,
{
    pub fn new(database: DB, num_workers: usize) -> Self {
        let global = Arc::new(GlobalContext {
            database,
            workers: Vec::from_iter((0..num_workers).map(|_| Worker {
                thread: OnceLock::new(),
                is_idle: AtomicBool::new(true),
                stealer: deque::Worker::new_fifo().stealer(),
            })),
            deadlock_detector: DeadlockDetector::new(num_workers),
            injector: Injector::new(),
            shutdown: AtomicBool::new(false),
        });

        let handles = (0..num_workers)
            .map(|id| {
                let global = global.clone();
                std::thread::spawn(move || {
                    let thread = std::thread::current();
                    global.workers[id].thread.set(thread.clone()).unwrap();
                    let context = Context {
                        id: WorkerId(id),
                        global,
                        stealable: deque::Worker::new_fifo(),
                        waker: waker_fn::waker_fn(move || {
                            thread.unpark();
                        }),
                        current_query: RefCell::new(None),
                        query_dependencies: RefCell::new(Vec::new()),
                    };

                    loop {
                        if context.global.shutdown.load(Ordering::Acquire) {
                            break;
                        }

                        if let Some(query) = context.find_work() {
                            DB::dispatch(Theft { context: &context }, query);
                            continue;
                        }

                        context.global.workers[id]
                            .is_idle
                            .store(true, Ordering::Release);
                        std::thread::park();
                        context.global.workers[id]
                            .is_idle
                            .store(false, Ordering::Release);
                    }
                })
            })
            .collect();

        Self { global, handles }
    }

    pub fn fetch<Q: Query<DB>>(&self, query: &Q) -> Q::Result {
        let storage = Q::storage(&self.global.database);

        loop {
            match storage.entry(query.clone()) {
                dashmap::Entry::Occupied(entry) => match entry.get() {
                    Entry::Completed { result, .. } => return result.clone(),
                    Entry::Poisoned => panic!("Query panicked during execution"),
                    Entry::InProgress { event, .. } | Entry::Queued { event } => {
                        listener!(event => listener);
                        listener.wait();
                    }
                },
                dashmap::Entry::Vacant(entry) => {
                    let event = Arc::new(Event::new());
                    listener!(event => listener);
                    entry.insert(Entry::Queued {
                        event: event.clone(),
                    });
                    self.global.injector.push(query.clone().into());
                    self.global.try_wake_idle_thread();
                    listener.wait();
                }
            }
        }
    }
}

impl<DB: Database> Drop for Engine<DB> {
    fn drop(&mut self) {
        self.global.shutdown.store(true, Ordering::Release);
        for worker in &self.global.workers {
            worker.thread.wait().unpark();
        }
        for handle in self.handles.drain(..) {
            handle.join().unwrap();
        }
    }
}
