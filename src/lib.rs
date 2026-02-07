use crossbeam::sync::Unparker;
use dashmap::DashMap;
use fxhash::FxBuildHasher;
use std::{cell::RefCell, collections::VecDeque, hash::Hash, rc::Rc, sync::Once, thread::ThreadId};
mod scratch;

type FxDashMap<K, V> = DashMap<K, V, FxBuildHasher>;

trait Database {
    type Query: Clone + Eq;
}

struct QueryContext<DB: Database> {
    stack: RefCell<Vec<DB::Query>>,
    dependencies: RefCell<Vec<DB::Query>>,
}

struct Waiters {
    storage: RefCell<VecDeque<(ThreadId, Unparker)>>,
}

impl Waiters {
    fn new() -> Self {
        Self {
            storage: RefCell::new(VecDeque::new()),
        }
    }

    fn add(&self, unparker: Unparker) {
        let tid = std::thread::current().id();
        self.storage.borrow_mut().push_back((tid, unparker));
    }

    fn remove(&self) -> Option<Unparker> {
        let tid = std::thread::current().id();
        let mut storage = self.storage.borrow_mut();
        storage
            .iter()
            .position(|(t, _)| *t == tid)
            .map(|pos| storage.remove(pos).unwrap().1)
    }

    fn unpark_all(&self) {
        let mut storage = self.storage.borrow_mut();
        for (_, unparker) in storage.drain(..) {
            unparker.unpark();
        }
    }

    fn unpark_one(&self) {
        if let Some((_, unparker)) = self.storage.borrow_mut().pop_front() {
            unparker.unpark();
        }
    }
}

struct Context<DB: Database> {
    query: QueryContext<DB>,
    stealable: RefCell<Vec<Stealable<DB::Query>>>,
    database: DB,
    thread_dependencies: DashMap<ThreadId, ThreadId>,
}

struct Stealable<Q> {
    query: Q,
    stack: Vec<Q>,
}

trait Query<DB: Database>
where
    Self: Clone,
{
    type Result: Clone;

    fn inject(self) -> DB::Query;
    fn rule(qc: &Context<DB>, query: &Self) -> Self::Result;
    fn try_fetch(qc: &Context<DB>, query: Self) -> EntryStatus<Self::Result>;
}

trait Queries<DB: Database>
where
    Self: Clone + std::fmt::Debug,
{
    fn try_fetch(qc: &Context<DB>, query: Self) -> EntryStatus<()>;
}

#[derive(Clone)]
pub enum EntryStatus<Result> {
    InProgress(ThreadId, Rc<Once>),
    Complete(Result),
}

impl<T> EntryStatus<T> {
    pub fn map<S>(self, f: impl FnOnce(T) -> S) -> EntryStatus<S> {
        match self {
            EntryStatus::InProgress(tid, once) => EntryStatus::InProgress(tid, once),
            EntryStatus::Complete(result) => EntryStatus::Complete(f(result)),
        }
    }

    pub fn as_ref(&self) -> EntryStatus<&T> {
        match self {
            EntryStatus::InProgress(tid, once) => EntryStatus::InProgress(*tid, once.clone()),
            EntryStatus::Complete(result) => EntryStatus::Complete(result),
        }
    }
}

impl<DB: Database> Context<DB>
where
    DB::Query: Queries<DB>,
{
    fn try_fetch_dash_map<Q: Query<DB> + Eq + Hash>(
        &self,
        query: Q,
        map: &FxDashMap<Q, EntryStatus<(Q::Result, Vec<DB::Query>)>>,
    ) -> EntryStatus<Q::Result> {
        let once = match map.entry(query.clone()) {
            dashmap::Entry::Occupied(occupied_entry) => {
                return occupied_entry
                    .get()
                    .as_ref()
                    .map(|(result, _)| result.clone());
            }
            dashmap::Entry::Vacant(vacant_entry) => {
                let once = Rc::new(Once::new());
                let tid = std::thread::current().id();
                vacant_entry.insert(EntryStatus::InProgress(tid, once.clone()));
                once
            }
        };

        let (result, dependencies) = self.rule(&query);
        map.insert(query, EntryStatus::Complete((result.clone(), dependencies)));
        let mut called = false;
        once.call_once(|| {
            called = true;
        });
        assert!(called);
        EntryStatus::Complete(result)
    }

    fn rule<Q: Query<DB>>(&self, query: &Q) -> (Q::Result, Vec<DB::Query>) {
        let injected_query = query.clone().inject();

        if self.query.stack.borrow().contains(&injected_query) {
            panic!("cyclic query detected: {:?}", self.query.stack.borrow());
        }

        let saved_dependencies = self.query.dependencies.take();

        self.query.stack.borrow_mut().push(injected_query);
        let result = Q::rule(self, query);
        self.query.stack.borrow_mut().pop();
        let query_dependencies = self.query.dependencies.replace(saved_dependencies);
        (result, query_dependencies)
    }

    fn wait(&self, other_tid: ThreadId, once: &Once) {
        while let Some(stealable) = self.stealable.borrow_mut().pop() {
            self.steal(stealable)
        }
        // TODO: We should wait for more stealable data _or_ the once.
        once.wait();
    }

    fn steal(&self, mut stealable: Stealable<DB::Query>) {
        std::mem::swap(self.query.stack.borrow_mut().as_mut(), &mut stealable.stack);
        match DB::Query::try_fetch(self, stealable.query) {
            EntryStatus::InProgress(..) => {}
            EntryStatus::Complete(()) => {}
        }
        std::mem::swap(self.query.stack.borrow_mut().as_mut(), &mut stealable.stack);
    }

    pub fn fetch<Q: Query<DB>>(&self, query: &Q) -> Q::Result {
        loop {
            match Q::try_fetch(self, query.clone()) {
                EntryStatus::InProgress(tid, once) => self.wait(tid, &once),
                EntryStatus::Complete(result) => return result,
            }
        }
    }
}
