use crate::WorkerId;
use bitvec::vec::BitVec;
use std::sync::Mutex;

pub(crate) struct DeadlockDetector {
    state: Mutex<State>,
}

struct State {
    waits_for: BitVec,
    todo: BitVec,
    not_visited: BitVec,
}

impl DeadlockDetector {
    pub fn new(num_workers: usize) -> Self {
        Self {
            state: Mutex::new(State {
                waits_for: BitVec::repeat(false, num_workers * num_workers),
                todo: BitVec::repeat(false, num_workers),
                not_visited: BitVec::repeat(false, num_workers),
            }),
        }
    }

    pub fn add_wait(&self, me: WorkerId, target: WorkerId) -> Result<(), ()> {
        if me == target {
            return Err(());
        }
        let mut guard = self.state.lock().unwrap();
        let state = &mut *guard;
        state.todo.fill(false);
        state.not_visited.fill(true);
        let num_workers = state.num_workers();

        state.todo.set(target.0, true);

        while let Some(current) = state.todo.first_one() {
            if current == me.0 {
                // Cycle detected.
                return Err(());
            }

            state.todo.set(current, false);
            state.not_visited.set(current, false);

            let neighbors = state
                .waits_for
                .get(current * num_workers..(current + 1) * num_workers)
                .unwrap();

            state.todo |= neighbors;
            state.todo &= &state.not_visited;
        }

        state.waits_for.set(me.0 * num_workers + target.0, true);
        Ok(())
    }

    pub fn remove_wait(&self, me: WorkerId, target: WorkerId) {
        let mut state = self.state.lock().unwrap();
        let num_workers = state.num_workers();
        state.waits_for.set(me.0 * num_workers + target.0, false);
    }
}

impl State {
    fn num_workers(&self) -> usize {
        self.not_visited.len()
    }
}

pub(crate) struct WaitGuard<'a> {
    pub detector: &'a DeadlockDetector,
    pub me: WorkerId,
    pub other: WorkerId,
}

impl<'a> Drop for WaitGuard<'a> {
    fn drop(&mut self) {
        self.detector.remove_wait(self.me, self.other);
    }
}
