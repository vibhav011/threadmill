use std::any::Any;
use std::cmp::Ordering;
use std::panic::{self, AssertUnwindSafe};
use std::sync::{Arc, Condvar, Mutex};

pub(crate) type CbResult<R> = Result<R, Box<dyn Any + Send>>;
// pub(crate) trait TaskTrait {
//     fn execute(&mut self) -> Result<(), Box<dyn std::error::Error>>;
//     fn get_status(&self) -> TaskStatus;
//     fn set_status(&mut self, status: TaskStatus);
// }

#[derive(Clone, Copy)]
pub enum TaskStatus {
    Created,
    Queued,
    Running,
    Finished,
}

// State shared with the user
pub(crate) struct TaskSharedState {
    status: Mutex<TaskStatus>,
}

impl TaskSharedState {
    pub fn new(status: TaskStatus) -> Self {
        Self {
            status: Mutex::new(status),
        }
    }

    pub fn default() -> Self {
        Self::new(TaskStatus::Created)
    }

    pub fn set_status(&self, status: TaskStatus) {
        *self.status.lock().unwrap() = status;
    }

    pub fn get_status(&self) -> TaskStatus {
        *self.status.lock().unwrap()
    }
}

pub(crate) trait QueuedTask {
    fn priority(&self) -> i64;
    fn get_state(&self) -> &Arc<TaskSharedState>;
    fn execute(&mut self);
    fn dyn_cmp(&self, other: &dyn QueuedTask) -> Ordering {
        self.priority().cmp(&other.priority())
    }
}
pub(crate) struct SingleTask<F, R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    id: u32,
    callback: Option<F>,
    callback_res: Arc<(Mutex<Option<CbResult<R>>>, Condvar)>,
    priority: i32,
    state: Arc<TaskSharedState>,
}

pub struct TaskHandle<R: Send + 'static> {
    state: Arc<TaskSharedState>,
    result: Arc<(Mutex<Option<CbResult<R>>>, Condvar)>,
}

unsafe impl<F, R> Send for SingleTask<F, R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
}

impl<F, R> SingleTask<F, R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    pub fn new(
        id: u32,
        cb: F,
        res: Arc<(Mutex<Option<CbResult<R>>>, Condvar)>,
        priority: i32,
    ) -> Self {
        Self {
            id: id,
            callback: Some(cb),
            callback_res: res,
            priority: priority,
            state: Arc::new(TaskSharedState::default()),
        }
    }
}

impl<F, R> QueuedTask for SingleTask<F, R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    fn priority(&self) -> i64 {
        (self.priority as i64) << 32 | self.id as i64
    }

    fn get_state(&self) -> &Arc<TaskSharedState> {
        &self.state
    }

    fn execute(&mut self) {
        if let Some(cb) = self.callback.take() {
            let mut r = self.callback_res.0.lock().unwrap();
            *r = Some(panic::catch_unwind(AssertUnwindSafe(|| cb())));
            self.callback_res.1.notify_all();
        }
    }
}

impl<R: Send + 'static> TaskHandle<R> {
    pub(crate) fn new(
        state: Arc<TaskSharedState>,
        res: Arc<(Mutex<Option<CbResult<R>>>, Condvar)>,
    ) -> Self {
        Self {
            state: state,
            result: res,
        }
    }

    pub fn get_result(&self) -> CbResult<R> {
        let mut r = self.result.0.lock().unwrap();
        while r.is_none() {
            r = self.result.1.wait(r).unwrap();
        }
        r.take().unwrap()
    }

    pub fn get_status(&self) -> TaskStatus {
        self.state.get_status()
    }
}

impl PartialEq for Box<dyn QueuedTask + Send> {
    fn eq(&self, other: &Self) -> bool {
        self.priority() == other.priority()
    }
}
impl Eq for Box<dyn QueuedTask + Send> {}
impl PartialOrd for Box<dyn QueuedTask + Send> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Box<dyn QueuedTask + Send> {
    fn cmp(&self, other: &Self) -> Ordering {
        (**self).dyn_cmp(&**other)
    }
}
