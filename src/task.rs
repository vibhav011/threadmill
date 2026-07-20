use std::any::Any;
use std::panic::{self, AssertUnwindSafe};
use std::sync::{Arc, Mutex, mpsc};

pub(crate) type ChannelMsg<R> = Result<R, Box<dyn Any + Send>>;
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

pub(crate) struct QueuedTask {
    id: usize,
    callback: Option<Box<dyn FnOnce()>>,
    priority: i32,
    state: Arc<TaskSharedState>,
}

pub struct TaskHandle<R: Send + 'static> {
    state: Arc<TaskSharedState>,
    rx: mpsc::Receiver<ChannelMsg<R>>,
}

unsafe impl Send for QueuedTask {}

impl QueuedTask {
    pub fn new<F: FnOnce() -> R + Send + 'static, R: Send + 'static>(
        id: usize,
        cb: F,
        priority: i32,
        tx: mpsc::Sender<ChannelMsg<R>>,
    ) -> Self {
        let erased_cb = move || {
            let _ = tx.send(panic::catch_unwind(AssertUnwindSafe(|| cb())));
        };
        Self {
            id: id,
            callback: Some(Box::new(erased_cb)),
            priority: priority,
            state: Arc::new(TaskSharedState::default()),
        }
    }

    pub fn get_state(&self) -> &Arc<TaskSharedState> {
        &self.state
    }

    pub(crate) fn execute(&mut self) {
        if let Some(cb) = self.callback.take() {
            cb();
        }
    }
}

impl<R: Send + 'static> TaskHandle<R> {
    pub(crate) fn new(state: Arc<TaskSharedState>, rx: mpsc::Receiver<ChannelMsg<R>>) -> Self {
        Self {
            state: state,
            rx: rx,
        }
    }
}

impl PartialEq for QueuedTask {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for QueuedTask {}

impl Ord for QueuedTask {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.priority
            .cmp(&other.priority)
            .then_with(|| self.id.cmp(&other.id).reverse())
    }
}

impl PartialOrd for QueuedTask {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
