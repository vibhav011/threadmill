use crate::task::*;
use crate::worker::*;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Condvar, Mutex};
// use priority_queue::PriorityQueue;
use std::collections::BinaryHeap;

pub(crate) type TaskQueue = BinaryHeap<Box<dyn QueuedTask + Send>>;

pub struct Executor {
    n_workers: u32,
    workers: Vec<Worker>,
    task_queue: Arc<(Mutex<TaskQueue>, Condvar)>,
    task_counter: AtomicU32,
}

impl Executor {
    pub fn new(n_workers: u32) -> Result<Self, Box<dyn std::error::Error>> {
        if n_workers == 0 {
            return Err("Number of threads cannot be zero".into());
        }
        let queue = Arc::new((Mutex::new(TaskQueue::new()), Condvar::new()));

        let mut workers = Vec::with_capacity(n_workers as usize);
        for _ in 0..n_workers {
            let mut worker = Worker::new(queue.clone());
            worker.start();
            workers.push(worker);
        }

        Ok(Self {
            n_workers: n_workers,
            workers: workers,
            task_queue: queue,
            task_counter: AtomicU32::new(0),
        })
    }

    pub fn queue_task<F: FnOnce() -> R + Send + 'static, R: Send + 'static>(
        &mut self,
        cb: F,
    ) -> TaskHandle<R> {
        self.queue_task_with_priority(cb, 0)
    }

    pub fn queue_task_with_priority<F: FnOnce() -> R + Send + 'static, R: Send + 'static>(
        &mut self,
        cb: F,
        priority: i32,
    ) -> TaskHandle<R> {
        let res = Arc::new((Mutex::new(None), Condvar::new()));
        let task = SingleTask::new(
            self.task_counter.fetch_add(1, Ordering::SeqCst),
            cb,
            res.clone(),
            priority,
        );
        let cloned_state = task.get_state().clone();
        let mut q = self.task_queue.0.lock().unwrap();
        task.get_state().set_status(TaskStatus::Queued);
        q.push(Box::new(task));
        self.task_queue.1.notify_one();

        TaskHandle::new(cloned_state, res.clone())
    }

    pub fn stop_after_completion(&mut self) {
        for w in self.workers.iter_mut() {
            w.stop();
        }
        let _q = self.task_queue.0.lock().unwrap();
        self.task_queue.1.notify_all();
    }

    pub fn join(&mut self) {
        for w in self.workers.iter_mut() {
            w.join();
        }
    }
}

impl Drop for Executor {
    // Dropping the Executor clears the task queue and stops the workers
    // Currently running tasks will still be finished
    fn drop(&mut self) {
        self.task_queue.0.lock().unwrap().clear();
        self.stop_after_completion();
    }
}
