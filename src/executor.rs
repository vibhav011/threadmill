use crate::task::*;
use crate::worker::*;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
// use priority_queue::PriorityQueue;
use std::collections::BinaryHeap;

pub(crate) type TaskQueue = BinaryHeap<QueuedTask>;

pub struct Executor {
    n_workers: u32,
    workers: Vec<Worker>,
    task_queue: Arc<(Mutex<TaskQueue>, Condvar)>,
    task_counter: AtomicUsize,
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
            task_counter: AtomicUsize::new(0),
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
        let (tx, rx) = mpsc::channel();
        let task = QueuedTask::new(
            self.task_counter.fetch_add(1, Ordering::SeqCst),
            cb,
            priority,
            tx,
        );
        let cloned_status = task.get_state().clone();
        let mut q = self.task_queue.0.lock().unwrap();
        task.get_state().set_status(TaskStatus::Queued);
        q.push(task);
        self.task_queue.1.notify_one();

        TaskHandle::new(cloned_status, rx)
    }

    pub fn stop_after_completion(&mut self) {
        for w in self.workers.iter_mut() {
            w.stop();
        }
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
