use crate::executor::TaskQueue;
use std::{
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
};

use crate::task::*;

pub(crate) struct Worker {
    task: Option<QueuedTask>,
    queue: Arc<(Mutex<TaskQueue>, Condvar)>,
    is_running: Arc<AtomicBool>,
    thread_handle: Option<JoinHandle<()>>,
}

impl Worker {
    pub(crate) fn new(queue: Arc<(Mutex<TaskQueue>, Condvar)>) -> Self {
        Self {
            task: None,
            queue: queue,
            is_running: Arc::new(AtomicBool::new(false)),
            thread_handle: None,
        }
    }

    pub(crate) fn start(&mut self) {
        if self.is_running.load(Ordering::Acquire) {
            return;
        }
        self.is_running.store(true, Ordering::Relaxed);

        let queue = self.queue.clone();
        let is_r = self.is_running.clone();
        self.thread_handle = Some(std::thread::spawn(move || {
            'outer: loop {
                let mut q = queue.0.lock().unwrap();
                while q.len() == 0 {
                    if !is_r.load(Ordering::Acquire) {
                        break 'outer;
                    }
                    q = queue.1.wait(q).unwrap();
                }
                let Some(mut task) = q.pop() else {
                    continue;
                };
                drop(q);

                task.get_state().set_status(TaskStatus::Running);
                task.execute();
                task.get_state().set_status(TaskStatus::Finished);
            }
        }));
    }

    pub(crate) fn stop(&mut self) {
        self.is_running.store(false, Ordering::Relaxed);
    }

    pub(crate) fn join(&mut self) {
        if let Some(h) = self.thread_handle.take() {
            let _ = h.join();
        }
    }
}
