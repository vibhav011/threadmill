use crate::executor::TaskQueue;
use std::{
    sync::{Arc, Condvar, Mutex},
    thread::JoinHandle,
};

use crate::task::*;

#[derive(Clone, Copy)]
enum WorkerStatus {
    Idle,
    Running,
}

pub(crate) struct Worker {
    task: Option<QueuedTask>,
    status: WorkerStatus,
    queue: Arc<(Mutex<TaskQueue>, Condvar)>,
    is_running: Arc<Mutex<bool>>,
    thread_handle: Option<JoinHandle<()>>,
}

impl Worker {
    pub(crate) fn new(queue: Arc<(Mutex<TaskQueue>, Condvar)>) -> Self {
        Self {
            task: None,
            status: WorkerStatus::Idle,
            queue: queue,
            is_running: Arc::new(Mutex::new(false)),
            thread_handle: None,
        }
    }

    pub(crate) fn start(&mut self) {
        let mut is_running = self.is_running.lock().unwrap();
        if *is_running {
            return;
        }
        *is_running = true;
        drop(is_running);

        let queue = self.queue.clone();
        let is_r = self.is_running.clone();
        self.thread_handle = Some(std::thread::spawn(move || {
            'outer: loop {
                let mut q = queue.0.lock().unwrap();
                while q.len() == 0 {
                    if !*is_r.lock().unwrap() {
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
        *self.is_running.lock().unwrap() = false;
    }

    pub(crate) fn join(&mut self) {
        if let Some(h) = self.thread_handle.take() {
            let _ = h.join();
        }
    }
}
