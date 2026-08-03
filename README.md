# threadmill

threadmill is a small Rust library for creating a fixed-size thread pool and scheduling work across a set of worker threads. It lets you queue tasks either with the default priority or with an explicit priority value so higher-priority work is processed first.

## Features

- Create an executor with a fixed number of worker threads
- Queue tasks with `queue_task`
- Queue prioritized tasks with `queue_task_with_priority`
- Stop the executor after the current work is drained and join the worker threads

## Quick start

Add the crate to your project dependencies and use the executor in your application:

```rust
use threadmill::executor::Executor;

fn main() {
    let mut executor = Executor::new(4).unwrap();

    executor.queue_task(|| {
        println!("Running a normal task");
    });

    executor.queue_task_with_priority(|| {
        println!("Running a high-priority task");
    }, 10);

    executor.stop_after_completion();
    executor.join();
}
```

Getting results from the tasks:

```rust
use threadmill::executor::Executor;

fn main() {
    let mut executor = Executor::new(4).unwrap();

    let handle = executor.queue_task(|| {
        42
    });

    executor.stop_after_completion();
    executor.join();

    let rx = handle.get_receiver();
    if let Ok(num) = rx.recv() {
        assert_eq!(num.unwrap(), 42);
    }
}
```

## API overview

### Executor

The `Executor` is the main entry point for the library.

- `Executor::new(n_workers)` creates a new executor with `n_workers` worker threads.
- `queue_task(cb)` queues a task with the default priority of `0`. Returns a `TaskHandle`.
- `queue_task_with_priority(cb, priority)` queues a task with a custom priority. This priority can be negative or positive, with larger values indicating higher priority. Returns a `TaskHandle`.
- `stop_after_completion()` tells workers to stop once the queued work has been processed.
- `join()` waits for all worker threads to finish.
- Dropping the executor will stop the executor and join the worker threads if they are still running. Any queued tasks that have not been processed will be dropped, but the currently running tasks will be allowed to finish.

### TaskHandle

The `TaskHandle` is returned when queuing tasks and can be used to retrieve the result of the task once it has completed, or query its status.

- `get_receiver()` returns a `Receiver` that can be used to receive the result of the task once it has completed. If the task panicked, the result will be an `Err` variant containing the panic information.
- `get_status()` returns the current status of the task, which can be `Created`, `Queued`, `Running`, or `Finished`.

## Notes

- The executor uses a priority queue internally, so tasks with larger priority values are preferred.
- This crate is intentionally lightweight and focused on the core behavior of a fixed worker pool with queued execution.
