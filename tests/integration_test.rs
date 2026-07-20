use std::{thread, time};
use threadmill::executor::Executor;

#[test]
fn basic_executor_test() {
    let mut executor = Executor::new(8).unwrap();
    let n = 10;
    for i in 0..n {
        let idx = i;
        executor.queue_task(move || {
            println!("Task {idx} started");
            thread::sleep(time::Duration::from_secs(n - idx));
            println!("Task {idx} done");
        });
    }
    executor.stop_after_completion();
    executor.join();
    assert!(true);
}
