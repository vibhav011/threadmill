use std::sync::mpsc;
use std::{thread, time};
use threadmill::executor::Executor;

#[test]
fn executor_rejects_zero_workers() {
    match Executor::new(0) {
        Err(err) => assert!(err.to_string().contains("Number of threads cannot be zero")),
        Ok(_) => panic!("expected executor creation with zero workers to fail"),
    }
}

#[test]
fn queue_task_with_priority_executes_in_priority_order() {
    let mut executor = Executor::new(1).unwrap();
    let (tx, rx) = mpsc::channel();
    let tx1 = tx.clone();
    let tx2 = tx.clone();

    // This first task keeps the one worker thread busy while
    // others can be queued and reordered by priority
    executor.queue_task(|| thread::sleep(time::Duration::from_millis(300)));

    // Queue tasks with custom priority
    executor.queue_task_with_priority(move || tx1.send(1).unwrap(), 1);
    executor.queue_task_with_priority(move || tx2.send(3).unwrap(), 3);
    executor.queue_task_with_priority(move || tx.send(2).unwrap(), 2);

    executor.stop_after_completion();
    executor.join();

    let mut observed = Vec::new();
    for _ in 0..3 {
        observed.push(rx.recv().unwrap());
    }

    assert_eq!(observed, vec![3, 2, 1]);
}

#[test]
fn executor_with_no_tasks() {
    let mut executor = Executor::new(4).unwrap();
    executor.stop_after_completion();
    executor.join();
}

#[test]
fn executor_with_one_task() {
    let mut executor = Executor::new(4).unwrap();
    executor.queue_task(|| 42);
    executor.stop_after_completion();
    executor.join();
}

const MOD: u64 = 10000007;

fn fact(n: u32) -> u32 {
    if n <= 1 {
        return 1;
    }
    (((n as u64) * (fact(n - 1) as u64)) % MOD) as u32
}

#[test]
fn executor_runs_all_tasks() {
    let mut task_handles = vec![];
    let mut executor = Executor::new(4).unwrap();
    for i in 0..10000 {
        let value = i;
        let t = executor.queue_task(move || -> u32 { fact(value as u32) });
        task_handles.push(t);
    }
    executor.stop_after_completion();
    executor.join();

    let mut ans = 0;
    for t in task_handles {
        let rx = t.get_receiver();
        while let Ok(num) = rx.recv() {
            ans = (ans + num.unwrap()) % MOD as u32;
        }
    }

    assert_eq!(ans, 1224702);
}

#[test]
fn executor_with_panicking_task() {
    let mut executor = Executor::new(4).unwrap();
    let h = executor.queue_task(|| panic!("Oops!"));
    let res = h.get_receiver().recv().unwrap();
    match res {
        Ok(_) => assert!(false),
        Err(e) => {
            println!("{e:?}")
        }
    }
}
