use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use threadmill::executor::Executor;

fn bench_queueing(c: &mut Criterion) {
    let mut group = c.benchmark_group("executor_queueing");

    for size in [10usize, 100usize, 1000usize] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter(|| {
                let mut executor = Executor::new(2).unwrap();
                for i in 0..size {
                    let value = i;
                    executor.queue_task(move || {
                        let _ = value;
                    });
                }
                executor.stop_after_completion();
                executor.join();
            });
        });
    }

    group.finish();
}

fn bench_priority_queueing(c: &mut Criterion) {
    let mut group = c.benchmark_group("executor_priority_queueing");

    for size in [10usize, 100usize, 1000usize] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter(|| {
                let mut executor = Executor::new(4).unwrap();
                for i in 0..size {
                    let value = i % 5;
                    executor.queue_task_with_priority(
                        move || {
                            let _ = value;
                        },
                        value as i32,
                    );
                }
                executor.stop_after_completion();
                executor.join();
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_queueing, bench_priority_queueing);
criterion_main!(benches);
