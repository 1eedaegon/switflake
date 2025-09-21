use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rayon::prelude::*;
use std::hint::black_box;
use std::sync::Arc;
use std::thread;
use switflake::*;

fn bench_single_thread_generation(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_thread");

    // Realtime
    let sf_realtime = Switflake::new(
        Settings::new()
            .with_machine_id(1)
            .with_mode(GeneratorMode::Realtime),
    )
    .unwrap();

    group.bench_function("realtime", |b| {
        b.iter(|| black_box(sf_realtime.next_id().unwrap()));
    });

    // Bucket
    for size in [100, 1000, 10000, 100000].iter() {
        let sf_bucket = Switflake::new(
            Settings::new()
                .with_machine_id(2)
                .with_mode(GeneratorMode::Bucket)
                .with_bucket_size(*size),
        )
        .unwrap();

        group.bench_with_input(BenchmarkId::new("bucket", size), size, |b, _| {
            b.iter(|| black_box(sf_bucket.next_id().unwrap()));
        });
    }

    // Lazy
    let sf_lazy = Switflake::new(
        Settings::new()
            .with_machine_id(3)
            .with_mode(GeneratorMode::Lazy),
    )
    .unwrap();

    group.bench_function("lazy", |b| {
        b.iter(|| black_box(sf_lazy.next_id().unwrap()));
    });

    group.finish();
}

fn bench_multi_thread_generation(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi_thread");

    for num_threads in [2, 4, 8, 16].iter() {
        let sf_realtime = Arc::new(
            Switflake::new(
                Settings::new()
                    .with_machine_id(10)
                    .with_mode(GeneratorMode::Realtime),
            )
            .unwrap(),
        );

        group.bench_with_input(
            BenchmarkId::new("realtime", num_threads),
            num_threads,
            |b, &num| {
                b.iter(|| {
                    let mut handles = vec![];
                    for _ in 0..num {
                        let sf_clone = sf_realtime.clone();
                        handles.push(thread::spawn(move || {
                            for _ in 0..100 {
                                black_box(sf_clone.next_id().unwrap());
                            }
                        }));
                    }
                    for handle in handles {
                        handle.join().unwrap();
                    }
                });
            },
        );

        let sf_bucket = Arc::new(
            Switflake::new(
                Settings::new()
                    .with_machine_id(11)
                    .with_mode(GeneratorMode::Bucket)
                    .with_bucket_size(10000),
            )
            .unwrap(),
        );

        group.bench_with_input(
            BenchmarkId::new("bucket", num_threads),
            num_threads,
            |b, &num| {
                b.iter(|| {
                    let mut handles = vec![];
                    for _ in 0..num {
                        let sf_clone = sf_bucket.clone();
                        handles.push(thread::spawn(move || {
                            for _ in 0..100 {
                                black_box(sf_clone.next_id().unwrap());
                            }
                        }));
                    }
                    for handle in handles {
                        handle.join().unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}

fn bench_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("throughput");
    group.throughput(Throughput::Elements(10000));

    let sf_realtime = Switflake::new(
        Settings::new()
            .with_machine_id(20)
            .with_mode(GeneratorMode::Realtime),
    )
    .unwrap();

    group.bench_function("realtime_10k", |b| {
        b.iter(|| {
            for _ in 0..10000 {
                black_box(sf_realtime.next_id().unwrap());
            }
        });
    });

    let sf_bucket = Switflake::new(
        Settings::new()
            .with_machine_id(21)
            .with_mode(GeneratorMode::Bucket)
            .with_bucket_size(100000),
    )
    .unwrap();

    group.bench_function("bucket_10k", |b| {
        b.iter(|| {
            for _ in 0..10000 {
                black_box(sf_bucket.next_id().unwrap());
            }
        });
    });

    group.finish();
}

// TOOD: contention condition을 지정해서 경계값을 좀 빡세게
fn bench_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("contention");

    let sf = Arc::new(
        Switflake::new(
            Settings::new()
                .with_machine_id(30)
                .with_mode(GeneratorMode::Realtime),
        )
        .unwrap(),
    );

    group.bench_function("high_contention", |b| {
        b.iter(|| {
            (0..100).into_par_iter().for_each(|_| {
                black_box(sf.next_id().unwrap());
            });
        });
    });

    group.finish();
}

fn bench_decompose(c: &mut Criterion) {
    c.bench_function("decompose", |b| {
        let id = 0x1234567890ABCDEF_u64;
        b.iter(|| black_box(Switflake::decompose(id)));
    });
}

fn bench_memory_efficiency(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory");

    // 메모리 효율성 비교
    group.bench_function("create_realtime", |b| {
        b.iter(|| {
            black_box(
                Switflake::new(
                    Settings::new()
                        .with_machine_id(40)
                        .with_mode(GeneratorMode::Realtime),
                )
                .unwrap(),
            )
        });
    });

    group.bench_function("create_bucket_small", |b| {
        b.iter(|| {
            black_box(
                Switflake::new(
                    Settings::new()
                        .with_machine_id(41)
                        .with_mode(GeneratorMode::Bucket)
                        .with_bucket_size(100),
                )
                .unwrap(),
            )
        });
    });

    group.bench_function("create_bucket_large", |b| {
        b.iter(|| {
            black_box(
                Switflake::new(
                    Settings::new()
                        .with_machine_id(42)
                        .with_mode(GeneratorMode::Bucket)
                        .with_bucket_size(100000),
                )
                .unwrap(),
            )
        });
    });

    group.finish();
}

// TODO: 버스터블 매치값을 조절
fn bench_burst_generation(c: &mut Criterion) {
    let mut group = c.benchmark_group("burst");

    let sf_realtime = Switflake::new(
        Settings::new()
            .with_machine_id(50)
            .with_mode(GeneratorMode::Realtime),
    )
    .unwrap();

    let sf_bucket = Switflake::new(
        Settings::new()
            .with_machine_id(51)
            .with_mode(GeneratorMode::Bucket)
            .with_bucket_size(10000),
    )
    .unwrap();

    group.bench_function("realtime_burst_1000", |b| {
        b.iter(|| {
            let mut ids = Vec::with_capacity(1000);
            for _ in 0..1000 {
                ids.push(sf_realtime.next_id().unwrap());
            }
            black_box(ids)
        });
    });

    group.bench_function("bucket_burst_1000", |b| {
        b.iter(|| {
            let mut ids = Vec::with_capacity(1000);
            for _ in 0..1000 {
                ids.push(sf_bucket.next_id().unwrap());
            }
            black_box(ids)
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_single_thread_generation,
    bench_multi_thread_generation,
    bench_throughput,
    bench_contention,
    bench_decompose,
    bench_memory_efficiency,
    bench_burst_generation
);

criterion_main!(benches);
