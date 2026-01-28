use btree_store::BTree;
use criterion::{Criterion, criterion_group, criterion_main};
use rand::Rng;
use std::sync::{Arc, Barrier};
use std::thread;
use tempfile::TempDir;

fn bench_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert");

    group.bench_function("insert_1k_tx", |b| {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("bench_insert.db");
        let btree = BTree::open(&db_path).unwrap();

        b.iter(|| {
            btree
                .exec("bench", |txn| {
                    for i in 0..1000 {
                        let k = format!("key_{}", i);
                        let v = format!("val_{}", i);
                        txn.put(k.as_bytes(), v.as_bytes())?;
                    }
                    Ok(())
                })
                .unwrap();
        });
    });

    group.finish();
}

fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("get");
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("bench_get.db");
    let btree = BTree::open(&db_path).unwrap();

    // Pre-fill 100k items
    let mut rng = rand::thread_rng();
    let keys: Vec<String> = (0..100_000).map(|i| format!("key_{:06}", i)).collect();

    // Batch insert to speed up setup
    for chunk in keys.chunks(10_000) {
        btree
            .exec("bench", |txn| {
                for k in chunk {
                    txn.put(k.as_bytes(), k.as_bytes())?;
                }
                Ok(())
            })
            .unwrap();
    }

    group.bench_function("random_get_100k", |b| {
        b.iter(|| {
            let k = &keys[rng.gen_range(0..100_000)];
            btree.view("bench", |txn| txn.get(k.as_bytes())).unwrap();
        });
    });

    group.finish();
}

fn bench_concurrent_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_get");
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("bench_con_get.db");
    let btree = Arc::new(BTree::open(&db_path).unwrap());

    // Pre-fill 100k items
    let keys: Vec<String> = (0..100_000).map(|i| format!("key_{:06}", i)).collect();
    let keys_arc = Arc::new(keys);

    for chunk in keys_arc.chunks(10_000) {
        btree
            .exec("bench", |txn| {
                for k in chunk {
                    txn.put(k.as_bytes(), k.as_bytes())?;
                }
                Ok(())
            })
            .unwrap();
    }

    // 4 threads concurrent reads
    group.bench_function("4_threads_random_get", |b| {
        b.iter_custom(|iters| {
            let mut threads = vec![];
            let start = std::time::Instant::now();
            let barrier = Arc::new(Barrier::new(4));

            for _ in 0..4 {
                let btree_clone = btree.clone();
                let keys_clone = keys_arc.clone();
                let barrier_clone = barrier.clone();

                threads.push(thread::spawn(move || {
                    let mut rng = rand::thread_rng();
                    barrier_clone.wait(); // Sync start
                    for _ in 0..(iters / 4) {
                        // Distribute load
                        let k = &keys_clone[rng.gen_range(0..100_000)];
                        btree_clone
                            .view("bench", |txn| {
                                txn.get(k.as_bytes()).unwrap();
                                Ok(())
                            })
                            .unwrap();
                    }
                }));
            }

            for t in threads {
                t.join().unwrap();
            }
            start.elapsed()
        });
    });

    group.finish();
}

fn bench_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("delete");

    group.bench_function("delete_insert_cycle_1k", |b| {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("bench_del.db");
        let btree = BTree::open(&db_path).unwrap();

        b.iter(|| {
            // Setup
            btree
                .exec("bench", |txn| {
                    for i in 0..1000 {
                        let k = format!("k{}", i);
                        txn.put(k.as_bytes(), b"val")?;
                    }
                    Ok(())
                })
                .unwrap();

            // Measure delete
            btree
                .exec("bench", |txn| {
                    for i in 0..1000 {
                        let k = format!("k{}", i);
                        txn.del(k.as_bytes())?;
                    }
                    Ok(())
                })
                .unwrap();
        });
    });

    group.finish();
}

fn bench_bucket_ops(c: &mut Criterion) {
    let mut group = c.benchmark_group("bucket_ops");

    group.bench_function("create_drop_empty_bucket", |b| {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("bench_buckets.db");
        let btree = BTree::open(&db_path).unwrap();
        let mut i = 0;

        b.iter(|| {
            i += 1;
            let bucket = format!("b_{}", i);
            // Create
            btree.exec(&bucket, |txn| txn.put(b"k", b"v")).unwrap();

            // Drop
            btree.del_bucket(&bucket).unwrap();
        });
    });

    group.sample_size(10); // Reduce sample size for heavy operation
    group.bench_function("drop_large_bucket_100k", |b| {
        b.iter_custom(|iters| {
            let mut total_duration = std::time::Duration::new(0, 0);

            for _ in 0..iters {
                let temp_dir = TempDir::new().unwrap();
                let db_path = temp_dir.path().join("large_bucket.db");
                let btree = BTree::open(&db_path).unwrap();

                // Setup: fill 100k items
                // Split into chunks to avoid giant memory usage during transaction
                for i in 0..10 {
                    btree
                        .exec("heavy", |txn| {
                            for j in 0..10_000 {
                                let k = format!("k_{:06}", i * 10_000 + j);
                                txn.put(k.as_bytes(), b"val")?;
                            }
                            Ok(())
                        })
                        .unwrap();
                }

                let start = std::time::Instant::now();
                btree.del_bucket("heavy").unwrap();
                total_duration += start.elapsed();
            }

            total_duration
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_insert,
    bench_get,
    bench_concurrent_get,
    bench_delete,
    bench_bucket_ops
);
criterion_main!(benches);
