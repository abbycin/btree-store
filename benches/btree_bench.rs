use btree_store::BTree;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
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
        btree.new_bucket("bench", false).unwrap();

        b.iter(|| {
            for i in 0..1000 {
                let k = format!("key_{}", i);
                let v = format!("val_{}", i);
                btree
                    .exec("bench", |txn| txn.put(k.as_bytes(), v.as_bytes()))
                    .unwrap();
            }
        });
    });

    group.finish();
}

/// Same-workload harness for plain vs prefix-encoded buckets. `workload` runs
/// the identical operations against a `plain` (flags=0) and a `prefix`
/// (flags=1) bucket; keys are whatever the caller passes, so no specially
/// shaped prefix key set is constructed. Bucket creation is in the untimed
/// batch setup; any `setup_fill` work inside a workload is part of the timed
/// measurement. Criterion records each layout as a separate measurement.
fn bench_plain_vs_prefix(
    c: &mut Criterion,
    name: &str,
    keys: &[Vec<u8>],
    values: &[Vec<u8>],
    workload: impl Fn(&BTree, &str, &[Vec<u8>], &[Vec<u8>]) + Copy,
) {
    let mut group = c.benchmark_group("prefix_encoding");
    group.sample_size(10);

    for (bucket, encoded, label) in [("plain", false, "plain"), ("prefix", true, "prefix")] {
        group.bench_function(format!("{name}/{label}"), |b| {
            b.iter_batched(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    let db_path = temp_dir.path().join(format!("{name}-{label}.db"));
                    let tree = BTree::open(&db_path).unwrap();
                    tree.new_bucket(bucket, encoded).unwrap();
                    (temp_dir, tree)
                },
                |(_temp_dir, tree)| workload(&tree, bucket, keys, values),
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

/// Batch-loads `keys`/`values` into `bucket` in chunks; callers invoke this from
/// the timed workload when setup is part of the measured operation.
fn setup_fill(tree: &BTree, bucket: &str, keys: &[Vec<u8>], values: &[Vec<u8>], chunk: usize) {
    for (ck, cv) in keys.chunks(chunk).zip(values.chunks(chunk)) {
        tree.exec(bucket, |txn| {
            for (k, v) in ck.iter().zip(cv.iter()) {
                txn.put(k, v)?;
            }
            Ok(())
        })
        .unwrap();
    }
}

/// The batch-insert workload: every key inserted in one transaction per chunk.
fn workload_insert(tree: &BTree, bucket: &str, keys: &[Vec<u8>], values: &[Vec<u8>]) {
    setup_fill(tree, bucket, keys, values, 200);
}

/// Point-get workload: after a batch setup, one transaction reads every key in
/// input order.
fn workload_point_get(tree: &BTree, bucket: &str, keys: &[Vec<u8>], values: &[Vec<u8>]) {
    setup_fill(tree, bucket, keys, values, 200);
    tree.view(bucket, |txn| {
        for key in keys {
            assert!(txn.get(key).is_ok());
        }
        Ok(())
    })
    .unwrap();
}

/// Update workload: after a batch setup, every key is updated in place.
fn workload_update(tree: &BTree, bucket: &str, keys: &[Vec<u8>], values: &[Vec<u8>]) {
    setup_fill(tree, bucket, keys, values, 200);
    for (ck, cv) in keys.chunks(200).zip(values.chunks(200)) {
        tree.exec(bucket, |txn| {
            for (k, v) in ck.iter().zip(cv.iter()) {
                assert!(txn.update(k, v)?);
            }
            Ok(())
        })
        .unwrap();
    }
}

/// Delete workload: after a batch setup, every key is deleted.
fn workload_delete(tree: &BTree, bucket: &str, keys: &[Vec<u8>], values: &[Vec<u8>]) {
    setup_fill(tree, bucket, keys, values, 200);
    for ck in keys.chunks(200) {
        tree.exec(bucket, |txn| {
            for k in ck {
                txn.del(k)?;
            }
            Ok(())
        })
        .unwrap();
    }
}

/// Iterate workload: after a batch setup, a full scan counts every entry.
fn workload_iterate(tree: &BTree, bucket: &str, keys: &[Vec<u8>], values: &[Vec<u8>]) {
    setup_fill(tree, bucket, keys, values, 200);
    tree.view(bucket, |txn| {
        let mut iter = txn.iter();
        let mut key_buf = Vec::new();
        let mut val_buf = Vec::new();
        let mut count = 0usize;
        while iter.next_ref(&mut key_buf, &mut val_buf) {
            count += 1;
        }
        assert_eq!(count, keys.len());
        Ok(())
    })
    .unwrap();
}

/// Mixed exec_multi workload: a cycle of put/update/get/del over a small key
/// set inside exec_multi commits, mirroring the existing exec_multi bench. The
/// del in a cycle may target a key whose value was never put (get/del of a
/// missing key are treated as no-ops), so every op is mapped to `Ok`.
fn workload_mixed(tree: &BTree, bucket: &str, keys: &[Vec<u8>], _values: &[Vec<u8>]) {
    for _ in 0..20 {
        tree.exec_multi(|multi| {
            for j in 0..100 {
                let key = &keys[j % keys.len()];
                // A get/del in the cycle can target a key that was never put
                // (or was already deleted), which is a legitimate outcome for
                // both bucket kinds. Swallow KeyNotFound so the same operation
                // sequence runs against the plain and prefix buckets.
                match j % 4 {
                    0 => {
                        multi.exec(bucket, |txn| {
                            txn.put(key, b"value")?;
                            Ok(())
                        })?;
                    }
                    1 => {
                        multi.exec(bucket, |txn| {
                            txn.update(key, b"value")?;
                            Ok(())
                        })?;
                    }
                    2 => {
                        multi.exec(bucket, |txn| match txn.get(key) {
                            Ok(value) => {
                                std::hint::black_box(value);
                                Ok(())
                            }
                            Err(btree_store::Error::KeyNotFound) => Ok(()),
                            Err(err) => Err(err),
                        })?;
                    }
                    3 => {
                        multi.exec(bucket, |txn| match txn.del(key) {
                            Ok(()) => Ok(()),
                            Err(btree_store::Error::KeyNotFound) => Ok(()),
                            Err(err) => Err(err),
                        })?;
                    }
                    _ => unreachable!(),
                }
            }
            Ok(())
        })
        .unwrap();
    }
}

/// Runs the six workloads against both bucket kinds. Criterion records each
/// `prefix_encoding/{workload}/{plain,prefix}` measurement separately; the
/// README presents the two layout measurements side by side.
fn bench_prefix_encoding_compare(c: &mut Criterion) {
    const N: u32 = 2000;

    let keys: Vec<Vec<u8>> = (0..N).map(|i| format!("key_{i:06}").into_bytes()).collect();
    let values: Vec<Vec<u8>> = keys.iter().map(|k| vec![k[0] % 251; 64]).collect();

    bench_plain_vs_prefix(c, "insert", &keys, &values, workload_insert);
    bench_plain_vs_prefix(c, "point_get", &keys, &values, workload_point_get);
    bench_plain_vs_prefix(c, "update", &keys, &values, workload_update);
    bench_plain_vs_prefix(c, "delete", &keys, &values, workload_delete);
    bench_plain_vs_prefix(c, "iterate", &keys, &values, workload_iterate);
    bench_plain_vs_prefix(c, "mixed", &keys, &values, workload_mixed);
}

fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("get");
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("bench_get.db");
    let btree = BTree::open(&db_path).unwrap();
    btree.new_bucket("bench", false).unwrap();

    let mut rng = rand::rng();
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
            let k = &keys[rng.random_range(0..100_000)];
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
    btree.new_bucket("bench", false).unwrap();

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
                    let mut rng = rand::rng();
                    barrier_clone.wait(); // Sync start
                    for _ in 0..(iters / 4) {
                        // Distribute load
                        let k = &keys_clone[rng.random_range(0..100_000)];
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
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                let db_path = temp_dir.path().join("bench_del.db");
                let btree = BTree::open(&db_path).unwrap();
                btree.new_bucket("bench", false).unwrap();

                btree
                    .exec("bench", |txn| {
                        for i in 0..1000 {
                            let k = format!("k{}", i);
                            txn.put(k.as_bytes(), b"val")?;
                        }
                        Ok(())
                    })
                    .unwrap();

                (temp_dir, btree)
            },
            |(_temp_dir, btree)| {
                for i in 0..1000 {
                    let k = format!("k{}", i);
                    btree.exec("bench", |txn| txn.del(k.as_bytes())).unwrap();
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_exec_multi(c: &mut Criterion) {
    let mut group = c.benchmark_group("exec_multi");
    group.sample_size(10);

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("bench_exec_multi.db");
    let btree = BTree::open(&db_path).unwrap();
    btree.new_bucket("bench", false).unwrap();
    let cycle_keys: Vec<Vec<u8>> = (0..250)
        .map(|index| format!("cycle-key-{index:04}").into_bytes())
        .collect();

    group.bench_function("mixed_1k_exec_multi_1k", |b| {
        b.iter(|| {
            // Each Criterion iteration executes 1,000 outer exec_multi calls.
            for _ in 0..1_000 {
                btree
                    .exec_multi(|multi| {
                        for index in 0..1_000 {
                            let key = &cycle_keys[index / 4];
                            match index % 4 {
                                0 => {
                                    multi.exec("bench", |txn| txn.put(key, b"value"))?;
                                }
                                1 => {
                                    multi.exec("bench", |txn| {
                                        assert!(txn.update(key, b"value")?);
                                        Ok(())
                                    })?;
                                }
                                2 => {
                                    multi.exec("bench", |txn| {
                                        std::hint::black_box(txn.get(key)?);
                                        Ok(())
                                    })?;
                                }
                                3 => {
                                    multi.exec("bench", |txn| txn.del(key))?;
                                }
                                _ => unreachable!(),
                            }
                        }
                        Ok(())
                    })
                    .unwrap();
            }
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
            btree.new_bucket(&bucket, false).unwrap();

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
                btree.new_bucket("heavy", false).unwrap();

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
    bench_exec_multi,
    bench_prefix_encoding_compare,
    bench_bucket_ops
);
criterion_main!(benches);
