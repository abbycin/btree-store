use btree_store::BTree;
use std::time::Instant;
use tempfile::TempDir;

#[test]
fn bench() {
    let mut temp_dir = TempDir::new().unwrap();
    temp_dir.disable_cleanup(true);
    let db_path = temp_dir.path().join("bench.db");
    let btree = BTree::open(&db_path).unwrap();

    let data: Vec<String> = (0..100000).map(|x| format!("key_{}", x)).collect();

    let b = Instant::now();
    btree
        .exec("default", |txn| {
            for x in &data {
                txn.put(x, x).unwrap();
            }
            Ok(())
        })
        .unwrap();
    let e1 = b.elapsed().as_millis();

    let b = Instant::now();
    btree
        .view("default", |txn| {
            for x in &data {
                let r = txn.get(x).expect("must exist");
                assert_eq!(r.as_slice(), x.as_bytes());
            }
            Ok(())
        })
        .unwrap();
    let e2 = b.elapsed().as_millis();

    eprintln!("put {}ms\nget {}ms", e1, e2);
    if let Ok(m) = db_path.metadata() {
        eprintln!("len {}", m.len());
        assert!(m.len() < (20 << 20));
    };
}
