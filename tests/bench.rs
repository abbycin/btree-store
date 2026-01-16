use std::time::Instant;

use btree_store::BTree;

#[test]
fn bench() {
    let mut path = std::env::temp_dir();
    path.push("xx.db");
    let path_str = path.to_str().unwrap();
    let _ = std::fs::remove_file(path_str);
    let btree = BTree::open(path_str).unwrap();
    let bkt = btree.new_bucket("default").unwrap();
    let data: Vec<String> = (0..100000).map(|x| format!("key_{}", x)).collect();

    let b = Instant::now();
    for x in &data {
        bkt.put(x, x).unwrap();
    }
    let e1 = b.elapsed().as_millis();

    let b = Instant::now();
    for x in &data {
        // let r = bkt.get(x).expect("must exist");
        let Ok(r) = bkt.get(x) else {
            eprintln!("can't find {}", x);
            panic!("damn");
        };
        assert_eq!(r.as_slice(), x.as_bytes());
    }
    let e2 = b.elapsed().as_millis();

    eprintln!("put {}\nget {}", e1, e2);

    btree.commit().expect("can't commit");
}
