use btree_store::BTree;
use std::fs;

#[test]
fn test_super_large_value() {
    let path = "test_super_large.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.pending", path));

    let tree = BTree::open(path).unwrap();
    let bucket = tree.new_bucket("large_data").unwrap();

    // 3MB Value > 2MB limit
    let size = 3 * 1024 * 1024;
    let large_value = vec![0x55u8; size];

    bucket.put(b"big_key", &large_value).unwrap();
    tree.commit().unwrap();

    let retrieved = bucket.get(b"big_key").unwrap();
    assert_eq!(retrieved.len(), size);
    assert_eq!(retrieved, large_value);

    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.pending", path));
}
