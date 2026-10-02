//! The value path's staging cost is bounded by the call, not by the cap.
//!
//! A value operation stages whole physical pages, and the staging budget bounds
//! peak memory per operation (1 MiB). Sizing the buffer to *that* budget on every
//! call makes a 300-byte value allocate a megabyte twice, which no benchmark in
//! `benches/btree_bench.rs` can see (every value there is inline). This test
//! watches the allocator instead: from the moment the database is opened, no
//! single request made by opening it, warming the value path, writing one
//! 300-byte value and reading it back may approach the cap.
//!
//! One test per binary on purpose: the counter is process-global, so a second test
//! running in another thread would pollute the window.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Largest single allocation request seen since the last reset.
static LARGEST: AtomicUsize = AtomicUsize::new(0);

struct Watched;

fn note(size: usize) {
    LARGEST.fetch_max(size, Ordering::Relaxed);
}

unsafe impl GlobalAlloc for Watched {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        note(layout.size());
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        note(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        note(new_size);
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static WATCHED: Watched = Watched;

/// The documented staging budget: `VALUE_STAGING_PAGES` pages of 4 KiB, i.e. 1 MiB.
/// Allocating that much for a small value is what this test forbids, whether once
/// per call or once and kept.
const CAP: usize = 1 << 20;
/// Above what a small value's write and read need (one page of staging plus the
/// value itself) and well below the cap. It is written as a fraction of `CAP` so
/// that lowering this literal tightens the assertion with it. Nothing links it to
/// the engine's own `VALUE_STAGING_PAGES`, which is crate-private: if that budget
/// were lowered below this bound the assertion would go vacuous, so the bound is
/// deliberately far enough below `CAP` that any plausible budget still clears it.
const BOUND: usize = CAP / 16;

#[test]
fn a_small_value_operation_stages_only_what_it_needs() {
    // The window opens before the database is opened: a buffer allocated once at the
    // cap and reused forever would otherwise happen before any later reset and stay
    // invisible.
    LARGEST.store(0, Ordering::Relaxed);
    let dir = tempfile::TempDir::new().unwrap();
    let tree = btree_store::BTree::open(dir.path().join("db")).unwrap();
    tree.new_bucket("staging", false).unwrap();

    // 300 bytes is over the inline threshold, so it takes the page path.
    let value = vec![0x5au8; 300];
    tree.exec("staging", |txn| txn.put(b"warm", &value))
        .unwrap();
    tree.exec("staging", |txn| txn.put(b"warm2", &value))
        .unwrap();
    tree.exec("staging", |txn| txn.get(b"warm2")).unwrap();

    tree.exec("staging", |txn| txn.put(b"measured", &value))
        .unwrap();
    let read = tree.exec("staging", |txn| txn.get(b"measured")).unwrap();
    assert_eq!(read, value, "the value must survive the round trip");

    let largest = LARGEST.load(Ordering::Relaxed);
    assert!(
        largest < BOUND,
        "writing and reading one 300-byte value requested a {largest}-byte allocation; \
         the staging buffer must be sized to the pages the call touches, capped at the \
         staging budget, instead of being allocated at the budget for every operation"
    );
}
