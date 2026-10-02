//! Per-(owner, thread) shard hints. Recycled slots may retain an old hint:
//! it only chooses the probe start, so cross-thread cleanup is unnecessary.

use parking_lot::Mutex;
use std::cell::RefCell;
use std::sync::atomic::{AtomicU32, Ordering};

thread_local! {
    static CELLS: RefCell<Vec<Option<u64>>> = const { RefCell::new(Vec::new()) };
}
static IDS: SlotIdAllocator = SlotIdAllocator::new();

pub(crate) struct LocalSlot {
    id: u32,
}

impl LocalSlot {
    #[inline]
    pub(crate) fn allocate() -> Self {
        Self { id: IDS.allocate() }
    }

    // Callbacks hold a TLS borrow: they must not block, re-enter this slot, or panic.
    #[inline]
    pub(crate) fn with<R>(&self, init: impl FnOnce() -> u64, f: impl FnOnce(&mut u64) -> R) -> R {
        CELLS.with(|cells| {
            let mut cells = cells.borrow_mut();
            let index = self.id as usize;
            if cells.len() <= index {
                cells.resize_with(index + 1, || None);
            }
            f(cells[index].get_or_insert_with(init))
        })
    }

    #[cfg(test)]
    pub(crate) fn slot_id(&self) -> u32 {
        self.id
    }
}

impl Drop for LocalSlot {
    fn drop(&mut self) {
        IDS.free(self.id);
    }
}

struct SlotIdAllocator {
    next: AtomicU32,
    free: Mutex<Vec<u32>>,
}

impl SlotIdAllocator {
    const fn new() -> Self {
        Self {
            next: AtomicU32::new(0),
            free: Mutex::new(Vec::new()),
        }
    }

    fn allocate(&self) -> u32 {
        if let Some(id) = self.free.lock().pop() {
            return id;
        }
        self.next.fetch_add(1, Ordering::Relaxed)
    }

    fn free(&self, id: u32) {
        self.free.lock().push(id);
    }
}

#[cfg(test)]
mod tests {
    use super::{LocalSlot, SlotIdAllocator};

    #[test]
    fn slot_ids_are_distinct_and_recycled() {
        let ids = SlotIdAllocator::new();
        let first = ids.allocate();
        let second = ids.allocate();
        assert_ne!(first, second);
        ids.free(second);
        assert_eq!(ids.allocate(), second);
    }

    #[test]
    fn slot_entries_are_per_thread_and_per_slot() {
        static FIRST: std::sync::LazyLock<LocalSlot> =
            std::sync::LazyLock::new(LocalSlot::allocate);
        static SECOND: std::sync::LazyLock<LocalSlot> =
            std::sync::LazyLock::new(LocalSlot::allocate);
        assert_ne!(FIRST.slot_id(), SECOND.slot_id());

        FIRST.with(|| 1, |value| *value += 1);
        assert_eq!(FIRST.with(|| 0, |value| *value), 2);
        assert_eq!(SECOND.with(|| 7, |value| *value), 7);

        let other = std::thread::spawn(|| {
            assert_eq!(
                FIRST.with(|| 0, |value| *value),
                0,
                "a thread starts with an empty entry"
            );
            assert_eq!(SECOND.with(|| 0, |value| *value), 0);
            FIRST.with(
                || 0,
                |value| {
                    *value = 5;
                    *value
                },
            )
        });
        assert_eq!(other.join().unwrap(), 5);
        assert_eq!(
            FIRST.with(|| 0, |value| *value),
            2,
            "another thread's writes stay in its own entry"
        );
        assert_eq!(SECOND.with(|| 0, |value| *value), 7);
    }
}
