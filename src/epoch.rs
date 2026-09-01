//! Reader-epoch tracking for lock-free read/write concurrency.
//!
//! A read view pins the current epoch for its whole lifetime. The allocator
//! consults [`EpochRegistry::oldest_active_reader_epoch`] before promoting
//! retired pages to reusable: promotion is safe only when every in-flight
//! reader pinned at or after the current epoch, because a reader on an older
//! snapshot may still reference the pages being reused.
//!
//! Pins are guard-carried slots (no thread-local storage): a pin takes one of
//! a fixed set of `AtomicU64` slots (or an overflow entry when the fixed set
//! is exhausted) and the slot index travels inside the guard, so multiple
//! `Store` instances in one process are fully isolated and a thread that exits
//! cannot leak a slot.
//!
//! Slots store `epoch + 1`; the idle marker is `0`. This closes the epoch-0
//! encoding gap: with the counter starting at 0, a reader pinned at epoch 0
//! stores 1 and is never mistaken for idle by the `oldest()` scan.

use parking_lot::Mutex;
use rustc_hash::FxHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

const SLOT_COUNT: usize = 256;
const SHARD_COUNT: usize = 64;
// The probe covers every shard so that any free fixed slot is found before
// the overflow path is taken: the "no allocation up to 256 concurrent views"
// fast-path claim holds exactly, not just for favorable hash distributions.
const SHARD_PROBE: usize = SHARD_COUNT;
const SLOTS_PER_SHARD: usize = SLOT_COUNT / SHARD_COUNT; // 4

/// Per-`Store` reader epoch registry. Isolated across database instances.
pub(crate) struct EpochRegistry {
    /// Monotonic epoch counter, advanced once per shared snapshot install
    /// (after `SharedMeta::update`).
    current: AtomicU64,
    /// Pinned epochs, encoded as `epoch + 1`; 0 means idle. Each slot is its
    /// own state machine: a pin CASes `0 -> epoch + 1` directly on the slot
    /// (ABA-free by construction — no shared free-list head, no tag), and an
    /// unpin stores `0` while the pinner still owns the slot exclusively.
    slots: [AtomicU64; SLOT_COUNT],
    /// Unique token source for overflow entries.
    next_token: AtomicU64,
    /// Overflow pins beyond the fixed slot set: `(token, epoch)` pairs.
    /// Presence in the vector means active, so no 0-encoding issue here.
    overflow: Mutex<Vec<(u64, u64)>>,
    /// Per-thread shard-hint slot id.
    hint_id: u32,
}

impl EpochRegistry {
    pub(crate) fn new() -> Self {
        let slots = std::array::from_fn(|_| AtomicU64::new(0));
        Self {
            current: AtomicU64::new(0),
            slots,
            next_token: AtomicU64::new(1),
            overflow: Mutex::new(Vec::new()),
            hint_id: alloc_hint_id(),
        }
    }

    /// Current epoch counter value.
    #[inline]
    pub(crate) fn current(&self) -> u64 {
        self.current.load(Ordering::Acquire)
    }

    /// Advance the epoch after a shared generation becomes visible.
    ///
    /// Ordered after `SharedMeta::update` so a reader that acquires the new
    /// epoch value subsequently observes the new shared snapshot.
    #[inline]
    pub(crate) fn advance(&self) {
        self.current.fetch_add(1, Ordering::Release);
    }

    /// Pin the current epoch for the duration of a read view.
    ///
    /// Common path (≤ 256 concurrent views): pop one slot index from a sharded
    /// free list and store `epoch + 1`; no allocation. The probe covers every
    /// shard, so any free fixed slot is found before the overflow path is
    /// taken. Overflow path (> 256 concurrent views): append an
    /// `(token, epoch)` entry; allocation and a possible short lock wait
    /// happen only here.
    pub(crate) fn pin(&self) -> EpochGuard<'_> {
        let e = self.current();
        let start = thread_shard(self.hint_id);
        for step in 0..SHARD_PROBE {
            let shard = (start + step) % SHARD_COUNT;
            let base = shard * SLOTS_PER_SHARD;
            for idx in base..base + SLOTS_PER_SHARD {
                let slot = &self.slots[idx];
                // CAS directly on the slot: succeeds only while it is idle, so
                // no free-list head, no tag, and no ABA window exists.
                if slot.load(Ordering::Relaxed) == 0
                    && slot
                        .compare_exchange(0, e + 1, Ordering::AcqRel, Ordering::Relaxed)
                        .is_ok()
                {
                    return EpochGuard {
                        registry: self,
                        inner: GuardInner::Slot(idx),
                    };
                }
            }
        }
        let token = self.next_token.fetch_add(1, Ordering::Relaxed);
        self.overflow.lock().push((token, e));
        EpochGuard {
            registry: self,
            inner: GuardInner::Overflow(token),
        }
    }

    /// Smallest epoch pinned by any active reader, or the current epoch when
    /// no reader is pinned. Called once per commit by the allocator.
    ///
    /// # Ordering
    /// The slot loads are `Relaxed`; safety comes from a happens-before chain,
    /// not from the loads themselves. A reader pins before it takes the
    /// `SharedMeta` read lock (`shared_snapshot`), and a writer's
    /// `shared.update` write lock is acquired only after every such reader
    /// released its read lock. The writer then advances the epoch and releases
    /// the writer mutex before the next writer's commit (which runs this scan)
    /// acquires it. Therefore every reader that can reference the promoted
    /// pages (pinned strictly before the current epoch) has its slot store
    /// happens-before this scan and is observed. Do not "strengthen" these
    /// loads without preserving that chain.
    pub(crate) fn oldest_active_reader_epoch(&self) -> u64 {
        let current = self.current();
        let mut oldest = current;
        for slot in &self.slots {
            let v = slot.load(Ordering::Relaxed);
            if v != 0 {
                oldest = oldest.min(v - 1);
            }
        }
        let overflow = self.overflow.lock();
        for &(_, e) in overflow.iter() {
            oldest = oldest.min(e);
        }
        oldest
    }

    #[cfg(test)]
    pub(crate) fn active_reader_count(&self) -> usize {
        let mut n = 0;
        for slot in &self.slots {
            if slot.load(Ordering::Relaxed) != 0 {
                n += 1;
            }
        }
        n += self.overflow.lock().len();
        n
    }
}

impl Drop for EpochRegistry {
    fn drop(&mut self) {
        free_hint_id(self.hint_id);
    }
}

enum GuardInner {
    Slot(usize),
    Overflow(u64),
}

/// RAII pin guard. One pin per active view; the guard carries its slot
/// (or overflow token) so no TLS or global index space is involved.
pub(crate) struct EpochGuard<'a> {
    registry: &'a EpochRegistry,
    inner: GuardInner,
}

impl Drop for EpochGuard<'_> {
    fn drop(&mut self) {
        match self.inner {
            GuardInner::Slot(idx) => {
                // The pinner owns the slot exclusively until this store: a new
                // pin can only CAS it after it reads 0 here.
                self.registry.slots[idx].store(0, Ordering::Relaxed);
            }
            GuardInner::Overflow(token) => {
                let mut overflow = self.registry.overflow.lock();
                overflow.retain(|(t, _)| *t != token);
            }
        }
    }
}

// --- per-thread, per-registry shard hint cache ---
//
// The pin's probe-start shard is cached per (thread, registry) the way: a global
// allocator hands each `EpochRegistry` a unique hint id, and each thread keeps
// a TLS vector of shard hints indexed by that id. The hint is a pure
// probe-start value, so a thread exiting needs no cleanup and a recycled id
// only ever sees a stale-but-harmless hint. This keeps per-instance isolation
// (registry A's hint never overwrites registry B's) without recomputing the
// thread hash on every pin.

use std::cell::RefCell;

const HINT_UNSET: u64 = u64::MAX;

static NEXT_HINT_ID: AtomicU32 = AtomicU32::new(0);
static FREE_HINT_IDS: Mutex<Vec<u32>> = Mutex::new(Vec::new());

thread_local! {
    static SHARD_HINTS: RefCell<Vec<u64>> = const { RefCell::new(Vec::new()) };
}

fn alloc_hint_id() -> u32 {
    if let Some(id) = FREE_HINT_IDS.lock().pop() {
        return id;
    }
    NEXT_HINT_ID.fetch_add(1, Ordering::Relaxed)
}

fn free_hint_id(id: u32) {
    FREE_HINT_IDS.lock().push(id);
}

fn thread_shard(hint_id: u32) -> usize {
    let cached = SHARD_HINTS.with(|h| {
        h.borrow()
            .get(hint_id as usize)
            .copied()
            .unwrap_or(HINT_UNSET)
    });
    if cached != HINT_UNSET {
        return cached as usize;
    }
    let mut hasher = FxHasher::default();
    std::thread::current().id().hash(&mut hasher);
    let shard = (hasher.finish() as usize) % SHARD_COUNT;
    SHARD_HINTS.with(|h| {
        let mut v = h.borrow_mut();
        if v.len() <= hint_id as usize {
            v.resize(hint_id as usize + 1, HINT_UNSET);
        }
        v[hint_id as usize] = shard as u64;
    });
    shard
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn shard_hint_ids_are_isolated_and_stable() {
        let a = EpochRegistry::new();
        let b = EpochRegistry::new();
        assert_ne!(
            a.hint_id, b.hint_id,
            "registries must not share a hint slot"
        );
        let s1 = thread_shard(a.hint_id);
        let s2 = thread_shard(a.hint_id);
        assert_eq!(s1, s2, "hint must be stable per thread");
        assert!(s1 < SHARD_COUNT);
        let id_a = a.hint_id;
        drop(a);
        let s3 = thread_shard(id_a);
        assert!(
            s3 < SHARD_COUNT,
            "recycled id must still yield a valid hint"
        );
    }

    /// A reader pinned at epoch 0 (the registry's initial value) must store 1
    /// in its slot and stay visible to `oldest()`. This is the regression for
    /// the slot-encoding gap: storing the raw epoch 0 would look idle.
    #[test]
    fn epoch_zero_reader_is_visible_not_idle() {
        let registry = EpochRegistry::new();
        assert_eq!(registry.current(), 0);
        let guard = registry.pin();
        assert_eq!(registry.oldest_active_reader_epoch(), 0);
        assert_eq!(registry.active_reader_count(), 1);
        assert_eq!(
            registry
                .slots
                .iter()
                .filter(|s| s.load(Ordering::Relaxed) == 1)
                .count(),
            1
        );
        drop(guard);
        assert_eq!(registry.active_reader_count(), 0);
        assert_eq!(registry.oldest_active_reader_epoch(), registry.current());
    }

    /// `oldest()` tracks the minimum active pinned epoch across advances.
    #[test]
    fn oldest_tracks_min_pinned_epoch() {
        let registry = EpochRegistry::new();
        let first = registry.pin(); // epoch 0
        registry.advance();
        registry.advance();
        assert_eq!(registry.current(), 2);
        let second = registry.pin(); // epoch 2
        assert_eq!(registry.oldest_active_reader_epoch(), 0);
        drop(first);
        assert_eq!(registry.oldest_active_reader_epoch(), 2);
        drop(second);
        assert_eq!(registry.oldest_active_reader_epoch(), registry.current());
    }

    /// Concurrent pins from many threads are all visible and individually
    /// releasable; every guard reports an oldest epoch at most its own pin.
    #[test]
    fn concurrent_pins_are_all_visible() {
        use std::sync::Barrier;
        let registry = Arc::new(EpochRegistry::new());
        let pinned = Arc::new(Barrier::new(17)); // 16 pinning threads + main
        let release = Arc::new(Barrier::new(17));
        let threads: Vec<_> = (0..16)
            .map(|_| {
                let registry = registry.clone();
                let pinned = pinned.clone();
                let release = release.clone();
                thread::spawn(move || {
                    let guard = registry.pin();
                    assert!(registry.oldest_active_reader_epoch() <= guard.pinned_epoch());
                    pinned.wait();
                    release.wait();
                    drop(guard);
                })
            })
            .collect();
        pinned.wait();
        assert_eq!(registry.active_reader_count(), 16);
        release.wait();
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(registry.active_reader_count(), 0);
    }

    /// More readers than fixed slots exercise the overflow path; `oldest()`
    /// and cleanup stay correct, and slots are reusable afterward. Also
    /// verifies the fixed slots are fully used before any overflow entry:
    /// with the all-shard probe, 256 pins fill the fixed set and the 257th
    /// is the first overflow entry.
    #[test]
    fn overflow_path_serves_more_than_slot_count_readers() {
        let registry = EpochRegistry::new();
        let mut guards: Vec<EpochGuard> = Vec::new();
        for _ in 0..SLOT_COUNT {
            guards.push(registry.pin());
        }
        assert_eq!(registry.active_reader_count(), SLOT_COUNT);
        assert_eq!(
            registry.overflow.lock().len(),
            0,
            "fixed slots must be fully used before any overflow entry"
        );
        for _ in 0..7 {
            guards.push(registry.pin());
        }
        assert_eq!(registry.active_reader_count(), SLOT_COUNT + 7);
        assert_eq!(registry.overflow.lock().len(), 7);
        assert_eq!(registry.oldest_active_reader_epoch(), registry.current());
        // release the overflow readers (last 7 pinned) first, then the rest
        for _ in 0..7 {
            guards.pop();
        }
        assert_eq!(registry.active_reader_count(), SLOT_COUNT);
        assert_eq!(registry.overflow.lock().len(), 0);
        // slots are reusable after full release
        guards.clear();
        assert_eq!(registry.active_reader_count(), 0);
        let again = registry.pin();
        assert_eq!(registry.active_reader_count(), 1);
        drop(again);
    }

    impl EpochGuard<'_> {
        fn pinned_epoch(&self) -> u64 {
            match &self.inner {
                GuardInner::Slot(idx) => self.registry.slots[*idx].load(Ordering::Relaxed) - 1,
                GuardInner::Overflow(token) => {
                    let overflow = self.registry.overflow.lock();
                    overflow.iter().find(|(t, _)| t == token).unwrap().1
                }
            }
        }
    }
}
