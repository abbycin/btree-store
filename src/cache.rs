use crate::{
    PageId,
    node::{AlignedPage, Node},
    store::PageReuseObserver,
};
use parking_lot::RwLock;
use rustc_hash::FxHashMap;
use std::sync::{
    Arc,
    atomic::{AtomicU8, Ordering},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum CacheState {
    Hot = 1,
    Warm = 2,
    Cold = 3,
}

impl CacheState {
    fn initial(node: &Node) -> Self {
        if node.is_leaf() {
            Self::Warm
        } else {
            Self::Hot
        }
    }

    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Hot,
            2 => Self::Warm,
            3 => Self::Cold,
            _ => unreachable!("invalid cache state"),
        }
    }

    fn as_u8(self) -> u8 {
        self as u8
    }
}

const RECENT_BIT: u8 = 0b100;
const STATE_MASK: u8 = 0b011;

struct CacheEntry {
    page_id: PageId,
    node: Arc<Node>,
    flags: AtomicU8,
}

impl CacheEntry {
    fn on_hit(&self) {
        let is_branch = !self.node.is_leaf();
        let flags = self.flags.load(Ordering::Relaxed);
        match CacheState::from_u8(flags & STATE_MASK) {
            CacheState::Hot => {
                if flags & RECENT_BIT == 0 {
                    self.flags.store(flags | RECENT_BIT, Ordering::Relaxed);
                }
            }
            CacheState::Warm if is_branch => {
                self.flags
                    .store(RECENT_BIT | CacheState::Hot.as_u8(), Ordering::Relaxed);
            }
            CacheState::Warm => {
                if flags & RECENT_BIT == 0 {
                    self.flags.store(flags | RECENT_BIT, Ordering::Relaxed);
                }
            }
            CacheState::Cold => {
                let warmed = RECENT_BIT | CacheState::Warm.as_u8();
                if self
                    .flags
                    .compare_exchange(flags, warmed, Ordering::Relaxed, Ordering::Relaxed)
                    .is_err()
                    && is_branch
                {
                    self.flags
                        .store(RECENT_BIT | CacheState::Hot.as_u8(), Ordering::Relaxed);
                }
            }
        }
    }

    fn on_eviction(&self) -> bool {
        let flags = self.flags.load(Ordering::Relaxed);
        if flags & RECENT_BIT != 0 {
            self.flags.store(flags & !RECENT_BIT, Ordering::Relaxed);
            return false;
        }
        match CacheState::from_u8(flags & STATE_MASK) {
            CacheState::Hot => {
                self.flags
                    .store(CacheState::Warm.as_u8(), Ordering::Relaxed);
                false
            }
            CacheState::Warm => {
                self.flags
                    .store(CacheState::Cold.as_u8(), Ordering::Relaxed);
                false
            }
            CacheState::Cold => true,
        }
    }
}

struct NodeCacheShard {
    entries: Vec<Option<CacheEntry>>,
    page_to_entry: FxHashMap<PageId, usize>,
    recycled_page: Option<AlignedPage>,
    hand: usize,
    capacity: usize,
}

impl NodeCacheShard {
    fn new(capacity: usize) -> Self {
        Self {
            entries: (0..capacity).map(|_| None).collect(),
            page_to_entry: FxHashMap::with_capacity_and_hasher(capacity, Default::default()),
            recycled_page: None,
            hand: 0,
            capacity,
        }
    }

    #[inline]
    fn find_entry_idx(&self, page_id: PageId) -> Option<usize> {
        self.page_to_entry.get(&page_id).copied()
    }

    #[inline(always)]
    fn get(&self, page_id: PageId) -> Option<Arc<Node>> {
        if let Some(idx) = self.find_entry_idx(page_id)
            && let Some(entry) = &self.entries[idx]
        {
            entry.on_hit();
            return Some(entry.node.clone());
        }
        None
    }

    fn put(&mut self, page_id: PageId, node: Arc<Node>) {
        if self.capacity == 0 {
            return;
        }
        let initial_state = CacheState::initial(&node);
        if let Some(idx) = self.find_entry_idx(page_id)
            && let Some(entry) = &mut self.entries[idx]
        {
            entry
                .flags
                .store(initial_state.as_u8() | RECENT_BIT, Ordering::Relaxed);
            entry.node = node;
            return;
        }

        loop {
            let evict = match &self.entries[self.hand] {
                None => true,
                Some(entry) => entry.on_eviction(),
            };

            if evict {
                if let Some(entry) = self.entries[self.hand].take() {
                    self.page_to_entry.remove(&entry.page_id);
                    if self.recycled_page.is_none()
                        && let Ok(node) = Arc::try_unwrap(entry.node)
                    {
                        self.recycled_page = Some(node.into_aligned_page());
                    }
                }
                self.entries[self.hand] = Some(CacheEntry {
                    page_id,
                    node,
                    flags: AtomicU8::new(initial_state.as_u8() | RECENT_BIT),
                });
                self.page_to_entry.insert(page_id, self.hand);
                self.hand = (self.hand + 1) % self.capacity;
                return;
            }
            self.hand = (self.hand + 1) % self.capacity;
        }
    }

    fn invalidate(&mut self, page_id: PageId) {
        if let Some(idx) = self.page_to_entry.remove(&page_id) {
            self.entries[idx] = None;
        }
    }

    fn take_recycled_page(&mut self) -> Option<AlignedPage> {
        self.recycled_page.take()
    }
}

pub(crate) const NUM_SHARDS: usize = 64;

// Keep a shard's lock and read-mostly metadata off adjacent shards' cache lines.
#[repr(align(64))]
struct CacheShard {
    lock: RwLock<NodeCacheShard>,
}

pub(crate) struct NodeCache {
    shards: Vec<CacheShard>,
}

impl NodeCache {
    pub(crate) fn new(capacity: usize) -> Self {
        let shard_count = capacity.min(NUM_SHARDS);
        let mut shards = Vec::with_capacity(shard_count);
        if shard_count == 0 {
            return Self { shards };
        }

        let base = capacity / shard_count;
        let remainder = capacity % shard_count;
        for idx in 0..shard_count {
            let shard_cap = base + usize::from(idx < remainder);
            shards.push(CacheShard {
                lock: RwLock::new(NodeCacheShard::new(shard_cap)),
            });
        }
        Self { shards }
    }

    #[inline(always)]
    fn get_shard(&self, page_id: PageId) -> &RwLock<NodeCacheShard> {
        debug_assert!(!self.shards.is_empty());
        &self.shards[self.shard_index(page_id)].lock
    }

    #[inline]
    fn shard_index(&self, page_id: PageId) -> usize {
        if self.shards.len().is_power_of_two() {
            (page_id as usize) & (self.shards.len() - 1)
        } else {
            (page_id as usize) % self.shards.len()
        }
    }

    #[inline(always)]
    pub(crate) fn get(&self, page_id: PageId) -> Option<Arc<Node>> {
        if self.shards.is_empty() {
            return None;
        }
        self.get_shard(page_id).read().get(page_id)
    }

    pub(crate) fn put(&self, page_id: PageId, node: Arc<Node>) {
        if self.shards.is_empty() {
            return;
        }
        self.get_shard(page_id).write().put(page_id, node)
    }

    pub(crate) fn take_recycled_page(&self, page_id: PageId) -> Option<AlignedPage> {
        if self.shards.is_empty() {
            return None;
        }
        self.get_shard(page_id).write().take_recycled_page()
    }

    pub(crate) fn invalidate(&self, page_id: PageId) {
        if self.shards.is_empty() {
            return;
        }
        self.get_shard(page_id).write().invalidate(page_id)
    }

    /// Test-only occupancy probe: reports a resident entry without counting a
    /// hit, so a test can assert "this PID is cached" without ageing it.
    #[cfg(test)]
    pub(crate) fn peek(&self, page_id: PageId) -> Option<Arc<Node>> {
        if self.shards.is_empty() {
            return None;
        }
        let shard = self.get_shard(page_id).read();
        shard
            .find_entry_idx(page_id)
            .and_then(|idx| shard.entries[idx].as_ref().map(|entry| entry.node.clone()))
    }

    pub(crate) fn clear(&self) {
        for shard in &self.shards {
            let mut guard = shard.lock.write();
            guard.entries.iter_mut().for_each(|entry| *entry = None);
            guard.page_to_entry.clear();
        }
    }
}

impl PageReuseObserver for NodeCache {
    fn invalidate(&self, page_id: PageId) {
        NodeCache::invalidate(self, page_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_cache_small_capacity_uses_only_nonzero_shards() {
        for capacity in [1usize, 17, 63] {
            let cache = NodeCache::new(capacity);
            assert_eq!(cache.shards.len(), capacity.min(NUM_SHARDS));
            assert_eq!(
                cache
                    .shards
                    .iter()
                    .map(|shard| shard.lock.read().capacity)
                    .sum::<usize>(),
                capacity
            );
            for page_id in [0_u32, 1, 63, 64, 127, 4095] {
                assert!(
                    cache.get_shard(page_id).read().capacity > 0,
                    "capacity={capacity} should give every active shard at least one slot"
                );
            }
        }
    }

    #[test]
    fn node_cache_shards_do_not_share_cache_lines() {
        assert_eq!(std::mem::align_of::<CacheShard>(), 64);
        assert_eq!(std::mem::size_of::<CacheShard>() % 64, 0);

        let cache = NodeCache::new(2);
        let first = std::ptr::from_ref(&cache.shards[0]).addr();
        let second = std::ptr::from_ref(&cache.shards[1]).addr();
        assert_eq!(first % 64, 0);
        assert_eq!(second - first, std::mem::size_of::<CacheShard>());
    }

    #[test]
    fn node_cache_distributes_sequential_page_ids_across_shards() {
        let cache = NodeCache::new(NUM_SHARDS * 128);
        let shard_count = cache.shards.len();
        let mut counts = vec![0usize; shard_count];

        for page_id in 2..(2 + (shard_count as u32 * 256)) {
            counts[cache.shard_index(page_id)] += 1;
        }

        let min = *counts.iter().min().unwrap();
        let max = *counts.iter().max().unwrap();
        assert!(min > 0);
        assert!(
            max <= min * 2,
            "sequential page ids are unevenly distributed: {counts:?}"
        );
    }

    #[test]
    fn node_cache_identity_is_page_id() {
        let cache = NodeCache::new(1);
        let page_id = 7;
        cache.put(page_id, Arc::new(Node::new_leaf()));

        assert!(cache.get(page_id).is_some());
        assert!(cache.get(page_id + 1).is_none());

        cache.invalidate(page_id);
        assert!(cache.get(page_id).is_none());

        cache.put(page_id, Arc::new(Node::new_leaf()));
        cache.put(page_id + 1, Arc::new(Node::new_leaf()));
        assert!(cache.take_recycled_page(page_id).is_some());
        assert!(cache.take_recycled_page(page_id).is_none());
    }

    /// `capacity=128` gives every one of the 64 shards two slots, and the shard
    /// index is `pid % 64`, so the PIDs used below are exactly the pairs that
    /// share a shard. Three `put`s into one shard fill it, and the third walks
    /// the hand over both resident entries, so the tier ladder decides which of
    /// them survives.
    fn two_slot_cache() -> NodeCache {
        NodeCache::new(NUM_SHARDS * 2)
    }

    fn sibling(pid: PageId) -> PageId {
        pid + NUM_SHARDS as PageId
    }

    fn leaf_node() -> Arc<Node> {
        Arc::new(Node::new_leaf())
    }

    fn branch_node() -> Arc<Node> {
        Arc::new(Node::new_branch_root(
            crate::DataPid::new(2).unwrap(),
            crate::node::NonEmptyKey::new(b"sep".to_vec()).unwrap(),
            crate::DataPid::new(3).unwrap(),
        ))
    }

    #[test]
    fn eviction_preserves_a_branch_longer_than_a_leaf_then_drops_it() {
        let cache = two_slot_cache();
        cache.put(1, branch_node());
        cache.put(sibling(1), leaf_node());
        cache.put(sibling(sibling(1)), leaf_node());
        assert!(cache.peek(1).is_some());
        assert!(cache.peek(sibling(1)).is_none());

        cache.put(sibling(sibling(sibling(1))), leaf_node());
        assert!(cache.peek(1).is_none());
    }

    fn hand_visits(entry: Arc<Node>, hits: &[usize]) -> Vec<bool> {
        let cache = two_slot_cache();
        cache.put(1, entry);
        let mut alive = Vec::new();
        for visit in 1..=6 {
            if hits.contains(&visit) {
                assert!(
                    cache.get(1).is_some(),
                    "visit {visit}: the entry is still cached"
                );
            }
            let filler = 1 + NUM_SHARDS as PageId * visit as PageId;
            cache.put(filler, leaf_node());
            cache.invalidate(filler);
            alive.push(cache.peek(1).is_some());
        }
        alive
    }

    #[test]
    fn a_hit_ages_an_entry_up_and_defers_its_eviction() {
        assert_eq!(
            hand_visits(leaf_node(), &[]),
            [true, true, true, false, false, false],
            "a leaf starts Warm, so it is evictable on its fourth hand visit"
        );

        assert_eq!(
            hand_visits(leaf_node(), &[4]),
            [true, true, true, true, true, false],
            "a hit must warm the entry up and set RECENT, deferring the eviction by one visit"
        );

        assert_eq!(
            hand_visits(branch_node(), &[]),
            [true, true, true, true, false, false],
            "a branch starts Hot, so it needs one more hand visit than a leaf"
        );
    }

    #[test]
    fn invalidate_drops_an_entry_at_every_tier() {
        let aged_to = |hit: bool| {
            let cache = two_slot_cache();
            cache.put(1, branch_node());
            cache.put(sibling(1), leaf_node());
            cache.put(sibling(sibling(1)), leaf_node());
            if hit {
                cache.get(1);
            }
            assert!(cache.peek(1).is_some(), "hit={hit}: reached resident");
            cache
        };
        for hit in [false, true] {
            let cache = aged_to(hit);
            cache.invalidate(1);
            assert!(
                cache.peek(1).is_none(),
                "hit={hit}: invalidate must not depend on the entry's tier"
            );
        }
    }
}
