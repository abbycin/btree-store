use crate::{
    PageId,
    node::{AlignedPage, Node},
    store::PageReuseObserver,
};
use parking_lot::RwLock;
use rustc_hash::FxHashMap;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

struct CacheEntry {
    page_id: PageId,
    node: Arc<Node>,
    usage: AtomicBool,
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
            if !entry.usage.load(Ordering::Relaxed) {
                entry.usage.store(true, Ordering::Relaxed);
            }
            return Some(entry.node.clone());
        }
        None
    }

    fn get_branch(&self, page_id: PageId) -> Option<Arc<Node>> {
        if let Some(idx) = self.find_entry_idx(page_id)
            && let Some(entry) = &self.entries[idx]
            && !entry.node.is_leaf()
        {
            if !entry.usage.load(Ordering::Relaxed) {
                entry.usage.store(true, Ordering::Relaxed);
            }
            return Some(entry.node.clone());
        }
        None
    }

    #[cfg(test)]
    fn peek(&self, page_id: PageId) -> Option<Arc<Node>> {
        self.find_entry_idx(page_id)
            .and_then(|idx| self.entries[idx].as_ref().map(|entry| entry.node.clone()))
    }

    fn put(&mut self, page_id: PageId, node: Arc<Node>) {
        if self.capacity == 0 {
            return;
        }
        if let Some(idx) = self.find_entry_idx(page_id)
            && let Some(entry) = &mut self.entries[idx]
        {
            entry.usage.store(true, Ordering::Relaxed);
            entry.node = node;
            return;
        }

        loop {
            let evict = match &mut self.entries[self.hand] {
                None => true,
                Some(entry) if entry.usage.swap(false, Ordering::Relaxed) => false,
                Some(_) => true,
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
                    usage: AtomicBool::new(true),
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

    #[cfg(test)]
    pub(crate) fn peek(&self, page_id: PageId) -> Option<Arc<Node>> {
        if self.shards.is_empty() {
            return None;
        }
        self.get_shard(page_id).read().peek(page_id)
    }

    pub(crate) fn get_branch(&self, page_id: PageId) -> Option<Arc<Node>> {
        if self.shards.is_empty() {
            return None;
        }
        self.get_shard(page_id).read().get_branch(page_id)
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
    fn node_cache_peek_does_not_refresh_usage_bit() {
        let cache = NodeCache::new(1);
        let page_id = 7;
        cache.put(page_id, Arc::new(Node::new_leaf()));

        {
            let shard = cache.get_shard(page_id);
            let mut guard = shard.write();
            let entry = guard.entries[0].as_mut().expect("cached entry");
            entry.usage.store(false, Ordering::Relaxed);
        }

        let node = cache.peek(page_id).expect("peeked node");
        assert!(node.is_leaf());

        let shard = cache.get_shard(page_id);
        let guard = shard.read();
        let entry = guard.entries[0].as_ref().expect("cached entry");
        assert!(
            !entry.usage.load(Ordering::Relaxed),
            "peek should not refresh the clock-cache usage bit"
        );
    }

    #[test]
    fn node_cache_get_refreshes_usage_bit() {
        let cache = NodeCache::new(1);
        let page_id = 7;
        cache.put(page_id, Arc::new(Node::new_leaf()));

        {
            let shard = cache.get_shard(page_id);
            let guard = shard.read();
            guard.entries[0]
                .as_ref()
                .expect("cached entry")
                .usage
                .store(false, Ordering::Relaxed);
        }

        assert!(cache.get(page_id).is_some());

        let shard = cache.get_shard(page_id);
        let guard = shard.read();
        assert!(
            guard.entries[0]
                .as_ref()
                .expect("cached entry")
                .usage
                .load(Ordering::Relaxed)
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
}
