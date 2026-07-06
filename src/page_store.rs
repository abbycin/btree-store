use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::node::Node;
use crate::store::Store;
use crate::{CacheMode, Error, OpenOptions, Result, Tree};
use parking_lot::Mutex;

pub(crate) type Lid = u32;
pub(crate) type Pid = u32;

pub(crate) fn encode_u32_key(value: u32) -> [u8; 4] {
    value.to_be_bytes()
}

pub(crate) fn decode_u32_key(bytes: &[u8]) -> Result<u32> {
    if bytes.len() != 4 {
        return Err(Error::Corruption);
    }
    Ok(u32::from_be_bytes(bytes.try_into().unwrap()))
}

const LID_PID_CACHE_SHARDS: usize = 64;

struct LidPidCacheEntry {
    lid: Lid,
    pid: Pid,
    usage: bool,
}

struct LidPidCacheShard {
    entries: Vec<Option<LidPidCacheEntry>>,
    map: HashMap<Lid, usize>,
    hand: usize,
    capacity: usize,
}

impl LidPidCacheShard {
    fn new(capacity: usize) -> Self {
        Self {
            entries: (0..capacity).map(|_| None).collect(),
            map: HashMap::with_capacity(capacity),
            hand: 0,
            capacity,
        }
    }

    fn get(&mut self, lid: Lid) -> Option<Pid> {
        if let Some(&idx) = self.map.get(&lid)
            && let Some(entry) = &mut self.entries[idx]
        {
            entry.usage = true;
            return Some(entry.pid);
        }
        None
    }

    fn put(&mut self, lid: Lid, pid: Pid) {
        if self.capacity == 0 {
            return;
        }
        if let Some(&idx) = self.map.get(&lid)
            && let Some(entry) = &mut self.entries[idx]
        {
            entry.usage = true;
            entry.pid = pid;
            return;
        }

        loop {
            let evict = match &mut self.entries[self.hand] {
                None => true,
                Some(entry) => {
                    if entry.usage {
                        entry.usage = false;
                        false
                    } else {
                        self.map.remove(&entry.lid);
                        true
                    }
                }
            };

            if evict {
                self.entries[self.hand] = Some(LidPidCacheEntry {
                    lid,
                    pid,
                    usage: true,
                });
                self.map.insert(lid, self.hand);
                self.hand = (self.hand + 1) % self.capacity;
                return;
            }
            self.hand = (self.hand + 1) % self.capacity;
        }
    }

    fn invalidate(&mut self, lid: Lid) {
        if let Some(&idx) = self.map.get(&lid) {
            self.entries[idx] = None;
            self.map.remove(&lid);
        }
    }

    fn clear(&mut self) {
        for slot in &mut self.entries {
            *slot = None;
        }
        self.map.clear();
        self.hand = 0;
    }
}

struct LidPidCache {
    shards: Vec<Mutex<LidPidCacheShard>>,
}

impl LidPidCache {
    fn new(capacity: usize) -> Self {
        let shard_count = capacity.min(LID_PID_CACHE_SHARDS);
        let mut shards = Vec::with_capacity(shard_count);
        if shard_count == 0 {
            return Self { shards };
        }

        let base = capacity / shard_count;
        let remainder = capacity % shard_count;
        for idx in 0..shard_count {
            let shard_cap = base + usize::from(idx < remainder);
            shards.push(Mutex::new(LidPidCacheShard::new(shard_cap)));
        }
        Self { shards }
    }

    fn shard(&self, lid: Lid) -> &Mutex<LidPidCacheShard> {
        debug_assert!(!self.shards.is_empty());
        let idx = if self.shards.len().is_power_of_two() {
            (lid as usize) & (self.shards.len() - 1)
        } else {
            (lid as usize) % self.shards.len()
        };
        &self.shards[idx]
    }

    fn get(&self, lid: Lid) -> Option<Pid> {
        if self.shards.is_empty() {
            return None;
        }
        self.shard(lid).lock().get(lid)
    }

    fn put(&self, lid: Lid, pid: Pid) {
        if self.shards.is_empty() {
            return;
        }
        self.shard(lid).lock().put(lid, pid)
    }

    fn invalidate(&self, lid: Lid) {
        if self.shards.is_empty() {
            return;
        }
        self.shard(lid).lock().invalidate(lid)
    }

    fn clear(&self) {
        for shard in &self.shards {
            shard.lock().clear();
        }
    }
}

struct LidPidHotCache {
    slots: Box<[AtomicU64]>,
}

impl LidPidHotCache {
    fn new(capacity: usize) -> Self {
        let slots = (0..capacity)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self { slots }
    }

    #[inline]
    fn idx(&self, lid: Lid) -> usize {
        if self.slots.len().is_power_of_two() {
            (lid as usize) & (self.slots.len() - 1)
        } else {
            (lid as usize) % self.slots.len()
        }
    }

    #[inline]
    fn encode(lid: Lid, pid: Pid) -> u64 {
        ((lid as u64) << 32) | (pid as u64)
    }

    #[inline]
    fn decode(entry: u64) -> (Lid, Pid) {
        ((entry >> 32) as Lid, entry as Pid)
    }

    fn get(&self, lid: Lid) -> Option<Pid> {
        if self.slots.is_empty() {
            return None;
        }
        let entry = self.slots[self.idx(lid)].load(Ordering::Relaxed);
        let (cached_lid, pid) = Self::decode(entry);
        if cached_lid == lid { Some(pid) } else { None }
    }

    fn put(&self, lid: Lid, pid: Pid) {
        if self.slots.is_empty() {
            return;
        }
        self.slots[self.idx(lid)].store(Self::encode(lid, pid), Ordering::Relaxed);
    }

    fn invalidate(&self, lid: Lid) {
        if self.slots.is_empty() {
            return;
        }
        let slot = &self.slots[self.idx(lid)];
        let entry = slot.load(Ordering::Relaxed);
        let (cached_lid, _) = Self::decode(entry);
        if cached_lid == lid {
            slot.store(0, Ordering::Relaxed);
        }
    }

    fn clear(&self) {
        for slot in self.slots.iter() {
            slot.store(0, Ordering::Relaxed);
        }
    }
}

pub(crate) trait PageStore: Send + Sync {
    fn alloc_pages(&self, nr_pages: u32, alloc: &mut HashSet<u32>) -> Result<Vec<Lid>>;

    fn alloc_page(&self, alloc: &mut HashSet<u32>) -> Result<Lid> {
        let pages = self.alloc_pages(1, alloc)?;
        Ok(pages[0])
    }

    fn schedule_free(&self, lid: Lid, freed: &mut Vec<(u32, u32)>) -> Result<()>;
    fn free_pages_immediate(&self, page_id: u32, nr_pages: u32) -> Result<()>;
    fn load_node(&self, lid: Lid) -> Result<Arc<Node>>;
    fn load_page(&self, lid: Lid) -> Result<Vec<u8>>;
    fn load_data(&self, lids: &[Lid], len: usize) -> Result<Vec<u8>>;
    fn read_data(&self, lids: &[Lid], buf: &mut [u8]) -> Result<()>;
    fn write_data(&self, lids: &[Lid], data: &[u8]) -> Result<()>;
    fn write_page(&self, lid: Lid, data: &[u8]) -> Result<()>;

    fn cached_node_is_leaf(&self, _lid: Lid) -> Option<bool> {
        None
    }

    fn cache_node(&self, _lid: Lid, _node: Arc<Node>) {}

    fn load_node_with_mode(&self, lid: Lid, mode: CacheMode) -> Result<Arc<Node>> {
        match mode {
            CacheMode::Default => self.load_node(lid),
            CacheMode::ByPass => {
                let raw = self.load_page_with_mode(lid, mode)?;
                Ok(Arc::new(Node::from_raw(raw)?))
            }
        }
    }

    fn load_page_with_mode(&self, lid: Lid, _mode: CacheMode) -> Result<Vec<u8>> {
        self.load_page(lid)
    }

    fn load_data_with_mode(&self, lids: &[Lid], len: usize, _mode: CacheMode) -> Result<Vec<u8>> {
        self.load_data(lids, len)
    }

    fn read_data_with_mode(&self, lids: &[Lid], buf: &mut [u8], _mode: CacheMode) -> Result<()> {
        self.read_data(lids, buf)
    }
}

pub(crate) struct LogicalStore {
    store: Arc<Store>,
    mapping_tree: Arc<Tree>,
    reverse_tree: Arc<Tree>,
    lid_cache: LidPidCache,
    hot_lid_cache: LidPidHotCache,
}

impl LogicalStore {
    pub(crate) fn new(
        store: Arc<Store>,
        mapping_tree: Arc<Tree>,
        reverse_tree: Arc<Tree>,
        options: &OpenOptions,
    ) -> Self {
        Self {
            store,
            mapping_tree,
            reverse_tree,
            lid_cache: LidPidCache::new(options.lid_pid_cache_capacity),
            hot_lid_cache: LidPidHotCache::new(options.lid_pid_hot_cache_capacity),
        }
    }

    pub(crate) fn clear_lid_cache(&self) {
        self.lid_cache.clear();
        self.hot_lid_cache.clear();
    }

    fn resolve_pid_with_mode(&self, lid: Lid, _mode: CacheMode) -> Result<Pid> {
        if let Some(pid) = self.hot_lid_cache.get(lid) {
            return Ok(pid);
        }
        if let Some(pid) = self.lid_cache.get(lid) {
            self.hot_lid_cache.put(lid, pid);
            return Ok(pid);
        }

        let key = encode_u32_key(lid);
        let value = self.mapping_tree.get(&key)?;
        let pid = decode_u32_key(&value)?;
        self.lid_cache.put(lid, pid);
        self.hot_lid_cache.put(lid, pid);
        Ok(pid)
    }

    fn resolve_pid(&self, lid: Lid) -> Result<Pid> {
        self.resolve_pid_with_mode(lid, CacheMode::Default)
    }

    fn map_lids_with_mode(&self, lids: &[Lid], mode: CacheMode) -> Result<Vec<Pid>> {
        let mut pids = Vec::with_capacity(lids.len());
        for &lid in lids {
            pids.push(self.resolve_pid_with_mode(lid, mode)?);
        }
        Ok(pids)
    }

    fn map_lids(&self, lids: &[Lid]) -> Result<Vec<Pid>> {
        self.map_lids_with_mode(lids, CacheMode::Default)
    }
}

impl PageStore for Store {
    fn alloc_pages(&self, nr_pages: u32, alloc: &mut HashSet<u32>) -> Result<Vec<Lid>> {
        let pages = self.alloc_pages(nr_pages)?;
        for &pid in &pages {
            alloc.insert(pid);
        }
        Ok(pages)
    }

    fn schedule_free(&self, lid: Lid, freed: &mut Vec<(u32, u32)>) -> Result<()> {
        if lid == 0 {
            return Ok(());
        }
        freed.push((lid, 1));
        Ok(())
    }

    fn free_pages_immediate(&self, page_id: u32, nr_pages: u32) -> Result<()> {
        self.free_pages(page_id, nr_pages)
    }

    fn load_node(&self, lid: Lid) -> Result<Arc<Node>> {
        self.load_node(lid)
    }

    fn load_page(&self, lid: Lid) -> Result<Vec<u8>> {
        self.load_page(lid)
    }

    fn load_data(&self, lids: &[Lid], len: usize) -> Result<Vec<u8>> {
        self.load_data(lids, len)
    }

    fn read_data(&self, lids: &[Lid], buf: &mut [u8]) -> Result<()> {
        self.read_data(lids, buf)
    }

    fn write_data(&self, lids: &[Lid], data: &[u8]) -> Result<()> {
        self.write_data(lids, data)
    }

    fn write_page(&self, lid: Lid, data: &[u8]) -> Result<()> {
        self.write_page(lid, data)
    }

    fn cached_node_is_leaf(&self, lid: Lid) -> Option<bool> {
        Store::cached_node_is_leaf(self, lid)
    }

    fn cache_node(&self, lid: Lid, node: Arc<Node>) {
        Store::cache_node(self, lid, node);
    }
}

impl PageStore for LogicalStore {
    fn alloc_pages(&self, nr_pages: u32, alloc: &mut HashSet<u32>) -> Result<Vec<Lid>> {
        if nr_pages == 0 {
            return Ok(Vec::new());
        }

        let pids = self.store.alloc_pages(nr_pages)?;
        for &pid in &pids {
            alloc.insert(pid);
        }

        let lids = self.store.alloc_lids(nr_pages)?;

        for (lid, pid) in lids.iter().zip(pids.iter()) {
            let lid_key = encode_u32_key(*lid);
            let pid_value = encode_u32_key(*pid);
            let pid_key = encode_u32_key(*pid);
            let lid_value = encode_u32_key(*lid);
            self.mapping_tree.put(&lid_key, &pid_value)?;
            self.reverse_tree.put(&pid_key, &lid_value)?;
            self.lid_cache.put(*lid, *pid);
            self.hot_lid_cache.put(*lid, *pid);
        }

        Ok(lids)
    }

    fn schedule_free(&self, lid: Lid, freed: &mut Vec<(u32, u32)>) -> Result<()> {
        if lid == 0 {
            return Ok(());
        }
        let pid = self.resolve_pid(lid)?;
        let lid_key = encode_u32_key(lid);
        let pid_key = encode_u32_key(pid);
        self.mapping_tree.del(&lid_key)?;
        self.reverse_tree.del(&pid_key)?;
        self.lid_cache.invalidate(lid);
        self.hot_lid_cache.invalidate(lid);
        freed.push((pid, 1));
        Ok(())
    }

    fn free_pages_immediate(&self, page_id: u32, nr_pages: u32) -> Result<()> {
        self.store.free_pages(page_id, nr_pages)
    }

    fn load_node(&self, lid: Lid) -> Result<Arc<Node>> {
        let pid = self.resolve_pid(lid)?;
        self.store.load_node(pid)
    }

    fn load_node_with_mode(&self, lid: Lid, mode: CacheMode) -> Result<Arc<Node>> {
        match mode {
            CacheMode::Default => self.load_node(lid),
            CacheMode::ByPass => {
                let raw = self.load_page_with_mode(lid, mode)?;
                Ok(Arc::new(Node::from_raw(raw)?))
            }
        }
    }

    fn load_page(&self, lid: Lid) -> Result<Vec<u8>> {
        let pid = self.resolve_pid(lid)?;
        self.store.load_page(pid)
    }

    fn load_page_with_mode(&self, lid: Lid, mode: CacheMode) -> Result<Vec<u8>> {
        let pid = self.resolve_pid_with_mode(lid, mode)?;
        self.store.load_page(pid)
    }

    fn load_data(&self, lids: &[Lid], len: usize) -> Result<Vec<u8>> {
        let pids = self.map_lids(lids)?;
        self.store.load_data(&pids, len)
    }

    fn load_data_with_mode(&self, lids: &[Lid], len: usize, mode: CacheMode) -> Result<Vec<u8>> {
        let pids = self.map_lids_with_mode(lids, mode)?;
        self.store.load_data(&pids, len)
    }

    fn read_data(&self, lids: &[Lid], buf: &mut [u8]) -> Result<()> {
        let pids = self.map_lids(lids)?;
        self.store.read_data(&pids, buf)
    }

    fn read_data_with_mode(&self, lids: &[Lid], buf: &mut [u8], mode: CacheMode) -> Result<()> {
        let pids = self.map_lids_with_mode(lids, mode)?;
        self.store.read_data(&pids, buf)
    }

    fn write_data(&self, lids: &[Lid], data: &[u8]) -> Result<()> {
        let pids = self.map_lids(lids)?;
        self.store.write_data(&pids, data)
    }

    fn write_page(&self, lid: Lid, data: &[u8]) -> Result<()> {
        let pid = self.resolve_pid(lid)?;
        self.store.write_page(pid, data)
    }

    fn cached_node_is_leaf(&self, lid: Lid) -> Option<bool> {
        let pid = self
            .hot_lid_cache
            .get(lid)
            .or_else(|| self.lid_cache.get(lid))?;
        self.store.cached_node_is_leaf(pid)
    }

    fn cache_node(&self, lid: Lid, node: Arc<Node>) {
        if let Ok(pid) = self.resolve_pid_with_mode(lid, CacheMode::ByPass) {
            self.lid_cache.put(lid, pid);
            self.hot_lid_cache.put(lid, pid);
            self.store.cache_node(pid, node);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::Node;
    use parking_lot::RwLock;
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use tempfile::TempDir;

    struct NoopPageStore;

    impl PageStore for NoopPageStore {
        fn alloc_pages(&self, _nr_pages: u32, _alloc: &mut HashSet<u32>) -> Result<Vec<Lid>> {
            Err(Error::Internal)
        }

        fn schedule_free(&self, _lid: Lid, _freed: &mut Vec<(u32, u32)>) -> Result<()> {
            Err(Error::Internal)
        }

        fn free_pages_immediate(&self, _page_id: u32, _nr_pages: u32) -> Result<()> {
            Err(Error::Internal)
        }

        fn load_node(&self, _lid: Lid) -> Result<Arc<Node>> {
            Err(Error::Internal)
        }

        fn load_page(&self, _lid: Lid) -> Result<Vec<u8>> {
            Err(Error::Internal)
        }

        fn load_data(&self, _lids: &[Lid], _len: usize) -> Result<Vec<u8>> {
            Err(Error::Internal)
        }

        fn read_data(&self, _lids: &[Lid], _buf: &mut [u8]) -> Result<()> {
            Err(Error::Internal)
        }

        fn write_data(&self, _lids: &[Lid], _data: &[u8]) -> Result<()> {
            Err(Error::Internal)
        }

        fn write_page(&self, _lid: Lid, _data: &[u8]) -> Result<()> {
            Err(Error::Internal)
        }
    }

    struct MappingTrackingStore {
        root: Arc<Node>,
        default_loads: AtomicUsize,
        bypass_loads: AtomicUsize,
    }

    impl MappingTrackingStore {
        fn new(root: Arc<Node>) -> Self {
            Self {
                root,
                default_loads: AtomicUsize::new(0),
                bypass_loads: AtomicUsize::new(0),
            }
        }
    }

    impl PageStore for MappingTrackingStore {
        fn alloc_pages(&self, _nr_pages: u32, _alloc: &mut HashSet<u32>) -> Result<Vec<Lid>> {
            Err(Error::Internal)
        }

        fn schedule_free(&self, _lid: Lid, _freed: &mut Vec<(u32, u32)>) -> Result<()> {
            Err(Error::Internal)
        }

        fn free_pages_immediate(&self, _page_id: u32, _nr_pages: u32) -> Result<()> {
            Err(Error::Internal)
        }

        fn load_node(&self, lid: Lid) -> Result<Arc<Node>> {
            if lid != 1 {
                return Err(Error::NotFound);
            }
            self.default_loads.fetch_add(1, AtomicOrdering::Relaxed);
            Ok(self.root.clone())
        }

        fn load_page(&self, _lid: Lid) -> Result<Vec<u8>> {
            Err(Error::Internal)
        }

        fn load_data(&self, _lids: &[Lid], _len: usize) -> Result<Vec<u8>> {
            Err(Error::Internal)
        }

        fn read_data(&self, _lids: &[Lid], _buf: &mut [u8]) -> Result<()> {
            Err(Error::Internal)
        }

        fn write_data(&self, _lids: &[Lid], _data: &[u8]) -> Result<()> {
            Err(Error::Internal)
        }

        fn write_page(&self, _lid: Lid, _data: &[u8]) -> Result<()> {
            Err(Error::Internal)
        }

        fn load_node_with_mode(&self, lid: Lid, mode: CacheMode) -> Result<Arc<Node>> {
            match mode {
                CacheMode::Default => self.load_node(lid),
                CacheMode::ByPass => {
                    if lid != 1 {
                        return Err(Error::NotFound);
                    }
                    self.bypass_loads.fetch_add(1, AtomicOrdering::Relaxed);
                    Ok(self.root.clone())
                }
            }
        }
    }

    #[test]
    fn lid_pid_cache_small_capacity_uses_only_nonzero_shards() {
        for capacity in [1usize, 17, 63] {
            let cache = LidPidCache::new(capacity);
            assert_eq!(cache.shards.len(), capacity);
            assert_eq!(
                cache
                    .shards
                    .iter()
                    .map(|shard| shard.lock().capacity)
                    .sum::<usize>(),
                capacity
            );
            for lid in [0_u32, 1, 63, 64, 127, 4095] {
                assert!(
                    cache.shard(lid).lock().capacity > 0,
                    "capacity={capacity} should give every active shard at least one slot"
                );
            }
        }
    }

    #[test]
    fn bypass_pid_resolution_keeps_mapping_tree_on_cached_path() {
        let mut mapping_root = Node::new_leaf();
        mapping_root
            .put(
                &NoopPageStore,
                &encode_u32_key(7),
                &encode_u32_key(99),
                &mut Vec::new(),
                &mut HashSet::new(),
            )
            .expect("seed mapping tree");

        let mapping_store = Arc::new(MappingTrackingStore::new(Arc::new(mapping_root)));
        let mapping_tree = Arc::new(
            Tree::open(
                mapping_store.clone(),
                Arc::new(RwLock::new(1)),
                Arc::new(RwLock::new(Vec::new())),
                Arc::new(RwLock::new(HashSet::new())),
            )
            .expect("open mapping tree"),
        );
        let reverse_tree = Arc::new(
            Tree::open(
                mapping_store.clone(),
                Arc::new(RwLock::new(0)),
                Arc::new(RwLock::new(Vec::new())),
                Arc::new(RwLock::new(HashSet::new())),
            )
            .expect("open reverse tree"),
        );
        let temp_dir = TempDir::new().expect("temp dir");
        let logical = LogicalStore::new(
            Arc::new(
                Store::open(
                    temp_dir.path().join("mapping-cache.db"),
                    &OpenOptions::default(),
                )
                .expect("store"),
            ),
            mapping_tree,
            reverse_tree,
            &OpenOptions::default(),
        );

        let pid = logical
            .resolve_pid_with_mode(7, CacheMode::ByPass)
            .expect("resolve pid");
        assert_eq!(pid, 99);
        assert_eq!(
            mapping_store.default_loads.load(AtomicOrdering::Relaxed),
            1,
            "mapping tree lookup should stay on the normal cached path"
        );
        assert_eq!(
            mapping_store.bypass_loads.load(AtomicOrdering::Relaxed),
            0,
            "mapping tree lookup must not bypass its own node cache"
        );
    }
}
