use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::node::Node;
use crate::store::Store;
use crate::{Error, Result, Tree};
use parking_lot::Mutex;

pub type Lid = u32;
pub type Pid = u32;

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
const LID_PID_CACHE_CAPACITY: usize = 8192;

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
        let shard_cap = std::cmp::max(1, capacity / LID_PID_CACHE_SHARDS);
        let mut shards = Vec::with_capacity(LID_PID_CACHE_SHARDS);
        for _ in 0..LID_PID_CACHE_SHARDS {
            shards.push(Mutex::new(LidPidCacheShard::new(shard_cap)));
        }
        Self { shards }
    }

    fn shard(&self, lid: Lid) -> &Mutex<LidPidCacheShard> {
        let idx = (lid as usize) % LID_PID_CACHE_SHARDS;
        &self.shards[idx]
    }

    fn get(&self, lid: Lid) -> Option<Pid> {
        self.shard(lid).lock().get(lid)
    }

    fn put(&self, lid: Lid, pid: Pid) {
        self.shard(lid).lock().put(lid, pid)
    }

    fn invalidate(&self, lid: Lid) {
        self.shard(lid).lock().invalidate(lid)
    }

    fn clear(&self) {
        for shard in &self.shards {
            shard.lock().clear();
        }
    }
}

pub trait PageStore: Send + Sync {
    fn alloc_pages(&self, nr_pages: u32, alloc: &mut HashSet<u32>) -> Result<Vec<Lid>>;

    fn alloc_page(&self, alloc: &mut HashSet<u32>) -> Result<Lid> {
        let pages = self.alloc_pages(1, alloc)?;
        Ok(pages[0])
    }

    fn schedule_free(&self, lid: Lid, freed: &mut Vec<(u32, u32)>) -> Result<()>;
    fn free_pages_immediate(&self, page_id: u32, nr_pages: u32) -> Result<()>;
    fn unfree_pages_immediate(&self, page_id: u32, nr_pages: u32) -> Result<()>;
    fn load_node(&self, lid: Lid) -> Result<Arc<Node>>;
    fn load_page(&self, lid: Lid) -> Result<Vec<u8>>;
    fn load_data(&self, lids: &[Lid], len: usize) -> Result<Vec<u8>>;
    fn read_data(&self, lids: &[Lid], buf: &mut [u8]) -> Result<()>;
    fn write_data(&self, lids: &[Lid], data: &[u8]) -> Result<()>;
    fn write_page(&self, lid: Lid, data: &[u8]) -> Result<()>;
}

pub struct LogicalStore {
    store: Arc<Store>,
    mapping_tree: Arc<Tree>,
    reverse_tree: Arc<Tree>,
    lid_cache: LidPidCache,
    lid_cache_seq: AtomicU64,
}

impl LogicalStore {
    pub fn new(store: Arc<Store>, mapping_tree: Arc<Tree>, reverse_tree: Arc<Tree>) -> Self {
        let (seq, _) = store.shared_snapshot();
        Self {
            store,
            mapping_tree,
            reverse_tree,
            lid_cache: LidPidCache::new(LID_PID_CACHE_CAPACITY),
            lid_cache_seq: AtomicU64::new(seq),
        }
    }

    pub(crate) fn clear_lid_cache(&self) {
        let (seq, _) = self.store.shared_snapshot();
        self.lid_cache.clear();
        self.lid_cache_seq.store(seq, Ordering::Release);
    }

    fn refresh_lid_cache(&self) {
        let (seq, _) = self.store.shared_snapshot();
        let cached = self.lid_cache_seq.load(Ordering::Acquire);
        if cached != seq {
            self.lid_cache.clear();
            self.lid_cache_seq.store(seq, Ordering::Release);
        }
    }

    fn resolve_pid(&self, lid: Lid) -> Result<Pid> {
        self.refresh_lid_cache();
        if let Some(pid) = self.lid_cache.get(lid) {
            return Ok(pid);
        }
        let key = encode_u32_key(lid);
        let value = self.mapping_tree.get(&key)?;
        let pid = decode_u32_key(&value)?;
        self.lid_cache.put(lid, pid);
        Ok(pid)
    }

    fn map_lids(&self, lids: &[Lid]) -> Result<Vec<Pid>> {
        let mut pids = Vec::with_capacity(lids.len());
        for &lid in lids {
            pids.push(self.resolve_pid(lid)?);
        }
        Ok(pids)
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

    fn unfree_pages_immediate(&self, page_id: u32, nr_pages: u32) -> Result<()> {
        self.unfree_pages(page_id, nr_pages)
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
        freed.push((pid, 1));
        Ok(())
    }

    fn free_pages_immediate(&self, page_id: u32, nr_pages: u32) -> Result<()> {
        self.store.free_pages(page_id, nr_pages)
    }

    fn unfree_pages_immediate(&self, page_id: u32, nr_pages: u32) -> Result<()> {
        self.store.unfree_pages(page_id, nr_pages)
    }

    fn load_node(&self, lid: Lid) -> Result<Arc<Node>> {
        let pid = self.resolve_pid(lid)?;
        self.store.load_node(pid)
    }

    fn load_page(&self, lid: Lid) -> Result<Vec<u8>> {
        let pid = self.resolve_pid(lid)?;
        self.store.load_page(pid)
    }

    fn load_data(&self, lids: &[Lid], len: usize) -> Result<Vec<u8>> {
        let pids = self.map_lids(lids)?;
        self.store.load_data(&pids, len)
    }

    fn read_data(&self, lids: &[Lid], buf: &mut [u8]) -> Result<()> {
        let pids = self.map_lids(lids)?;
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
}
