use parking_lot::Mutex;
use std::{
    collections::HashMap,
    collections::HashSet,
    collections::hash_map::DefaultHasher,
    fs::{File, OpenOptions},
    hash::{Hash, Hasher},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{
    Error, FreeNode, MetaNode, Result,
    node::{Node, PAGE_SIZE},
};

#[cfg(unix)]
fn fsync_parent_dir(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        let parent = if parent.as_os_str().is_empty() {
            Path::new(".")
        } else {
            parent
        };
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn fsync_parent_dir(_path: &Path) -> io::Result<()> {
    Ok(())
}

/// Abstract Trait for Positional I/O to support Linux, Windows, macOS, FreeBSD.
pub(crate) trait FileIO {
    fn pread_exact(&self, buf: &mut [u8], offset: u64) -> io::Result<()>;
    fn pwrite_all(&self, buf: &[u8], offset: u64) -> io::Result<()>;
    fn psync_all(&self) -> io::Result<()>;
}

impl FileIO for File {
    #[cfg(unix)]
    fn pread_exact(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        use std::os::unix::fs::FileExt;
        self.read_exact_at(buf, offset)
    }

    #[cfg(unix)]
    fn pwrite_all(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        use std::os::unix::fs::FileExt;
        self.write_all_at(buf, offset)
    }

    #[cfg(windows)]
    fn pread_exact(&self, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
        use std::os::windows::fs::FileExt;
        while !buf.is_empty() {
            match self.seek_read(buf, offset) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "failed to fill whole buffer",
                    ));
                }
                Ok(n) => {
                    let tmp = buf;
                    buf = &mut tmp[n..];
                    offset += n as u64;
                }
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    #[cfg(windows)]
    fn pwrite_all(&self, mut buf: &[u8], mut offset: u64) -> io::Result<()> {
        use std::os::windows::fs::FileExt;
        while !buf.is_empty() {
            match self.seek_write(buf, offset) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "failed to write whole buffer",
                    ));
                }
                Ok(n) => {
                    buf = &buf[n..];
                    offset += n as u64;
                }
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    fn psync_all(&self) -> io::Result<()> {
        self.sync_all()
    }
}
const FREE_NODE_SIZE: usize = std::mem::size_of::<FreeNode>();

#[repr(C)]
#[derive(Clone, Copy)]
struct PendingLogHeader {
    seq: u64,
    nr_freed: u32,
    nr_alloc: u32,
    checksum: u32,
    _padding: u32,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct PendingEntry {
    page_id: u64,
    nr_pages: u32,
    _padding: u32,
}

pub struct Store {
    file: Arc<Mutex<File>>,
    sb: Mutex<MetaNode>,
    pending_path: PathBuf,
    /// contention is concentrated at the B+ Tree's root
    cache: NodeCache,
}

impl Store {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();

        let (file, sb) = if !path.exists() {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(path)?;

            let mut sb = MetaNode::new();

            file.pwrite_all(&sb.as_page_slice(), 0)?;

            sb.seq += 1;
            sb.update_checksum();
            file.pwrite_all(&sb.as_page_slice(), PAGE_SIZE as u64)?;

            file.psync_all()?;
            (file, sb)
        } else {
            let file = OpenOptions::new().read(true).write(true).open(path)?;

            let mut buf0 = vec![0u8; PAGE_SIZE];
            let mut buf1 = vec![0u8; PAGE_SIZE];

            let r0 = file.pread_exact(&mut buf0, 0);
            let r1 = file.pread_exact(&mut buf1, PAGE_SIZE as u64);

            let sb0 = if r0.is_ok() {
                let s = MetaNode::from_slice(&buf0);
                if s.validate().is_ok() { Some(s) } else { None }
            } else {
                None
            };

            let sb1 = if r1.is_ok() {
                let s = MetaNode::from_slice(&buf1);
                if s.validate().is_ok() { Some(s) } else { None }
            } else {
                None
            };

            let sb = match (sb0, sb1) {
                (Some(s0), Some(s1)) => {
                    if s0.seq >= s1.seq {
                        s0
                    } else {
                        s1
                    }
                }
                (Some(s0), None) => s0,
                (None, Some(s1)) => s1,
                (None, None) => return Err(Error::Corruption),
            };
            (file, sb)
        };

        let pending_path = path.with_extension("pending");
        let store = Self {
            file: Arc::new(Mutex::new(file)),
            sb: Mutex::new(sb),
            pending_path,
            cache: NodeCache::new(4096),
        };

        store.recover_pending()?;
        Ok(store)
    }

    fn recover_pending(&self) -> Result<()> {
        if !self.pending_path.exists() || std::fs::metadata(&self.pending_path)?.len() == 0 {
            return Ok(());
        }

        let mut f = File::open(&self.pending_path)?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;

        let header_size = std::mem::size_of::<PendingLogHeader>();
        if buf.len() < header_size {
            let _ = self.clear_pending_log();
            return Ok(());
        }

        let header = unsafe { std::ptr::read_unaligned(buf.as_ptr() as *const PendingLogHeader) };
        let data_part = &buf[header_size..];

        if crc32c::crc32c(data_part) != header.checksum {
            let _ = self.clear_pending_log();
            return Ok(());
        }

        let entry_size = std::mem::size_of::<PendingEntry>();
        let (current_seq, next_id) = {
            let sb = self.sb.lock();
            (sb.seq, sb.next_page_id)
        };

        if header.seq == current_seq {
            // case 1: Redo-Free. SB was updated but old pages were not freed
            let mut pos = 0;
            for _ in 0..header.nr_freed {
                if pos + entry_size > data_part.len() {
                    break;
                }
                let entry = unsafe {
                    std::ptr::read_unaligned(data_part.as_ptr().add(pos) as *const PendingEntry)
                };
                self.free_pages(entry.page_id, entry.nr_pages)?;
                pos += entry_size;
            }
            let root = self.sb.lock().root_current;
            self.update_root(root)?;
        } else if header.seq == current_seq + 1 {
            // case 2: Undo-Alloc. transaction failed before SB update
            let mut pos = (header.nr_freed as usize) * entry_size;
            for _ in 0..header.nr_alloc {
                if pos + entry_size > data_part.len() {
                    break;
                }
                let entry = unsafe {
                    std::ptr::read_unaligned(data_part.as_ptr().add(pos) as *const PendingEntry)
                };
                if entry.page_id >= next_id {
                    self.free_pages(entry.page_id, entry.nr_pages)?;
                }
                pos += entry_size;
            }
            let root = self.sb.lock().root_current;
            self.update_root(root)?;
        } else {
            // stale/future/malformed log: ignore
        }

        let _ = self.clear_pending_log();
        Ok(())
    }

    pub fn write_pending_log(
        &self,
        seq: u64,
        freed: &[(u64, u32)],
        alloc: &HashSet<u64>,
    ) -> Result<()> {
        let temp_path = self.pending_path.with_extension("tmp");
        {
            let mut f = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&temp_path)?;

            let mut entries = Vec::with_capacity(freed.len() + alloc.len());
            for &(pid, nr) in freed {
                entries.push(PendingEntry {
                    page_id: pid,
                    nr_pages: nr,
                    _padding: 0,
                });
            }
            for &pid in alloc.iter() {
                entries.push(PendingEntry {
                    page_id: pid,
                    nr_pages: 1,
                    _padding: 0,
                });
            }

            let data_bytes = unsafe {
                std::slice::from_raw_parts(
                    entries.as_ptr() as *const u8,
                    entries.len() * std::mem::size_of::<PendingEntry>(),
                )
            };

            let header = PendingLogHeader {
                seq,
                nr_freed: freed.len() as u32,
                nr_alloc: alloc.len() as u32,
                checksum: crc32c::crc32c(data_bytes),
                _padding: 0,
            };

            let header_bytes = unsafe {
                std::slice::from_raw_parts(
                    &header as *const PendingLogHeader as *const u8,
                    std::mem::size_of::<PendingLogHeader>(),
                )
            };

            f.write_all(header_bytes)?;
            f.write_all(data_bytes)?;
            f.sync_all()?;
        }

        std::fs::rename(&temp_path, &self.pending_path)?;
        // After rename, we still need to fsync the parent dir to ensure the rename is permanent
        fsync_parent_dir(&self.pending_path)?;
        Ok(())
    }

    pub fn clear_pending_log(&self) -> Result<()> {
        if self.pending_path.exists() {
            let f = OpenOptions::new().write(true).open(&self.pending_path)?;
            f.set_len(0)?;
            f.sync_all()?;
        }
        Ok(())
    }

    pub fn get_seq(&self) -> u64 {
        self.sb.lock().seq
    }

    pub fn alloc_page(&self) -> Result<u64> {
        let pages = self.alloc_pages(1)?;
        Ok(pages[0])
    }

    pub fn alloc_pages(&self, nr_pages: u32) -> Result<Vec<u64>> {
        let mut sb_guard = self.sb.lock();
        let f = self.file.lock();

        let mut temp_sb = *sb_guard;
        let mut gathered = Vec::new();

        // 1. Try to allocate from the cached head chunk
        if temp_sb.nr_free >= nr_pages as u64 {
            let start_id = (temp_sb.free_list_head + temp_sb.nr_free) - nr_pages as u64;

            if start_id == temp_sb.free_list_head {
                // Allocating the head page itself. Read the next pointer first.
                let mut buf = [0u8; FREE_NODE_SIZE];
                f.pread_exact(&mut buf, temp_sb.free_list_head * PAGE_SIZE as u64)?;
                let free_node = FreeNode::from_slice(&buf);

                temp_sb.free_list_head = free_node.next;
                if temp_sb.free_list_head != 0 {
                    let mut next_buf = [0u8; FREE_NODE_SIZE];
                    f.pread_exact(&mut next_buf, temp_sb.free_list_head * PAGE_SIZE as u64)?;
                    let next_node = FreeNode::from_slice(&next_buf);
                    if !next_node.validate() {
                        return Err(Error::Corruption);
                    }
                    temp_sb.nr_free = next_node.nr_pages as u64;
                } else {
                    temp_sb.nr_free = 0;
                }
            } else {
                temp_sb.nr_free -= nr_pages as u64;
            }

            for i in 0..nr_pages {
                self.cache.invalidate(start_id + i as u64);
            }
            *sb_guard = temp_sb;
            return Ok((0..nr_pages).map(|i| start_id + i as u64).collect());
        }

        // 2. Gather discrete pages from free list
        while gathered.len() < nr_pages as usize && temp_sb.free_list_head != 0 {
            let to_take =
                std::cmp::min(nr_pages as usize - gathered.len(), temp_sb.nr_free as usize);
            let start_id = (temp_sb.free_list_head + temp_sb.nr_free) - to_take as u64;

            for i in 0..to_take {
                let pid = start_id + i as u64;
                self.cache.invalidate(pid);
                gathered.push(pid);
            }
            temp_sb.nr_free -= to_take as u64;

            if temp_sb.nr_free == 0 {
                let mut buf = [0u8; FREE_NODE_SIZE];
                f.pread_exact(&mut buf, temp_sb.free_list_head * PAGE_SIZE as u64)?;
                let h_node = FreeNode::from_slice(&buf);
                if !h_node.validate() {
                    return Err(Error::Corruption);
                }
                temp_sb.free_list_head = h_node.next;
                if temp_sb.free_list_head != 0 {
                    let mut n_buf = [0u8; FREE_NODE_SIZE];
                    f.pread_exact(&mut n_buf, temp_sb.free_list_head * PAGE_SIZE as u64)?;
                    let n_node = FreeNode::from_slice(&n_buf);
                    if !n_node.validate() {
                        return Err(Error::Corruption);
                    }
                    temp_sb.nr_free = n_node.nr_pages as u64;
                }
            }
        }

        // 3. Allocate from end of file for remaining
        if gathered.len() < nr_pages as usize {
            let needed = nr_pages as usize - gathered.len();
            let start_id = temp_sb.next_page_id;
            temp_sb.next_page_id += needed as u64;
            for i in 0..needed {
                let pid = start_id + i as u64;
                self.cache.invalidate(pid);
                gathered.push(pid);
            }
        }

        *sb_guard = temp_sb;
        Ok(gathered)
    }

    pub fn free_pages(&self, page_id: u64, nr_pages: u32) -> Result<()> {
        if page_id == 0 {
            return Ok(());
        }

        for i in 0..nr_pages {
            self.cache.invalidate(page_id + i as u64);
        }

        let mut sb = self.sb.lock();
        let f = self.file.lock();

        if sb.free_list_head == page_id {
            return Ok(());
        }

        // Ensure cached head's state is persisted before moving head
        if sb.free_list_head != 0 {
            let mut buf = vec![0u8; FREE_NODE_SIZE];
            if f.pread_exact(&mut buf, sb.free_list_head * PAGE_SIZE as u64)
                .is_ok()
            {
                let mut head_node = FreeNode::from_slice(&buf);
                if head_node.validate() && head_node.nr_pages as u64 != sb.nr_free {
                    head_node.nr_pages = sb.nr_free as u32;
                    head_node.update_checksum();
                    let _ = f.pwrite_all(
                        unsafe {
                            std::slice::from_raw_parts(
                                (&head_node as *const FreeNode) as *const u8,
                                FREE_NODE_SIZE,
                            )
                        },
                        sb.free_list_head * PAGE_SIZE as u64,
                    );
                }
            }
        }

        let mut new_node = FreeNode {
            next: sb.free_list_head,
            nr_pages,
            checksum: 0,
        };
        new_node.update_checksum();
        f.pwrite_all(
            unsafe {
                std::slice::from_raw_parts(
                    (&new_node as *const FreeNode) as *const u8,
                    FREE_NODE_SIZE,
                )
            },
            page_id * PAGE_SIZE as u64,
        )?;

        sb.free_list_head = page_id;
        sb.nr_free = nr_pages as u64;

        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        self.file.lock().psync_all().map_err(|_| Error::IoError)
    }

    pub fn load_node(&self, page_id: u64) -> Result<Arc<Node>> {
        if let Some(node) = self.cache.get(page_id) {
            return Ok(node);
        }

        let raw = self.load_page(page_id)?;
        let node = Arc::new(Node::from_raw(raw)?);

        self.cache.put(page_id, node.clone());
        Ok(node)
    }

    pub fn load_page(&self, page_id: u64) -> Result<Vec<u8>> {
        self.load_data(&[page_id], PAGE_SIZE)
    }

    pub fn load_data(&self, pages: &[u64], len: usize) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; len];
        self.read_data(pages, &mut buf)?;
        Ok(buf)
    }

    pub fn read_data(&self, pages: &[u64], buf: &mut [u8]) -> Result<()> {
        let f = self.file.lock();
        for (i, &pid) in pages.iter().enumerate() {
            let offset = pid * PAGE_SIZE as u64;
            let start = i * PAGE_SIZE;
            if start >= buf.len() {
                break;
            }
            let end = std::cmp::min(start + PAGE_SIZE, buf.len());
            f.pread_exact(&mut buf[start..end], offset)?;
        }
        Ok(())
    }

    pub fn write_data(&self, pages: &[u64], data: &[u8]) -> Result<()> {
        let f = self.file.lock();
        for (i, &pid) in pages.iter().enumerate() {
            let offset = pid * PAGE_SIZE as u64;
            let start = i * PAGE_SIZE;
            if start >= data.len() {
                break;
            }
            let end = std::cmp::min(start + PAGE_SIZE, data.len());
            f.pwrite_all(&data[start..end], offset)?;
        }
        Ok(())
    }

    pub fn read_page(&self, page_id: u64, buf: &mut [u8]) -> Result<()> {
        self.read_data(&[page_id], buf)
    }

    pub fn write_page(&self, page_id: u64, data: &[u8]) -> Result<()> {
        self.write_data(&[page_id], data)
    }

    pub fn get_root_id(&self) -> u64 {
        self.sb.lock().root_current
    }

    pub fn refresh_sb(&self) -> Result<u64> {
        let file = self.file.lock();
        let mut buf0 = vec![0u8; PAGE_SIZE];
        let mut buf1 = vec![0u8; PAGE_SIZE];

        let r0 = file.pread_exact(&mut buf0, 0);
        let r1 = file.pread_exact(&mut buf1, PAGE_SIZE as u64);
        drop(file);

        let sb0 = if r0.is_ok() {
            let s = MetaNode::from_slice(&buf0);
            if s.validate().is_ok() { Some(s) } else { None }
        } else {
            None
        };

        let sb1 = if r1.is_ok() {
            let s = MetaNode::from_slice(&buf1);
            if s.validate().is_ok() { Some(s) } else { None }
        } else {
            None
        };

        let sb = match (sb0, sb1) {
            (Some(s0), Some(s1)) => {
                if s0.seq >= s1.seq {
                    s0
                } else {
                    s1
                }
            }
            (Some(s0), None) => s0,
            (None, Some(s1)) => s1,
            (None, None) => return Err(Error::Corruption),
        };

        let mut current_sb = self.sb.lock();
        if sb.seq > current_sb.seq {
            *current_sb = sb;
            Ok(sb.root_current)
        } else {
            Ok(current_sb.root_current)
        }
    }

    pub fn clear_cache(&self) {
        for shard in &self.cache.shards {
            let mut guard = shard.lock();
            guard.entries.iter_mut().for_each(|e| *e = None);
            guard.map.clear();
        }
    }

    pub(crate) fn update_root(&self, root_id: u64) -> Result<()> {
        let mut sb = self.sb.lock();

        sb.root_current = root_id;
        sb.seq += 1;
        sb.update_checksum();

        let write_offset = if sb.seq.is_multiple_of(2) {
            PAGE_SIZE as u64
        } else {
            0
        };

        let file = self.file.lock();
        file.pwrite_all(&sb.as_page_slice(), write_offset)?;
        file.psync_all()?;

        Ok(())
    }
}

struct CacheEntry {
    page_id: u64,
    node: Arc<Node>,
    usage: bool,
}

struct NodeCacheShard {
    entries: Vec<Option<CacheEntry>>,
    map: HashMap<u64, usize>,
    hand: usize,
    capacity: usize,
}

impl NodeCacheShard {
    fn new(capacity: usize) -> Self {
        Self {
            entries: (0..capacity).map(|_| None).collect(),
            map: HashMap::with_capacity(capacity),
            hand: 0,
            capacity,
        }
    }

    fn get(&mut self, page_id: u64) -> Option<Arc<Node>> {
        if let Some(&idx) = self.map.get(&page_id)
            && let Some(entry) = &mut self.entries[idx]
        {
            entry.usage = true;
            return Some(entry.node.clone());
        }
        None
    }

    fn put(&mut self, page_id: u64, node: Arc<Node>) {
        if let Some(&idx) = self.map.get(&page_id)
            && let Some(entry) = &mut self.entries[idx]
        {
            entry.usage = true;
            entry.node = node;
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
                        self.map.remove(&entry.page_id);
                        true
                    }
                }
            };

            if evict {
                self.entries[self.hand] = Some(CacheEntry {
                    page_id,
                    node,
                    usage: true,
                });
                self.map.insert(page_id, self.hand);
                self.hand = (self.hand + 1) % self.capacity;
                return;
            }
            self.hand = (self.hand + 1) % self.capacity;
        }
    }

    fn invalidate(&mut self, page_id: u64) {
        if let Some(&idx) = self.map.get(&page_id) {
            self.entries[idx] = None;
            self.map.remove(&page_id);
        }
    }
}

const NUM_SHARDS: usize = 64;

pub struct NodeCache {
    shards: Vec<Mutex<NodeCacheShard>>,
}

impl NodeCache {
    fn new(capacity: usize) -> Self {
        let shard_cap = std::cmp::max(1, capacity / NUM_SHARDS);
        let mut shards = Vec::with_capacity(NUM_SHARDS);
        for _ in 0..NUM_SHARDS {
            shards.push(Mutex::new(NodeCacheShard::new(shard_cap)));
        }
        Self { shards }
    }

    fn get_shard(&self, page_id: u64) -> &Mutex<NodeCacheShard> {
        let mut hasher = DefaultHasher::new();
        page_id.hash(&mut hasher);
        let hash = hasher.finish();
        &self.shards[(hash as usize) % NUM_SHARDS]
    }

    pub fn get(&self, page_id: u64) -> Option<Arc<Node>> {
        self.get_shard(page_id).lock().get(page_id)
    }

    pub fn put(&self, page_id: u64, node: Arc<Node>) {
        self.get_shard(page_id).lock().put(page_id, node)
    }

    pub fn invalidate(&self, page_id: u64) {
        self.get_shard(page_id).lock().invalidate(page_id)
    }
}
