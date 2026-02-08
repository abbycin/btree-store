use parking_lot::Mutex;
use std::{
    collections::HashMap,
    collections::HashSet,
    collections::hash_map::DefaultHasher,
    fs::{File, OpenOptions},
    hash::{Hash, Hasher},
    io,
    path::{Path, PathBuf},
    sync::{
        Arc, OnceLock, Weak,
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
    },
};

use crate::{
    Error, FORMAT_VERSION, MAGIC, MetaNode, PageId, Result,
    node::{Node, PAGE_SIZE},
};

pub struct MetaSnapshot {
    pub catalog_root: PageId,
    pub mapping_root: PageId,
    pub reverse_root: PageId,
    pub seq: u64,
}

/// Abstract Trait for Positional I/O to support Linux, Windows, macOS, FreeBSD.
pub(crate) trait FileIO {
    fn pread_exact(&self, buf: &mut [u8], offset: u64) -> io::Result<()>;
    fn pwrite_all(&self, buf: &[u8], offset: u64) -> io::Result<()>;
    fn psync_all(&self) -> io::Result<()>;
    fn psync_data(&self) -> io::Result<()>;
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

    fn psync_data(&self) -> io::Result<()> {
        self.sync_data()
    }
}

struct SharedMeta {
    seq: AtomicU64,
    root: AtomicU32,
}

impl SharedMeta {
    fn new(seq: u64, root: PageId) -> Self {
        Self {
            seq: AtomicU64::new(seq),
            root: AtomicU32::new(root),
        }
    }

    fn update(&self, root: PageId, seq: u64) {
        self.root.store(root, Ordering::Release);
        self.seq.store(seq, Ordering::Release);
    }

    fn snapshot(&self) -> (u64, PageId) {
        loop {
            let seq1 = self.seq.load(Ordering::Acquire);
            let root = self.root.load(Ordering::Acquire);
            let seq2 = self.seq.load(Ordering::Acquire);
            if seq1 == seq2 {
                return (seq1, root);
            }
        }
    }
}

static SHARED_META_REGISTRY: OnceLock<Mutex<HashMap<PathBuf, Weak<SharedMeta>>>> = OnceLock::new();

fn shared_meta_registry() -> &'static Mutex<HashMap<PathBuf, Weak<SharedMeta>>> {
    SHARED_META_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

fn get_shared_meta(path: &Path, sb: &MetaNode) -> Arc<SharedMeta> {
    let key = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let registry = shared_meta_registry();
    let mut map = registry.lock();
    if let Some(entry) = map.get(&key)
        && let Some(shared) = entry.upgrade()
    {
        let current_seq = shared.seq.load(Ordering::Acquire);
        if sb.seq > current_seq {
            shared.update(sb.catalog_root, sb.seq);
        }
        return shared;
    }

    let shared = Arc::new(SharedMeta::new(sb.seq, sb.catalog_root));
    map.insert(key, Arc::downgrade(&shared));
    shared
}
#[repr(C)]
#[derive(Clone, Copy)]
struct FreeListHeader {
    next: PageId,
    count: u32,
    checksum: u32,
    _padding: u32,
}

impl FreeListHeader {
    fn from_slice(x: &[u8]) -> Self {
        unsafe { std::ptr::read_unaligned(x.as_ptr().cast::<Self>()) }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct FreeListEntry {
    page_id: PageId,
    nr_pages: u32,
}

impl FreeListEntry {
    fn from_slice(x: &[u8]) -> Self {
        unsafe { std::ptr::read_unaligned(x.as_ptr().cast::<Self>()) }
    }
}

#[derive(Clone, Copy)]
struct FreeExtent {
    page_id: PageId,
    nr_pages: u32,
}

impl FreeExtent {
    fn end(&self) -> u64 {
        self.page_id as u64 + self.nr_pages as u64
    }
}

const FREE_LIST_HEADER_SIZE: usize = std::mem::size_of::<FreeListHeader>();
const FREE_LIST_ENTRY_SIZE: usize = std::mem::size_of::<FreeListEntry>();
const FREE_LIST_ENTRIES_PER_PAGE: usize =
    (PAGE_SIZE - FREE_LIST_HEADER_SIZE) / FREE_LIST_ENTRY_SIZE;

#[repr(C)]
#[derive(Clone, Copy)]
struct MetaNodeV1 {
    magic: u64,
    version: u64,
    page_size: u64,
    root_current: u64,
    root_backup: u64,
    next_page_id: u64,
    free_list_head: u64,
    nr_free: u64,
    seq: u64,
    checksum: u64,
}

impl MetaNodeV1 {
    fn from_slice(x: &[u8]) -> Self {
        unsafe { std::ptr::read_unaligned(x.as_ptr().cast::<Self>()) }
    }

    fn calc_checksum(&self) -> u64 {
        let mut clone = *self;
        clone.checksum = 0;
        let s = unsafe {
            std::slice::from_raw_parts(
                (&clone as *const Self) as *const u8,
                std::mem::size_of::<Self>(),
            )
        };
        crc32c::crc32c(s) as u64
    }

    fn validate(&self) -> bool {
        if self.magic == 0 && self.seq == 0 {
            return false;
        }
        self.checksum == self.calc_checksum()
    }
}

fn is_valid_v1_meta(buf: &[u8]) -> bool {
    let v1 = MetaNodeV1::from_slice(buf);
    if v1.magic != MAGIC {
        return false;
    }
    if v1.version != 1 {
        return false;
    }
    v1.validate()
}

#[repr(C)]
#[derive(Clone, Copy)]
struct MetaNodeV2 {
    magic: u64,
    format_version: u32,
    _padding: u32,
    page_size: u64,
    root_current: u64,
    root_backup: u64,
    next_page_id: u64,
    freelist_root: u64,
    seq: u64,
    checksum: u64,
}

impl MetaNodeV2 {
    fn from_slice(x: &[u8]) -> Self {
        unsafe { std::ptr::read_unaligned(x.as_ptr().cast::<Self>()) }
    }

    fn calc_checksum(&self) -> u64 {
        let mut clone = *self;
        clone.checksum = 0;
        let s = unsafe {
            std::slice::from_raw_parts(
                (&clone as *const Self) as *const u8,
                std::mem::size_of::<Self>(),
            )
        };
        crc32c::crc32c(s) as u64
    }

    fn validate(&self) -> bool {
        if self.magic == 0 && self.seq == 0 {
            return false;
        }
        self.checksum == self.calc_checksum()
    }
}

fn is_valid_v2_meta(buf: &[u8]) -> bool {
    let v2 = MetaNodeV2::from_slice(buf);
    if v2.magic != MAGIC {
        return false;
    }
    if v2.format_version != 2 {
        return false;
    }
    v2.validate()
}

pub struct Store {
    file: File,
    sb: Mutex<MetaNode>,
    shared: Arc<SharedMeta>,
    freelist: Mutex<Vec<FreeExtent>>,
    freelist_stale: Mutex<Vec<FreeExtent>>,
    freelist_pages: Mutex<Vec<PageId>>,
    freelist_pages_stale: Mutex<Vec<PageId>>,
    /// contention is concentrated at the B+ Tree's root
    cache: NodeCache,
    file_extended: AtomicBool,
}

impl Store {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();

        let (file, sb, stale_sb) = if !path.exists() {
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
            (file, sb, None)
        } else {
            let file = OpenOptions::new().read(true).write(true).open(path)?;

            let mut buf0 = [0u8; PAGE_SIZE];
            let mut buf1 = [0u8; PAGE_SIZE];

            let r0 = file.pread_exact(&mut buf0, 0);
            let r1 = file.pread_exact(&mut buf1, PAGE_SIZE as u64);

            let sb0 = if r0.is_ok() {
                let s = MetaNode::from_slice(&buf0);
                if s.validate().is_ok() {
                    if s.format_version == FORMAT_VERSION {
                        Some(s)
                    } else if is_valid_v1_meta(&buf0) || is_valid_v2_meta(&buf0) {
                        return Err(Error::Invalid);
                    } else {
                        None
                    }
                } else if is_valid_v1_meta(&buf0) || is_valid_v2_meta(&buf0) {
                    return Err(Error::Invalid);
                } else {
                    None
                }
            } else {
                None
            };

            let sb1 = if r1.is_ok() {
                let s = MetaNode::from_slice(&buf1);
                if s.validate().is_ok() {
                    if s.format_version == FORMAT_VERSION {
                        Some(s)
                    } else if is_valid_v1_meta(&buf1) || is_valid_v2_meta(&buf1) {
                        return Err(Error::Invalid);
                    } else {
                        None
                    }
                } else if is_valid_v1_meta(&buf1) || is_valid_v2_meta(&buf1) {
                    return Err(Error::Invalid);
                } else {
                    None
                }
            } else {
                None
            };

            let (sb, stale_sb) = match (sb0, sb1) {
                (Some(s0), Some(s1)) => {
                    if s0.seq >= s1.seq {
                        (s0, Some(s1))
                    } else {
                        (s1, Some(s0))
                    }
                }
                (Some(s0), None) => (s0, None),
                (None, Some(s1)) => (s1, None),
                (None, None) => return Err(Error::Corruption),
            };
            (file, sb, stale_sb)
        };

        let shared = get_shared_meta(path, &sb);
        let store = Self {
            file,
            sb: Mutex::new(sb),
            shared,
            freelist: Mutex::new(Vec::new()),
            freelist_stale: Mutex::new(Vec::new()),
            freelist_pages: Mutex::new(Vec::new()),
            freelist_pages_stale: Mutex::new(Vec::new()),
            cache: NodeCache::new(4096),
            file_extended: AtomicBool::new(false),
        };
        let (freelist, freelist_pages) =
            store.read_freelist_from_disk(store.sb.lock().freelist_root)?;
        *store.freelist.lock() = freelist;
        *store.freelist_pages.lock() = freelist_pages;
        if let Some(stale) = stale_sb
            && let Ok((_, pages)) = store.read_freelist_from_disk(stale.freelist_root)
        {
            *store.freelist_pages_stale.lock() = pages;
        }
        Ok(store)
    }

    fn read_freelist_from_disk(&self, root: PageId) -> Result<(Vec<FreeExtent>, Vec<PageId>)> {
        if root == 0 {
            return Ok((Vec::new(), Vec::new()));
        }
        if FREE_LIST_ENTRIES_PER_PAGE == 0 {
            return Err(Error::Corruption);
        }

        let mut freelist = Vec::new();
        let mut pages = Vec::new();
        let mut visited = HashSet::new();
        let mut current = root;

        while current != 0 {
            if !visited.insert(current) {
                return Err(Error::Corruption);
            }
            pages.push(current);

            let mut buf = vec![0u8; PAGE_SIZE];
            self.file
                .pread_exact(&mut buf, current as u64 * PAGE_SIZE as u64)?;

            let header = FreeListHeader::from_slice(&buf);
            if header.count as usize > FREE_LIST_ENTRIES_PER_PAGE {
                return Err(Error::Corruption);
            }

            let stored_checksum = header.checksum;
            let mut header_zero = header;
            header_zero.checksum = 0;
            let header_bytes = unsafe {
                std::slice::from_raw_parts(
                    &header_zero as *const FreeListHeader as *const u8,
                    FREE_LIST_HEADER_SIZE,
                )
            };
            buf[..FREE_LIST_HEADER_SIZE].copy_from_slice(header_bytes);
            let computed_checksum = crc32c::crc32c(&buf);
            if computed_checksum != stored_checksum {
                return Err(Error::Corruption);
            }

            let mut offset = FREE_LIST_HEADER_SIZE;
            for _ in 0..header.count {
                let entry = FreeListEntry::from_slice(&buf[offset..offset + FREE_LIST_ENTRY_SIZE]);
                if entry.page_id == 0 || entry.nr_pages == 0 {
                    return Err(Error::Corruption);
                }
                Self::freelist_add_extent(&mut freelist, entry.page_id, entry.nr_pages);
                offset += FREE_LIST_ENTRY_SIZE;
            }

            current = header.next;
        }

        Ok((freelist, pages))
    }

    fn freelist_pages_needed(entries: usize) -> usize {
        if entries == 0 {
            0
        } else {
            entries.div_ceil(FREE_LIST_ENTRIES_PER_PAGE)
        }
    }

    fn freelist_add_extent(freelist: &mut Vec<FreeExtent>, page_id: PageId, nr_pages: u32) {
        if page_id == 0 || nr_pages == 0 {
            return;
        }

        let mut start = page_id as u64;
        let mut end = start + nr_pages as u64;
        let mut idx = 0;

        while idx < freelist.len() && freelist[idx].end() < start {
            idx += 1;
        }

        while idx < freelist.len() {
            let ext = freelist[idx];
            let ext_start = ext.page_id as u64;
            if ext_start > end {
                break;
            }
            start = start.min(ext_start);
            end = end.max(ext.end());
            freelist.remove(idx);
        }

        freelist.insert(
            idx,
            FreeExtent {
                page_id: start as PageId,
                nr_pages: (end - start) as u32,
            },
        );
    }

    fn freelist_remove_extent(
        freelist: &mut Vec<FreeExtent>,
        page_id: PageId,
        nr_pages: u32,
    ) -> bool {
        if page_id == 0 || nr_pages == 0 {
            return true;
        }

        let start = page_id as u64;
        let end = start + nr_pages as u64;
        let mut idx = 0;

        while idx < freelist.len() {
            let ext = freelist[idx];
            let ext_start = ext.page_id as u64;
            if end <= ext_start {
                return false;
            }
            if start >= ext.end() {
                idx += 1;
                continue;
            }

            if start < ext_start || end > ext.end() {
                return false;
            }

            if start == ext_start && end == ext.end() {
                freelist.remove(idx);
                return true;
            }
            if start == ext_start {
                freelist[idx].page_id = end as PageId;
                freelist[idx].nr_pages = (ext.end() - end) as u32;
                return true;
            }
            if end == ext.end() {
                freelist[idx].nr_pages = (start - ext_start) as u32;
                return true;
            }

            let right = FreeExtent {
                page_id: end as PageId,
                nr_pages: (ext.end() - end) as u32,
            };
            freelist[idx].nr_pages = (start - ext_start) as u32;
            freelist.insert(idx + 1, right);
            return true;
        }

        false
    }

    fn alloc_pages_inner(
        &self,
        sb: &mut MetaNode,
        freelist: &mut Vec<FreeExtent>,
        nr_pages: u32,
    ) -> Result<Vec<PageId>> {
        if nr_pages == 0 {
            return Ok(Vec::new());
        }
        const LOW_ADDR_SCAN_BUDGET: usize = 64;

        let mut pages = Vec::with_capacity(nr_pages as usize);
        let mut needed = nr_pages as u64;

        let mut idx = 0usize;
        let mut scanned = 0usize;
        while needed > 0 && idx < freelist.len() && scanned < LOW_ADDR_SCAN_BUDGET {
            let ext = freelist[idx];
            if ext.nr_pages == 0 {
                idx += 1;
                scanned += 1;
                continue;
            }

            let take = std::cmp::min(ext.nr_pages as u64, needed);
            for i in 0..take {
                pages.push((ext.page_id as u64 + i) as PageId);
            }

            if take as u32 == ext.nr_pages {
                freelist.remove(idx);
            } else {
                freelist[idx].page_id = (freelist[idx].page_id as u64 + take) as PageId;
                freelist[idx].nr_pages -= take as u32;
                idx += 1;
            }

            needed -= take;
            scanned += 1;
        }

        if needed > 0 {
            let mut segments = Vec::new();
            let mut tail_idx = freelist.len();
            let mut remaining = needed;

            while remaining > 0 && tail_idx > 0 {
                tail_idx -= 1;
                let ext = freelist[tail_idx];
                let take = std::cmp::min(ext.nr_pages as u64, remaining);
                let start = ext.end() - take;
                segments.push((start, take as u32));

                if take as u32 == ext.nr_pages {
                    freelist.remove(tail_idx);
                } else {
                    freelist[tail_idx].nr_pages = ext.nr_pages - take as u32;
                }

                remaining -= take;
            }

            for (start, len) in segments.into_iter().rev() {
                for i in 0..len {
                    pages.push((start + i as u64) as PageId);
                }
            }

            if remaining > 0 {
                let start_id = sb.next_page_id as u64;
                let end_id = start_id + remaining;
                if end_id > PageId::MAX as u64 {
                    return Err(Error::TooLarge);
                }
                sb.next_page_id = end_id as PageId;
                for i in 0..remaining {
                    pages.push((start_id + i) as PageId);
                }
                self.file_extended.store(true, Ordering::Relaxed);
            }
        }

        for &pid in pages.iter() {
            self.cache.invalidate(pid);
        }

        Ok(pages)
    }

    fn write_freelist_pages(&self, page_ids: &[PageId], freelist: &[FreeExtent]) -> Result<()> {
        if page_ids.is_empty() {
            return Ok(());
        }
        if FREE_LIST_ENTRIES_PER_PAGE == 0 {
            return Err(Error::Corruption);
        }

        let mut idx = 0;
        for (i, &pid) in page_ids.iter().enumerate() {
            let mut page = vec![0u8; PAGE_SIZE];
            let mut header = FreeListHeader {
                next: if i + 1 < page_ids.len() {
                    page_ids[i + 1]
                } else {
                    0
                },
                count: 0,
                checksum: 0,
                _padding: 0,
            };

            let mut count = 0usize;
            let mut offset = FREE_LIST_HEADER_SIZE;
            while idx < freelist.len() && count < FREE_LIST_ENTRIES_PER_PAGE {
                let entry = FreeListEntry {
                    page_id: freelist[idx].page_id,
                    nr_pages: freelist[idx].nr_pages,
                };
                let entry_bytes = unsafe {
                    std::slice::from_raw_parts(
                        &entry as *const FreeListEntry as *const u8,
                        FREE_LIST_ENTRY_SIZE,
                    )
                };
                page[offset..offset + FREE_LIST_ENTRY_SIZE].copy_from_slice(entry_bytes);
                offset += FREE_LIST_ENTRY_SIZE;
                count += 1;
                idx += 1;
            }

            header.count = count as u32;
            let header_bytes = unsafe {
                std::slice::from_raw_parts(
                    &header as *const FreeListHeader as *const u8,
                    FREE_LIST_HEADER_SIZE,
                )
            };
            page[..FREE_LIST_HEADER_SIZE].copy_from_slice(header_bytes);
            header.checksum = crc32c::crc32c(&page);
            let header_bytes = unsafe {
                std::slice::from_raw_parts(
                    &header as *const FreeListHeader as *const u8,
                    FREE_LIST_HEADER_SIZE,
                )
            };
            page[..FREE_LIST_HEADER_SIZE].copy_from_slice(header_bytes);

            self.file.pwrite_all(&page, pid as u64 * PAGE_SIZE as u64)?;
        }

        if idx != freelist.len() {
            return Err(Error::Internal);
        }

        Ok(())
    }

    pub(crate) fn commit_roots(
        &self,
        catalog_root: PageId,
        mapping_root: PageId,
        reverse_root: PageId,
        pending_free: &[(PageId, u32)],
    ) -> Result<()> {
        let mut sb = self.sb.lock();
        let mut freelist = self.freelist.lock();
        let mut freelist_stale = self.freelist_stale.lock();
        let mut freelist_pages = self.freelist_pages.lock();
        let mut freelist_pages_stale = self.freelist_pages_stale.lock();

        for ext in freelist_stale.drain(..) {
            Self::freelist_add_extent(&mut freelist, ext.page_id, ext.nr_pages);
        }
        for &(pid, nr) in pending_free {
            Self::freelist_add_extent(&mut freelist_stale, pid, nr);
        }

        for &pid in freelist_pages_stale.iter() {
            Self::freelist_add_extent(&mut freelist, pid, 1);
        }

        let mut new_pages = Vec::new();
        if !freelist.is_empty() {
            let target = Self::freelist_pages_needed(freelist.len());
            let alloc = self.alloc_pages_inner(&mut sb, &mut freelist, target as u32)?;
            new_pages.extend(alloc);
            if new_pages.len() < target {
                return Err(Error::Internal);
            }
        }
        if new_pages.is_empty() && !freelist.is_empty() {
            eprintln!("commit_root freelist empty? len={}", freelist.len());
        }

        if !new_pages.is_empty() {
            self.write_freelist_pages(&new_pages, &freelist)?;
            sb.freelist_root = new_pages[0];
        } else {
            sb.freelist_root = 0;
        }

        sb.catalog_root = catalog_root;
        sb.mapping_root = mapping_root;
        sb.reverse_root = reverse_root;
        sb.seq += 1;
        sb.update_checksum();

        let write_offset = if sb.seq.is_multiple_of(2) {
            PAGE_SIZE as u64
        } else {
            0
        };

        self.file.pwrite_all(&sb.as_page_slice(), write_offset)?;
        self.shared.update(sb.catalog_root, sb.seq);

        *freelist_pages_stale = std::mem::take(&mut *freelist_pages);
        *freelist_pages = new_pages;
        Ok(())
    }

    pub(crate) fn try_truncate_tail_with_floor(&self, min_end: PageId) -> Result<u64> {
        let mut sb = self.sb.lock();
        let mut freelist = self.freelist.lock();
        let mut freelist_stale = self.freelist_stale.lock();
        let mut freelist_pages = self.freelist_pages.lock();
        let mut freelist_pages_stale = self.freelist_pages_stale.lock();

        let mut dirty = false;
        if !freelist_stale.is_empty() {
            for ext in freelist_stale.drain(..) {
                Self::freelist_add_extent(&mut freelist, ext.page_id, ext.nr_pages);
            }
            dirty = true;
        }
        if !freelist_pages_stale.is_empty() {
            for &pid in freelist_pages_stale.iter() {
                Self::freelist_add_extent(&mut freelist, pid, 1);
            }
            freelist_pages_stale.clear();
            dirty = true;
        }

        if !freelist.is_empty() {
            let target = Self::freelist_pages_needed(freelist.len());
            if freelist_pages.len() < target {
                let alloc = self.alloc_pages_inner(
                    &mut sb,
                    &mut freelist,
                    (target - freelist_pages.len()) as u32,
                )?;
                freelist_pages.extend(alloc);
                dirty = true;
            }
        }

        let original_next = sb.next_page_id;
        let mut new_end = original_next;
        while let Some(ext) = freelist.last() {
            if ext.end() != new_end as u64 {
                break;
            }
            new_end = ext.page_id;
            freelist.pop();
        }

        let floor = min_end.max(2).min(original_next);
        if new_end < floor {
            new_end = floor;
        }

        let truncated = original_next.saturating_sub(new_end) as u64;
        if truncated == 0 && !dirty {
            return Ok(0);
        }

        sb.next_page_id = new_end;
        if !freelist_pages.is_empty() {
            self.write_freelist_pages(&freelist_pages, &freelist)?;
            sb.freelist_root = freelist_pages[0];
        } else {
            sb.freelist_root = 0;
        }

        sb.seq += 1;
        sb.update_checksum();

        let write_offset = if sb.seq.is_multiple_of(2) {
            PAGE_SIZE as u64
        } else {
            0
        };
        self.file.pwrite_all(&sb.as_page_slice(), write_offset)?;
        self.shared.update(sb.catalog_root, sb.seq);

        if truncated > 0 {
            let new_len = new_end as u64 * PAGE_SIZE as u64;
            self.file.set_len(new_len)?;
        }

        self.file.psync_all().map_err(|_| Error::IoError)?;
        Ok(truncated)
    }

    pub fn get_seq(&self) -> u64 {
        self.sb.lock().seq
    }

    pub fn shared_snapshot(&self) -> (u64, PageId) {
        self.shared.snapshot()
    }

    pub fn get_next_page_id(&self) -> PageId {
        self.sb.lock().next_page_id
    }

    pub(crate) fn free_pages_below(&self, limit: PageId) -> u64 {
        let freelist = self.freelist.lock();
        let limit_u64 = limit as u64;
        let mut total = 0u64;

        for ext in freelist.iter() {
            let start = ext.page_id as u64;
            if start >= limit_u64 {
                break;
            }
            let end = ext.end().min(limit_u64);
            if end > start {
                total += end - start;
            }
        }

        total
    }

    pub(crate) fn alloc_pages_below(
        &self,
        limit: PageId,
        nr_pages: u32,
    ) -> Result<Option<Vec<PageId>>> {
        if nr_pages == 0 {
            return Ok(Some(Vec::new()));
        }

        let mut freelist = self.freelist.lock();
        let mut pages = Vec::with_capacity(nr_pages as usize);
        let mut segments: Vec<(PageId, u32)> = Vec::new();
        let mut needed = nr_pages as u64;
        let limit_u64 = limit as u64;
        let mut idx = 0usize;

        while needed > 0 && idx < freelist.len() {
            let ext = freelist[idx];
            let ext_start = ext.page_id as u64;
            if ext_start >= limit_u64 {
                break;
            }

            let ext_end = ext.end().min(limit_u64);
            let available = ext_end.saturating_sub(ext_start);
            if available == 0 {
                idx += 1;
                continue;
            }

            let take = std::cmp::min(available, needed);
            segments.push((ext.page_id, take as u32));
            for i in 0..take {
                pages.push((ext.page_id as u64 + i) as PageId);
            }

            if take as u32 == ext.nr_pages {
                freelist.remove(idx);
            } else {
                freelist[idx].page_id = (freelist[idx].page_id as u64 + take) as PageId;
                freelist[idx].nr_pages -= take as u32;
                idx += 1;
            }

            needed -= take;
        }

        if needed > 0 {
            for (start, len) in segments {
                Self::freelist_add_extent(&mut freelist, start, len);
            }
            return Ok(None);
        }

        for &pid in pages.iter() {
            self.cache.invalidate(pid);
        }

        Ok(Some(pages))
    }

    pub fn alloc_pages(&self, nr_pages: u32) -> Result<Vec<PageId>> {
        let mut sb = self.sb.lock();
        let mut freelist = self.freelist.lock();
        self.alloc_pages_inner(&mut sb, &mut freelist, nr_pages)
    }

    pub fn alloc_lids(&self, nr_pages: u32) -> Result<Vec<PageId>> {
        if nr_pages == 0 {
            return Ok(Vec::new());
        }

        let mut sb = self.sb.lock();
        let start = sb.next_lid;
        sb.next_lid = sb.next_lid.checked_add(nr_pages).ok_or(Error::NoSpace)?;

        let mut lids = Vec::with_capacity(nr_pages as usize);
        for i in 0..nr_pages {
            lids.push(start + i);
        }
        Ok(lids)
    }

    pub fn free_pages(&self, page_id: PageId, nr_pages: u32) -> Result<()> {
        if page_id == 0 || nr_pages == 0 {
            return Ok(());
        }

        for i in 0..nr_pages {
            self.cache.invalidate(page_id + i);
        }

        let mut freelist = self.freelist.lock();
        Self::freelist_add_extent(&mut freelist, page_id, nr_pages);

        Ok(())
    }

    pub fn unfree_pages(&self, page_id: PageId, nr_pages: u32) -> Result<()> {
        if page_id == 0 || nr_pages == 0 {
            return Ok(());
        }

        let mut freelist = self.freelist.lock();
        let _ = Self::freelist_remove_extent(&mut freelist, page_id, nr_pages);
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        if self.file_extended.swap(false, Ordering::SeqCst) {
            return self.file.psync_all().map_err(|_| Error::IoError);
        }
        self.file.psync_data().map_err(|_| Error::IoError)
    }

    pub fn load_node(&self, page_id: PageId) -> Result<Arc<Node>> {
        if let Some(node) = self.cache.get(page_id) {
            return Ok(node);
        }

        let raw = self.load_page(page_id)?;
        let node = Arc::new(Node::from_raw(raw)?);

        self.cache.put(page_id, node.clone());
        Ok(node)
    }

    pub fn load_page(&self, page_id: PageId) -> Result<Vec<u8>> {
        self.load_data(&[page_id], PAGE_SIZE)
    }

    pub fn load_data(&self, pages: &[PageId], len: usize) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; len];
        self.read_data(pages, &mut buf)?;
        Ok(buf)
    }

    pub fn read_data(&self, pages: &[PageId], buf: &mut [u8]) -> Result<()> {
        for (i, &pid) in pages.iter().enumerate() {
            let offset = pid as u64 * PAGE_SIZE as u64;
            let start = i * PAGE_SIZE;
            if start >= buf.len() {
                break;
            }
            let end = std::cmp::min(start + PAGE_SIZE, buf.len());
            self.file.pread_exact(&mut buf[start..end], offset)?;
        }
        Ok(())
    }

    pub fn write_data(&self, pages: &[PageId], data: &[u8]) -> Result<()> {
        for (i, &pid) in pages.iter().enumerate() {
            let offset = pid as u64 * PAGE_SIZE as u64;
            let start = i * PAGE_SIZE;
            if start >= data.len() {
                break;
            }
            let end = std::cmp::min(start + PAGE_SIZE, data.len());
            self.file.pwrite_all(&data[start..end], offset)?;
        }
        Ok(())
    }

    pub fn write_page(&self, page_id: PageId, data: &[u8]) -> Result<()> {
        self.write_data(&[page_id], data)
    }

    pub fn get_catalog_root(&self) -> PageId {
        self.sb.lock().catalog_root
    }

    pub fn get_mapping_root(&self) -> PageId {
        self.sb.lock().mapping_root
    }

    pub fn get_reverse_root(&self) -> PageId {
        self.sb.lock().reverse_root
    }

    pub fn max_freelist_page_id(&self) -> PageId {
        self.freelist_pages
            .lock()
            .iter()
            .copied()
            .max()
            .unwrap_or(0)
    }

    pub fn refresh_sb(&self) -> Result<MetaSnapshot> {
        let mut buf0 = [0u8; PAGE_SIZE];
        let mut buf1 = [0u8; PAGE_SIZE];

        let r0 = self.file.pread_exact(&mut buf0, 0);
        let r1 = self.file.pread_exact(&mut buf1, PAGE_SIZE as u64);

        let sb0 = if r0.is_ok() {
            let s = MetaNode::from_slice(&buf0);
            if s.validate().is_ok() {
                if s.format_version == FORMAT_VERSION {
                    Some(s)
                } else if is_valid_v1_meta(&buf0) || is_valid_v2_meta(&buf0) {
                    return Err(Error::Invalid);
                } else {
                    None
                }
            } else if is_valid_v1_meta(&buf0) || is_valid_v2_meta(&buf0) {
                return Err(Error::Invalid);
            } else {
                None
            }
        } else {
            None
        };

        let sb1 = if r1.is_ok() {
            let s = MetaNode::from_slice(&buf1);
            if s.validate().is_ok() {
                if s.format_version == FORMAT_VERSION {
                    Some(s)
                } else if is_valid_v1_meta(&buf1) || is_valid_v2_meta(&buf1) {
                    return Err(Error::Invalid);
                } else {
                    None
                }
            } else if is_valid_v1_meta(&buf1) || is_valid_v2_meta(&buf1) {
                return Err(Error::Invalid);
            } else {
                None
            }
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

        let current_seq = self.sb.lock().seq;
        if sb.seq > current_seq {
            let (freelist, freelist_pages) = self.read_freelist_from_disk(sb.freelist_root)?;
            let mut current_sb = self.sb.lock();
            if sb.seq > current_sb.seq {
                *current_sb = sb;
                self.shared.update(current_sb.catalog_root, current_sb.seq);
                let mut free_guard = self.freelist.lock();
                *free_guard = freelist;
                let mut pages_guard = self.freelist_pages.lock();
                let mut stale_guard = self.freelist_pages_stale.lock();
                *stale_guard = std::mem::take(&mut *pages_guard);
                *pages_guard = freelist_pages;
                let mut stale_free = self.freelist_stale.lock();
                stale_free.clear();
                return Ok(MetaSnapshot {
                    catalog_root: current_sb.catalog_root,
                    mapping_root: current_sb.mapping_root,
                    reverse_root: current_sb.reverse_root,
                    seq: current_sb.seq,
                });
            }
        }

        let sb = self.sb.lock();
        Ok(MetaSnapshot {
            catalog_root: sb.catalog_root,
            mapping_root: sb.mapping_root,
            reverse_root: sb.reverse_root,
            seq: sb.seq,
        })
    }

    pub fn clear_cache(&self) {
        for shard in &self.cache.shards {
            let mut guard = shard.lock();
            guard.entries.iter_mut().for_each(|e| *e = None);
            guard.map.clear();
        }
    }
}

struct CacheEntry {
    page_id: PageId,
    node: Arc<Node>,
    usage: bool,
}

struct NodeCacheShard {
    entries: Vec<Option<CacheEntry>>,
    map: HashMap<PageId, usize>,
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

    fn get(&mut self, page_id: PageId) -> Option<Arc<Node>> {
        if let Some(&idx) = self.map.get(&page_id)
            && let Some(entry) = &mut self.entries[idx]
        {
            entry.usage = true;
            return Some(entry.node.clone());
        }
        None
    }

    fn put(&mut self, page_id: PageId, node: Arc<Node>) {
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

    fn invalidate(&mut self, page_id: PageId) {
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

    fn get_shard(&self, page_id: PageId) -> &Mutex<NodeCacheShard> {
        let mut hasher = DefaultHasher::new();
        page_id.hash(&mut hasher);
        let hash = hasher.finish();
        &self.shards[(hash as usize) % NUM_SHARDS]
    }

    pub fn get(&self, page_id: PageId) -> Option<Arc<Node>> {
        self.get_shard(page_id).lock().get(page_id)
    }

    pub fn put(&self, page_id: PageId, node: Arc<Node>) {
        self.get_shard(page_id).lock().put(page_id, node)
    }

    pub fn invalidate(&self, page_id: PageId) {
        self.get_shard(page_id).lock().invalidate(page_id)
    }
}
