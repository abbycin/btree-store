use parking_lot::{Mutex, RwLock};
use std::{
    collections::{BTreeMap, HashSet},
    fs::{File, OpenOptions as FileOpenOptions, TryLockError},
    io,
    ops::Bound::{Excluded, Included, Unbounded},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use crate::{
    CorruptionReport, DataPid, FORMAT_VERSION, FatalReason, IdSpace, IoFault, MAGIC, MetaNode,
    OpenError, OpenIoError, OpenOptions, OpenResult, PageId, StoreFault as Error,
    StoreResult as Result, SyncMode, abort_store_fault, fatal,
    node::{AlignedPage, Node, PAGE_SIZE},
    physical_value,
};

pub(crate) trait PageReuseObserver: Send + Sync {
    fn invalidate(&self, page_id: PageId);
}

#[cfg(test)]
pub(crate) struct NoopPageReuseObserver;

#[cfg(test)]
impl PageReuseObserver for NoopPageReuseObserver {
    fn invalidate(&self, _page_id: PageId) {}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct MetaSnapshot {
    pub(crate) catalog_root: PageId,
    pub(crate) next_page_id: PageId,
    pub(crate) reusable_root: PageId,
    pub(crate) retired_root: PageId,
    pub(crate) seq: u64,
}

/// Abstract trait for positional I/O on supported operating systems.
pub(crate) trait FileIO {
    fn pread_exact(&self, buf: &mut [u8], offset: u64) -> io::Result<()>;
    fn pwrite_all(&self, buf: &[u8], offset: u64) -> io::Result<()>;
    fn psync_all(&self) -> io::Result<()>;
    fn psync_data(&self) -> io::Result<()>;
}

fn parent_dir_for_sync(path: &Path) -> Option<&Path> {
    match path.parent() {
        Some(parent) if parent.as_os_str().is_empty() => Some(Path::new(".")),
        Some(parent) => Some(parent),
        None => None,
    }
}

#[cfg(unix)]
fn sync_dir(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

#[cfg(windows)]
fn sync_dir(path: &Path) -> io::Result<()> {
    use std::os::windows::fs::OpenOptionsExt;

    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    const ERROR_ACCESS_DENIED: i32 = 5;
    const ERROR_INVALID_HANDLE: i32 = 6;
    const ERROR_INVALID_FUNCTION: i32 = 1;
    const ERROR_NOT_SUPPORTED: i32 = 50;

    let dir = FileOpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)?;
    match dir.sync_all() {
        Ok(()) => Ok(()),
        Err(err)
            if matches!(
                err.raw_os_error(),
                Some(
                    ERROR_ACCESS_DENIED
                        | ERROR_INVALID_HANDLE
                        | ERROR_INVALID_FUNCTION
                        | ERROR_NOT_SUPPORTED
                )
            ) =>
        {
            Ok(())
        }
        Err(err) => Err(err),
    }
}

fn sync_parent_dir(path: &Path) -> io::Result<()> {
    if let Some(parent) = parent_dir_for_sync(path) {
        sync_dir(parent)?;
    }
    Ok(())
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

struct RawFile {
    file: File,
    path: Arc<PathBuf>,
    #[cfg(test)]
    fault: Option<TestFault>,
}

const OPEN_LOCK_TIMEOUT: Duration = Duration::from_secs(1);
const OPEN_LOCK_RETRY_INTERVAL: Duration = Duration::from_millis(1);

#[cfg(test)]
const TEST_LIVE_FAULT_ENV: &str = "BTREE_STORE_TEST_LIVE_FAULT";

#[cfg(test)]
#[derive(Clone)]
struct TestFault {
    operation: &'static str,
    raw_os_error: i32,
    remaining: Arc<std::sync::atomic::AtomicUsize>,
}

#[cfg(test)]
impl TestFault {
    fn from_env() -> Option<Self> {
        let spec = std::env::var(TEST_LIVE_FAULT_ENV).ok()?;
        let mut fields = spec.split(':');
        let operation = match fields.next()? {
            "pread" => "pread",
            "pwrite" => "pwrite",
            "sync_all" => "sync_all",
            "sync_data" => "sync_data",
            _ => return None,
        };
        let occurrence = fields.next()?.parse().ok()?;
        let raw_os_error = fields.next()?.parse().ok()?;
        (fields.next().is_none() && occurrence > 0).then(|| Self {
            operation,
            raw_os_error,
            remaining: Arc::new(std::sync::atomic::AtomicUsize::new(occurrence)),
        })
    }

    fn should_fail(&self, operation: &'static str) -> bool {
        self.operation == operation
            && self
                .remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    (remaining > 0).then_some(remaining - 1)
                })
                == Ok(1)
    }
}

impl RawFile {
    fn open(path: &Path) -> OpenResult<(Self, bool)> {
        let file = FileOpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(|source| {
                OpenError::Io(OpenIoError {
                    operation: "open",
                    path: path.to_path_buf(),
                    offset: None,
                    length: None,
                    source,
                })
            })?;

        let deadline = Instant::now() + OPEN_LOCK_TIMEOUT;
        loop {
            match file.try_lock() {
                Ok(()) => break,
                Err(TryLockError::WouldBlock) if Instant::now() < deadline => {
                    std::thread::sleep(OPEN_LOCK_RETRY_INTERVAL);
                }
                Err(TryLockError::WouldBlock) => {
                    return Err(OpenError::DatabaseBusy {
                        path: path.to_path_buf(),
                    });
                }
                Err(TryLockError::Error(source)) => {
                    return Err(OpenError::Io(OpenIoError {
                        operation: "try_lock",
                        path: path.to_path_buf(),
                        offset: None,
                        length: None,
                        source,
                    }));
                }
            }
        }

        let is_new = file
            .metadata()
            .map_err(|source| {
                OpenError::Io(OpenIoError {
                    operation: "metadata",
                    path: path.to_path_buf(),
                    offset: None,
                    length: None,
                    source,
                })
            })?
            .len()
            == 0;

        Ok((
            Self {
                file,
                path: Arc::new(path.to_path_buf()),
                #[cfg(test)]
                fault: TestFault::from_env(),
            },
            is_new,
        ))
    }

    #[cfg(test)]
    fn injected_error(&self, operation: &'static str) -> Option<io::Error> {
        self.fault
            .as_ref()
            .filter(|fault| fault.should_fail(operation))
            .map(|fault| io::Error::from_raw_os_error(fault.raw_os_error))
    }
}

struct OpeningStore {
    raw: RawFile,
}

impl OpeningStore {
    fn open(path: &Path) -> OpenResult<(Self, bool)> {
        let (raw, is_new) = RawFile::open(path)?;
        Ok((Self { raw }, is_new))
    }

    fn io_error(
        &self,
        operation: &'static str,
        offset: Option<u64>,
        length: Option<u64>,
        source: io::Error,
    ) -> OpenError {
        OpenError::Io(OpenIoError {
            operation,
            path: self.raw.path.as_ref().clone(),
            offset,
            length,
            source,
        })
    }

    fn reserve_meta_pages(&self) -> OpenResult<()> {
        self.raw
            .file
            .set_len(PAGE_SIZE as u64 * 2)
            .map_err(|source| self.io_error("set_len", Some(0), Some(PAGE_SIZE as u64 * 2), source))
    }

    fn pread_exact(&self, buf: &mut [u8], offset: u64) -> OpenResult<()> {
        #[cfg(test)]
        if let Some(source) = self.raw.injected_error("pread") {
            return Err(self.io_error("pread", Some(offset), Some(buf.len() as u64), source));
        }
        self.raw
            .file
            .pread_exact(buf, offset)
            .map_err(|source| self.io_error("pread", Some(offset), Some(buf.len() as u64), source))
    }

    fn read_meta_page(&self, offset: u64) -> OpenResult<Option<[u8; PAGE_SIZE]>> {
        let mut buf = [0u8; PAGE_SIZE];
        #[cfg(test)]
        if let Some(source) = self.raw.injected_error("pread") {
            return Err(self.io_error("pread", Some(offset), Some(PAGE_SIZE as u64), source));
        }
        match self.raw.file.pread_exact(&mut buf, offset) {
            Ok(()) => Ok(Some(buf)),
            Err(source) if source.kind() == io::ErrorKind::UnexpectedEof => Ok(None),
            Err(source) => {
                Err(self.io_error("pread", Some(offset), Some(PAGE_SIZE as u64), source))
            }
        }
    }

    fn pwrite_all(&self, buf: &[u8], offset: u64) -> OpenResult<()> {
        #[cfg(test)]
        if let Some(source) = self.raw.injected_error("pwrite") {
            return Err(self.io_error("pwrite", Some(offset), Some(buf.len() as u64), source));
        }
        self.raw
            .file
            .pwrite_all(buf, offset)
            .map_err(|source| self.io_error("pwrite", Some(offset), Some(buf.len() as u64), source))
    }

    fn sync_all(&self) -> OpenResult<()> {
        #[cfg(test)]
        if let Some(source) = self.raw.injected_error("sync_all") {
            return Err(self.io_error("sync_all", None, None, source));
        }
        self.raw
            .file
            .psync_all()
            .map_err(|source| self.io_error("sync_all", None, None, source))
    }

    fn sync_parent(&self) -> OpenResult<()> {
        sync_parent_dir(self.raw.path.as_ref())
            .map_err(|source| self.io_error("sync_parent_dir", None, None, source))
    }

    fn corruption(
        &self,
        code: &'static str,
        generation: Option<u64>,
        page_kind: &'static str,
        pid: Option<PageId>,
        check: &'static str,
    ) -> OpenError {
        OpenError::Corruption(CorruptionReport {
            code,
            generation,
            page_kind,
            pid,
            check,
            expected: None,
            actual: None,
        })
    }

    fn read_extent(
        &self,
        root: PageId,
        next_page_id: PageId,
        generation: u64,
    ) -> OpenResult<(Vec<Extent>, Vec<PageId>)> {
        read_extent_pages(
            root,
            next_page_id,
            |current, buf| self.pread_exact(buf, current as u64 * PAGE_SIZE as u64),
            |code, current, check| {
                self.corruption(code, Some(generation), "extent", Some(current), check)
            },
        )
    }

    fn read_allocator_state(
        &self,
        reusable_root: PageId,
        retired_root: PageId,
        next_page_id: PageId,
        generation: u64,
    ) -> OpenResult<DiskAllocatorState> {
        let (reusable, reusable_pages) =
            self.read_extent(reusable_root, next_page_id, generation)?;
        let (retired, retired_pages) = self.read_extent(retired_root, next_page_id, generation)?;
        validate_allocator_sets(
            &reusable,
            &reusable_pages,
            &retired,
            &retired_pages,
            |code, pid, check| self.corruption(code, Some(generation), "extent", Some(pid), check),
        )?;
        Ok((reusable, reusable_pages, retired, retired_pages))
    }

    fn finish(self, generation: u64) -> LiveStore {
        LiveStore {
            raw: self.raw,
            generation: AtomicU64::new(generation),
        }
    }
}

struct LiveStore {
    raw: RawFile,
    generation: AtomicU64,
}

impl LiveStore {
    fn set_generation(&self, generation: u64) {
        self.generation.store(generation, Ordering::Release);
    }

    fn io_fault(
        &self,
        operation: &'static str,
        offset: Option<u64>,
        length: Option<u64>,
        source: io::Error,
    ) -> ! {
        fatal(FatalReason::Io(IoFault {
            operation,
            path: self.raw.path.as_ref().clone(),
            generation: self.generation.load(Ordering::Acquire),
            offset,
            length,
            source,
        }))
    }

    fn pread_exact(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        #[cfg(test)]
        if let Some(source) = self.raw.injected_error("pread") {
            self.io_fault("pread", Some(offset), Some(buf.len() as u64), source);
        }
        if let Err(source) = self.raw.file.pread_exact(buf, offset) {
            self.io_fault("pread", Some(offset), Some(buf.len() as u64), source);
        }
        Ok(())
    }

    fn pwrite_all(&self, buf: &[u8], offset: u64) -> Result<()> {
        #[cfg(test)]
        if let Some(source) = self.raw.injected_error("pwrite") {
            self.io_fault("pwrite", Some(offset), Some(buf.len() as u64), source);
        }
        if let Err(source) = self.raw.file.pwrite_all(buf, offset) {
            self.io_fault("pwrite", Some(offset), Some(buf.len() as u64), source);
        }
        Ok(())
    }

    fn psync_all(&self) -> Result<()> {
        #[cfg(test)]
        if let Some(source) = self.raw.injected_error("sync_all") {
            self.io_fault("sync_all", None, None, source);
        }
        if let Err(source) = self.raw.file.psync_all() {
            self.io_fault("sync_all", None, None, source);
        }
        Ok(())
    }

    fn psync_data(&self) -> Result<()> {
        #[cfg(test)]
        if let Some(source) = self.raw.injected_error("sync_data") {
            self.io_fault("sync_data", None, None, source);
        }
        if let Err(source) = self.raw.file.psync_data() {
            self.io_fault("sync_data", None, None, source);
        }
        Ok(())
    }
}

struct SharedMeta {
    state: RwLock<(u64, PageId)>,
}

impl SharedMeta {
    fn new(seq: u64, root: PageId) -> Self {
        Self {
            state: RwLock::new((seq, root)),
        }
    }

    fn update(&self, root: PageId, seq: u64) {
        *self.state.write() = (seq, root);
    }

    fn snapshot(&self) -> (u64, PageId) {
        *self.state.read()
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct ExtentHeader {
    next: PageId,
    count: u32,
}

impl ExtentHeader {
    fn from_slice(x: &[u8]) -> Self {
        unsafe { std::ptr::read_unaligned(x.as_ptr().cast::<Self>()) }
    }

    fn as_slice(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                (self as *const Self).cast::<u8>(),
                std::mem::size_of::<Self>(),
            )
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Extent {
    page_id: PageId,
    nr_pages: u32,
}

type DiskAllocatorState = (Vec<Extent>, Vec<PageId>, Vec<Extent>, Vec<PageId>);

impl Extent {
    fn end(&self) -> u64 {
        self.page_id as u64 + self.nr_pages as u64
    }

    fn from_slice(x: &[u8]) -> Self {
        unsafe { std::ptr::read_unaligned(x.as_ptr().cast::<Self>()) }
    }

    fn as_slice(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                (self as *const Self).cast::<u8>(),
                std::mem::size_of::<Self>(),
            )
        }
    }
}

#[derive(Clone, Default, Debug, PartialEq, Eq)]
struct ExtentSet {
    ranges: BTreeMap<PageId, u32>,
}

#[derive(Debug, Default)]
struct ExtentBuffer {
    first: Option<Extent>,
    rest: Vec<Extent>,
}

impl ExtentBuffer {
    fn one(extent: Extent) -> Self {
        Self {
            first: Some(extent),
            rest: Vec::new(),
        }
    }

    fn push(&mut self, extent: Extent) {
        if self.first.is_none() {
            self.first = Some(extent);
        } else {
            self.rest.push(extent);
        }
    }

    fn len(&self) -> usize {
        usize::from(self.first.is_some()) + self.rest.len()
    }

    fn is_empty(&self) -> bool {
        self.first.is_none()
    }

    fn first(&self) -> Option<Extent> {
        self.first
    }

    fn iter(&self) -> impl Iterator<Item = &Extent> {
        self.first.iter().chain(self.rest.iter())
    }
}

#[derive(Debug)]
struct ExtentSetChange {
    before: ExtentBuffer,
    after: ExtentBuffer,
}

impl ExtentSet {
    fn from_extents(extents: Vec<Extent>) -> Self {
        let mut set = Self::default();
        for extent in extents {
            set.add(extent.page_id, extent.nr_pages);
        }
        set
    }

    fn len(&self) -> usize {
        self.ranges.len()
    }

    fn iter(&self) -> impl Iterator<Item = Extent> + '_ {
        self.ranges
            .iter()
            .map(|(&page_id, &nr_pages)| Extent { page_id, nr_pages })
    }

    #[cfg(test)]
    fn to_vec(&self) -> Vec<Extent> {
        self.iter().collect()
    }

    fn add(&mut self, page_id: PageId, nr_pages: u32) {
        if page_id == 0 || nr_pages == 0 {
            return;
        }

        let mut start = u64::from(page_id);
        let mut end = start + u64::from(nr_pages);

        if let Some((&previous_start, &previous_len)) = self.ranges.range(..=page_id).next_back()
            && previous_start < page_id
            && u64::from(previous_start) + u64::from(previous_len) >= start
        {
            start = u64::from(previous_start);
            end = end.max(start + u64::from(previous_len));
            self.ranges.remove(&previous_start);
        }

        while let Some((&next_start, &next_len)) = self.ranges.range(start as PageId..).next() {
            if u64::from(next_start) > end {
                break;
            }
            end = end.max(u64::from(next_start) + u64::from(next_len));
            self.ranges.remove(&next_start);
        }

        self.ranges.insert(start as PageId, (end - start) as u32);
    }

    fn add_with_change(&mut self, page_id: PageId, nr_pages: u32) -> Option<ExtentSetChange> {
        if page_id == 0 || nr_pages == 0 {
            return None;
        }

        let mut start = u64::from(page_id);
        let mut end = start + u64::from(nr_pages);
        let mut before = ExtentBuffer::default();

        if let Some((&previous_start, &previous_len)) = self.ranges.range(..=page_id).next_back()
            && previous_start < page_id
            && u64::from(previous_start) + u64::from(previous_len) >= start
        {
            start = start.min(u64::from(previous_start));
            end = end.max(u64::from(previous_start) + u64::from(previous_len));
            before.push(Extent {
                page_id: previous_start,
                nr_pages: previous_len,
            });
        }

        for (&next_start, &next_len) in self.ranges.range((Included(page_id), Unbounded)) {
            if u64::from(next_start) > end {
                break;
            }
            start = start.min(u64::from(next_start));
            end = end.max(u64::from(next_start) + u64::from(next_len));
            before.push(Extent {
                page_id: next_start,
                nr_pages: next_len,
            });
        }

        let after = ExtentBuffer::one(Extent {
            page_id: start as PageId,
            nr_pages: (end - start) as u32,
        });

        if before.len() == 1 && before.first() == after.first() {
            return None;
        }

        for extent in before.iter() {
            self.ranges.remove(&extent.page_id);
        }
        let extent = after.first().unwrap();
        self.ranges.insert(extent.page_id, extent.nr_pages);
        Some(ExtentSetChange { before, after })
    }

    fn remove(&mut self, page_id: PageId, nr_pages: u32) {
        if page_id == 0 || nr_pages == 0 {
            return;
        }

        let start = u64::from(page_id);
        let end = start + u64::from(nr_pages);

        if let Some((&extent_start, &extent_len)) = self.ranges.range(..=page_id).next_back() {
            let extent_start_u64 = u64::from(extent_start);
            let extent_end = extent_start_u64 + u64::from(extent_len);
            if extent_end > start && extent_start_u64 < end {
                self.ranges.remove(&extent_start);
                if extent_start_u64 < start {
                    self.ranges
                        .insert(extent_start, (start - extent_start_u64) as u32);
                }
                if end < extent_end {
                    self.ranges.insert(end as PageId, (extent_end - end) as u32);
                }
            }
        }

        while let Some((&extent_start, &extent_len)) = self.ranges.range(page_id..).next() {
            let extent_start_u64 = u64::from(extent_start);
            if extent_start_u64 >= end {
                break;
            }
            let extent_end = extent_start_u64 + u64::from(extent_len);
            self.ranges.remove(&extent_start);
            if end < extent_end {
                self.ranges.insert(end as PageId, (extent_end - end) as u32);
                break;
            }
        }
    }

    fn remove_with_change(&mut self, page_id: PageId, nr_pages: u32) -> Option<ExtentSetChange> {
        if page_id == 0 || nr_pages == 0 {
            return None;
        }

        let start = u64::from(page_id);
        let end = start + u64::from(nr_pages);
        let mut affected = ExtentBuffer::default();

        if let Some((&extent_start, &extent_len)) = self.ranges.range(..=page_id).next_back() {
            let extent_end = u64::from(extent_start) + u64::from(extent_len);
            if extent_end > start && u64::from(extent_start) < end {
                affected.push(Extent {
                    page_id: extent_start,
                    nr_pages: extent_len,
                });
            }
        }

        for (&extent_start, &extent_len) in self.ranges.range((Excluded(page_id), Unbounded)) {
            if u64::from(extent_start) >= end {
                break;
            }
            let extent_start = u64::from(extent_start);
            let extent_end = extent_start + u64::from(extent_len);
            if extent_end > start {
                affected.push(Extent {
                    page_id: extent_start as PageId,
                    nr_pages: extent_len,
                });
            }
        }

        if affected.is_empty() {
            return None;
        }

        let mut after = ExtentBuffer::default();
        for extent in affected.iter() {
            let extent_start = u64::from(extent.page_id);
            let extent_end = extent.end();
            if extent_start < start {
                after.push(Extent {
                    page_id: extent.page_id,
                    nr_pages: (start - extent_start) as u32,
                });
            }
            if end < extent_end {
                after.push(Extent {
                    page_id: end as PageId,
                    nr_pages: (extent_end - end) as u32,
                });
            }
        }

        for extent in affected.iter() {
            self.ranges.remove(&extent.page_id);
        }
        for extent in after.iter() {
            self.ranges.insert(extent.page_id, extent.nr_pages);
        }
        Some(ExtentSetChange {
            before: affected,
            after,
        })
    }

    fn take_first(&mut self, nr_pages: u32) -> Vec<PageId> {
        let mut pages = Vec::with_capacity(nr_pages as usize);
        let mut needed = u64::from(nr_pages);

        while needed > 0 {
            let Some((&page_id, &extent_len)) = self.ranges.iter().next() else {
                break;
            };
            let take = needed.min(u64::from(extent_len));
            self.ranges.remove(&page_id);
            for offset in 0..take {
                pages.push((u64::from(page_id) + offset) as PageId);
            }
            if take < u64::from(extent_len) {
                self.ranges.insert(
                    (u64::from(page_id) + take) as PageId,
                    (u64::from(extent_len) - take) as u32,
                );
            }
            needed -= take;
        }

        pages
    }

    fn take_first_with_change(&mut self, nr_pages: u32) -> (Vec<PageId>, Option<ExtentSetChange>) {
        let mut pages = Vec::with_capacity(nr_pages as usize);
        let mut needed = u64::from(nr_pages);
        let mut affected = ExtentBuffer::default();

        for (&page_id, &extent_len) in &self.ranges {
            if needed == 0 {
                break;
            }
            affected.push(Extent {
                page_id,
                nr_pages: extent_len,
            });
            needed = needed.saturating_sub(u64::from(extent_len));
        }

        if affected.is_empty() {
            return (pages, None);
        }

        needed = u64::from(nr_pages);
        let mut after = ExtentBuffer::default();
        for extent in affected.iter() {
            let extent_len = u64::from(extent.nr_pages);
            let take = needed.min(extent_len);
            for offset in 0..take {
                pages.push((u64::from(extent.page_id) + offset) as PageId);
            }

            self.ranges.remove(&extent.page_id);
            if take < extent_len {
                after.push(Extent {
                    page_id: (u64::from(extent.page_id) + take) as PageId,
                    nr_pages: (extent_len - take) as u32,
                });
            }
            needed -= take;
            if needed == 0 {
                break;
            }
        }

        for extent in after.iter() {
            self.ranges.insert(extent.page_id, extent.nr_pages);
        }
        (
            pages,
            Some(ExtentSetChange {
                before: affected,
                after,
            }),
        )
    }

    #[cfg(test)]
    fn contains(&self, page_id: PageId) -> bool {
        self.ranges
            .range(..=page_id)
            .next_back()
            .is_some_and(|(&start, &nr_pages)| {
                u64::from(page_id) < u64::from(start) + u64::from(nr_pages)
            })
    }

    fn undo(&mut self, change: ExtentSetChange) {
        for extent in change.after.iter() {
            self.remove(extent.page_id, extent.nr_pages);
        }
        for extent in change.before.iter() {
            self.add(extent.page_id, extent.nr_pages);
        }
    }
}

#[derive(Clone, Copy)]
enum ExtentSetKind {
    Reusable,
    Retired,
}

struct AllocatorMutation {
    target: ExtentSetKind,
    change: ExtentSetChange,
}

struct AllocatorMutationJournal {
    inline: [Option<AllocatorMutation>; 4],
    inline_len: usize,
    overflow: Vec<AllocatorMutation>,
    next_page_id: PageId,
    file_extended: bool,
}

impl AllocatorMutationJournal {
    fn new(next_page_id: PageId, file_extended: bool) -> Self {
        Self {
            inline: std::array::from_fn(|_| None),
            inline_len: 0,
            overflow: Vec::new(),
            next_page_id,
            file_extended,
        }
    }

    fn add(&mut self, target: ExtentSetKind, set: &mut ExtentSet, page_id: PageId, nr_pages: u32) {
        if let Some(change) = set.add_with_change(page_id, nr_pages) {
            self.push(AllocatorMutation { target, change });
        }
    }

    fn remove(
        &mut self,
        target: ExtentSetKind,
        set: &mut ExtentSet,
        page_id: PageId,
        nr_pages: u32,
    ) {
        if let Some(change) = set.remove_with_change(page_id, nr_pages) {
            self.push(AllocatorMutation { target, change });
        }
    }

    fn take_first(
        &mut self,
        target: ExtentSetKind,
        set: &mut ExtentSet,
        nr_pages: u32,
    ) -> Vec<PageId> {
        let (pages, change) = set.take_first_with_change(nr_pages);
        if let Some(change) = change {
            self.push(AllocatorMutation { target, change });
        }
        pages
    }

    fn push(&mut self, mutation: AllocatorMutation) {
        if self.inline_len < self.inline.len() {
            self.inline[self.inline_len] = Some(mutation);
            self.inline_len += 1;
        } else {
            self.overflow.push(mutation);
        }
    }

    fn rollback(
        &mut self,
        sb: &mut MetaNode,
        reusable: &mut ExtentSet,
        retired: &mut ExtentSet,
        file_extended: &AtomicBool,
    ) {
        for mutation in self.overflow.drain(..).rev() {
            match mutation.target {
                ExtentSetKind::Reusable => reusable.undo(mutation.change),
                ExtentSetKind::Retired => retired.undo(mutation.change),
            }
        }
        for slot in self.inline[..self.inline_len].iter_mut().rev() {
            let mutation = slot.take().unwrap();
            match mutation.target {
                ExtentSetKind::Reusable => reusable.undo(mutation.change),
                ExtentSetKind::Retired => retired.undo(mutation.change),
            }
        }
        self.inline_len = 0;
        sb.next_page_id = self.next_page_id;
        file_extended.store(self.file_extended, Ordering::Relaxed);
    }

    fn disarm(&mut self) {
        for slot in self.inline[..self.inline_len].iter_mut() {
            *slot = None;
        }
        self.inline_len = 0;
        self.overflow.clear();
    }
}

const EXTENT_HEADER_SIZE: usize = std::mem::size_of::<ExtentHeader>();
const EXTENT_SIZE: usize = std::mem::size_of::<Extent>();
const EXTENT_PER_PAGE: usize = (PAGE_SIZE - EXTENT_HEADER_SIZE) / EXTENT_SIZE;

fn validate_allocator_page_id<E, C>(
    pid: PageId,
    next_page_id: PageId,
    code: &'static str,
    check: &'static str,
    corruption: &mut C,
) -> std::result::Result<(), E>
where
    C: FnMut(&'static str, PageId, &'static str) -> E,
{
    if !(2..next_page_id).contains(&pid) {
        return Err(corruption(code, pid, check));
    }
    Ok(())
}

fn validate_extent_pages_disjoint<E, C>(
    extents: &[Extent],
    pages: &[PageId],
    mut corruption: C,
) -> std::result::Result<(), E>
where
    C: FnMut(&'static str, PageId, &'static str) -> E,
{
    for extent in extents {
        let start = u64::from(extent.page_id);
        let end = extent.end();
        if pages
            .iter()
            .any(|page_id| start <= u64::from(*page_id) && u64::from(*page_id) < end)
        {
            return Err(corruption(
                "ALLOCATOR_STATE_OVERLAP",
                extent.page_id,
                "allocator extents must not cover allocator list pages",
            ));
        }
    }
    Ok(())
}

fn validate_allocator_sets<E, C>(
    reusable: &[Extent],
    reusable_pages: &[PageId],
    retired: &[Extent],
    retired_pages: &[PageId],
    mut corruption: C,
) -> std::result::Result<(), E>
where
    C: FnMut(&'static str, PageId, &'static str) -> E,
{
    let mut intervals = Vec::with_capacity(
        reusable.len() + retired.len() + reusable_pages.len() + retired_pages.len(),
    );
    for extent in reusable {
        intervals.push((u64::from(extent.page_id), extent.end(), extent.page_id));
    }
    for extent in retired {
        intervals.push((u64::from(extent.page_id), extent.end(), extent.page_id));
    }
    for &page_id in reusable_pages {
        intervals.push((u64::from(page_id), u64::from(page_id) + 1, page_id));
    }
    for &page_id in retired_pages {
        intervals.push((u64::from(page_id), u64::from(page_id) + 1, page_id));
    }
    intervals.sort_unstable_by_key(|(start, end, _)| (*start, *end));

    let mut previous = None;
    for (start, end, pid) in intervals {
        if let Some((_, previous_end, _)) = previous
            && start < previous_end
        {
            return Err(corruption(
                "ALLOCATOR_STATE_OVERLAP",
                pid,
                "allocator ownership classes must be disjoint",
            ));
        }
        previous = Some((start, end, pid));
    }
    Ok(())
}

fn read_extent_pages<E, R, C>(
    root: PageId,
    next_page_id: PageId,
    mut read_page: R,
    mut corruption: C,
) -> std::result::Result<(Vec<Extent>, Vec<PageId>), E>
where
    R: FnMut(PageId, &mut [u8; PAGE_SIZE]) -> std::result::Result<(), E>,
    C: FnMut(&'static str, PageId, &'static str) -> E,
{
    if root == 0 {
        return Ok((Vec::new(), Vec::new()));
    }

    let mut extents: Vec<Extent> = Vec::new();
    let mut pages = Vec::new();
    let mut visited = HashSet::new();
    let mut current = root;
    let mut previous_end = None;

    while current != 0 {
        validate_allocator_page_id(
            current,
            next_page_id,
            "INVALID_EXTENT_LIST_PAGE",
            "allocator list pages must stay within [2, next_page_id)",
            &mut corruption,
        )?;
        if !visited.insert(current) {
            return Err(corruption(
                "EXTENT_CYCLE",
                current,
                "extent chain must be acyclic",
            ));
        }
        pages.push(current);

        let mut buf = [0u8; PAGE_SIZE];
        read_page(current, &mut buf)?;

        let header = ExtentHeader::from_slice(&buf);
        if header.count as usize > EXTENT_PER_PAGE {
            return Err(corruption(
                "INVALID_EXTENT_COUNT",
                current,
                "extent count exceeds page capacity",
            ));
        }

        let mut offset = EXTENT_HEADER_SIZE;
        for _ in 0..header.count {
            let entry = Extent::from_slice(&buf[offset..offset + EXTENT_SIZE]);
            if entry.page_id == 0 || entry.nr_pages == 0 {
                return Err(corruption(
                    "INVALID_EXTENT_ENTRY",
                    current,
                    "extent entry page and length must be non-zero",
                ));
            }
            let start = u64::from(entry.page_id);
            let end = start
                .checked_add(u64::from(entry.nr_pages))
                .ok_or_else(|| {
                    corruption(
                        "EXTENT_OUT_OF_RANGE",
                        entry.page_id,
                        "extent end must use checked arithmetic within next_page_id",
                    )
                })?;
            if start < 2 || end > u64::from(next_page_id) {
                return Err(corruption(
                    "EXTENT_OUT_OF_RANGE",
                    entry.page_id,
                    "extent range must stay within [2, next_page_id)",
                ));
            }
            if previous_end.is_some_and(|prev_end| start < prev_end) {
                return Err(corruption(
                    "ALLOCATOR_STATE_OVERLAP",
                    entry.page_id,
                    "allocator extents within one list must be sorted and disjoint",
                ));
            }
            if let Some(previous) = extents.last_mut() {
                if previous.end() == start {
                    previous.nr_pages = (end - u64::from(previous.page_id)) as u32;
                } else {
                    extents.push(entry);
                }
            } else {
                extents.push(entry);
            }
            previous_end = Some(end);
            offset += EXTENT_SIZE;
        }
        current = header.next;
    }

    validate_extent_pages_disjoint(&extents, &pages, corruption)?;
    Ok((extents, pages))
}

fn parse_current_meta(buf: &[u8]) -> Result<Option<MetaNode>> {
    let Ok(s) = MetaNode::decode(buf) else {
        return Ok(None);
    };
    Ok(
        (s.validate().is_ok() && s.magic == MAGIC && s.format_version == FORMAT_VERSION)
            .then_some(s),
    )
}

fn open_meta_corruption(
    code: &'static str,
    generation: Option<u64>,
    check: &'static str,
) -> OpenError {
    OpenError::Corruption(CorruptionReport {
        code,
        generation,
        page_kind: "meta",
        pid: None,
        check,
        expected: None,
        actual: None,
    })
}

fn parse_open_meta(buf: &[u8]) -> Option<MetaNode> {
    let Ok(meta) = MetaNode::decode(buf) else {
        return None;
    };
    (meta.validate().is_ok() && meta.magic == MAGIC && meta.format_version == FORMAT_VERSION)
        .then_some(meta)
}

pub(crate) struct Store {
    file: LiveStore,
    sb: Mutex<MetaNode>,
    shared: SharedMeta,
    reusable: Mutex<ExtentSet>,
    retired: Mutex<ExtentSet>,
    reusable_pages: Mutex<Vec<PageId>>,
    retired_pages: Mutex<Vec<PageId>>,
    file_extended: AtomicBool,
    sync_mode: SyncMode,
}

impl Store {
    pub(crate) fn open<P: AsRef<Path>>(path: P, options: &OpenOptions) -> OpenResult<Self> {
        let path = path.as_ref();
        let (opening, is_new) = OpeningStore::open(path)?;

        let sb = if is_new {
            let mut sb = MetaNode::new();
            opening.reserve_meta_pages()?;
            opening.pwrite_all(sb.as_page_slice(), 0)?;
            sb.seq += 1;
            sb.update_checksum();
            opening.pwrite_all(sb.as_page_slice(), PAGE_SIZE as u64)?;
            opening.sync_all()?;
            opening.sync_parent()?;
            sb
        } else {
            let sb0 = match opening.read_meta_page(0)? {
                Some(buf) => parse_open_meta(&buf),
                None => None,
            };
            let sb1 = match opening.read_meta_page(PAGE_SIZE as u64)? {
                Some(buf) => parse_open_meta(&buf),
                None => None,
            };

            match (sb0, sb1) {
                (Some(s0), Some(s1)) => {
                    if s0.seq >= s1.seq {
                        s0
                    } else {
                        s1
                    }
                }
                (Some(s0), _) => s0,
                (_, Some(s1)) => s1,
                _ => {
                    return Err(open_meta_corruption(
                        "NO_VALID_META",
                        None,
                        "neither meta page is valid",
                    ));
                }
            }
        };

        let (reusable, reusable_pages, retired, retired_pages) = opening.read_allocator_state(
            sb.reusable_root,
            sb.retired_root,
            sb.next_page_id,
            sb.seq,
        )?;
        let file = opening.finish(sb.seq);
        let store = Self {
            file,
            sb: Mutex::new(sb),
            shared: SharedMeta::new(sb.seq, sb.catalog_root),
            reusable: Mutex::new(ExtentSet::from_extents(reusable)),
            retired: Mutex::new(ExtentSet::from_extents(retired)),
            reusable_pages: Mutex::new(reusable_pages),
            retired_pages: Mutex::new(retired_pages),
            file_extended: AtomicBool::new(false),
            sync_mode: options.sync_mode,
        };
        Ok(store)
    }

    fn read_allocator_state_from_disk(
        &self,
        reusable_root: PageId,
        retired_root: PageId,
        next_page_id: PageId,
    ) -> Result<(ExtentSet, Vec<PageId>, ExtentSet, Vec<PageId>)> {
        let (reusable, reusable_pages) = read_extent_pages(
            reusable_root,
            next_page_id,
            |current, buf| {
                self.file
                    .pread_exact(buf, current as u64 * PAGE_SIZE as u64)
            },
            |_, _, _| Error::Corruption,
        )?;
        let (retired, retired_pages) = read_extent_pages(
            retired_root,
            next_page_id,
            |current, buf| {
                self.file
                    .pread_exact(buf, current as u64 * PAGE_SIZE as u64)
            },
            |_, _, _| Error::Corruption,
        )?;
        validate_allocator_sets(
            &reusable,
            &reusable_pages,
            &retired,
            &retired_pages,
            |_, _, _| Error::Corruption,
        )?;
        Ok((
            ExtentSet::from_extents(reusable),
            reusable_pages,
            ExtentSet::from_extents(retired),
            retired_pages,
        ))
    }

    fn extent_pages_needed(entries: usize) -> usize {
        if entries == 0 {
            0
        } else {
            entries.div_ceil(EXTENT_PER_PAGE)
        }
    }

    fn alloc_pages_inner(
        &self,
        sb: &mut MetaNode,
        reusable: &mut ExtentSet,
        nr_pages: u32,
        observer: &dyn PageReuseObserver,
        mut journal: Option<&mut AllocatorMutationJournal>,
    ) -> Result<Vec<PageId>> {
        if nr_pages == 0 {
            return Ok(Vec::new());
        }

        let mut pages = match journal.as_mut() {
            Some(journal) => journal.take_first(ExtentSetKind::Reusable, reusable, nr_pages),
            None => reusable.take_first(nr_pages),
        };
        let needed = u64::from(nr_pages) - pages.len() as u64;

        if needed > 0 {
            let start_id = sb.next_page_id as u64;
            let end_id = start_id + needed;
            if end_id > PageId::MAX as u64 {
                fatal(FatalReason::AddressSpaceExhausted {
                    space: IdSpace::Physical,
                    next: start_id,
                    requested: needed,
                });
            }
            sb.next_page_id = end_id as PageId;
            for i in 0..needed {
                pages.push((start_id + i) as PageId);
            }
            self.file_extended.store(true, Ordering::Relaxed);
        }

        for &pid in pages.iter() {
            observer.invalidate(pid);
        }

        Ok(pages)
    }

    fn write_extent_pages<I>(&self, page_ids: &[PageId], extents: I) -> Result<()>
    where
        I: IntoIterator<Item = Extent>,
    {
        if page_ids.is_empty() {
            return Ok(());
        }
        if EXTENT_PER_PAGE == 0 {
            return Err(Error::Corruption);
        }

        let mut extents = extents.into_iter();
        let mut page = [0u8; PAGE_SIZE];
        for (i, &pid) in page_ids.iter().enumerate() {
            let mut header = ExtentHeader {
                next: if i + 1 < page_ids.len() {
                    page_ids[i + 1]
                } else {
                    0
                },
                count: 0,
            };

            let mut count = 0usize;
            let mut offset = EXTENT_HEADER_SIZE;
            while count < EXTENT_PER_PAGE {
                let Some(entry) = extents.next() else {
                    break;
                };
                page[offset..offset + EXTENT_SIZE].copy_from_slice(entry.as_slice());
                offset += EXTENT_SIZE;
                count += 1;
            }

            header.count = count as u32;
            page[..EXTENT_HEADER_SIZE].copy_from_slice(header.as_slice());

            self.file.pwrite_all(&page, pid as u64 * PAGE_SIZE as u64)?;
            if i + 1 < page_ids.len() {
                page.fill(0);
            }
        }

        if extents.next().is_some() {
            crate::invariant(
                "EXTENT_SERIALIZATION_CAPACITY",
                "allocated extent pages cannot encode every free extent",
            );
        }

        Ok(())
    }

    fn write_allocator_state(
        &self,
        sb: &mut MetaNode,
        reusable: &mut ExtentSet,
        retired: &ExtentSet,
        observer: &dyn PageReuseObserver,
        journal: &mut AllocatorMutationJournal,
    ) -> Result<(Vec<PageId>, Vec<PageId>)> {
        // Allocator-list pages are dependencies of the new generation. Allocate
        // them through the normal allocator and write them before publishing
        // their roots.
        let mut pages = Vec::new();
        const MAX_ROUND: u32 = 32;
        // Allocating list pages consumes reusable extents, which can reduce the
        // number of encoded extent entries. Recompute the layout after each
        // allocation; the bound is only a non-convergence guard, not a page
        // count limit.
        for _ in 0..MAX_ROUND {
            let needed = Self::extent_pages_needed(reusable.len())
                + Self::extent_pages_needed(retired.len());
            if pages.len() >= needed {
                // Keep already reserved pages as empty reusable-list pages when
                // the extent count shrank, rather than leaving their PIDs
                // without an ownership class.
                let reusable_count = Self::extent_pages_needed(reusable.len())
                    .max(pages.len() - Self::extent_pages_needed(retired.len()));
                let retired_pages = pages.split_off(reusable_count);
                self.write_extent_pages(&pages, reusable.iter())?;
                self.write_extent_pages(&retired_pages, retired.iter())?;
                sb.reusable_root = pages.first().copied().unwrap_or(0);
                sb.retired_root = retired_pages.first().copied().unwrap_or(0);
                return Ok((pages, retired_pages));
            }
            pages.extend(self.alloc_pages_inner(
                sb,
                reusable,
                (needed - pages.len()) as u32,
                observer,
                Some(journal),
            )?);
        }
        crate::invariant(
            "ALLOCATOR_LAYOUT_NONCONVERGENT",
            "allocator list page allocation did not converge",
        );
    }

    fn publish_generation(&self, sb: &mut MetaNode) -> Result<()> {
        sb.seq += 1;
        sb.update_checksum();

        // The meta page is an atomic switch only after every referenced page is durable.
        self.sync_impl(false)?;
        let write_offset = if sb.seq.is_multiple_of(2) {
            PAGE_SIZE as u64
        } else {
            0
        };
        self.file.pwrite_all(sb.as_page_slice(), write_offset)?;
        self.sync_publication()?;
        self.file.set_generation(sb.seq);
        self.shared.update(sb.catalog_root, sb.seq);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn commit_roots_with_pending_alloc(
        &self,
        catalog_root: PageId,
        pending_free: &[(PageId, u32)],
        pending_alloc: &HashSet<PageId>,
    ) -> Result<()> {
        let observer = NoopPageReuseObserver;
        self.commit_roots_with_alloc(
            catalog_root,
            pending_free,
            pending_alloc,
            &HashSet::new(),
            &observer,
        )
    }

    pub(crate) fn commit_roots_with_pending_alloc_observed(
        &self,
        catalog_root: PageId,
        pending_free: &[(PageId, u32)],
        pending_alloc: &HashSet<PageId>,
        observer: &dyn PageReuseObserver,
    ) -> Result<()> {
        self.commit_roots_with_alloc(
            catalog_root,
            pending_free,
            pending_alloc,
            &HashSet::new(),
            observer,
        )
    }

    pub(crate) fn commit_generation_only_observed(
        &self,
        catalog_root: PageId,
        deferred_alloc: &HashSet<PageId>,
        observer: &dyn PageReuseObserver,
    ) -> Result<()> {
        self.commit_roots_with_alloc(catalog_root, &[], &HashSet::new(), deferred_alloc, observer)
    }

    fn commit_roots_with_alloc(
        &self,
        catalog_root: PageId,
        pending_free: &[(PageId, u32)],
        adopted_alloc: &HashSet<PageId>,
        deferred_alloc: &HashSet<PageId>,
        observer: &dyn PageReuseObserver,
    ) -> Result<()> {
        let mut sb = self.sb.lock();
        let mut reusable = self.reusable.lock();
        let mut retired = self.retired.lock();
        let mut reusable_pages = self.reusable_pages.lock();
        let mut retired_pages = self.retired_pages.lock();
        let before_sb = *sb;
        let mut journal = AllocatorMutationJournal::new(
            sb.next_page_id,
            self.file_extended.load(Ordering::Relaxed),
        );
        let previous_reusable_pages = std::mem::take(&mut *reusable_pages);
        let previous_retired_pages = std::mem::take(&mut *retired_pages);

        let result = (|| {
            // A generation-only publication keeps the outer transaction's
            // unpublished allocations quarantined. A normal commit adopts
            // those pages into the new roots instead. In both cases remove
            // them from any allocator extent before the new lists are built.
            for &pid in adopted_alloc {
                journal.remove(ExtentSetKind::Reusable, &mut reusable, pid, 1);
                journal.remove(ExtentSetKind::Retired, &mut retired, pid, 1);
            }
            for &pid in deferred_alloc {
                journal.remove(ExtentSetKind::Reusable, &mut reusable, pid, 1);
                journal.remove(ExtentSetKind::Retired, &mut retired, pid, 1);
            }

            // Generation g+1 may reuse the prior generation's quarantine, but all
            // allocator pages reachable from g remain quarantined until g+1 commits.
            let mut next_retired = ExtentSet::default();
            for &(pid, nr) in pending_free {
                next_retired.add(pid, nr);
            }
            // COW can schedule a page after an intermediate rewrite has already
            // recycled it. The durable allocator state remains authoritative, so
            // such duplicate retirements cannot create a second ownership class.
            for free_extent in reusable.iter().chain(retired.iter()) {
                next_retired.remove(free_extent.page_id, free_extent.nr_pages);
            }
            for pid in previous_reusable_pages
                .iter()
                .chain(previous_retired_pages.iter())
            {
                next_retired.add(*pid, 1);
            }
            for &pid in deferred_alloc {
                next_retired.add(pid, 1);
            }
            // Move the current generation's retired extents to reusable state
            // while constructing the next generation.
            for extent in retired.iter() {
                journal.add(
                    ExtentSetKind::Reusable,
                    &mut reusable,
                    extent.page_id,
                    extent.nr_pages,
                );
            }
            let (new_reusable_pages, new_retired_pages) = self.write_allocator_state(
                &mut sb,
                &mut reusable,
                &next_retired,
                observer,
                &mut journal,
            )?;

            sb.catalog_root = catalog_root;
            self.publish_generation(&mut sb)?;
            *retired = next_retired;
            *reusable_pages = new_reusable_pages;
            *retired_pages = new_retired_pages;
            journal.disarm();
            Ok(())
        })();

        if result.is_err() {
            journal.rollback(&mut sb, &mut reusable, &mut retired, &self.file_extended);
            *sb = before_sb;
            *reusable_pages = previous_reusable_pages;
            *retired_pages = previous_retired_pages;
        }
        result
    }

    pub(crate) fn get_seq(&self) -> u64 {
        self.sb.lock().seq
    }

    pub(crate) fn shared_snapshot(&self) -> (u64, PageId) {
        self.shared.snapshot()
    }

    #[cfg(test)]
    pub(crate) fn assert_complete_page_ownership(&self, reachable: &HashSet<PageId>) {
        let sb = *self.sb.lock();
        let reusable = self.reusable.lock();
        let retired = self.retired.lock();
        let reusable_pages = self.reusable_pages.lock();
        let retired_pages = self.retired_pages.lock();

        for pid in 2..sb.next_page_id {
            let is_reachable = reachable.contains(&pid);
            let is_reusable = reusable.contains(pid);
            let is_retired = retired.contains(pid);
            let is_reusable_page = reusable_pages.contains(&pid);
            let is_retired_page = retired_pages.contains(&pid);
            let classes = usize::from(is_reachable)
                + usize::from(is_reusable)
                + usize::from(is_retired)
                + usize::from(is_reusable_page)
                + usize::from(is_retired_page);
            assert_eq!(
                classes, 1,
                "PID {pid} has {classes} ownership classes in generation {}; \
                 reachable={is_reachable}, reusable={is_reusable}, retired={is_retired}, \
                 reusable_page={is_reusable_page}, retired_page={is_retired_page}",
                sb.seq,
            );
        }
    }

    #[cfg(test)]
    pub(crate) fn alloc_pages(&self, nr_pages: u32) -> Result<Vec<PageId>> {
        let observer = NoopPageReuseObserver;
        self.alloc_pages_observed(nr_pages, &observer)
    }

    pub(crate) fn alloc_pages_observed(
        &self,
        nr_pages: u32,
        observer: &dyn PageReuseObserver,
    ) -> Result<Vec<PageId>> {
        let mut sb = self.sb.lock();
        let mut reusable = self.reusable.lock();
        self.alloc_pages_inner(&mut sb, &mut reusable, nr_pages, observer, None)
    }

    pub(crate) fn alloc_data_pages_observed(
        &self,
        nr_pages: u32,
        alloc: &mut HashSet<PageId>,
        observer: &dyn PageReuseObserver,
    ) -> Result<Vec<DataPid>> {
        let pages = self.alloc_pages_observed(nr_pages, observer)?;
        alloc.extend(pages.iter().copied());
        Ok(pages
            .into_iter()
            .map(|page_id| DataPid::new(page_id).unwrap())
            .collect())
    }

    #[cfg(test)]
    pub(crate) fn alloc_data_page(&self, alloc: &mut HashSet<PageId>) -> Result<DataPid> {
        let observer = NoopPageReuseObserver;
        self.alloc_data_page_observed(alloc, &observer)
    }

    pub(crate) fn alloc_data_page_observed(
        &self,
        alloc: &mut HashSet<PageId>,
        observer: &dyn PageReuseObserver,
    ) -> Result<DataPid> {
        let mut sb = self.sb.lock();
        let mut reusable = self.reusable.lock();
        let page_id = self.alloc_page_inner(&mut sb, &mut reusable, observer)?;
        alloc.insert(page_id);
        Ok(DataPid::new(page_id).unwrap())
    }

    fn alloc_page_inner(
        &self,
        sb: &mut MetaNode,
        reusable: &mut ExtentSet,
        observer: &dyn PageReuseObserver,
    ) -> Result<PageId> {
        if let Some((&page_id, &nr_pages)) = reusable.ranges.iter().next() {
            reusable.ranges.remove(&page_id);
            if nr_pages > 1 {
                reusable.ranges.insert(page_id + 1, nr_pages - 1);
            }
            observer.invalidate(page_id);
            return Ok(page_id);
        }

        let start_id = u64::from(sb.next_page_id);
        let end_id = start_id + 1;
        if end_id > PageId::MAX as u64 {
            fatal(FatalReason::AddressSpaceExhausted {
                space: IdSpace::Physical,
                next: start_id,
                requested: 1,
            });
        }
        sb.next_page_id = end_id as PageId;
        self.file_extended.store(true, Ordering::Relaxed);
        let page_id = start_id as PageId;
        observer.invalidate(page_id);
        Ok(page_id)
    }

    pub(crate) fn recycle_allocated_pages_observed(
        &self,
        page_id: PageId,
        nr_pages: u32,
        observer: &dyn PageReuseObserver,
    ) {
        physical_value(
            self.free_pages_observed(page_id, nr_pages, observer),
            "physical transaction page recycle",
        );
    }

    pub(crate) fn free_pages_observed(
        &self,
        page_id: PageId,
        nr_pages: u32,
        observer: &dyn PageReuseObserver,
    ) -> Result<()> {
        if page_id == 0 || nr_pages == 0 {
            return Ok(());
        }

        for i in 0..nr_pages {
            observer.invalidate(page_id + i);
        }

        let mut reusable = self.reusable.lock();
        let mut retired = self.retired.lock();
        // A page allocated by an outer transaction may have been placed in
        // durable quarantine by an intermediate nested-rollback publication.
        // Releasing that outer transaction removes the reservation from the
        // in-memory projection before returning the page to reusable space.
        retired.remove(page_id, nr_pages);
        reusable.add(page_id, nr_pages);

        Ok(())
    }

    fn sync_impl(&self, metadata_changed: bool) -> Result<()> {
        let file_extended = self.file_extended.swap(false, Ordering::SeqCst);
        match self.sync_mode {
            SyncMode::Adaptive if file_extended || metadata_changed => self.file.psync_all(),
            SyncMode::Adaptive | SyncMode::Data => self.file.psync_data(),
            SyncMode::All => self.file.psync_all(),
        }
    }

    fn sync_publication(&self) -> Result<()> {
        match self.sync_mode {
            SyncMode::Adaptive | SyncMode::Data => self.file.psync_data(),
            SyncMode::All => self.file.psync_all(),
        }
    }

    pub(crate) fn read_node(&self, id: DataPid, mut page: AlignedPage) -> Result<Arc<Node>> {
        self.read_data(&[id.get()], page.as_mut_slice())?;
        Ok(self.decode_live_node(id, page))
    }

    pub(crate) fn read_node_without_cache(&self, id: DataPid) -> Arc<Node> {
        // Read directly into an aligned node page for the uncached iterator path.
        let mut page = AlignedPage::new();
        physical_value(
            self.read_data(&[id.get()], page.as_mut_slice()),
            "physical page load",
        );
        self.decode_live_node(id, page)
    }

    pub(crate) fn load_page(&self, id: DataPid) -> Vec<u8> {
        let mut buf = vec![0u8; PAGE_SIZE];
        if let Err(e) = self.read_data(&[id.get()], &mut buf) {
            abort_store_fault(e, "physical page load")
        } else {
            buf
        }
    }

    pub(crate) fn load_data_pids(&self, pages: &[DataPid], len: usize) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; len];
        self.read_page_runs(pages.iter().map(|page_id| page_id.get()), &mut buf)?;
        Ok(buf)
    }

    pub(crate) fn read_data(&self, pages: &[PageId], buf: &mut [u8]) -> Result<()> {
        self.read_page_runs(pages.iter().copied(), buf)
    }

    fn read_page_runs<I>(&self, pages: I, buf: &mut [u8]) -> Result<()>
    where
        I: IntoIterator<Item = PageId>,
    {
        let mut run_start = None;
        let mut run_buf_start = 0usize;
        let mut run_len = 0usize;

        for (page_index, page_id) in pages.into_iter().enumerate() {
            let start = page_index * PAGE_SIZE;
            if start >= buf.len() {
                break;
            }

            let contiguous = run_start
                .is_some_and(|first| u64::from(page_id) == u64::from(first) + run_len as u64);
            if !contiguous {
                if let Some(first) = run_start {
                    self.read_page_run(first, run_buf_start, run_len, buf)?;
                }
                run_start = Some(page_id);
                run_buf_start = page_index;
                run_len = 1;
            } else {
                run_len += 1;
            }
        }

        if let Some(first) = run_start {
            self.read_page_run(first, run_buf_start, run_len, buf)?;
        }
        Ok(())
    }

    fn read_page_run(
        &self,
        first_page: PageId,
        first_buf_page: usize,
        nr_pages: usize,
        buf: &mut [u8],
    ) -> Result<()> {
        let start = first_buf_page * PAGE_SIZE;
        let end = std::cmp::min(start + nr_pages * PAGE_SIZE, buf.len());
        self.file
            .pread_exact(&mut buf[start..end], first_page as u64 * PAGE_SIZE as u64)
    }

    pub(crate) fn write_data(&self, pages: &[PageId], data: &[u8]) -> Result<()> {
        let mut run_start = None;
        let mut run_data_page = 0usize;
        let mut run_len = 0usize;

        for (page_index, &page_id) in pages.iter().enumerate() {
            let start = page_index * PAGE_SIZE;
            if start >= data.len() {
                break;
            }

            let contiguous = run_start
                .is_some_and(|first| u64::from(page_id) == u64::from(first) + run_len as u64);
            if !contiguous {
                if let Some(first) = run_start {
                    self.write_page_run(first, run_data_page, run_len, data)?;
                }
                run_start = Some(page_id);
                run_data_page = page_index;
                run_len = 1;
            } else {
                run_len += 1;
            }
        }

        if let Some(first) = run_start {
            self.write_page_run(first, run_data_page, run_len, data)?;
        }
        Ok(())
    }

    pub(crate) fn write_page_result(&self, page_id: PageId, data: &[u8]) -> Result<()> {
        self.write_data(&[page_id], data)
    }

    pub(crate) fn write_page(&self, id: DataPid, data: &[u8]) {
        physical_value(
            self.write_page_result(id.get(), data),
            "physical page write",
        );
    }

    pub(crate) fn cached_snapshot(&self) -> MetaSnapshot {
        let sb = self.sb.lock();
        MetaSnapshot {
            catalog_root: sb.catalog_root,
            next_page_id: sb.next_page_id,
            reusable_root: sb.reusable_root,
            retired_root: sb.retired_root,
            seq: sb.seq,
        }
    }

    pub(crate) fn refresh_sb(&self) -> Result<MetaSnapshot> {
        let sb0 = self.read_current_meta_candidate(0)?;
        let sb1 = self.read_current_meta_candidate(PAGE_SIZE as u64)?;

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
            let (reusable, reusable_pages, retired, retired_pages) = self
                .read_allocator_state_from_disk(
                    sb.reusable_root,
                    sb.retired_root,
                    sb.next_page_id,
                )?;
            let mut current_sb = self.sb.lock();
            if sb.seq > current_sb.seq {
                *current_sb = sb;
                self.file.set_generation(current_sb.seq);
                self.shared.update(current_sb.catalog_root, current_sb.seq);
                let mut reusable_guard = self.reusable.lock();
                *reusable_guard = reusable;
                let mut retired_guard = self.retired.lock();
                *retired_guard = retired;
                let mut reusable_pages_guard = self.reusable_pages.lock();
                *reusable_pages_guard = reusable_pages;
                let mut retired_pages_guard = self.retired_pages.lock();
                *retired_pages_guard = retired_pages;
                return Ok(MetaSnapshot {
                    catalog_root: current_sb.catalog_root,
                    next_page_id: current_sb.next_page_id,
                    reusable_root: current_sb.reusable_root,
                    retired_root: current_sb.retired_root,
                    seq: current_sb.seq,
                });
            }
        }

        let sb = self.sb.lock();
        Ok(MetaSnapshot {
            catalog_root: sb.catalog_root,
            next_page_id: sb.next_page_id,
            reusable_root: sb.reusable_root,
            retired_root: sb.retired_root,
            seq: sb.seq,
        })
    }

    fn decode_live_node(&self, id: DataPid, page: AlignedPage) -> Arc<Node> {
        Arc::new(Node::from_aligned_page(page).unwrap_or_else(|_| {
            fatal(FatalReason::Corruption(CorruptionReport {
                code: "INVALID_LIVE_NODE",
                generation: Some(self.get_seq()),
                page_kind: "node",
                pid: Some(id.get()),
                check: "node header bounds",
                expected: None,
                actual: None,
            }))
        }))
    }

    fn write_page_run(
        &self,
        first_page: PageId,
        first_data_page: usize,
        nr_pages: usize,
        data: &[u8],
    ) -> Result<()> {
        let start = first_data_page * PAGE_SIZE;
        let end = std::cmp::min(start + nr_pages * PAGE_SIZE, data.len());
        self.file
            .pwrite_all(&data[start..end], first_page as u64 * PAGE_SIZE as u64)
    }

    fn read_current_meta_candidate(&self, offset: u64) -> Result<Option<MetaNode>> {
        let mut buf = [0u8; PAGE_SIZE];
        self.file.pread_exact(&mut buf, offset)?;
        parse_current_meta(&buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    const LIVE_FAULT_CHILD_ENV: &str = "BTREE_STORE_LIVE_FAULT_CHILD";
    const GENERATION_FAULT_CHILD_PATH: &str = "BTREE_STORE_GENERATION_FAULT_CHILD_PATH";

    fn parse_runner(value: &str) -> Vec<String> {
        value
            .split_whitespace()
            .map(str::to_owned)
            .collect::<Vec<_>>()
    }

    fn detect_cargo_runner() -> Option<Vec<String>> {
        let arch = std::env::consts::ARCH
            .to_ascii_uppercase()
            .replace('-', "_");
        let mut preferred = Vec::new();
        let mut all = Vec::new();

        for (key, value) in std::env::vars() {
            if !key.starts_with("CARGO_TARGET_") || !key.ends_with("_RUNNER") {
                continue;
            }
            if value.trim().is_empty() {
                continue;
            }

            all.push((key.clone(), value.clone()));
            if key.contains(&format!("_{arch}_")) {
                preferred.push((key, value));
            }
        }

        preferred.sort_by(|a, b| a.0.cmp(&b.0));
        all.sort_by(|a, b| a.0.cmp(&b.0));

        if let Some((_, value)) = preferred.into_iter().next() {
            return Some(parse_runner(&value)).filter(|parts| !parts.is_empty());
        }
        if all.len() == 1 {
            return Some(parse_runner(&all[0].1)).filter(|parts| !parts.is_empty());
        }
        None
    }

    fn child_test_command(exe: &Path) -> Command {
        match detect_cargo_runner() {
            Some(parts) => {
                let mut it = parts.into_iter();
                let mut cmd = Command::new(it.next().expect("runner must not be empty"));
                cmd.args(it);
                cmd.arg(exe);
                cmd
            }
            None => Command::new(exe),
        }
    }

    fn extent_contains(extents: &ExtentSet, pid: PageId) -> bool {
        extents.contains(pid)
    }

    fn assert_empty_store_allocator_complete(store: &Store) {
        let sb = *store.sb.lock();
        assert_eq!(sb.catalog_root, 0);
        let reusable = store.reusable.lock();
        let retired = store.retired.lock();
        let reusable_pages = store.reusable_pages.lock();
        let retired_pages = store.retired_pages.lock();
        for pid in 2..sb.next_page_id {
            assert!(
                extent_contains(&reusable, pid)
                    || extent_contains(&retired, pid)
                    || reusable_pages.contains(&pid)
                    || retired_pages.contains(&pid),
                "PID {pid} is orphaned in generation {}",
                sb.seq
            );
        }
    }

    fn encode_extent_page(next: PageId, extents: &[Extent]) -> [u8; PAGE_SIZE] {
        assert!(extents.len() <= EXTENT_PER_PAGE);
        let mut page = [0u8; PAGE_SIZE];
        let header = ExtentHeader {
            next,
            count: extents.len() as u32,
        };
        page[..EXTENT_HEADER_SIZE].copy_from_slice(header.as_slice());
        let mut offset = EXTENT_HEADER_SIZE;
        for extent in extents {
            page[offset..offset + EXTENT_SIZE].copy_from_slice(extent.as_slice());
            offset += EXTENT_SIZE;
        }
        page
    }

    #[test]
    fn extent_set_merges_splits_and_allocates_in_order() {
        let mut set = ExtentSet::default();
        set.add(10, 3);
        set.add(20, 2);
        set.add(13, 7);
        assert_eq!(
            set.to_vec(),
            vec![Extent {
                page_id: 10,
                nr_pages: 12,
            }]
        );

        set.remove(14, 3);
        assert_eq!(
            set.to_vec(),
            vec![
                Extent {
                    page_id: 10,
                    nr_pages: 4,
                },
                Extent {
                    page_id: 17,
                    nr_pages: 5,
                },
            ]
        );

        assert_eq!(set.take_first(5), vec![10, 11, 12, 13, 17]);
        assert_eq!(
            set.to_vec(),
            vec![Extent {
                page_id: 18,
                nr_pages: 4,
            }]
        );
    }

    #[test]
    fn allocator_mutation_journal_restores_local_changes() {
        let mut reusable = ExtentSet::from_extents(vec![
            Extent {
                page_id: 10,
                nr_pages: 10,
            },
            Extent {
                page_id: 40,
                nr_pages: 4,
            },
        ]);
        let mut retired = ExtentSet::from_extents(vec![Extent {
            page_id: 70,
            nr_pages: 3,
        }]);
        let original_reusable = reusable.clone();
        let original_retired = retired.clone();
        let mut sb = MetaNode::new();
        sb.next_page_id = 100;
        let file_extended = AtomicBool::new(false);
        let mut journal = AllocatorMutationJournal::new(100, false);

        journal.remove(ExtentSetKind::Reusable, &mut reusable, 12, 3);
        journal.add(ExtentSetKind::Retired, &mut retired, 80, 2);
        journal.take_first(ExtentSetKind::Reusable, &mut reusable, 4);
        sb.next_page_id = 123;
        file_extended.store(true, Ordering::Relaxed);

        journal.rollback(&mut sb, &mut reusable, &mut retired, &file_extended);

        assert_eq!(reusable, original_reusable);
        assert_eq!(retired, original_retired);
        assert_eq!(sb.next_page_id, 100);
        assert!(!file_extended.load(Ordering::Relaxed));
    }

    #[test]
    fn read_extent_pages_merges_adjacent_extents_while_streaming() {
        let page = encode_extent_page(
            0,
            &[
                Extent {
                    page_id: 10,
                    nr_pages: 2,
                },
                Extent {
                    page_id: 12,
                    nr_pages: 3,
                },
                Extent {
                    page_id: 20,
                    nr_pages: 1,
                },
            ],
        );

        let (extents, pages) = read_extent_pages(
            2,
            64,
            |pid, buf| {
                assert_eq!(pid, 2);
                *buf = page;
                Ok::<_, (&'static str, PageId, &'static str)>(())
            },
            |code, pid, check| (code, pid, check),
        )
        .unwrap();

        assert_eq!(pages, vec![2]);
        assert_eq!(
            extents,
            vec![
                Extent {
                    page_id: 10,
                    nr_pages: 5,
                },
                Extent {
                    page_id: 20,
                    nr_pages: 1,
                },
            ]
        );
    }

    #[test]
    fn read_extent_pages_rejects_unsorted_extents() {
        let page = encode_extent_page(
            0,
            &[
                Extent {
                    page_id: 20,
                    nr_pages: 1,
                },
                Extent {
                    page_id: 10,
                    nr_pages: 1,
                },
            ],
        );

        let err = read_extent_pages(
            2,
            64,
            |_, buf| {
                *buf = page;
                Ok::<_, (&'static str, PageId, &'static str)>(())
            },
            |code, pid, check| (code, pid, check),
        )
        .unwrap_err();

        assert_eq!(
            err,
            (
                "ALLOCATOR_STATE_OVERLAP",
                10,
                "allocator extents within one list must be sorted and disjoint",
            )
        );
    }

    #[test]
    fn read_extent_pages_rejects_extent_covering_later_allocator_page() {
        let root_page = encode_extent_page(
            5,
            &[Extent {
                page_id: 4,
                nr_pages: 2,
            }],
        );
        let next_page = encode_extent_page(0, &[]);

        let err = read_extent_pages(
            2,
            64,
            |pid, buf| {
                *buf = match pid {
                    2 => root_page,
                    5 => next_page,
                    _ => unreachable!(),
                };
                Ok::<_, (&'static str, PageId, &'static str)>(())
            },
            |code, pid, check| (code, pid, check),
        )
        .unwrap_err();

        assert_eq!(
            err,
            (
                "ALLOCATOR_STATE_OVERLAP",
                4,
                "allocator extents must not cover allocator list pages",
            )
        );
    }

    #[test]
    fn opening_waits_for_a_transient_lock_release() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("transient-lock.db");
        let holder = FileOpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .unwrap();
        holder.try_lock().unwrap();

        let release = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            drop(holder);
        });

        let (raw, is_new) = RawFile::open(&path).unwrap();
        assert!(is_new);
        drop(raw);
        release.join().unwrap();
    }

    fn reopen_test_store(path: &Path, options: &crate::OpenOptions) -> Store {
        for _ in 0..1_000 {
            match Store::open(path, options) {
                Ok(store) => return store,
                Err(OpenError::DatabaseBusy { .. }) => {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                Err(error) => panic!("test store reopen failed: {error:?}"),
            }
        }
        panic!("test store lock remained busy after prior handle drop")
    }

    fn reopen_test_tree(path: &Path) -> crate::BTree {
        for _ in 0..1_000 {
            match crate::BTree::open(path) {
                Ok(tree) => return tree,
                Err(OpenError::DatabaseBusy { .. }) => {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                Err(error) => panic!("test tree reopen failed: {error:?}"),
            }
        }
        panic!("test tree lock remained busy after prior handle drop")
    }

    #[test]
    fn durable_retired_state_survives_reopen_and_is_promoted_without_orphans() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("durable-retired.db");
        let options = crate::OpenOptions::default();

        let store = reopen_test_store(&path, &options);
        let retired_pids = store.alloc_pages(3).unwrap();
        assert_eq!(retired_pids, vec![2, 3, 4]);
        store
            .commit_roots_with_pending_alloc(
                0,
                &[(retired_pids[0], retired_pids.len() as u32)],
                &HashSet::new(),
            )
            .unwrap();
        assert_empty_store_allocator_complete(&store);
        drop(store);

        let store = reopen_test_store(&path, &options);
        assert!(
            retired_pids
                .iter()
                .all(|pid| extent_contains(&store.retired.lock(), *pid))
        );
        assert_empty_store_allocator_complete(&store);
        store
            .commit_roots_with_pending_alloc(0, &[], &HashSet::new())
            .unwrap();
        drop(store);

        let store = reopen_test_store(&path, &options);
        assert_empty_store_allocator_complete(&store);
        let reused = store.alloc_pages(1).unwrap();
        assert!(retired_pids.contains(&reused[0]));
    }

    #[test]
    fn alloc_pages_consumes_reusable_extents_in_page_id_order() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = reopen_test_store(
            &dir.path().join("ordered-allocation.db"),
            &OpenOptions::default(),
        );

        {
            let mut sb = store.sb.lock();
            sb.next_page_id = 200;
        }
        let expected: Vec<PageId> = (0..65).map(|index| 10 + index * 2).collect();
        *store.reusable.lock() = ExtentSet::from_extents(
            (0..66)
                .map(|index| Extent {
                    page_id: 10 + index * 2,
                    nr_pages: 1,
                })
                .collect(),
        );

        assert_eq!(store.alloc_pages(65).unwrap(), expected);
        let remaining = store.reusable.lock();
        assert_eq!(remaining.len(), 1);
        assert_eq!(
            remaining.iter().next().unwrap(),
            Extent {
                page_id: 10 + 65 * 2,
                nr_pages: 1,
            }
        );
    }

    #[test]
    #[ignore = "subprocess target for generation publication crash cuts"]
    fn generation_fault_child() {
        let Ok(path) = std::env::var(GENERATION_FAULT_CHILD_PATH) else {
            return;
        };
        let mut options = crate::OpenOptions::new();
        options.sync_mode = SyncMode::All;
        let tree = options.open(path).unwrap();
        tree.exec("bucket", |txn| txn.put(b"new", b"new-value"))
            .unwrap();
    }

    fn verify_old_or_new_generation(path: &Path) {
        let tree = reopen_test_tree(path);
        tree.view("bucket", |txn| {
            assert_eq!(txn.get(b"stable").unwrap(), b"stable-value");
            match txn.get(b"new") {
                Ok(value) => assert_eq!(value, b"new-value"),
                Err(crate::Error::KeyNotFound) => {}
                Err(error) => panic!("unexpected generation result: {error:?}"),
            }
            Ok::<_, crate::Error>(())
        })
        .unwrap();
        tree.exec("bucket", |txn| txn.put(b"continued", b"ok"))
            .unwrap();
        drop(tree);

        let reopened = reopen_test_tree(path);
        reopened
            .view("bucket", |txn| {
                assert_eq!(txn.get(b"stable").unwrap(), b"stable-value");
                assert_eq!(txn.get(b"continued").unwrap(), b"ok");
                Ok::<_, crate::Error>(())
            })
            .unwrap();
    }

    fn exercise_generation_fault_cuts(
        dir: &Path,
        baseline: &Path,
        baseline_generation: u64,
        operation: &str,
        max_occurrence: usize,
    ) -> usize {
        let mut saw_failure = false;
        for occurrence in 1..=max_occurrence {
            let path = dir.join(format!("{operation}-{occurrence}.db"));
            std::fs::copy(baseline, &path).unwrap();
            let output = child_test_command(&std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "store::tests::generation_fault_child",
                    "--ignored",
                    "--nocapture",
                ])
                .env(GENERATION_FAULT_CHILD_PATH, &path)
                .env(TEST_LIVE_FAULT_ENV, format!("{operation}:{occurrence}:5"))
                .env("LSAN_OPTIONS", "detect_leaks=0")
                .output()
                .unwrap();
            if output.status.success() {
                assert!(saw_failure, "{operation} had no injectable cut");
                return occurrence - 1;
            }
            saw_failure = true;
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(
                stderr.contains("code=BTREE_FATAL_IO")
                    && stderr.contains(&format!("operation={operation}"))
                    && stderr.contains(&format!("generation={baseline_generation}")),
                "{stderr}"
            );
            verify_old_or_new_generation(&path);
        }
        panic!("{operation} still failed after {max_occurrence} occurrences");
    }

    #[test]
    fn dependency_writes_and_two_sync_publication_recover_old_or_new_generation() {
        let dir = tempfile::TempDir::new().unwrap();
        let baseline = dir.path().join("baseline.db");
        {
            let mut options = crate::OpenOptions::new();
            options.sync_mode = SyncMode::All;
            let tree = options.open(&baseline).unwrap();
            tree.new_bucket("bucket", false).unwrap();
            tree.exec("bucket", |txn| txn.put(b"stable", b"stable-value"))
                .unwrap();
        }
        let baseline_generation = latest_test_meta(&baseline).seq;

        let _pwrite_cuts = exercise_generation_fault_cuts(
            dir.path(),
            &baseline,
            baseline_generation,
            "pwrite",
            32,
        );
        let sync_cuts = exercise_generation_fault_cuts(
            dir.path(),
            &baseline,
            baseline_generation,
            "sync_all",
            4,
        );
        assert!(
            sync_cuts >= 2,
            "dependency and meta publication must have separate sync cuts"
        );
    }

    fn latest_test_meta(path: &Path) -> MetaNode {
        let file = FileOpenOptions::new().read(true).open(path).unwrap();
        let mut first = [0u8; PAGE_SIZE];
        let mut second = [0u8; PAGE_SIZE];
        file.pread_exact(&mut first, 0).unwrap();
        file.pread_exact(&mut second, PAGE_SIZE as u64).unwrap();
        let first = MetaNode::from_slice(&first);
        let second = MetaNode::from_slice(&second);
        if first.validate().is_ok()
            && (!matches!(second.validate(), Ok(())) || first.seq >= second.seq)
        {
            first
        } else {
            second
        }
    }

    fn raw_file_with_fault(
        operation: &'static str,
        raw_os_error: i32,
    ) -> (tempfile::TempDir, RawFile) {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("fault.db");
        let file = FileOpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .unwrap();
        (
            dir,
            RawFile {
                file,
                path: Arc::new(path),
                fault: Some(TestFault {
                    operation,
                    raw_os_error,
                    remaining: Arc::new(std::sync::atomic::AtomicUsize::new(1)),
                }),
            },
        )
    }

    #[test]
    fn opening_io_error_preserves_context_and_source() {
        let (_dir, raw) = raw_file_with_fault("pread", 5);
        let opening = OpeningStore { raw };
        let mut buf = [0u8; 32];
        let error = opening.pread_exact(&mut buf, 4096).unwrap_err();
        let OpenError::Io(error) = error else {
            panic!("expected opening I/O error");
        };
        assert_eq!(error.operation, "pread");
        assert_eq!(error.offset, Some(4096));
        assert_eq!(error.length, Some(32));
        assert_eq!(error.source_error().raw_os_error(), Some(5));
        assert!(error.path.ends_with("fault.db"));
    }

    #[test]
    #[ignore = "subprocess target for live fatal matrix"]
    fn live_fault_child() {
        let Ok(operation) = std::env::var(LIVE_FAULT_CHILD_ENV) else {
            return;
        };
        let operation: &'static str = match operation.as_str() {
            "pread" => "pread",
            "pwrite" => "pwrite",
            "sync_all" => "sync_all",
            "sync_data" => "sync_data",
            other => panic!("unknown child operation: {other}"),
        };
        let raw_os_error = if operation == "pwrite" {
            #[cfg(unix)]
            {
                28
            }
            #[cfg(windows)]
            {
                112
            }
        } else {
            5
        };
        let (_dir, raw) = raw_file_with_fault(operation, raw_os_error);
        let live = LiveStore {
            raw,
            generation: AtomicU64::new(41),
        };

        match operation {
            "pread" => {
                let mut buf = [0u8; 16];
                let _ = live.pread_exact(&mut buf, 8192);
            }
            "pwrite" => {
                let _ = live.pwrite_all(&[0u8; 16], 12288);
            }
            "sync_all" => {
                let _ = live.psync_all();
            }
            "sync_data" => {
                let _ = live.psync_data();
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn live_io_faults_abort_with_diagnostics_and_cannot_unwind() {
        for operation in ["pread", "pwrite", "sync_all", "sync_data"] {
            let output = child_test_command(&std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "store::tests::live_fault_child",
                    "--ignored",
                    "--nocapture",
                ])
                .env(LIVE_FAULT_CHILD_ENV, operation)
                .output()
                .unwrap();

            assert!(
                !output.status.success(),
                "{operation} fatal was caught or returned normally"
            );
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(stderr.contains("code=BTREE_FATAL_IO"), "{stderr}");
            assert!(
                stderr.contains(&format!("operation={operation}")),
                "{stderr}"
            );
            assert!(stderr.contains("path="), "{stderr}");
            assert!(stderr.contains("generation=41"), "{stderr}");
            assert!(stderr.contains("source_kind="), "{stderr}");
            assert!(stderr.contains("os_error="), "{stderr}");
            if operation == "pwrite" {
                #[cfg(unix)]
                let expected_os_error = 28;
                #[cfg(windows)]
                let expected_os_error = 112;
                assert!(
                    stderr.contains(&format!("os_error={expected_os_error}")),
                    "{stderr}"
                );
            }
        }
    }
}
