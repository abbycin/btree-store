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
    CorruptionReport, CorruptionSite, DataPid, FORMAT_VERSION, FatalReason, IdSpace, IoFault,
    MAGIC, MetaNode, OpenError, OpenIoError, OpenOptions, OpenResult, PageId, Snapshot,
    StoreFault as Error, StoreResult as Result, SyncMode, abort_store_fault,
    epoch::EpochRegistry,
    fatal,
    node::{AlignedPage, Node, PAGE_SIZE},
    physical_value,
};

pub(crate) trait PageReuseObserver: Send + Sync {
    fn invalidate(&self, page_id: PageId);
}

/// Test-only synchronization hook: when armed for a specific thread, a commit
/// on that thread waits here after the epoch scan decides promotion and before
/// the promotion loop runs. Lets a deterministic test place a reader inside
/// the scan-to-publish window without affecting commits on other threads
/// (tests run in parallel within one binary).
#[cfg(test)]
pub(crate) struct AfterOldestScanHook {
    thread_id: std::thread::ThreadId,
    reached: std::sync::mpsc::Sender<()>,
    resume: Mutex<std::sync::mpsc::Receiver<()>>,
}

#[cfg(test)]
pub(crate) static AFTER_OLDEST_SCAN: Mutex<Option<Arc<AfterOldestScanHook>>> = Mutex::new(None);

/// Test-only publication-point hook (modelled on `AFTER_OLDEST_SCAN`): invoked with
/// the live high-water mark, the live file length, the sequence being published and the meta slot's
/// byte offset immediately **before** the meta slot is written, so a test can assert the
/// dense-length invariant at exactly that instant — and, from the slot's on-disk bytes, that the
/// publication is not durable yet. The values are passed in rather than read by the hook, because
/// the caller already holds the superblock lock.
#[cfg(test)]
type Mtx = Mutex<Option<Arc<dyn Fn(PublicationPoint) + Send + Sync>>>;
#[cfg(test)]
pub(crate) static BEFORE_META_SLOT_WRITE: Mtx = Mutex::new(None);

/// What `BEFORE_META_SLOT_WRITE` reports about the publication it is about to make durable.
#[cfg(test)]
#[derive(Clone, Copy, Debug)]
pub(crate) struct PublicationPoint {
    pub(crate) next_page_id: PageId,
    pub(crate) file_len: u64,
    pub(crate) seq: u64,
    pub(crate) write_offset: u64,
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
pub(crate) const TEST_LIVE_FAULT_ENV: &str = "BTREE_STORE_TEST_LIVE_FAULT";

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
    /// Opens the file: read-only handles ask for no write permission, never
    /// create the file, and take a shared lock so that several readers can
    /// coexist without excluding each other.
    fn open(path: &Path, read_only: bool) -> OpenResult<(Self, bool)> {
        let file = FileOpenOptions::new()
            .read(true)
            .write(!read_only)
            .create(!read_only)
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
            let attempt = if read_only {
                file.try_lock_shared()
            } else {
                file.try_lock()
            };
            match attempt {
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
    fn open(path: &Path, read_only: bool) -> OpenResult<(Self, bool)> {
        let (raw, is_new) = RawFile::open(path, read_only)?;
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

    fn corruption_site(
        &self,
        site: CorruptionSite,
        generation: Option<u64>,
        page_kind: &'static str,
    ) -> OpenError {
        OpenError::Corruption(site.report(generation, page_kind))
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
            |site| self.corruption_site(site, Some(generation), "extent"),
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
            |site| self.corruption_site(site, Some(generation), "extent"),
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
    state: RwLock<MetaSnapshot>,
}

impl SharedMeta {
    fn new(snapshot: MetaSnapshot) -> Self {
        Self {
            state: RwLock::new(snapshot),
        }
    }

    fn update(&self, snapshot: MetaSnapshot) {
        *self.state.write() = snapshot;
    }

    fn snapshot(&self) -> MetaSnapshot {
        *self.state.read()
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct ExtentHeader {
    /// CRC32C of the whole page with this field read as zero; first field, like
    /// every other page header in the format (see `crate::page`).
    checksum: u32,
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
/// Capacity check for the extent page: the header plus the entry slots it can
/// hold must fit. The checksum covers the whole page, so the entry slots an
/// extent leaves unused - and the rest of the page - are zeroed by the writer.
const EXTENT_ENTRIES_END: usize = PAGE_SIZE;
const EXTENT_PER_PAGE: usize = (EXTENT_ENTRIES_END - EXTENT_HEADER_SIZE) / EXTENT_SIZE;
const _: () = assert!(EXTENT_HEADER_SIZE + EXTENT_PER_PAGE * EXTENT_SIZE <= EXTENT_ENTRIES_END);

fn validate_allocator_page_id<E, C>(
    pid: PageId,
    next_page_id: PageId,
    code: &'static str,
    check: &'static str,
    corruption: &mut C,
) -> std::result::Result<(), E>
where
    C: FnMut(CorruptionSite) -> E,
{
    if !(2..next_page_id).contains(&pid) {
        return Err(corruption(CorruptionSite::structure(pid, code, check)));
    }
    Ok(())
}

fn validate_extent_pages_disjoint<E, C>(
    extents: &[Extent],
    pages: &[PageId],
    mut corruption: C,
) -> std::result::Result<(), E>
where
    C: FnMut(CorruptionSite) -> E,
{
    for extent in extents {
        let start = u64::from(extent.page_id);
        let end = extent.end();
        if pages
            .iter()
            .any(|page_id| start <= u64::from(*page_id) && u64::from(*page_id) < end)
        {
            return Err(corruption(CorruptionSite::structure(
                extent.page_id,
                "ALLOCATOR_STATE_OVERLAP",
                "allocator extents must not cover allocator list pages",
            )));
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
    C: FnMut(CorruptionSite) -> E,
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
            return Err(corruption(CorruptionSite::structure(
                pid,
                "ALLOCATOR_STATE_OVERLAP",
                "allocator ownership classes must be disjoint",
            )));
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
    C: FnMut(CorruptionSite) -> E,
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
            return Err(corruption(CorruptionSite::structure(
                current,
                "EXTENT_CYCLE",
                "extent chain must be acyclic",
            )));
        }
        pages.push(current);

        let mut buf = [0u8; PAGE_SIZE];
        read_page(current, &mut buf)?;
        // Verify the checksum before interpreting the header.
        if let Err(mismatch) =
            crate::page::verify_page(&buf, crate::page::HEADER_CRC_OFFSET, current)
        {
            return Err(corruption(CorruptionSite::crc(
                current,
                "PAGE_CRC_MISMATCH",
                "crc32c over the whole page",
                mismatch.expected,
                mismatch.actual,
            )));
        }
        // An out-of-range count is a structural failure, not a checksum mismatch.
        let header = ExtentHeader::from_slice(&buf);
        if header.count as usize > EXTENT_PER_PAGE {
            return Err(corruption(CorruptionSite::structure(
                current,
                "INVALID_EXTENT_COUNT",
                "extent count exceeds page capacity",
            )));
        }

        let mut offset = EXTENT_HEADER_SIZE;
        for _ in 0..header.count {
            let entry = Extent::from_slice(&buf[offset..offset + EXTENT_SIZE]);
            if entry.page_id == 0 || entry.nr_pages == 0 {
                return Err(corruption(CorruptionSite::structure(
                    current,
                    "INVALID_EXTENT_ENTRY",
                    "extent entry page and length must be non-zero",
                )));
            }
            let start = u64::from(entry.page_id);
            let end = start
                .checked_add(u64::from(entry.nr_pages))
                .ok_or_else(|| {
                    corruption(CorruptionSite::structure(
                        entry.page_id,
                        "EXTENT_OUT_OF_RANGE",
                        "extent end must use checked arithmetic within next_page_id",
                    ))
                })?;
            if start < 2 || end > u64::from(next_page_id) {
                return Err(corruption(CorruptionSite::structure(
                    entry.page_id,
                    "EXTENT_OUT_OF_RANGE",
                    "extent range must stay within [2, next_page_id)",
                )));
            }
            if previous_end.is_some_and(|prev_end| start < prev_end) {
                return Err(corruption(CorruptionSite::structure(
                    entry.page_id,
                    "ALLOCATOR_STATE_OVERLAP",
                    "allocator extents within one list must be sorted and disjoint",
                )));
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

#[derive(Clone, Copy, Debug)]
enum MetaCandidate {
    /// Checksum, magic and format version are valid for this binary.
    Supported(MetaNode),
    /// Checksum and magic are valid, but the format version is not one this
    /// binary may interpret. This is its own outcome, never "invalid slot", so
    /// a mixed or newer file is rejected instead of silently falling back to
    /// the other slot.
    UnsupportedVersion(u32),
    /// Torn write, all-zero page, wrong magic or bad checksum.
    Invalid,
}

enum MetaSelection {
    Selected(MetaNode),
    /// No slot is usable at all (both torn/all-zero/bad magic/bad checksum).
    None,
    /// At least one slot carries a valid checksum under an unsupported version.
    UnsupportedVersion {
        found: u32,
    },
    /// One slot is supported while another valid slot disagrees on the version.
    MixedVersions {
        found: u32,
    },
}

fn meta_candidate(buf: &[u8]) -> MetaCandidate {
    let Ok(meta) = MetaNode::decode(buf) else {
        return MetaCandidate::Invalid;
    };
    if meta.validate().is_err() || meta.magic != MAGIC {
        return MetaCandidate::Invalid;
    }
    if meta.format_version == FORMAT_VERSION {
        MetaCandidate::Supported(meta)
    } else {
        MetaCandidate::UnsupportedVersion(meta.format_version)
    }
}

/// Selects the generation to use, or reports why no generation may be used.
///
/// Same version: highest `seq`, ties keep slot A (the existing recovery rule).
/// Any checksum-valid slot under a different version: refuse, never downgrade
/// to the other slot and never interpret the file with the wrong capacities.
fn select_meta(slots: [MetaCandidate; 2]) -> MetaSelection {
    let mut supported = Vec::new();
    let mut unsupported = Vec::new();
    for slot in slots {
        match slot {
            MetaCandidate::Supported(meta) => supported.push(meta),
            MetaCandidate::UnsupportedVersion(found) => unsupported.push(found),
            MetaCandidate::Invalid => {}
        }
    }

    if let Some(found) = unsupported.first().copied() {
        return if supported.is_empty() {
            MetaSelection::UnsupportedVersion { found }
        } else {
            MetaSelection::MixedVersions { found }
        };
    }

    let Some(mut chosen) = supported.first().copied() else {
        return MetaSelection::None;
    };
    for candidate in &supported[1..] {
        if candidate.seq > chosen.seq {
            chosen = *candidate;
        }
    }
    MetaSelection::Selected(chosen)
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

fn open_meta_corruption_with(
    code: &'static str,
    generation: Option<u64>,
    check: &'static str,
    expected: Option<String>,
    actual: Option<String>,
) -> OpenError {
    OpenError::Corruption(CorruptionReport {
        code,
        generation,
        page_kind: "meta",
        pid: None,
        check,
        expected: expected.map(Into::into),
        actual: actual.map(Into::into),
    })
}

/// Compares open handles, so replacing a pathname cannot change either identity.
fn same_file_handles(a: &std::fs::File, b: &std::fs::File) -> io::Result<bool> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let a = a.metadata()?;
        let b = b.metadata()?;
        Ok((a.dev(), a.ino()) == (b.dev(), b.ino()))
    }
    #[cfg(windows)]
    {
        use std::os::windows::io::AsRawHandle;
        #[repr(C)]
        #[derive(Default)]
        struct FileInformation {
            attributes: u32,
            creation_time: [u32; 2],
            access_time: [u32; 2],
            write_time: [u32; 2],
            volume: u32,
            size_high: u32,
            size_low: u32,
            links: u32,
            index_high: u32,
            index_low: u32,
        }
        #[link(name = "kernel32")]
        unsafe extern "system" {
            fn GetFileInformationByHandle(
                handle: *mut std::ffi::c_void,
                information: *mut FileInformation,
            ) -> i32;
        }
        let identity = |file: &std::fs::File| -> io::Result<(u32, u32, u32)> {
            let mut information = FileInformation::default();
            // The borrowed handle stays open and the output has the Win32 structure layout.
            if unsafe { GetFileInformationByHandle(file.as_raw_handle(), &mut information) } == 0 {
                return Err(io::Error::last_os_error());
            }
            Ok((
                information.volume,
                information.index_high,
                information.index_low,
            ))
        };
        Ok(identity(a)? == identity(b)?)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (a, b);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "file identity is unavailable",
        ))
    }
}

/// A snapshot must reject aliases too: a hard link would truncate its source.
fn is_same_file(
    dst_file: &std::fs::File,
    source: &std::fs::File,
    dst: &Path,
    _source_path: &Path,
) -> OpenResult<bool> {
    same_file_handles(dst_file, source)
        .map_err(|error| snapshot_io_error("file identity", dst, error))
}

/// Test-only node-class event stream: `alloc` at the PID hand-out,
/// `write` at the single `write_physical_page` production call site, `read` at the
/// `read_node_pages` exit. Events are sequenced so a write can be attributed to its
/// **incarnation** — the PID's most recent preceding `alloc` — because `merge_pending` returns
/// a freed PID to `reusable` inside the same transaction and the next allocation can hand the
/// same PID out again; a bare PID cannot tell those two legitimate writes apart.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct NodeIoLog {
    events: Mutex<Vec<NodeIoEvent>>,
    flushes: Mutex<Vec<FlushStats>>,
    cover_calls: Mutex<usize>,
}

#[cfg(test)]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct FlushStats {
    pub(crate) dirty_pages: usize,
    pub(crate) runs: usize,
    /// The PIDs the flush handed over, so a test can assert the `Dirty` tier it observed is a subset
    /// of what actually reached the disk.
    pub(crate) pids: Vec<PageId>,
}

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum NodeIoKind {
    Alloc,
    Write,
    Read,
    Cover,
    /// Shared-cache backfill. Tests assert zero of these for a transaction's own
    /// pages, which is the observable form of "transaction-private pages never enter the cache".
    CachePut,
    /// A read answered from a resident overlay entry (an overlay hit).
    /// Recorded separately from a shared-cache hit, so a warmed `NodeCache` can never be mistaken
    /// for overlay residency.
    OverlayHit,
    Release,
    OverlayMiss,
    /// Page-class events, counted separately so the `strace` total is
    /// decomposable into classes: `Write`/`Read` above stay **node-only**, while
    /// these carry the other classes and never merge into them.
    ValueWrite,
    IndirectWrite,
    AllocatorWrite,
    MetaWrite,
    ValueRead,
    IndirectRead,
    MetaRead,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug)]
pub(crate) struct NodeIoEvent {
    pub(crate) kind: NodeIoKind,
    pub(crate) pid: PageId,
}

#[cfg(test)]
impl NodeIoLog {
    fn record(&self, kind: NodeIoKind, pid: PageId) {
        self.events.lock().push(NodeIoEvent { kind, pid });
    }

    pub(crate) fn record_alloc(&self, pid: PageId) {
        self.record(NodeIoKind::Alloc, pid);
    }

    pub(crate) fn record_write(&self, pid: PageId) {
        self.record(NodeIoKind::Write, pid);
    }

    pub(crate) fn record_read(&self, pid: PageId) {
        self.record(NodeIoKind::Read, pid);
    }

    /// Dense-coverage zero-page write. It is recorded with the PID whose slot it
    /// fills so a report can name the offset, but it never counts as a node write.
    pub(crate) fn record_cover(&self, pid: PageId) {
        self.record(NodeIoKind::Cover, pid);
    }

    pub(crate) fn covers(&self) -> Vec<PageId> {
        self.class_pids(NodeIoKind::Cover)
    }

    /// Records a shared-cache backfill at the point *before* `cache.put`.
    pub(crate) fn record_cache_put(&self, pid: PageId) {
        self.record(NodeIoKind::CachePut, pid);
    }

    pub(crate) fn cache_puts(&self) -> Vec<PageId> {
        self.class_pids(NodeIoKind::CachePut)
    }

    /// Records a released PID (the third event class, at the `drop_overlay_pages`
    /// choke point every leaving point goes through).
    pub(crate) fn record_release(&self, pid: PageId) {
        self.record(NodeIoKind::Release, pid);
    }

    pub(crate) fn releases(&self) -> Vec<PageId> {
        self.class_pids(NodeIoKind::Release)
    }

    /// Records a whole page class at once (per-class evidence).
    pub(crate) fn record_many(&self, kind: NodeIoKind, pids: &[PageId]) {
        for pid in pids {
            self.record(kind, *pid);
        }
    }

    /// Every PID recorded for one class, in order.
    pub(crate) fn class_pids(&self, kind: NodeIoKind) -> Vec<PageId> {
        self.events()
            .into_iter()
            .filter(|event| event.kind == kind)
            .map(|event| event.pid)
            .collect()
    }

    /// Records an overlay miss on a transaction-owned PID.
    pub(crate) fn record_overlay_miss(&self, pid: PageId) {
        self.record(NodeIoKind::OverlayMiss, pid);
    }

    /// Records a read answered from a resident overlay entry.
    pub(crate) fn record_overlay_hit(&self, pid: PageId) {
        self.record(NodeIoKind::OverlayHit, pid);
    }

    pub(crate) fn overlay_hits(&self) -> Vec<PageId> {
        self.class_pids(NodeIoKind::OverlayHit)
    }

    /// Records one commit flush's shape.
    pub(crate) fn record_flush(&self, stats: FlushStats) {
        self.flushes.lock().push(stats);
    }

    pub(crate) fn flushes(&self) -> Vec<FlushStats> {
        self.flushes.lock().clone()
    }

    /// Counts a call to the dense-coverage step, whether or not its gate triggered — asserts
    /// "called once per publication, wrote a zero page only when the gate was true".
    pub(crate) fn record_cover_call(&self) {
        *self.cover_calls.lock() += 1;
    }

    pub(crate) fn cover_calls(&self) -> usize {
        *self.cover_calls.lock()
    }

    pub(crate) fn events(&self) -> Vec<NodeIoEvent> {
        self.events.lock().clone()
    }

    pub(crate) fn writes(&self) -> Vec<PageId> {
        self.class_pids(NodeIoKind::Write)
    }

    pub(crate) fn reads(&self) -> Vec<PageId> {
        self.class_pids(NodeIoKind::Read)
    }

    /// Largest number of writes attributed to one `(PID, incarnation)`: the
    /// incarnation is identified by the sequence slot of the PID's most recent preceding
    /// `alloc` event, so a PID that was released and handed out again starts a fresh count.
    pub(crate) fn max_writes_per_incarnation(&self) -> usize {
        let mut current: std::collections::HashMap<PageId, usize> =
            std::collections::HashMap::new();
        let mut counts: std::collections::HashMap<(PageId, usize), usize> =
            std::collections::HashMap::new();
        let mut max = 0;
        for (slot, event) in self.events().into_iter().enumerate() {
            match event.kind {
                NodeIoKind::Alloc => {
                    current.insert(event.pid, slot);
                }
                NodeIoKind::Write => {
                    if let Some(&incarnation) = current.get(&event.pid) {
                        let count = counts.entry((event.pid, incarnation)).or_default();
                        *count += 1;
                        max = max.max(*count);
                    }
                }
                NodeIoKind::Read
                | NodeIoKind::Cover
                | NodeIoKind::CachePut
                | NodeIoKind::OverlayHit
                | NodeIoKind::OverlayMiss
                | NodeIoKind::Release
                | NodeIoKind::ValueWrite
                | NodeIoKind::IndirectWrite
                | NodeIoKind::AllocatorWrite
                | NodeIoKind::MetaWrite
                | NodeIoKind::ValueRead
                | NodeIoKind::IndirectRead
                | NodeIoKind::MetaRead => {}
            }
        }
        max
    }
}

pub(crate) struct Store {
    file: LiveStore,
    sb: Mutex<MetaNode>,
    shared: SharedMeta,
    /// Reader epoch registry: pins, oldest-active tracking, and the epoch
    /// counter advanced at each generation publication.
    pub(crate) epoch: EpochRegistry,
    reusable: Mutex<ExtentSet>,
    retired: Mutex<ExtentSet>,
    reusable_pages: Mutex<Vec<PageId>>,
    retired_pages: Mutex<Vec<PageId>>,
    file_extended: AtomicBool,
    sync_mode: SyncMode,
    /// Serializes snapshots of this database.
    snapshot_lock: Mutex<()>,
    /// Test-only node-class I/O log.
    #[cfg(test)]
    pub(crate) node_io: NodeIoLog,
}

/// Copy chunk for `Store::take_snapshot`: one buffer per call, reused per chunk.
const SNAPSHOT_COPY_CHUNK: usize = 1 << 20;

/// Value I/O staging cap: one buffer per batch operation, reused across the runs
/// of that operation, holding at most this many physical pages (1 MiB).
///
/// The cap bounds the peak memory of one value operation; the buffer itself is
/// allocated once per operation and sized to the pages that operation actually
/// touches, so a small value never pays for the cap.
pub(crate) const VALUE_STAGING_PAGES: usize = (1 << 20) / PAGE_SIZE;
const _: () = assert!(VALUE_STAGING_PAGES * PAGE_SIZE == 1 << 20);

/// Pages of staging one value operation needs: what it touches, capped by
/// [`VALUE_STAGING_PAGES`] so a large value is still copied in bounded chunks.
///
/// Never zero: `chunks` needs a non-zero chunk size, and an empty page list (a
/// zero-length value) simply never uses the buffer.
fn staging_pages(page_count: usize) -> usize {
    page_count.clamp(1, VALUE_STAGING_PAGES)
}

fn snapshot_io_error(operation: &'static str, path: &Path, source: io::Error) -> OpenError {
    OpenError::Io(OpenIoError {
        operation,
        path: path.to_path_buf(),
        offset: None,
        length: None,
        source,
    })
}

impl Store {
    pub(crate) fn path_is_same_file(&self, path: &Path) -> io::Result<bool> {
        let named = std::fs::File::open(path)?;
        same_file_handles(&self.file.raw.file, &named)
    }

    #[cfg(test)]
    pub(crate) fn test_pread_exact(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.file.raw.file.pread_exact(buf, offset)
    }

    pub(crate) fn open<P: AsRef<Path>>(path: P, options: &OpenOptions) -> OpenResult<Self> {
        let path = path.as_ref();
        let (opening, is_new) = OpeningStore::open(path, options.read_only)?;

        let sb = if is_new {
            if options.read_only {
                // A zero-length file has no meta page to serve, and read-only
                // opens must not turn it into a new database.
                return Err(open_meta_corruption(
                    "NO_VALID_META",
                    None,
                    "read-only open of an empty file",
                ));
            }
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
                Some(buf) => meta_candidate(&buf),
                None => MetaCandidate::Invalid,
            };
            let sb1 = match opening.read_meta_page(PAGE_SIZE as u64)? {
                Some(buf) => meta_candidate(&buf),
                None => MetaCandidate::Invalid,
            };

            match select_meta([sb0, sb1]) {
                MetaSelection::Selected(meta) => meta,
                MetaSelection::None => {
                    return Err(open_meta_corruption(
                        "NO_VALID_META",
                        None,
                        "neither meta page is valid",
                    ));
                }
                MetaSelection::UnsupportedVersion { found } => {
                    return Err(open_meta_corruption_with(
                        "UNSUPPORTED_FORMAT_VERSION",
                        None,
                        "the file's format version is not supported by this build",
                        Some(FORMAT_VERSION.to_string()),
                        Some(found.to_string()),
                    ));
                }
                MetaSelection::MixedVersions { found } => {
                    return Err(open_meta_corruption_with(
                        "MIXED_FORMAT_VERSIONS",
                        None,
                        "meta slots disagree on the format version",
                        Some(FORMAT_VERSION.to_string()),
                        Some(found.to_string()),
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
            shared: SharedMeta::new(MetaSnapshot {
                catalog_root: sb.catalog_root,
                next_page_id: sb.next_page_id,
                reusable_root: sb.reusable_root,
                retired_root: sb.retired_root,
                seq: sb.seq,
            }),
            epoch: EpochRegistry::new(),
            reusable: Mutex::new(ExtentSet::from_extents(reusable)),
            retired: Mutex::new(ExtentSet::from_extents(retired)),
            reusable_pages: Mutex::new(reusable_pages),
            retired_pages: Mutex::new(retired_pages),
            file_extended: AtomicBool::new(false),
            sync_mode: options.sync_mode,
            snapshot_lock: Mutex::new(()),
            #[cfg(test)]
            node_io: NodeIoLog::default(),
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
                #[cfg(test)]
                self.node_io.record(NodeIoKind::MetaRead, current);
                self.file
                    .pread_exact(buf, current as u64 * PAGE_SIZE as u64)
            },
            |site| self.abort_site(site, "extent"),
        )?;
        let (retired, retired_pages) = read_extent_pages(
            retired_root,
            next_page_id,
            |current, buf| {
                #[cfg(test)]
                self.node_io.record(NodeIoKind::MetaRead, current);
                self.file
                    .pread_exact(buf, current as u64 * PAGE_SIZE as u64)
            },
            |site| self.abort_site(site, "extent"),
        )?;
        validate_allocator_sets(
            &reusable,
            &reusable_pages,
            &retired,
            &retired_pages,
            |site| self.abort_site(site, "extent"),
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
        #[cfg(test)]
        self.node_io
            .record_many(NodeIoKind::AllocatorWrite, page_ids);

        let mut extents = extents.into_iter();
        let mut page = [0u8; PAGE_SIZE];
        for (i, &pid) in page_ids.iter().enumerate() {
            let mut header = ExtentHeader {
                checksum: 0,
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
            crate::page::seal_page(&mut page, crate::page::HEADER_CRC_OFFSET, pid);

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
        let mut pages = Vec::new();
        const MAX_ROUND: u32 = 32;
        for _ in 0..MAX_ROUND {
            let needed = Self::extent_pages_needed(reusable.len())
                + Self::extent_pages_needed(retired.len());
            if pages.len() >= needed {
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
        // The last instant before the meta slot becomes durable state, where a
        // publication must already cover the id space it is about to declare.
        #[cfg(test)]
        {
            let hook = BEFORE_META_SLOT_WRITE.lock().clone();
            if let Some(hook) = hook {
                hook(PublicationPoint {
                    next_page_id: sb.next_page_id,
                    file_len: self.live_file_len(),
                    seq: sb.seq,
                    write_offset,
                });
            }
        }
        #[cfg(test)]
        self.node_io.record(
            NodeIoKind::MetaWrite,
            (write_offset / PAGE_SIZE as u64) as PageId,
        );
        self.file.pwrite_all(sb.as_page_slice(), write_offset)?;
        self.sync_publication()?;
        self.file.set_generation(sb.seq);
        self.shared.update(MetaSnapshot {
            catalog_root: sb.catalog_root,
            next_page_id: sb.next_page_id,
            reusable_root: sb.reusable_root,
            retired_root: sb.retired_root,
            seq: sb.seq,
        });
        // Publication ordering: the new generation must be visible to readers
        // before the epoch advances, so a reader acquiring the new epoch value
        // subsequently observes the new shared snapshot.
        self.epoch.advance();
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
            // unpublished allocations quarantined. A normal commit adopts them
            // into the new roots, and both an adopting commit and a deferred
            // one remove those pages from the old allocator extents before the
            // new lists are built.
            for &pid in adopted_alloc {
                journal.remove(ExtentSetKind::Reusable, &mut reusable, pid, 1);
                journal.remove(ExtentSetKind::Retired, &mut retired, pid, 1);
            }
            for &pid in deferred_alloc {
                journal.remove(ExtentSetKind::Reusable, &mut reusable, pid, 1);
                journal.remove(ExtentSetKind::Retired, &mut retired, pid, 1);
            }

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
            // Move the current generation's retired extents to reusable state while
            // before the current epoch can still reference them. Deferred extents
            // stay quarantined and are published with the next generation instead.
            // Promotion is safe only while no in-flight reader can still
            // reference the retired pages. The Relaxed slot scan is ordered by
            // the SharedMeta RwLock + writer-mutex happens-before chain (see
            // EpochRegistry::oldest_active_reader_epoch), so every reader
            // pinned before the current epoch is observed here.
            let promotable = self.epoch.oldest_active_reader_epoch() >= self.epoch.current();
            #[cfg(test)]
            {
                let hook = AFTER_OLDEST_SCAN.lock().clone();
                if let Some(hook) = hook
                    && std::thread::current().id() == hook.thread_id
                {
                    hook.reached
                        .send(())
                        .expect("window test receiver must remain alive");
                    hook.resume
                        .lock()
                        .recv_timeout(Duration::from_secs(10))
                        .expect("window test reader must release the writer within 10 seconds");
                }
            }
            if promotable {
                for extent in retired.iter() {
                    journal.add(
                        ExtentSetKind::Reusable,
                        &mut reusable,
                        extent.page_id,
                        extent.nr_pages,
                    );
                }
            } else {
                for extent in retired.iter() {
                    next_retired.add(extent.page_id, extent.nr_pages);
                }
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
        let snapshot = self.shared.snapshot();
        (snapshot.seq, snapshot.catalog_root)
    }

    /// Writes a snapshot of the current published state to `dst`.
    ///
    /// `dst` is used exactly as given: it is created if missing and truncated if
    /// present, and a failed call can leave a partial file behind. The snapshot
    /// equals the generation named by the returned `seq`: its page bytes are
    /// pinned while they are read, so commits running meanwhile are neither
    /// blocked nor included.
    pub(crate) fn take_snapshot(&self, dst: &Path) -> OpenResult<Snapshot> {
        let _serial = self.snapshot_lock.lock();

        let (snapshot, dst_file) = {
            let _pin = self.epoch.pin();
            let snapshot = self.shared.snapshot();
            let source = &self.file.raw.file;
            let source_path: &Path = self.file.raw.path.as_ref();
            let len = source
                .metadata()
                .map_err(|error| snapshot_io_error("metadata", source_path, error))?
                .len();

            let dst_file = FileOpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(dst)
                .map_err(|error| snapshot_io_error("create", dst, error))?;
            if is_same_file(&dst_file, &self.file.raw.file, dst, source_path)? {
                return Err(snapshot_io_error(
                    "open",
                    dst,
                    io::Error::other("destination is this store's own file"),
                ));
            }
            // Truncate by hand, after the identity check above: opening with
            // `truncate(true)` would destroy the store before we could reject
            // it, and a mistyped backup path must never truncate the live
            // database. Whatever the caller left at the destination therefore
            // starts from empty, so a failed copy leaves a partial file rather
            // than a mix of old and new contents.
            dst_file
                .set_len(0)
                .map_err(|error| snapshot_io_error("set_len", dst, error))?;

            let mut buffer = vec![0u8; SNAPSHOT_COPY_CHUNK];
            let mut offset = 0u64;
            while offset < len {
                let count = (len - offset).min(SNAPSHOT_COPY_CHUNK as u64) as usize;
                let chunk = &mut buffer[..count];
                FileIO::pread_exact(source, chunk, offset)
                    .map_err(|error| snapshot_io_error("pread", source_path, error))?;
                FileIO::pwrite_all(&dst_file, chunk, offset)
                    .map_err(|error| snapshot_io_error("pwrite", dst, error))?;
                offset += count as u64;
            }
            (snapshot, dst_file)
        };

        // slots can name a newer generation whose pages this snapshot does not hold.
        let mut meta = MetaNode {
            magic: MAGIC,
            seq: snapshot.seq,
            format_version: FORMAT_VERSION,
            catalog_root: snapshot.catalog_root,
            next_page_id: snapshot.next_page_id,
            reusable_root: snapshot.reusable_root,
            retired_root: snapshot.retired_root,
            checksum: 0,
        };
        meta.update_checksum();
        for slot in [0u64, PAGE_SIZE as u64] {
            FileIO::pwrite_all(&dst_file, meta.as_page_slice(), slot)
                .map_err(|error| snapshot_io_error("pwrite", dst, error))?;
        }
        // The source only grows, so the copy can carry bytes past the frozen
        // `next_page_id` (pages appended after the freeze). They sit outside the
        // snapshot's page-id space and would be dead weight forever, so the file
        // is cut back to that id space: the snapshot is never grown beyond its
        // frozen `next_page_id`, so nothing past it is carried over.
        let id_space = u64::from(snapshot.next_page_id) * PAGE_SIZE as u64;
        let copied_len = dst_file
            .metadata()
            .map_err(|error| snapshot_io_error("metadata", dst, error))?
            .len();
        if copied_len > id_space {
            dst_file
                .set_len(id_space)
                .map_err(|error| snapshot_io_error("set_len", dst, error))?;
        }
        dst_file
            .sync_all()
            .map_err(|error| snapshot_io_error("sync_all", dst, error))?;
        // Make the destination's directory entry durable too, so an `Ok` snapshot
        // survives a crash of the machine, not only of this process.
        sync_parent_dir(dst).map_err(|error| snapshot_io_error("sync_dir", dst, error))?;

        Ok(Snapshot {
            seq: snapshot.seq,
            path: dst.to_path_buf(),
        })
    }

    /// Pages this generation can hand out again: reusable extents, retired
    /// extents (quarantined, promoted later), and the allocator list pages that
    /// describe them.
    #[cfg(test)]
    pub(crate) fn free_page_count_for_test(&self) -> usize {
        let reusable = self.reusable.lock();
        let retired = self.retired.lock();
        let reusable_pages = self.reusable_pages.lock();
        let retired_pages = self.retired_pages.lock();
        reusable
            .iter()
            .map(|extent| extent.nr_pages as usize)
            .sum::<usize>()
            + retired
                .iter()
                .map(|extent| extent.nr_pages as usize)
                .sum::<usize>()
            + reusable_pages.len()
            + retired_pages.len()
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
        // A page allocated by an outer transaction may have been placed in durable
        // quarantine by an intermediate nested-rollback publication. Releasing that
        // outer transaction removes the reservation from the in-memory projection
        // before returning the page to reusable space; both sets are mutex-guarded
        // and no I/O happens here, so nothing observes an intermediate state.
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
        self.read_node_pages(&[id.get()], page.as_mut_slice())?;
        Ok(self.decode_live_node(id, page))
    }

    pub(crate) fn read_node_without_cache(&self, id: DataPid) -> Arc<Node> {
        let mut page = AlignedPage::new();
        physical_value(
            self.read_node_pages(&[id.get()], page.as_mut_slice()),
            "physical page load",
        );
        self.decode_live_node(id, page)
    }

    /// Loads an *indirect* page (an overflow index page) and verifies its whole
    /// page: it carries no count, so nothing but its own bytes and the expected
    /// page id enter the check.
    pub(crate) fn load_page(&self, id: DataPid) -> Vec<u8> {
        #[cfg(test)]
        self.node_io.record(NodeIoKind::IndirectRead, id.get());
        let mut buf = vec![0u8; PAGE_SIZE];
        if let Err(e) = self.read_page_runs([id.get()], &mut buf) {
            abort_store_fault(e, "physical page load");
        }
        if let Err(mismatch) =
            crate::page::verify_page(&buf, crate::page::TRAILER_CRC_OFFSET, id.get())
        {
            self.abort_crc_mismatch(mismatch, id.get(), "indirect");
        }
        buf
    }

    /// Physical node-page-array read: `buf` must hold exactly one `PAGE_SIZE` per
    /// PID, every node page must verify its header checksum against its own PID,
    /// and nothing may be silently dropped. A mismatch records the diagnostic here
    /// (expected and actual CRC, PID) and terminates: the live boundary.
    ///
    /// Node pages only: the header checksum rule ([`crate::page::HEADER_CRC_OFFSET`])
    /// is fixed here, so another page class has to go through its own loader
    /// ([`Self::load_page`], [`Self::load_data_pids`]).
    pub(crate) fn read_node_pages(&self, pages: &[PageId], buf: &mut [u8]) -> Result<()> {
        if buf.len() != pages.len() * PAGE_SIZE {
            self.corrupt_page(
                "PAGE_ARRAY_LENGTH",
                None,
                "physical read length must be an exact page multiple",
            );
        }
        self.read_page_runs(pages.iter().copied(), buf)?;
        for (index, &pid) in pages.iter().enumerate() {
            let page = &buf[index * PAGE_SIZE..(index + 1) * PAGE_SIZE];
            if let Err(mismatch) =
                crate::page::verify_page(page, crate::page::HEADER_CRC_OFFSET, pid)
            {
                self.abort_crc_mismatch(mismatch, pid, "node");
            }
        }
        #[cfg(test)]
        {
            for &pid in pages {
                self.node_io.record_read(pid);
            }
        }
        Ok(())
    }

    /// Logical value read.
    ///
    /// `pages` must be exactly `ceil(len / TRAILER_CONTENT_SIZE)` physical pages
    /// (zero pages for a zero-length value). Every page is read in full and
    /// verified before its valid prefix is copied out, so media damage inside a
    /// page's content is caught even when the caller only wants the last bytes.
    /// The unused tail of the final page is covered by the checksum, so it must be
    /// the writer's zeroed, deterministic tail: a writer that sealed a page it never
    /// fully initialized would make every read of it a checksum failure.
    pub(crate) fn load_data_pids(&self, pages: &[DataPid], len: usize) -> Result<Vec<u8>> {
        let expected_pages = len.div_ceil(crate::page::TRAILER_CONTENT_SIZE);
        if pages.len() != expected_pages {
            self.corrupt_page(
                "VALUE_PAGE_COUNT",
                pages.first().map(|page| page.get()),
                "value page count must equal ceil(len / TRAILER_CONTENT_SIZE)",
            );
        }

        let mut buf = vec![0u8; len];
        let staging_pages = staging_pages(pages.len());
        let mut staging = vec![0u8; staging_pages * PAGE_SIZE];
        let mut copied = 0usize;

        for chunk in pages.chunks(staging_pages) {
            let span = chunk.len() * PAGE_SIZE;
            let staging = &mut staging[..span];
            #[cfg(test)]
            self.node_io.record_many(
                NodeIoKind::ValueRead,
                &chunk.iter().map(|page| page.get()).collect::<Vec<_>>(),
            );
            self.read_page_runs(chunk.iter().map(|page| page.get()), staging)?;

            for (index, page_id) in chunk.iter().enumerate() {
                let page = &staging[index * PAGE_SIZE..(index + 1) * PAGE_SIZE];
                let take = (len - copied).min(crate::page::TRAILER_CONTENT_SIZE);
                if let Err(mismatch) =
                    crate::page::verify_page(page, crate::page::TRAILER_CRC_OFFSET, page_id.get())
                {
                    self.abort_crc_mismatch(mismatch, page_id.get(), "value");
                }
                buf[copied..copied + take].copy_from_slice(&page[..take]);
                copied += take;
            }
        }
        Ok(buf)
    }

    fn read_page_runs<I>(&self, pages: I, buf: &mut [u8]) -> Result<()>
    where
        I: IntoIterator<Item = PageId>,
    {
        let mut run_start = None;
        let mut run_buf_start = 0usize;
        let mut run_len = 0usize;
        let mut count = 0usize;

        for (page_index, page_id) in pages.into_iter().enumerate() {
            if (page_index + 1) * PAGE_SIZE > buf.len() {
                self.corrupt_page(
                    "PAGE_ARRAY_COUNT",
                    Some(page_id),
                    "physical read has more pages than the buffer holds",
                );
            }
            count += 1;

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
        if count * PAGE_SIZE != buf.len() {
            self.corrupt_page(
                "PAGE_ARRAY_COUNT",
                None,
                "physical read has fewer pages than the buffer expects",
            );
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
        let end = start + nr_pages * PAGE_SIZE;
        self.file
            .pread_exact(&mut buf[start..end], first_page as u64 * PAGE_SIZE as u64)
    }

    /// Physical page-array write. Every page must already carry its CRC trailer
    /// for the PID it is written as; the caller seals it (see
    /// [`Self::write_physical_page`] and [`Self::write_physical_pages`]).
    fn write_page_runs(&self, pages: &[PageId], buf: &[u8]) -> Result<()> {
        if buf.len() != pages.len() * PAGE_SIZE {
            self.corrupt_page(
                "PAGE_ARRAY_LENGTH",
                None,
                "physical write length must be an exact page multiple",
            );
        }

        let mut run_start = None;
        let mut run_data_page = 0usize;
        let mut run_len = 0usize;

        for (page_index, &page_id) in pages.iter().enumerate() {
            let contiguous = run_start
                .is_some_and(|first| u64::from(page_id) == u64::from(first) + run_len as u64);
            if !contiguous {
                if let Some(first) = run_start {
                    self.write_page_run(first, run_data_page, run_len, buf)?;
                }
                run_start = Some(page_id);
                run_data_page = page_index;
                run_len = 1;
            } else {
                run_len += 1;
            }
        }

        if let Some(first) = run_start {
            self.write_page_run(first, run_data_page, run_len, buf)?;
        }
        Ok(())
    }

    /// Writes one sealed physical page (node write path). The page buffer is
    /// sealed in place with `id`, so no copy of the 4096 bytes is made.
    pub(crate) fn write_physical_page(&self, id: DataPid, page: &mut [u8]) {
        crate::page::seal_page(page, crate::node::PLAIN_CHECKSUM_OFFSET, id.get());
        physical_value(
            self.write_page_runs(&[id.get()], page),
            "physical page write",
        );
    }

    /// Commit flush (step 1): `pages` must already be sorted by PID. Each contiguous
    /// run is copied into a staging buffer, sealed there with its final PID, and written in one
    /// `pwrite_all`; the resident entries are **not** mutated, so a Clean entry's in-memory bytes
    /// differ from disk only in the checksum field, which no read path checks
    /// implementation note.
    ///
    /// This is the flush-path node-write accounting point: value/indirect pages
    /// never come through here, so `write_page_runs` stays free of node-only counters.
    pub(crate) fn write_node_runs(&self, pages: &[(PageId, Arc<crate::node::Node>)]) -> Result<()> {
        let mut staging: Vec<u8> = Vec::new();
        let mut index = 0usize;
        #[cfg(test)]
        let mut runs = 0usize;
        while index < pages.len() {
            let mut end = index + 1;
            while end < pages.len() && pages[end].0 == pages[end - 1].0 + 1 {
                end += 1;
            }
            let run_len = end - index;
            #[cfg(test)]
            {
                runs += 1;
            }
            staging.resize(run_len * PAGE_SIZE, 0);
            for (slot, (pid, node)) in pages[index..end].iter().enumerate() {
                let start = slot * PAGE_SIZE;
                let target = &mut staging[start..start + PAGE_SIZE];
                target.copy_from_slice(node.finalize());
                crate::page::seal_page(target, crate::node::PLAIN_CHECKSUM_OFFSET, *pid);
            }
            let run: Vec<PageId> = pages[index..end].iter().map(|(pid, _)| *pid).collect();
            // Exactly this run's bytes: the buffer is reused across runs and never shrinks, so
            self.write_page_runs(&run, &staging[..run_len * PAGE_SIZE])?;
            #[cfg(test)]
            for pid in &run {
                self.node_io.record_write(*pid);
            }
            index = end;
        }
        #[cfg(test)]
        self.node_io.record_flush(FlushStats {
            dirty_pages: pages.len(),
            runs,
            pids: pages.iter().map(|(pid, _)| *pid).collect(),
        });
        Ok(())
    }

    /// Live file length in bytes. The dense-coverage gate must read the **live** file — never a
    /// snapshot target, never a cached or derived value — and `FileIO` has no
    /// length accessor, so this is the thin method the cover step needs. A failing `metadata`
    /// call is an I/O fault like any other on the live path.
    pub(crate) fn live_file_len(&self) -> u64 {
        match self.file.raw.file.metadata() {
            Ok(metadata) => metadata.len(),
            Err(source) => self.file.io_fault("metadata", None, None, source),
        }
    }

    /// Dense coverage: when the live file is shorter than the id space,
    /// write one 4 KiB all-zero page at the highest allocated slot so the length covers
    /// `next_page_id * PAGE_SIZE` again. It is a real data write, so the growth is durable through
    /// the ordinary dependency sync; `file_extended` is set exactly as the allocator's monotonic
    /// branch does, which is what upgrades that sync under `SyncMode::Adaptive`.
    pub(crate) fn cover_id_space(&self, next_page_id: PageId) {
        #[cfg(test)]
        self.node_io.record_cover_call();
        if next_page_id == 0 {
            return;
        }
        let high_water = u64::from(next_page_id) * PAGE_SIZE as u64;
        if self.live_file_len() >= high_water {
            return;
        }
        let last = next_page_id - 1;
        physical_value(
            self.file
                .pwrite_all(&[0u8; PAGE_SIZE], u64::from(last) * PAGE_SIZE as u64),
            "dense coverage write",
        );
        self.file_extended.store(true, Ordering::Relaxed);
        #[cfg(test)]
        self.node_io.record_cover(last);
    }

    /// Writes several physical pages. This only writes: every caller seals each
    /// page over its whole 4096 bytes first, the indirect chain builder doing it
    /// with the page's own PID and its never-filled id slots left zero. A page
    /// handed over unsealed would not verify on read, so there is no such caller.
    pub(crate) fn write_physical_pages(&self, pages: &[PageId], buf: &mut [u8]) -> Result<()> {
        if buf.len() != pages.len() * PAGE_SIZE {
            self.corrupt_page(
                "PAGE_ARRAY_LENGTH",
                None,
                "physical write length must be an exact page multiple",
            );
        }
        self.write_page_runs(pages, buf)
    }

    /// Logical value write.
    ///
    /// Packs `value` into exactly `ceil(len / TRAILER_CONTENT_SIZE)` pages, seals
    /// each over its whole page and writes the run. The bytes a page does not use are
    /// zeroed, so the covered page is deterministic instead of carrying a reused
    /// staging buffer's leftovers. The staging buffer is bounded (see
    /// [`VALUE_STAGING_PAGES`]).
    pub(crate) fn write_value_pages(&self, pages: &[PageId], value: &[u8]) -> Result<()> {
        let expected_pages = value.len().div_ceil(crate::page::TRAILER_CONTENT_SIZE);
        if pages.len() != expected_pages {
            self.corrupt_page(
                "VALUE_PAGE_COUNT",
                pages.first().copied(),
                "value page count must equal ceil(len / TRAILER_CONTENT_SIZE)",
            );
        }

        let staging_pages = staging_pages(pages.len());
        let mut staging = vec![0u8; staging_pages * PAGE_SIZE];
        let mut written = 0usize;
        for chunk in pages.chunks(staging_pages) {
            let span = chunk.len() * PAGE_SIZE;
            let staging = &mut staging[..span];
            for (index, pid) in chunk.iter().enumerate() {
                let page = &mut staging[index * PAGE_SIZE..(index + 1) * PAGE_SIZE];
                let take = (value.len() - written).min(crate::page::TRAILER_CONTENT_SIZE);
                page[..take].copy_from_slice(&value[written..written + take]);
                written += take;
                // Zero what this page does not use: the checksum covers the whole
                // page and the staging buffer is reused across chunks, so without
                // it the previous chunk's leftover bytes would reach both the
                // file and the checksum.
                page[take..crate::page::TRAILER_CONTENT_SIZE].fill(0);
                crate::page::seal_page(page, crate::page::TRAILER_CRC_OFFSET, *pid);
            }
            self.write_page_runs(chunk, staging)?;
        }
        Ok(())
    }

    /// Records a page-level failure at its detection site and terminates: the
    /// live boundary for every page load.
    fn abort_site(&self, site: CorruptionSite, page_kind: &'static str) -> ! {
        fatal(FatalReason::Corruption(
            site.report(Some(self.get_seq()), page_kind),
        ))
    }

    fn abort_crc_mismatch(
        &self,
        mismatch: crate::page::CrcMismatch,
        pid: PageId,
        page_kind: &'static str,
    ) -> ! {
        self.abort_site(
            CorruptionSite::crc(
                pid,
                "PAGE_CRC_MISMATCH",
                "crc32c over the whole page",
                mismatch.expected,
                mismatch.actual,
            ),
            page_kind,
        )
    }

    fn corrupt_page(&self, code: &'static str, pid: Option<PageId>, check: &'static str) -> ! {
        self.abort_site(CorruptionSite::buffer_shape(code, pid, check), "page")
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

    #[cfg(test)]
    pub(crate) fn forge_cached_seq_for_test(&self, seq: u64) {
        self.sb.lock().seq = seq;
    }

    /// Refresh the in-memory superblock and allocator state from disk.
    ///
    /// `allow_install` is true only for writer paths (which hold the writer
    /// mutex): they may adopt a disk-newer generation left by a failed
    /// publication. Readers pass false: a reader must never install a
    /// disk-newer generation into the shared snapshot, because that would
    /// advance the shared state mid-exec and trip the writer's
    /// `COMMIT_SEQUENCE_CONFLICT` check (and the failed generation's allocator
    /// state is inconsistent with a concurrent writer's transaction). A reader
    /// refresh always returns the current (shared) generation.
    pub(crate) fn refresh_sb(&self, allow_install: bool) -> Result<MetaSnapshot> {
        let sb0 = self.read_current_meta_candidate(0)?;
        let sb1 = self.read_current_meta_candidate(PAGE_SIZE as u64)?;

        // actual version) and terminates, exactly like a page-level failure; it
        // never escapes as a user-level error.
        let sb = match select_meta([sb0, sb1]) {
            MetaSelection::Selected(meta) => meta,
            MetaSelection::None => self.abort_meta(
                "NO_VALID_META",
                None,
                "neither meta page is valid",
                None,
                None,
            ),
            MetaSelection::UnsupportedVersion { found } => self.abort_meta(
                "UNSUPPORTED_FORMAT_VERSION",
                None,
                "the file's format version is not supported by this build",
                Some(FORMAT_VERSION.to_string()),
                Some(found.to_string()),
            ),
            MetaSelection::MixedVersions { found } => self.abort_meta(
                "MIXED_FORMAT_VERSIONS",
                None,
                "meta slots disagree on the format version",
                Some(FORMAT_VERSION.to_string()),
                Some(found.to_string()),
            ),
        };

        let current_seq = self.sb.lock().seq;
        if allow_install && sb.seq > current_seq {
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
                self.shared.update(MetaSnapshot {
                    catalog_root: current_sb.catalog_root,
                    next_page_id: current_sb.next_page_id,
                    reusable_root: current_sb.reusable_root,
                    retired_root: current_sb.retired_root,
                    seq: current_sb.seq,
                });
                self.epoch.advance();
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

        if !allow_install {
            return Ok(self.shared.snapshot());
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
        let end = start + nr_pages * PAGE_SIZE;
        self.file
            .pwrite_all(&data[start..end], first_page as u64 * PAGE_SIZE as u64)
    }

    fn read_current_meta_candidate(&self, offset: u64) -> Result<MetaCandidate> {
        #[cfg(test)]
        self.node_io
            .record(NodeIoKind::MetaRead, (offset / PAGE_SIZE as u64) as PageId);
        let mut buf = [0u8; PAGE_SIZE];
        self.file.pread_exact(&mut buf, offset)?;
        Ok(meta_candidate(&buf))
    }

    fn abort_meta(
        &self,
        code: &'static str,
        generation: Option<u64>,
        check: &'static str,
        expected: Option<String>,
        actual: Option<String>,
    ) -> ! {
        fatal(FatalReason::Corruption(CorruptionReport {
            code,
            generation,
            page_kind: "meta",
            pid: None,
            check,
            expected: expected.map(Into::into),
            actual: actual.map(Into::into),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::child_test_command;

    const LIVE_FAULT_CHILD_ENV: &str = "BTREE_STORE_LIVE_FAULT_CHILD";
    const GENERATION_FAULT_CHILD_PATH: &str = "BTREE_STORE_GENERATION_FAULT_CHILD_PATH";
    const LIVE_FAULT_CHILD_DIR: &str = "BTREE_STORE_LIVE_FAULT_CHILD_DIR";

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

    /// Test-only encoder. Mirrors the production writer: content laid out,
    /// padding left zero, CRC trailer sealed for `pid`.
    fn encode_extent_page(pid: PageId, next: PageId, extents: &[Extent]) -> [u8; PAGE_SIZE] {
        assert!(extents.len() <= EXTENT_PER_PAGE);
        let mut page = [0u8; PAGE_SIZE];
        let header = ExtentHeader {
            checksum: 0,
            next,
            count: extents.len() as u32,
        };
        page[..EXTENT_HEADER_SIZE].copy_from_slice(header.as_slice());
        let mut offset = EXTENT_HEADER_SIZE;
        for extent in extents {
            page[offset..offset + EXTENT_SIZE].copy_from_slice(extent.as_slice());
            offset += EXTENT_SIZE;
        }
        crate::page::seal_page(&mut page, crate::page::HEADER_CRC_OFFSET, pid);
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

    /// Capacity boundary of the extent page: 510 entries plus the 12-byte header
    /// fit; the checksum field covers the rest of the page.
    #[test]
    fn extent_page_capacity_is_510_entries() {
        assert_eq!(EXTENT_PER_PAGE, 510);
        assert_eq!(EXTENT_ENTRIES_END, PAGE_SIZE);
        const { assert!(EXTENT_HEADER_SIZE + EXTENT_PER_PAGE * EXTENT_SIZE <= EXTENT_ENTRIES_END) };
        assert_eq!(Store::extent_pages_needed(0), 0);
        assert_eq!(Store::extent_pages_needed(1), 1);
        assert_eq!(Store::extent_pages_needed(510), 1);
        assert_eq!(Store::extent_pages_needed(511), 2);
        assert_eq!(Store::extent_pages_needed(1020), 2);
        assert_eq!(Store::extent_pages_needed(1021), 3);
    }

    #[test]
    fn read_extent_pages_merges_adjacent_extents_while_streaming() {
        let page = encode_extent_page(
            2,
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
            |site| {
                (
                    site.code,
                    site.pid.expect("page-level failure must name its page"),
                    site.check,
                )
            },
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
            2,
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
            |site| {
                (
                    site.code,
                    site.pid.expect("page-level failure must name its page"),
                    site.check,
                )
            },
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
            2,
            5,
            &[Extent {
                page_id: 4,
                nr_pages: 2,
            }],
        );
        let next_page = encode_extent_page(5, 0, &[]);

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
            |site| {
                (
                    site.code,
                    site.pid.expect("page-level failure must name its page"),
                    site.check,
                )
            },
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

        let (raw, is_new) = RawFile::open(&path, false).unwrap();
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
    fn deferred_promotion_holds_pages_until_reader_quiesces() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("deferred-promotion.db");
        let options = crate::OpenOptions::default();

        let store = reopen_test_store(&path, &options);
        // A reader pinned before any publication holds epoch 0. The epoch+1
        // slot encoding must keep it visible (regression for the encoding gap:
        // storing the raw epoch 0 would look idle and let the writer promote).
        let guard = store.epoch.pin();
        assert_eq!(store.epoch.current(), 0);
        assert_eq!(store.epoch.oldest_active_reader_epoch(), 0);
        assert_eq!(store.epoch.active_reader_count(), 1);

        let retired_pids = store.alloc_pages(3).unwrap();
        assert_eq!(retired_pids, vec![2, 3, 4]);
        store
            .commit_roots_with_pending_alloc(
                0,
                &[(retired_pids[0], retired_pids.len() as u32)],
                &HashSet::new(),
            )
            .unwrap();
        // first publication advanced the epoch 0 -> 1 while the reader is pinned at 0
        assert!(store.epoch.current() >= 1);
        assert!(
            retired_pids
                .iter()
                .all(|pid| extent_contains(&store.retired.lock(), *pid))
        );

        // A second publication must NOT promote while the epoch-0 reader is
        // still active: the retired pages stay quarantined.
        store
            .commit_roots_with_pending_alloc(0, &[], &HashSet::new())
            .unwrap();
        assert!(
            retired_pids
                .iter()
                .all(|pid| extent_contains(&store.retired.lock(), *pid))
        );
        assert!(
            retired_pids
                .iter()
                .all(|pid| !extent_contains(&store.reusable.lock(), *pid))
        );
        assert_empty_store_allocator_complete(&store);

        // pages become reusable again.
        drop(guard);
        store
            .commit_roots_with_pending_alloc(0, &[], &HashSet::new())
            .unwrap();
        assert_empty_store_allocator_complete(&store);
        let reused = store.alloc_pages(1).unwrap();
        assert!(retired_pids.contains(&reused[0]));
    }

    /// A reader refresh must never install a disk-newer generation into the
    /// shared snapshot: doing so would advance the shared state mid-exec and
    /// trip the writer's COMMIT_SEQUENCE_CONFLICT abort. A writer refresh, by
    /// contrast, adopts the failed generation (pre-existing behavior). The
    /// forged slot is written through the store's own file handle so the test
    /// works on Windows, where the engine's file lock blocks external writers.
    #[test]
    fn reader_refresh_never_installs_disk_newer_generation() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("reader-refresh-unit.db");
        let store = reopen_test_store(&path, &crate::OpenOptions::default());
        store
            .commit_roots_with_pending_alloc(0, &[], &HashSet::new())
            .unwrap();
        let current_seq = store.sb.lock().seq;

        let other_offset = if current_seq.is_multiple_of(2) {
            0
        } else {
            PAGE_SIZE as u64
        };
        let mut forged = *store.sb.lock();
        forged.seq += 1;
        forged.update_checksum();
        store
            .file
            .pwrite_all(forged.as_page_slice(), other_offset)
            .unwrap();

        // reader refresh (allow_install=false) must NOT install
        let snapshot = store.refresh_sb(false).unwrap();
        assert_eq!(snapshot.seq, current_seq);
        assert_eq!(
            store.sb.lock().seq,
            current_seq,
            "a reader refresh must not install a disk-newer (failed) generation"
        );

        let snapshot = store.refresh_sb(true).unwrap();
        assert_eq!(snapshot.seq, current_seq + 1);
        assert_eq!(store.sb.lock().seq, current_seq + 1);

        store
            .commit_roots_with_pending_alloc(0, &[], &HashSet::new())
            .unwrap();
        assert_eq!(store.sb.lock().seq, current_seq + 2);
    }

    #[test]
    fn window_reader_between_oldest_scan_and_publish_sees_consistent_snapshot() {
        use std::sync::{Barrier, mpsc};
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("window-test.db");
        let tree = reopen_test_tree(&path);
        tree.new_bucket("window", false).unwrap();
        tree.exec("window", |txn| {
            for i in 0..64u32 {
                txn.put(format!("k{i:03}").as_bytes(), b"v0").unwrap();
            }
            Ok(())
        })
        .unwrap();

        // arm the hook for the writer thread only: the writer parks after the
        // oldest scan and before the promotion loop, so the reader lands
        struct WindowHookGuard;
        impl Drop for WindowHookGuard {
            fn drop(&mut self) {
                *AFTER_OLDEST_SCAN.lock() = None;
            }
        }
        let (reached_tx, reached_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let (id_tx, id_rx) = mpsc::channel();
        let start = Arc::new(Barrier::new(2));

        let writer_tree = tree.clone();
        let start_w = start.clone();
        let writer = std::thread::spawn(move || {
            id_tx.send(std::thread::current().id()).unwrap();
            start_w.wait();
            writer_tree
                .exec("window", |txn| {
                    for i in 0..64u32 {
                        txn.put(format!("k{i:03}").as_bytes(), b"v2").unwrap();
                    }
                    Ok(())
                })
                .unwrap();
        });

        let writer_id = id_rx.recv().unwrap();
        *AFTER_OLDEST_SCAN.lock() = Some(Arc::new(AfterOldestScanHook {
            thread_id: writer_id,
            reached: reached_tx,
            resume: Mutex::new(resume_rx),
        }));
        let _hook_guard = WindowHookGuard;
        start.wait();
        reached_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("writer must reach the epoch-scan hook within 10 seconds");

        let reader_tree = tree.clone();
        let reader = std::thread::spawn(move || {
            // if the reader panics before releasing the writer, release it on
            // unwind so the writer thread cannot strand at the hook
            struct ReleaseOnDrop(Option<mpsc::Sender<()>>);
            impl ReleaseOnDrop {
                fn release(&mut self) {
                    if let Some(tx) = self.0.take() {
                        let _ = tx.send(());
                    }
                }
            }
            impl Drop for ReleaseOnDrop {
                fn drop(&mut self) {
                    self.release();
                }
            }
            let mut release = ReleaseOnDrop(Some(resume_tx));
            reader_tree
                .view("window", |txn| {
                    // the writer is parked before publication: the view must
                    // observe the pre-publication snapshot
                    let first = txn.get(b"k000").unwrap();
                    assert_eq!(first, b"v0");
                    // release the writer: it promotes and publishes while this
                    // reader keeps traversing its fixed snapshot
                    release.release();
                    for i in 0..64u32 {
                        let v = txn.get(format!("k{i:03}").as_bytes()).unwrap();
                        assert_eq!(v, b"v0", "reader must keep its fixed snapshot");
                    }
                    Ok::<_, crate::Error>(())
                })
                .unwrap();
        });

        reader.join().unwrap();
        writer.join().unwrap();
        // the writer's commit is visible to a later view
        tree.view("window", |txn| {
            assert_eq!(txn.get(b"k000").unwrap(), b"v2");
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn writer_adoption_advances_epoch_with_shared_snapshot() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = reopen_test_store(&dir.path().join("adopt-epoch.db"), &OpenOptions::default());
        let current = *store.sb.lock();
        let counter_before = store.epoch.current();

        // Model a complete meta-slot write whose publication failed before
        // the in-memory shared snapshot was updated.
        let mut disk_newer = current;
        disk_newer.seq += 1;
        disk_newer.update_checksum();
        let write_offset = if disk_newer.seq.is_multiple_of(2) {
            PAGE_SIZE as u64
        } else {
            0
        };
        store
            .file
            .pwrite_all(disk_newer.as_page_slice(), write_offset)
            .unwrap();

        let adopted = store.refresh_sb(true).unwrap();
        assert_eq!(adopted.seq, disk_newer.seq);
        assert_eq!(store.shared.snapshot().seq, disk_newer.seq);
        assert_eq!(store.epoch.current(), counter_before + 1);
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

    fn raw_file_with_fault_at(dir: &Path, operation: &'static str, raw_os_error: i32) -> RawFile {
        let path = dir.join("fault.db");
        let file = FileOpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .unwrap();
        RawFile {
            file,
            path: Arc::new(path),
            fault: Some(TestFault {
                operation,
                raw_os_error,
                remaining: Arc::new(std::sync::atomic::AtomicUsize::new(1)),
            }),
        }
    }

    fn raw_file_with_fault(
        operation: &'static str,
        raw_os_error: i32,
    ) -> (tempfile::TempDir, RawFile) {
        let dir = tempfile::TempDir::new().unwrap();
        let raw = raw_file_with_fault_at(dir.path(), operation, raw_os_error);
        (dir, raw)
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
        // The parent owns the directory: this process is about to abort, and a
        // temporary directory of its own would never be removed.
        let Ok(dir) = std::env::var(LIVE_FAULT_CHILD_DIR) else {
            return;
        };
        let raw = raw_file_with_fault_at(Path::new(&dir), operation, raw_os_error);
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
            // One directory per child, dropped with the iteration, so an aborted
            // child leaves nothing for the parent to collect.
            let dir = tempfile::TempDir::new().unwrap();
            let output = child_test_command(&std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "store::tests::live_fault_child",
                    "--ignored",
                    "--nocapture",
                ])
                .env(LIVE_FAULT_CHILD_ENV, operation)
                .env(LIVE_FAULT_CHILD_DIR, dir.path())
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
