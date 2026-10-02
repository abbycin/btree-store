use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    fmt,
    hash::Hasher,
    io::{self, Write},
    num::NonZeroU32,
    path::{Path, PathBuf},
    sync::{
        Arc, OnceLock, Weak,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

use parking_lot::{Mutex, RwLock};

use crate::epoch::EpochGuard;

#[cfg(not(target_endian = "little"))]
compile_error!("btree-store requires a little-endian target");

pub(crate) mod cache;
pub(crate) mod check;
pub(crate) mod epoch;
pub(crate) mod instance_local;
pub(crate) mod node;
pub(crate) mod page;
pub(crate) mod store;

#[cfg(test)]
#[path = "../tests/common/mod.rs"]
mod test_support;

pub use check::{
    BucketStats, CheckDiagnostic, CheckError, CheckOptions, CheckReport, CheckReportWithSpace,
    CheckResult, CheckSpaceStats, CheckStatus, DiagnosticSeverity, MetaSlot, check_path,
    check_path_with_options, check_path_with_space,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// A mutating call was made on a read-only handle.
    ReadOnly,
    KeyNotFound,
    BucketNotFound,
    BucketExists,
    InvalidKey(KeyError),
    InvalidBucket(BucketError),
    ValueTooLarge {
        len: usize,
        max: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyError {
    Empty,
    TooLarge { len: usize, max: usize },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BucketError {
    Empty,
    TooLarge { len: usize, max: usize },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OptionsError {
    LiveInstanceOptionsMismatch,
}

#[derive(Debug)]
pub struct OpenIoError {
    pub operation: &'static str,
    pub path: PathBuf,
    pub offset: Option<u64>,
    pub length: Option<u64>,
    source: io::Error,
}

impl OpenIoError {
    pub fn source_error(&self) -> &io::Error {
        &self.source
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorruptionReport {
    pub code: &'static str,
    pub generation: Option<u64>,
    pub page_kind: &'static str,
    pub pid: Option<PageId>,
    pub check: &'static str,
    pub expected: Option<Box<str>>,
    pub actual: Option<Box<str>>,
}

/// A page-level validation failure, recorded where it is detected.
///
/// The detection site owns the diagnostic: it knows the expected physical page
/// id and, for a checksum failure, both CRC values. Boundaries only decide how
/// the process exits (abort on a live path, `OpenError` when opening), so the
/// report never degrades into empty `pid`/`expected`/`actual` fields.
#[derive(Clone, Copy, Debug)]
pub(crate) struct CorruptionSite {
    pub(crate) code: &'static str,
    /// The page the failure is about, when there is one. Buffer-shape checks
    /// (a length or count that does not match the call) name no single page;
    /// reporting an invented id there would point operators at a page that was
    /// never involved.
    pub(crate) pid: Option<PageId>,
    pub(crate) check: &'static str,
    /// `(expected, actual)` CRC32C when the failure is a checksum mismatch.
    pub(crate) crc: Option<(u32, u32)>,
}

impl CorruptionSite {
    pub(crate) fn crc(
        pid: PageId,
        code: &'static str,
        check: &'static str,
        expected: u32,
        actual: u32,
    ) -> Self {
        Self {
            code,
            pid: Some(pid),
            check,
            crc: Some((expected, actual)),
        }
    }

    pub(crate) fn structure(pid: PageId, code: &'static str, check: &'static str) -> Self {
        Self {
            code,
            pid: Some(pid),
            check,
            crc: None,
        }
    }

    /// A failure about the shape of a page buffer rather than one page's
    /// content; `pid` is only set when the check happens to know the page.
    pub(crate) fn buffer_shape(
        code: &'static str,
        pid: Option<PageId>,
        check: &'static str,
    ) -> Self {
        Self {
            code,
            pid,
            check,
            crc: None,
        }
    }

    pub(crate) fn report(
        self,
        generation: Option<u64>,
        page_kind: &'static str,
    ) -> CorruptionReport {
        CorruptionReport {
            code: self.code,
            generation,
            page_kind,
            pid: self.pid,
            check: self.check,
            expected: self.crc.map(|(expected, _)| expected.to_string().into()),
            actual: self.crc.map(|(_, actual)| actual.to_string().into()),
        }
    }
}

#[derive(Debug)]
pub enum OpenError {
    Io(OpenIoError),
    Corruption(CorruptionReport),
    InvalidOptions(OptionsError),
    DatabaseBusy {
        path: PathBuf,
    },
    /// A mutating call was made on a read-only handle.
    ReadOnly,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for Error {}

impl fmt::Display for OpenIoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} failed for {}", self.operation, self.path.display())?;
        if let Some(offset) = self.offset {
            write!(f, " at offset {offset}")?;
        }
        if let Some(length) = self.length {
            write!(f, " for {length} bytes")?;
        }
        write!(f, ": {}", self.source)
    }
}

impl std::error::Error for OpenIoError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

impl fmt::Display for OpenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "{err}"),
            Self::Corruption(report) => {
                write!(f, "database corruption [{}]: {}", report.code, report.check)
            }
            Self::InvalidOptions(err) => write!(f, "invalid open options: {err:?}"),
            Self::DatabaseBusy { path } => {
                write!(
                    f,
                    "database is already open by another process: {}",
                    path.display()
                )
            }
            Self::ReadOnly => write!(f, "the database was opened read-only"),
        }
    }
}

impl std::error::Error for OpenError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            _ => None,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
pub type OpenResult<T> = std::result::Result<T, OpenError>;

/// A standalone copy of the store's persistent state as of one published generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    /// Generation the snapshot equals (same scale as `MetaNode::seq`). It is the
    /// generation frozen when the call started, which is not necessarily the
    /// newest one when the call returns.
    pub seq: u64,
    /// The destination the snapshot was written to, exactly as it was given.
    pub path: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StoreFault {
    Corruption,
}

pub(crate) type StoreResult<T> = std::result::Result<T, StoreFault>;

#[derive(Debug)]
pub(crate) struct IoFault {
    pub(crate) operation: &'static str,
    pub(crate) path: PathBuf,
    pub(crate) generation: u64,
    pub(crate) offset: Option<u64>,
    pub(crate) length: Option<u64>,
    pub(crate) source: io::Error,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum IdSpace {
    Physical,
}

#[derive(Debug)]
pub(crate) enum InvariantReport {
    Message { code: &'static str, detail: String },
}

#[derive(Debug)]
pub(crate) enum FatalReason {
    Io(IoFault),
    Corruption(CorruptionReport),
    AddressSpaceExhausted {
        space: IdSpace,
        next: u64,
        requested: u64,
    },
    InvariantViolation(InvariantReport),
}

#[cold]
#[inline(never)]
pub(crate) fn fatal(reason: FatalReason) -> ! {
    let mut stderr = io::stderr().lock();
    match &reason {
        FatalReason::Io(fault) => {
            let _ = writeln!(
                stderr,
                "btree-store fatal code=BTREE_FATAL_IO operation={} path={} generation={} offset={} length={} source_kind={:?} os_error={} source={}",
                fault.operation,
                fault.path.display(),
                fault.generation,
                fault
                    .offset
                    .map_or_else(|| "none".to_string(), |v| v.to_string()),
                fault
                    .length
                    .map_or_else(|| "none".to_string(), |v| v.to_string()),
                fault.source.kind(),
                fault
                    .source
                    .raw_os_error()
                    .map_or_else(|| "none".to_string(), |v| v.to_string()),
                fault.source
            );
        }
        FatalReason::Corruption(report) => {
            let _ = writeln!(
                stderr,
                "btree-store fatal code=BTREE_FATAL_CORRUPTION fault={} generation={} page_kind={} pid={} check={} expected={} actual={}",
                report.code,
                report
                    .generation
                    .map_or_else(|| "none".to_string(), |v| v.to_string()),
                report.page_kind,
                report
                    .pid
                    .map_or_else(|| "none".to_string(), |v| v.to_string()),
                report.check,
                report.expected.as_deref().unwrap_or("none"),
                report.actual.as_deref().unwrap_or("none")
            );
        }
        FatalReason::AddressSpaceExhausted {
            space,
            next,
            requested,
        } => {
            let _ = writeln!(
                stderr,
                "btree-store fatal code=BTREE_FATAL_ADDRESS_SPACE space={space:?} next={next} requested={requested}"
            );
        }
        FatalReason::InvariantViolation(report) => match report {
            InvariantReport::Message { code, detail } => {
                let _ = writeln!(
                    stderr,
                    "btree-store fatal code=BTREE_FATAL_INVARIANT fault={code} detail={detail}"
                );
            }
        },
    }
    let _ = stderr.flush();
    std::process::abort()
}

pub(crate) fn invariant(code: &'static str, detail: impl Into<String>) -> ! {
    fatal(FatalReason::InvariantViolation(InvariantReport::Message {
        code,
        detail: detail.into(),
    }))
}

pub(crate) fn abort_store_fault(error: StoreFault, context: &'static str) -> ! {
    match error {
        StoreFault::Corruption => fatal(FatalReason::Corruption(CorruptionReport {
            code: "LIVE_ENGINE_CORRUPTION",
            generation: None,
            page_kind: "engine",
            pid: None,
            check: context,
            expected: None,
            actual: None,
        })),
    }
}

pub(crate) fn physical_value<T>(result: StoreResult<T>, context: &'static str) -> T {
    result.unwrap_or_else(|error| abort_store_fault(error, context))
}

pub type PageId = u32;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct DataPid(NonZeroU32);

impl DataPid {
    pub(crate) fn new(raw: u32) -> Option<Self> {
        (raw >= 2).then(|| Self(NonZeroU32::new(raw).unwrap()))
    }

    pub(crate) fn get(self) -> u32 {
        self.0.get()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RootRef {
    Empty,
    Node(DataPid),
}

impl RootRef {
    pub(crate) fn node(self) -> Option<DataPid> {
        match self {
            Self::Empty => None,
            Self::Node(id) => Some(id),
        }
    }

    pub(crate) fn decode(raw: u32) -> Self {
        if raw == 0 {
            return Self::Empty;
        }

        Self::Node(DataPid::new(raw).unwrap_or_else(|| {
            invariant(
                "INVALID_PHYSICAL_PAGE_ID",
                format!("physical root reference has invalid raw value {raw}"),
            )
        }))
    }

    pub(crate) fn get(self) -> u32 {
        self.node().map_or(0, DataPid::get)
    }
}

pub const MAGIC: u64 = 0x636f776274726565; // cowbtree
pub const FORMAT_VERSION: u32 = 2;
pub use crate::node::{
    IDS_PER_INDIRECT_PAGE, MAX_INLINE_LEN, MAX_KEY_LEN, MAX_VAL_LEN, PAGE_SIZE, SLOT_SIZE,
    VALUE_PAGE_CONTENT,
};

/// Runtime sync policy used after commits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SyncMode {
    /// Sync data normally and use a full sync when the file grows.
    #[default]
    Adaptive,
    /// Always use data-only sync.
    Data,
    /// Always use a full file sync.
    All,
}

/// Runtime-only options used when opening a database handle.
///
/// These settings do not change the on-disk format. Within a single process,
/// the first successful open of a given path fixes the runtime options for the
/// shared live instance. Later opens of the same path must use identical
/// options or they return [`OpenError::InvalidOptions`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenOptions {
    /// Number of physical page nodes cached by the shared BTree runtime.
    pub cache_capacity: usize,
    /// Sync policy used after metadata commits.
    pub sync_mode: SyncMode,
    /// Open an **existing** database read-only: the file is not created, no byte
    /// of it is ever written, and it is locked shared instead of exclusively
    ///
    /// Defaults to `false`.
    pub read_only: bool,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            cache_capacity: 8192,
            sync_mode: SyncMode::Adaptive,
            read_only: false,
        }
    }
}

impl OpenOptions {
    /// Create a new options object with the default runtime settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Open or create a database using these runtime options.
    pub fn open<P: AsRef<Path>>(&self, path: P) -> OpenResult<BTree> {
        BTree::open_with_options(path, self.clone())
    }
}

struct RegistryEntry {
    instance: Weak<BTree>,
    gate: Arc<Mutex<()>>,
}

static BTREE_INSTANCE_REGISTRY: OnceLock<Mutex<HashMap<PathBuf, RegistryEntry>>> = OnceLock::new();

fn btree_instance_registry() -> &'static Mutex<HashMap<PathBuf, RegistryEntry>> {
    BTREE_INSTANCE_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

fn sweep_dead_btree_instances(reg: &mut HashMap<PathBuf, RegistryEntry>) {
    reg.retain(|_, entry| entry.instance.strong_count() > 0 || Arc::strong_count(&entry.gate) > 1);
}

fn normalize_db_path(path: &Path) -> PathBuf {
    if let Ok(canonical) = std::fs::canonicalize(path) {
        return canonical;
    }

    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else if let Ok(cwd) = std::env::current_dir() {
        cwd.join(path)
    } else {
        path.to_path_buf()
    };

    let parent_canonical = absolute
        .parent()
        .and_then(|p| std::fs::canonicalize(p).ok());
    if let Some(parent) = parent_canonical
        && let Some(name) = absolute.file_name()
    {
        return parent.join(name);
    }
    absolute
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct MetaNode {
    pub magic: u64,
    pub seq: u64,
    pub format_version: u32,
    pub catalog_root: PageId,
    pub next_page_id: PageId,
    pub reusable_root: PageId,
    pub retired_root: PageId,
    pub checksum: u32,
}

const META_NODE_SIZE: usize = std::mem::size_of::<MetaNode>();
const _: () = assert!(META_NODE_SIZE == 40);

impl MetaNode {
    pub fn as_page_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts((self as *const Self).cast::<u8>(), META_NODE_SIZE) }
    }

    pub fn from_slice(x: &[u8]) -> Self {
        Self::decode(x).expect("meta slice must contain a complete fixed-width record")
    }

    pub(crate) fn decode(x: &[u8]) -> StoreResult<Self> {
        if x.len() < META_NODE_SIZE {
            return Err(StoreFault::Corruption);
        }
        Ok(unsafe { std::ptr::read_unaligned(x.as_ptr().cast::<Self>()) })
    }
}

impl Default for MetaNode {
    fn default() -> Self {
        Self::new()
    }
}

impl MetaNode {
    pub fn new() -> Self {
        let mut this = Self {
            magic: MAGIC,
            format_version: FORMAT_VERSION,
            catalog_root: 0,
            next_page_id: 2, // skip two meta pages
            reusable_root: 0,
            retired_root: 0,
            seq: 1,
            checksum: 0,
        };
        this.update_checksum();
        this
    }

    // callers must serialize updates when a MetaNode is shared
    pub fn update_checksum(&mut self) {
        self.checksum = 0;
        self.checksum = self.calc_checksum();
    }

    fn calc_checksum(&self) -> u32 {
        let mut h = crc32c::Crc32cHasher::default();
        h.write(&self.as_page_slice()[..META_NODE_SIZE - size_of_val(&self.checksum)]);
        h.finish() as u32
    }

    pub(crate) fn validate(&self) -> StoreResult<()> {
        if self.magic == 0 && self.seq == 0 {
            return Err(StoreFault::Corruption);
        }
        if self.checksum != self.calc_checksum() {
            return Err(StoreFault::Corruption);
        }
        Ok(())
    }
}

use crate::{
    cache::NodeCache,
    node::{AlignedPage, BranchRewrite, ChildPos, LeafWrite, Node, NonEmptyKey},
    store::{MetaSnapshot, Store},
};
pub(crate) fn validate_input(key: &[u8], val: &[u8]) -> Result<()> {
    if key.is_empty() {
        return Err(Error::InvalidKey(KeyError::Empty));
    }
    if key.len() > MAX_KEY_LEN {
        return Err(Error::InvalidKey(KeyError::TooLarge {
            len: key.len(),
            max: MAX_KEY_LEN,
        }));
    }
    if val.len() > MAX_VAL_LEN {
        return Err(Error::ValueTooLarge {
            len: val.len(),
            max: MAX_VAL_LEN,
        });
    }
    Ok(())
}

pub(crate) fn validate_bucket_input(bucket: &str) -> Result<()> {
    if bucket.is_empty() {
        return Err(Error::InvalidBucket(BucketError::Empty));
    }
    if bucket.len() > MAX_KEY_LEN {
        return Err(Error::InvalidBucket(BucketError::TooLarge {
            len: bucket.len(),
            max: MAX_KEY_LEN,
        }));
    }
    Ok(())
}

fn layout_from_flags(flags: u32) -> Layout {
    if flags & 1 == 1 {
        Layout::Prefix
    } else {
        Layout::Plain
    }
}

struct Route {
    node: Arc<Node>,
    page_id: DataPid,
    pos: usize,
}

pub(crate) struct BTreeRuntime {
    store: Arc<Store>,
    cache: NodeCache,
}

impl BTreeRuntime {
    fn new(store: Arc<Store>, cache_capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            store,
            cache: NodeCache::new(cache_capacity),
        })
    }

    fn store(&self) -> &Store {
        self.store.as_ref()
    }

    #[inline(always)]
    fn load_node(&self, id: DataPid) -> Arc<Node> {
        if let Some(node) = self.cache.get(id.get()) {
            return node;
        }

        self.load_node_miss(id)
    }

    fn load_node_miss(&self, id: DataPid) -> Arc<Node> {
        let page = self
            .cache
            .take_recycled_page(id.get())
            .unwrap_or_else(AlignedPage::new);
        let node = physical_value(self.store.read_node(id, page), "physical node load");
        #[cfg(test)]
        self.store.node_io.record_cache_put(id.get());
        self.cache.put(id.get(), node.clone());
        node
    }

    fn load_node_uncached(&self, id: DataPid) -> Arc<Node> {
        self.store.read_node_without_cache(id)
    }

    fn clear_cache(&self) {
        self.cache.clear();
    }

    fn invalidate_node(&self, page_id: PageId) {
        self.cache.invalidate(page_id);
    }

    /// Test-only positive occupancy probe. `get` ages an entry, so a test that
    /// merely asks whether a PID is resident needs a non-mutating read.
    #[cfg(test)]
    fn cached_node_is_leaf(&self, id: DataPid) -> Option<bool> {
        self.cache.peek(id.get()).map(|node| node.is_leaf())
    }

    fn alloc_data_page(&self, alloc: &mut HashSet<PageId>) -> StoreResult<DataPid> {
        self.store.alloc_data_page_observed(alloc, &self.cache)
    }

    fn alloc_data_pages(
        &self,
        nr_pages: u32,
        alloc: &mut HashSet<PageId>,
    ) -> StoreResult<Vec<DataPid>> {
        self.store
            .alloc_data_pages_observed(nr_pages, alloc, &self.cache)
    }

    fn recycle_allocated_pages(&self, page_id: PageId, nr_pages: u32) {
        self.store
            .recycle_allocated_pages_observed(page_id, nr_pages, &self.cache);
    }

    fn free_pages(&self, page_id: PageId, nr_pages: u32) -> StoreResult<()> {
        self.store
            .free_pages_observed(page_id, nr_pages, &self.cache)
    }

    fn commit_roots_with_pending_alloc(
        &self,
        catalog_root: PageId,
        pending_free: &[(PageId, u32)],
        pending_alloc: &HashSet<PageId>,
    ) -> StoreResult<()> {
        self.store.commit_roots_with_pending_alloc_observed(
            catalog_root,
            pending_free,
            pending_alloc,
            &self.cache,
        )
    }

    fn commit_generation_only(
        &self,
        catalog_root: PageId,
        deferred_alloc: &HashSet<PageId>,
    ) -> StoreResult<()> {
        self.store
            .commit_generation_only_observed(catalog_root, deferred_alloc, &self.cache)
    }
}

#[derive(Clone)]
pub(crate) struct TreeReadContext {
    runtime: Arc<BTreeRuntime>,
    layout: Layout,
    /// Transaction-private residency layer. `None` for handle-level reads,
    /// views and read-only transactions; clones only ever propagate it.
    overlay: Option<Arc<RwLock<TxnOverlay>>>,
}

/// Node-class selection for newly created nodes. Reads are self-describing
/// from the page's first u32; this only decides which class to construct.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Layout {
    Plain,
    Prefix,
}

impl TreeReadContext {
    pub(crate) fn new(runtime: Arc<BTreeRuntime>) -> Self {
        Self {
            runtime,
            layout: Layout::Plain,
            overlay: None,
        }
    }

    /// Returns a clone that builds new nodes using `layout` (the catalog and
    /// the TxnCore base always stay plain; bucket trees override per bucket).
    /// The residency layer is copied, never created.
    pub(crate) fn with_layout(&self, layout: Layout) -> Self {
        Self {
            runtime: self.runtime.clone(),
            layout,
            overlay: self.overlay.clone(),
        }
    }

    /// Attaches the transaction-private residency layer. Every clone made
    /// from this context (bucket transactions, iterators, write contexts) shares the same one.
    pub(crate) fn with_overlay(&self, overlay: Arc<RwLock<TxnOverlay>>) -> Self {
        Self {
            runtime: self.runtime.clone(),
            layout: self.layout,
            overlay: Some(overlay),
        }
    }

    /// Whether this write may be deferred. Turning the budget off is the *entire*
    /// exhaustion action, and it is sticky: once off it never re-opens.
    fn take_dirty_slot(&self) -> bool {
        let Some(overlay) = &self.overlay else {
            return false;
        };
        let mut overlay = overlay.write();
        if overlay.may_defer() {
            true
        } else {
            overlay.disable_defer();
            false
        }
    }

    fn insert_overlay_dirty(&self, page_id: PageId, node: Arc<Node>) {
        if let Some(overlay) = &self.overlay {
            overlay.write().insert_dirty(page_id, node);
        }
    }

    fn insert_overlay_clean(&self, page_id: PageId, node: Arc<Node>) {
        if let Some(overlay) = &self.overlay {
            overlay.write().insert_clean(page_id, node);
        }
    }

    /// Commit flush (step 1): write every Dirty page — sealed with its final PID —
    /// and only after all of them are on disk degrade them to Clean. The step runs under the
    /// overlay's write lock, and the store's live write path is fatal on I/O error, so a partial
    /// flush is unobservable.
    fn flush_overlay(&self) {
        let Some(overlay) = &self.overlay else {
            return;
        };
        let mut overlay = overlay.write();
        let pages = overlay.dirty_pages_sorted();
        if pages.is_empty() {
            return;
        }
        physical_value(self.store().write_node_runs(&pages), "commit flush");
        overlay.mark_all_dirty_clean(&pages);
    }

    /// Dense coverage (step 2): called once per publication, before any meta slot is
    /// written, with `h` = the **live** high-water mark (never a published snapshot's).
    fn cover_id_space(&self) {
        let high_water = self.store().cached_snapshot().next_page_id;
        self.store().cover_id_space(high_water);
    }

    fn drop_overlay_pages(&self, page_id: PageId, nr_pages: u32) {
        #[cfg(test)]
        for offset in 0..u64::from(nr_pages) {
            self.store()
                .node_io
                .record_release(page_id + offset as PageId);
        }
        if let Some(overlay) = &self.overlay {
            overlay.write().drop_pages(page_id, nr_pages);
        }
    }

    fn clear_overlay(&self) {
        if let Some(overlay) = &self.overlay {
            overlay.write().clear();
        }
    }

    fn overlay_has_dirty(&self) -> bool {
        self.overlay
            .as_ref()
            .is_some_and(|overlay| overlay.read().has_dirty())
    }

    #[cfg(test)]
    pub(crate) fn overlay_len(&self) -> usize {
        self.overlay
            .as_ref()
            .map_or(0, |overlay| overlay.read().len())
    }

    // Ownership survives Clean eviction, so an empty residency set is insufficient.
    pub(crate) fn overlay_is_empty(&self) -> bool {
        self.overlay
            .as_ref()
            .is_none_or(|overlay| overlay.read().is_empty())
    }

    #[cfg(test)]
    pub(crate) fn overlay_owned_pids(&self) -> Vec<PageId> {
        self.overlay.as_ref().map_or_else(Vec::new, |overlay| {
            let mut pids: Vec<PageId> = overlay.read().owned.iter().copied().collect();
            pids.sort_unstable();
            pids
        })
    }

    #[cfg(test)]
    pub(crate) fn overlay_dirty_pids(&self) -> Vec<PageId> {
        self.overlay.as_ref().map_or_else(Vec::new, |overlay| {
            let mut pids: Vec<PageId> = overlay
                .read()
                .entries
                .iter()
                .filter(|(_, entry)| entry.tier == OverlayTier::Dirty)
                .map(|(pid, _)| *pid)
                .collect();
            pids.sort_unstable();
            pids
        })
    }

    #[cfg(test)]
    pub(crate) fn overlay_entry_pids(&self) -> Vec<PageId> {
        self.overlay.as_ref().map_or_else(Vec::new, |overlay| {
            let mut pids: Vec<PageId> = overlay.read().entries.keys().copied().collect();
            pids.sort_unstable();
            pids
        })
    }

    #[cfg(test)]
    pub(crate) fn overlay_tiers(&self) -> (usize, usize) {
        self.overlay.as_ref().map_or((0, 0), |overlay| {
            let overlay = overlay.read();
            (overlay.dirty_count(), overlay.clean_count())
        })
    }

    #[cfg(test)]
    pub(crate) fn overlay_order_len(&self) -> usize {
        self.overlay
            .as_ref()
            .map_or(0, |overlay| overlay.read().order_len())
    }

    fn overlay_owns(&self, page_id: PageId) -> bool {
        self.overlay
            .as_ref()
            .is_some_and(|overlay| overlay.read().owns(page_id))
    }

    #[cfg(test)]
    pub(crate) fn overlay_defer_enabled(&self) -> bool {
        self.overlay
            .as_ref()
            .is_some_and(|overlay| overlay.read().defer_enabled())
    }

    #[cfg(test)]
    pub(crate) fn overlay_owned_peak(&self) -> usize {
        self.overlay
            .as_ref()
            .map_or(0, |overlay| overlay.read().owned_peak())
    }

    /// Overlay routing:
    /// - entry hit: answer from memory — no `pread`, no CRC check;
    /// - owned PID whose entry was evicted: read *without* the shared cache and re-insert,
    ///   because a transaction-private page must never enter the shared `NodeCache`;
    /// - otherwise: `None`, and the caller takes the pre-existing path.
    ///
    /// A read-only transaction has no overlay at all (`view` and every read-only context build
    /// theirs with `None`). Keeping that test in this inlined guard — and the query, whose miss path
    /// reads a page from disk, out of line — is what leaves the read-only node load a single
    /// predictable branch: inlining the whole body instead would put a non-inlinable call, and its
    /// `Option<Arc<Node>>` drop glue, on every level of every `get`.
    #[inline(always)]
    fn overlay_lookup(&self, id: DataPid) -> Option<Arc<Node>> {
        self.overlay.as_ref()?;
        self.overlay_lookup_active(id)
    }

    fn overlay_lookup_active(&self, id: DataPid) -> Option<Arc<Node>> {
        let overlay = self
            .overlay
            .as_ref()
            .expect("overlay checked by the caller");
        if let Some(node) = overlay.read().get(id.get()) {
            #[cfg(test)]
            self.store().node_io.record_overlay_hit(id.get());
            return Some(node);
        }
        if !overlay.read().owns(id.get()) {
            return None;
        }
        #[cfg(test)]
        self.store().node_io.record_overlay_miss(id.get());
        let node = self.runtime.load_node_uncached(id);
        overlay.write().insert_clean(id.get(), node.clone());
        Some(node)
    }

    pub(crate) fn new_leaf(&self) -> Node {
        match self.layout {
            Layout::Plain => Node::new_leaf(),
            Layout::Prefix => Node::new_encoded_leaf(),
        }
    }

    pub(crate) fn new_branch_root(
        &self,
        left_page_id: DataPid,
        separator: NonEmptyKey,
        right_page_id: DataPid,
    ) -> Node {
        match self.layout {
            Layout::Plain => Node::new_branch_root(left_page_id, separator, right_page_id),
            Layout::Prefix => Node::new_encoded_branch_root(left_page_id, separator, right_page_id),
        }
    }

    fn store(&self) -> &Store {
        self.runtime.store()
    }

    #[inline(always)]
    fn load_node(&self, id: DataPid) -> Arc<Node> {
        if let Some(node) = self.overlay_lookup(id) {
            return node;
        }
        self.runtime.load_node(id)
    }

    fn load_node_uncached(&self, id: DataPid) -> Arc<Node> {
        if let Some(node) = self.overlay_lookup(id) {
            return node;
        }
        self.runtime.load_node_uncached(id)
    }

    fn alloc_data_page(&self, alloc: &mut HashSet<PageId>) -> StoreResult<DataPid> {
        let pid = self.runtime.alloc_data_page(alloc)?;
        #[cfg(test)]
        self.store().node_io.record_alloc(pid.get());
        Ok(pid)
    }

    fn alloc_data_pages(
        &self,
        nr_pages: u32,
        alloc: &mut HashSet<PageId>,
    ) -> StoreResult<Vec<DataPid>> {
        let pids = self.runtime.alloc_data_pages(nr_pages, alloc)?;
        #[cfg(test)]
        for pid in &pids {
            self.store().node_io.record_alloc(pid.get());
        }
        Ok(pids)
    }

    fn recycle_allocated_pages(&self, page_id: PageId, nr_pages: u32) {
        self.drop_overlay_pages(page_id, nr_pages);
        self.runtime.recycle_allocated_pages(page_id, nr_pages);
    }

    fn free_pages(&self, page_id: PageId, nr_pages: u32) -> StoreResult<()> {
        self.drop_overlay_pages(page_id, nr_pages);
        self.runtime.free_pages(page_id, nr_pages)
    }

    fn load_page(&self, id: DataPid) -> Vec<u8> {
        self.store().load_page(id)
    }

    fn load_data(&self, pages: &[DataPid], len: usize) -> Vec<u8> {
        physical_value(
            self.store().load_data_pids(pages, len),
            "physical value pages load",
        )
    }
}

/// Operation-local physical effects. The transaction page state merges these effects only after
/// the complete COW rewrite succeeds.
pub(crate) struct TreeWriteContext<'a> {
    read: &'a TreeReadContext,
    freed: &'a mut Vec<(PageId, u32)>,
    alloc: &'a mut HashSet<PageId>,
}

impl<'a> TreeWriteContext<'a> {
    fn new(
        read: &'a TreeReadContext,
        freed: &'a mut Vec<(PageId, u32)>,
        alloc: &'a mut HashSet<PageId>,
    ) -> Self {
        Self { read, freed, alloc }
    }

    fn alloc_page(&mut self) -> StoreResult<DataPid> {
        self.read.alloc_data_page(self.alloc)
    }

    fn alloc_pages(&mut self, nr_pages: u32) -> StoreResult<Vec<DataPid>> {
        self.read.alloc_data_pages(nr_pages, self.alloc)
    }

    fn write_node(&mut self, node: Node) -> StoreResult<DataPid> {
        let pid = self.alloc_page()?;
        if self.read.take_dirty_slot() {
            self.read.insert_overlay_dirty(pid.get(), Arc::new(node));
            return Ok(pid);
        }
        // With the dirty tier exhausted the page goes to disk now, sealed in place with
        // its final PID — the 4096-byte buffer is never copied just to compute the CRC.
        let mut node = node;
        let page = node.finalize_mut();
        self.read.store().write_physical_page(pid, page);
        #[cfg(test)]
        self.read.store().node_io.record_write(pid.get());
        self.read.insert_overlay_clean(pid.get(), Arc::new(node));
        Ok(pid)
    }

    /// Physical page-array write (indirect chains): the caller seals each page
    /// with its own PID before calling this (the chain builder does it, leaving the
    /// id slots it never filled zero); the store only writes.
    fn write_physical_pages(&mut self, ids: &[DataPid], data: &mut [u8]) -> StoreResult<()> {
        let ids: Vec<PageId> = ids.iter().map(|id| id.get()).collect();
        // Indirect pages get their own class so a node-only write counter never counts them.
        #[cfg(test)]
        self.read
            .store()
            .node_io
            .record_many(crate::store::NodeIoKind::IndirectWrite, &ids);
        self.read.store().write_physical_pages(&ids, data)
    }

    fn write_value_pages(&mut self, ids: &[DataPid], value: &[u8]) -> StoreResult<()> {
        let ids: Vec<PageId> = ids.iter().map(|id| id.get()).collect();
        #[cfg(test)]
        self.read
            .store()
            .node_io
            .record_many(crate::store::NodeIoKind::ValueWrite, &ids);
        self.read.store().write_value_pages(&ids, value)
    }

    /// Frees the pages referenced by `slot` (inline slots have none).
    pub(crate) fn free_slot(&mut self, slot: &crate::node::Slot) {
        crate::node::free_slot_pages_for(self.read, slot, self.freed);
    }

    fn free_page(&mut self, id: DataPid) {
        self.freed.push((id.get(), 1));
    }
}

pub(crate) struct Tree;

impl Tree {
    fn traverse_to_leaf(
        read: &TreeReadContext,
        mut node: Arc<Node>,
        mut page_id: DataPid,
        key: &[u8],
    ) -> (Vec<Route>, Arc<Node>, DataPid) {
        let mut stack = Vec::new();
        while !node.is_leaf() {
            let pos = match node.search(key) {
                Ok(pos) => pos,
                Err(pos) => pos.saturating_sub(1),
            };
            let child_id = node.child_at(pos);
            let child_node = read.load_node(child_id);
            stack.push(Route { node, page_id, pos });
            node = child_node;
            page_id = child_id;
        }
        (stack, node, page_id)
    }

    pub(crate) fn put(
        read: &TreeReadContext,
        ctx: &mut TreeWriteContext,
        root: RootRef,
        key: &[u8],
        value: &[u8],
    ) -> StoreResult<RootRef> {
        Self::execute_put(read, ctx, root, key, value)
    }

    pub(crate) fn update(
        read: &TreeReadContext,
        ctx: &mut TreeWriteContext,
        root: RootRef,
        key: &[u8],
        value: &[u8],
    ) -> StoreResult<(bool, RootRef)> {
        Self::execute_update(read, ctx, root, key, value)
    }

    fn execute_put(
        read: &TreeReadContext,
        ctx: &mut TreeWriteContext,
        root: RootRef,
        key: &[u8],
        value: &[u8],
    ) -> StoreResult<RootRef> {
        let current_root_id = root.node();

        let Some(current_root_id) = current_root_id else {
            let mut node = read.new_leaf();
            node.put_leaf(ctx, key, value)?;
            return Ok(RootRef::Node(ctx.write_node(node)?));
        };

        let root_node = read.load_node(current_root_id);
        let (mut stack, leaf_node_arc, leaf_id) =
            Self::traverse_to_leaf(read, root_node, current_root_id, key);

        let mut current_node = (*leaf_node_arc).clone();

        let mut split_info = Self::apply_insert(ctx, &mut current_node, key, value)?;

        let mut new_child_id = ctx.write_node(current_node)?;
        ctx.free_page(leaf_id);

        while let Some(Route {
            node: parent_arc,
            page_id: parent_id,
            pos,
        }) = stack.pop()
        {
            let mut parent = (*parent_arc).clone();

            if let Some((sep, rhs)) = split_info.take() {
                let expected_old = parent.child_at(pos);
                let rhs_id = ctx.write_node(rhs)?;
                split_info = match parent.apply_branch_split_rewrite(
                    ChildPos::new(pos),
                    expected_old,
                    new_child_id,
                    sep,
                    rhs_id,
                ) {
                    BranchRewrite::Applied => None,
                    BranchRewrite::Split { separator, right } => Some((separator, right)),
                };
            } else {
                parent.update_child_page(ChildPos::new(pos), new_child_id);
            }

            new_child_id = ctx.write_node(parent)?;
            ctx.free_page(parent_id);
        }

        if let Some((sep, rhs)) = split_info {
            let rhs_id = ctx.write_node(rhs)?;
            let new_root = read.new_branch_root(new_child_id, sep, rhs_id);
            Ok(RootRef::Node(ctx.write_node(new_root)?))
        } else {
            // root did not split, simply update root pointer
            Ok(RootRef::Node(new_child_id))
        }
    }

    fn execute_update(
        read: &TreeReadContext,
        ctx: &mut TreeWriteContext,
        root: RootRef,
        key: &[u8],
        value: &[u8],
    ) -> StoreResult<(bool, RootRef)> {
        let current_root_id = root.node();

        let Some(current_root_id) = current_root_id else {
            return Ok((false, root));
        };

        let root_node = read.load_node(current_root_id);
        let (mut stack, leaf_node_arc, leaf_id) =
            Self::traverse_to_leaf(read, root_node, current_root_id, key);
        let mut current_node = (*leaf_node_arc).clone();

        let pos = match current_node.search(key) {
            Ok(pos) => pos,
            Err(_) => return Ok((false, root)),
        };

        current_node.update_leaf_at(ctx, pos, value)?;

        let mut new_child_id = ctx.write_node(current_node)?;
        ctx.free_page(leaf_id);

        while let Some(Route {
            node: parent_arc,
            page_id: parent_id,
            pos,
        }) = stack.pop()
        {
            let mut parent = (*parent_arc).clone();
            parent.update_child_page(ChildPos::new(pos), new_child_id);
            new_child_id = ctx.write_node(parent)?;
            ctx.free_page(parent_id);
        }

        Ok((true, RootRef::Node(new_child_id)))
    }

    fn apply_insert(
        ctx: &mut TreeWriteContext,
        node: &mut Node,
        key: &[u8],
        value: &[u8],
    ) -> StoreResult<Option<(NonEmptyKey, Node)>> {
        debug_assert!(node.is_leaf());
        let r = match node.put_leaf(ctx, key, value)? {
            LeafWrite::Applied => None,
            LeafWrite::SplitRequired => Some(node.split_leaf_for_insert(ctx, key, value)?),
        };
        Ok(r)
    }

    pub(crate) fn get(read: &TreeReadContext, root: RootRef, key: &[u8]) -> Option<Vec<u8>> {
        let root_id = root.node()?;
        let mut current = read.load_node(root_id);
        loop {
            if current.is_leaf() {
                return current.get(read, key);
            }
            let pos = current.child_pos_for_key(key);
            current = read.load_node(current.child_at(pos));
        }
    }

    pub(crate) fn find(
        read: &TreeReadContext,
        root: RootRef,
        key: &[u8],
    ) -> Option<(Arc<Node>, usize)> {
        let root_id = root.node()?;
        let mut current = read.load_node(root_id);
        loop {
            if current.is_leaf() {
                return current.search(key).ok().map(|pos| (current, pos));
            }
            let pos = current.child_pos_for_key(key);
            current = read.load_node(current.child_at(pos));
        }
    }

    pub(crate) fn del(
        read: &TreeReadContext,
        ctx: &mut TreeWriteContext,
        root: RootRef,
        key: &[u8],
    ) -> StoreResult<(bool, RootRef)> {
        Self::execute_del(read, ctx, root, key)
    }

    fn execute_del(
        read: &TreeReadContext,
        ctx: &mut TreeWriteContext,
        root: RootRef,
        key: &[u8],
    ) -> StoreResult<(bool, RootRef)> {
        let current_root_id = root.node();

        let Some(current_root_id) = current_root_id else {
            return Ok((false, root));
        };

        let root_node = read.load_node(current_root_id);
        let (mut stack, leaf_arc, leaf_id) =
            Self::traverse_to_leaf(read, root_node, current_root_id, key);

        let mut current_node = (*leaf_arc).clone();
        if current_node.search(key).is_err() {
            return Ok((false, root));
        }
        current_node.delete_leaf_key(ctx, key);

        let mut empty = current_node.is_empty();
        let mut new_child_id = if !empty {
            Some(ctx.write_node(current_node)?)
        } else {
            None
        };
        ctx.free_page(leaf_id);

        while let Some(Route {
            node: parent_arc,
            page_id: parent_id,
            pos,
        }) = stack.pop()
        {
            let mut parent = (*parent_arc).clone();

            if empty {
                parent.remove_branch_child(ChildPos::new(pos));
            } else {
                // if child node only changed content, update pointer in parent
                parent.update_child_page(
                    ChildPos::new(pos),
                    new_child_id.unwrap_or_else(|| {
                        invariant(
                            "DELETE_CHILD_MISSING",
                            "non-empty child must have a node id",
                        )
                    }),
                );
            }

            if parent.is_empty() {
                empty = true;
                new_child_id = None;
            } else {
                empty = false;
                new_child_id = Some(ctx.write_node(parent)?);
            }
            ctx.free_page(parent_id);
        }

        // if root is a branch node with only one child, elevate child to be the new root
        if let Some(mut promoted_id) = new_child_id {
            loop {
                let node_id = promoted_id;
                let node = read.load_node(node_id);
                if !node.is_leaf() && node.num_children() == 1 {
                    let child_id =
                        Self::canonicalize_promoted_root_child(read, ctx, node.child_at(0))?;
                    ctx.free_page(node_id);
                    promoted_id = child_id;
                } else {
                    break;
                }
            }
            new_child_id = Some(promoted_id);
        }

        Ok((true, new_child_id.map_or(RootRef::Empty, RootRef::Node)))
    }

    fn canonicalize_promoted_root_child(
        read: &TreeReadContext,
        ctx: &mut TreeWriteContext,
        child_id: DataPid,
    ) -> StoreResult<DataPid> {
        let child = read.load_node(child_id);
        if child.is_leaf() || child.is_empty() || child.slot_at(0).klen == 0 {
            return Ok(child_id);
        }

        let mut rewritten = (*child).clone();
        rewritten.canonicalize_branch_slot_zero();
        let new_child_id = ctx.write_node(rewritten)?;
        ctx.free_page(child_id);
        Ok(new_child_id)
    }

    pub(crate) fn collect_tree_pages_uncached(
        read: &TreeReadContext,
        root: RootRef,
        freed: &mut Vec<(PageId, u32)>,
        node_pages: &mut Vec<PageId>,
    ) {
        let Some(root_id) = root.node() else {
            return;
        };

        let mut stack = vec![root_id];
        let mut visited = HashSet::new();

        while let Some(current_id) = stack.pop() {
            if !visited.insert(current_id.get()) {
                continue;
            }

            let node = read.load_node_uncached(current_id);
            node_pages.push(current_id.get());
            freed.push((current_id.get(), 1));

            for i in 0..node.num_children() {
                if node.is_leaf() {
                    let slot = node.slot_at(i);
                    if !slot.is_inline() {
                        node.free_slot_pages(read, slot, freed);
                    }
                } else {
                    stack.push(node.child_at(i));
                }
            }
        }
    }

    pub(crate) fn iterator(read: &TreeReadContext, root: RootRef) -> TreeIterator<'_> {
        TreeIterator::new(read.clone(), root, None)
    }
}

pub struct TreeIterator<'txn> {
    read: TreeReadContext,
    root: RootRef,
    root_node: Option<Arc<Node>>,
    forward_initialized: bool,
    reverse_initialized: bool,
    stack: Vec<(Arc<Node>, usize)>,
    current_leaf: Option<(Arc<Node>, usize)>,
    reverse_stack: Vec<(Arc<Node>, usize)>,
    reverse_leaf: Option<(Arc<Node>, usize)>,
    _borrow: std::marker::PhantomData<&'txn ()>,
}

impl TreeIterator<'_> {
    #[inline]
    fn load_child_node(&self, child_id: DataPid) -> Arc<Node> {
        self.read.load_node(child_id)
    }

    fn new(read: TreeReadContext, root: RootRef, root_node: Option<Arc<Node>>) -> Self {
        Self {
            read,
            root,
            root_node,
            forward_initialized: false,
            reverse_initialized: false,
            stack: Vec::new(),
            current_leaf: None,
            reverse_stack: Vec::new(),
            reverse_leaf: None,
            _borrow: std::marker::PhantomData,
        }
    }

    fn load_root_node(&mut self) -> Option<Arc<Node>> {
        let root_id = self.root.node()?;
        if let Some(root_node) = self.root_node.as_ref() {
            return Some(root_node.clone());
        }

        let root_node = self.read.load_node(root_id);
        self.root_node = Some(root_node.clone());
        Some(root_node)
    }

    fn initialize_forward(&mut self) {
        if self.forward_initialized {
            return;
        }
        self.forward_initialized = true;
        if let Some(root_node) = self.load_root_node() {
            self.push_node(root_node);
        }
    }

    fn initialize_reverse(&mut self) {
        if self.reverse_initialized {
            return;
        }
        self.reverse_initialized = true;
        if let Some(root_node) = self.load_root_node() {
            self.push_reverse_node(root_node);
        }
    }

    fn push_node(&mut self, node: Arc<Node>) {
        if node.is_leaf() {
            self.current_leaf = Some((node, 0));
        } else {
            self.stack.push((node, 0));
        }
    }

    fn push_reverse_node(&mut self, node: Arc<Node>) {
        let num_children = node.num_children();
        if node.is_leaf() {
            self.reverse_leaf = Some((node, num_children));
        } else {
            self.reverse_stack.push((node, num_children));
        }
    }

    fn copy_item(
        read: &TreeReadContext,
        leaf: &Node,
        idx: usize,
        key_buf: &mut Vec<u8>,
        val_buf: &mut Vec<u8>,
    ) {
        let slot = leaf.slot_at(idx);

        leaf.full_key(idx, key_buf);

        val_buf.clear();
        if slot.is_inline() {
            val_buf.extend_from_slice(leaf.value_at(idx));
        } else {
            val_buf.extend_from_slice(&leaf.load_overflow_value(read, slot));
        }
    }

    /// Fills the supplied buffers with the next key/value pair in ascending
    /// key order. A newly created iterator starts at the smallest key.
    pub fn next_ref(&mut self, key_buf: &mut Vec<u8>, val_buf: &mut Vec<u8>) -> bool {
        self.initialize_forward();
        loop {
            if let Some((leaf, idx)) = self.current_leaf.as_mut() {
                if *idx < leaf.num_children() {
                    Self::copy_item(&self.read, leaf, *idx, key_buf, val_buf);
                    *idx += 1;
                    return true;
                } else {
                    self.current_leaf = None;
                }
            }

            if let Some((node, idx)) = self.stack.last_mut() {
                if *idx < node.num_children() {
                    let child_id = node.child_at(*idx);
                    *idx += 1;
                    let child_node = self.load_child_node(child_id);
                    self.push_node(child_node);
                } else {
                    self.stack.pop();
                }
            } else {
                return false;
            }
        }
    }

    /// Fills the supplied buffers with the next key/value pair in descending
    /// key order. A newly created iterator starts at the largest key.
    pub fn prev_ref(&mut self, key_buf: &mut Vec<u8>, val_buf: &mut Vec<u8>) -> bool {
        self.initialize_reverse();
        loop {
            if let Some((leaf, idx)) = self.reverse_leaf.as_mut() {
                if *idx > 0 {
                    *idx -= 1;
                    Self::copy_item(&self.read, leaf, *idx, key_buf, val_buf);
                    return true;
                } else {
                    self.reverse_leaf = None;
                }
            }

            if let Some((node, idx)) = self.reverse_stack.last_mut() {
                if *idx > 0 {
                    *idx -= 1;
                    let child_id = node.child_at(*idx);
                    let child_node = self.load_child_node(child_id);
                    self.push_reverse_node(child_node);
                } else {
                    self.reverse_stack.pop();
                }
            } else {
                return false;
            }
        }
    }
}

/// A mutable transaction handle scoped to a single bucket.
///
/// Instances are provided by [`BTree::exec`] and [`MultiTxn::exec`]. The handle
/// is valid only for the duration of the callback that receives it.
pub struct Txn<'a> {
    pub(crate) read: TreeReadContext,
    pub(crate) root: RootRef,
    pub(crate) page_state: &'a mut TxnPageState,
    pub(crate) pending_counts: Arc<PendingPageCounts>,
}

impl<'a> Txn<'a> {
    /// Inserts a key/value pair or overwrites the existing value for `key`.
    ///
    /// The key must be non-empty and no longer than [`MAX_KEY_LEN`] bytes.
    pub fn put<K, V>(&mut self, key: K, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let key = key.as_ref();
        let val = value.as_ref();
        validate_input(key, val)?;
        let root = self.root;
        self.root = physical_value(
            self.page_state
                .run(&self.read, |ctx| Tree::put(&self.read, ctx, root, key, val)),
            "transaction put",
        );
        self.pending_counts.update(self.page_state);
        Ok(())
    }

    /// Updates the value for `key` only if the key already exists.
    ///
    /// Returns `Ok(true)` when the key existed and was updated, or `Ok(false)`
    /// when the key was missing and no existing key/value state changed.
    /// The key must be non-empty and no longer than [`MAX_KEY_LEN`] bytes.
    pub fn update<K, V>(&mut self, key: K, value: V) -> Result<bool>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let key = key.as_ref();
        let val = value.as_ref();
        validate_input(key, val)?;
        let root = self.root;
        let (updated, new_root) = physical_value(
            self.page_state.run(&self.read, |ctx| {
                Tree::update(&self.read, ctx, root, key, val)
            }),
            "transaction update",
        );
        self.pending_counts.update(self.page_state);
        if updated {
            self.root = new_root;
        }
        Ok(updated)
    }

    /// Returns the value for `key`.
    ///
    /// Returns [`Error::KeyNotFound`] when the key does not exist. The
    /// key must be non-empty and no longer than [`MAX_KEY_LEN`] bytes.
    pub fn get<K>(&self, key: K) -> Result<Vec<u8>>
    where
        K: AsRef<[u8]>,
    {
        let key = key.as_ref();
        validate_input(key, &[])?;
        Tree::get(&self.read, self.root, key).ok_or(Error::KeyNotFound)
    }

    /// Deletes `key` from the current bucket.
    ///
    /// Returns [`Error::KeyNotFound`] when the key does not exist. The
    /// key must be non-empty and no longer than [`MAX_KEY_LEN`] bytes.
    pub fn del<K>(&mut self, key: K) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        let key = key.as_ref();
        validate_input(key, &[])?;
        let root = self.root;
        let (deleted, new_root) = physical_value(
            self.page_state
                .run(&self.read, |ctx| Tree::del(&self.read, ctx, root, key)),
            "transaction delete",
        );
        self.pending_counts.update(self.page_state);
        if deleted {
            self.root = new_root;
            Ok(())
        } else {
            Err(Error::KeyNotFound)
        }
    }

    /// Returns an iterator over the current bucket in key order.
    pub fn iter(&self) -> TreeIterator<'_> {
        Tree::iterator(&self.read, self.root)
    }
}

pub(crate) struct ReadOnlyTree<'read> {
    read: &'read TreeReadContext,
    root: RootRef,
    root_node: Option<Arc<Node>>,
}

impl<'read> ReadOnlyTree<'read> {
    fn new(read: &'read TreeReadContext, root: RootRef) -> Self {
        let root_node = root.node().map(|id| read.load_node(id));
        Self {
            read,
            root,
            root_node,
        }
    }

    #[inline(always)]
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let root = self.root_node.as_deref()?;

        if root.is_leaf() {
            return root.get(self.read, key);
        }

        let pos = root.child_pos_for_key(key);
        let mut current = self.read.load_node(root.child_at(pos));
        loop {
            if current.is_leaf() {
                return current.get(self.read, key);
            }
            let pos = current.child_pos_for_key(key);
            current = self.read.load_node(current.child_at(pos));
        }
    }

    pub(crate) fn iterator(&self) -> TreeIterator<'_> {
        TreeIterator::new((*self.read).clone(), self.root, self.root_node.clone())
    }
}

/// A read-only transaction handle scoped to a single bucket snapshot.
///
/// Instances are provided by [`BTree::view`]. The handle is valid only for the
/// duration of the callback that receives it.
pub struct ReadOnlyTxn<'a> {
    pub(crate) tree: ReadOnlyTree<'a>,
    pub(crate) _guard: EpochGuard<'a>,
}

impl<'a> ReadOnlyTxn<'a> {
    /// Returns the value for `key` from the read-only snapshot.
    ///
    /// Returns [`Error::KeyNotFound`] when the key does not exist. The
    /// key must be non-empty and no longer than [`MAX_KEY_LEN`] bytes.
    pub fn get<K>(&self, key: K) -> Result<Vec<u8>>
    where
        K: AsRef<[u8]>,
    {
        let key = key.as_ref();
        validate_input(key, &[])?;
        self.tree.get(key).ok_or(Error::KeyNotFound)
    }

    /// Returns an iterator over the read-only bucket snapshot in key order.
    pub fn iter(&self) -> TreeIterator<'_> {
        self.tree.iterator()
    }
}

#[derive(Clone)]
struct TxnCheckpoint {
    catalog_root: RootRef,
    page_state: TxnPageCheckpoint,
}

#[derive(Clone)]
struct TxnPageCheckpoint {
    pending_free: Vec<(PageId, u32)>,
    pending_alloc: HashSet<PageId>,
}

#[derive(Default)]
struct TxnPageSavepoint {
    pending_free: Vec<(PageId, u32)>,
    pending_alloc: HashSet<PageId>,
}

#[derive(Default)]
struct TxnPageScratch {
    op_freed: Vec<(PageId, u32)>,
    op_alloc: HashSet<PageId>,
    released_pages: Vec<PageId>,
    free_runs: Vec<(PageId, u32)>,
}

#[derive(Default)]
struct TxnPageState {
    pending_free: Vec<(PageId, u32)>,
    pending_alloc: HashSet<PageId>,
    savepoint: Option<TxnPageSavepoint>,
    scratch: TxnPageScratch,
}

impl TxnPageState {
    fn run<T>(
        &mut self,
        read: &TreeReadContext,
        operation: impl FnOnce(&mut TreeWriteContext) -> StoreResult<T>,
    ) -> StoreResult<T> {
        let mut freed = std::mem::take(&mut self.scratch.op_freed);
        freed.clear();
        let mut alloc = std::mem::take(&mut self.scratch.op_alloc);
        alloc.clear();
        let result = {
            let mut ctx = TreeWriteContext::new(read, &mut freed, &mut alloc);
            operation(&mut ctx)
        };

        match result {
            Ok(value) => {
                self.merge_pending(read, freed, alloc);
                Ok(value)
            }
            Err(error) => {
                for page_id in alloc.drain() {
                    read.recycle_allocated_pages(page_id, 1);
                }
                freed.clear();
                self.scratch.op_freed = freed;
                self.scratch.op_alloc = alloc;
                Err(error)
            }
        }
    }

    fn merge_pending(
        &mut self,
        read: &TreeReadContext,
        mut freed: Vec<(PageId, u32)>,
        mut alloc: HashSet<PageId>,
    ) {
        let mut released_pages = std::mem::take(&mut self.scratch.released_pages);
        let mut free_runs = std::mem::take(&mut self.scratch.free_runs);
        Self::collect_released_pages(&mut freed, &mut released_pages);

        if let Some(savepoint) = self.savepoint.as_mut() {
            let mut kept = 0usize;
            for idx in 0..released_pages.len() {
                let page_id = released_pages[idx];
                if alloc.remove(&page_id) || savepoint.pending_alloc.remove(&page_id) {
                    read.recycle_allocated_pages(page_id, 1);
                } else {
                    released_pages[kept] = page_id;
                    kept += 1;
                }
            }
            released_pages.truncate(kept);
            Self::merge_released_pages(
                &mut savepoint.pending_free,
                &released_pages,
                &mut free_runs,
            );
            savepoint.pending_alloc.extend(alloc.drain());
        } else {
            let mut kept = 0usize;
            for idx in 0..released_pages.len() {
                let page_id = released_pages[idx];
                if alloc.remove(&page_id) || self.pending_alloc.remove(&page_id) {
                    read.recycle_allocated_pages(page_id, 1);
                } else {
                    released_pages[kept] = page_id;
                    kept += 1;
                }
            }
            released_pages.truncate(kept);
            Self::merge_released_pages(&mut self.pending_free, &released_pages, &mut free_runs);
            self.pending_alloc.extend(alloc.drain());
        }

        released_pages.clear();
        free_runs.clear();
        freed.clear();
        self.scratch.released_pages = released_pages;
        self.scratch.free_runs = free_runs;
        self.scratch.op_freed = freed;
        self.scratch.op_alloc = alloc;
    }

    fn begin_savepoint(&mut self) {
        if self.savepoint.is_some() {
            invariant(
                "SAVEPOINT_NESTING",
                "started a transaction savepoint while another savepoint was active",
            );
        }
        self.savepoint = Some(TxnPageSavepoint::default());
    }

    fn commit_savepoint(&mut self, read: &TreeReadContext) {
        let mut savepoint = self.savepoint.take().unwrap_or_else(|| {
            invariant(
                "SAVEPOINT_COMMIT",
                "committed a transaction savepoint without an active savepoint",
            )
        });

        let mut released_pages = std::mem::take(&mut self.scratch.released_pages);
        let mut free_runs = std::mem::take(&mut self.scratch.free_runs);
        Self::collect_released_pages(&mut savepoint.pending_free, &mut released_pages);

        let mut kept = 0usize;
        for idx in 0..released_pages.len() {
            let page_id = released_pages[idx];
            if self.pending_alloc.remove(&page_id) {
                read.recycle_allocated_pages(page_id, 1);
            } else {
                released_pages[kept] = page_id;
                kept += 1;
            }
        }
        released_pages.truncate(kept);
        Self::merge_released_pages(&mut self.pending_free, &released_pages, &mut free_runs);

        self.pending_alloc.extend(savepoint.pending_alloc);
        released_pages.clear();
        free_runs.clear();
        self.scratch.released_pages = released_pages;
        self.scratch.free_runs = free_runs;
    }

    fn rollback_savepoint(&mut self, read: &TreeReadContext) {
        let savepoint = self.savepoint.take().unwrap_or_else(|| {
            invariant(
                "SAVEPOINT_ROLLBACK",
                "rolled back a transaction savepoint without an active savepoint",
            )
        });

        for page_id in savepoint.pending_alloc {
            let _ = read.free_pages(page_id, 1);
        }
    }

    fn pending_alloc_len(&self) -> usize {
        self.pending_alloc.len()
            + self
                .savepoint
                .as_ref()
                .map_or(0, |savepoint| savepoint.pending_alloc.len())
    }

    fn pending_free_len(&self) -> usize {
        self.pending_free.len()
            + self
                .savepoint
                .as_ref()
                .map_or(0, |savepoint| savepoint.pending_free.len())
    }

    fn rollback_to(&mut self, read: &TreeReadContext, checkpoint: TxnPageCheckpoint) {
        if self.savepoint.is_some() {
            invariant(
                "SAVEPOINT_ROLLBACK_ORDER",
                "rolled back a transaction checkpoint while a savepoint was active",
            );
        }

        let current_alloc = std::mem::replace(&mut self.pending_alloc, checkpoint.pending_alloc);
        self.pending_free = checkpoint.pending_free;
        for page_id in current_alloc {
            if !self.pending_alloc.contains(&page_id) {
                let _ = read.free_pages(page_id, 1);
            }
        }
    }

    fn checkpoint(&self) -> TxnPageCheckpoint {
        if self.savepoint.is_some() {
            invariant(
                "SAVEPOINT_CHECKPOINT",
                "created a transaction checkpoint while a savepoint was active",
            );
        }
        TxnPageCheckpoint {
            pending_free: self.pending_free.clone(),
            pending_alloc: self.pending_alloc.clone(),
        }
    }

    fn merge_free_extent(free: &mut Vec<(PageId, u32)>, page_id: PageId, nr_pages: u32) {
        if page_id == 0 || nr_pages == 0 {
            return;
        }

        let mut start = page_id as u64;
        let mut end = start + nr_pages as u64;
        let mut idx = 0;

        while idx < free.len() && (free[idx].0 as u64) + (free[idx].1 as u64) < start {
            idx += 1;
        }

        while idx < free.len() {
            let (free_start, free_len) = free[idx];
            let free_start = free_start as u64;
            let free_end = free_start + free_len as u64;
            if free_start > end {
                break;
            }
            start = start.min(free_start);
            end = end.max(free_end);
            free.remove(idx);
        }

        free.insert(idx, (start as PageId, (end - start) as u32));
    }

    fn collect_released_pages(freed: &mut Vec<(PageId, u32)>, released_pages: &mut Vec<PageId>) {
        released_pages.clear();
        for (pid, nr) in freed.drain(..) {
            for page_id in pid..pid.saturating_add(nr) {
                released_pages.push(page_id);
            }
        }
        released_pages.sort_unstable();
        released_pages.dedup();
    }

    fn collect_free_runs(released_pages: &[PageId], free_runs: &mut Vec<(PageId, u32)>) {
        free_runs.clear();
        let Some(&first) = released_pages.first() else {
            return;
        };

        let mut start = first;
        let mut prev = first;
        for &page_id in &released_pages[1..] {
            if u64::from(page_id) == u64::from(prev) + 1 {
                prev = page_id;
                continue;
            }
            free_runs.push((start, prev - start + 1));
            start = page_id;
            prev = page_id;
        }
        free_runs.push((start, prev - start + 1));
    }

    fn merge_released_pages(
        free: &mut Vec<(PageId, u32)>,
        released_pages: &[PageId],
        free_runs: &mut Vec<(PageId, u32)>,
    ) {
        Self::collect_free_runs(released_pages, free_runs);
        for &(page_id, nr_pages) in free_runs.iter() {
            Self::merge_free_extent(free, page_id, nr_pages);
        }
    }
}

struct PendingPageCounts {
    snapshot: AtomicU64,
}

impl PendingPageCounts {
    fn encode(alloc: usize, free: usize) -> u64 {
        let alloc = u32::try_from(alloc).expect("too large");
        let free = u32::try_from(free).expect("too large");
        (u64::from(alloc) << 32) | u64::from(free)
    }

    fn decode(snapshot: u64) -> (usize, usize) {
        ((snapshot >> 32) as u32 as usize, snapshot as u32 as usize)
    }

    fn update(&self, state: &TxnPageState) {
        self.snapshot.store(
            Self::encode(state.pending_alloc_len(), state.pending_free_len()),
            Ordering::Release,
        );
    }

    fn clear(&self) {
        self.snapshot.store(0, Ordering::Release);
    }

    fn snapshot(&self) -> (usize, usize) {
        Self::decode(self.snapshot.load(Ordering::Acquire))
    }
}

/// Default residency budget, in pages (4096 pages ≈ 16 MiB).
pub(crate) const DEFAULT_RESIDENT_LIMIT: usize = 4096;

/// Default dirty budget, in pages (1024 pages ≈ 4 MiB).
pub(crate) const DEFAULT_DIRTY_LIMIT: usize = 1024;

/// Residency tier of an overlay entry. A `Dirty` entry is the only copy of its
/// page — nothing has been written to disk yet; a `Clean` entry's content is already on disk.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum OverlayTier {
    Dirty,
    Clean,
}

struct OverlayEntry {
    node: Arc<Node>,
    tier: OverlayTier,
}

/// Transaction-private page residency layer: resident pages plus deferred (not yet written) pages.
///
/// A `Dirty` entry's page has not reached the disk yet: the commit flush writes it and only then
/// degrades it to `Clean`. `Clean` entries only make the transaction's own
/// re-reads cheaper. `owned` is the authority for "this PID belongs to this transaction";
/// eviction never touches it (ownership outlives residency), while a PID leaving the pending set
/// drops both.
pub(crate) struct TxnOverlay {
    entries: HashMap<PageId, OverlayEntry>,
    order: VecDeque<PageId>,
    owned: HashSet<PageId>,
    #[cfg(test)]
    owned_peak: usize,
    dirty_count: usize,
    clean_count: usize,
    dirty_limit: usize,
    resident_limit: usize,
    /// Sticky: once the dirty budget is exhausted the transaction writes through
    /// for the rest of its life; it never re-opens.
    defer: bool,
}

impl TxnOverlay {
    fn new(resident_limit: usize, dirty_limit: usize) -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            owned: HashSet::new(),
            #[cfg(test)]
            owned_peak: 0,
            dirty_count: 0,
            clean_count: 0,
            dirty_limit,
            resident_limit,
            defer: true,
        }
    }

    /// Entry count. Test-only since the checkpoint guard asks whether the
    /// overlay is *untouched* (`is_empty`) rather than how many entries it holds.
    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the overlay is untouched (a checkpoint requires an *empty*
    /// overlay). `owned` counts: with a write-through configuration (`resident_limit = 0`) a
    /// transaction can own pages whose entries were all evicted, and `rollback_to` could not
    /// restore a state that ownership describes.
    fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.owned.is_empty()
    }

    #[cfg(test)]
    fn dirty_count(&self) -> usize {
        self.dirty_count
    }

    #[cfg(test)]
    fn clean_count(&self) -> usize {
        self.clean_count
    }

    #[cfg(test)]
    fn order_len(&self) -> usize {
        self.order.len()
    }

    fn may_defer(&self) -> bool {
        self.defer && self.dirty_count < self.dirty_limit
    }

    fn disable_defer(&mut self) {
        self.defer = false;
    }

    /// Peak `owned` size, i.e. the transaction's workset. Its only reader is the
    /// test-only accessor, so it is compiled only for tests.
    #[cfg(test)]
    fn owned_peak(&self) -> usize {
        self.owned_peak
    }

    fn get(&self, page_id: PageId) -> Option<Arc<Node>> {
        self.entries.get(&page_id).map(|entry| entry.node.clone())
    }

    fn owns(&self, page_id: PageId) -> bool {
        self.owned.contains(&page_id)
    }

    fn note_owned(&mut self, page_id: PageId) {
        self.owned.insert(page_id);
        #[cfg(test)]
        {
            self.owned_peak = self.owned_peak.max(self.owned.len());
        }
    }

    /// Records a deferred write: the entry becomes the page's only copy, so it is
    /// never a candidate for eviction.
    fn insert_dirty(&mut self, page_id: PageId, node: Arc<Node>) {
        self.note_owned(page_id);
        match self
            .entries
            .insert(
                page_id,
                OverlayEntry {
                    node,
                    tier: OverlayTier::Dirty,
                },
            )
            .map(|entry| entry.tier)
        {
            Some(OverlayTier::Dirty) => {}
            Some(OverlayTier::Clean) => {
                self.clean_count -= 1;
                self.dirty_count += 1;
            }
            None => {
                self.dirty_count += 1;
                // never evictable. `mark_all_dirty_clean` queues them when they become Clean.
            }
        }
        self.maybe_compact_order();
    }

    /// Records an already-written page and FIFO-evicts the oldest `Clean` entries down to
    /// `resident_limit`, oldest first. Dirty entries are skipped by
    /// construction — only Clean PIDs ever enter `order`.
    fn insert_clean(&mut self, page_id: PageId, node: Arc<Node>) {
        self.note_owned(page_id);
        match self
            .entries
            .insert(
                page_id,
                OverlayEntry {
                    node,
                    tier: OverlayTier::Clean,
                },
            )
            .map(|entry| entry.tier)
        {
            Some(OverlayTier::Clean) => {}
            Some(OverlayTier::Dirty) => {
                self.dirty_count -= 1;
                self.clean_count += 1;
                self.order.push_back(page_id);
            }
            None => {
                self.clean_count += 1;
                self.order.push_back(page_id);
            }
        }
        self.evict_excess_clean();
        self.maybe_compact_order();
    }

    /// Drops oldest Clean entries until `clean_count <= resident_limit`. Only Clean PIDs occupy
    /// `order`, so the loop always reaches the limit; the tier check is a defensive guard.
    /// `owned` is never touched: losing residency is a locality loss,
    /// never a change of ownership.
    fn evict_excess_clean(&mut self) {
        while self.clean_count > self.resident_limit {
            let Some(old) = self.order.pop_front() else {
                break;
            };
            if let Some(entry) = self.entries.get(&old)
                && entry.tier == OverlayTier::Clean
            {
                self.entries.remove(&old);
                self.clean_count -= 1;
            }
        }
    }

    /// Sweeps the queue slots that no longer name a resident `Clean` entry — a released PID leaves
    /// its slot behind, and the write-through path releases on every write, so without a sweep
    /// `order` would grow with the transaction's write count while `clean_count` stayed at the
    /// workset. A sweep is O(order) and the threshold is only reached after `clean_count` inserts,
    /// so it costs O(1) amortized per insert; the survivors keep their relative order, so FIFO
    /// eviction order is unchanged.
    ///
    /// Invariant after every overlay mutation: `order.len() <= 2 * clean_count + 1`.
    fn maybe_compact_order(&mut self) {
        if self.order.len() <= 2 * self.clean_count + 1 {
            return;
        }
        let entries = &self.entries;
        self.order.retain(|pid| {
            matches!(
                entries.get(pid).map(|entry| entry.tier),
                Some(OverlayTier::Clean)
            )
        });
    }

    // Reuse flush order so Clean FIFO residency remains deterministic.
    fn mark_all_dirty_clean(&mut self, pages: &[(PageId, Arc<Node>)]) {
        for &(pid, _) in pages {
            let entry = self
                .entries
                .get_mut(&pid)
                .expect("flushed page is resident");
            entry.tier = OverlayTier::Clean;
            self.dirty_count -= 1;
            self.clean_count += 1;
            self.order.push_back(pid);
        }
        self.evict_excess_clean();
        self.maybe_compact_order();
    }

    fn dirty_pages_sorted(&self) -> Vec<(PageId, Arc<Node>)> {
        let mut pages: Vec<(PageId, Arc<Node>)> = self
            .entries
            .iter()
            .filter(|(_, entry)| entry.tier == OverlayTier::Dirty)
            .map(|(pid, entry)| (*pid, entry.node.clone()))
            .collect();
        pages.sort_unstable_by_key(|(pid, _)| *pid);
        pages
    }

    fn drop_pages(&mut self, page_id: PageId, nr_pages: u32) {
        for offset in 0..nr_pages {
            let pid = page_id + offset;
            self.owned.remove(&pid);
            if let Some(entry) = self.entries.remove(&pid) {
                match entry.tier {
                    OverlayTier::Dirty => self.dirty_count -= 1,
                    OverlayTier::Clean => self.clean_count -= 1,
                }
            }
        }
        self.maybe_compact_order();
    }

    /// Drops every entry and ownership record. The budget is **not** part of what a rollback
    /// restores: `defer` is sticky for the overlay's whole life, so a transaction that already
    /// switched to write-through stays there (`take_dirty_slot`), and `owned_peak` is a high-water
    /// mark that later samples still have to see.
    fn clear(&mut self) {
        self.entries.clear();
        self.order.clear();
        self.owned.clear();
        self.dirty_count = 0;
        self.clean_count = 0;
    }

    /// Whether any entry is still `Dirty`; the commit fast path refuses to publish nothing while
    /// one exists.
    fn has_dirty(&self) -> bool {
        self.dirty_count > 0
    }

    #[cfg(test)]
    fn defer_enabled(&self) -> bool {
        self.defer
    }
}

struct TxnCore<'a> {
    btree: &'a BTree,
    read: TreeReadContext,
    catalog_root: RootRef,
    page_state: TxnPageState,
}

impl<'a> TxnCore<'a> {
    fn new(btree: &'a BTree) -> Self {
        let snapshot = btree.store.cached_snapshot();
        btree.pending_counts.clear();
        let overlay = Arc::new(RwLock::new(TxnOverlay::new(
            btree.resident_limit.load(Ordering::Relaxed),
            btree.dirty_limit.load(Ordering::Relaxed),
        )));
        Self {
            btree,
            read: btree.read.with_overlay(overlay),
            catalog_root: RootRef::decode(snapshot.catalog_root),
            page_state: TxnPageState::default(),
        }
    }

    fn checkpoint(&self) -> TxnCheckpoint {
        // `rollback_to` restores that state by clearing the whole overlay — so residency must be
        // empty here. `invariant` is a two-argument *function* (not a macro) and ends the process,
        // which is why the negative test must run in a subprocess.
        if !self.read.overlay_is_empty() {
            invariant(
                "CHECKPOINT_AT_TXN_START",
                "a checkpoint must be taken before any page enters the overlay",
            );
        }
        TxnCheckpoint {
            catalog_root: self.catalog_root,
            page_state: self.page_state.checkpoint(),
        }
    }

    fn rollback_to(&mut self, checkpoint: TxnCheckpoint) {
        self.catalog_root = checkpoint.catalog_root;
        // Everything in the overlay was written after the checkpoint, so it is
        // dropped wholesale before the checkpoint's sets come back.
        self.read.clear_overlay();
        self.page_state
            .rollback_to(&self.read, checkpoint.page_state);
        self.sync_pending_counts();
    }

    fn open_bucket_txn(&mut self, root: RootRef, layout: Layout) -> Txn<'_> {
        Txn {
            read: self.read.with_layout(layout),
            root,
            page_state: &mut self.page_state,
            pending_counts: self.btree.pending_counts.clone(),
        }
    }

    fn begin_savepoint(&mut self) {
        self.page_state.begin_savepoint();
    }

    fn commit_savepoint(&mut self) {
        self.page_state.commit_savepoint(&self.read);
        self.sync_pending_counts();
    }

    fn rollback_savepoint(&mut self) {
        self.page_state.rollback_savepoint(&self.read);
        self.sync_pending_counts();
    }

    fn catalog_root(&self) -> RootRef {
        self.catalog_root
    }

    fn catalog_bucket_root(&self, key: &[u8]) -> Option<(RootRef, u32)> {
        let (leaf, pos) = Tree::find(&self.read, self.catalog_root(), key)?;
        let metadata = physical_value(
            BucketMetadata::decode(leaf.value_at(pos)),
            "bucket metadata record within slot value",
        );
        Some((metadata.root(), metadata.flags()))
    }

    fn catalog_put(&mut self, key: &[u8], value: &[u8]) -> StoreResult<()> {
        let root = self.catalog_root();
        let new_root = self.page_state.run(&self.read, |ctx| {
            Tree::put(&self.read, ctx, root, key, value)
        })?;
        self.catalog_root = new_root;
        self.sync_pending_counts();
        Ok(())
    }

    fn sync_pending_counts(&self) {
        self.btree.pending_counts.update(&self.page_state);
    }

    /// Retires pages collected from a **published** tree (bucket deletion). They are disjoint from
    /// the overlay by construction — the transaction has written nothing and the collection only
    /// reads — which is why this release point does not go through `drop_overlay_pages` like every
    /// other one. The check makes that argument executable: a page this transaction owns is still
    /// the only copy of its bytes, so retiring it here would let the commit's flush write stale
    /// bytes onto a PID the same commit retires.
    fn retire_published_pages(&mut self, pages: Vec<(PageId, u32)>) {
        for &(page_id, nr_pages) in &pages {
            for offset in 0..nr_pages {
                if self.read.overlay_owns(page_id + offset) {
                    invariant(
                        "RELEASED_PAGE_OWNED_BY_TXN",
                        "a page of a published tree cannot belong to this transaction's overlay",
                    );
                }
            }
        }
        self.page_state.pending_free.extend(pages);
    }

    fn persist_nested_rollback_meta(&mut self, context: &'static str) {
        self.btree.persist_nested_rollback_meta(self, context);
    }
}

pub struct MultiTxn<'a> {
    core: TxnCore<'a>,
    bucket_roots: BTreeMap<String, MultiTxnBucketRoot>,
}

#[derive(Clone, Copy)]
struct MultiTxnBucketRoot {
    initial: RootRef,
    current: RootRef,
    flags: u32,
}

impl<'a> MultiTxn<'a> {
    fn bucket_root(&self, bucket: &str) -> Result<MultiTxnBucketRoot> {
        if let Some(root) = self.bucket_roots.get(bucket) {
            return Ok(*root);
        }

        let (current, flags) = self
            .core
            .catalog_bucket_root(bucket.as_bytes())
            .ok_or(Error::BucketNotFound)?;
        Ok(MultiTxnBucketRoot {
            initial: current,
            current,
            flags,
        })
    }

    /// Executes a transaction on one bucket within this multi-bucket transaction.
    ///
    /// The callback and this method use [`Result`] directly. Callers that need a
    /// domain-specific error can map the returned `Error` after this boundary.
    pub fn exec<F, R>(&mut self, bucket: &str, f: F) -> Result<R>
    where
        F: FnOnce(&mut Txn) -> Result<R>,
    {
        validate_bucket_input(bucket)?;

        let root = self.bucket_root(bucket)?;

        self.core.begin_savepoint();
        let mut txn = self
            .core
            .open_bucket_txn(root.current, layout_from_flags(root.flags));

        let res = f(&mut txn);
        match res {
            Ok(value) => {
                let current = txn.root;
                drop(txn);
                if let Some(bucket_root) = self.bucket_roots.get_mut(bucket) {
                    bucket_root.current = current;
                } else {
                    self.bucket_roots.insert(
                        bucket.to_owned(),
                        MultiTxnBucketRoot {
                            initial: root.initial,
                            current,
                            flags: root.flags,
                        },
                    );
                }
                self.core.commit_savepoint();
                Ok(value)
            }
            Err(error) => {
                drop(txn);
                self.core.rollback_savepoint();
                self.core.persist_nested_rollback_meta(
                    "nested multi-transaction rollback meta publication",
                );
                Err(error)
            }
        }
    }
}

#[repr(C)]
pub(crate) struct BucketMetadata {
    root_page_id: PageId,
    flags: u32,
}

impl BucketMetadata {
    fn new(root: RootRef, flags: u32) -> Self {
        Self {
            root_page_id: root.get(),
            flags,
        }
    }

    pub(crate) fn decode(x: &[u8]) -> StoreResult<Self> {
        if x.len() < std::mem::size_of::<Self>() {
            return Err(StoreFault::Corruption);
        }
        Ok(unsafe { std::ptr::read_unaligned(x.as_ptr().cast::<Self>()) })
    }

    fn root(&self) -> RootRef {
        RootRef::decode(self.root_page_id)
    }

    fn flags(&self) -> u32 {
        self.flags
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                (self as *const Self).cast::<u8>(),
                std::mem::size_of::<Self>(),
            )
        }
    }
}

pub struct BTree {
    pub(crate) store: Arc<Store>,
    runtime: Arc<BTreeRuntime>,
    pub(crate) writer_lock: Arc<Mutex<()>>,
    read: TreeReadContext,
    pending_counts: Arc<PendingPageCounts>,
    /// Residency budget (pages) handed to every write transaction this handle creates
    /// (the budget entry point). Shared across clones; never touches the disk format.
    resident_limit: Arc<AtomicUsize>,
    dirty_limit: Arc<AtomicUsize>,
    pub(crate) start_seq: Arc<AtomicU64>,
    local_snapshot: Arc<RwLock<MetaSnapshot>>,
    options: OpenOptions,
    instance_anchor: Option<Arc<BTree>>,
}

impl BTree {
    fn apply_local_snapshot(&self, snapshot: MetaSnapshot) {
        self.apply_handle_snapshot(snapshot);
    }

    fn apply_handle_snapshot(&self, snapshot: MetaSnapshot) {
        let mut local = self.local_snapshot.write();
        if snapshot.seq < local.seq {
            return;
        }
        self.start_seq.store(snapshot.seq, Ordering::Release);
        *local = snapshot;
    }

    fn sync_local_snapshot_from_store(&self) {
        // Opening an already-live path updates only this handle's snapshot, so it can run
        // from inside a view callback while a writer holds the write lock and owns a
        // separate writer-local transaction core.
        self.apply_handle_snapshot(self.store.cached_snapshot());
    }

    /// Open or create a btree database at the given path using default runtime options.
    ///
    /// This is equivalent to `BTree::open_with_options(path, OpenOptions::default)`.
    pub fn open<P: AsRef<Path>>(path: P) -> OpenResult<Self> {
        Self::open_with_options(path, OpenOptions::default())
    }

    /// Opens an **existing** database read-only.
    ///
    /// Equivalent to [`BTree::open_with_options`] with [`OpenOptions::read_only`]
    /// set: the file is not created, nothing is ever written to it, and it is
    /// locked shared, so several read-only handles can coexist (a read-write open
    /// of the same path succeeds once they are dropped). The handle serves the
    /// published generation it opened, rejects every mutating call with
    /// [`Error::ReadOnly`], and rejects [`BTree::take_snapshot`] with
    /// [`OpenError::ReadOnly`].
    ///
    /// # Failure
    ///
    /// - Path missing → `Io(NotFound)`; the path is not created.
    /// - File present but empty → `Corruption(NO_VALID_META)`; the file is left
    ///   at zero bytes instead of being initialised into a database.
    /// - Another process holds the exclusive lock → `DatabaseBusy`.
    /// - A live read-write instance of this path exists in this process → refused
    ///   with `InvalidOptions(LiveInstanceOptionsMismatch)`; a read-write and a
    ///   read-only handle are never the same instance.
    /// - Both metadata slots invalid → `Corruption(NO_VALID_META)`; a torn newest
    ///   slot serves the older generation.
    /// - Allocator or page corruption that the open-time validation reaches →
    ///   `Corruption(report)`, whose `check` names the validation that failed; the
    ///   process stays alive. (Corruption that only a page *read* can reach is the
    ///   next case.)
    /// - A file that cannot be read at all (permissions, read-only medium) →
    ///   `Io(PermissionDenied)`; a file that can be read opens as usual.
    /// - A mutating call on the returned handle, e.g. `exec` or `commit` →
    ///   `Error::ReadOnly`; nothing is written and nothing panics.
    /// - Corruption or a page-IO failure found *while reading* → the engine
    ///   prints its `fatal` diagnostic and aborts the process. A fault the store
    ///   can attribute to a page carries `generation`/`pid`; a node-level slot
    ///   fault prints `page_kind=node` with `generation=none`/`pid=none`, because
    ///   the node decoders do not know which page they were handed. Either way it
    ///   is deliberately not a returnable error and never a panic, so a caller
    ///   that must survive a damaged database has to isolate the read in a
    ///   subprocess.
    pub fn open_read_only<P: AsRef<Path>>(path: P) -> OpenResult<BTree> {
        Self::open_with_options(
            path,
            OpenOptions {
                read_only: true,
                ..Default::default()
            },
        )
    }

    /// Rejects mutating calls on a read-only handle.
    #[inline]
    fn require_writable(&self) -> Result<()> {
        if self.options.read_only {
            return Err(Error::ReadOnly);
        }
        Ok(())
    }

    /// Open or create a btree database at the given path using explicit runtime
    /// options.
    ///
    /// Within a single process, opening the same path again reuses the live
    /// instance. Reopens must use identical runtime options.
    pub fn open_with_options<P: AsRef<Path>>(path: P, options: OpenOptions) -> OpenResult<Self> {
        let path = path.as_ref();
        let key = normalize_db_path(path);
        let gate = {
            let mut reg = btree_instance_registry().lock();
            sweep_dead_btree_instances(&mut reg);
            if let Some(entry) = reg.get(&key)
                && let Some(existing) = entry.instance.upgrade()
            {
                if existing.options != options {
                    return Err(OpenError::InvalidOptions(
                        OptionsError::LiveInstanceOptionsMismatch,
                    ));
                }
                let mut handle = existing.as_ref().clone();
                handle.instance_anchor = Some(existing);
                handle.sync_local_snapshot_from_store();
                return Ok(handle);
            }

            reg.entry(key.clone())
                .or_insert_with(|| RegistryEntry {
                    instance: Weak::new(),
                    gate: Arc::new(Mutex::new(())),
                })
                .gate
                .clone()
        };

        let _gate = gate.lock();
        {
            let mut reg = btree_instance_registry().lock();
            sweep_dead_btree_instances(&mut reg);
            if let Some(entry) = reg.get(&key)
                && let Some(existing) = entry.instance.upgrade()
            {
                if existing.options != options {
                    return Err(OpenError::InvalidOptions(
                        OptionsError::LiveInstanceOptionsMismatch,
                    ));
                }
                let mut handle = existing.as_ref().clone();
                handle.instance_anchor = Some(existing);
                handle.sync_local_snapshot_from_store();
                return Ok(handle);
            }
        }

        let store = Arc::new(Store::open(path, &options)?);
        let initial_snapshot = store.cached_snapshot();
        let initial_seq = initial_snapshot.seq;
        let runtime = BTreeRuntime::new(store.clone(), options.cache_capacity);
        let read = TreeReadContext::new(runtime.clone());

        let instance = Self {
            store: store.clone(),
            runtime,
            writer_lock: Arc::new(Mutex::new(())),
            read,
            pending_counts: Arc::new(PendingPageCounts {
                snapshot: AtomicU64::new(0),
            }),
            resident_limit: Arc::new(AtomicUsize::new(DEFAULT_RESIDENT_LIMIT)),
            dirty_limit: Arc::new(AtomicUsize::new(DEFAULT_DIRTY_LIMIT)),
            start_seq: Arc::new(AtomicU64::new(initial_seq)),
            local_snapshot: Arc::new(RwLock::new(initial_snapshot)),
            options: options.clone(),
            instance_anchor: None,
        };
        let instance_arc = Arc::new(instance);
        {
            let mut reg = btree_instance_registry().lock();
            reg.insert(
                key,
                RegistryEntry {
                    instance: Arc::downgrade(&instance_arc),
                    gate: gate.clone(),
                },
            );
        }

        let mut handle = instance_arc.as_ref().clone();
        handle.instance_anchor = Some(instance_arc);
        Ok(handle)
    }

    /// Executes a read-write transaction on the specified bucket.
    ///
    /// The bucket must already exist, created with [`BTree::new_bucket`]; otherwise
    /// [`Error::BucketNotFound`] is returned. The transaction is committed if the closure
    /// returns `Ok`. Live storage faults and engine invariant violations terminate through
    /// the fatal boundary instead of returning an error. If the closure returns `Err`, the
    /// transaction is rolled back (allocated pages are reclaimed). If the failed attempt
    /// modified metadata, that changed metadata is published before the closure error is
    /// returned.
    ///
    /// The callback and this method use [`Result`] directly. Callers that need a
    /// domain-specific error can map the returned `Error` after this boundary.
    ///
    /// # Warning
    /// Nested calls on the same `BTree` instance are NOT supported. Writer
    /// methods (`exec`, `exec_multi`, `commit`, `new_bucket`, `del_bucket`)
    /// called from inside another writer closure deadlock on the writer
    /// mutex. A `view` called from inside a writer closure does not deadlock
    /// but observes the last published generation — the enclosing
    /// transaction's uncommitted writes are not visible.
    pub fn exec<F, R>(&self, bucket: &str, f: F) -> Result<R>
    where
        F: FnOnce(&mut Txn) -> Result<R>,
    {
        self.require_writable()?;
        validate_bucket_input(bucket)?;

        let _lock = self.writer_lock.lock();

        // Refresh to the latest published shared generation before starting a
        // new transaction when this handle's snapshot is stale.
        physical_value(self.refresh_internal(true), "BTree::exec refresh");
        let mut core = TxnCore::new(self);
        let origin = core.checkpoint();

        let name_bytes = bucket.as_bytes();
        let (initial_root, bucket_flags) = core
            .catalog_bucket_root(name_bytes)
            .ok_or(Error::BucketNotFound)?;

        let mut txn = core.open_bucket_txn(initial_root, layout_from_flags(bucket_flags));

        match f(&mut txn) {
            Ok(res) => {
                let new_root = txn.root;
                drop(txn);
                if initial_root != new_root {
                    let metadata = BucketMetadata::new(new_root, bucket_flags);
                    let metadata = metadata.as_slice();
                    physical_value(
                        core.catalog_put(name_bytes, metadata),
                        "catalog bucket update",
                    );
                }
                self.commit_txn_core(&mut core);
                Ok(res)
            }
            Err(e) => {
                drop(txn);
                core.rollback_to(origin);
                self.persist_rollback_meta(&mut core, "transaction rollback meta publication");
                Err(e)
            }
        }
    }

    /// Creates a bucket with a persistent prefix-encoding option.
    ///
    /// The bucket becomes a catalog record immediately and durably. A later
    /// [`BTree::exec`]/[`BTree::view`] on the same name reads the stored
    /// `enable_prefix_encoding` flag to select the node encoding. Returns
    /// [`Error::BucketExists`] when the name is already present in the catalog.
    pub fn new_bucket(&self, name: &str, enable_prefix_encoding: bool) -> Result<()> {
        self.require_writable()?;
        validate_bucket_input(name)?;

        let _lock = self.writer_lock.lock();

        physical_value(self.refresh_internal(true), "BTree::new_bucket refresh");
        let mut core = TxnCore::new(self);

        let name_bytes = name.as_bytes();
        if core.catalog_bucket_root(name_bytes).is_some() {
            return Err(Error::BucketExists);
        }

        let flags = u32::from(enable_prefix_encoding);
        let metadata = BucketMetadata::new(RootRef::Empty, flags);
        physical_value(
            core.catalog_put(name_bytes, metadata.as_slice()),
            "new_bucket catalog put",
        );
        self.commit_txn_core(&mut core);
        Ok(())
    }

    /// Executes multiple operations across different buckets in a single atomic transaction.
    ///
    /// This is more efficient than calling `exec` multiple times because on success it performs
    /// one generation publication and its associated synchronization at the end.
    /// On failure it restores the working projection and publishes any changed MetaNode
    /// high-water state before returning the closure error.
    ///
    /// The callback and this method use [`Result`] directly. Callers that need a
    /// domain-specific error can map the returned `Error` after this boundary.
    ///
    /// # Warning
    /// Nested calls on the same `BTree` instance are NOT supported. Writer
    /// methods (`exec`, `exec_multi`, `commit`, `new_bucket`, `del_bucket`)
    /// called from inside this closure deadlock on the writer mutex. A `view`
    /// called from inside this closure does not deadlock but observes the last
    /// published generation — this transaction's uncommitted writes are not
    /// visible.
    pub fn exec_multi<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mut MultiTxn) -> Result<R>,
    {
        self.require_writable()?;
        let _lock = self.writer_lock.lock();

        physical_value(self.refresh_internal(true), "BTree::exec_multi refresh");
        let core = TxnCore::new(self);
        let origin = core.checkpoint();

        let mut multi_txn = MultiTxn {
            core,
            bucket_roots: BTreeMap::new(),
        };

        match f(&mut multi_txn) {
            Ok(res) => {
                for (name, roots) in multi_txn.bucket_roots {
                    if roots.current == roots.initial {
                        continue;
                    }
                    let metadata = BucketMetadata::new(roots.current, roots.flags);
                    physical_value(
                        multi_txn
                            .core
                            .catalog_put(name.as_bytes(), metadata.as_slice()),
                        "multi catalog bucket update",
                    );
                }
                self.commit_txn_core(&mut multi_txn.core);
                Ok(res)
            }
            Err(e) => {
                multi_txn.core.rollback_to(origin);
                self.persist_rollback_meta(
                    &mut multi_txn.core,
                    "multi-transaction rollback meta publication",
                );
                Err(e)
            }
        }
    }

    fn persist_rollback_meta(&self, core: &mut TxnCore<'_>, context: &'static str) {
        // Page allocation is monotonic: a failed transaction rolls the roots and its
        // pending ownership back, but the MetaNode state it already consumed still has
        // to be published, or the next reader would re-allocate those page ids.
        physical_value(self.commit_internal(core), context);
    }

    fn persist_nested_rollback_meta(&self, core: &mut TxnCore<'_>, context: &'static str) {
        let snapshot = self.store.cached_snapshot();
        let published_snapshot = *self.local_snapshot.read();
        if snapshot == published_snapshot {
            return;
        }

        // This path publishes the **live** high-water mark without going through
        // `commit_internal`, so it must flush the retained Dirty items and cover the id space
        // released-but-never-written PID outside the file.
        core.read.flush_overlay();
        core.read.cover_id_space();

        physical_value(
            self.runtime
                .commit_generation_only(snapshot.catalog_root, &core.page_state.pending_alloc),
            context,
        );
        self.sync_local_snapshot_from_store();
    }

    fn commit_txn_core(&self, core: &mut TxnCore<'_>) {
        physical_value(self.commit_internal(core), "transaction core commit");
    }

    /// Executes a read-only transaction on the specified bucket.
    ///
    /// # Warning
    /// Nested calls on the same `BTree` instance are NOT supported. A `view`
    /// called from inside an `exec`/`exec_multi` closure observes the last
    /// published generation — the enclosing transaction's uncommitted writes
    /// are not visible. Writer methods (`exec`, `exec_multi`, `commit`,
    /// `new_bucket`, `del_bucket`) called from inside a `view` closure are
    /// outside the contract.
    ///
    /// # Resource note
    /// The view pins its snapshot for the whole closure: pages it references
    /// are not reused until the view ends. Keep views short-lived — a long or
    /// permanent view delays page reclamation and grows the database file
    /// (writes are never blocked, but space is retained).
    pub fn view<F, R>(&self, bucket: &str, f: F) -> Result<R>
    where
        F: FnOnce(&ReadOnlyTxn) -> Result<R>,
    {
        validate_bucket_input(bucket)?;

        let _guard = self.store.epoch.pin();

        // Refresh only when the shared published sequence is newer than this
        // handle's snapshot, then keep the selected root fixed for the view.
        let (latest_seq, mut latest_root) = self.store.shared_snapshot();
        let seq_changed = latest_seq != self.start_seq.load(Ordering::Acquire);
        if seq_changed {
            // A reader passes `false`: the refresh reports the current *shared*
            // generation and never installs a disk-newer one into the shared
            // snapshot, because that would advance shared state mid-exec and trip
            // the writer's sequence-conflict check. Only a writer, holding the
            // writer lock, may install one.
            let snapshot = physical_value(self.store.refresh_sb(false), "BTree::view refresh");
            latest_root = snapshot.catalog_root;
            self.runtime.clear_cache();
            self.apply_handle_snapshot(snapshot);
        }

        let name_bytes = bucket.as_bytes();
        let read = &self.read;
        let (catalog_leaf, catalog_pos) =
            Tree::find(read, RootRef::decode(latest_root), name_bytes)
                .ok_or(Error::BucketNotFound)?;
        let metadata = physical_value(
            BucketMetadata::decode(catalog_leaf.value_at(catalog_pos)),
            "bucket metadata record within catalog value",
        );
        let bucket_root = metadata.root();
        let bucket_layout = layout_from_flags(metadata.flags());
        let read = read.with_layout(bucket_layout);

        let tree = ReadOnlyTree::new(&read, bucket_root);
        let txn = ReadOnlyTxn { tree, _guard };
        f(&txn)
    }

    /// Delete a bucket by name and persist the change.
    pub fn del_bucket<N>(&self, name: N) -> Result<()>
    where
        N: AsRef<str>,
    {
        let name = name.as_ref();
        self.require_writable()?;
        validate_bucket_input(name)?;

        let _lock = self.writer_lock.lock();

        physical_value(self.refresh_internal(true), "BTree::del_bucket refresh");

        let name_bytes = name.as_bytes();
        let mut core = TxnCore::new(self);
        let read = core.read.clone();
        let (catalog_leaf, catalog_pos) =
            Tree::find(&read, core.catalog_root(), name_bytes).ok_or(Error::BucketNotFound)?;
        let bucket_root = physical_value(
            BucketMetadata::decode(catalog_leaf.value_at(catalog_pos)),
            "bucket metadata record within catalog value",
        )
        .root();

        let mut pages_to_free = Vec::new();
        let mut node_pages = Vec::new();
        if bucket_root.node().is_some() {
            Tree::collect_tree_pages_uncached(
                &read,
                bucket_root,
                &mut pages_to_free,
                &mut node_pages,
            );
            for page_id in &node_pages {
                self.runtime.invalidate_node(*page_id);
            }
        }

        let catalog_root = core.catalog_root();
        let (deleted, new_catalog_root) = physical_value(
            core.page_state
                .run(&read, |ctx| Tree::del(&read, ctx, catalog_root, name_bytes)),
            "catalog bucket delete",
        );
        if !deleted {
            invariant(
                "DELETE_BUCKET_CATALOG_MISSING",
                "catalog root disappeared during bucket deletion",
            );
        }
        core.catalog_root = new_catalog_root;
        core.retire_published_pages(pages_to_free);
        core.sync_pending_counts();
        physical_value(self.commit_internal(&mut core), "BTree::del_bucket commit");
        Ok(())
    }

    fn commit_internal(&self, core: &mut TxnCore<'_>) -> StoreResult<()> {
        let start_seq = self.start_seq.load(Ordering::Acquire);
        let (latest_seq, _) = self.store.shared_snapshot();
        if latest_seq != start_seq {
            invariant(
                "COMMIT_SEQUENCE_CONFLICT",
                "writer lock must serialize every compatible handle commit",
            );
        }
        let snapshot = self.store.cached_snapshot();
        let published_snapshot = *self.local_snapshot.read();
        // A transaction can extend the physical address space before its roots are
        // published. That MetaNode change is not represented by pending tree roots,
        // so it must also force a generation.
        let meta_changed = snapshot != published_snapshot;

        let catalog_root = core.catalog_root.get();
        let page_state = &core.page_state;

        if page_state.pending_free.is_empty()
            && page_state.pending_alloc.is_empty()
            && snapshot.catalog_root == catalog_root
            && !meta_changed
        {
            if core.read.overlay_has_dirty() {
                invariant(
                    "COMMIT_FAST_PATH_WITH_DEFERRED_PAGES",
                    "a deferred page must reach the disk before a commit may publish nothing",
                );
            }
            return Ok(());
        }

        core.read.flush_overlay();
        core.read.cover_id_space();

        self.runtime.commit_roots_with_pending_alloc(
            catalog_root,
            &page_state.pending_free,
            &page_state.pending_alloc,
        )?;

        self.pending_counts.clear();
        self.apply_local_snapshot(self.store.cached_snapshot());
        Ok(())
    }

    /// Flushes any pending internal metadata changes held by this handle.
    ///
    /// This is a low-level API. Normal write operations should use [`BTree::exec`],
    /// [`BTree::exec_multi`], or [`BTree::del_bucket`], which already commit on
    /// success.
    ///
    /// If there are no pending page allocations/frees and the current catalog
    /// root already matches the cached snapshot, this is a no-op and
    /// returns `Ok()`.
    ///
    /// Unlike [`BTree::exec`] and [`BTree::exec_multi`], this method does not
    /// refresh the handle to the latest on-disk state before attempting the
    /// commit. A sequence mismatch is an engine invariant violation because all
    /// compatible handles share the same writer lock.
    ///
    /// # Warning
    /// Must not be called from inside an [`BTree::exec`] or
    /// [`BTree::exec_multi`] closure on the same instance: the writer mutex is
    /// not reentrant and the call deadlocks.
    pub fn commit(&self) -> Result<()> {
        self.require_writable()?;
        let _lock = self.writer_lock.lock();
        let mut core = TxnCore::new(self);
        physical_value(self.commit_internal(&mut core), "BTree::commit");
        Ok(())
    }

    fn refresh_internal(&self, allow_install: bool) -> StoreResult<()> {
        // fast path: snapshot version unchanged, so current in-memory roots and node cache are valid
        let (latest_seq, _) = self.store.shared_snapshot();
        let start_seq = self.start_seq.load(Ordering::Acquire);
        if latest_seq == start_seq {
            return Ok(());
        }

        self.runtime.clear_cache();

        let snapshot = self.store.refresh_sb(allow_install)?;
        self.apply_local_snapshot(snapshot);
        Ok(())
    }

    /// Returns all bucket names.
    pub fn buckets(&self) -> Result<Vec<String>> {
        Ok(physical_value(self.buckets_internal(), "BTree::buckets"))
    }

    /// Bucket names with their persisted prefix-encoding policy, in catalog order.
    ///
    /// This is the migration's read-back surface: it must not skip an entry, so a
    /// name that is not UTF-8 or a metadata record with an unexpected length is an
    /// engine invariant violation rather than something to filter out (the engine
    /// only ever writes `&str` names and fixed-width metadata records).
    pub fn buckets_with_policy(&self) -> Result<Vec<(String, bool)>> {
        Ok(physical_value(
            self.buckets_with_policy_internal(),
            "BTree::buckets_with_policy",
        ))
    }

    fn buckets_with_policy_internal(&self) -> StoreResult<Vec<(String, bool)>> {
        let _guard = self.store.epoch.pin();
        self.refresh_internal(false)?;
        let snapshot = *self.local_snapshot.read();
        let read = TreeReadContext::new(self.runtime.clone());

        let mut iter = Tree::iterator(&read, RootRef::decode(snapshot.catalog_root));
        let mut key_buf = Vec::new();
        let mut val_buf = Vec::new();
        let mut res = Vec::new();
        while iter.next_ref(&mut key_buf, &mut val_buf) {
            let Ok(name) = std::str::from_utf8(&key_buf) else {
                invariant(
                    "BUCKET_NAME_NOT_UTF8",
                    "a catalog bucket name must be valid UTF-8",
                );
            };
            if val_buf.len() != std::mem::size_of::<BucketMetadata>() {
                invariant(
                    "BUCKET_METADATA_LENGTH",
                    "bucket metadata is a fixed-width record",
                );
            }
            let flags = physical_value(
                BucketMetadata::decode(&val_buf),
                "bucket metadata record within catalog value",
            )
            .flags();
            res.push((name.to_string(), layout_from_flags(flags) == Layout::Prefix));
        }
        Ok(res)
    }

    fn buckets_internal(&self) -> StoreResult<Vec<String>> {
        let _guard = self.store.epoch.pin();

        // Same-process handles share the published sequence, so avoid rereading
        // both superblock pages when the local snapshot is already current. Passing
        // `false` reports the current shared generation without installing a
        // disk-newer one into the shared snapshot; only a writer, which holds the
        // writer lock, may do that.
        self.refresh_internal(false)?;
        let snapshot = *self.local_snapshot.read();
        let read = TreeReadContext::new(self.runtime.clone());

        let mut iter = Tree::iterator(&read, RootRef::decode(snapshot.catalog_root));
        let mut key_buf = Vec::new();
        let mut val_buf = Vec::new();
        let mut res = Vec::new();
        while iter.next_ref(&mut key_buf, &mut val_buf) {
            if let Ok(s) = std::str::from_utf8(&key_buf) {
                res.push(s.to_string());
            }
        }
        Ok(res)
    }

    /// Writes a snapshot of this store to `dst`.
    ///
    /// The snapshot is a standalone database file equal to the generation named
    /// by the returned [`Snapshot::seq`]: every byte is read while that
    /// generation is pinned, so commits running during the copy are neither
    /// blocked nor included, and later generations never appear in it.
    /// [`BTree::open`] accepts the result, and it can be written to afterwards.
    /// Late writes can still be present as bytes in pages the frozen generation
    /// classifies as free; no traversal can reach them, and a page is always
    /// rewritten before it is referenced again.
    ///
    /// `dst` is used exactly as given: it is created when missing and truncated
    /// when present, and its contents are replaced. The bytes are written
    /// directly to `dst` — there is no staging file and no rename — so a failed
    /// call can leave a partial file behind.
    ///
    /// This store's own file is refused: if `dst` resolves to the same file as
    /// this store, the call returns [`OpenError::Io`] and the database is left
    /// untouched. The refusal matches files that share an inode where the
    /// platform exposes file identity (unix) and otherwise compares resolved
    /// paths, so a hard link to this store's own file is only recognised on
    /// unix.
    ///
    /// Any other destination is the caller's responsibility: the destination
    /// must not be read, opened, or written by anyone else while the call runs,
    /// and that includes another open [`BTree`] on the same path, which this
    /// call would silently overwrite.
    ///
    /// Calling this from inside a transaction closure of this instance snapshots
    /// the last published generation; the closure's uncommitted changes are not
    /// part of it.
    pub fn take_snapshot<P: AsRef<Path>>(&self, dst: P) -> OpenResult<Snapshot> {
        if self.options.read_only {
            return Err(OpenError::ReadOnly);
        }
        self.store.take_snapshot(dst.as_ref())
    }

    #[doc(hidden)]
    pub fn current_seq(&self) -> u64 {
        self.store.get_seq()
    }

    /// Returns whether `path` still names the file held by this handle.
    ///
    /// A path can be replaced with `rename` without changing the inode locked by
    /// this handle, so callers that audit a path and then read through this handle
    /// must check the identity at their phase boundaries. Platforms without a
    /// file identity API return an error rather than accepting an unproven match.
    #[doc(hidden)]
    pub fn path_is_same_file<P: AsRef<Path>>(&self, path: P) -> io::Result<bool> {
        self.store.path_is_same_file(path.as_ref())
    }

    #[doc(hidden)]
    pub fn pending_pages(&self) -> (usize, usize) {
        self.pending_counts.snapshot()
    }

    /// Sets the residency budget (in pages) used by write transactions this handle creates
    /// afterwards; `0` keeps ownership but no residency (the budget entry point). Monitoring
    /// and testing only: the default stays an internal constant.
    #[cfg(test)]
    fn set_resident_limit(&self, limit: usize) {
        self.resident_limit.store(limit, Ordering::Relaxed);
    }

    /// Sets the dirty budget (in pages) used by write transactions this handle creates afterwards;
    /// `0` writes through immediately (the budget is exhausted). Monitoring and testing only.
    #[cfg(test)]
    fn set_dirty_limit(&self, limit: usize) {
        self.dirty_limit.store(limit, Ordering::Relaxed);
    }

    /// Puts one page into a write transaction's overlay and takes a checkpoint, which must abort
    /// the process (the subprocess test asserts the fatal path, not the `unreachable!` below).
    #[cfg(test)]
    pub(crate) fn checkpoint_with_a_resident_page(&self) {
        let _lock = self.writer_lock.lock();
        physical_value(
            self.refresh_internal(true),
            "checkpoint precondition refresh",
        );
        let core = TxnCore::new(self);
        if let Some(overlay) = &core.read.overlay {
            overlay.write().insert_dirty(7, Arc::new(Node::new_leaf()));
        }
        let _ = core.checkpoint();
        unreachable!("the checkpoint precondition must abort before this point");
    }

    /// The other forbidden state: pages owned while every entry was evicted (reachable with
    /// `resident_limit = 0`). A checkpoint here must abort too.
    #[cfg(test)]
    pub(crate) fn checkpoint_with_owned_pages_only(&self) {
        let _lock = self.writer_lock.lock();
        physical_value(
            self.refresh_internal(true),
            "checkpoint precondition refresh",
        );
        let core = TxnCore::new(self);
        if let Some(overlay) = &core.read.overlay {
            overlay.write().note_owned(7);
        }
        let _ = core.checkpoint();
        unreachable!("the checkpoint precondition must abort before this point");
    }
}

impl Clone for BTree {
    /// Cloning a BTree handle shares the store, writer lock, and pending page tracking.
    fn clone(&self) -> Self {
        let snapshot = { *self.local_snapshot.read() };

        Self {
            store: self.store.clone(),
            runtime: self.runtime.clone(),
            writer_lock: self.writer_lock.clone(),
            read: self.read.clone(),
            pending_counts: self.pending_counts.clone(),
            resident_limit: self.resident_limit.clone(),
            dirty_limit: self.dirty_limit.clone(),
            start_seq: Arc::new(AtomicU64::new(snapshot.seq)),
            local_snapshot: Arc::new(RwLock::new(snapshot)),
            options: self.options.clone(),
            instance_anchor: self.instance_anchor.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::Node;
    use crate::node::PAGE_SIZE;
    use crate::store::BEFORE_META_SLOT_WRITE;
    use crate::store::PublicationPoint;
    use crate::test_support::child_test_command;

    const DEFERRED_FAULT_CHILD_PATH: &str = "BTREE_DEFERRED_FAULT_CHILD_PATH";
    use crate::store::NodeIoKind;
    use rand::{Rng, SeedableRng, rngs::StdRng};
    use std::collections::BTreeMap;

    fn test_store(dir: &tempfile::TempDir) -> Arc<Store> {
        Arc::new(Store::open(dir.path().join("tree.db"), &OpenOptions::default()).unwrap())
    }

    fn test_runtime(store: Arc<Store>) -> Arc<BTreeRuntime> {
        BTreeRuntime::new(store, OpenOptions::default().cache_capacity)
    }

    /// Eviction is FIFO and drops the oldest entry only. Losing residency must
    /// never lose ownership — otherwise a later read would be routed to the shared
    /// cache as if the page were published.
    #[test]
    fn overlay_eviction_is_fifo_and_preserves_ownership() {
        let mut overlay = TxnOverlay::new(2, DEFAULT_DIRTY_LIMIT);
        let first = Arc::new(Node::new_leaf());
        let second = Arc::new(Node::new_leaf());
        let third = Arc::new(Node::new_leaf());
        overlay.insert_clean(10, first);
        overlay.insert_clean(11, second.clone());
        overlay.insert_clean(12, third.clone());

        assert_eq!(overlay.len(), 2, "the budget is honoured");
        assert!(
            overlay.get(10).is_none(),
            "the oldest entry is the one dropped"
        );
        assert!(overlay.owns(10), "eviction must not delete ownership");
        assert!(Arc::ptr_eq(&overlay.get(11).unwrap(), &second));
        assert!(Arc::ptr_eq(&overlay.get(12).unwrap(), &third));
    }

    #[test]
    fn overlay_rewrite_refreshes_and_reinserts_after_eviction() {
        let mut overlay = TxnOverlay::new(1, DEFAULT_DIRTY_LIMIT);
        let rewritten = Arc::new(Node::new_leaf());
        overlay.insert_clean(5, Arc::new(Node::new_leaf()));
        overlay.insert_clean(5, rewritten.clone());
        assert_eq!(overlay.len(), 1);
        assert!(Arc::ptr_eq(&overlay.get(5).unwrap(), &rewritten));

        overlay.insert_clean(6, Arc::new(Node::new_leaf()));
        assert!(overlay.get(5).is_none() && overlay.get(6).is_some());

        overlay.insert_clean(5, rewritten.clone());
        assert!(overlay.owns(5) && Arc::ptr_eq(&overlay.get(5).unwrap(), &rewritten));
    }

    /// `resident_limit = 0` keeps ownership without residency: reads fall back to
    /// `read_node_without_cache` + re-insert (the zero-budget case).
    #[test]
    fn overlay_zero_budget_keeps_ownership_without_residency() {
        let mut overlay = TxnOverlay::new(0, DEFAULT_DIRTY_LIMIT);
        overlay.insert_clean(7, Arc::new(Node::new_leaf()));
        assert_eq!(overlay.len(), 0);
        assert!(overlay.owns(7));
    }

    #[test]
    fn overlay_queue_stays_tied_to_clean_entries_not_to_writes() {
        let mut overlay = TxnOverlay::new(DEFAULT_RESIDENT_LIMIT, 0);
        let node = || Arc::new(Node::new_leaf());
        for _ in 0..1_000 {
            // released — the freed PID goes straight back to the reusable set.
            overlay.insert_clean(7, node());
            overlay.drop_pages(7, 1);
        }
        assert_eq!(overlay.clean_count(), 0, "every release drops its entry");
        assert!(
            overlay.order_len() <= 2 * overlay.clean_count() + 1,
            "{} queue slots after 1000 writes; the queue must stay tied to the resident entries",
            overlay.order_len()
        );
    }

    /// Record the workset (= peak of `owned`) first, then drive the
    /// budgets from it. A write transaction must answer its own reads at every budget —
    /// including budgets that evict immediately — and **no page it wrote may enter the shared
    /// cache** (transaction-private pages never enter the shared cache). Ownership survives eviction, so an evicted page still takes the
    /// overlay-miss path instead of `load_node_miss`.
    #[test]
    fn transaction_owns_its_writes_across_residency_budgets() {
        let dir = tempfile::TempDir::new().unwrap();
        let tree = BTree::open(dir.path().join("overlay-budget.db")).unwrap();
        tree.new_bucket("warm", false).unwrap();
        tree.set_resident_limit(DEFAULT_RESIDENT_LIMIT);
        tree.set_dirty_limit(0);

        let workset = tree
            .exec("warm", |txn| {
                txn.put(b"k", b"v")?;
                txn.put(b"shared", b"shared-value")?;
                Ok(txn.read.overlay_owned_peak())
            })
            .unwrap();
        assert!(
            workset > 0,
            "the workset must be recorded before a budget is chosen"
        );

        fn collect(txn: &Txn<'_>) -> Vec<(Vec<u8>, Vec<u8>)> {
            let mut it = txn.iter();
            let (mut key, mut value) = (Vec::new(), Vec::new());
            let mut out = Vec::new();
            while it.next_ref(&mut key, &mut value) {
                out.push((key.clone(), value.clone()));
            }
            out
        }

        for limit in [0usize, 1, workset * 2] {
            let bucket = format!("b{limit}");
            tree.set_resident_limit(limit);
            tree.new_bucket(&bucket, false).unwrap();
            let writes_before = tree.store.node_io.writes().len();
            let key = format!("k{limit}").into_bytes();
            let value = format!("v{limit}").into_bytes();

            tree.exec(&bucket, |txn| {
                txn.put(&key, &value)?;
                txn.put(b"shared", b"shared-value")?;
                assert_eq!(txn.get(&key)?, value);
                assert_eq!(txn.get(b"shared")?, b"shared-value".to_vec());

                let visible = collect(txn);
                assert_eq!(
                    visible.len(),
                    2,
                    "both keys are visible inside the transaction"
                );

                let (_, clean) = txn.read.overlay_tiers();
                if limit == 0 {
                    assert_eq!(clean, 0, "a zero residency budget keeps no Clean entry");
                } else {
                    assert!(clean > 0, "a non-zero budget holds Clean residency");
                }
                assert_eq!(
                    tree.store.node_io.max_writes_per_incarnation(),
                    1,
                    "no (PID, incarnation) may be written twice"
                );
                Ok(())
            })
            .unwrap();

            let written = tree.store.node_io.writes()[writes_before..].to_vec();
            assert!(!written.is_empty(), "the put wrote node pages");
            tree.set_resident_limit(DEFAULT_RESIDENT_LIMIT);
        }
        tree.set_dirty_limit(DEFAULT_DIRTY_LIMIT);
    }

    /// A PID released inside a transaction and handed out again is a **new
    /// incarnation**. The second bucket below allocates node pages and then fails, so its PIDs
    /// return to `reusable` and the outer transaction allocates again — the very case where a
    /// bare PID count would report a double write that never happened.
    #[test]
    fn write_log_attributes_reused_pids_to_separate_incarnations() {
        let dir = tempfile::TempDir::new().unwrap();
        let tree = BTree::open(dir.path().join("incarnation.db")).unwrap();
        tree.new_bucket("a", false).unwrap();
        tree.new_bucket("b", false).unwrap();

        tree.exec_multi(|multi| {
            multi.exec("a", |txn| txn.put(b"k1", b"v1"))?;
            let failed = multi.exec("b", |txn| -> Result<()> {
                txn.put(b"k2", b"v2")?;
                Err(Error::KeyNotFound)
            });
            assert!(
                failed.is_err(),
                "bucket b must fail after allocating node pages"
            );
            multi.exec("a", |txn| txn.put(b"k3", b"v3"))
        })
        .unwrap();

        let events = tree.store.node_io.events();
        let mut allocs: std::collections::HashMap<PageId, usize> = std::collections::HashMap::new();
        for event in &events {
            if event.kind == NodeIoKind::Alloc {
                *allocs.entry(event.pid).or_default() += 1;
            }
        }
        assert!(
            allocs.values().any(|&count| count >= 2),
            "the workload must hand a recycled PID out twice, otherwise this test proves nothing"
        );
        assert_eq!(
            tree.store.node_io.max_writes_per_incarnation(),
            1,
            "each write belongs to its own incarnation"
        );
        let mut per_pid: std::collections::HashMap<PageId, usize> =
            std::collections::HashMap::new();
        for pid in tree.store.node_io.writes() {
            *per_pid.entry(pid).or_default() += 1;
        }
        assert!(
            per_pid.values().any(|&count| count >= 2),
            "a bare PID count would report a double write for a recycled PID"
        );
    }

    /// A checkpoint taken while a page is resident must end the process on the
    /// invariant path — `rollback_to` could not otherwise restore a state the overlay describes.
    /// Subprocess, because `invariant` aborts (`#[should_panic]` would kill this test binary).
    #[test]
    fn checkpoint_with_a_nonempty_overlay_aborts() {
        // The child aborts, so a temporary directory of its own would outlive
        // the process that made it. The parent owns one and names the file.
        let dir = tempfile::TempDir::new().unwrap();
        // Both forbidden shapes: a resident entry, and the ownership-only state that outlives
        for mode in ["resident", "owned"] {
            let output = child_test_command(&std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "tests::checkpoint_with_a_resident_page_child",
                    "--ignored",
                    "--nocapture",
                ])
                .env("BTREE_CHECKPOINT_CHILD", mode)
                .env(
                    "BTREE_CHECKPOINT_CHILD_DB",
                    dir.path().join(format!("checkpoint-child-{mode}.db")),
                )
                .output()
                .unwrap();
            let combined = format!(
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(
                !output.status.success(),
                "[{mode}] a forbidden checkpoint must abort: {combined}"
            );
            assert!(
                combined.contains("btree-store fatal code=BTREE_FATAL_INVARIANT")
                    && combined.contains("fault=CHECKPOINT_AT_TXN_START"),
                "[{mode}] the failure must name the invariant: {combined}"
            );
            assert!(
                !combined.contains("panicked at"),
                "[{mode}] the guard must be the fatal path, not a panic: {combined}"
            );
            assert!(
                !combined.contains("unreachable"),
                "[{mode}] the guard must fire before the helper's `unreachable!`: {combined}"
            );
        }
    }

    #[test]
    #[ignore = "subprocess target for the checkpoint precondition"]
    fn checkpoint_with_a_resident_page_child() {
        let Ok(mode) = std::env::var("BTREE_CHECKPOINT_CHILD") else {
            return;
        };
        // The parent owns the directory: this process is about to abort, and a
        // temporary directory of its own would never be removed.
        let Ok(path) = std::env::var("BTREE_CHECKPOINT_CHILD_DB") else {
            return;
        };
        let tree = BTree::open(path).unwrap();
        tree.new_bucket("b", false).unwrap();
        match mode.as_str() {
            "owned" => tree.checkpoint_with_owned_pages_only(),
            _ => tree.checkpoint_with_a_resident_page(),
        }
    }

    /// A whole-transaction rollback restores its checkpoint by clearing the overlay,
    /// so the publication that follows cannot flush the discarded pages. Witnesses: no node page
    /// reaches the disk, and the earlier content survives a reopen.
    #[test]
    fn whole_transaction_rollback_clears_residency() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("rollback-clears.db");
        let tree = BTree::open(&path).unwrap();
        tree.new_bucket("a", false).unwrap();
        tree.exec("a", |txn| txn.put(b"keep", b"value")).unwrap();

        let writes_before = tree.store.node_io.writes().len();
        let failed = tree.exec_multi(|multi| -> Result<()> {
            multi.exec("a", |txn| txn.put(b"gone", b"value"))?;
            Err(Error::KeyNotFound)
        });
        assert!(failed.is_err());
        assert_eq!(
            tree.store.node_io.writes().len(),
            writes_before,
            "a rolled-back transaction must leave nothing for the publication to flush"
        );

        drop(tree);
        let reopened = BTree::open(&path).unwrap();
        assert_eq!(
            reopened.view("a", |v| v.get(b"keep")).unwrap(),
            b"value".to_vec()
        );
        assert!(reopened.view("a", |v| v.get(b"gone")).is_err());
    }

    /// Across the dirty budgets the write path must switch exactly once —
    /// exhaust the budget, write through from then on, keep the already-dirty pages dirty until the
    /// commit flush — and the documented bounds must hold at every sampling point.
    #[test]
    fn dirty_budget_matrix_holds_the_documented_bounds() {
        for (dirty_limit, resident_limit, puts, survives) in [
            (1024usize, 8usize, 128usize, true),
            (64, 8, 128, true),
            (8, 8, 128, true),
            (64, 8, 4000, false),
            (8, 8, 4000, false),
            (1, 8, 4000, false),
            (8, 4096, 4000, false),
        ] {
            let dir = tempfile::TempDir::new().unwrap();
            let path = dir.path().join(format!("budget-{dirty_limit}.db"));
            let tree = BTree::open(&path).unwrap();
            tree.new_bucket("b", false).unwrap();
            tree.set_dirty_limit(dirty_limit);
            tree.set_resident_limit(resident_limit);

            let mut dirty_before_flush = 0usize;
            let mut dirty_pids = Vec::new();
            tree.exec("b", |txn| {
                for index in 0..puts {
                    txn.put(format!("k{index:06}").as_bytes(), b"value")?;
                }
                let (dirty, clean) = txn.read.overlay_tiers();
                assert_eq!(
                    txn.read.overlay_defer_enabled(),
                    survives,
                    "dirty_limit {dirty_limit} over {puts} puts: the budget must {} have been exhausted",
                    if survives { "not" } else { "" }
                );
                assert!(
                    dirty <= txn.read.overlay_owned_peak(),
                    "dirty_limit {dirty_limit}: the dirty set is a subset of the pages written within the budget"
                );
                if survives {
                    assert!(
                        dirty > 0,
                        "dirty_limit {dirty_limit}: pages written within the budget stay Dirty"
                    );
                    assert_eq!(
                        clean, 0,
                        "dirty_limit {dirty_limit}: nothing is written through while the budget lasts"
                    );
                } else {
                    assert!(
                        dirty <= dirty_limit,
                        "dirty_limit {dirty_limit}: {dirty} dirty entries must stay within budget"
                    );
                    assert!(
                        clean > 0,
                        "pages written after the budget ran out go to disk immediately"
                    );
                }
                assert!(
                    clean <= resident_limit,
                    "resident_limit {resident_limit}: {clean} clean entries exceed the budget"
                );
                assert!(
                    dirty + clean <= dirty_limit + resident_limit,
                    "entry memory must stay within dirty_limit + resident_limit"
                );
                assert!(
                    txn.read.overlay_order_len() <= 2 * clean + 1,
                    "dirty_limit {dirty_limit}: {} eviction-queue slots for {clean} resident \
                     entries; the queue must not grow with the transaction's writes",
                    txn.read.overlay_order_len()
                );
                dirty_before_flush = dirty;
                dirty_pids = txn.read.overlay_dirty_pids();
                Ok(())
            })
            .unwrap();

            let stats = tree
                .store
                .node_io
                .flushes()
                .last()
                .cloned()
                .expect("a commit flush");
            assert!(
                stats.dirty_pages >= dirty_before_flush,
                "dirty_limit {dirty_limit}: the flush must hand over at least the Dirty tier"
            );
            assert!(
                dirty_pids.iter().all(|pid| stats.pids.contains(pid)),
                "dirty_limit {dirty_limit}: every page that was Dirty at commit time must be written by the flush (the commit path may dirty the catalog/root on top)"
            );
            assert_eq!(
                stats.pids.len(),
                stats.dirty_pages,
                "dirty_limit {dirty_limit}: the flush writes each handed page once"
            );
            let mut unique = stats.pids.clone();
            unique.sort_unstable();
            unique.dedup();
            assert_eq!(
                unique.len(),
                stats.pids.len(),
                "dirty_limit {dirty_limit}: the flush must not hand a page over twice"
            );
            for pid in &stats.pids {
                assert_eq!(
                    tree.store
                        .node_io
                        .writes()
                        .iter()
                        .filter(|written| *written == pid)
                        .count(),
                    1,
                    "dirty_limit {dirty_limit}: page {pid} is written exactly once per incarnation"
                );
            }
            assert_eq!(
                tree.store.node_io.max_writes_per_incarnation(),
                1,
                "dirty_limit {dirty_limit}: a page is written once per incarnation"
            );

            drop(tree);
            let reopened = BTree::open(&path).unwrap();
            for index in (0..puts).step_by(97) {
                assert_eq!(
                    reopened
                        .view("b", |view| view.get(format!("k{index:06}").as_bytes()))
                        .unwrap(),
                    b"value".to_vec(),
                    "dirty_limit {dirty_limit}: reopened content must match"
                );
            }
        }
    }

    /// Residency must be observable independent of the shared cache. With a
    /// budget that covers the workset every read-back is answered from the overlay; with a budget
    /// below it the misses must not backfill the shared cache and the content must still be right.
    /// `defer` on/off decides which tier provides the residency, not the guarantees.
    #[test]
    fn residency_and_misses_are_independent_of_the_shared_cache() {
        const GENEROUS: usize = 4096;
        // Enough keys that the leaf splits: the workset must exceed the small budgets below.
        let requests = 512usize;
        let value = vec![b'v'; 1024];
        for defer in [true, false] {
            let dir = tempfile::TempDir::new().unwrap();
            let tree = BTree::open(dir.path().join("residency.db")).unwrap();
            tree.new_bucket("b", false).unwrap();
            tree.set_dirty_limit(if defer { DEFAULT_DIRTY_LIMIT } else { 0 });

            for (label, resident) in [
                ("resident>=workset", GENEROUS),
                ("resident=2<workset", 2),
                ("resident=0<workset", 0),
            ] {
                tree.set_resident_limit(resident);
                let cache_puts_before = tree.store.node_io.cache_puts().len();
                let mut owned = Vec::new();
                let mut hits = 0usize;
                let mut misses = 0usize;
                tree.exec("b", |txn| {
                    for index in 0..requests {
                        txn.put(format!("k{index:04}").as_bytes(), &value)?;
                    }
                    owned = txn.read.overlay_owned_pids();
                    assert!(
                        txn.read.overlay_owned_peak() > 2,
                        "defer={defer} {label}: the workload's workset must exceed the small budgets"
                    );

                    // The `true` rows below only hold while the dirty budget has not
                    // been exhausted. Exhausting it turns defer off, after which
                    // entries become `Clean` and `resident_limit` binds them, so a
                    // larger workset would break those rows for the wrong reason.
                    assert_eq!(
                        txn.read.overlay_defer_enabled(),
                        defer,
                        "defer={defer} {label}: the dirty budget must not have been exhausted, \
                         or this row is testing the clean-tier path under a defer label"
                    );
                    for index in 0..requests {
                        assert_eq!(
                            txn.get(format!("k{index:04}").as_bytes())?,
                            value,
                            "defer={defer} {label}: content must be correct"
                        );
                    }
                    hits = tree
                        .store
                        .node_io
                        .overlay_hits()
                        .iter()
                        .filter(|pid| owned.contains(pid))
                        .count();
                    // A miss is the owned-PID read-back that residency could not answer. It
                    // counts node loads, not keys: one `get` may traverse several levels.
                    misses = tree
                        .store
                        .node_io
                        .class_pids(NodeIoKind::OverlayMiss)
                        .iter()
                        .filter(|pid| owned.contains(pid))
                        .count();
                    Ok(())
                })
                .unwrap();

                let cache_puts = tree.store.node_io.cache_puts();
                assert!(
                    !cache_puts[cache_puts_before..]
                        .iter()
                        .any(|pid| owned.contains(pid)),
                    "defer={defer} {label}: a miss must never backfill the shared cache"
                );
                match (defer, resident) {
                    // A budget that covers the workset must answer every read from the overlay.
                    (_, GENEROUS) => {
                        assert!(
                            hits >= requests,
                            "defer={defer} {label}: every read-back must hit the overlay, saw {hits}"
                        );
                        assert_eq!(
                            misses, 0,
                            "defer={defer} {label}: a budget over the workset misses nothing"
                        );
                    }
                    (false, 0) => {
                        assert_eq!(
                            hits, 0,
                            "defer={defer} {label}: with no residency at all nothing can hit"
                        );
                        assert!(
                            misses > 0,
                            "defer={defer} {label}: with no residency every read-back misses"
                        );
                    }
                    // With `defer` on, `Dirty` entries are the page's only copy and are never
                    // evicted, so `resident_limit` cannot bound them however small it is:
                    // `evict_excess_clean` only pops PIDs that entered the `Clean` queue, and
                    // `insert_dirty` never enqueues one. Evicting a Dirty entry would drop the
                    // only copy of a page that is not on disk yet, so the read-back must still
                    // be served at every budget.
                    (true, 2) | (true, 0) => {
                        assert!(
                            hits >= requests,
                            "defer={defer} {label}: Dirty residency ignores the budget, so every \
                             read-back must hit the overlay, saw {hits}"
                        );
                        assert_eq!(
                            misses, 0,
                            "defer={defer} {label}: a Dirty entry is never an eviction candidate, \
                             so a budget of {resident} must still miss nothing"
                        );
                    }
                    // Without defer the entries are `Clean` and the budget binds them, so a
                    // budget below the workset *must* cost residency — that is what makes
                    // `resident=2` a residency test at all. Eviction is only a locality loss:
                    // the content assertion above covers correctness either way.
                    (false, 2) => assert!(
                        misses > 0,
                        "defer={defer} {label}: a budget below the workset must miss at least one \
                         owned read-back, saw {misses} misses over {hits} hits"
                    ),
                    (defer, resident) => unreachable!(
                        "defer={defer} {label}: unclassified residency row (defer={defer}, \
                         resident={resident})"
                    ),
                }
                eprintln!(
                    "residency report: defer={defer} {label} reads={requests} overlay-hits={hits} \
                     misses={misses} shared-cache-puts=0"
                );
            }
            tree.set_dirty_limit(DEFAULT_DIRTY_LIMIT);
            tree.set_resident_limit(DEFAULT_RESIDENT_LIMIT);
        }
    }

    /// A reachable Dirty page must not leave its tier except through a flush:
    /// inside one transaction a page that sits in the `Dirty` tier stays there — it may leave only
    /// because it was released (`releases` holds it) or because a flush ran. Sampled after every
    /// put, so a silent drop between the flush and the commit would fail here.
    #[test]
    fn a_reachable_dirty_page_never_leaves_the_dirty_tier() {
        let dir = tempfile::TempDir::new().unwrap();
        let tree = BTree::open(dir.path().join("dirty-monotone.db")).unwrap();
        tree.new_bucket("b", false).unwrap();
        tree.set_dirty_limit(DEFAULT_DIRTY_LIMIT);

        let mut samples: Vec<(Vec<PageId>, Vec<PageId>, usize)> = Vec::new();
        tree.exec("b", |txn| {
            for index in 0..24u32 {
                txn.put(format!("k{index:03}").as_bytes(), b"value")?;
                let (_, clean) = txn.read.overlay_tiers();
                assert_eq!(
                    clean, 0,
                    "no page is written through while the budget lasts"
                );
                samples.push((
                    txn.read.overlay_dirty_pids(),
                    tree.store.node_io.releases(),
                    tree.store.node_io.flushes().len(),
                ));
            }
            Ok(())
        })
        .unwrap();

        assert!(!samples.is_empty(), "the workload sampled the tier");
        for window in samples.windows(2) {
            let (before, _, flushes_before) = &window[0];
            let (after, releases_after, flushes_after) = &window[1];
            assert_eq!(
                flushes_before, flushes_after,
                "a transaction must not flush before its commit"
            );
            for pid in before {
                assert!(
                    after.contains(pid) || releases_after.contains(pid),
                    "page {pid} left the Dirty tier with neither a flush nor a release"
                );
            }
        }
        assert!(
            !samples.last().unwrap().0.is_empty(),
            "the transaction must still hold Dirty pages when it ends"
        );
    }

    /// The bucket-deletion path frees *published* pages
    /// only — it hands no page of its own transaction back — so it needs no overlay hook. This is
    /// the executable form of that claim: the deletion reports no release (a release would have to
    /// come from its own overlay) and the deleted tree's pages land in the durable allocator.
    #[test]
    fn bucket_deletion_releases_published_pages_only() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("bucket-drop.db");
        let tree = BTree::open(&path).unwrap();
        tree.new_bucket("keep", false).unwrap();
        tree.new_bucket("drop", false).unwrap();
        tree.exec("drop", |txn| {
            for index in 0..64u32 {
                txn.put(format!("k{index:03}").as_bytes(), b"value")?;
            }
            Ok(())
        })
        .unwrap();

        let releases_before = tree.store.node_io.releases().len();
        let free_before = tree.store.free_page_count_for_test();
        tree.del_bucket("drop").unwrap();

        assert_eq!(
            tree.store.node_io.releases().len(),
            releases_before,
            "dropping a bucket frees published pages only: it releases no page of its own transaction"
        );
        assert!(
            tree.store.free_page_count_for_test() > free_before,
            "the deleted tree's pages are returned to the allocator"
        );
        assert!(
            tree.buckets().unwrap().iter().all(|name| name != "drop"),
            "the bucket is gone"
        );
        assert!(tree.view("keep", |view| view.get(b"k000")).is_err());
        assert_nonempty_generation_page_accounting(&tree);

        drop(tree);
        let reopened = BTree::open(&path).unwrap();
        assert_nonempty_generation_page_accounting(&reopened);
    }

    #[test]
    fn publication_io_classes_name_the_written_meta_and_allocator_pages() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("publication-classes.db");
        let tree = BTree::open(&path).unwrap();
        tree.new_bucket("b", false).unwrap();
        tree.exec("b", |txn| txn.put(b"k", b"first")).unwrap();
        let events_before = tree.store.node_io.events().len();
        tree.exec("b", |txn| txn.put(b"k", b"second")).unwrap();
        let events = tree.store.node_io.events();
        let events = &events[events_before..];
        // An open store locks its file for as long as it lives, so the pages are
        // read after the handle is gone: Windows refuses a second handle on a
        // locked range even inside the same process.
        drop(tree);
        let bytes = std::fs::read(&path).unwrap();
        let mut allocator_pages = HashSet::new();
        let meta = snapshot_meta(&path);
        for root in [meta.reusable_root, meta.retired_root] {
            let mut pid = root;
            while pid != 0 {
                assert!(allocator_pages.insert(pid));
                let start = pid as usize * PAGE_SIZE;
                pid = u32::from_le_bytes(bytes[start + 4..start + 8].try_into().unwrap());
            }
        }
        assert!(!allocator_pages.is_empty());
        let observed: HashSet<_> = events
            .iter()
            .filter(|event| event.kind == NodeIoKind::AllocatorWrite)
            .map(|event| event.pid)
            .collect();
        assert_eq!(observed, allocator_pages);
        let meta_writes: Vec<_> = events
            .iter()
            .filter(|event| event.kind == NodeIoKind::MetaWrite)
            .map(|event| event.pid)
            .collect();
        assert_eq!(meta_writes.len(), 1);
        let start = meta_writes[0] as usize * PAGE_SIZE;
        assert_eq!(
            MetaNode::from_slice(&bytes[start..start + 40]).seq,
            meta.seq
        );
        assert_eq!(events.last().unwrap().kind, NodeIoKind::MetaWrite);
    }

    /// An update/delete of an **absent** key must not allocate or write any page: the lookup fails
    /// before anything is modified, so only read-class events may appear.
    #[test]
    fn absent_key_update_and_delete_touch_no_page() {
        let dir = tempfile::TempDir::new().unwrap();
        let tree = BTree::open(dir.path().join("noop.db")).unwrap();
        tree.new_bucket("b", false).unwrap();
        tree.exec("b", |txn| txn.put(b"k", b"v")).unwrap();

        tree.exec("b", |txn| {
            let events_before = tree.store.node_io.events().len();
            let owned_before = txn.read.overlay_owned_pids();
            assert!(
                !txn.update(b"missing", b"x")?,
                "an absent key is not updated"
            );
            assert!(
                matches!(txn.del(b"missing"), Err(Error::KeyNotFound)),
                "an absent key cannot be deleted"
            );
            assert_eq!(
                txn.read.overlay_owned_pids(),
                owned_before,
                "a no-op update/delete must not own a page"
            );
            let writes = tree.store.node_io.events()[events_before..]
                .iter()
                .filter(|event| {
                    matches!(
                        event.kind,
                        NodeIoKind::Alloc
                            | NodeIoKind::Write
                            | NodeIoKind::ValueWrite
                            | NodeIoKind::IndirectWrite
                            | NodeIoKind::AllocatorWrite
                            | NodeIoKind::MetaWrite
                            | NodeIoKind::Cover
                    )
                })
                .count();
            assert_eq!(
                writes, 0,
                "a no-op update/delete must not allocate or write any page (reads are fine)"
            );
            Ok(())
        })
        .unwrap();
        assert_eq!(
            tree.view("b", |view| view.get(b"k")).unwrap(),
            b"v".to_vec()
        );
    }

    /// The observable form of the one-write-per-incarnation rule. After a PID is
    /// released and handed out again, every read path — the same handle, a later write transaction,
    /// and a reopen — must see the *new* incarnation's content, and a later write transaction must
    /// never COW from the previous incarnation's bytes.
    #[test]
    fn reused_page_ids_never_expose_a_previous_incarnation() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("t10.db");
        let tree = BTree::open(&path).unwrap();
        tree.new_bucket("b", false).unwrap();

        let first = vec![0xAAu8; 200_000];
        tree.exec("b", |txn| txn.put(b"first", &first)).unwrap();
        assert_eq!(tree.view("b", |v| v.get(b"first")).unwrap(), first);
        tree.exec("b", |txn| txn.del(b"first")).unwrap();

        let second = vec![0xBBu8; 200_000];
        tree.exec("b", |txn| txn.put(b"second", &second)).unwrap();

        assert!(
            matches!(tree.view("b", |v| v.get(b"first")), Err(Error::KeyNotFound)),
            "the deleted key must not come back through a recycled PID"
        );
        assert_eq!(
            tree.view("b", |v| v.get(b"second")).unwrap(),
            second,
            "a recycled PID must expose the new incarnation's bytes"
        );

        // A later write transaction must not COW from the previous incarnation: rewriting the
        tree.exec("b", |txn| txn.put(b"second", b"small")).unwrap();
        assert_eq!(
            tree.view("b", |v| v.get(b"second")).unwrap(),
            b"small".to_vec()
        );
        tree.exec("b", |txn| txn.put(b"third", &first)).unwrap();
        assert_eq!(tree.view("b", |v| v.get(b"third")).unwrap(), first);

        // from the first transaction must have been handed out again. Those pages are written
        // immediately and never live in the overlay, so they are the reuse case where stale bytes
        let value_pids: Vec<PageId> = tree
            .store
            .node_io
            .class_pids(NodeIoKind::ValueWrite)
            .into_iter()
            .chain(tree.store.node_io.class_pids(NodeIoKind::IndirectWrite))
            .collect();
        assert!(
            !value_pids.is_empty(),
            "the spilled value must write value pages"
        );
        let events = tree.store.node_io.events();
        let mut allocs: std::collections::HashMap<PageId, usize> = std::collections::HashMap::new();
        for event in &events {
            if event.kind == NodeIoKind::Alloc {
                *allocs.entry(event.pid).or_default() += 1;
            }
        }
        assert!(
            value_pids
                .iter()
                .any(|pid| allocs.get(pid).copied().unwrap_or(0) >= 2),
            "the workload must hand a value/indirect PID out twice, otherwise this test does not \
             exercise the reuse path it claims"
        );

        // Reopen: the durable state matches what the handle reported.
        drop(tree);
        let reopened = BTree::open(&path).unwrap();
        assert_eq!(
            reopened.view("b", |v| v.get(b"second")).unwrap(),
            b"small".to_vec()
        );
        assert_eq!(reopened.view("b", |v| v.get(b"third")).unwrap(), first);
        assert!(matches!(
            reopened.view("b", |v| v.get(b"first")),
            Err(Error::KeyNotFound)
        ));
        assert_nonempty_generation_page_accounting(&reopened);
    }

    /// `TxnCore::rollback_to` restores the checkpoint by clearing the
    /// whole overlay, and `TxnOverlay::clear` is the only thing it does to it — so entries *and*
    /// ownership must both be gone, while the write-through decision stays: exhausting the dirty
    /// budget is sticky for the overlay's whole life, and a rollback does not re-open it.
    #[test]
    fn clear_overlay_drops_entries_and_ownership_but_not_the_budget() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = test_store(&dir);
        let overlay = Arc::new(RwLock::new(TxnOverlay::new(8, 8)));
        let read = TreeReadContext::new(test_runtime(store)).with_overlay(overlay.clone());

        read.insert_overlay_dirty(7, Arc::new(Node::new_leaf()));
        read.insert_overlay_dirty(9, Arc::new(Node::new_leaf()));
        assert_eq!(overlay.read().len(), 2);
        assert_eq!(read.overlay_owned_pids(), vec![7, 9]);

        for pid in 11..17 {
            read.insert_overlay_dirty(pid, Arc::new(Node::new_leaf()));
        }
        assert!(
            !read.take_dirty_slot(),
            "a full budget sends the write through"
        );
        assert!(!read.overlay_defer_enabled(), "exhaustion turns defer off");

        read.clear_overlay();
        assert_eq!(overlay.read().len(), 0, "entries are gone");
        assert!(
            read.overlay_owned_pids().is_empty(),
            "ownership goes with the entries (`rollback_to` must not leave an owned PID)"
        );
        assert_eq!(read.overlay_tiers(), (0, 0), "both tiers restart empty");
        assert!(
            !read.overlay_defer_enabled(),
            "a rollback must not re-open the budget: `defer` is sticky for the overlay's whole life"
        );
    }

    /// The shared cache's capacity must not change any observable result, and a
    /// transaction's own pages must never be backfilled into it (zero `cache.put` for an
    /// owned PID), including on the `del_bucket` path.
    #[test]
    fn shared_cache_capacity_does_not_change_results_and_never_sees_owned_pages() {
        for capacity in [0usize, 1, 8192] {
            let dir = tempfile::TempDir::new().unwrap();
            let path = dir.path().join("cache-capacity.db");
            let options = OpenOptions {
                cache_capacity: capacity,
                ..OpenOptions::default()
            };
            let tree = BTree::open_with_options(&path, options).unwrap();
            tree.new_bucket("b", false).unwrap();
            tree.new_bucket("drop", false).unwrap();
            tree.exec("drop", |txn| txn.put(b"x", b"y")).unwrap();
            tree.del_bucket("drop").unwrap();

            let cache_puts_before = tree.store.node_io.cache_puts().len();
            let mut reads = Vec::new();
            let mut owned = Vec::new();
            tree.exec("b", |txn| {
                txn.put(b"k1", b"v1")?;
                txn.put(b"k2", b"v2")?;
                reads.push(txn.get(b"k1")?);
                let (mut key, mut value) = (Vec::new(), Vec::new());
                let mut iter = txn.iter();
                while iter.next_ref(&mut key, &mut value) {
                    reads.push(value.clone());
                }
                owned = txn.read.overlay_owned_pids();
                Ok(())
            })
            .unwrap();

            assert_eq!(
                reads,
                vec![b"v1".to_vec(), b"v1".to_vec(), b"v2".to_vec()],
                "capacity {capacity} must not change what a transaction reads"
            );
            assert!(!owned.is_empty(), "the transaction owns pages");
            let hits = tree.store.node_io.overlay_hits();
            assert!(
                hits.iter().any(|pid| owned.contains(pid)),
                "capacity {capacity}: reading own writes must be answered by the overlay"
            );
            let puts = tree.store.node_io.cache_puts();
            for pid in &owned {
                assert!(
                    !puts[cache_puts_before..].contains(pid),
                    "capacity {capacity}: transaction-private page {pid} must never enter the \
                     shared cache"
                );
            }
            assert_eq!(
                tree.view("b", |view| view.get(b"k1")).unwrap(),
                b"v1".to_vec(),
                "published reads keep working at capacity {capacity}"
            );
        }
    }

    /// One ownership class per PID, entries ⊆ owned ⊆ pending, every owned PID
    /// came from an allocation, and only node writes create entries — a spilled value allocates
    /// dozens of pages but adds none.
    #[test]
    fn overlay_ownership_invariants() {
        let measure = |value_len: usize| {
            const PUTS: u32 = 16;
            let dir = tempfile::TempDir::new().unwrap();
            let tree = BTree::open(dir.path().join("ownership.db")).unwrap();
            tree.new_bucket("b", false).unwrap();
            let mut entries = Vec::new();
            let mut owned = Vec::new();
            // a PID can be handed out again in a later transaction after a legitimate release).
            let window_start = tree.store.node_io.events().len();
            tree.exec("b", |txn| {
                for index in 0..PUTS {
                    txn.put(format!("k{index:03}").as_bytes(), vec![b'x'; value_len])?;
                }
                entries = txn.read.overlay_entry_pids();
                owned = txn.read.overlay_owned_pids();
                assert!(
                    entries.iter().all(|pid| owned.contains(pid)),
                    "every resident entry belongs to an owned PID"
                );
                assert!(
                    owned
                        .iter()
                        .all(|pid| txn.page_state.pending_alloc.contains(pid)),
                    "every owned PID is still pending"
                );
                Ok(())
            })
            .unwrap();
            let allocs: Vec<PageId> = tree
                .store
                .node_io
                .events()
                .into_iter()
                .filter(|event| event.kind == NodeIoKind::Alloc)
                .map(|event| event.pid)
                .collect();
            assert!(
                owned.iter().all(|pid| allocs.contains(pid)),
                "ownership only comes from allocations"
            );
            // only transaction-allocated page ids may be reused: every owned PID's *current* incarnation was handed out by
            // this transaction, which is what keeps pages referenced by a published generation out
            // of the overlay (an older generation's reader can therefore never see them change).
            // released it is legitimately free to come back from `reusable` here.
            let in_window_allocs: Vec<PageId> = tree
                .store
                .node_io
                .events()
                .into_iter()
                .skip(window_start)
                .filter(|event| event.kind == NodeIoKind::Alloc)
                .map(|event| event.pid)
                .collect();
            assert!(
                owned.iter().all(|pid| in_window_allocs.contains(pid)),
                "every owned PID must have been handed out by this transaction"
            );
            // reuse is exercised, and it stays inside one transaction's own page ids.
            assert!(
                in_window_allocs.len() > owned.len(),
                "the workload must reuse transaction-born PIDs (in-window allocations {} vs owned {})",
                in_window_allocs.len(),
                owned.len()
            );
            (entries.len(), owned.len())
        };

        let (small_entries, _) = measure(8);
        let (large_entries, _) = measure(200_000);
        assert!(small_entries > 0, "a put creates overlay entries");
        assert_eq!(
            small_entries, large_entries,
            "value and indirect pages are not nodes, so a spilled value must not create overlay entries"
        );
    }

    /// At the instant the meta slot is written, the live file
    /// must already cover the id space that publication declares.
    #[test]
    fn publication_never_declares_an_uncovered_id_space() {
        let dir = tempfile::TempDir::new().unwrap();
        let database = dir.path().join("publish-hook.db");
        let tree = BTree::open(&database).unwrap();
        tree.new_bucket("b", false).unwrap();
        tree.exec("b", |txn| txn.put(b"keep", b"value")).unwrap();

        let observations = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorded = observations.clone();
        let live_file = tree.store.clone();
        let arming_thread = std::thread::current().id();
        *BEFORE_META_SLOT_WRITE.lock() = Some(Arc::new(move |point: PublicationPoint| {
            // Only this test's thread: other tests publish in parallel.
            if std::thread::current().id() != arming_thread {
                return;
            }
            assert!(
                u64::from(point.next_page_id) * PAGE_SIZE as u64 <= point.file_len,
                "a publication declared {} pages while the file holds {} bytes",
                point.next_page_id,
                point.file_len
            );
            // A pre-write hook must find the slot still holding an *older* generation; if the hook
            // ran after the write this would read back `point.seq`.
            let mut bytes = [0u8; 40];
            let slot_seq = live_file
                .test_pread_exact(&mut bytes, point.write_offset)
                .map(|()| MetaNode::from_slice(&bytes).seq);
            recorded.lock().unwrap().push((point.seq, slot_seq));
        }));

        // cover the id space before its meta slot goes out.
        let failed = tree.exec("b", |txn| -> Result<()> {
            txn.put(b"gone", b"value")?;
            Err(Error::KeyNotFound)
        });
        assert!(failed.is_err());

        let observed = observations.lock().unwrap();
        *BEFORE_META_SLOT_WRITE.lock() = None;
        assert!(
            !observed.is_empty(),
            "the publication hook must have inspected at least one meta slot write"
        );
        let readable = observed.iter().filter(|(_, slot)| slot.is_ok()).count();
        assert!(
            readable > 0,
            "the hook must be able to read the slot it is about to write: {observed:?}"
        );
        for (seq, slot) in observed.iter() {
            if let Ok(slot_seq) = slot {
                assert_ne!(
                    *slot_seq, *seq,
                    "meta slot at write_offset already held generation {seq}: the hook ran *after* the write"
                );
            }
        }
        drop(observed);

        // The snapshot's own file must cover the id space its meta declares, and
        // the frozen copy must open with the same content.
        let snapshot_path = dir.path().join("frozen.db");
        let snapshot = tree.take_snapshot(&snapshot_path).unwrap();
        let frozen = snapshot_meta(&snapshot_path).next_page_id;
        let length = std::fs::metadata(&snapshot_path).unwrap().len();
        assert!(
            u64::from(frozen) * PAGE_SIZE as u64 <= length,
            "the snapshot declares {frozen} pages but its file holds only {length} bytes"
        );
        let copy = BTree::open(&snapshot_path).unwrap();
        assert_eq!(copy.current_seq(), snapshot.seq);
        assert_eq!(
            copy.view("b", |view| view.get(b"keep")).unwrap(),
            b"value".to_vec(),
            "the frozen copy must hold the same content"
        );
    }

    fn snapshot_meta(path: &Path) -> MetaNode {
        let bytes = std::fs::read(path).unwrap();
        let mut newest: Option<MetaNode> = None;
        for slot in [0usize, 1] {
            let start = slot * PAGE_SIZE;
            if start + PAGE_SIZE > bytes.len() {
                continue;
            }
            let meta = MetaNode::from_slice(&bytes[start..start + PAGE_SIZE]);
            if meta.validate().is_err() {
                continue;
            }
            let newer = match &newest {
                Some(current) => meta.seq > current.seq,
                None => true,
            };
            if newer {
                newest = Some(meta);
            }
        }
        newest.expect("a valid meta page")
    }

    /// While the dirty budget lasts, node writes stay in the overlay and no
    /// page reaches the disk; the commit flush writes them all and degrades them to `Clean`. The
    /// budget never re-opens once exhausted, and later writes go straight through.
    #[test]
    fn dirty_budget_defers_then_sticks_off() {
        let dir = tempfile::TempDir::new().unwrap();
        let tree = BTree::open(dir.path().join("dirty-budget.db")).unwrap();
        tree.new_bucket("b", false).unwrap();

        let writes_before = tree.store.node_io.writes().len();
        let covers_before = tree.store.node_io.covers().len();
        let cover_calls_before = tree.store.node_io.cover_calls();
        let mut deferred = 0usize;
        tree.exec("b", |txn| {
            txn.put(b"a", b"1")?;
            txn.put(b"b", b"2")?;
            let (dirty, clean) = txn.read.overlay_tiers();
            assert!(
                txn.read.overlay_defer_enabled(),
                "defer is on while the budget lasts"
            );
            assert!(
                dirty > 0 && clean == 0,
                "every node write is deferred, got {dirty}/{clean}"
            );
            assert_eq!(
                tree.store.node_io.writes().len(),
                writes_before,
                "no node page may reach the disk before the commit flush"
            );
            assert_eq!(
                tree.store.node_io.cover_calls(),
                cover_calls_before,
                "the cover step belongs to the publication, not to the operations"
            );
            deferred = txn.read.overlay_len();
            Ok(())
        })
        .unwrap();

        let flushed = tree.store.node_io.writes()[writes_before..].to_vec();
        assert!(
            !flushed.is_empty(),
            "the commit flush wrote the deferred pages"
        );
        assert!(
            deferred >= 1,
            "the transaction held residency while it deferred"
        );
        let stats = tree
            .store
            .node_io
            .flushes()
            .last()
            .cloned()
            .expect("a commit flush");
        assert_eq!(
            stats.dirty_pages,
            flushed.len(),
            "the flush wrote exactly the Dirty pages it was handed"
        );
        assert!(
            stats.runs < stats.dirty_pages,
            "a deferred batch must be written as runs: {} runs for {} pages",
            stats.runs,
            stats.dirty_pages
        );
        assert_eq!(
            tree.store.node_io.covers().len(),
            covers_before,
            "the flush covered the id space, so the gate must not have fired"
        );
        assert_eq!(
            tree.store.node_io.cover_calls(),
            cover_calls_before + 1,
            "one publication, one cover call"
        );
        assert_eq!(
            tree.store.node_io.max_writes_per_incarnation(),
            1,
            "a deferred page is written exactly once, by the commit flush that publishes it"
        );

        tree.set_dirty_limit(1);
        tree.exec("b", |txn| {
            txn.put(b"c", b"3")?;
            let after_first = tree.store.node_io.writes().len();
            txn.put(b"d", b"4")?;
            let after_second = tree.store.node_io.writes().len();
            txn.put(b"e", b"5")?;
            let after_third = tree.store.node_io.writes().len();
            assert!(
                !txn.read.overlay_defer_enabled(),
                "the dirty budget must not re-open once exhausted"
            );
            assert!(
                after_second > after_first,
                "a write after the budget is gone must write through, not defer again"
            );
            assert!(
                after_third > after_second,
                "and it must keep writing through after that op's page is released"
            );
            let (dirty, clean) = txn.read.overlay_tiers();
            assert_eq!(
                dirty + clean,
                txn.read.overlay_len(),
                "the tier counts must describe exactly the resident entries"
            );
            Ok(())
        })
        .unwrap();
        tree.set_dirty_limit(DEFAULT_DIRTY_LIMIT);
    }

    /// A PID released inside the transaction loses its entry **and** its
    /// ownership, so the publication that follows a failure cannot flush stale node bytes, and the
    /// dense-coverage step has to extend the file because the topmost allocated page was never
    /// written (its non-crash part).
    #[test]
    fn failed_transaction_releases_deferred_pages_and_covers_the_id_space() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("released-deferred.db");
        let tree = BTree::open(&path).unwrap();
        tree.new_bucket("b", false).unwrap();
        tree.exec("b", |txn| txn.put(b"keep", b"value")).unwrap();

        let writes_before = tree.store.node_io.writes().len();
        let covers_before = tree.store.node_io.covers().len();
        let mut owned_by_the_failed_txn = Vec::new();
        let failed = tree.exec("b", |txn| -> Result<()> {
            txn.put(b"gone", b"value")?; // deferred: nothing written yet
            owned_by_the_failed_txn = txn.read.overlay_owned_pids();
            Err(Error::KeyNotFound)
        });
        assert!(failed.is_err());
        assert!(
            !owned_by_the_failed_txn.is_empty(),
            "the failed transaction owned the pages it deferred"
        );

        assert_eq!(
            tree.store.node_io.writes().len(),
            writes_before,
            "pages of a failed transaction must never be flushed"
        );
        assert_eq!(
            tree.store.node_io.covers().len(),
            covers_before + 1,
            "the released top PID left the id space uncovered, so the rollback publication covers it"
        );
        // wrote nothing — the deferred bytes died with the rollback.
        let released = tree.store.node_io.releases();
        // `writes` never grew (asserted above), so this is the whole write history: a page the
        // failed transaction deferred must not appear in it at all.
        let writes = tree.store.node_io.writes();
        for pid in &owned_by_the_failed_txn {
            assert!(
                released.contains(pid),
                "a failed transaction releases every page it owned (page {pid})"
            );
            assert!(
                !writes.contains(pid),
                "releasing page {pid} must not write it"
            );
        }
        let snapshot = tree.store.cached_snapshot();
        assert!(
            tree.store.live_file_len() >= u64::from(snapshot.next_page_id) * PAGE_SIZE as u64,
            "a publication must never declare an id space the live file does not cover"
        );

        drop(tree);
        let reopened = BTree::open(&path).unwrap();
        assert_eq!(
            reopened.view("b", |view| view.get(b"keep")).unwrap(),
            b"value".to_vec()
        );
        assert!(
            reopened.view("b", |view| view.get(b"gone")).is_err(),
            "a failed transaction must leave nothing visible"
        );
    }

    #[test]
    fn nested_publication_keeps_the_outer_deferred_writes() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("nested-cover.db");
        let tree = BTree::open(&path).unwrap();
        tree.new_bucket("a", false).unwrap();
        tree.new_bucket("b", false).unwrap();

        let flushes_before = tree.store.node_io.flushes().len();
        let mut outer_residency = 0usize;
        let mut outer_owned = Vec::new();
        let mut inner_owned = Vec::new();
        tree.exec_multi(|multi| {
            multi.exec("a", |txn| {
                txn.put(b"k1", b"v1")?;
                outer_residency = txn.read.overlay_len();
                outer_owned = txn.read.overlay_owned_pids();
                Ok(())
            })?;

            let writes_before = tree.store.node_io.writes().len();
            let mut inner_writes = usize::MAX;
            let failed = multi.exec("b", |txn| -> Result<()> {
                txn.put(b"gone", b"v1")?; // deferred, then released by the savepoint rollback
                inner_writes = tree.store.node_io.writes().len();
                inner_owned = txn.read.overlay_owned_pids();
                Err(Error::KeyNotFound)
            });
            assert!(failed.is_err(), "bucket b must fail");
            assert_eq!(
                inner_writes, writes_before,
                "the inner bucket deferred its node write instead of writing it"
            );
            assert_eq!(
                tree.store.node_io.flushes().len(),
                flushes_before + 1,
                "the nested publication flushes the retained Dirty set itself"
            );

            multi.exec("a", |txn| -> Result<()> {
                assert_eq!(
                    txn.read.overlay_len(),
                    outer_residency,
                    "the savepoint rollback drops the inner bucket's entries and leaves the outer ones"
                );
                assert_eq!(
                    txn.read.overlay_owned_pids(),
                    outer_owned,
                    "a released page loses its entry and its ownership together"
                );
                assert!(
                    !inner_owned.is_empty() && inner_owned.len() > outer_owned.len(),
                    "the failing bucket owned pages of its own"
                );
                // A spilled value right after the rollback: its pages may reuse the released PIDs,
                // and stale deferred bytes must never land on them.
                txn.put(b"k2", vec![b'x'; 200_000])
            })
        })
        .unwrap();

        let snapshot = tree.store.cached_snapshot();
        assert!(
            tree.store.live_file_len() >= u64::from(snapshot.next_page_id) * PAGE_SIZE as u64,
            "the nested publication must also leave the id space covered"
        );
        assert_eq!(tree.view("a", |v| v.get(b"k1")).unwrap(), b"v1".to_vec());
        assert_eq!(
            tree.view("a", |v| v.get(b"k2")).unwrap(),
            vec![b'x'; 200_000],
            "the spilled value written after the rollback must read back intact"
        );
        assert!(tree.view("b", |v| v.get(b"gone")).is_err());
    }

    /// `dirty_count`/`clean_count` count **entries**, not insertions. One write per (PID, incarnation) means a PID
    /// is written once per incarnation, so a re-insert cannot happen in production — the invariant
    /// has to hold anyway, and this is the regression test for it.
    #[test]
    fn overlay_tier_counts_count_entries_not_insertions() {
        let mut overlay = TxnOverlay::new(DEFAULT_RESIDENT_LIMIT, DEFAULT_DIRTY_LIMIT);
        let node = || Arc::new(Node::new_leaf());

        overlay.insert_dirty(4, node());
        overlay.insert_dirty(4, node());
        assert_eq!(overlay.len(), 1);
        assert_eq!((overlay.dirty_count(), overlay.clean_count()), (1, 0));

        overlay.insert_clean(4, node());
        assert_eq!(overlay.len(), 1);
        assert_eq!((overlay.dirty_count(), overlay.clean_count()), (0, 1));

        overlay.insert_clean(4, node());
        assert_eq!(overlay.len(), 1);
        assert_eq!((overlay.dirty_count(), overlay.clean_count()), (0, 1));
    }

    /// Node-class write accounting covers exactly the pages `write_node`
    /// produces. A spilled large value writes dozens of value/indirect pages through
    /// `write_value_pages`/`write_physical_pages`; the counter must not see them, so the same
    /// transaction shape with an 8-byte and a 200 000-byte value must record the same count.
    #[test]
    fn node_write_counter_excludes_value_and_indirect_pages() {
        let count = |value: Vec<u8>| {
            let dir = tempfile::TempDir::new().unwrap();
            let tree = BTree::open(dir.path().join("counter.db")).unwrap();
            tree.new_bucket("b", false).unwrap();
            let before = tree.store.node_io.writes().len();
            tree.exec("b", |txn| txn.put(b"k", &value)).unwrap();
            tree.store.node_io.writes()[before..].to_vec()
        };

        let small = count(vec![7u8; 8]);
        let large = count(vec![7u8; 200_000]);
        assert!(!small.is_empty(), "a put writes node pages");
        assert_eq!(
            small.len(),
            large.len(),
            "value/indirect pages must stay out of the node-class write counter"
        );
    }

    /// Node-class read accounting is recorded at the `read_node_pages` exit.
    /// A cold handle has no overlay and an empty cache, so resolving a bucket key must fetch
    /// exactly the node pages the writer published.
    #[test]
    fn node_read_counter_sees_the_pages_a_cold_read_fetches() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("cold-read.db");
        let writer = BTree::open(&path).unwrap();
        writer.new_bucket("b", false).unwrap();
        let before = writer.store.node_io.writes().len();
        writer.exec("b", |txn| txn.put(b"k", b"v")).unwrap();
        let published = writer.store.node_io.writes()[before..].to_vec();
        assert!(!published.is_empty());
        drop(writer);

        let reader = BTree::open(&path).unwrap();
        assert!(reader.store.node_io.reads().is_empty(), "nothing read yet");
        assert_eq!(
            reader.view("b", |view| view.get(b"k")).unwrap(),
            b"v".to_vec()
        );
        let reads = reader.store.node_io.reads();
        for pid in published {
            assert!(
                reads.contains(&pid),
                "the cold read must fetch published node page {pid}"
            );
        }
    }

    /// The diagnostic mapping must pass a page id through unchanged and never
    /// invent one: a buffer-shape failure that names no page reports absence.
    #[test]
    fn corruption_site_report_maps_fields_without_inventing_them() {
        let report = CorruptionSite::crc(7, "PAGE_CRC_MISMATCH", "check", 0xAA, 0xBB)
            .report(Some(3), "value");
        assert_eq!(report.pid, Some(7));
        assert_eq!(report.generation, Some(3));
        assert_eq!(report.page_kind, "value");
        assert_eq!(report.expected.as_deref(), Some("170"));
        assert_eq!(report.actual.as_deref(), Some("187"));

        let report =
            CorruptionSite::structure(9, "INVALID_LIVE_NODE", "check").report(None, "node");
        assert_eq!(report.pid, Some(9));
        assert!(report.expected.is_none() && report.actual.is_none());

        let report = CorruptionSite::buffer_shape("PAGE_ARRAY_LENGTH", None, "check")
            .report(Some(1), "page");
        assert_eq!(report.pid, None, "a page-less check must not name page 0");
    }

    #[test]
    fn reader_refresh_does_not_adopt_unpublished_cached_meta() {
        let dir = tempfile::TempDir::new().unwrap();
        let tree = BTree::open(dir.path().join("tree.db")).unwrap();
        tree.new_bucket("bucket", false).unwrap();

        let published_seq = tree.store.shared_snapshot().0;
        tree.start_seq
            .store(published_seq.saturating_sub(1), Ordering::Release);
        tree.store.forge_cached_seq_for_test(published_seq + 1);

        tree.view("bucket", |_| Ok(())).unwrap();

        assert_eq!(
            tree.start_seq.load(Ordering::Acquire),
            published_seq,
            "a reader must not adopt the writer's unpublished cached sequence"
        );
    }

    #[test]
    fn stale_reader_snapshot_cannot_regress_handle_generation() {
        let dir = tempfile::TempDir::new().unwrap();
        let tree = BTree::open(dir.path().join("tree.db")).unwrap();
        tree.new_bucket("bucket", false).unwrap();

        let published = *tree.local_snapshot.read();
        let mut newer = published;
        newer.seq += 1;
        tree.apply_handle_snapshot(newer);
        tree.apply_handle_snapshot(published);

        assert_eq!(tree.start_seq.load(Ordering::Acquire), newer.seq);
        assert_eq!(*tree.local_snapshot.read(), newer);
    }

    /// `Clone` shares one `Arc<BTreeRuntime>`, hence one `NodeCache`: an entry the
    /// original handle loaded must be visible through the clone, and an invalidation
    /// issued on either handle must reach the other's next `load_node`.
    #[test]
    fn cloned_btree_handles_share_runtime_node_cache() {
        let dir = tempfile::TempDir::new().unwrap();
        let tree = BTree::open(dir.path().join("shared-runtime-cache.db")).unwrap();
        let mut alloc = HashSet::new();
        let pid = tree.runtime.alloc_data_page(&mut alloc).unwrap();
        let mut node = Node::new_leaf();
        tree.store.write_physical_page(pid, node.finalize_mut());

        tree.runtime.clear_cache();
        let clone = tree.clone();
        assert_eq!(
            clone.runtime.cached_node_is_leaf(pid),
            None,
            "the cache starts empty"
        );

        tree.runtime.load_node(pid);
        assert_eq!(
            clone.runtime.cached_node_is_leaf(pid),
            Some(true),
            "a clone must observe the entry the original handle loaded"
        );

        // The observer is reached through the shared runtime, so the clone's next
        // read must not resurrect the node the original handle retired.
        tree.runtime.free_pages(pid.get(), 1).unwrap();
        assert_eq!(
            clone.runtime.cached_node_is_leaf(pid),
            None,
            "freeing on one handle must invalidate the entry on the other"
        );
    }

    /// The production `PageReuseObserver` is the `NodeCache`, and it is reached only from
    /// `alloc_page_inner` and `free_pages_observed`. A PID handed out again after a free
    /// names a *different* page, so a surviving cache entry would alias the previous
    /// incarnation's node. The re-allocation takes the reusable-extent early return,
    /// the branch that has no other reason to touch the observer.
    #[test]
    fn runtime_allocator_invalidates_reused_page_ids() {
        let dir = tempfile::TempDir::new().unwrap();
        let tree = BTree::open(dir.path().join("runtime-cache-reuse.db")).unwrap();
        let mut alloc = HashSet::new();
        let pid = tree.runtime.alloc_data_page(&mut alloc).unwrap();
        let mut node = Node::new_leaf();
        tree.store.write_physical_page(pid, node.finalize_mut());

        tree.runtime.load_node(pid);
        assert_eq!(
            tree.runtime.cached_node_is_leaf(pid),
            Some(true),
            "the load must leave the page resident"
        );

        tree.runtime.free_pages(pid.get(), 1).unwrap();
        assert_eq!(
            tree.runtime.cached_node_is_leaf(pid),
            None,
            "freeing a PID must invalidate its cache entry"
        );

        // Re-load before the hand-out: the free above already cleared the entry, so
        // without a fresh residency only the free's invalidation is under test. Now
        // the re-allocation is the only thing that can drop the entry, and it takes
        // the reusable-extent early return in `alloc_page_inner`.
        tree.runtime.load_node(pid);
        assert_eq!(tree.runtime.cached_node_is_leaf(pid), Some(true));

        let mut reallocated = HashSet::new();
        assert_eq!(
            tree.runtime.alloc_data_page(&mut reallocated).unwrap(),
            pid,
            "the freed PID must come back out of the reusable set"
        );
        assert_eq!(
            tree.runtime.cached_node_is_leaf(pid),
            None,
            "a re-handed-out PID must not resolve to the previous incarnation's node"
        );
    }

    fn alloc_and_write_node(store: &Store, node: &mut Node) -> DataPid {
        let mut alloc = HashSet::new();
        let pid = store.alloc_data_page(&mut alloc).unwrap();
        store.write_physical_page(pid, node.finalize_mut());
        pid
    }

    struct TestTree {
        read: TreeReadContext,
        root: RootRef,
        page_state: TxnPageState,
    }

    impl TestTree {
        fn new(store: Arc<Store>, root: RootRef) -> Self {
            Self {
                read: TreeReadContext::new(test_runtime(store)),
                root,
                page_state: TxnPageState::default(),
            }
        }

        fn put(&mut self, key: &[u8], value: &[u8]) -> StoreResult<()> {
            let root = self.root;
            self.root = self.page_state.run(&self.read, |ctx| {
                Tree::put(&self.read, ctx, root, key, value)
            })?;
            Ok(())
        }

        fn del(&mut self, key: &[u8]) -> StoreResult<bool> {
            let root = self.root;
            let (deleted, new_root) = self
                .page_state
                .run(&self.read, |ctx| Tree::del(&self.read, ctx, root, key))?;
            if deleted {
                self.root = new_root;
            }
            Ok(deleted)
        }

        fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
            Tree::get(&self.read, self.root, key)
        }

        fn iterator(&self) -> TreeIterator<'_> {
            Tree::iterator(&self.read, self.root)
        }
    }

    fn padded_key(id: u32) -> Vec<u8> {
        let mut key = format!("{id:03}").into_bytes();
        key.resize(MAX_KEY_LEN, b'k');
        key
    }

    fn padded_value(id: u32) -> Vec<u8> {
        let mut value = format!("value-{id:03}").into_bytes();
        value.resize(128, b'v');
        value
    }

    fn build_leaf(store: &Arc<Store>, keys: &[u32]) -> Node {
        let mut leaf = Node::new_leaf();
        let read = TreeReadContext::new(test_runtime(store.clone()));
        let mut page_state = TxnPageState::default();
        page_state
            .run(&read, |ctx| {
                for &key in keys {
                    leaf.put_leaf(ctx, &padded_key(key), &padded_value(key))?;
                }
                Ok(())
            })
            .unwrap();
        assert!(page_state.pending_free.is_empty());
        assert!(page_state.pending_alloc.is_empty());
        leaf
    }

    fn collect_tree(tree: &TestTree) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut iter = tree.iterator();
        let mut key_buf = Vec::new();
        let mut val_buf = Vec::new();
        let mut entries = Vec::new();
        while iter.next_ref(&mut key_buf, &mut val_buf) {
            entries.push((key_buf.clone(), val_buf.clone()));
        }
        entries
    }

    #[test]
    fn txn_page_state_batches_adjacent_frees_into_one_extent() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = test_store(&dir);
        let read = TreeReadContext::new(test_runtime(store));
        let mut state = TxnPageState::default();

        state.merge_pending(
            &read,
            vec![(12, 1), (15, 1), (13, 1), (14, 1)],
            HashSet::new(),
        );

        assert_eq!(state.pending_free, vec![(12, 4)]);
    }

    #[test]
    fn txn_page_state_savepoint_commit_batches_remaining_pages() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = test_store(&dir);
        let read = TreeReadContext::new(test_runtime(store));
        let mut state = TxnPageState {
            pending_alloc: HashSet::from([20]),
            ..TxnPageState::default()
        };
        state.begin_savepoint();
        state.savepoint.as_mut().unwrap().pending_free = vec![(20, 1), (23, 1), (21, 1), (22, 1)];

        state.commit_savepoint(&read);

        assert!(state.pending_alloc.is_empty());
        assert_eq!(state.pending_free, vec![(21, 3)]);
    }

    #[test]
    fn iterator_initializes_only_the_requested_direction() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = test_store(&dir);
        let mut tree = TestTree::new(store, RootRef::Empty);
        for key in 0..200u32 {
            tree.put(&padded_key(key), b"value").unwrap();
        }

        let mut forward = tree.iterator();
        assert!(forward.root_node.is_none());
        assert!(!forward.forward_initialized);
        assert!(!forward.reverse_initialized);
        assert!(forward.stack.is_empty());
        assert!(forward.reverse_stack.is_empty());

        let mut key_buf = Vec::new();
        let mut val_buf = Vec::new();
        assert!(forward.next_ref(&mut key_buf, &mut val_buf));
        assert!(forward.forward_initialized);
        assert!(!forward.stack.is_empty());
        assert!(!forward.reverse_initialized);
        assert!(forward.reverse_stack.is_empty());

        let mut reverse = tree.iterator();
        assert!(reverse.prev_ref(&mut key_buf, &mut val_buf));
        assert!(!reverse.forward_initialized);
        assert!(reverse.stack.is_empty());
        assert!(reverse.reverse_initialized);
        assert!(!reverse.reverse_stack.is_empty());
    }

    fn collect_physical_tree_pages(
        read: &TreeReadContext,
        root: RootRef,
        reachable: &mut HashSet<PageId>,
    ) {
        let Some(root) = root.node() else {
            return;
        };
        let mut stack = vec![root];
        while let Some(id) = stack.pop() {
            assert!(reachable.insert(id.get()), "duplicate physical tree PID");
            let node = read.load_node(id);
            for index in 0..node.num_children() {
                if node.is_leaf() {
                    for storage_id in node.collect_slot_storage_ids(read, node.slot_at(index)) {
                        assert!(
                            reachable.insert(storage_id.get()),
                            "duplicate physical value PID"
                        );
                    }
                } else {
                    stack.push(node.child_at(index));
                }
            }
        }
    }

    /// A snapshot must not leak pages: opened as its own store, every pid in its
    /// `[2, next_page_id)` range has to be exactly one of {reachable from the
    /// catalog, reusable, retired, allocator list page}. The fixture churns to
    /// leave real free space (deleted keys) and retired pages behind, so a
    /// snapshot that dropped or mis-copied allocator state would leave untracked
    /// pids that no later allocation can ever reach.
    #[test]
    fn snapshot_has_no_unaccounted_pages() {
        let dir = tempfile::TempDir::new().unwrap();
        let source = dir.path().join("accounting-source.db");
        let dst = dir.path().join("accounting-snapshot.db");
        let tree = BTree::open(&source).unwrap();
        tree.new_bucket("data", false).unwrap();
        tree.exec("data", |txn| {
            for index in 0..600u32 {
                txn.put(format!("k{index:05}").into_bytes(), vec![0x5A; 2048])?;
            }
            Ok(())
        })
        .unwrap();
        tree.exec("data", |txn| {
            for index in 0..500u32 {
                txn.del(format!("k{index:05}").as_bytes())?;
            }
            Ok(())
        })
        .unwrap();
        let source_free_pages = tree.store.free_page_count_for_test();
        let snapshot = tree.take_snapshot(&dst).unwrap();

        let copy = BTree::open(&dst).unwrap();
        assert_eq!(copy.current_seq(), snapshot.seq);
        assert_nonempty_generation_page_accounting(&copy);

        assert_eq!(
            copy.store.free_page_count_for_test(),
            source_free_pages,
            "the snapshot must carry the frozen generation's free space"
        );
    }

    /// The deferred-commit path — deferred writes flushed at commit,
    /// plus the dense-coverage extension the publication depends on — must survive an I/O cut at
    /// *every* point with the base generation intact, nothing left behind by the uncommitted
    /// transaction, no leaked page, and a further commit still possible. The injected cut is the
    /// deterministic stand-in for a kill between the same two writes (its durable-state coverage).
    #[test]
    fn deferred_commit_io_faults_leave_the_base_generation_intact() {
        let dir = tempfile::TempDir::new().unwrap();
        let baseline = dir.path().join("baseline.db");
        {
            let mut options = OpenOptions::new();
            options.sync_mode = SyncMode::All;
            let tree = options.open(&baseline).unwrap();
            tree.new_bucket("bucket", false).unwrap();
            tree.exec("bucket", |txn| txn.put(b"stable", b"stable-value"))
                .unwrap();
        }

        let mut cuts = 0usize;
        for (operation, max_occurrence) in [("pwrite", 48usize), ("sync_data", 8), ("sync_all", 8)]
        {
            let mut saw_failure = false;
            let mut reached_clean_run = false;
            for occurrence in 1..=max_occurrence {
                let path = dir.path().join(format!("{operation}-{occurrence}.db"));
                std::fs::copy(&baseline, &path).unwrap();
                let output = child_test_command(&std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "tests::deferred_commit_fault_child",
                        "--ignored",
                        "--nocapture",
                    ])
                    .env(DEFERRED_FAULT_CHILD_PATH, &path)
                    .env(
                        crate::store::TEST_LIVE_FAULT_ENV,
                        format!("{operation}:{occurrence}:5"),
                    )
                    .env("LSAN_OPTIONS", "detect_leaks=0")
                    .output()
                    .unwrap();
                if output.status.success() {
                    assert!(
                        saw_failure,
                        "{operation} had no injectable cut in the deferred-commit path"
                    );
                    reached_clean_run = true;
                    break;
                }
                saw_failure = true;
                cuts += 1;
                let stderr = String::from_utf8_lossy(&output.stderr);
                assert!(
                    stderr.contains("code=BTREE_FATAL_IO")
                        && stderr.contains(&format!("operation={operation}")),
                    "{stderr}"
                );
                assert_deferred_cut_left_base_generation(&path);
            }
            // Falling out of the loop means every occurrence in the range still
            // failed: no clean run was reached, so the range stopped covering the
            // operation rather than the operation running out of injectable cuts.
            assert!(
                reached_clean_run,
                "{operation} still failed after {max_occurrence} occurrences"
            );
        }
        assert!(cuts > 0, "the sweep must have exercised at least one cut");
    }

    /// The child of `deferred_commit_io_faults_leave_the_base_generation_intact`: it allocates and
    /// releases pages without publishing (leaving the id space ahead of the file, so the next
    /// publication has to cover it), then commits a deferred transaction.
    #[test]
    #[ignore = "spawned by deferred_commit_io_faults_leave_the_base_generation_intact"]
    fn deferred_commit_fault_child() {
        let path = std::env::var(DEFERRED_FAULT_CHILD_PATH).expect("child path");
        let tree = BTree::open(&path).unwrap();
        let failed = tree.exec("bucket", |txn| -> Result<()> {
            // A spilled value: dozens of allocations, all released by the rollback below.
            txn.put(b"ghost", vec![0x7E; 300_000])?;
            Err(Error::KeyNotFound)
        });
        assert!(failed.is_err(), "the ghost transaction must roll back");

        tree.set_dirty_limit(4096);
        tree.exec("bucket", |txn| {
            for index in 0..64u32 {
                txn.put(format!("new{index:03}").as_bytes(), b"value")?;
            }
            Ok(())
        })
        .unwrap();
        tree.set_dirty_limit(DEFAULT_DIRTY_LIMIT);
    }

    /// After a fatal cut the database must reopen with the baseline content and no unaccounted page;
    /// the interrupted transaction is all-or-nothing (the cut may land after the publication, in
    /// which case its whole work is durable), and the database must still accept a commit.
    fn assert_deferred_cut_left_base_generation(path: &Path) {
        let tree = BTree::open(path).unwrap();
        assert_eq!(
            tree.view("bucket", |view| view.get(b"stable")).unwrap(),
            b"stable-value".to_vec(),
            "the base generation must survive an I/O cut"
        );
        // Rolled back before any publication, so it may never be visible.
        assert!(
            matches!(
                tree.view("bucket", |view| view.get(b"ghost")),
                Err(Error::KeyNotFound)
            ),
            "a rolled-back transaction may not leave a key behind"
        );
        let mut committed = 0usize;
        for index in 0..64u32 {
            if let Ok(value) = tree.view("bucket", |view| {
                view.get(format!("new{index:03}").as_bytes())
            }) {
                assert_eq!(value, b"value".to_vec(), "a committed key keeps its value");
                committed += 1;
            }
        }
        assert!(
            committed == 0 || committed == 64,
            "the interrupted transaction must be all-or-nothing, saw {committed}/64 keys"
        );
        assert_nonempty_generation_page_accounting(&tree);
        tree.exec("bucket", |txn| txn.put(b"continued", b"ok"))
            .unwrap();
        drop(tree);

        let reopened = BTree::open(path).unwrap();
        assert_eq!(
            reopened
                .view("bucket", |view| view.get(b"continued"))
                .unwrap(),
            b"ok".to_vec(),
            "a commit after the cut must be durable"
        );
    }

    /// A transaction that fails (`exec` rollback and a per-bucket failure
    /// inside `exec_multi`) must leave nothing behind — the content equals the pre-failure state
    /// after a reopen, and only the failing bucket's work is dropped.
    #[test]
    fn failed_transactions_leave_no_residue_after_reopen() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("residue.db");
        let tree = BTree::open(&path).unwrap();
        tree.new_bucket("a", false).unwrap();
        tree.new_bucket("b", false).unwrap();
        tree.exec("a", |txn| txn.put(b"keep", b"a-value")).unwrap();
        tree.exec("b", |txn| txn.put(b"keep", b"b-value")).unwrap();

        let failed = tree.exec("a", |txn| -> Result<()> {
            txn.put(b"rolled-back", b"value")?;
            Err(Error::KeyNotFound)
        });
        assert!(failed.is_err());

        let failed = tree.exec_multi(|multi| -> Result<()> {
            multi.exec("a", |txn| txn.put(b"committed", b"a2"))?;
            multi.exec("b", |txn| -> Result<()> {
                txn.put(b"dropped", b"b2")?;
                Err(Error::KeyNotFound)
            })?;
            Ok(())
        });
        assert!(failed.is_err());

        // (3) A `del_bucket` that fails (no such bucket) must leave the database untouched.
        assert!(
            tree.del_bucket("missing").is_err(),
            "deleting a missing bucket fails"
        );

        drop(tree);

        let reopened = BTree::open(&path).unwrap();
        assert_eq!(
            reopened.view("a", |view| view.get(b"keep")).unwrap(),
            b"a-value".to_vec()
        );
        assert_eq!(
            reopened.view("b", |view| view.get(b"keep")).unwrap(),
            b"b-value".to_vec()
        );
        assert!(
            matches!(
                reopened.view("a", |view| view.get(b"rolled-back")),
                Err(Error::KeyNotFound)
            ),
            "a rolled-back transaction must leave no key behind"
        );
        // survive: (no failure path may publish).
        for (bucket, key) in [("a", b"committed".as_slice()), ("b", b"dropped")] {
            assert!(
                matches!(
                    reopened.view(bucket, |view| view.get(key)),
                    Err(Error::KeyNotFound)
                ),
                "{bucket}/{key:?} must not survive a failed `exec_multi`"
            );
        }
        assert_nonempty_generation_page_accounting(&reopened);
    }

    /// A bucket-metadata record that is shorter than the record struct must end in
    /// the engine's corruption diagnostic, not in a panic: it is reachable from a
    /// read-only handle that only walks buckets.
    #[test]
    fn bucket_metadata_short_record_aborts() {
        let output = child_test_command(&std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tests::bucket_metadata_short_record_child",
                "--ignored",
                "--nocapture",
            ])
            .env("BTREE_SHORT_RECORD_CHILD", "1")
            .output()
            .unwrap();
        let combined = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            !output.status.success(),
            "a short record must not be served: {combined}"
        );
        assert!(
            combined.contains("btree-store fatal code=BTREE_FATAL_"),
            "the failure must carry the engine diagnostic: {combined}"
        );
        assert!(
            !combined.contains("panicked at"),
            "the failure must not be a bare panic: {combined}"
        );
    }

    #[test]
    #[ignore = "subprocess target for the short bucket-metadata record"]
    fn bucket_metadata_short_record_child() {
        if std::env::var("BTREE_SHORT_RECORD_CHILD").is_err() {
            return;
        }
        let _ = physical_value(
            BucketMetadata::decode(&[0u8; 4]),
            "bucket metadata record within slot value",
        );
        println!("short record was accepted");
    }

    fn assert_nonempty_generation_page_accounting(tree: &BTree) {
        let mut reachable_pids = HashSet::new();
        let read = &tree.read;
        let catalog_root = RootRef::decode(tree.store.cached_snapshot().catalog_root);

        collect_physical_tree_pages(read, catalog_root, &mut reachable_pids);

        let mut catalog = Tree::iterator(read, catalog_root);
        let mut key = Vec::new();
        let mut value = Vec::new();
        while catalog.next_ref(&mut key, &mut value) {
            collect_physical_tree_pages(
                read,
                BucketMetadata::decode(&value)
                    .expect("fixture record")
                    .root(),
                &mut reachable_pids,
            );
        }
        tree.store.assert_complete_page_ownership(&reachable_pids);
    }

    #[test]
    fn prefix_encoding_establishes_shared_prefix_from_first_insert() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("prefix-from-first.db");
        let tree = BTree::open(&path).unwrap();
        tree.new_bucket("encoded", true).unwrap();

        tree.exec("encoded", |txn| txn.put(b"user/001", b"v"))
            .unwrap();
        let single = leaf_prefix_len(&tree, "encoded");
        assert_eq!(
            single,
            b"user/001".len(),
            "a single-key leaf must store the key as its prefix"
        );

        tree.exec("encoded", |txn| txn.put(b"user/002", b"v"))
            .unwrap();
        let two = leaf_prefix_len(&tree, "encoded");
        assert_eq!(
            two,
            b"user/00".len(),
            "two shared-prefix keys must compress to the common prefix"
        );

        tree.view("encoded", |txn| {
            let mut iter = txn.iter();
            let mut key = Vec::new();
            let mut value = Vec::new();
            let mut keys = Vec::new();
            while iter.next_ref(&mut key, &mut value) {
                keys.push(key.clone());
            }
            assert_eq!(keys, vec![b"user/001".to_vec(), b"user/002".to_vec()]);
            Ok::<_, Error>(())
        })
        .unwrap();
    }

    fn leaf_prefix_len(tree: &BTree, bucket: &str) -> usize {
        let read = &tree.read;
        let catalog_root = RootRef::decode(tree.store.cached_snapshot().catalog_root);
        let (catalog_leaf, pos) = Tree::find(read, catalog_root, bucket.as_bytes()).unwrap();
        let bucket_root = BucketMetadata::decode(catalog_leaf.value_at(pos))
            .expect("fixture record")
            .root();
        let node = read.load_node(bucket_root.node().expect("bucket root"));
        assert!(node.is_leaf(), "a two-entry bucket root must be a leaf");
        node.test_encoded_prefix_len()
    }

    #[test]
    fn prefix_encoding_cross_prefix_splits_keep_page_ownership_exact() {
        // overflow values repeatedly hit the rebuild-or-split path. Before the
        // fix this path allocated the overflow pages before checking whether
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("encoded-cross-prefix-ownership.db");
        let tree = BTree::open(&path).unwrap();
        tree.new_bucket("encoded", true).unwrap();

        // cross-prefix overflow insert whose rebuild cannot fit must split
        // without leaking the freshly allocated overflow page.
        for i in 0..24u32 {
            let key = format!("user/{i:03}");
            tree.exec("encoded", |txn| txn.put(key.as_bytes(), vec![i as u8; 128]))
                .unwrap();
        }
        for i in 0..30u32 {
            let key = format!("admin/{i:04}");
            tree.exec("encoded", |txn| {
                txn.put(key.as_bytes(), vec![(i as u8).wrapping_mul(7); 300])
            })
            .unwrap();
        }
        assert_nonempty_generation_page_accounting(&tree);

        drop(tree);
        let reopened = BTree::open(&path).unwrap();
        assert_nonempty_generation_page_accounting(&reopened);
    }

    #[test]
    fn prefix_encoding_cross_prefix_split_preserves_uniform_leaf_depth() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("encoded-uniform-leaf-depth.db");
        let tree = BTree::open(&path).unwrap();
        tree.new_bucket("encoded", true).unwrap();

        let prefix = "u".repeat(120);
        for i in 0..300u32 {
            let key = format!("{prefix}/{i:04}");
            tree.exec("encoded", |txn| txn.put(key.as_bytes(), b"v"))
                .unwrap();
        }
        tree.exec("encoded", |txn| txn.put(b"aaa", b"v")).unwrap();

        let catalog_root = RootRef::decode(tree.store.cached_snapshot().catalog_root);
        let (catalog_leaf, pos) = Tree::find(&tree.read, catalog_root, b"encoded").unwrap();
        let bucket_root = BucketMetadata::decode(catalog_leaf.value_at(pos))
            .expect("fixture record")
            .root();
        assert!(tree_height(&tree.read, bucket_root) >= 2);
    }

    #[test]
    fn nonempty_generation_has_exact_page_ownership_after_reopen_and_continued_commit() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("nonempty-page-accounting.db");
        {
            let tree = BTree::open(&path).unwrap();
            tree.new_bucket("alpha", false).unwrap();
            tree.new_bucket("beta", false).unwrap();
            tree.exec("alpha", |txn| {
                for key in 0..160u32 {
                    txn.put(key.to_be_bytes(), vec![key as u8; 512])?;
                }
                txn.put(b"indirect", vec![0x5a; 30_000])?;
                Ok::<_, Error>(())
            })
            .unwrap();
            tree.exec("beta", |txn| {
                txn.put(b"overflow", vec![0x33; 8192])?;
                Ok::<_, Error>(())
            })
            .unwrap();
            tree.exec("alpha", |txn| {
                for key in 0..80u32 {
                    txn.del(key.to_be_bytes())?;
                }
                Ok::<_, Error>(())
            })
            .unwrap();
            assert_nonempty_generation_page_accounting(&tree);
        }

        let tree = BTree::open(&path).unwrap();
        assert_nonempty_generation_page_accounting(&tree);
        tree.exec("beta", |txn| txn.put(b"continued", b"write"))
            .unwrap();
        assert_nonempty_generation_page_accounting(&tree);
    }

    fn assert_tree_matches(tree: &TestTree, expected: &BTreeMap<Vec<u8>, Vec<u8>>) {
        let actual = collect_tree(tree);
        let expected_entries: Vec<(Vec<u8>, Vec<u8>)> = expected
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        assert_eq!(actual, expected_entries);

        for key in expected.keys() {
            assert_eq!(tree.get(key), Some(expected[key].clone()));
        }
    }

    fn tree_height(read: &TreeReadContext, root: RootRef) -> usize {
        let Some(root_id) = root.node() else {
            return 0;
        };
        tree_height_from_node(read, root_id)
    }

    fn tree_height_from_node(read: &TreeReadContext, id: DataPid) -> usize {
        let node = read.load_node(id);
        if node.is_leaf() {
            return 1;
        }

        let child_heights: Vec<_> = (0..node.num_children())
            .map(|idx| tree_height_from_node(read, node.child_at(idx)))
            .collect();
        assert!(child_heights.iter().all(|h| *h == child_heights[0]));
        1 + child_heights[0]
    }

    #[test]
    fn branch_rewrite_replaces_expected_edge() {
        let physical = |raw| DataPid::new(raw).unwrap();
        let mut branch = Node::new_branch_root(
            physical(10),
            NonEmptyKey::new(b"sep".to_vec()).unwrap(),
            physical(20),
        );
        let res = branch.apply_branch_split_rewrite(
            ChildPos::new(0),
            physical(10),
            physical(11),
            NonEmptyKey::new(b"sep".to_vec()).unwrap(),
            physical(12),
        );
        assert!(matches!(res, BranchRewrite::Applied));
        assert_eq!(branch.child_at(0), physical(11));
        assert_eq!(branch.child_at(1), physical(12));
    }

    #[test]
    fn non_empty_branch_separator_rejects_empty_key() {
        assert!(NonEmptyKey::new(Vec::new()).is_none());
    }

    #[test]
    fn failed_transaction_publishes_all_consumed_meta_allocators() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("failed-transaction-meta.db");
        let tree = BTree::open(&path).unwrap();
        tree.new_bucket("data", false).unwrap();
        let before = tree.store.cached_snapshot();

        let result: std::result::Result<(), Error> = tree.exec("data", |txn| {
            txn.put(b"aborted", vec![0x6a; 2 * PAGE_SIZE])?;
            Err(Error::KeyNotFound)
        });
        assert_eq!(result, Err(Error::KeyNotFound));

        let after = tree.store.cached_snapshot();
        assert!(after.seq > before.seq);
        assert!(after.next_page_id > before.next_page_id);
        drop(tree);

        let reopened = BTree::open(&path).unwrap();
        assert_eq!(reopened.store.cached_snapshot(), after);
        assert_eq!(
            reopened.view("data", |txn| txn.get(b"aborted")),
            Err(Error::KeyNotFound)
        );
    }

    #[test]
    fn failed_exec_multi_publishes_complete_meta_snapshot() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("failed-exec-multi-meta.db");
        let tree = BTree::open(&path).unwrap();
        tree.new_bucket("data", false).unwrap();
        let before = tree.store.cached_snapshot();

        let result: std::result::Result<(), Error> = tree.exec_multi(|multi| {
            multi.exec("data", |txn| {
                txn.put(b"aborted", vec![0x4d; 2 * PAGE_SIZE])?;
                Err::<(), _>(Error::KeyNotFound)
            })?;
            Ok(())
        });
        assert_eq!(result, Err(Error::KeyNotFound));

        let after = tree.store.cached_snapshot();
        assert!(after.seq > before.seq);
        assert!(after.next_page_id > before.next_page_id);
        assert_ne!(after.reusable_root, before.reusable_root);
        drop(tree);

        let reopened = BTree::open(&path).unwrap();
        assert_eq!(reopened.store.cached_snapshot(), after);
        assert_eq!(
            reopened.view("data", |txn| txn.get(b"aborted")),
            Err(Error::KeyNotFound)
        );
    }

    #[test]
    fn nested_failed_exec_publishes_meta_before_outer_success() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("nested-failed-exec-meta.db");
        let tree = BTree::open(&path).unwrap();
        tree.new_bucket("kept", false).unwrap();
        tree.new_bucket("aborted", false).unwrap();
        let before = tree.store.cached_snapshot();
        // The nested publication keeps deferred outer allocations in durable
        // retired ownership until the outer roots are published.

        tree.exec_multi(|multi| {
            multi.exec("kept", |txn| txn.put(b"before", vec![0x31; 2 * PAGE_SIZE]))?;

            let aborted = multi.exec("aborted", |txn| {
                txn.put(b"large", vec![0x7a; 2 * PAGE_SIZE])?;
                Err::<(), _>(Error::KeyNotFound)
            });
            assert_eq!(aborted, Err(Error::KeyNotFound));

            let during = tree.store.cached_snapshot();
            assert!(during.seq > before.seq);
            assert!(during.next_page_id > before.next_page_id);

            multi.exec("kept", |txn| txn.put(b"after", b"continued"))?;
            Ok::<_, Error>(())
        })
        .unwrap();

        let after = tree.store.cached_snapshot();
        assert!(after.seq > before.seq);
        assert!(
            after.next_page_id >= before.next_page_id,
            "MetaNode high-water state must not move backwards after nested rollback publication"
        );
        drop(tree);

        let reopened = BTree::open(&path).unwrap();
        assert_eq!(reopened.store.cached_snapshot(), after);
        reopened
            .view("kept", |txn| {
                assert_eq!(txn.get(b"before")?, vec![0x31; 2 * PAGE_SIZE]);
                assert_eq!(txn.get(b"after")?, b"continued");
                Ok::<_, Error>(())
            })
            .unwrap();
        assert_eq!(
            reopened.view("aborted", |txn| txn.get(b"large")),
            Err(Error::KeyNotFound)
        );
        assert_nonempty_generation_page_accounting(&reopened);
    }

    #[test]
    fn nested_failed_exec_then_outer_abort_keeps_deferred_pages_owned() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("nested-failed-outer-abort-meta.db");
        let tree = BTree::open(&path).unwrap();
        tree.new_bucket("kept", false).unwrap();
        tree.new_bucket("aborted", false).unwrap();
        tree.new_bucket("continued", false).unwrap();
        let before = tree.store.cached_snapshot();
        // An outer abort must release only its in-memory projection; the
        // published retired ownership remains recoverable after reopen.

        let result: std::result::Result<(), Error> = tree.exec_multi(|multi| {
            multi.exec("kept", |txn| txn.put(b"before", vec![0x31; 2 * PAGE_SIZE]))?;

            let aborted = multi.exec("aborted", |txn| {
                txn.put(b"large", vec![0x7a; 2 * PAGE_SIZE])?;
                Err::<(), _>(Error::KeyNotFound)
            });
            assert_eq!(aborted, Err(Error::KeyNotFound));
            assert!(tree.store.cached_snapshot().seq > before.seq);

            Err(Error::KeyNotFound)
        });
        assert_eq!(result, Err(Error::KeyNotFound));

        let after = tree.store.cached_snapshot();
        assert!(after.seq > before.seq);
        drop(tree);

        let reopened = BTree::open(&path).unwrap();
        assert_eq!(reopened.store.cached_snapshot(), after);
        assert_eq!(
            reopened.view("kept", |txn| txn.get(b"before")),
            Err(Error::KeyNotFound)
        );
        assert_eq!(
            reopened.view("aborted", |txn| txn.get(b"large")),
            Err(Error::KeyNotFound)
        );
        assert_nonempty_generation_page_accounting(&reopened);
        reopened
            .exec("continued", |txn| {
                txn.put(b"key", vec![0x42; 2 * PAGE_SIZE])
            })
            .unwrap();
        assert_nonempty_generation_page_accounting(&reopened);
    }

    #[test]
    fn three_level_root_collapse_canonicalizes_historical_branch_before_duplicate_separator() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = test_store(&dir);

        let left_leaf_pid = alloc_and_write_node(&store, &mut build_leaf(&store, &[0, 1]));
        let right_left_leaf_pid = alloc_and_write_node(
            &store,
            &mut build_leaf(&store, &[91, 92, 93, 94, 95, 96, 97, 98]),
        );
        let right_right_leaf_pid = alloc_and_write_node(
            &store,
            &mut build_leaf(&store, &[99, 100, 101, 102, 103, 104, 105, 106]),
        );

        let mut left_branch = Node::new_branch_single(left_leaf_pid);
        let left_branch_pid = alloc_and_write_node(&store, &mut left_branch);

        let mut right_branch = Node::new_branch_root(
            right_left_leaf_pid,
            NonEmptyKey::new(padded_key(99)).unwrap(),
            right_right_leaf_pid,
        );
        let right_branch_pid = alloc_and_write_node(&store, &mut right_branch);

        let mut root = Node::new_branch_root(
            left_branch_pid,
            NonEmptyKey::new(padded_key(91)).unwrap(),
            right_branch_pid,
        );
        let root_pid = alloc_and_write_node(&store, &mut root);
        let mut tree = TestTree::new(store.clone(), RootRef::Node(root_pid));

        let mut expected = BTreeMap::new();
        for key in [
            0, 1, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106,
        ] {
            expected.insert(padded_key(key), padded_value(key));
        }

        assert_eq!(tree_height(&tree.read, tree.root), 3);
        let right_branch = tree.read.load_node(right_branch_pid);
        assert!(right_branch.stored_key_at(0).is_empty());

        for key in [0, 1] {
            assert!(tree.del(&padded_key(key)).unwrap());
            expected.remove(padded_key(key).as_slice());
        }

        let collapsed_root_ref = tree.root;
        assert_eq!(tree_height(&tree.read, collapsed_root_ref), 2);
        let collapsed_root = tree.read.load_node(collapsed_root_ref.node().unwrap());
        assert!(!collapsed_root.is_leaf());
        assert!(collapsed_root.stored_key_at(0).is_empty());

        for key in [84, 85, 86, 87, 88, 89, 90, 83] {
            tree.put(&padded_key(key), &padded_value(key)).unwrap();
            expected.insert(padded_key(key), padded_value(key));
        }

        assert_tree_matches(&tree, &expected);
        for missing in [0, 1, 2, 82] {
            assert_eq!(tree.get(&padded_key(missing)), None);
        }
    }

    #[test]
    fn multilevel_split_delete_root_collapse_oracle_matches_btreemap() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = test_store(&dir);
        let mut tree = TestTree::new(store.clone(), RootRef::Empty);
        let mut expected = BTreeMap::new();

        for key in 0..700u32 {
            let k = padded_key(key);
            let v = padded_value(key);
            tree.put(&k, &v).unwrap();
            expected.insert(k, v);
        }

        let mut max_height = tree_height(&tree.read, tree.root);
        assert!(
            max_height >= 3,
            "initial inserts must build a multi-level tree"
        );

        let mut saw_root_collapse = false;
        let mut prev_height = max_height;
        for key in 0..550u32 {
            assert!(tree.del(&padded_key(key)).unwrap());
            expected.remove(padded_key(key).as_slice());
            let height = tree_height(&tree.read, tree.root);
            if prev_height >= 3 && height < prev_height {
                saw_root_collapse = true;
            }
            prev_height = height;
            if key % 64 == 63 {
                assert_tree_matches(&tree, &expected);
            }
        }

        assert!(
            saw_root_collapse,
            "delete phase must trigger a root collapse"
        );

        let mut rng = StdRng::seed_from_u64(0xB1_B2_B3_B4);
        for step in 0..240u32 {
            let key = rng.random_range(0..900);
            if rng.random_range(0..100) < 60 {
                let k = padded_key(key);
                let v = padded_value(10_000 + step);
                tree.put(&k, &v).unwrap();
                expected.insert(k, v);
            } else {
                let k = padded_key(key);
                let res = tree.del(&k).unwrap();
                if expected.remove(k.as_slice()).is_some() {
                    assert!(res);
                } else {
                    assert!(!res);
                }
            }

            let height = tree_height(&tree.read, tree.root);
            max_height = max_height.max(height);
            if step % 24 == 23 {
                assert_tree_matches(&tree, &expected);
            }
        }

        assert!(max_height >= 3);
        assert_tree_matches(&tree, &expected);
    }

    #[test]
    fn bucket_metadata_uses_native_u32_root_and_flags() {
        let root = RootRef::Node(DataPid::new(17).unwrap());
        let metadata = BucketMetadata::new(root, 1);
        let mut expected = [0u8; 8];
        expected[..4].copy_from_slice(&17u32.to_ne_bytes());
        expected[4..].copy_from_slice(&1u32.to_ne_bytes());
        assert_eq!(metadata.as_slice(), &expected);
        assert_eq!(metadata.flags(), 1);

        let mut containing_record = [0xa5; 8];
        containing_record[..4].copy_from_slice(&23u32.to_ne_bytes());
        containing_record[4..].copy_from_slice(&1u32.to_ne_bytes());
        let parsed = BucketMetadata::decode(&containing_record).expect("fixture record");
        assert_eq!(parsed.root(), RootRef::Node(DataPid::new(23).unwrap()));
        assert_eq!(parsed.flags(), 1);
    }

    #[test]
    fn physical_root_encoding_reserves_meta_pages() {
        assert_eq!(DataPid::new(0), None);
        assert_eq!(DataPid::new(1), None);
        assert_eq!(RootRef::decode(0), RootRef::Empty);
        assert_eq!(RootRef::decode(2), RootRef::Node(DataPid::new(2).unwrap()));

        let packed = PendingPageCounts::encode(3, 5);
        assert_eq!(PendingPageCounts::decode(packed), (3, 5));
    }
}
