use std::{
    collections::{HashMap, HashSet},
    fmt,
    hash::Hasher,
    io::{self, Write},
    num::NonZeroU32,
    path::{Path, PathBuf},
    sync::{
        Arc, OnceLock, Weak,
        atomic::{AtomicU64, Ordering},
    },
};

use parking_lot::{Mutex, RwLock};

use crate::epoch::EpochGuard;

#[cfg(not(target_endian = "little"))]
compile_error!("btree-store requires a little-endian target");

pub(crate) mod cache;
pub(crate) mod epoch;
pub(crate) mod node;
pub(crate) mod store;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    KeyNotFound,
    BucketNotFound,
    BucketExists,
    InvalidKey(KeyError),
    InvalidBucket(BucketError),
    ValueTooLarge { len: usize, max: usize },
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

#[derive(Debug)]
pub enum OpenError {
    Io(OpenIoError),
    Corruption(CorruptionReport),
    InvalidOptions(OptionsError),
    DatabaseBusy { path: PathBuf },
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
pub const FORMAT_VERSION: u32 = 1;
pub use crate::node::{MAX_KEY_LEN, MAX_VAL_LEN};

/// Runtime sync policy used after commits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SyncMode {
    /// Sync data by default, but upgrade to a full sync when file size changes.
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
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            cache_capacity: 8192,
            sync_mode: SyncMode::Adaptive,
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
        // Torn write detection: treat an all-zero meta page as invalid.
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

/// Maps a persisted bucket flag word to the node-class layout used to build
/// new nodes for that bucket. bit0 = prefix encoding.
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
        self.cache.put(id.get(), node.clone());
        node
    }

    fn load_iterator_child(&self, id: DataPid) -> Arc<Node> {
        if let Some(node) = self.cache.get_branch(id.get()) {
            return node;
        }

        let node = self.store.read_node_without_cache(id);
        if !node.is_leaf() {
            self.cache.put(id.get(), node.clone());
        }
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
        }
    }

    /// Returns a clone that builds new nodes using `layout` (the catalog and
    /// the TxnCore base always stay plain; bucket trees override per bucket).
    pub(crate) fn with_layout(&self, layout: Layout) -> Self {
        Self {
            runtime: self.runtime.clone(),
            layout,
        }
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
        self.runtime.load_node(id)
    }

    fn load_iterator_child(&self, id: DataPid) -> Arc<Node> {
        self.runtime.load_iterator_child(id)
    }

    fn load_node_uncached(&self, id: DataPid) -> Arc<Node> {
        self.runtime.load_node_uncached(id)
    }

    fn alloc_data_page(&self, alloc: &mut HashSet<PageId>) -> StoreResult<DataPid> {
        self.runtime.alloc_data_page(alloc)
    }

    fn alloc_data_pages(
        &self,
        nr_pages: u32,
        alloc: &mut HashSet<PageId>,
    ) -> StoreResult<Vec<DataPid>> {
        self.runtime.alloc_data_pages(nr_pages, alloc)
    }

    fn recycle_allocated_pages(&self, page_id: PageId, nr_pages: u32) {
        self.runtime.recycle_allocated_pages(page_id, nr_pages);
    }

    fn free_pages(&self, page_id: PageId, nr_pages: u32) -> StoreResult<()> {
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

    fn write_node(&mut self, node: &mut Node) -> StoreResult<DataPid> {
        let pid = self.alloc_page()?;
        let data = node.finalize();
        self.read.store().write_page(pid, data);
        Ok(pid)
    }

    fn write_pages(&mut self, ids: &[DataPid], data: &[u8]) -> StoreResult<()> {
        let ids: Vec<PageId> = ids.iter().map(|id| id.get()).collect();
        self.read.store().write_data(&ids, data)
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

        // root is empty
        let Some(current_root_id) = current_root_id else {
            let mut node = read.new_leaf();
            node.put_leaf(ctx, key, value)?;
            return Ok(RootRef::Node(ctx.write_node(&mut node)?));
        };

        // 1. find target leaf node
        let root_node = read.load_node(current_root_id);
        let (mut stack, leaf_node_arc, leaf_id) =
            Self::traverse_to_leaf(read, root_node, current_root_id, key);

        let mut current_node = (*leaf_node_arc).clone();

        // 2. modify leaf node and get split info (if any)
        let mut split_info = Self::apply_insert(ctx, &mut current_node, key, value)?;

        // write new COW leaf node
        let mut new_child_id = ctx.write_node(&mut current_node)?;
        ctx.free_page(leaf_id);

        // 3. backtrack up the path, propagating changes and splits
        while let Some(Route {
            node: parent_arc,
            page_id: parent_id,
            pos,
        }) = stack.pop()
        {
            let mut parent = (*parent_arc).clone();

            if let Some((sep, mut rhs)) = split_info.take() {
                let expected_old = parent.child_at(pos);
                let rhs_id = ctx.write_node(&mut rhs)?;
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

            // write new COW parent node and prepare for the next level up
            new_child_id = ctx.write_node(&mut parent)?;
            ctx.free_page(parent_id);
        }

        // 4. handle root node split
        if let Some((sep, mut rhs)) = split_info {
            let rhs_id = ctx.write_node(&mut rhs)?;
            let mut new_root = read.new_branch_root(new_child_id, sep, rhs_id);
            Ok(RootRef::Node(ctx.write_node(&mut new_root)?))
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

        let mut new_child_id = ctx.write_node(&mut current_node)?;
        ctx.free_page(leaf_id);

        while let Some(Route {
            node: parent_arc,
            page_id: parent_id,
            pos,
        }) = stack.pop()
        {
            let mut parent = (*parent_arc).clone();
            parent.update_child_page(ChildPos::new(pos), new_child_id);
            new_child_id = ctx.write_node(&mut parent)?;
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
            LeafWrite::SplitRequired => {
                // The split routine includes the pending key when choosing
                // the pivot, so both returned children remain leaves at the
                // current level even when prefix re-encoding changes their
                // storage size.
                Some(node.split_leaf_for_insert(ctx, key, value)?)
            }
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

        // 1. find target leaf node
        let root_node = read.load_node(current_root_id);
        let (mut stack, leaf_arc, leaf_id) =
            Self::traverse_to_leaf(read, root_node, current_root_id, key);

        let mut current_node = (*leaf_arc).clone();
        if current_node.search(key).is_err() {
            return Ok((false, root));
        }
        current_node.delete_leaf_key(ctx, key);

        // 2. handle leaf node changes
        let mut empty = current_node.is_empty();
        let mut new_child_id = if !empty {
            Some(ctx.write_node(&mut current_node)?)
        } else {
            None
        };
        ctx.free_page(leaf_id);

        // 3. backtrack up the path, handling parent node updates or shrinks
        while let Some(Route {
            node: parent_arc,
            page_id: parent_id,
            pos,
        }) = stack.pop()
        {
            let mut parent = (*parent_arc).clone();

            if empty {
                // if child node became empty, remove corresponding slot from parent
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

            // check if current parent node also becomes empty
            if parent.is_empty() {
                empty = true;
                new_child_id = None;
            } else {
                empty = false;
                new_child_id = Some(ctx.write_node(&mut parent)?);
            }
            ctx.free_page(parent_id);
        }

        // 4. root collapse optimization
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
        // Both node classes keep branch slot 0 as the empty canonical sentinel,
        // so a zero `klen` is the class-independent short-circuit. Testing the
        // decoded full key instead would misjudge encoded branches (whose
        // sentinel reconstructs to the shared prefix, not ""), forcing a
        // byte-identical COW rewrite on every root collapse.
        if child.is_leaf() || child.is_empty() || child.slot_at(0).klen == 0 {
            return Ok(child_id);
        }

        let mut rewritten = (*child).clone();
        rewritten.canonicalize_branch_slot_zero();
        let new_child_id = ctx.write_node(&mut rewritten)?;
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

    pub(crate) fn iterator(
        read: &TreeReadContext,
        root: RootRef,
        mode: IteratorCacheMode,
    ) -> TreeIterator<'_> {
        TreeIterator::new(read.clone(), root, None, mode)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum IteratorCacheMode {
    #[default]
    Default,
    ByPass,
}

pub struct TreeIterator<'txn> {
    read: TreeReadContext,
    mode: IteratorCacheMode,
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
        match self.mode {
            IteratorCacheMode::Default => self.read.load_node(child_id),
            IteratorCacheMode::ByPass => self.read.load_iterator_child(child_id),
        }
    }

    fn new(
        read: TreeReadContext,
        root: RootRef,
        root_node: Option<Arc<Node>>,
        mode: IteratorCacheMode,
    ) -> Self {
        Self {
            read,
            mode,
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

        // The root is part of the hot path even for an uncached scan. Only
        // descendants use ByPass so a leaf root remains resident in NodeCache.
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
        Tree::iterator(&self.read, self.root, IteratorCacheMode::Default)
    }

    /// Returns an iterator over the current bucket without caching leaf nodes.
    ///
    /// Branch nodes may still be cached and reused so point reads and future
    /// traversals keep their hot upper-level path.
    pub fn iter_uncached(&self) -> TreeIterator<'_> {
        Tree::iterator(&self.read, self.root, IteratorCacheMode::ByPass)
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

    pub(crate) fn iterator(&self, mode: IteratorCacheMode) -> TreeIterator<'_> {
        let root_node = (mode == IteratorCacheMode::Default)
            .then(|| self.root_node.clone())
            .flatten();
        TreeIterator::new((*self.read).clone(), self.root, root_node, mode)
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
        self.tree.iterator(IteratorCacheMode::Default)
    }

    /// Returns an iterator over the read-only bucket snapshot without caching
    /// leaf nodes.
    ///
    /// The root may already have been loaded through the normal cache path when
    /// [`BTree::view`] validated the snapshot, and branch nodes may still be
    /// cached. Overflow and indirect pages are read directly because they do
    /// not have a page cache of their own.
    pub fn iter_uncached(&self) -> TreeIterator<'_> {
        self.tree.iterator(IteratorCacheMode::ByPass)
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
        Self {
            btree,
            read: btree.read.clone(),
            catalog_root: RootRef::decode(snapshot.catalog_root),
            page_state: TxnPageState::default(),
        }
    }

    fn checkpoint(&self) -> TxnCheckpoint {
        TxnCheckpoint {
            catalog_root: self.catalog_root,
            page_state: self.page_state.checkpoint(),
        }
    }

    fn rollback_to(&mut self, checkpoint: TxnCheckpoint) {
        self.catalog_root = checkpoint.catalog_root;
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
        let metadata = BucketMetadata::from_slice(leaf.value_at(pos));
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

    fn persist_nested_rollback_meta(&self, context: &'static str) {
        self.btree.persist_nested_rollback_meta(self, context);
    }
}

pub struct MultiTxn<'a> {
    core: TxnCore<'a>,
    bucket_roots: HashMap<String, MultiTxnBucketRoot>,
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

/// A persistent bucket record stored as the catalog value for a bucket name.
///
/// The record is a fixed 8-byte native little-endian pair: the physical root
/// page id followed by per-bucket flags. `flags & 1` selects prefix encoding
/// for the bucket's tree nodes.
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

    pub(crate) fn from_slice(x: &[u8]) -> Self {
        assert!(x.len() >= std::mem::size_of::<Self>());
        unsafe { std::ptr::read_unaligned(x.as_ptr().cast::<Self>()) }
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
        // Opening an already-live path updates only this handle's snapshot. It
        // may run from an active view callback while a writer owns a separate
        // writer-local TxnCore under the write lock.
        self.apply_handle_snapshot(self.store.cached_snapshot());
    }

    /// Open or create a btree database at the given path using default runtime options.
    ///
    /// This is equivalent to `BTree::open_with_options(path, OpenOptions::default())`.
    pub fn open<P: AsRef<Path>>(path: P) -> OpenResult<Self> {
        Self::open_with_options(path, OpenOptions::default())
    }

    /// Open or create a btree database at the given path using explicit runtime options.
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
                self.commit_txn_core(&core);
                Ok(res)
            }
            Err(e) => {
                drop(txn);
                core.rollback_to(origin);
                self.persist_rollback_meta(&core, "transaction rollback meta publication");
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
        self.commit_txn_core(&core);
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
        let _lock = self.writer_lock.lock();

        physical_value(self.refresh_internal(true), "BTree::exec_multi refresh");
        let core = TxnCore::new(self);
        let origin = core.checkpoint();

        let mut multi_txn = MultiTxn {
            core,
            bucket_roots: HashMap::new(),
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
                self.commit_txn_core(&multi_txn.core);
                Ok(res)
            }
            Err(e) => {
                multi_txn.core.rollback_to(origin);
                self.persist_rollback_meta(
                    &multi_txn.core,
                    "multi-transaction rollback meta publication",
                );
                Err(e)
            }
        }
    }

    fn persist_rollback_meta(&self, core: &TxnCore<'_>, context: &'static str) {
        // Physical page allocation is monotonic. A failed transaction restores roots
        // and pending ownership, but it must publish any consumed MetaNode state.
        physical_value(self.commit_internal(core), context);
    }

    fn persist_nested_rollback_meta(&self, core: &TxnCore<'_>, context: &'static str) {
        let snapshot = self.store.cached_snapshot();
        let published_snapshot = *self.local_snapshot.read();
        if snapshot == published_snapshot {
            return;
        }

        // The outer transaction's bucket roots are still unpublished. Keep the
        // durable catalog root at the last published generation while the
        // working allocations remain quarantined until outer success.
        physical_value(
            self.runtime
                .commit_generation_only(snapshot.catalog_root, &core.page_state.pending_alloc),
            context,
        );
        self.sync_local_snapshot_from_store();
    }

    fn commit_txn_core(&self, core: &TxnCore<'_>) {
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

        // Pin the current epoch before any page read: the reader's snapshot
        // pages stay protected from reuse for the whole closure, and the pin
        // value being older than the refreshed snapshot only makes the
        // allocator's promotion condition more conservative.
        let _guard = self.store.epoch.pin();

        // Refresh only when the shared published sequence is newer than this
        // handle's snapshot, then keep the selected root fixed for the view.
        let (latest_seq, mut latest_root) = self.store.shared_snapshot();
        let seq_changed = latest_seq != self.start_seq.load(Ordering::Acquire);
        if seq_changed {
            // readers never install a disk-newer generation into the shared
            // snapshot (see Store::refresh_sb)
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
        let metadata = BucketMetadata::from_slice(catalog_leaf.value_at(catalog_pos));
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
        validate_bucket_input(name)?;

        let _lock = self.writer_lock.lock();

        // ensure we are operating on the latest state
        physical_value(self.refresh_internal(true), "BTree::del_bucket refresh");

        let name_bytes = name.as_bytes();
        let mut core = TxnCore::new(self);
        let read = core.read.clone();
        let (catalog_leaf, catalog_pos) =
            Tree::find(&read, core.catalog_root(), name_bytes).ok_or(Error::BucketNotFound)?;
        let bucket_root = BucketMetadata::from_slice(catalog_leaf.value_at(catalog_pos)).root();

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
        core.page_state.pending_free.extend(pages_to_free);
        core.sync_pending_counts();
        physical_value(self.commit_internal(&core), "BTree::del_bucket commit");
        Ok(())
    }

    fn commit_internal(&self, core: &TxnCore<'_>) -> StoreResult<()> {
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
            return Ok(());
        }

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
    /// returns `Ok(())`.
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
        let _lock = self.writer_lock.lock();
        let core = TxnCore::new(self);
        physical_value(self.commit_internal(&core), "BTree::commit");
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

    fn buckets_internal(&self) -> StoreResult<Vec<String>> {
        let _guard = self.store.epoch.pin();

        // Same-process handles share the published sequence, so avoid rereading
        // both superblock pages when the local snapshot is already current.
        // readers never install a disk-newer generation into the shared
        // snapshot (see Store::refresh_sb)
        self.refresh_internal(false)?;
        let snapshot = *self.local_snapshot.read();
        let read = TreeReadContext::new(self.runtime.clone());

        let mut iter = Tree::iterator(
            &read,
            RootRef::decode(snapshot.catalog_root),
            IteratorCacheMode::Default,
        );
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

    /// Returns the current transaction sequence number.
    /// Useful for monitoring and testing.
    #[doc(hidden)]
    pub fn current_seq(&self) -> u64 {
        self.store.get_seq()
    }

    /// Returns the number of (allocated, freed) pages currently pending commit in this handle.
    /// Useful for monitoring and testing.
    #[doc(hidden)]
    pub fn pending_pages(&self) -> (usize, usize) {
        self.pending_counts.snapshot()
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
    use rand::{Rng, SeedableRng, rngs::StdRng};
    use std::collections::BTreeMap;

    fn test_store(dir: &tempfile::TempDir) -> Arc<Store> {
        Arc::new(Store::open(dir.path().join("tree.db"), &OpenOptions::default()).unwrap())
    }

    fn test_runtime(store: Arc<Store>) -> Arc<BTreeRuntime> {
        BTreeRuntime::new(store, OpenOptions::default().cache_capacity)
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

    fn alloc_and_write_node(store: &Store, node: &mut Node) -> DataPid {
        let mut alloc = HashSet::new();
        let pid = store.alloc_data_page(&mut alloc).unwrap();
        store.write_page(pid, node.finalize());
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

        fn iterator(&self, mode: IteratorCacheMode) -> TreeIterator<'_> {
            Tree::iterator(&self.read, self.root, mode)
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
        let mut iter = tree.iterator(IteratorCacheMode::Default);
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

        let mut forward = tree.iterator(IteratorCacheMode::Default);
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

        let mut reverse = tree.iterator(IteratorCacheMode::Default);
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

    fn assert_nonempty_generation_page_accounting(tree: &BTree) {
        let mut reachable_pids = HashSet::new();
        let read = &tree.read;
        let catalog_root = RootRef::decode(tree.store.cached_snapshot().catalog_root);

        collect_physical_tree_pages(read, catalog_root, &mut reachable_pids);

        let mut catalog = Tree::iterator(read, catalog_root, IteratorCacheMode::ByPass);
        let mut key = Vec::new();
        let mut value = Vec::new();
        while catalog.next_ref(&mut key, &mut value) {
            collect_physical_tree_pages(
                read,
                BucketMetadata::from_slice(&value).root(),
                &mut reachable_pids,
            );
        }
        tree.store.assert_complete_page_ownership(&reachable_pids);
    }

    #[test]
    fn prefix_encoding_establishes_shared_prefix_from_first_insert() {
        // A fresh encoded leaf has prefix_len 0; the empty prefix matches every
        // key, so without establishing a prefix a small bucket stores every key
        // at full length until a split. Seeding the prefix on the first insert
        // makes the second shared-prefix key trigger a rebuild that computes
        // the real common prefix.
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

        // Values and iteration stay correct.
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
        let bucket_root = BucketMetadata::from_slice(catalog_leaf.value_at(pos)).root();
        let node = read.load_node(bucket_root.node().expect("bucket root"));
        assert!(node.is_leaf(), "a two-entry bucket root must be a leaf");
        node.test_encoded_prefix_len()
    }

    #[test]
    fn prefix_encoding_cross_prefix_splits_keep_page_ownership_exact() {
        // Shared-prefix inserts fill leaves; cross-prefix inserts with large
        // overflow values repeatedly hit the rebuild-or-split path. Before the
        // fix this path allocated the overflow pages before checking whether
        // the rebuild fit, leaking an orphan page on every split trigger.
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("encoded-cross-prefix-ownership.db");
        let tree = BTree::open(&path).unwrap();
        tree.new_bucket("encoded", true).unwrap();

        // Fill one leaf toward capacity with shared-prefix keys, then a
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
        let bucket_root = BucketMetadata::from_slice(catalog_leaf.value_at(pos)).root();
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
    fn uncached_iterator_caches_leaf_root() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = test_store(&dir);
        let mut tree = TestTree::new(store.clone(), RootRef::Empty);
        tree.put(b"key", b"value").unwrap();

        let root = tree.root.node().expect("test tree root");
        tree.read.runtime.clear_cache();
        assert_eq!(tree.read.runtime.cached_node_is_leaf(root), None);

        let mut iter = tree.iterator(IteratorCacheMode::ByPass);
        let mut key = Vec::new();
        let mut value = Vec::new();
        assert!(iter.next_ref(&mut key, &mut value));
        assert_eq!(key, b"key");
        assert_eq!(value, b"value");
        assert_eq!(tree.read.runtime.cached_node_is_leaf(root), Some(true));
    }

    fn collect_node_ids(
        read: &TreeReadContext,
        id: DataPid,
        branches: &mut Vec<DataPid>,
        leaves: &mut Vec<DataPid>,
    ) {
        let node = read.load_node(id);
        if node.is_leaf() {
            leaves.push(id);
            return;
        }

        branches.push(id);
        for index in 0..node.num_children() {
            collect_node_ids(read, node.child_at(index), branches, leaves);
        }
    }

    #[test]
    fn uncached_iterator_bypasses_leaf_cache_but_keeps_branch_cache() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = test_store(&dir);
        let mut tree = TestTree::new(store.clone(), RootRef::Empty);

        for key in 0..200u32 {
            tree.put(&padded_key(key), b"value").unwrap();
        }

        let root = tree.root.node().expect("test tree root");
        let mut branches = Vec::new();
        let mut leaves = Vec::new();
        collect_node_ids(&tree.read, root, &mut branches, &mut leaves);
        assert!(!branches.is_empty(), "test tree must have branch nodes");
        assert!(!leaves.is_empty(), "test tree must have leaf nodes");

        tree.read.runtime.clear_cache();
        let mut iter = tree.iterator(IteratorCacheMode::ByPass);
        let mut key = Vec::new();
        let mut value = Vec::new();
        let mut count = 0;
        while iter.next_ref(&mut key, &mut value) {
            count += 1;
        }
        assert_eq!(count, 200);

        for page_id in branches {
            assert_eq!(
                tree.read.runtime.cached_node_is_leaf(page_id),
                Some(false),
                "uncached iteration must keep branch node {page_id:?} cached"
            );
        }
        for page_id in &leaves {
            assert_eq!(
                tree.read.runtime.cached_node_is_leaf(*page_id),
                None,
                "uncached iteration must not cache leaf node {page_id:?}"
            );
        }

        tree.read.runtime.clear_cache();
        let mut iter = tree.iterator(IteratorCacheMode::Default);
        while iter.next_ref(&mut key, &mut value) {}
        for page_id in leaves {
            assert_eq!(
                tree.read.runtime.cached_node_is_leaf(page_id),
                Some(true),
                "default iteration must cache leaf node {page_id:?}"
            );
        }
    }

    #[test]
    fn cloned_btree_handles_share_runtime_node_cache() {
        let dir = tempfile::TempDir::new().unwrap();
        let tree = BTree::open(dir.path().join("shared-runtime-cache.db")).unwrap();
        let mut alloc = HashSet::new();
        let page_id = tree.runtime.alloc_data_page(&mut alloc).unwrap();
        let node = Node::new_leaf();
        tree.store.write_page(page_id, node.finalize());

        tree.runtime.clear_cache();
        let clone = tree.clone();
        assert_eq!(clone.runtime.cached_node_is_leaf(page_id), None);

        tree.runtime.load_node(page_id);
        assert_eq!(clone.runtime.cached_node_is_leaf(page_id), Some(true));
    }

    #[test]
    fn runtime_allocator_invalidates_reused_page_ids() {
        let dir = tempfile::TempDir::new().unwrap();
        let tree = BTree::open(dir.path().join("runtime-cache-reuse.db")).unwrap();
        let mut alloc = HashSet::new();
        let page_id = tree.runtime.alloc_data_page(&mut alloc).unwrap();
        let node = Node::new_leaf();
        tree.store.write_page(page_id, node.finalize());
        tree.runtime.load_node(page_id);
        assert_eq!(tree.runtime.cached_node_is_leaf(page_id), Some(true));

        tree.runtime.free_pages(page_id.get(), 1).unwrap();
        assert_eq!(tree.runtime.cached_node_is_leaf(page_id), None);

        let mut reallocated = HashSet::new();
        assert_eq!(
            tree.runtime.alloc_data_page(&mut reallocated).unwrap(),
            page_id
        );
        assert_eq!(tree.runtime.cached_node_is_leaf(page_id), None);
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
        let parsed = BucketMetadata::from_slice(&containing_record);
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
