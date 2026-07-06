use std::{
    collections::{HashMap, HashSet},
    fmt, io,
    path::{Path, PathBuf},
    sync::{
        Arc, OnceLock, Weak,
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
    },
};

use parking_lot::{Mutex, RwLock, RwLockReadGuard};

#[cfg(feature = "ffi")]
mod ffi;
pub(crate) mod node;
pub(crate) mod page_store;
pub(crate) mod store;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    NotFound,
    Corruption,
    TooLarge,
    Internal,
    NoSpace,
    IoError,
    Invalid,
    Duplicate,
    Conflict,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for Error {}

impl From<io::Error> for Error {
    fn from(_: io::Error) -> Self {
        Error::IoError
    }
}

pub type Result<T> = std::result::Result<T, Error>;

pub type PageId = u32;

pub const MAGIC: u64 = 0x636f776274726565;
pub const FORMAT_VERSION: u32 = 3;
pub use crate::node::MAX_KEY_LEN;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum CacheMode {
    #[default]
    Default,
    ByPass,
}

/// Runtime sync policy used after commits and compaction.
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
/// options or they return [`Error::Invalid`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenOptions {
    /// Number of physical page nodes cached by the underlying store.
    pub node_cache_capacity: usize,
    /// Number of logical-id to physical-id mappings cached per handle.
    pub lid_pid_cache_capacity: usize,
    /// Number of entries in the direct-mapped hot logical-id cache per handle.
    pub lid_pid_hot_cache_capacity: usize,
    /// Maximum number of bucket root lookups cached across shared handles.
    pub bucket_root_cache_capacity: usize,
    /// Maximum number of read-only bucket trees cached across shared handles.
    pub bucket_tree_cache_capacity: usize,
    /// Sync policy used after metadata commits.
    pub sync_mode: SyncMode,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            node_cache_capacity: 8192,
            lid_pid_cache_capacity: 8192,
            lid_pid_hot_cache_capacity: 32 * 1024,
            bucket_root_cache_capacity: 8192,
            bucket_tree_cache_capacity: 8192,
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
    pub fn open<P: AsRef<Path>>(&self, path: P) -> Result<BTree> {
        BTree::open_with_options(path, self.clone())
    }
}

static BTREE_INSTANCE_REGISTRY: OnceLock<Mutex<HashMap<PathBuf, Weak<BTree>>>> = OnceLock::new();

fn btree_instance_registry() -> &'static Mutex<HashMap<PathBuf, Weak<BTree>>> {
    BTREE_INSTANCE_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

fn sweep_dead_btree_instances(reg: &mut HashMap<PathBuf, Weak<BTree>>) {
    reg.retain(|_, weak| weak.strong_count() > 0);
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
    pub format_version: u32,
    pub page_size: u32,
    pub catalog_root: PageId,
    pub mapping_root: PageId,
    pub reverse_root: PageId,
    pub next_lid: PageId,
    pub next_page_id: PageId,
    pub freelist_root: PageId,
    pub seq: u64,
    pub checksum: u64,
}

impl MetaNode {
    pub fn as_page_slice(&self) -> [u8; PAGE_SIZE] {
        let mut buf = [0u8; PAGE_SIZE];
        let src = unsafe {
            std::slice::from_raw_parts(
                (self as *const Self) as *const u8,
                std::mem::size_of::<Self>(),
            )
        };
        buf[..src.len()].copy_from_slice(src);
        buf
    }

    pub fn from_slice(x: &[u8]) -> Self {
        unsafe { std::ptr::read_unaligned(x.as_ptr().cast::<Self>()) }
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
            page_size: PAGE_SIZE as u32,
            catalog_root: 0,
            mapping_root: 0,
            reverse_root: 0,
            next_lid: 1,
            next_page_id: 2, // skip two meta pages
            freelist_root: 0,
            seq: 1,
            checksum: 0,
        };
        this.update_checksum();
        this
    }

    pub fn update_checksum(&mut self) {
        self.checksum = 0;
        let s = unsafe {
            std::slice::from_raw_parts(
                (self as *const Self) as *const u8,
                std::mem::size_of::<Self>(),
            )
        };
        self.checksum = crc32c::crc32c(s) as u64;
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

    pub fn validate(&self) -> Result<()> {
        // Torn write detection: treat an all-zero meta page as invalid.
        if self.magic == 0 && self.seq == 0 {
            return Err(Error::Corruption);
        }
        if self.checksum != self.calc_checksum() {
            return Err(Error::Corruption);
        }
        Ok(())
    }
}

use crate::{
    node::{Node, PAGE_SIZE},
    page_store::{LogicalStore, PageStore, decode_u32_key, encode_u32_key},
    store::{MetaSnapshot, Store},
};

fn validate_user_key(key: &[u8]) -> Result<()> {
    if key.is_empty() {
        return Err(Error::Invalid);
    }
    if key.len() > MAX_KEY_LEN {
        return Err(Error::TooLarge);
    }
    Ok(())
}

fn validate_bucket_name(bucket: &str) -> Result<()> {
    validate_user_key(bucket.as_bytes())
}

struct Route {
    node: Arc<Node>,
    page_id: PageId,
    pos: usize,
}

/// encapsulates page allocation and freeing during a transaction, ensuring COW safety
struct TxContext<'a> {
    store: &'a dyn PageStore,
    freed: &'a mut Vec<(PageId, u32)>,
    alloc: &'a mut HashSet<PageId>,
}

impl<'a> TxContext<'a> {
    fn new(
        store: &'a dyn PageStore,
        freed: &'a mut Vec<(PageId, u32)>,
        alloc: &'a mut HashSet<PageId>,
    ) -> Self {
        Self {
            store,
            freed,
            alloc,
        }
    }

    fn alloc_page(&mut self) -> Result<PageId> {
        self.store.alloc_page(self.alloc)
    }

    fn write_node(&mut self, node: &mut Node) -> Result<PageId> {
        let pid = self.alloc_page()?;
        self.store.write_data(&[pid], node.finalize())?;
        Ok(pid)
    }

    fn free_page(&mut self, pid: PageId) -> Result<()> {
        self.store.schedule_free(pid, self.freed)
    }
}

pub(crate) struct Tree {
    store: Arc<dyn PageStore>,
    pub(crate) root_page_id: Arc<RwLock<PageId>>,
    pending_free: Arc<RwLock<Vec<(PageId, u32)>>>,
    pending_alloc: Arc<RwLock<HashSet<PageId>>>,
}

impl Tree {
    pub(crate) fn open(
        store: Arc<dyn PageStore>,
        root_page_id: Arc<RwLock<PageId>>,
        pending_free: Arc<RwLock<Vec<(PageId, u32)>>>,
        pending_alloc: Arc<RwLock<HashSet<PageId>>>,
    ) -> Result<Self> {
        Ok(Self {
            store,
            root_page_id,
            pending_free,
            pending_alloc,
        })
    }

    fn traverse_to_leaf(
        &self,
        mut node: Arc<Node>,
        mut page_id: PageId,
        key: &[u8],
    ) -> Result<(Vec<Route>, Arc<Node>, PageId)> {
        let mut stack = Vec::new();
        while !node.is_leaf() {
            let pos = match node.search(key) {
                Ok(pos) => pos,
                Err(pos) => pos.saturating_sub(1),
            };
            let child_id = node.child_at(pos);
            if child_id == 0 {
                return Err(Error::Corruption);
            }
            let child_node = self.store.load_node(child_id)?;
            stack.push(Route { node, page_id, pos });
            node = child_node;
            page_id = child_id;
        }
        Ok((stack, node, page_id))
    }

    // put tmp pids to global containers
    fn merge_pending(&self, freed: Vec<(PageId, u32)>, mut alloc: HashSet<PageId>) {
        let mut main_free = self.pending_free.write();
        let mut main_alloc = self.pending_alloc.write();

        for (pid, nr) in freed {
            if alloc.remove(&pid) {
                // If it was allocated and freed in the same operation, it's a transient COW node.
                // It's safe to return it to the store immediately for reuse within this txn.
                let _ = self.store.free_pages_immediate(pid, nr);
                continue;
            }
            if main_alloc.remove(&pid) {
                // Previously allocated in this same transaction, now replaced by a newer COW version.
                let _ = self.store.free_pages_immediate(pid, nr);
            } else {
                Self::merge_free_extent(&mut main_free, pid, nr);
            }
        }
        main_alloc.extend(alloc);
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

    pub(crate) fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        validate_user_key(key)?;
        // use local buffers to reduce lock contention and keep partial changes local until success
        let mut freed = Vec::new();
        let mut alloc = HashSet::new();

        let result = self.execute_put(key, value, &mut freed, &mut alloc);

        if result.is_err() {
            // reclaim pages allocated during this failed operation to prevent leak
            for pid in alloc {
                let _ = self.store.free_pages_immediate(pid, 1);
            }
            return result;
        }

        self.merge_pending(freed, alloc);
        Ok(())
    }

    pub(crate) fn update(&self, key: &[u8], value: &[u8]) -> Result<bool> {
        validate_user_key(key)?;

        let mut freed = Vec::new();
        let mut alloc = HashSet::new();

        let result = self.execute_update(key, value, &mut freed, &mut alloc);
        match result {
            Ok(true) => {
                self.merge_pending(freed, alloc);
                Ok(true)
            }
            Ok(false) => {
                for pid in alloc {
                    let _ = self.store.free_pages_immediate(pid, 1);
                }
                Ok(false)
            }
            Err(e) => {
                for pid in alloc {
                    let _ = self.store.free_pages_immediate(pid, 1);
                }
                Err(e)
            }
        }
    }

    fn execute_put(
        &self,
        key: &[u8],
        value: &[u8],
        freed: &mut Vec<(PageId, u32)>,
        alloc: &mut HashSet<PageId>,
    ) -> Result<()> {
        let mut ctx = TxContext::new(self.store.as_ref(), freed, alloc);
        let mut root_lock = self.root_page_id.write();
        let current_root_id = *root_lock;

        // root is empty
        if current_root_id == 0 {
            let mut node = Node::new_leaf();
            node.put(ctx.store, key, value, ctx.freed, ctx.alloc)?;
            *root_lock = ctx.write_node(&mut node)?;
            return Ok(());
        }

        // 1. find target leaf node
        let root_node = self.store.load_node(current_root_id)?;
        let (mut stack, leaf_node_arc, leaf_id) =
            self.traverse_to_leaf(root_node, current_root_id, key)?;

        let mut current_node = (*leaf_node_arc).clone();

        // 2. modify leaf node and get split info (if any)
        let mut split_info = self.apply_insert(&mut ctx, &mut current_node, key, value)?;

        // write new COW leaf node
        let mut new_child_id = ctx.write_node(&mut current_node)?;
        ctx.free_page(leaf_id)?;

        // 3. backtrack up the path, propagating changes and splits
        while let Some(Route {
            node: parent_arc,
            page_id: parent_id,
            pos,
        }) = stack.pop()
        {
            let mut parent = (*parent_arc).clone();

            // point the current slot to the newly created COW node
            parent.update_child_page(pos, new_child_id);

            // if the previous child node split, insert the right-hand side (rhs) into the current parent
            if let Some((sep, mut rhs)) = split_info.take() {
                let rhs_id = ctx.write_node(&mut rhs)?;
                // inserting a separator may cause the parent node to split as well
                split_info =
                    self.apply_insert(&mut ctx, &mut parent, &sep, &rhs_id.to_le_bytes())?;
            }

            // write new COW parent node and prepare for the next level up
            new_child_id = ctx.write_node(&mut parent)?;
            ctx.free_page(parent_id)?;
        }

        // 4. handle root node split
        if let Some((sep, mut rhs)) = split_info {
            let rhs_id = ctx.write_node(&mut rhs)?;
            let mut new_root = Node::new_branch();
            // left child points to the COW version of the old root; its key is always empty in branch nodes
            new_root.put(
                ctx.store,
                &[],
                &new_child_id.to_le_bytes(),
                ctx.freed,
                ctx.alloc,
            )?;
            // right child points to the newly split node
            new_root.put(ctx.store, &sep, &rhs_id.to_le_bytes(), ctx.freed, ctx.alloc)?;
            *root_lock = ctx.write_node(&mut new_root)?;
        } else {
            // root did not split, simply update root pointer
            *root_lock = new_child_id;
        }

        Ok(())
    }

    fn execute_update(
        &self,
        key: &[u8],
        value: &[u8],
        freed: &mut Vec<(PageId, u32)>,
        alloc: &mut HashSet<PageId>,
    ) -> Result<bool> {
        let mut ctx = TxContext::new(self.store.as_ref(), freed, alloc);
        let mut root_lock = self.root_page_id.write();
        let current_root_id = *root_lock;

        if current_root_id == 0 {
            return Ok(false);
        }

        let root_node = self.store.load_node(current_root_id)?;
        let (mut stack, leaf_node_arc, leaf_id) =
            self.traverse_to_leaf(root_node, current_root_id, key)?;
        let mut current_node = (*leaf_node_arc).clone();

        let pos = match current_node.search(key) {
            Ok(pos) => pos,
            Err(_) => return Ok(false),
        };

        current_node.update_at(ctx.store, pos, value, ctx.freed, ctx.alloc)?;

        let mut new_child_id = ctx.write_node(&mut current_node)?;
        ctx.free_page(leaf_id)?;

        while let Some(Route {
            node: parent_arc,
            page_id: parent_id,
            pos,
        }) = stack.pop()
        {
            let mut parent = (*parent_arc).clone();
            parent.update_child_page(pos, new_child_id);
            new_child_id = ctx.write_node(&mut parent)?;
            ctx.free_page(parent_id)?;
        }

        *root_lock = new_child_id;
        Ok(true)
    }

    fn apply_insert(
        &self,
        ctx: &mut TxContext,
        node: &mut Node,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<(Vec<u8>, Node)>> {
        match node.put(ctx.store, key, value, ctx.freed, ctx.alloc) {
            Ok(()) => Ok(None),
            Err(Error::NoSpace) => {
                // split to right
                let (sep, mut rhs) = node.split()?;
                if key < &sep {
                    node.put(ctx.store, key, value, ctx.freed, ctx.alloc)?;
                } else {
                    rhs.put(ctx.store, key, value, ctx.freed, ctx.alloc)?;
                }
                Ok(Some((sep, rhs)))
            }
            Err(e) => Err(e),
        }
    }

    #[inline(always)]
    pub(crate) fn get(&self, key: &[u8]) -> Result<Vec<u8>> {
        self.get_with_mode(key, CacheMode::Default)
    }

    pub(crate) fn get_with_mode(&self, key: &[u8], mode: CacheMode) -> Result<Vec<u8>> {
        validate_user_key(key)?;

        let root_id = *self.root_page_id.read();
        if root_id == 0 {
            return Err(Error::NotFound);
        }

        let root_node = self.store.load_node_with_mode(root_id, mode)?;
        let mut current = root_node;
        loop {
            if current.is_leaf() {
                return current.get_with_mode(self.store.as_ref(), key, mode);
            }
            let pos = current.child_pos_for_key(key);
            current = self
                .store
                .load_node_with_mode(current.child_at(pos), mode)?;
        }
    }

    pub(crate) fn del(&self, key: &[u8]) -> Result<()> {
        validate_user_key(key)?;

        // use local buffers to reduce lock contention and keep partial changes local until success
        let mut freed = Vec::new();
        let mut alloc = HashSet::new();

        let result = self.execute_del(key, &mut freed, &mut alloc);

        if result.is_err() {
            // reclaim pages allocated during this failed operation to prevent leak
            for pid in alloc {
                let _ = self.store.free_pages_immediate(pid, 1);
            }
            return result;
        }

        self.merge_pending(freed, alloc);
        Ok(())
    }

    fn execute_del(
        &self,
        key: &[u8],
        freed: &mut Vec<(PageId, u32)>,
        alloc: &mut HashSet<PageId>,
    ) -> Result<()> {
        let mut ctx = TxContext::new(self.store.as_ref(), freed, alloc);
        let mut root_lock = self.root_page_id.write();
        let current_root_id = *root_lock;

        if current_root_id == 0 {
            return Err(Error::NotFound);
        }

        // 1. find target leaf node
        let root_node = self.store.load_node(current_root_id)?;
        let (mut stack, leaf_arc, leaf_id) =
            self.traverse_to_leaf(root_node, current_root_id, key)?;

        let mut current_node = (*leaf_arc).clone();
        current_node.del(ctx.store, key, ctx.freed)?;

        // 2. handle leaf node changes
        let mut empty = current_node.is_empty();
        let mut new_child_id = if !empty {
            ctx.write_node(&mut current_node)?
        } else {
            0
        };
        ctx.free_page(leaf_id)?;

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
                // NOTE: merge is triggered when node is empty and thus no elements borrow is required
                parent.shrink_slot(pos);
                // special case: if the leftmost child of a branch node is deleted, ensure the new first child has an
                // empty key
                if !parent.is_leaf() && pos == 0 && !parent.is_empty() {
                    parent.slot_at_mut(0).klen = 0;
                    parent.dirty = true;
                }
            } else {
                // if child node only changed content, update pointer in parent
                parent.update_child_page(pos, new_child_id);
            }

            // check if current parent node also becomes empty
            if parent.is_empty() {
                empty = true;
                new_child_id = 0;
            } else {
                empty = false;
                new_child_id = ctx.write_node(&mut parent)?;
            }
            ctx.free_page(parent_id)?;
        }

        // 4. root collapse optimization
        // if root is a branch node with only one child, elevate child to be the new root
        if new_child_id != 0 {
            loop {
                let node_id = new_child_id;
                let node = self.store.load_node(node_id)?;
                if !node.is_leaf() && node.num_children() == 1 {
                    let child_id = node.child_at(0);
                    ctx.free_page(node_id)?;
                    new_child_id = child_id;
                } else {
                    break;
                }
            }
        }

        *root_lock = new_child_id;
        Ok(())
    }

    pub(crate) fn collect_tree_pages(
        store: &dyn PageStore,
        root_id: PageId,
        freed: &mut Vec<(PageId, u32)>,
    ) -> Result<()> {
        if root_id == 0 {
            return Ok(());
        }

        let mut stack = vec![root_id];

        while let Some(current_id) = stack.pop() {
            let node = store.load_node(current_id)?;
            store.schedule_free(current_id, freed)?;

            for i in 0..node.num_children() {
                if node.is_leaf() {
                    let slot = node.slot_at(i);
                    if !slot.is_inline() {
                        node.free_slot_pages(store, slot, freed)?;
                    }
                } else {
                    let child_id = node.child_at(i);
                    if child_id != 0 {
                        stack.push(child_id);
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) fn iterator(&self, mode: CacheMode) -> TreeIterator {
        let root_id = *self.root_page_id.read();
        TreeIterator::new(self.store.clone(), root_id, None, mode)
    }

    pub(crate) fn iterator_from(&self, key: &[u8], mode: CacheMode) -> TreeIterator {
        let root_id = *self.root_page_id.read();
        TreeIterator::new_from(self.store.clone(), root_id, None, key, mode)
    }
}

pub struct TreeIterator {
    store: Arc<dyn PageStore>,
    mode: CacheMode,
    stack: Vec<(Arc<Node>, usize)>,
    current_leaf: Option<(Arc<Node>, usize)>,
}

impl TreeIterator {
    #[inline]
    fn load_child_node(&self, child_id: PageId) -> Result<Arc<Node>> {
        match self.mode {
            CacheMode::Default => self.store.load_node_with_mode(child_id, CacheMode::Default),
            CacheMode::ByPass => {
                if matches!(self.store.cached_node_is_leaf(child_id), Some(false)) {
                    return self.store.load_node_with_mode(child_id, CacheMode::Default);
                }

                let node = self
                    .store
                    .load_node_with_mode(child_id, CacheMode::ByPass)?;
                if !node.is_leaf() {
                    self.store.cache_node(child_id, node.clone());
                }
                Ok(node)
            }
        }
    }

    fn new(
        store: Arc<dyn PageStore>,
        root_id: PageId,
        root_node: Option<Arc<Node>>,
        mode: CacheMode,
    ) -> Self {
        let mut iter = Self {
            store,
            mode,
            stack: Vec::new(),
            current_leaf: None,
        };

        if root_id != 0 {
            let node = root_node.or_else(|| iter.store.load_node_with_mode(root_id, mode).ok());
            if let Some(node) = node {
                iter.push_node(node);
            }
        }
        iter
    }

    fn new_from(
        store: Arc<dyn PageStore>,
        root_id: PageId,
        root_node: Option<Arc<Node>>,
        key: &[u8],
        mode: CacheMode,
    ) -> Self {
        let mut iter = Self {
            store,
            mode,
            stack: Vec::new(),
            current_leaf: None,
        };

        if root_id == 0 {
            return iter;
        }

        let mut node = match root_node {
            Some(node) => node,
            None => match iter.store.load_node_with_mode(root_id, mode) {
                Ok(node) => node,
                Err(_) => return iter,
            },
        };

        while !node.is_leaf() {
            let pos = match node.search(key) {
                Ok(pos) => pos,
                Err(pos) => pos.saturating_sub(1),
            };
            let child_id = node.child_at(pos);
            if child_id == 0 {
                return iter;
            }
            iter.stack.push((node.clone(), pos + 1));
            match iter.load_child_node(child_id) {
                Ok(child) => node = child,
                Err(_) => return iter,
            }
        }

        let leaf_pos = match node.search(key) {
            Ok(pos) => pos,
            Err(pos) => pos,
        };
        iter.current_leaf = Some((node, leaf_pos));
        iter
    }

    fn push_node(&mut self, node: Arc<Node>) {
        if node.is_leaf() {
            self.current_leaf = Some((node, 0));
        } else {
            self.stack.push((node, 0));
        }
    }

    pub fn next_ref(&mut self, key_buf: &mut Vec<u8>, val_buf: &mut Vec<u8>) -> bool {
        loop {
            if let Some((leaf, idx)) = self.current_leaf.as_mut() {
                if *idx < leaf.num_children() {
                    let slot = leaf.slot_at(*idx);

                    key_buf.clear();
                    key_buf.extend_from_slice(leaf.key_at(*idx));

                    val_buf.clear();
                    if slot.is_inline() {
                        val_buf.extend_from_slice(leaf.value_at(*idx));
                    } else if let Ok(pages) =
                        leaf.collect_page_ids_with_mode(self.store.as_ref(), slot, self.mode)
                    {
                        val_buf.resize(slot.value_len(), 0);
                        if self
                            .store
                            .read_data_with_mode(&pages, val_buf, self.mode)
                            .is_err()
                        {
                            *idx += 1;
                            continue;
                        }
                    } else {
                        *idx += 1;
                        continue;
                    }

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
                    if let Ok(child_node) = self.load_child_node(child_id) {
                        self.push_node(child_node);
                    }
                } else {
                    self.stack.pop();
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
    pub(crate) tree: Tree,
    pub(crate) _marker: std::marker::PhantomData<&'a ()>,
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
        self.tree.put(key.as_ref(), value.as_ref())
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
        self.tree.update(key.as_ref(), value.as_ref())
    }

    /// Returns the value for `key`.
    ///
    /// Returns [`Error::NotFound`] when the bucket or key does not exist. The
    /// key must be non-empty and no longer than [`MAX_KEY_LEN`] bytes.
    pub fn get<K>(&self, key: K) -> Result<Vec<u8>>
    where
        K: AsRef<[u8]>,
    {
        self.tree.get(key.as_ref())
    }

    /// Deletes `key` from the current bucket.
    ///
    /// Returns [`Error::NotFound`] when the bucket or key does not exist. The
    /// key must be non-empty and no longer than [`MAX_KEY_LEN`] bytes.
    pub fn del<K>(&mut self, key: K) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        self.tree.del(key.as_ref())
    }

    /// Returns an iterator over the current bucket in key order.
    pub fn iter(&self) -> TreeIterator {
        self.tree.iterator(CacheMode::Default)
    }

    /// Returns an iterator over the current bucket without caching leaf nodes
    /// or overflow value pages.
    ///
    /// Branch nodes may still be cached and reused so point reads and future
    /// traversals keep their hot upper-level path.
    pub fn iter_uncached(&self) -> TreeIterator {
        self.tree.iterator(CacheMode::ByPass)
    }
}

pub(crate) struct ReadOnlyTree {
    store: Arc<dyn PageStore>,
    root_page_id: PageId,
    root_node: Option<Arc<Node>>,
}

impl ReadOnlyTree {
    fn new(store: Arc<dyn PageStore>, root_page_id: PageId) -> Result<Self> {
        let root_node = if root_page_id == 0 {
            None
        } else {
            Some(store.load_node(root_page_id)?)
        };
        Ok(Self {
            store,
            root_page_id,
            root_node,
        })
    }

    fn load_root(&self, mode: CacheMode) -> Result<Arc<Node>> {
        if self.root_page_id == 0 {
            return Err(Error::NotFound);
        }

        let node = match mode {
            CacheMode::Default | CacheMode::ByPass => self
                .root_node
                .as_ref()
                .cloned()
                .expect("validated read-only tree must retain its root node"),
        };
        Ok(node)
    }

    #[inline(always)]
    fn get(&self, key: &[u8]) -> Result<Vec<u8>> {
        self.get_with_mode(key, CacheMode::Default)
    }

    fn get_with_mode(&self, key: &[u8], mode: CacheMode) -> Result<Vec<u8>> {
        validate_user_key(key)?;

        if self.root_page_id == 0 {
            return Err(Error::NotFound);
        }

        let mut current = self.load_root(mode)?;
        loop {
            if current.is_leaf() {
                return current.get_with_mode(self.store.as_ref(), key, mode);
            }
            let pos = current.child_pos_for_key(key);
            current = self
                .store
                .load_node_with_mode(current.child_at(pos), mode)?;
        }
    }

    pub(crate) fn iterator(&self, mode: CacheMode) -> TreeIterator {
        let root_node = self.root_node.clone();
        TreeIterator::new(self.store.clone(), self.root_page_id, root_node, mode)
    }
}

/// A read-only transaction handle scoped to a single bucket snapshot.
///
/// Instances are provided by [`BTree::view`]. The handle is valid only for the
/// duration of the callback that receives it.
pub struct ReadOnlyTxn<'a> {
    pub(crate) tree: Arc<ReadOnlyTree>,
    pub(crate) _guard: RwLockReadGuard<'a, ()>,
}

impl<'a> ReadOnlyTxn<'a> {
    /// Returns the value for `key` from the read-only snapshot.
    ///
    /// Returns [`Error::NotFound`] when the bucket or key does not exist. The
    /// key must be non-empty and no longer than [`MAX_KEY_LEN`] bytes.
    pub fn get<K>(&self, key: K) -> Result<Vec<u8>>
    where
        K: AsRef<[u8]>,
    {
        self.tree.get(key.as_ref())
    }

    /// Returns an iterator over the read-only bucket snapshot in key order.
    pub fn iter(&self) -> TreeIterator {
        self.tree.iterator(CacheMode::Default)
    }

    /// Returns an iterator over the read-only bucket snapshot without caching
    /// leaf nodes or overflow value pages.
    ///
    /// The root may already have been loaded through the normal cache path when
    /// [`BTree::view`] validated the snapshot, and branch nodes may still be
    /// cached. Leaf nodes and overflow value page reads performed by this
    /// iterator bypass the page caches.
    pub fn iter_uncached(&self) -> TreeIterator {
        self.tree.iterator(CacheMode::ByPass)
    }
}

pub struct MultiTxn<'a> {
    btree: &'a BTree,
    bucket_roots: HashMap<String, MultiTxnBucketRoot>,
}

#[derive(Clone, Copy)]
struct MultiTxnBucketRoot {
    initial_exists: bool,
    initial: PageId,
    current: PageId,
}

impl<'a> MultiTxn<'a> {
    pub fn exec<F, R>(&mut self, bucket: &str, f: F) -> Result<R>
    where
        F: FnOnce(&mut Txn) -> Result<R>,
    {
        validate_bucket_name(bucket)?;

        let name_bytes = bucket.as_bytes();

        let root = if let Some(root) = self.bucket_roots.get(bucket) {
            *root
        } else {
            let (initial_exists, current) = match self.btree.catalog_tree.get(name_bytes) {
                Ok(bytes) => (true, BucketMetadata::from_slice(&bytes).root_page_id),
                Err(Error::NotFound) => (false, 0),
                Err(e) => return Err(e),
            };
            MultiTxnBucketRoot {
                initial_exists,
                initial: current,
                current,
            }
        };

        let logical_store_obj: Arc<dyn PageStore> = self.btree.logical_store.clone();
        let tree = Tree::open(
            logical_store_obj,
            Arc::new(RwLock::new(root.current)),
            self.btree.pending_free.clone(),
            self.btree.pending_alloc.clone(),
        )?;

        let mut txn = Txn {
            tree,
            _marker: std::marker::PhantomData,
        };

        let res = f(&mut txn);
        if res.is_ok() {
            let current = *txn.tree.root_page_id.read();
            self.bucket_roots.insert(
                bucket.to_string(),
                MultiTxnBucketRoot {
                    initial_exists: root.initial_exists,
                    initial: root.initial,
                    current,
                },
            );
        }
        res
    }
}

pub(crate) struct BucketMetadata {
    pub(crate) root_page_id: PageId,
}

impl BucketMetadata {
    pub(crate) fn from_slice(x: &[u8]) -> Self {
        assert!(x.len() >= std::mem::size_of::<Self>());
        unsafe { std::ptr::read_unaligned(x.as_ptr().cast::<Self>()) }
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self as *const Self as *const u8,
                std::mem::size_of::<Self>(),
            )
        }
    }
}

pub struct BTree {
    pub(crate) store: Arc<Store>,
    pub(crate) catalog_tree: Arc<Tree>,
    pub(crate) mapping_tree: Arc<Tree>,
    pub(crate) reverse_tree: Arc<Tree>,
    pub(crate) logical_store: Arc<LogicalStore>,
    pub(crate) pending_free: Arc<RwLock<Vec<(PageId, u32)>>>,
    pub(crate) pending_alloc: Arc<RwLock<HashSet<PageId>>>,
    pub(crate) writer_lock: Arc<RwLock<()>>,
    pub(crate) start_root_id: Arc<AtomicU32>,
    pub(crate) start_seq: Arc<AtomicU64>,
    local_snapshot: Arc<RwLock<MetaSnapshot>>,
    pub(crate) bucket_root_cache: Arc<RwLock<BucketRootCache>>,
    pub(crate) bucket_tree_cache: Arc<RwLock<BucketTreeCache>>,
    options: OpenOptions,
    instance_anchor: Option<Arc<BTree>>,
}

struct BucketCacheEntry<V> {
    value: V,
    seq: u64,
    used: AtomicBool,
}

struct BucketCache<V> {
    capacity: usize,
    hand: usize,
    entries: HashMap<Vec<u8>, BucketCacheEntry<V>>,
    slots: Vec<Option<Vec<u8>>>,
}

impl<V> BucketCache<V> {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            hand: 0,
            entries: HashMap::new(),
            slots: Vec::new(),
        }
    }

    fn clear(&mut self) {
        self.hand = 0;
        self.entries.clear();
        self.slots.clear();
    }

    fn get(&self, key: &[u8], seq: u64) -> Option<&V> {
        let entry = self.entries.get(key)?;
        if entry.seq != seq {
            return None;
        }
        entry.used.store(true, Ordering::Relaxed);
        Some(&entry.value)
    }

    fn insert(&mut self, key: Vec<u8>, seq: u64, value: V) {
        if self.capacity == 0 {
            return;
        }
        if let Some(entry) = self.entries.get_mut(key.as_slice()) {
            entry.value = value;
            entry.seq = seq;
            entry.used.store(true, Ordering::Relaxed);
            return;
        }

        let slot_idx = if self.entries.len() < self.capacity {
            let idx = self.slots.len();
            self.slots.push(None);
            idx
        } else {
            self.evict_slot()
        };

        self.slots[slot_idx] = Some(key.clone());
        self.entries.insert(
            key,
            BucketCacheEntry {
                value,
                seq,
                used: AtomicBool::new(false),
            },
        );
    }

    fn evict_slot(&mut self) -> usize {
        debug_assert!(!self.slots.is_empty());
        loop {
            let idx = self.hand;
            self.hand = (self.hand + 1) % self.slots.len();

            let should_evict = {
                let key = self.slots[idx]
                    .as_ref()
                    .expect("bucket cache slot should contain a key");
                let entry = self
                    .entries
                    .get(key.as_slice())
                    .expect("bucket cache slot should map to a live entry");
                !entry.used.swap(false, Ordering::Relaxed)
            };

            if should_evict {
                let evicted_key = self.slots[idx]
                    .take()
                    .expect("bucket cache slot should contain the evicted key");
                self.entries
                    .remove(evicted_key.as_slice())
                    .expect("bucket cache eviction should remove the mapped entry");
                return idx;
            }
        }
    }
}

type BucketRootCache = BucketCache<PageId>;
type BucketTreeCache = BucketCache<Arc<ReadOnlyTree>>;

/// compact all pages when total data pages are at or below this threshold
const COMPACT_SMALL_DATA_THRESHOLD_PAGES: u64 = 1024;
/// default tail ratio when target_bytes is zero
const COMPACT_TAIL_RATIO: f64 = 0.5;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompactStats {
    pub moved_pages: u64,
    pub remaining_candidates: u64,
}

impl BTree {
    fn apply_local_snapshot(&self, snapshot: MetaSnapshot, clear_caches: bool) {
        *self.catalog_tree.root_page_id.write() = snapshot.catalog_root;
        *self.mapping_tree.root_page_id.write() = snapshot.mapping_root;
        *self.reverse_tree.root_page_id.write() = snapshot.reverse_root;
        self.start_root_id
            .store(snapshot.catalog_root, Ordering::Release);
        self.start_seq.store(snapshot.seq, Ordering::Release);
        *self.local_snapshot.write() = snapshot;
        if clear_caches {
            self.bucket_root_cache.write().clear();
            self.bucket_tree_cache.write().clear();
        }
    }

    fn sync_local_snapshot_from_store(&self) {
        self.apply_local_snapshot(self.store.cached_snapshot(), false);
    }

    /// Open or create a btree database at the given path using default runtime options.
    ///
    /// This is equivalent to `BTree::open_with_options(path, OpenOptions::default())`.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_options(path, OpenOptions::default())
    }

    /// Open or create a btree database at the given path using explicit runtime options.
    ///
    /// Within a single process, opening the same path again reuses the live
    /// instance. Reopens must use identical runtime options.
    pub fn open_with_options<P: AsRef<Path>>(path: P, options: OpenOptions) -> Result<Self> {
        let path = path.as_ref();
        let key = normalize_db_path(path);

        if let Some(existing) = {
            let mut reg = btree_instance_registry().lock();
            sweep_dead_btree_instances(&mut reg);
            let upgraded = reg.get(&key).and_then(|w| w.upgrade());
            if upgraded.is_none() {
                reg.remove(&key);
            }
            upgraded
        } {
            if existing.options != options {
                return Err(Error::Invalid);
            }
            let mut handle = existing.as_ref().clone();
            handle.instance_anchor = Some(existing);
            handle.sync_local_snapshot_from_store();
            return Ok(handle);
        }

        let store = Arc::new(Store::open(path, &options)?);
        let catalog_root = store.get_catalog_root();
        let mapping_root = store.get_mapping_root();
        let reverse_root = store.get_reverse_root();
        let initial_seq = store.get_seq();
        let initial_snapshot = MetaSnapshot {
            catalog_root,
            mapping_root,
            reverse_root,
            seq: initial_seq,
        };
        let pending_free = Arc::new(RwLock::new(Vec::new()));
        let pending_alloc = Arc::new(RwLock::new(HashSet::new()));
        let mapping_root_lock = Arc::new(RwLock::new(mapping_root));
        let reverse_root_lock = Arc::new(RwLock::new(reverse_root));

        let physical_store: Arc<dyn PageStore> = store.clone();
        let mapping_tree = Arc::new(Tree::open(
            physical_store.clone(),
            mapping_root_lock,
            pending_free.clone(),
            pending_alloc.clone(),
        )?);
        let reverse_tree = Arc::new(Tree::open(
            physical_store,
            reverse_root_lock,
            pending_free.clone(),
            pending_alloc.clone(),
        )?);

        let logical_store = Arc::new(LogicalStore::new(
            store.clone(),
            mapping_tree.clone(),
            reverse_tree.clone(),
            &options,
        ));
        let catalog_tree_root_lock = Arc::new(RwLock::new(catalog_root));
        let logical_store_obj: Arc<dyn PageStore> = logical_store.clone();
        let catalog_tree = Arc::new(Tree::open(
            logical_store_obj,
            catalog_tree_root_lock,
            pending_free.clone(),
            pending_alloc.clone(),
        )?);

        let instance = Self {
            store: store.clone(),
            catalog_tree,
            mapping_tree,
            reverse_tree,
            logical_store,
            pending_free,
            pending_alloc,
            writer_lock: Arc::new(RwLock::new(())),
            start_root_id: Arc::new(AtomicU32::new(catalog_root)),
            start_seq: Arc::new(AtomicU64::new(initial_seq)),
            local_snapshot: Arc::new(RwLock::new(initial_snapshot)),
            bucket_root_cache: Arc::new(RwLock::new(BucketRootCache::new(
                options.bucket_root_cache_capacity,
            ))),
            bucket_tree_cache: Arc::new(RwLock::new(BucketTreeCache::new(
                options.bucket_tree_cache_capacity,
            ))),
            options: options.clone(),
            instance_anchor: None,
        };
        let instance_arc = Arc::new(instance);
        {
            let mut reg = btree_instance_registry().lock();
            sweep_dead_btree_instances(&mut reg);
            if let Some(existing) = reg.get(&key).and_then(|w| w.upgrade()) {
                if existing.options != options {
                    return Err(Error::Invalid);
                }
                let mut handle = existing.as_ref().clone();
                handle.instance_anchor = Some(existing);
                handle.sync_local_snapshot_from_store();
                return Ok(handle);
            }
            reg.insert(key, Arc::downgrade(&instance_arc));
        }

        let mut handle = instance_arc.as_ref().clone();
        handle.instance_anchor = Some(instance_arc);
        Ok(handle)
    }

    /// Executes a read-write transaction on the specified bucket.
    /// Creates the bucket on successful commit if it doesn't exist.
    ///
    /// The transaction is committed if the closure returns `Ok`, but the commit can still fail
    /// (e.g., conflict or I/O error). On failure, changes are rolled back.
    /// If the closure returns `Err`, the transaction is rolled back (allocated pages are reclaimed).
    ///
    /// # Warning
    /// Nested calls to `exec` or `view` on the same `BTree` instance are NOT supported
    /// and may lead to deadlocks or undefined behavior.
    pub fn exec<F, R>(&self, bucket: &str, f: F) -> Result<R>
    where
        F: FnOnce(&mut Txn) -> Result<R>,
    {
        validate_bucket_name(bucket)?;

        let _lock = self.writer_lock.write();

        // Auto-refresh to the latest disk state before starting a new transaction.
        // This makes the "Session" always start from the freshest data.
        self.refresh_internal()?;

        // Check if there's an existing bucket
        let name_bytes = bucket.as_bytes();
        let (initial_exists, initial_root) = match self.catalog_tree.get(name_bytes) {
            Ok(bytes) => (true, BucketMetadata::from_slice(&bytes).root_page_id),
            Err(Error::NotFound) => (false, 0),
            Err(e) => return Err(e),
        };

        // Snapshot pending state for rollback
        let pre_alloc = self.pending_alloc.read().clone();
        let pre_free = self.pending_free.read().clone();
        let pre_catalog_root = *self.catalog_tree.root_page_id.read();
        let pre_mapping_root = *self.mapping_tree.root_page_id.read();
        let pre_reverse_root = *self.reverse_tree.root_page_id.read();

        let logical_store_obj: Arc<dyn PageStore> = self.logical_store.clone();
        let tree = Tree::open(
            logical_store_obj,
            Arc::new(RwLock::new(initial_root)),
            self.pending_free.clone(),
            self.pending_alloc.clone(),
        )?;

        let mut txn = Txn {
            tree,
            _marker: std::marker::PhantomData,
        };

        match f(&mut txn) {
            Ok(res) => {
                let new_root = *txn.tree.root_page_id.read();
                if !initial_exists || initial_root != new_root {
                    let metadata = BucketMetadata {
                        root_page_id: new_root,
                    };
                    self.catalog_tree.put(name_bytes, metadata.as_slice())?;
                }
                if let Err(e) = self.commit_internal() {
                    // Rollback catalog and pages on commit failure (e.g. Conflict)
                    *self.catalog_tree.root_page_id.write() = pre_catalog_root;
                    *self.mapping_tree.root_page_id.write() = pre_mapping_root;
                    *self.reverse_tree.root_page_id.write() = pre_reverse_root;
                    self.rollback_pages(&pre_alloc, &pre_free);
                    return Err(e);
                }
                let latest_seq = self.store.get_seq();
                self.bucket_root_cache
                    .write()
                    .insert(name_bytes.to_vec(), latest_seq, new_root);
                Ok(res)
            }
            Err(e) => {
                *self.catalog_tree.root_page_id.write() = pre_catalog_root;
                *self.mapping_tree.root_page_id.write() = pre_mapping_root;
                *self.reverse_tree.root_page_id.write() = pre_reverse_root;
                self.rollback_pages(&pre_alloc, &pre_free);
                Err(e)
            }
        }
    }

    /// Executes multiple operations across different buckets in a single atomic transaction.
    ///
    /// This is more efficient than calling `exec` multiple times because on success it performs
    /// a single superblock update and disk sync at the end.
    pub fn exec_multi<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mut MultiTxn) -> Result<R>,
    {
        let _lock = self.writer_lock.write();

        self.refresh_internal()?;

        let pre_alloc = self.pending_alloc.read().clone();
        let pre_free = self.pending_free.read().clone();
        let pre_catalog_root = *self.catalog_tree.root_page_id.read();
        let pre_mapping_root = *self.mapping_tree.root_page_id.read();
        let pre_reverse_root = *self.reverse_tree.root_page_id.read();

        let mut multi_txn = MultiTxn {
            btree: self,
            bucket_roots: HashMap::new(),
        };

        match f(&mut multi_txn) {
            Ok(res) => {
                let mut updated = Vec::new();
                let commit_res = (|| {
                    for (name, roots) in multi_txn.bucket_roots {
                        if roots.initial_exists && roots.current == roots.initial {
                            continue;
                        }
                        let metadata = BucketMetadata {
                            root_page_id: roots.current,
                        };
                        self.catalog_tree
                            .put(name.as_bytes(), metadata.as_slice())?;
                        updated.push((name.into_bytes(), roots.current));
                    }
                    self.commit_internal()
                })();
                if let Err(e) = commit_res {
                    *self.catalog_tree.root_page_id.write() = pre_catalog_root;
                    *self.mapping_tree.root_page_id.write() = pre_mapping_root;
                    *self.reverse_tree.root_page_id.write() = pre_reverse_root;
                    self.rollback_pages(&pre_alloc, &pre_free);
                    return Err(e);
                }
                let latest_seq = self.store.get_seq();
                let mut cache = self.bucket_root_cache.write();
                for (name, new_root) in updated {
                    cache.insert(name, latest_seq, new_root);
                }
                Ok(res)
            }
            Err(e) => {
                *self.catalog_tree.root_page_id.write() = pre_catalog_root;
                *self.mapping_tree.root_page_id.write() = pre_mapping_root;
                *self.reverse_tree.root_page_id.write() = pre_reverse_root;
                self.rollback_pages(&pre_alloc, &pre_free);
                Err(e)
            }
        }
    }

    fn rollback_pages(&self, pre_alloc: &HashSet<PageId>, pre_free: &[(PageId, u32)]) {
        let mut alloc = self.pending_alloc.write();
        let mut freed_now = Vec::new();
        for &pid in alloc.iter() {
            if !pre_alloc.contains(&pid) {
                freed_now.push(pid);
            }
        }
        for pid in freed_now {
            alloc.remove(&pid);
            let _ = self.store.free_pages(pid, 1);
        }
        drop(alloc);

        let current_free = self.pending_free.read().clone();
        let freed_delta = Self::diff_free_extents(&current_free, pre_free);
        for (pid, nr) in freed_delta {
            let _ = self.store.unfree_pages(pid, nr);
        }

        *self.pending_free.write() = pre_free.to_owned();
    }

    fn diff_free_extents(current: &[(PageId, u32)], base: &[(PageId, u32)]) -> Vec<(PageId, u32)> {
        let mut res = Vec::new();
        let mut j = 0usize;

        for &(cur_start, cur_len) in current {
            let cur_start_u64 = cur_start as u64;
            let cur_end = cur_start_u64 + cur_len as u64;
            let mut start = cur_start_u64;

            while j < base.len() && (base[j].0 as u64) + base[j].1 as u64 <= start {
                j += 1;
            }

            let mut k = j;
            while k < base.len() {
                let (base_start, base_len) = base[k];
                let base_start_u64 = base_start as u64;
                let base_end = base_start_u64 + base_len as u64;

                if base_start_u64 >= cur_end {
                    break;
                }

                if base_start_u64 > start {
                    res.push((start as PageId, (base_start_u64 - start) as u32));
                }

                if base_end >= cur_end {
                    start = cur_end;
                    break;
                }

                start = base_end;
                k += 1;
            }

            if start < cur_end {
                res.push((start as PageId, (cur_end - start) as u32));
            }
        }

        res
    }

    fn max_tree_page_id(store: &dyn PageStore, root_id: PageId) -> Result<PageId> {
        if root_id == 0 {
            return Ok(0);
        }

        let mut max_pid = root_id;
        let mut stack = vec![root_id];

        while let Some(pid) = stack.pop() {
            if pid > max_pid {
                max_pid = pid;
            }
            let node = store.load_node(pid)?;
            if node.is_leaf() {
                for i in 0..node.num_children() {
                    let slot = node.slot_at(i);
                    if !slot.is_inline() {
                        let pages = node.collect_page_ids(store, slot)?;
                        for page in pages {
                            if page > max_pid {
                                max_pid = page;
                            }
                        }
                    }
                }
            } else {
                for i in 0..node.num_children() {
                    let child = node.child_at(i);
                    if child != 0 {
                        stack.push(child);
                    }
                }
            }
        }

        Ok(max_pid)
    }

    /// Executes a read-only transaction on the specified bucket.
    ///
    /// # Warning
    /// Nested calls to `exec` or `view` on the same `BTree` instance are NOT supported
    /// and may lead to deadlocks or undefined behavior.
    pub fn view<F, R>(&self, bucket: &str, f: F) -> Result<R>
    where
        F: FnOnce(&ReadOnlyTxn) -> Result<R>,
    {
        validate_bucket_name(bucket)?;

        let lock = self.writer_lock.read();

        // For view, we also want the freshest data.
        let (mut latest_seq, mut latest_root) = self.store.shared_snapshot();
        let seq_changed = latest_seq != self.start_seq.load(Ordering::Acquire);
        if seq_changed {
            let snapshot = self.store.refresh_sb()?;
            latest_seq = snapshot.seq;
            latest_root = snapshot.catalog_root;
            // Clear cache to avoid stale reads if version moved
            self.store.clear_cache();
            self.logical_store.clear_lid_cache();
            self.apply_local_snapshot(snapshot, true);
        }

        let name_bytes = bucket.as_bytes();
        let cached_tree = self
            .bucket_tree_cache
            .read()
            .get(name_bytes, latest_seq)
            .cloned();
        if let Some(tree) = cached_tree {
            let txn = ReadOnlyTxn { tree, _guard: lock };
            return f(&txn);
        }

        let cached_root = self
            .bucket_root_cache
            .read()
            .get(name_bytes, latest_seq)
            .copied();

        let bucket_root = if let Some(root) = cached_root {
            root
        } else {
            // Use the latest root for the catalog lookup
            let logical_store_obj: Arc<dyn PageStore> = self.logical_store.clone();
            let catalog = Tree::open(
                logical_store_obj,
                Arc::new(RwLock::new(latest_root)),
                Arc::new(RwLock::new(Vec::new())),
                Arc::new(RwLock::new(HashSet::new())),
            )?;

            let metadata_bytes = catalog.get(name_bytes)?;
            let metadata = BucketMetadata::from_slice(&metadata_bytes);
            let root = metadata.root_page_id;
            self.bucket_root_cache
                .write()
                .insert(name_bytes.to_vec(), latest_seq, root);
            root
        };

        let logical_store_obj: Arc<dyn PageStore> = self.logical_store.clone();
        let tree = Arc::new(ReadOnlyTree::new(logical_store_obj, bucket_root)?);
        self.bucket_tree_cache
            .write()
            .insert(name_bytes.to_vec(), latest_seq, tree.clone());
        let txn = ReadOnlyTxn {
            tree: tree.clone(),
            _guard: lock,
        };
        f(&txn)
    }

    /// Delete a bucket by name and persist the change.
    pub fn del_bucket<N>(&self, name: N) -> Result<()>
    where
        N: AsRef<str>,
    {
        let name = name.as_ref();
        validate_bucket_name(name)?;

        let _lock = self.writer_lock.write();

        // ensure we are operating on the latest state
        self.refresh_internal()?;

        let name_bytes = name.as_bytes();
        let metadata_bytes = self.catalog_tree.get(name_bytes)?;
        let bucket_metadata = BucketMetadata::from_slice(&metadata_bytes);

        let mut pages_to_free = Vec::new();
        if bucket_metadata.root_page_id != 0 {
            Tree::collect_tree_pages(
                self.logical_store.as_ref(),
                bucket_metadata.root_page_id,
                &mut pages_to_free,
            )?;
        }

        self.catalog_tree.del(name_bytes)?;
        self.pending_free.write().extend(pages_to_free);
        self.commit_internal()
    }

    fn commit_internal(&self) -> Result<()> {
        let start_seq = self.start_seq.load(Ordering::Acquire);
        let (latest_seq, _) = self.store.shared_snapshot();
        if latest_seq != start_seq {
            return Err(Error::Conflict);
        }
        let snapshot = self.store.cached_snapshot();

        let catalog_root = *self.catalog_tree.root_page_id.read();
        let mapping_root = *self.mapping_tree.root_page_id.read();
        let reverse_root = *self.reverse_tree.root_page_id.read();

        let mut freed_lock = self.pending_free.write();
        let mut alloc_lock = self.pending_alloc.write();

        if freed_lock.is_empty()
            && alloc_lock.is_empty()
            && snapshot.catalog_root == catalog_root
            && snapshot.mapping_root == mapping_root
            && snapshot.reverse_root == reverse_root
        {
            return Ok(());
        }

        self.store
            .commit_roots(catalog_root, mapping_root, reverse_root, &freed_lock)?;
        self.store.sync()?;

        freed_lock.clear();
        alloc_lock.clear();
        let seq = self.store.get_seq();
        self.apply_local_snapshot(
            MetaSnapshot {
                catalog_root,
                mapping_root,
                reverse_root,
                seq,
            },
            false,
        );
        Ok(())
    }

    /// Flushes any pending internal metadata changes held by this handle.
    ///
    /// This is a low-level API. Normal write operations should use [`BTree::exec`],
    /// [`BTree::exec_multi`], or [`BTree::del_bucket`], which already commit on
    /// success.
    ///
    /// If there are no pending page allocations/frees and the current catalog,
    /// mapping, and reverse roots already match the cached snapshot, this is a
    /// no-op and returns `Ok(())`.
    ///
    /// Unlike [`BTree::exec`] and [`BTree::exec_multi`], this method does not
    /// refresh the handle to the latest on-disk state before attempting the
    /// commit. If another writer has already advanced the sequence, this method
    /// returns [`Error::Conflict`].
    pub fn commit(&self) -> Result<()> {
        let _lock = self.writer_lock.write();
        self.commit_internal()
    }

    fn compact_tail_window(
        total_pages: PageId,
        target_bytes: u64,
    ) -> Option<(PageId, PageId, u64)> {
        let usable_pages = total_pages.saturating_sub(2);
        if usable_pages == 0 {
            return None;
        }

        let usable_pages_u64 = usable_pages as u64;
        let mut target_pages_u64 = if usable_pages_u64 <= COMPACT_SMALL_DATA_THRESHOLD_PAGES {
            usable_pages_u64
        } else if target_bytes == 0 {
            ((usable_pages_u64 as f64) * COMPACT_TAIL_RATIO).ceil() as u64
        } else {
            target_bytes.saturating_add(PAGE_SIZE as u64 - 1) / PAGE_SIZE as u64
        };
        if target_pages_u64 == 0 {
            return None;
        }
        if target_pages_u64 > usable_pages_u64 {
            target_pages_u64 = usable_pages_u64;
        }

        let target_pages = target_pages_u64 as PageId;
        let tail_start = total_pages.saturating_sub(target_pages).max(2);
        Some((tail_start, target_pages, target_pages_u64))
    }

    fn compact_move_tail(
        &self,
        total_pages: PageId,
        tail_start: PageId,
        target_pages_u64: u64,
        prealloc: Option<&[PageId]>,
    ) -> Result<(u64, u64, usize)> {
        let mut candidates = Vec::new();
        let mut iter = self
            .reverse_tree
            .iterator_from(&encode_u32_key(tail_start), CacheMode::Default);
        let mut key_buf = Vec::new();
        let mut val_buf = Vec::new();
        while iter.next_ref(&mut key_buf, &mut val_buf) {
            let pid = decode_u32_key(&key_buf)?;
            if pid < tail_start {
                continue;
            }
            if pid >= total_pages {
                break;
            }
            let lid = decode_u32_key(&val_buf)?;
            candidates.push((pid, lid));
            if candidates.len() as u64 >= target_pages_u64 {
                break;
            }
        }

        let mut moved = 0u64;
        let mut prealloc_idx = 0usize;
        let physical_store: &dyn PageStore = self.store.as_ref();

        for (pid, lid) in candidates {
            let new_pid = if let Some(pids) = prealloc {
                if prealloc_idx >= pids.len() {
                    return Err(Error::Internal);
                }
                let pid = pids[prealloc_idx];
                prealloc_idx += 1;
                pid
            } else {
                let mut alloc = self.pending_alloc.write();
                physical_store.alloc_page(&mut alloc)?
            };

            let page = self.store.load_page(pid)?;
            self.store.write_page(new_pid, &page)?;

            let lid_key = encode_u32_key(lid);
            let new_pid_key = encode_u32_key(new_pid);
            let old_pid_key = encode_u32_key(pid);

            self.mapping_tree.put(&lid_key, &new_pid_key)?;
            self.reverse_tree.del(&old_pid_key)?;
            self.reverse_tree.put(&new_pid_key, &lid_key)?;

            {
                let mut freed = self.pending_free.write();
                physical_store.schedule_free(pid, &mut freed)?;
            }

            moved += 1;
        }

        Ok((moved, moved, prealloc_idx))
    }

    fn compact_tail_live_pages(&self, total_pages: PageId, tail_start: PageId) -> Result<u64> {
        let mut total_candidates = 0u64;
        let mut iter = self
            .reverse_tree
            .iterator_from(&encode_u32_key(tail_start), CacheMode::Default);
        let mut key_buf = Vec::new();
        let mut val_buf = Vec::new();

        while iter.next_ref(&mut key_buf, &mut val_buf) {
            let pid = decode_u32_key(&key_buf)?;
            if pid < tail_start {
                continue;
            }
            if pid >= total_pages {
                break;
            }
            total_candidates += 1;
        }

        Ok(total_candidates)
    }

    fn compact_release_unused_prealloc(&self, prealloc: &[PageId], used: usize) -> Result<()> {
        if used >= prealloc.len() {
            return Ok(());
        }

        let unused: Vec<PageId> = prealloc[used..].to_vec();
        {
            let mut alloc = self.pending_alloc.write();
            for pid in &unused {
                alloc.remove(pid);
            }
        }

        for pid in unused {
            self.store.free_pages(pid, 1)?;
        }

        Ok(())
    }

    /// run tail-window compaction
    ///
    /// target_bytes is the desired amount to reclaim, 0 uses the default ratio
    /// this moves live pages out of the tail window and tries to truncate the file
    /// if low-address free pages exceed the threshold, the mover allocates only below the tail
    pub fn compact(&self, target_bytes: u64) -> Result<CompactStats> {
        let _lock = self.writer_lock.write();

        self.refresh_internal()?;

        let total_pages = self.store.get_next_page_id();
        let (tail_start, _target_pages, target_pages_u64) =
            if let Some(params) = Self::compact_tail_window(total_pages, target_bytes) {
                params
            } else {
                return Ok(CompactStats {
                    moved_pages: 0,
                    remaining_candidates: 0,
                });
            };
        if target_pages_u64 == 0 {
            return Ok(CompactStats {
                moved_pages: 0,
                remaining_candidates: 0,
            });
        }
        let total_candidates = self.compact_tail_live_pages(total_pages, tail_start)?;
        if total_candidates == 0 {
            return Ok(CompactStats {
                moved_pages: 0,
                remaining_candidates: 0,
            });
        }
        let planned_moves = total_candidates.min(target_pages_u64);
        // For default compaction, avoid file growth by requiring all relocated pages
        // to be preallocated from low addresses below the compaction tail.
        let strict_no_growth = target_bytes == 0;

        let pre_alloc = self.pending_alloc.read().clone();
        let pre_free = self.pending_free.read().clone();
        let pre_catalog_root = *self.catalog_tree.root_page_id.read();
        let pre_mapping_root = *self.mapping_tree.root_page_id.read();
        let pre_reverse_root = *self.reverse_tree.root_page_id.read();

        let mut prealloc = None;
        if strict_no_growth && self.store.free_pages_below(tail_start) < planned_moves {
            return Ok(CompactStats {
                moved_pages: 0,
                remaining_candidates: total_candidates,
            });
        }
        if let Some(pids) = self
            .store
            .alloc_pages_below(tail_start, planned_moves as PageId)?
        {
            let mut alloc = self.pending_alloc.write();
            for pid in &pids {
                alloc.insert(*pid);
            }
            prealloc = Some(pids);
        }
        if strict_no_growth && prealloc.is_none() {
            return Ok(CompactStats {
                moved_pages: 0,
                remaining_candidates: total_candidates,
            });
        }
        let move_budget = if strict_no_growth {
            planned_moves
        } else {
            target_pages_u64
        };

        let move_result =
            self.compact_move_tail(total_pages, tail_start, move_budget, prealloc.as_deref());

        let (moved, _scanned_candidates, used_prealloc) = match move_result {
            Ok(res) => res,
            Err(e) => {
                *self.catalog_tree.root_page_id.write() = pre_catalog_root;
                *self.mapping_tree.root_page_id.write() = pre_mapping_root;
                *self.reverse_tree.root_page_id.write() = pre_reverse_root;
                self.rollback_pages(&pre_alloc, &pre_free);
                return Err(e);
            }
        };

        if let Some(prealloc) = prealloc.as_ref()
            && let Err(e) = self.compact_release_unused_prealloc(prealloc, used_prealloc)
        {
            *self.catalog_tree.root_page_id.write() = pre_catalog_root;
            *self.mapping_tree.root_page_id.write() = pre_mapping_root;
            *self.reverse_tree.root_page_id.write() = pre_reverse_root;
            self.rollback_pages(&pre_alloc, &pre_free);
            return Err(e);
        }

        if let Err(e) = self.commit_internal() {
            *self.catalog_tree.root_page_id.write() = pre_catalog_root;
            *self.mapping_tree.root_page_id.write() = pre_mapping_root;
            *self.reverse_tree.root_page_id.write() = pre_reverse_root;
            self.rollback_pages(&pre_alloc, &pre_free);
            return Err(e);
        }

        let max_reverse_pid = {
            let mut max_pid = 0u32;
            let mut iter = self.reverse_tree.iterator(CacheMode::Default);
            let mut key_buf = Vec::new();
            let mut val_buf = Vec::new();
            while iter.next_ref(&mut key_buf, &mut val_buf) {
                let pid = decode_u32_key(&key_buf)?;
                if pid > max_pid {
                    max_pid = pid;
                }
            }
            max_pid
        };
        let max_mapping_pid =
            Self::max_tree_page_id(self.store.as_ref(), *self.mapping_tree.root_page_id.read())?;
        let max_reverse_tree_pid =
            Self::max_tree_page_id(self.store.as_ref(), *self.reverse_tree.root_page_id.read())?;
        let max_freelist_pid = self.store.max_freelist_page_id();
        let mut min_end = max_reverse_pid
            .max(max_mapping_pid)
            .max(max_reverse_tree_pid)
            .max(max_freelist_pid)
            .saturating_add(1);
        if min_end < 2 {
            min_end = 2;
        }

        let _ = self.store.try_truncate_tail_with_floor(min_end)?;
        let catalog_root = *self.catalog_tree.root_page_id.read();
        let mapping_root = *self.mapping_tree.root_page_id.read();
        let reverse_root = *self.reverse_tree.root_page_id.read();
        let seq = self.store.get_seq();
        self.apply_local_snapshot(
            MetaSnapshot {
                catalog_root,
                mapping_root,
                reverse_root,
                seq,
            },
            false,
        );
        self.logical_store.clear_lid_cache();

        Ok(CompactStats {
            moved_pages: moved,
            remaining_candidates: total_candidates.saturating_sub(moved),
        })
    }

    fn refresh_internal(&self) -> Result<()> {
        self.pending_free.write().clear();
        self.pending_alloc.write().clear();

        // Fast path: snapshot version unchanged, so current in-memory roots and caches are valid.
        let (latest_seq, _) = self.store.shared_snapshot();
        if latest_seq == self.start_seq.load(Ordering::Acquire) {
            return Ok(());
        }

        self.store.clear_cache();
        self.logical_store.clear_lid_cache();

        let snapshot = self.store.refresh_sb()?;
        self.apply_local_snapshot(snapshot, true);
        Ok(())
    }

    /// Returns all bucket names.
    pub fn buckets(&self) -> Result<Vec<String>> {
        let _lock = self.writer_lock.read();

        // Ensure we see the latest buckets from disk
        let snapshot = self.store.refresh_sb()?;
        let physical_store: Arc<dyn PageStore> = self.store.clone();
        let mapping_tree = Arc::new(Tree::open(
            physical_store.clone(),
            Arc::new(RwLock::new(snapshot.mapping_root)),
            Arc::new(RwLock::new(Vec::new())),
            Arc::new(RwLock::new(HashSet::new())),
        )?);
        let reverse_tree = Arc::new(Tree::open(
            physical_store,
            Arc::new(RwLock::new(snapshot.reverse_root)),
            Arc::new(RwLock::new(Vec::new())),
            Arc::new(RwLock::new(HashSet::new())),
        )?);
        let logical_store = Arc::new(LogicalStore::new(
            self.store.clone(),
            mapping_tree,
            reverse_tree,
            &self.options,
        ));
        let logical_store_obj: Arc<dyn PageStore> = logical_store;
        let catalog = Tree::open(
            logical_store_obj,
            Arc::new(RwLock::new(snapshot.catalog_root)),
            Arc::new(RwLock::new(Vec::new())),
            Arc::new(RwLock::new(HashSet::new())),
        )?;

        let mut iter = catalog.iterator(CacheMode::Default);
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
        (
            self.pending_alloc.read().len(),
            self.pending_free.read().len(),
        )
    }
}

impl Clone for BTree {
    /// Cloning a BTree handle shares the store, writer lock, and pending page tracking.
    fn clone(&self) -> Self {
        let snapshot = {
            let snapshot = self.local_snapshot.read();
            MetaSnapshot {
                catalog_root: snapshot.catalog_root,
                mapping_root: snapshot.mapping_root,
                reverse_root: snapshot.reverse_root,
                seq: snapshot.seq,
            }
        };

        let physical_store: Arc<dyn PageStore> = self.store.clone();
        let mapping_tree = Arc::new(
            Tree::open(
                physical_store.clone(),
                Arc::new(RwLock::new(snapshot.mapping_root)),
                self.pending_free.clone(),
                self.pending_alloc.clone(),
            )
            .expect("failed to clone mapping"),
        );
        let reverse_tree = Arc::new(
            Tree::open(
                physical_store,
                Arc::new(RwLock::new(snapshot.reverse_root)),
                self.pending_free.clone(),
                self.pending_alloc.clone(),
            )
            .expect("failed to clone reverse"),
        );
        let logical_store = Arc::new(LogicalStore::new(
            self.store.clone(),
            mapping_tree.clone(),
            reverse_tree.clone(),
            &self.options,
        ));
        let logical_store_obj: Arc<dyn PageStore> = logical_store.clone();
        let catalog_tree = Arc::new(
            Tree::open(
                logical_store_obj,
                Arc::new(RwLock::new(snapshot.catalog_root)),
                self.pending_free.clone(),
                self.pending_alloc.clone(),
            )
            .expect("failed to clone catalog"),
        );

        Self {
            store: self.store.clone(),
            catalog_tree,
            mapping_tree,
            reverse_tree,
            logical_store,
            pending_free: self.pending_free.clone(),
            pending_alloc: self.pending_alloc.clone(),
            writer_lock: self.writer_lock.clone(),
            start_root_id: Arc::new(AtomicU32::new(snapshot.catalog_root)),
            start_seq: Arc::new(AtomicU64::new(snapshot.seq)),
            local_snapshot: Arc::new(RwLock::new(snapshot)),
            bucket_root_cache: self.bucket_root_cache.clone(),
            bucket_tree_cache: self.bucket_tree_cache.clone(),
            options: self.options.clone(),
            instance_anchor: self.instance_anchor.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::Node;
    use crate::page_store::{Lid, PageStore};

    struct DummyPageStore;

    impl PageStore for DummyPageStore {
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

    fn dummy_read_only_tree() -> Arc<ReadOnlyTree> {
        Arc::new(ReadOnlyTree {
            store: Arc::new(DummyPageStore),
            root_page_id: 0,
            root_node: None,
        })
    }

    #[test]
    fn bucket_root_cache_overwrite_keeps_storage_bounded() {
        let mut cache = BucketRootCache::new(usize::MAX);
        cache.insert(b"a".to_vec(), 1, 1);

        for seq in 1..=64 {
            cache.insert(b"b".to_vec(), seq as u64, seq);
        }

        assert_eq!(cache.entries.len(), 2);
        assert_eq!(cache.slots.len(), 2);
        assert_eq!(cache.get(b"a", 1).copied(), Some(1));
        assert_eq!(cache.get(b"b", 64).copied(), Some(64));
        assert_eq!(cache.get(b"b", 63).copied(), None);
    }

    #[test]
    fn bucket_root_cache_keeps_hot_entry_under_cold_insert_pressure() {
        let mut cache = BucketRootCache::new(2);
        cache.insert(b"a".to_vec(), 1, 1);
        cache.insert(b"b".to_vec(), 1, 2);

        assert_eq!(cache.get(b"a", 1).copied(), Some(1));
        cache.insert(b"c".to_vec(), 1, 3);
        assert_eq!(cache.get(b"a", 1).copied(), Some(1));

        cache.insert(b"d".to_vec(), 1, 4);

        assert_eq!(cache.get(b"a", 1).copied(), Some(1));
        assert_eq!(cache.entries.len(), 2);
        assert_eq!(cache.slots.len(), 2);
    }

    #[test]
    fn bucket_tree_cache_overwrite_keeps_storage_bounded() {
        let mut cache = BucketTreeCache::new(usize::MAX);
        cache.insert(b"a".to_vec(), 1, dummy_read_only_tree());

        let mut expected_tree = dummy_read_only_tree();
        for seq in 1..=64 {
            expected_tree = dummy_read_only_tree();
            cache.insert(b"b".to_vec(), seq, expected_tree.clone());
        }

        assert_eq!(cache.entries.len(), 2);
        assert_eq!(cache.slots.len(), 2);
        assert!(cache.get(b"a", 1).is_some());
        let cached_tree = cache
            .get(b"b", 64)
            .cloned()
            .expect("latest tree should stay cached");
        assert!(Arc::ptr_eq(&cached_tree, &expected_tree));
        assert!(cache.get(b"b", 63).is_none());
    }
}
