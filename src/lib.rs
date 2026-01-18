use std::{collections::HashMap, collections::HashSet, fmt, io, path::Path, sync::Arc};

use parking_lot::{RwLock, RwLockReadGuard};

pub(crate) mod node;
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

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PageId(pub u64);

impl PageId {
    pub const INVALID: Self = Self(0);

    pub fn is_valid(&self) -> bool {
        self.0 != 0
    }

    pub fn to_le_bytes(self) -> [u8; 8] {
        self.0.to_le_bytes()
    }

    pub fn from_le_bytes(bytes: [u8; 8]) -> Self {
        Self(u64::from_le_bytes(bytes))
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FreeNode {
    pub next: u64,
    pub nr_pages: u32,
    pub checksum: u32,
}

impl FreeNode {
    pub fn from_slice(x: &[u8]) -> Self {
        unsafe { std::ptr::read_unaligned(x.as_ptr().cast::<Self>()) }
    }

    pub fn update_checksum(&mut self) {
        self.checksum = 0;
        let s = unsafe {
            std::slice::from_raw_parts(
                (self as *const Self) as *const u8,
                std::mem::size_of::<Self>(),
            )
        };
        self.checksum = crc32c::crc32c(s);
    }

    pub fn validate(&self) -> bool {
        let mut clone = *self;
        clone.checksum = 0;
        let s = unsafe {
            std::slice::from_raw_parts(
                (&clone as *const Self) as *const u8,
                std::mem::size_of::<Self>(),
            )
        };
        crc32c::crc32c(s) == self.checksum
    }
}

pub const MAGIC: u64 = 0x636f776274726565;
pub const VERSION: u32 = 1;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct MetaNode {
    pub magic: u64,
    pub version: u64,
    pub page_size: u64,
    pub root_current: u64,
    pub root_backup: u64,
    pub next_page_id: u64,
    pub free_list_head: u64,
    pub nr_free: u64,
    pub seq: u64,
    pub checksum: u64,
}

impl MetaNode {
    pub fn as_page_slice(&self) -> [u8; crate::node::PAGE_SIZE] {
        let mut buf = [0u8; crate::node::PAGE_SIZE];
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
            version: VERSION as u64,
            page_size: crate::node::PAGE_SIZE as u64,
            root_current: 0,
            root_backup: 0,
            next_page_id: 2, // skip two meta pages
            free_list_head: 0,
            nr_free: 0,
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
        // Torn write detection: if the block is zeroed out by tests, it's invalid.
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
    node::{MAX_KEY_LEN, Node},
    store::Store,
};

struct Route {
    node: Arc<Node>,
    page_id: u64,
    pos: usize,
}

/// encapsulates page allocation and freeing during a transaction, ensuring COW safety
struct TxContext<'a> {
    store: &'a Store,
    freed: &'a mut Vec<(u64, u32)>,
    alloc: &'a mut HashSet<u64>,
}

impl<'a> TxContext<'a> {
    fn new(store: &'a Store, freed: &'a mut Vec<(u64, u32)>, alloc: &'a mut HashSet<u64>) -> Self {
        Self {
            store,
            freed,
            alloc,
        }
    }

    fn alloc_page(&mut self) -> Result<u64> {
        let pid = self.store.alloc_page()?;
        self.alloc.insert(pid);
        Ok(pid)
    }

    fn write_node(&mut self, node: &mut Node) -> Result<u64> {
        let pid = self.alloc_page()?;
        self.store.write_data(&[pid], node.finalize())?;
        Ok(pid)
    }

    fn free_page(&mut self, pid: u64) {
        self.freed.push((pid, 1));
    }
}

pub struct Tree {
    store: Arc<Store>,
    pub root_page_id: Arc<RwLock<u64>>,
    pending_free: Arc<RwLock<Vec<(u64, u32)>>>,
    pending_alloc: Arc<RwLock<HashSet<u64>>>,
}

impl Tree {
    pub fn open(
        store: Arc<Store>,
        root_page_id: Arc<RwLock<u64>>,
        pending_free: Arc<RwLock<Vec<(u64, u32)>>>,
        pending_alloc: Arc<RwLock<HashSet<u64>>>,
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
        mut page_id: u64,
        key: &[u8],
    ) -> Result<(Vec<Route>, Arc<Node>, u64)> {
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
    fn merge_pending(&self, freed: Vec<(u64, u32)>, mut alloc: HashSet<u64>) {
        let mut main_free = self.pending_free.write();
        let mut main_alloc = self.pending_alloc.write();

        for (pid, nr) in freed {
            if alloc.remove(&pid) {
                // If it was allocated and freed in the same operation, it's a transient COW node.
                // It's safe to return it to the store immediately for reuse within this txn.
                let _ = self.store.free_pages(pid, nr);
                continue;
            }
            if main_alloc.remove(&pid) {
                // Previously allocated in this same transaction, now replaced by a newer COW version.
                let _ = self.store.free_pages(pid, nr);
            } else {
                main_free.push((pid, nr));
            }
        }
        main_alloc.extend(alloc);
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        if key.len() > MAX_KEY_LEN {
            return Err(Error::TooLarge);
        }
        // use extra vec to reduce lock contention and ensure crash safety
        let mut freed = Vec::new();
        let mut alloc = HashSet::new();

        let result = self.execute_put(key, value, &mut freed, &mut alloc);

        if result.is_err() {
            // reclaim pages allocated during this failed operation to prevent leak
            for pid in alloc {
                let _ = self.store.free_pages(pid, 1);
            }
            return result;
        }

        self.merge_pending(freed, alloc);
        Ok(())
    }

    fn execute_put(
        &self,
        key: &[u8],
        value: &[u8],
        freed: &mut Vec<(u64, u32)>,
        alloc: &mut HashSet<u64>,
    ) -> Result<()> {
        let mut ctx = TxContext::new(&self.store, freed, alloc);
        let mut root_lock = self.root_page_id.write();
        let current_root_id = *root_lock;

        // root is empty
        if current_root_id == 0 {
            let mut node = Node::new_leaf();
            node.put(ctx.store, key, value, ctx.freed)?;
            *root_lock = ctx.write_node(&mut node)?;
            return Ok(());
        }

        // 1. find insert node
        let root_node = self.store.load_node(current_root_id)?;
        let (mut stack, leaf_node_arc, leaf_id) =
            self.traverse_to_leaf(root_node, current_root_id, key)?;

        let mut current_node = (*leaf_node_arc).clone();

        // 2. modify leaf node and get split info (if any)
        let mut split_info = self.apply_insert(&mut ctx, &mut current_node, key, value)?;

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
            ctx.free_page(parent_id);
        }

        // 4. handle root node split
        if let Some((sep, mut rhs)) = split_info {
            let rhs_id = ctx.write_node(&mut rhs)?;
            let mut new_root = Node::new_branch();
            // left child points to the COW version of the old root; its key is always empty in branch nodes
            new_root.put(ctx.store, &[], &new_child_id.to_le_bytes(), ctx.freed)?;
            // right child points to the newly split node
            new_root.put(ctx.store, &sep, &rhs_id.to_le_bytes(), ctx.freed)?;
            *root_lock = ctx.write_node(&mut new_root)?;
        } else {
            // root did not split, simply update root pointer
            *root_lock = new_child_id;
        }

        Ok(())
    }

    fn apply_insert(
        &self,
        ctx: &mut TxContext,
        node: &mut Node,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<(Vec<u8>, Node)>> {
        match node.put(ctx.store, key, value, ctx.freed) {
            Ok(()) => Ok(None),
            Err(Error::NoSpace) => {
                // split to right
                let (sep, mut rhs) = node.split()?;
                if key < &sep {
                    node.put(ctx.store, key, value, ctx.freed)?;
                } else {
                    rhs.put(ctx.store, key, value, ctx.freed)?;
                }
                Ok(Some((sep, rhs)))
            }
            Err(e) => Err(e),
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Vec<u8>> {
        let root_id = *self.root_page_id.read();
        if root_id == 0 {
            return Err(Error::NotFound);
        }

        let root_node = self.store.load_node(root_id)?;
        let mut current = root_node;
        loop {
            if current.is_leaf() {
                return current.get(&self.store, key);
            }
            let pos = current.search(key).unwrap_or_else(|x| x.saturating_sub(1));
            current = self.store.load_node(current.child_at(pos))?;
        }
    }

    pub fn del(&self, key: &[u8]) -> Result<()> {
        // use extra vec to reduce lock contention and ensure crash safety
        let mut freed = Vec::new();
        let mut alloc = HashSet::new();

        let result = self.execute_del(key, &mut freed, &mut alloc);

        if result.is_err() {
            // reclaim pages allocated during this failed operation to prevent leak
            for pid in alloc {
                let _ = self.store.free_pages(pid, 1);
            }
            return result;
        }

        self.merge_pending(freed, alloc);
        Ok(())
    }

    fn execute_del(
        &self,
        key: &[u8],
        freed: &mut Vec<(u64, u32)>,
        alloc: &mut HashSet<u64>,
    ) -> Result<()> {
        let mut ctx = TxContext::new(&self.store, freed, alloc);
        let mut root_lock = self.root_page_id.write();
        let current_root_id = *root_lock;

        if current_root_id == 0 {
            return Err(Error::NotFound);
        }

        // 1. find insert node
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
            ctx.free_page(parent_id);
        }

        // 4. root collapse optimization
        // if root is a branch node with only one child, elevate child to be the new root
        if new_child_id != 0 {
            loop {
                let node_id = new_child_id;
                let node = self.store.load_node(node_id)?;
                if !node.is_leaf() && node.num_children() == 1 {
                    let child_id = node.child_at(0);
                    ctx.free_page(node_id);
                    new_child_id = child_id;
                } else {
                    break;
                }
            }
        }

        *root_lock = new_child_id;
        Ok(())
    }

    pub fn commit(&self) -> Result<()> {
        let root_id = *self.root_page_id.read();

        let mut freed_lock = self.pending_free.write();
        let mut alloc_lock = self.pending_alloc.write();

        if freed_lock.is_empty() && alloc_lock.is_empty() && root_id == self.store.get_root_id() {
            return Ok(());
        }

        let next_seq = self.store.get_seq() + 1;

        // 1. ensure all data pages are persisted to disk
        self.store.sync()?;

        // 2. write pending Log
        self.store
            .write_pending_log(next_seq, &freed_lock, &alloc_lock)?;

        // 3. update superblock (commit point)
        self.store.update_root(root_id)?;

        // --- transaction committed successfully ---

        // 4. physically reclaim old pages
        for (pid, nr) in freed_lock.iter() {
            let _ = self.store.free_pages(*pid, *nr);
        }

        // 5. persist free list changes and clear log
        let _ = self.store.clear_pending_log();

        // 6. Double-Write: Update superblock AGAIN.
        // This ensures that if we crash/corrupt this update, we fall back to the PREVIOUS update (step 3).
        // The previous update (Step 3) points to the NEW root, but with the OLD free list.
        // This is safe because the old free list just means we might leak some pages (which we just freed),
        // but the DATA (New Root) is valid and persisted.
        self.store.update_root(root_id)?;

        freed_lock.clear();
        alloc_lock.clear();

        Ok(())
    }

    fn collect_tree_pages(store: &Store, root_id: u64, pages: &mut Vec<(u64, u32)>) -> Result<()> {
        if root_id == 0 {
            return Ok(());
        }

        let mut stack = vec![root_id];

        while let Some(current_id) = stack.pop() {
            let node = store.load_node(current_id)?;
            pages.push((current_id, 1));

            for i in 0..node.num_children() {
                if node.is_leaf() {
                    let slot = node.slot_at(i);
                    if !slot.is_inline() {
                        node.free_slot_pages(store, slot, pages)?;
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

    pub fn iterator(&self) -> TreeIterator {
        let root_id = *self.root_page_id.read();
        TreeIterator::new(self.store.clone(), root_id)
    }
}

pub struct TreeIterator {
    store: Arc<Store>,
    stack: Vec<(Arc<Node>, usize)>,
    current_leaf: Option<(Arc<Node>, usize)>,
}

impl TreeIterator {
    fn new(store: Arc<Store>, root_id: u64) -> Self {
        let mut iter = Self {
            store,
            stack: Vec::new(),
            current_leaf: None,
        };

        if root_id != 0
            && let Ok(node) = iter.store.load_node(root_id)
        {
            iter.push_node(node);
        }
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
                    } else if let Ok(pages) = leaf.collect_page_ids(&self.store, slot) {
                        val_buf.resize(slot.value_len(), 0);
                        if self.store.read_data(&pages, val_buf).is_err() {
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
                    if let Ok(child_node) = self.store.load_node(child_id) {
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

pub struct Txn<'a> {
    pub(crate) tree: Tree,
    pub(crate) _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a> Txn<'a> {
    pub fn put<K, V>(&mut self, key: K, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.tree.put(key.as_ref(), value.as_ref())
    }

    pub fn get<K>(&self, key: K) -> Result<Vec<u8>>
    where
        K: AsRef<[u8]>,
    {
        self.tree.get(key.as_ref())
    }

    pub fn del<K>(&mut self, key: K) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        self.tree.del(key.as_ref())
    }

    pub fn iter(&self) -> TreeIterator {
        self.tree.iterator()
    }
}

pub struct ReadOnlyTxn<'a> {
    pub(crate) tree: Tree,
    pub(crate) _guard: RwLockReadGuard<'a, ()>,
}

impl<'a> ReadOnlyTxn<'a> {
    pub fn get<K>(&self, key: K) -> Result<Vec<u8>>
    where
        K: AsRef<[u8]>,
    {
        self.tree.get(key.as_ref())
    }

    pub fn iter(&self) -> TreeIterator {
        self.tree.iterator()
    }
}

pub struct MultiTxn<'a> {
    btree: &'a BTree,
    bucket_roots: HashMap<String, u64>,
}

impl<'a> MultiTxn<'a> {
    pub fn execute<F, R>(&mut self, bucket: &str, f: F) -> Result<R>
    where
        F: FnOnce(&mut Txn) -> Result<R>,
    {
        let name_bytes = bucket.as_bytes();

        let initial_root = if let Some(&root) = self.bucket_roots.get(bucket) {
            root
        } else {
            match self.btree.catalog_tree.get(name_bytes) {
                Ok(bytes) => BucketMetadata::from_slice(&bytes).root_page_id,
                Err(Error::NotFound) => 0,
                Err(e) => return Err(e),
            }
        };

        let tree = Tree::open(
            self.btree.store.clone(),
            Arc::new(RwLock::new(initial_root)),
            self.btree.pending_free.clone(),
            self.btree.pending_alloc.clone(),
        )?;

        let mut txn = Txn {
            tree,
            _marker: std::marker::PhantomData,
        };

        let res = f(&mut txn);
        if res.is_ok() {
            let new_root = *txn.tree.root_page_id.read();
            self.bucket_roots.insert(bucket.to_string(), new_root);
        }
        res
    }
}

pub struct BucketMetadata {
    pub(crate) root_page_id: u64,
}

impl BucketMetadata {
    pub fn from_slice(x: &[u8]) -> Self {
        assert!(x.len() >= std::mem::size_of::<Self>());
        unsafe { std::ptr::read_unaligned(x.as_ptr().cast::<Self>()) }
    }

    pub fn as_slice(&self) -> &[u8] {
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
    pub(crate) pending_free: Arc<RwLock<Vec<(u64, u32)>>>,
    pub(crate) pending_alloc: Arc<RwLock<HashSet<u64>>>,
    pub(crate) writer_lock: Arc<RwLock<()>>,
    pub(crate) start_root_id: Arc<RwLock<u64>>,
}

impl BTree {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let store = Arc::new(Store::open(path)?);
        let initial_catalog_root_id = store.get_root_id();
        let pending_free = Arc::new(RwLock::new(Vec::new()));
        let pending_alloc = Arc::new(RwLock::new(HashSet::new()));
        let catalog_tree_root_lock = Arc::new(RwLock::new(initial_catalog_root_id));
        let catalog_tree = Arc::new(Tree::open(
            store.clone(),
            catalog_tree_root_lock,
            pending_free.clone(),
            pending_alloc.clone(),
        )?);

        Ok(Self {
            store,
            catalog_tree,
            pending_free,
            pending_alloc,
            writer_lock: Arc::new(RwLock::new(())),
            start_root_id: Arc::new(RwLock::new(initial_catalog_root_id)),
        })
    }

    /// Executes a read-write transaction on the specified bucket. It'll create a new one if given bucket is not exist
    ///
    /// The transaction is automatically committed if the closure returns `Ok`.
    /// If the closure returns `Err`, the transaction is rolled back (allocated pages are reclaimed).
    ///
    /// # Warning
    /// Nested calls to `exec` or `view` on the same `BTree` instance are NOT supported
    /// and may lead to deadlocks or undefined behavior.
    pub fn exec<F, R>(&self, bucket: &str, f: F) -> Result<R>
    where
        F: FnOnce(&mut Txn) -> Result<R>,
    {
        let _lock = self.writer_lock.write();

        // Auto-refresh to the latest disk state before starting a new transaction.
        // This makes the "Session" always start from the freshest data.
        self.refresh_internal()?;

        // Check if there's an existing bucket
        let name_bytes = bucket.as_bytes();
        let initial_root = match self.catalog_tree.get(name_bytes) {
            Ok(bytes) => BucketMetadata::from_slice(&bytes).root_page_id,
            Err(Error::NotFound) => 0,
            Err(e) => return Err(e),
        };

        // Snapshot pending state for rollback
        let pre_alloc = self.pending_alloc.read().clone();
        let pre_free = self.pending_free.read().clone();
        let pre_catalog_root = *self.catalog_tree.root_page_id.read();

        let tree = Tree::open(
            self.store.clone(),
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
                let metadata = BucketMetadata {
                    root_page_id: new_root,
                };
                self.catalog_tree.put(name_bytes, metadata.as_slice())?;
                if let Err(e) = self.commit_internal() {
                    // Rollback catalog and pages on commit failure (e.g. Conflict)
                    *self.catalog_tree.root_page_id.write() = pre_catalog_root;
                    self.rollback_pages(&pre_alloc, &pre_free);
                    return Err(e);
                }
                Ok(res)
            }
            Err(e) => {
                *self.catalog_tree.root_page_id.write() = pre_catalog_root;
                self.rollback_pages(&pre_alloc, &pre_free);
                Err(e)
            }
        }
    }

    /// Executes multiple operations across different buckets in a single atomic transaction.
    ///
    /// This is more efficient than calling `exec` multiple times because it only performs
    /// a single disk sync and superblock update at the end.
    pub fn exec_multi<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mut MultiTxn) -> Result<R>,
    {
        let _lock = self.writer_lock.write();

        self.refresh_internal()?;

        let pre_alloc = self.pending_alloc.read().clone();
        let pre_free = self.pending_free.read().clone();
        let pre_catalog_root = *self.catalog_tree.root_page_id.read();

        let mut multi_txn = MultiTxn {
            btree: self,
            bucket_roots: HashMap::new(),
        };

        match f(&mut multi_txn) {
            Ok(res) => {
                for (name, new_root) in multi_txn.bucket_roots {
                    let metadata = BucketMetadata {
                        root_page_id: new_root,
                    };
                    self.catalog_tree
                        .put(name.as_bytes(), metadata.as_slice())?;
                }
                if let Err(e) = self.commit_internal() {
                    *self.catalog_tree.root_page_id.write() = pre_catalog_root;
                    self.rollback_pages(&pre_alloc, &pre_free);
                    return Err(e);
                }
                Ok(res)
            }
            Err(e) => {
                *self.catalog_tree.root_page_id.write() = pre_catalog_root;
                self.rollback_pages(&pre_alloc, &pre_free);
                Err(e)
            }
        }
    }

    fn rollback_pages(&self, pre_alloc: &HashSet<u64>, pre_free: &[(u64, u32)]) {
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
        *self.pending_free.write() = pre_free.to_owned();
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
        let lock = self.writer_lock.read();

        // For view, we also want the freshest data.
        let latest_root = self.store.refresh_sb()?;
        if latest_root != *self.start_root_id.read() {
            // Clear cache to avoid stale reads if version moved
            self.store.clear_cache();
        }

        let name_bytes = bucket.as_bytes();
        // Use the latest root for the catalog lookup
        let catalog = Tree::open(
            self.store.clone(),
            Arc::new(RwLock::new(latest_root)),
            Arc::new(RwLock::new(Vec::new())),
            Arc::new(RwLock::new(HashSet::new())),
        )?;

        let metadata_bytes = catalog.get(name_bytes)?;
        let metadata = BucketMetadata::from_slice(&metadata_bytes);

        let tree = Tree::open(
            self.store.clone(),
            Arc::new(RwLock::new(metadata.root_page_id)),
            Arc::new(RwLock::new(Vec::new())),
            Arc::new(RwLock::new(HashSet::new())),
        )?;

        let txn = ReadOnlyTxn { tree, _guard: lock };

        f(&txn)
    }

    pub fn del_bucket<N>(&self, name: N) -> Result<()>
    where
        N: AsRef<str>,
    {
        let _lock = self.writer_lock.write();

        // Ensure we are operating on the latest state
        self.refresh_internal()?;

        let name_bytes = name.as_ref().as_bytes();
        let metadata_bytes = self.catalog_tree.get(name_bytes)?;
        let bucket_metadata = BucketMetadata::from_slice(&metadata_bytes);

        let mut pages_to_free = Vec::new();
        if bucket_metadata.root_page_id != 0 {
            Tree::collect_tree_pages(
                &self.store,
                bucket_metadata.root_page_id,
                &mut pages_to_free,
            )?;
        }

        self.catalog_tree.del(name_bytes)?;
        self.pending_free.write().extend(pages_to_free);
        self.commit_internal()
    }

    fn commit_internal(&self) -> Result<()> {
        let current_disk_root = self.store.refresh_sb()?;
        if current_disk_root != *self.start_root_id.read() {
            return Err(Error::Conflict);
        }

        self.catalog_tree.commit()?;

        let new_root = self.store.get_root_id();
        *self.start_root_id.write() = new_root;
        Ok(())
    }

    pub fn commit(&self) -> Result<()> {
        let _lock = self.writer_lock.write();
        self.commit_internal()
    }

    fn refresh_internal(&self) -> Result<()> {
        self.pending_free.write().clear();
        self.pending_alloc.write().clear();
        self.store.clear_cache();

        let latest_root = self.store.refresh_sb()?;
        *self.catalog_tree.root_page_id.write() = latest_root;
        *self.start_root_id.write() = latest_root;
        Ok(())
    }

    /// Returns an iterator over all bucket names.
    pub fn buckets(&self) -> Result<Vec<String>> {
        let _lock = self.writer_lock.read();

        // Ensure we see the latest buckets from disk
        let latest_root = self.store.refresh_sb()?;
        let catalog = Tree::open(
            self.store.clone(),
            Arc::new(RwLock::new(latest_root)),
            Arc::new(RwLock::new(Vec::new())),
            Arc::new(RwLock::new(HashSet::new())),
        )?;

        let mut iter = catalog.iterator();
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
    /// Cloning a BTree handle creates a NEW independent transaction context.
    fn clone(&self) -> Self {
        let initial_root = *self.start_root_id.read();

        let catalog_tree = Arc::new(
            Tree::open(
                self.store.clone(),
                Arc::new(RwLock::new(initial_root)),
                self.pending_free.clone(),
                self.pending_alloc.clone(),
            )
            .expect("failed to clone catalog"),
        );

        Self {
            store: self.store.clone(),
            catalog_tree,
            pending_free: self.pending_free.clone(),
            pending_alloc: self.pending_alloc.clone(),
            writer_lock: self.writer_lock.clone(),
            start_root_id: Arc::new(RwLock::new(initial_root)),
        }
    }
}
