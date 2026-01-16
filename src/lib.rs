use std::{
    collections::{HashMap, HashSet},
    fmt, io,
    path::Path,
    sync::Arc,
};

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
        assert!(x.len() >= std::mem::size_of::<Self>());
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
        assert!(x.len() >= std::mem::size_of::<Self>());
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

    pub fn validate(&self) -> Result<()> {
        if self.magic != MAGIC {
            return Err(Error::Corruption);
        }
        if self.version != VERSION as u64 {
            return Err(Error::Corruption);
        }
        if self.page_size != crate::node::PAGE_SIZE as u64 {
            return Err(Error::Corruption);
        }

        let mut clone = *self;
        clone.checksum = 0;
        let s = unsafe {
            std::slice::from_raw_parts(
                (&clone as *const Self) as *const u8,
                std::mem::size_of::<Self>(),
            )
        };
        if crc32c::crc32c(s) as u64 != self.checksum {
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
    root_page_id: Arc<RwLock<u64>>,
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
                let _ = self.store.free_pages(pid, nr);
                continue;
            }
            if main_alloc.remove(&pid) {
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

        // use RAII pattern to manage commit context.
        // if function exits early via ? error, ctx will automatically extend freed/alloc vectors back to global lists
        // on drop
        let mut ctx = CommitContext::new(&self.pending_free, &self.pending_alloc);

        if ctx.freed.is_empty() && ctx.alloc.is_empty() && root_id == self.store.get_root_id() {
            return Ok(());
        }

        let next_seq = self.store.get_seq() + 1;

        // 1. ensure all data pages are persisted to disk
        self.store.sync()?;

        // 2. write pending Log
        self.store
            .write_pending_log(next_seq, &ctx.freed, &ctx.alloc)?;

        // 3. update superblock (commit point)
        self.store.update_root(root_id)?;

        // --- transaction committed successfully ---

        // 4. physically reclaim old pages
        for (pid, nr) in &ctx.freed {
            let _ = self.store.free_pages(*pid, *nr);
        }

        // 5. persist free list changes and clear log
        let _ = self.store.update_root(root_id);
        let _ = self.store.clear_pending_log();

        ctx.done();
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
                let slot = node.slot_at(i);
                if node.is_leaf() {
                    if !slot.is_inline() {
                        node.free_slot_pages(store, slot, pages)?;
                    }
                } else {
                    let child_id = slot.page_id[0];
                    if child_id != 0 {
                        stack.push(child_id);
                    }
                }
            }
        }
        Ok(())
    }

    fn iterator(&self) -> TreeIterator {
        let root_id = *self.root_page_id.read();
        TreeIterator::new(self.store.clone(), root_id)
    }
}

/// helper structure: manages page lists during commit, supporting automatic rollback
struct CommitContext<'a> {
    main_free: &'a RwLock<Vec<(u64, u32)>>,
    main_alloc: &'a RwLock<HashSet<u64>>,
    freed: Vec<(u64, u32)>,
    alloc: HashSet<u64>,
    success: bool,
}

impl<'a> CommitContext<'a> {
    fn new(pf: &'a RwLock<Vec<(u64, u32)>>, pa: &'a RwLock<HashSet<u64>>) -> Self {
        let mut pf_lock = pf.write();
        let mut pa_lock = pa.write();
        Self {
            main_free: pf,
            main_alloc: pa,
            freed: std::mem::take(&mut *pf_lock),
            alloc: std::mem::take(&mut *pa_lock),
            success: false,
        }
    }

    fn done(&mut self) {
        self.success = true;
    }
}

impl Drop for CommitContext<'_> {
    fn drop(&mut self) {
        if !self.success {
            // if transaction failed (e.g., error return), return data to global lists for next retry
            if !self.freed.is_empty() {
                self.main_free
                    .write()
                    .extend(std::mem::take(&mut self.freed));
            }
            if !self.alloc.is_empty() {
                self.main_alloc
                    .write()
                    .extend(std::mem::take(&mut self.alloc));
            }
        }
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
            && let Ok(node_bytes) = iter.store.load_page(root_id)
            && let Ok(node) = Node::from_raw(node_bytes)
        {
            iter.push_node(Arc::new(node));
        }
        iter
    }

    fn push_node(&mut self, node: Arc<Node>) {
        if node.is_leaf() {
            self.current_leaf = Some((node, 0));
        } else {
            self.stack.push((node, 0));
            self.descend();
        }
    }

    fn descend(&mut self) {
        loop {
            let (node, idx) = match self.stack.last_mut() {
                Some(x) => x,
                None => return,
            };

            if *idx >= node.num_children() {
                break;
            }

            let child_id = node.child_at(*idx);
            match self.store.load_page(child_id) {
                Ok(bytes) => match Node::from_raw(bytes) {
                    Ok(child_node) => {
                        let child_node_arc = Arc::new(child_node);
                        if child_node_arc.is_leaf() {
                            self.current_leaf = Some((child_node_arc, 0));
                            return;
                        } else {
                            self.stack.push((child_node_arc, 0));
                        }
                    }
                    Err(_) => return,
                },
                Err(_) => return,
            }
        }
    }

    fn next_ref(&mut self, key_buf: &mut Vec<u8>, val_buf: &mut Vec<u8>) -> bool {
        loop {
            if let Some((ref leaf, ref mut idx)) = self.current_leaf {
                if *idx < leaf.num_children() {
                    let slot = leaf.slot_at(*idx);

                    key_buf.clear();
                    key_buf.extend_from_slice(leaf.key_at(*idx));

                    val_buf.clear();
                    if slot.is_inline() {
                        val_buf.extend_from_slice(leaf.value_at(*idx));
                    } else {
                        match leaf.collect_page_ids(&self.store, slot) {
                            Ok(pages) => {
                                val_buf.resize(slot.value_len(), 0);
                                if self.store.read_data(&pages, val_buf).is_err() {
                                    *idx += 1;
                                    continue;
                                }
                            }
                            Err(_) => {
                                *idx += 1;
                                continue;
                            }
                        }
                    }

                    *idx += 1;
                    return true;
                } else {
                    self.current_leaf = None;
                }
            } else {
                if self.stack.is_empty() {
                    return false;
                }

                while let Some((_, idx)) = self.stack.last() {
                    let node = &self.stack.last().unwrap().0;
                    if *idx + 1 >= node.num_children() {
                        self.stack.pop();
                        if self.stack.is_empty() {
                            return false;
                        }
                    } else {
                        break;
                    }
                }

                if let Some((_, idx)) = self.stack.last_mut() {
                    *idx += 1;
                    self.descend();
                }
            }
        }
    }
}

pub struct BucketIterator<'a> {
    tree_iter: TreeIterator,
    key_buf: Vec<u8>,
    val_buf: Vec<u8>,
    _guard: RwLockReadGuard<'a, ()>,
}

impl<'a> Iterator for BucketIterator<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        if self
            .tree_iter
            .next_ref(&mut self.key_buf, &mut self.val_buf)
        {
            unsafe {
                Some((
                    std::mem::transmute::<&[u8], &[u8]>(self.key_buf.as_slice()),
                    std::mem::transmute::<&[u8], &[u8]>(self.val_buf.as_slice()),
                ))
            }
        } else {
            None
        }
    }
}

pub struct BTreeIterator<'a> {
    tree_iter: TreeIterator,
    main_tree: Arc<BTree>,
    key_buf: Vec<u8>,
    val_buf: Vec<u8>,
    _guard: RwLockReadGuard<'a, ()>,
}

impl<'a> Iterator for BTreeIterator<'a> {
    type Item = Bucket;

    fn next(&mut self) -> Option<Self::Item> {
        if self
            .tree_iter
            .next_ref(&mut self.key_buf, &mut self.val_buf)
        {
            let name_str = std::str::from_utf8(&self.key_buf).expect("invalid bucket name");
            Some(
                self.main_tree
                    .get_bucket(name_str)
                    .expect("failed to get bucket during iteration"),
            )
        } else {
            None
        }
    }
}

pub struct BucketMetadata {
    pub root_page_id: u64,
}

impl BucketMetadata {
    pub fn as_slice(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                (self as *const Self) as *const u8,
                std::mem::size_of::<Self>(),
            )
        }
    }

    pub fn from_slice(x: &[u8]) -> Self {
        assert!(x.len() >= std::mem::size_of::<Self>());
        unsafe { std::ptr::read_unaligned(x.as_ptr().cast::<Self>()) }
    }
}

pub struct Bucket {
    store: Arc<Store>,
    name: Vec<u8>,
    root_page_id: Arc<RwLock<u64>>,
    main_tree: Arc<BTree>,
}

impl Bucket {
    pub fn iter(&self) -> Result<BucketIterator<'_>> {
        let lock = self.main_tree.writer_lock.read();
        let tree = Tree::open(
            self.store.clone(),
            self.root_page_id.clone(),
            self.main_tree.pending_free.clone(),
            self.main_tree.pending_alloc.clone(),
        )?;
        Ok(BucketIterator {
            tree_iter: tree.iterator(),
            key_buf: Vec::new(),
            val_buf: Vec::new(),
            _guard: lock,
        })
    }

    pub fn name(&self) -> &str {
        std::str::from_utf8(&self.name).expect("bad data")
    }

    pub fn put<K, V>(&self, key: K, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let _lock = self.main_tree.writer_lock.write();

        // Read-your-writes: check pending updates first, then the shared lock
        // if shared bucket is not commited it will use root_page_id and when commit the first committer wins
        let current_root_id = self
            .main_tree
            .pending_bucket_updates
            .read()
            .get(&self.name)
            .cloned()
            .unwrap_or_else(|| *self.root_page_id.read());

        let tree_instance = Tree::open(
            self.store.clone(),
            Arc::new(RwLock::new(current_root_id)),
            self.main_tree.pending_free.clone(),
            self.main_tree.pending_alloc.clone(),
        )?;
        tree_instance.put(key.as_ref(), value.as_ref())?;

        let new_root_id = *tree_instance.root_page_id.read();

        if current_root_id != new_root_id {
            self.main_tree
                .pending_bucket_updates
                .write()
                .insert(self.name.clone(), new_root_id);
        }

        Ok(())
    }

    pub fn get<K>(&self, key: K) -> Result<Vec<u8>>
    where
        K: AsRef<[u8]>,
    {
        let _lock = self.main_tree.writer_lock.read();

        // Read-your-writes
        let current_root_id = self
            .main_tree
            .pending_bucket_updates
            .read()
            .get(&self.name)
            .cloned()
            .unwrap_or_else(|| *self.root_page_id.read());

        let tree_instance = Tree::open(
            self.store.clone(),
            Arc::new(RwLock::new(current_root_id)),
            self.main_tree.pending_free.clone(),
            self.main_tree.pending_alloc.clone(),
        )?;
        tree_instance.get(key.as_ref())
    }

    pub fn del<K>(&self, key: K) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        let _lock = self.main_tree.writer_lock.write();

        let current_root_id = self
            .main_tree
            .pending_bucket_updates
            .read()
            .get(&self.name)
            .cloned()
            .unwrap_or_else(|| *self.root_page_id.read());

        let tree_instance = Tree::open(
            self.store.clone(),
            Arc::new(RwLock::new(current_root_id)),
            self.main_tree.pending_free.clone(),
            self.main_tree.pending_alloc.clone(),
        )?;
        tree_instance.del(key.as_ref())?;

        let new_root_id = *tree_instance.root_page_id.read();

        if current_root_id != new_root_id {
            self.main_tree
                .pending_bucket_updates
                .write()
                .insert(self.name.clone(), new_root_id);
        }
        Ok(())
    }
}

pub struct BTree {
    store: Arc<Store>,
    catalog_tree: Arc<Tree>,
    pending_free: Arc<RwLock<Vec<(u64, u32)>>>,
    pending_alloc: Arc<RwLock<HashSet<u64>>>,
    writer_lock: Arc<RwLock<()>>,
    buckets: Arc<RwLock<HashMap<Vec<u8>, Arc<RwLock<u64>>>>>,
    pending_bucket_updates: Arc<RwLock<HashMap<Vec<u8>, u64>>>,
    start_root_id: Arc<RwLock<u64>>,
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
            buckets: Arc::new(RwLock::new(HashMap::new())),
            pending_bucket_updates: Arc::new(RwLock::new(HashMap::new())),
            start_root_id: Arc::new(RwLock::new(initial_catalog_root_id)),
        })
    }

    pub fn new_bucket<N>(&self, name: N) -> Result<Bucket>
    where
        N: AsRef<str>,
    {
        let _lock = self.writer_lock.write();
        let name_bytes = name.as_ref().as_bytes();

        if self.catalog_tree.get(name_bytes).is_ok() {
            return Err(Error::Duplicate);
        }

        let new_bucket_root_id = 0;

        // Add to pending updates so it's written during commit
        self.pending_bucket_updates
            .write()
            .insert(name_bytes.to_vec(), new_bucket_root_id);

        let root_page_id_lock = Arc::new(RwLock::new(new_bucket_root_id));
        self.buckets
            .write()
            .insert(name_bytes.to_vec(), root_page_id_lock.clone());

        Ok(Bucket {
            store: self.store.clone(),
            name: name_bytes.to_vec(),
            root_page_id: root_page_id_lock,
            main_tree: Arc::new(self.clone()),
        })
    }

    /// Refreshes the BTree instance to the latest disk state.                                                                        │
    /// This will discard all pending (uncommitted) changes in this instance.                                                         │
    ///                                                                                                                               │
    /// # Best Practice (Conflict Resolution)                                                                                         │
    /// When `commit()` returns `Err(Error::Conflict)`, it means another instance or process                                          │
    /// has committed changes since this transaction started. To resolve this:                                                        │
    /// 1. Call `refresh()` to sync with the latest disk state and clear stale pending updates.                                       │
    /// 2. Re-fetch your buckets and re-apply your operations (puts/deletes).                                                         │
    /// 3. Call `commit()` again.  
    pub fn refresh(&self) -> Result<()> {
        let _lock = self.writer_lock.write();
        self.pending_bucket_updates.write().clear();
        self.pending_free.write().clear();
        self.pending_alloc.write().clear();

        let latest_root = self.store.refresh_sb()?;
        let mut cat_root = self.catalog_tree.root_page_id.write();
        *cat_root = latest_root;
        *self.start_root_id.write() = latest_root;
        Ok(())
    }

    pub fn get_bucket<N>(&self, name: N) -> Result<Bucket>
    where
        N: AsRef<str>,
    {
        let name_bytes = name.as_ref().as_bytes();

        // 1. Check pending updates (Read-Your-Writes)
        // If the bucket was created or modified in the current transaction,
        // it might not be in the catalog_tree yet.
        if let Some(&pending_root) = self.pending_bucket_updates.read().get(name_bytes) {
            let mut buckets = self.buckets.write();
            let lock = buckets
                .entry(name_bytes.to_vec())
                .or_insert_with(|| Arc::new(RwLock::new(pending_root)));

            return Ok(Bucket {
                store: self.store.clone(),
                name: name_bytes.to_vec(),
                root_page_id: lock.clone(),
                main_tree: Arc::new(self.clone()),
            });
        }

        // 2. Load metadata from our snapshot
        let metadata_bytes = self.catalog_tree.get(name_bytes)?;
        let metadata = BucketMetadata::from_slice(&metadata_bytes);

        let mut buckets = self.buckets.write();
        let lock = buckets
            .entry(name_bytes.to_vec())
            .or_insert_with(|| Arc::new(RwLock::new(metadata.root_page_id)));

        // Ensure the lock matches our snapshot root
        let mut lock_val = lock.write();
        if *lock_val != metadata.root_page_id {
            *lock_val = metadata.root_page_id;
        }

        Ok(Bucket {
            store: self.store.clone(),
            name: name_bytes.to_vec(),
            root_page_id: lock.clone(),
            main_tree: Arc::new(self.clone()),
        })
    }

    pub fn del_bucket<N>(&self, name: N) -> Result<()>
    where
        N: AsRef<str>,
    {
        let _lock = self.writer_lock.write();
        let bucket_metadata = match self.catalog_tree.get(name.as_ref().as_bytes()) {
            Ok(bytes) => BucketMetadata::from_slice(&bytes),
            Err(e) => return Err(e),
        };

        let mut pages_to_free = Vec::new();
        if bucket_metadata.root_page_id != 0 {
            Tree::collect_tree_pages(
                &self.store,
                bucket_metadata.root_page_id,
                &mut pages_to_free,
            )?;
        }

        self.catalog_tree.del(name.as_ref().as_bytes())?;
        self.buckets.write().remove(name.as_ref().as_bytes());
        self.pending_free.write().extend(pages_to_free);

        Ok(())
    }

    pub fn commit(&self) -> Result<()> {
        let _lock = self.writer_lock.write();

        // 1. Conflict Detection (CAS)
        // Check if someone else committed while we were working
        let current_disk_root = self.store.refresh_sb()?;
        let start_root = *self.start_root_id.read();

        if current_disk_root != start_root {
            return Err(Error::Conflict);
        }

        // 2. Apply all pending bucket updates to the catalog tree
        let pending_updates = self.pending_bucket_updates.read().clone();
        for (name, new_root) in &pending_updates {
            let metadata = BucketMetadata {
                root_page_id: *new_root,
            };
            self.catalog_tree.put(name, metadata.as_slice())?;
        }

        // 3. Commit the catalog tree (and thus the whole Store)
        self.catalog_tree.commit()?;

        // 4. Success! Synchronize all memory locks
        let new_committed_root = self.store.get_root_id();
        let buckets = self.buckets.write();
        for (name, new_root) in pending_updates {
            if let Some(lock) = buckets.get(&name) {
                *lock.write() = new_root;
            }
        }

        // 5. Reset transaction state
        *self.start_root_id.write() = new_committed_root;
        self.pending_bucket_updates.write().clear();

        Ok(())
    }

    pub fn iter(&self) -> BTreeIterator<'_> {
        let lock = self.writer_lock.read();
        let committed_catalog_root = self.store.get_root_id();
        let tree_iter = TreeIterator::new(self.store.clone(), committed_catalog_root);

        BTreeIterator {
            tree_iter,
            main_tree: Arc::new(self.clone()),
            key_buf: Vec::new(),
            val_buf: Vec::new(),
            _guard: lock,
        }
    }
}

impl Clone for BTree {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            catalog_tree: self.catalog_tree.clone(),
            pending_free: self.pending_free.clone(),
            pending_alloc: self.pending_alloc.clone(),
            writer_lock: self.writer_lock.clone(),
            buckets: self.buckets.clone(),
            pending_bucket_updates: self.pending_bucket_updates.clone(),
            start_root_id: self.start_root_id.clone(),
        }
    }
}
