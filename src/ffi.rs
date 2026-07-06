#![cfg(feature = "ffi")]

use crate::page_store::PageStore;
use crate::{
    BTree, BucketMetadata, Error, MAX_KEY_LEN, OpenOptions, PageId, ReadOnlyTree, SyncMode, Tree,
    TreeIterator, validate_bucket_name,
};
use parking_lot::RwLock;
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};
use std::ptr;
use std::sync::Arc;

const ERR_NOT_FOUND: c_int = -1;
const ERR_CORRUPTION: c_int = -2;
const ERR_TOO_LARGE: c_int = -3;
const ERR_INTERNAL: c_int = -4;
const ERR_NO_SPACE: c_int = -5;
const ERR_IO: c_int = -6;
const ERR_INVALID: c_int = -7;
const ERR_DUPLICATE: c_int = -8;
const ERR_CONFLICT: c_int = -9;
#[allow(dead_code)]
const ERR_UNKNOWN: c_int = -1000;

pub const BTREE_SYNC_ADAPTIVE: u32 = 0;
pub const BTREE_SYNC_DATA: u32 = 1;
pub const BTREE_SYNC_ALL: u32 = 2;

struct LastError {
    code: c_int,
    msg: Option<CString>,
}

thread_local! {
    static LAST_ERROR: RefCell<LastError> = const { RefCell::new(LastError { code: 0, msg: None }) };
}

fn map_error(err: Error) -> c_int {
    match err {
        Error::NotFound => ERR_NOT_FOUND,
        Error::Corruption => ERR_CORRUPTION,
        Error::TooLarge => ERR_TOO_LARGE,
        Error::Internal => ERR_INTERNAL,
        Error::NoSpace => ERR_NO_SPACE,
        Error::IoError => ERR_IO,
        Error::Invalid => ERR_INVALID,
        Error::Duplicate => ERR_DUPLICATE,
        Error::Conflict => ERR_CONFLICT,
    }
}

fn set_last_error(code: c_int, msg: &str) -> c_int {
    let cmsg = CString::new(msg).unwrap_or_else(|_| CString::new("invalid error message").unwrap());
    LAST_ERROR.with(|slot| {
        *slot.borrow_mut() = LastError {
            code,
            msg: Some(cmsg),
        };
    });
    code
}

fn set_last_error_from(err: Error) -> c_int {
    let code = map_error(err);
    set_last_error(code, &err.to_string())
}

fn clear_last_error() {
    LAST_ERROR.with(|slot| {
        *slot.borrow_mut() = LastError { code: 0, msg: None };
    });
}

fn invalid_arg(msg: &str) -> c_int {
    set_last_error(ERR_INVALID, msg)
}

fn cstr_to_str<'a>(ptr: *const c_char) -> std::result::Result<&'a str, c_int> {
    if ptr.is_null() {
        return Err(invalid_arg("null pointer"));
    }
    let cstr = unsafe { CStr::from_ptr(ptr) };
    match cstr.to_str() {
        Ok(s) => Ok(s),
        Err(_) => Err(invalid_arg("invalid utf-8")),
    }
}

fn bytes_from_raw<'a>(ptr: *const u8, len: usize) -> std::result::Result<&'a [u8], c_int> {
    if len == 0 {
        return Ok(&[]);
    }
    if ptr.is_null() {
        return Err(invalid_arg("null pointer"));
    }
    Ok(unsafe { std::slice::from_raw_parts(ptr, len) })
}

#[derive(Clone, Copy)]
struct WriteTxnState {
    tree: *const Tree,
}

enum TxnKind {
    Write(WriteTxnState),
    Read(*const ReadOnlyTree),
}

#[repr(C)]
pub struct Txn {
    kind: TxnKind,
}

#[repr(C)]
pub struct BTreeOpenOptions {
    pub node_cache_capacity: usize,
    pub lid_pid_cache_capacity: usize,
    pub lid_pid_hot_cache_capacity: usize,
    pub bucket_root_cache_capacity: usize,
    pub bucket_tree_cache_capacity: usize,
    pub sync_mode: u32,
}

impl BTreeOpenOptions {
    fn from_rust(options: OpenOptions) -> Self {
        Self {
            node_cache_capacity: options.node_cache_capacity,
            lid_pid_cache_capacity: options.lid_pid_cache_capacity,
            lid_pid_hot_cache_capacity: options.lid_pid_hot_cache_capacity,
            bucket_root_cache_capacity: options.bucket_root_cache_capacity,
            bucket_tree_cache_capacity: options.bucket_tree_cache_capacity,
            sync_mode: match options.sync_mode {
                SyncMode::Adaptive => BTREE_SYNC_ADAPTIVE,
                SyncMode::Data => BTREE_SYNC_DATA,
                SyncMode::All => BTREE_SYNC_ALL,
            },
        }
    }

    fn to_rust(&self) -> crate::Result<OpenOptions> {
        let sync_mode = match self.sync_mode {
            BTREE_SYNC_ADAPTIVE => SyncMode::Adaptive,
            BTREE_SYNC_DATA => SyncMode::Data,
            BTREE_SYNC_ALL => SyncMode::All,
            _ => return Err(Error::Invalid),
        };
        Ok(OpenOptions {
            node_cache_capacity: self.node_cache_capacity,
            lid_pid_cache_capacity: self.lid_pid_cache_capacity,
            lid_pid_hot_cache_capacity: self.lid_pid_hot_cache_capacity,
            bucket_root_cache_capacity: self.bucket_root_cache_capacity,
            bucket_tree_cache_capacity: self.bucket_tree_cache_capacity,
            sync_mode,
        })
    }
}

#[repr(C)]
pub struct BTreeIter {
    iter: TreeIterator,
}

struct FfiBucket {
    initial_exists: bool,
    initial_root: PageId,
    tree: Box<Tree>,
    handle: Box<Txn>,
}

#[repr(C)]
pub struct MultiTxn {
    btree: *const BTree,
    buckets: HashMap<String, FfiBucket>,
}

impl MultiTxn {
    fn get_or_open_bucket(&mut self, name: &str) -> crate::Result<&mut FfiBucket> {
        validate_bucket_name(name)?;

        if self.buckets.contains_key(name) {
            return Ok(self.buckets.get_mut(name).unwrap());
        }

        let btree = unsafe { &*self.btree };
        let name_bytes = name.as_bytes();
        let (initial_exists, initial_root) = match btree.catalog_tree.get(name_bytes) {
            Ok(bytes) => (true, BucketMetadata::from_slice(&bytes).root_page_id),
            Err(Error::NotFound) => (false, 0),
            Err(e) => return Err(e),
        };

        let logical_store_obj: Arc<dyn PageStore> = btree.logical_store.clone();
        let tree = Tree::open(
            logical_store_obj,
            Arc::new(RwLock::new(initial_root)),
            btree.pending_free.clone(),
            btree.pending_alloc.clone(),
        )?;

        let tree = Box::new(tree);
        let handle = Box::new(Txn {
            kind: TxnKind::Write(WriteTxnState {
                tree: tree.as_ref() as *const Tree,
            }),
        });

        self.buckets.insert(
            name.to_string(),
            FfiBucket {
                initial_exists,
                initial_root,
                tree,
                handle,
            },
        );
        Ok(self.buckets.get_mut(name).unwrap())
    }
}

type TxnCallback = Option<unsafe extern "C" fn(*mut Txn, *mut c_void) -> c_int>;
type MultiTxnCallback = Option<unsafe extern "C" fn(*mut MultiTxn, *mut c_void) -> c_int>;
type IterItemCallback =
    Option<unsafe extern "C" fn(*const u8, usize, *const u8, usize, *mut c_void) -> c_int>;

fn rollback_multi(
    btree: &BTree,
    pre_alloc: &HashSet<PageId>,
    pre_free: &[(PageId, u32)],
    pre_catalog_root: PageId,
    pre_mapping_root: PageId,
    pre_reverse_root: PageId,
) {
    *btree.catalog_tree.root_page_id.write() = pre_catalog_root;
    *btree.mapping_tree.root_page_id.write() = pre_mapping_root;
    *btree.reverse_tree.root_page_id.write() = pre_reverse_root;
    btree.rollback_pages(pre_alloc, pre_free);
}

#[unsafe(no_mangle)]
pub extern "C" fn btree_open(path: *const c_char, out: *mut *mut BTree) -> c_int {
    let defaults = OpenOptions::default();
    btree_open_with_options(
        path,
        &BTreeOpenOptions::from_rust(defaults) as *const BTreeOpenOptions,
        out,
    )
}

#[unsafe(no_mangle)]
pub extern "C" fn btree_default_open_options(out: *mut BTreeOpenOptions) -> c_int {
    if out.is_null() {
        return invalid_arg("null pointer");
    }
    unsafe {
        *out = BTreeOpenOptions::from_rust(OpenOptions::default());
    }
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn btree_open_with_options(
    path: *const c_char,
    options: *const BTreeOpenOptions,
    out: *mut *mut BTree,
) -> c_int {
    if out.is_null() {
        return invalid_arg("null pointer");
    }
    if options.is_null() {
        return invalid_arg("null pointer");
    }
    unsafe {
        *out = ptr::null_mut();
    }
    let path = match cstr_to_str(path) {
        Ok(s) => s,
        Err(code) => return code,
    };
    let options = match unsafe { &*options }.to_rust() {
        Ok(options) => options,
        Err(e) => return set_last_error_from(e),
    };

    match BTree::open_with_options(path, options) {
        Ok(tree) => {
            let boxed = Box::new(tree);
            unsafe {
                *out = Box::into_raw(boxed);
            }
            0
        }
        Err(e) => set_last_error_from(e),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn btree_max_key_len() -> usize {
    MAX_KEY_LEN
}

#[unsafe(no_mangle)]
pub extern "C" fn btree_close(db: *mut BTree) {
    if db.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(db));
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn btree_exec(
    db: *mut BTree,
    bucket: *const c_char,
    cb: TxnCallback,
    ctx: *mut c_void,
) -> c_int {
    if db.is_null() {
        return invalid_arg("null pointer");
    }
    let cb = match cb {
        Some(f) => f,
        None => return invalid_arg("null callback"),
    };
    let bucket = match cstr_to_str(bucket) {
        Ok(s) => s,
        Err(code) => return code,
    };

    let btree = unsafe { &*db };
    let cb_rc = Cell::new(0);

    let result = btree.exec(bucket, |txn| {
        let mut handle = Txn {
            kind: TxnKind::Write(WriteTxnState {
                tree: &txn.tree as *const Tree,
            }),
        };
        let rc = unsafe { cb(&mut handle as *mut Txn, ctx) };
        if rc == 0 {
            Ok(())
        } else {
            cb_rc.set(rc);
            Err(Error::Internal)
        }
    });

    let rc = cb_rc.get();
    if rc != 0 {
        return rc;
    }

    match result {
        Ok(_) => 0,
        Err(e) => set_last_error_from(e),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn btree_view(
    db: *mut BTree,
    bucket: *const c_char,
    cb: TxnCallback,
    ctx: *mut c_void,
) -> c_int {
    if db.is_null() {
        return invalid_arg("null pointer");
    }
    let cb = match cb {
        Some(f) => f,
        None => return invalid_arg("null callback"),
    };
    let bucket = match cstr_to_str(bucket) {
        Ok(s) => s,
        Err(code) => return code,
    };

    let btree = unsafe { &*db };
    let cb_rc = Cell::new(0);

    let result = btree.view(bucket, |txn| {
        let mut handle = Txn {
            kind: TxnKind::Read(txn.tree.as_ref() as *const ReadOnlyTree),
        };
        let rc = unsafe { cb(&mut handle as *mut Txn, ctx) };
        if rc == 0 {
            Ok(())
        } else {
            cb_rc.set(rc);
            Err(Error::Internal)
        }
    });

    let rc = cb_rc.get();
    if rc != 0 {
        return rc;
    }

    match result {
        Ok(_) => 0,
        Err(e) => set_last_error_from(e),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn btree_exec_multi(
    db: *mut BTree,
    cb: MultiTxnCallback,
    ctx: *mut c_void,
) -> c_int {
    if db.is_null() {
        return invalid_arg("null pointer");
    }
    let cb = match cb {
        Some(f) => f,
        None => return invalid_arg("null callback"),
    };

    let btree = unsafe { &*db };
    let _lock = btree.writer_lock.write();

    if let Err(e) = btree.refresh_internal() {
        return set_last_error_from(e);
    }

    let pre_alloc = btree.pending_alloc.read().clone();
    let pre_free = btree.pending_free.read().clone();
    let pre_catalog_root = *btree.catalog_tree.root_page_id.read();
    let pre_mapping_root = *btree.mapping_tree.root_page_id.read();
    let pre_reverse_root = *btree.reverse_tree.root_page_id.read();

    let mut mtxn = MultiTxn {
        btree,
        buckets: HashMap::new(),
    };

    let rc = unsafe { cb(&mut mtxn as *mut MultiTxn, ctx) };
    if rc != 0 {
        rollback_multi(
            btree,
            &pre_alloc,
            &pre_free,
            pre_catalog_root,
            pre_mapping_root,
            pre_reverse_root,
        );
        return rc;
    }

    let mut updated = Vec::new();
    for (name, bucket) in mtxn.buckets.iter() {
        let new_root = *bucket.tree.root_page_id.read();
        if bucket.initial_exists && new_root == bucket.initial_root {
            continue;
        }
        let metadata = BucketMetadata {
            root_page_id: new_root,
        };
        if let Err(e) = btree.catalog_tree.put(name.as_bytes(), metadata.as_slice()) {
            rollback_multi(
                btree,
                &pre_alloc,
                &pre_free,
                pre_catalog_root,
                pre_mapping_root,
                pre_reverse_root,
            );
            return set_last_error_from(e);
        }
        updated.push((name.as_bytes().to_vec(), new_root));
    }

    if let Err(e) = btree.commit_internal() {
        rollback_multi(
            btree,
            &pre_alloc,
            &pre_free,
            pre_catalog_root,
            pre_mapping_root,
            pre_reverse_root,
        );
        return set_last_error_from(e);
    }

    let latest_seq = btree.store.get_seq();
    let mut cache = btree.bucket_root_cache.write();
    for (name, new_root) in updated {
        cache.insert(name, latest_seq, new_root);
    }

    0
}

#[unsafe(no_mangle)]
pub extern "C" fn mtxn_bucket(
    mtxn: *mut MultiTxn,
    bucket: *const c_char,
    out: *mut *mut Txn,
) -> c_int {
    if mtxn.is_null() || out.is_null() {
        return invalid_arg("null pointer");
    }
    let bucket = match cstr_to_str(bucket) {
        Ok(s) => s,
        Err(code) => return code,
    };

    let mtxn = unsafe { &mut *mtxn };
    match mtxn.get_or_open_bucket(bucket) {
        Ok(bucket) => {
            unsafe {
                *out = bucket.handle.as_mut() as *mut Txn;
            }
            0
        }
        Err(e) => {
            unsafe {
                *out = ptr::null_mut();
            }
            set_last_error_from(e)
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn txn_get(
    txn: *const Txn,
    key: *const u8,
    klen: usize,
    out: *mut *mut u8,
    out_len: *mut usize,
) -> c_int {
    if txn.is_null() || out.is_null() || out_len.is_null() {
        return invalid_arg("null pointer");
    }
    unsafe {
        *out = ptr::null_mut();
        *out_len = 0;
    }
    let key = match bytes_from_raw(key, klen) {
        Ok(s) => s,
        Err(code) => return code,
    };

    let txn = unsafe { &*txn };
    let result = match txn.kind {
        TxnKind::Write(state) => unsafe { (&*state.tree).get(key) },
        TxnKind::Read(ptr) => unsafe { (&*ptr).get(key) },
    };

    match result {
        Ok(val) => {
            if val.is_empty() {
                return 0;
            }
            let boxed = val.into_boxed_slice();
            let len = boxed.len();
            let ptr = Box::into_raw(boxed) as *mut u8;
            unsafe {
                *out = ptr;
                *out_len = len;
            }
            0
        }
        Err(e) => set_last_error_from(e),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn txn_put(
    txn: *mut Txn,
    key: *const u8,
    klen: usize,
    val: *const u8,
    vlen: usize,
) -> c_int {
    if txn.is_null() {
        return invalid_arg("null pointer");
    }
    let key = match bytes_from_raw(key, klen) {
        Ok(s) => s,
        Err(code) => return code,
    };
    let val = match bytes_from_raw(val, vlen) {
        Ok(s) => s,
        Err(code) => return code,
    };

    let txn = unsafe { &*txn };
    match txn.kind {
        TxnKind::Write(state) => match unsafe { (&*state.tree).put(key, val) } {
            Ok(_) => 0,
            Err(e) => set_last_error_from(e),
        },
        TxnKind::Read(_) => set_last_error(ERR_INVALID, "read only transaction"),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn txn_update(
    txn: *mut Txn,
    key: *const u8,
    klen: usize,
    val: *const u8,
    vlen: usize,
    updated: *mut c_int,
) -> c_int {
    if txn.is_null() || updated.is_null() {
        return invalid_arg("null pointer");
    }
    unsafe {
        *updated = 0;
    }
    let key = match bytes_from_raw(key, klen) {
        Ok(s) => s,
        Err(code) => return code,
    };
    let val = match bytes_from_raw(val, vlen) {
        Ok(s) => s,
        Err(code) => return code,
    };

    let txn = unsafe { &*txn };
    match txn.kind {
        TxnKind::Write(state) => match unsafe { (&*state.tree).update(key, val) } {
            Ok(was_updated) => {
                unsafe {
                    *updated = if was_updated { 1 } else { 0 };
                }
                0
            }
            Err(e) => set_last_error_from(e),
        },
        TxnKind::Read(_) => set_last_error(ERR_INVALID, "read only transaction"),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn txn_del(txn: *mut Txn, key: *const u8, klen: usize) -> c_int {
    if txn.is_null() {
        return invalid_arg("null pointer");
    }
    let key = match bytes_from_raw(key, klen) {
        Ok(s) => s,
        Err(code) => return code,
    };

    let txn = unsafe { &*txn };
    match txn.kind {
        TxnKind::Write(state) => match unsafe { (&*state.tree).del(key) } {
            Ok(_) => 0,
            Err(e) => set_last_error_from(e),
        },
        TxnKind::Read(_) => set_last_error(ERR_INVALID, "read only transaction"),
    }
}

fn txn_make_iter(txn: &Txn, uncached: bool) -> Box<BTreeIter> {
    let iter = match txn.kind {
        TxnKind::Write(state) => unsafe {
            if uncached {
                (&*state.tree).iterator(crate::CacheMode::ByPass)
            } else {
                (&*state.tree).iterator(crate::CacheMode::Default)
            }
        },
        TxnKind::Read(ptr) => unsafe {
            if uncached {
                (&*ptr).iterator(crate::CacheMode::ByPass)
            } else {
                (&*ptr).iterator(crate::CacheMode::Default)
            }
        },
    };
    Box::new(BTreeIter { iter })
}

#[unsafe(no_mangle)]
pub extern "C" fn txn_iter(txn: *const Txn, out: *mut *mut BTreeIter) -> c_int {
    if txn.is_null() || out.is_null() {
        return invalid_arg("null pointer");
    }
    let txn = unsafe { &*txn };
    let iter = txn_make_iter(txn, false);
    unsafe {
        *out = Box::into_raw(iter);
    }
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn txn_iter_uncached(txn: *const Txn, out: *mut *mut BTreeIter) -> c_int {
    if txn.is_null() || out.is_null() {
        return invalid_arg("null pointer");
    }
    let txn = unsafe { &*txn };
    let iter = txn_make_iter(txn, true);
    unsafe {
        *out = Box::into_raw(iter);
    }
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn btree_iter_next(
    iter: *mut BTreeIter,
    cb: IterItemCallback,
    ctx: *mut c_void,
) -> c_int {
    if iter.is_null() {
        return invalid_arg("null pointer");
    }
    let cb = match cb {
        Some(cb) => cb,
        None => return invalid_arg("null callback"),
    };

    let iter = unsafe { &mut *iter };
    let mut key = Vec::new();
    let mut val = Vec::new();
    if !iter.iter.next_ref(&mut key, &mut val) {
        return 1;
    }
    unsafe { cb(key.as_ptr(), key.len(), val.as_ptr(), val.len(), ctx) }
}

#[unsafe(no_mangle)]
pub extern "C" fn btree_iter_close(iter: *mut BTreeIter) {
    if iter.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(iter));
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn btree_free(ptr: *mut u8, len: usize) {
    if ptr.is_null() || len == 0 {
        return;
    }
    unsafe {
        let slice = std::slice::from_raw_parts_mut(ptr, len);
        drop(Box::from_raw(slice));
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn btree_last_error(msg: *mut *const c_char, len: *mut usize) -> c_int {
    LAST_ERROR.with(|slot| {
        let state = slot.borrow();
        if !msg.is_null() {
            unsafe {
                *msg = ptr::null();
            }
        }
        if !len.is_null() {
            unsafe {
                *len = 0;
            }
        }
        if state.code != 0
            && let Some(ref cmsg) = state.msg
        {
            if !msg.is_null() {
                unsafe {
                    *msg = cmsg.as_ptr();
                }
            }
            if !len.is_null() {
                unsafe {
                    *len = cmsg.as_bytes().len();
                }
            }
        }
        state.code
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn btree_last_error_clear() {
    clear_last_error();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ptr;
    use tempfile::TempDir;

    struct UpdateCtx {
        key: Vec<u8>,
        value: Vec<u8>,
        updated: c_int,
        rc: c_int,
    }

    struct UpdateThenPutCtx {
        missing_key: Vec<u8>,
        missing_value: Vec<u8>,
        present_key: Vec<u8>,
        present_value: Vec<u8>,
        updated: c_int,
        update_rc: c_int,
        put_rc: c_int,
    }

    struct IterCollectCtx {
        pairs: Vec<(Vec<u8>, Vec<u8>)>,
    }

    unsafe extern "C" fn update_only_cb(txn: *mut Txn, ctx: *mut c_void) -> c_int {
        let ctx = unsafe { &mut *(ctx as *mut UpdateCtx) };
        let mut updated = -1;
        let rc = txn_update(
            txn,
            ctx.key.as_ptr(),
            ctx.key.len(),
            ctx.value.as_ptr(),
            ctx.value.len(),
            &mut updated,
        );
        ctx.updated = updated;
        ctx.rc = rc;
        if rc == 0 { 0 } else { 1 }
    }

    unsafe extern "C" fn update_then_put_cb(txn: *mut Txn, ctx: *mut c_void) -> c_int {
        let ctx = unsafe { &mut *(ctx as *mut UpdateThenPutCtx) };
        let mut updated = -1;
        ctx.update_rc = txn_update(
            txn,
            ctx.missing_key.as_ptr(),
            ctx.missing_key.len(),
            ctx.missing_value.as_ptr(),
            ctx.missing_value.len(),
            &mut updated,
        );
        ctx.updated = updated;
        if ctx.update_rc != 0 {
            return 1;
        }

        ctx.put_rc = txn_put(
            txn,
            ctx.present_key.as_ptr(),
            ctx.present_key.len(),
            ctx.present_value.as_ptr(),
            ctx.present_value.len(),
        );
        if ctx.put_rc == 0 { 0 } else { 2 }
    }

    unsafe extern "C" fn multi_update_only_cb(mtxn: *mut MultiTxn, ctx: *mut c_void) -> c_int {
        let ctx = unsafe { &mut *(ctx as *mut UpdateCtx) };
        let mut txn = ptr::null_mut();
        let bucket_rc = mtxn_bucket(mtxn, c"ffi_bucket".as_ptr(), &mut txn);
        if bucket_rc != 0 {
            ctx.rc = bucket_rc;
            return 1;
        }

        let mut updated = -1;
        let rc = txn_update(
            txn,
            ctx.key.as_ptr(),
            ctx.key.len(),
            ctx.value.as_ptr(),
            ctx.value.len(),
            &mut updated,
        );
        ctx.updated = updated;
        ctx.rc = rc;
        if rc == 0 { 0 } else { 2 }
    }

    unsafe extern "C" fn collect_iter_item(
        key_ptr: *const u8,
        key_len: usize,
        val_ptr: *const u8,
        val_len: usize,
        ctx: *mut c_void,
    ) -> c_int {
        let ctx = unsafe { &mut *(ctx as *mut IterCollectCtx) };
        let key = unsafe { std::slice::from_raw_parts(key_ptr, key_len) }.to_vec();
        let val = unsafe { std::slice::from_raw_parts(val_ptr, val_len) }.to_vec();
        ctx.pairs.push((key, val));
        0
    }

    unsafe extern "C" fn fill_iter_bucket(txn: *mut Txn, _ctx: *mut c_void) -> c_int {
        let rc = txn_put(txn, b"a".as_ptr(), 1, b"1".as_ptr(), 1);
        if rc != 0 {
            return rc;
        }
        txn_put(txn, b"b".as_ptr(), 1, b"2".as_ptr(), 1)
    }

    #[test]
    fn txn_update_missing_key_reports_false() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("ffi_update_missing_bucket.db");
        let path = CString::new(db_path.to_str().unwrap()).unwrap();
        let bucket = CString::new("ffi_bucket").unwrap();
        let mut db = ptr::null_mut();

        assert_eq!(btree_open(path.as_ptr(), &mut db), 0);

        let mut ctx = UpdateCtx {
            key: b"missing".to_vec(),
            value: b"value".to_vec(),
            updated: -1,
            rc: -1,
        };
        assert_eq!(
            btree_exec(
                db,
                bucket.as_ptr(),
                Some(update_only_cb),
                &mut ctx as *mut UpdateCtx as *mut c_void,
            ),
            0
        );
        assert_eq!(ctx.rc, 0);
        assert_eq!(ctx.updated, 0);
        assert!(
            unsafe { (&*db).buckets().unwrap() }
                .iter()
                .any(|bucket_name| bucket_name == "ffi_bucket"),
            "successful ffi update(false) should materialize the empty bucket"
        );

        btree_close(db);
    }

    #[test]
    fn txn_update_miss_then_put_still_materializes_bucket() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("ffi_update_then_put.db");
        let path = CString::new(db_path.to_str().unwrap()).unwrap();
        let bucket = CString::new("ffi_bucket").unwrap();
        let mut db = ptr::null_mut();

        assert_eq!(btree_open(path.as_ptr(), &mut db), 0);

        let mut ctx = UpdateThenPutCtx {
            missing_key: b"missing".to_vec(),
            missing_value: b"value".to_vec(),
            present_key: b"present".to_vec(),
            present_value: b"value".to_vec(),
            updated: -1,
            update_rc: -1,
            put_rc: -1,
        };
        assert_eq!(
            btree_exec(
                db,
                bucket.as_ptr(),
                Some(update_then_put_cb),
                &mut ctx as *mut UpdateThenPutCtx as *mut c_void,
            ),
            0
        );
        assert_eq!(ctx.update_rc, 0);
        assert_eq!(ctx.updated, 0);
        assert_eq!(ctx.put_rc, 0);
        assert_eq!(
            unsafe {
                (&*db)
                    .view("ffi_bucket", |txn| txn.get(b"present"))
                    .unwrap()
            },
            b"value".to_vec()
        );

        btree_close(db);
    }

    #[test]
    fn txn_update_missing_key_reports_false_in_exec_multi() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("ffi_multi_update_missing_bucket.db");
        let path = CString::new(db_path.to_str().unwrap()).unwrap();
        let mut db = ptr::null_mut();

        assert_eq!(btree_open(path.as_ptr(), &mut db), 0);

        let mut ctx = UpdateCtx {
            key: b"missing".to_vec(),
            value: b"value".to_vec(),
            updated: -1,
            rc: -1,
        };
        assert_eq!(
            btree_exec_multi(
                db,
                Some(multi_update_only_cb),
                &mut ctx as *mut UpdateCtx as *mut c_void,
            ),
            0
        );
        assert_eq!(ctx.rc, 0);
        assert_eq!(ctx.updated, 0);
        assert!(
            unsafe { (&*db).buckets().unwrap() }
                .iter()
                .any(|bucket_name| bucket_name == "ffi_bucket"),
            "successful ffi multi update(false) should materialize the empty bucket"
        );

        btree_close(db);
    }

    #[test]
    fn ffi_exposes_current_max_key_len() {
        assert_eq!(btree_max_key_len(), MAX_KEY_LEN);
    }

    #[test]
    fn ffi_default_open_options_match_rust_defaults() {
        let mut options = BTreeOpenOptions {
            node_cache_capacity: 0,
            lid_pid_cache_capacity: 0,
            lid_pid_hot_cache_capacity: 0,
            bucket_root_cache_capacity: 0,
            bucket_tree_cache_capacity: 0,
            sync_mode: u32::MAX,
        };
        assert_eq!(btree_default_open_options(&mut options), 0);
        let defaults = OpenOptions::default();
        assert_eq!(options.node_cache_capacity, defaults.node_cache_capacity);
        assert_eq!(options.lid_pid_cache_capacity, defaults.lid_pid_cache_capacity);
        assert_eq!(
            options.lid_pid_hot_cache_capacity,
            defaults.lid_pid_hot_cache_capacity
        );
        assert_eq!(
            options.bucket_root_cache_capacity,
            defaults.bucket_root_cache_capacity
        );
        assert_eq!(
            options.bucket_tree_cache_capacity,
            defaults.bucket_tree_cache_capacity
        );
        assert_eq!(options.sync_mode, BTREE_SYNC_ADAPTIVE);
    }

    #[test]
    fn ffi_open_with_options_rejects_invalid_sync_mode() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("ffi_invalid_sync_mode.db");
        let path = CString::new(db_path.to_str().unwrap()).unwrap();
        let mut db = ptr::null_mut();
        let mut options = BTreeOpenOptions::from_rust(OpenOptions::default());
        options.sync_mode = 99;

        assert_eq!(
            btree_open_with_options(path.as_ptr(), &options, &mut db),
            ERR_INVALID
        );
        assert!(db.is_null());
    }

    #[test]
    fn ffi_iter_and_iter_uncached_follow_rust_order() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("ffi_iter_uncached.db");
        let path = CString::new(db_path.to_str().unwrap()).unwrap();
        let bucket = CString::new("ffi_bucket").unwrap();
        let mut db = ptr::null_mut();

        assert_eq!(btree_open(path.as_ptr(), &mut db), 0);
        assert_eq!(btree_exec(db, bucket.as_ptr(), Some(fill_iter_bucket), ptr::null_mut()), 0);

        let mut normal_pairs = IterCollectCtx { pairs: Vec::new() };
        let mut uncached_pairs = IterCollectCtx { pairs: Vec::new() };

        let view_result = unsafe {
            (&*db)
                .view("ffi_bucket", |txn| {
                    let ffi_txn = Txn {
                        kind: TxnKind::Read(txn.tree.as_ref() as *const ReadOnlyTree),
                    };

                    let mut iter = ptr::null_mut();
                    assert_eq!(txn_iter(&ffi_txn, &mut iter), 0);
                    assert_eq!(
                        btree_iter_next(
                            iter,
                            Some(collect_iter_item),
                            &mut normal_pairs as *mut IterCollectCtx as *mut c_void
                        ),
                        0
                    );
                    assert_eq!(
                        btree_iter_next(
                            iter,
                            Some(collect_iter_item),
                            &mut normal_pairs as *mut IterCollectCtx as *mut c_void
                        ),
                        0
                    );
                    assert_eq!(
                        btree_iter_next(
                            iter,
                            Some(collect_iter_item),
                            &mut normal_pairs as *mut IterCollectCtx as *mut c_void
                        ),
                        1
                    );
                    btree_iter_close(iter);

                    let mut uncached_iter = ptr::null_mut();
                    assert_eq!(txn_iter_uncached(&ffi_txn, &mut uncached_iter), 0);
                    assert_eq!(
                        btree_iter_next(
                            uncached_iter,
                            Some(collect_iter_item),
                            &mut uncached_pairs as *mut IterCollectCtx as *mut c_void
                        ),
                        0
                    );
                    assert_eq!(
                        btree_iter_next(
                            uncached_iter,
                            Some(collect_iter_item),
                            &mut uncached_pairs as *mut IterCollectCtx as *mut c_void
                        ),
                        0
                    );
                    assert_eq!(
                        btree_iter_next(
                            uncached_iter,
                            Some(collect_iter_item),
                            &mut uncached_pairs as *mut IterCollectCtx as *mut c_void
                        ),
                        1
                    );
                    btree_iter_close(uncached_iter);
                    Ok(())
                })
                .unwrap()
        };
        assert_eq!(view_result, ());
        assert_eq!(normal_pairs.pairs, uncached_pairs.pairs);
        assert_eq!(
            normal_pairs.pairs,
            vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec())
            ]
        );

        btree_close(db);
    }
}
