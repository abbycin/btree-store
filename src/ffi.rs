#![cfg(feature = "ffi")]

use crate::page_store::PageStore;
use crate::{BTree, BucketMetadata, Error, PageId, ReadOnlyTree, Tree};
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

struct LastError {
    code: c_int,
    msg: Option<CString>,
}

thread_local! {
    static LAST_ERROR: RefCell<LastError> = RefCell::new(LastError { code: 0, msg: None });
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

enum TxnKind {
    Write(*const Tree),
    Read(*const ReadOnlyTree),
}

#[repr(C)]
pub struct Txn {
    kind: TxnKind,
}

struct FfiBucket {
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
        if self.buckets.contains_key(name) {
            return Ok(self.buckets.get_mut(name).unwrap());
        }

        let btree = unsafe { &*self.btree };
        let name_bytes = name.as_bytes();
        let initial_root = match btree.catalog_tree.get(name_bytes) {
            Ok(bytes) => BucketMetadata::from_slice(&bytes).root_page_id,
            Err(Error::NotFound) => 0,
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
            kind: TxnKind::Write(tree.as_ref() as *const Tree),
        });

        self.buckets
            .insert(name.to_string(), FfiBucket { tree, handle });
        Ok(self.buckets.get_mut(name).unwrap())
    }
}

type TxnCallback = Option<unsafe extern "C" fn(*mut Txn, *mut c_void) -> c_int>;
type MultiTxnCallback = Option<unsafe extern "C" fn(*mut MultiTxn, *mut c_void) -> c_int>;

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
    if out.is_null() {
        return invalid_arg("null pointer");
    }
    unsafe {
        *out = ptr::null_mut();
    }
    let path = match cstr_to_str(path) {
        Ok(s) => s,
        Err(code) => return code,
    };

    match BTree::open(path) {
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
            kind: TxnKind::Write(&txn.tree as *const Tree),
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
        cache.insert(name, (new_root, latest_seq));
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
        TxnKind::Write(ptr) => unsafe { (&*ptr).get(key) },
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
        TxnKind::Write(ptr) => match unsafe { (&*ptr).put(key, val) } {
            Ok(_) => 0,
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
        TxnKind::Write(ptr) => match unsafe { (&*ptr).del(key) } {
            Ok(_) => 0,
            Err(e) => set_last_error_from(e),
        },
        TxnKind::Read(_) => set_last_error(ERR_INVALID, "read only transaction"),
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
        if state.code != 0 {
            if let Some(ref cmsg) = state.msg {
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
        }
        state.code
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn btree_last_error_clear() {
    clear_last_error();
}
