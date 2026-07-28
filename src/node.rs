use crate::{DataPid, PageId, StoreResult, TreeReadContext, TreeWriteContext};
use std::alloc::{Layout, alloc, dealloc};
use std::cmp::Ordering;
use std::ptr;

pub const PAGE_SIZE: usize = 4096;
pub const MAX_INLINE_LEN: usize = 256;
pub const MAX_KEY_LEN: usize = 128;
pub const MAX_VAL_LEN: usize = 2 << 30;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ChildPos(usize);

impl ChildPos {
    pub(crate) const fn new(pos: usize) -> Self {
        Self(pos)
    }

    fn get(self) -> usize {
        self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct NonEmptyKey(Vec<u8>);

impl NonEmptyKey {
    pub(crate) fn new(key: Vec<u8>) -> Option<Self> {
        (!key.is_empty()).then_some(Self(key))
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

/// A leaf mutation either fit in place or requires the caller to perform the
/// COW split protocol. Capacity is control flow, not an engine error.
pub(crate) enum LeafWrite {
    Applied,
    SplitRequired,
}

/// A branch edge rewrite either fits or produces the promoted separator and
/// right sibling needed by its parent.
pub(crate) enum BranchRewrite {
    Applied,
    Split { separator: NonEmptyKey, right: Node },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NodeDecodeError {
    Corruption,
}

const OFFSET_NEXT_INDIRECT: usize = PAGE_SIZE - 4;
const IDS_PER_INDIRECT_PAGE: usize = OFFSET_NEXT_INDIRECT / std::mem::size_of::<PageId>();

pub(crate) struct AlignedPage {
    ptr: *mut u8,
    layout: Layout,
}

impl AlignedPage {
    pub(crate) fn new() -> Self {
        let layout = Layout::from_size_align(PAGE_SIZE, 8).unwrap();
        let ptr = unsafe { alloc(layout) };
        if ptr.is_null() {
            std::alloc::handle_alloc_error(layout);
        }
        Self { ptr, layout }
    }

    #[cfg(test)]
    fn from_vec(data: Vec<u8>) -> Self {
        let page = Self::new();
        let len = std::cmp::min(data.len(), PAGE_SIZE);
        unsafe {
            ptr::copy_nonoverlapping(data.as_ptr(), page.ptr, len);
        }
        page
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, PAGE_SIZE) }
    }

    pub(crate) fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, PAGE_SIZE) }
    }

    pub(crate) fn as_ptr(&self) -> *const u8 {
        self.ptr
    }

    pub(crate) fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr
    }
}

unsafe impl Send for AlignedPage {}
unsafe impl Sync for AlignedPage {}

impl Drop for AlignedPage {
    fn drop(&mut self) {
        unsafe {
            dealloc(self.ptr, self.layout);
        }
    }
}

impl Clone for AlignedPage {
    fn clone(&self) -> Self {
        let new_page = Self::new();
        unsafe {
            ptr::copy_nonoverlapping(self.ptr, new_page.ptr, PAGE_SIZE);
        }
        new_page
    }
}

/// Plain node header: 12 bytes. `is_leaf` is 0 for a branch and 1 for a leaf.
#[repr(C, align(4))]
#[derive(Clone, Copy, Debug)]
pub(crate) struct PlainHeader {
    pub is_leaf: u32,
    pub elems: u32,
    pub offset: u32,
}

const PLAIN_HEADER_SIZE: usize = std::mem::size_of::<PlainHeader>();

/// Encoded node header: 16 bytes. The first u32 `kind` doubles as the class
/// discriminant: `ENCODED_BRANCH` (2) / `ENCODED_LEAF` (3). `prefix_len` is
/// the length of the shared prefix stored immediately after the header.
#[repr(C, align(4))]
#[derive(Clone, Copy, Debug)]
pub(crate) struct EncodedHeader {
    pub kind: u32,
    pub elems: u32,
    pub offset: u32,
    pub prefix_len: u32,
}

const ENCODED_HEADER_SIZE: usize = std::mem::size_of::<EncodedHeader>();

/// Encoded node discriminant values: the only first-u32 values whose bit0
/// (0/1) overlaps the plain `is_leaf` domain, so a page's first u32 alone
/// distinguishes the two node classes without a magic constant.
pub(crate) const ENCODED_BRANCH: u32 = 2;
pub(crate) const ENCODED_LEAF: u32 = 3;

const SLOT_SIZE: usize = std::mem::size_of::<Slot>();
const NR_INLINE_PAGE: usize = 5;

#[repr(C, align(4))]
#[derive(Clone, Copy, Debug)]
pub(crate) struct Slot {
    pub pos: u32,
    pub klen: u32,
    pub vlen: u32,
    pub page_id: [PageId; NR_INLINE_PAGE],
}

impl Slot {
    pub(crate) fn is_inline(&self) -> bool {
        self.page_id[0] == 0
    }

    pub(crate) fn data_offset(&self) -> usize {
        self.pos as usize
    }

    pub(crate) fn key_len(&self) -> usize {
        self.klen as usize
    }

    pub(crate) fn value_len(&self) -> usize {
        self.vlen as usize
    }

    pub(crate) fn nr_pages(&self) -> u32 {
        if self.vlen == 0 {
            0
        } else {
            self.vlen.div_ceil(PAGE_SIZE as u32)
        }
    }

    fn update_vlen(&mut self, vlen: u32) {
        self.vlen = vlen;
    }
}

fn decode_pid(raw: PageId) -> DataPid {
    DataPid::new(raw).unwrap_or_else(|| {
        crate::invariant(
            "INVALID_PHYSICAL_PAGE_ID",
            format!("physical page reference has invalid raw value {raw}"),
        )
    })
}

/// Length of the longest byte prefix shared by all keys in `keys`.
pub(crate) fn common_prefix_len(keys: &[&[u8]]) -> usize {
    let Some(first) = keys.first().copied() else {
        return 0;
    };
    let mut n = first.len();
    for k in &keys[1..] {
        n = common_prefix_len_pair(&first[..n], k);
        if n == 0 {
            break;
        }
    }
    n
}

/// Compares a full key against a node's shared `prefix` + stored `tail`
/// without reconstructing the full key. For one shared prefix, ordering is
/// preserved: `prefix + tail1 < prefix + tail2` iff `tail1 < tail2`, so binary
/// search on the stored suffixes stays valid. Ported from mace
/// `data.rs::cmp_raw_with_prefixed_tail`.
#[inline]
pub(crate) fn cmp_raw_with_prefixed_tail(
    lhs: &[u8],
    rhs_prefix_tail: &[u8],
    rhs_base: &[u8],
) -> Ordering {
    let n = lhs.len().min(rhs_prefix_tail.len());
    let o = lhs[..n].cmp(&rhs_prefix_tail[..n]);
    if o != Ordering::Equal {
        return o;
    }
    if lhs.len() < rhs_prefix_tail.len() {
        return Ordering::Less;
    }
    lhs[n..].cmp(rhs_base)
}

// ---------------------------------------------------------------------------
// Overflow / indirect helpers shared by both node classes. They operate on a
// `Slot` and a read/write context, never on the node's key layout.
// ---------------------------------------------------------------------------

pub(crate) fn free_slot_pages_for(
    read: &TreeReadContext,
    slot: &Slot,
    freed: &mut Vec<(PageId, u32)>,
) {
    let nr_pages = slot.nr_pages() as usize;
    if nr_pages == 0 {
        return;
    }

    if nr_pages <= NR_INLINE_PAGE {
        for i in 0..nr_pages {
            freed.push((decode_pid(slot.page_id[i]).get(), 1));
        }
    } else {
        let mut curr_index_page = decode_pid(slot.page_id[0]);
        let mut collected_data_pages = 0;
        while collected_data_pages < nr_pages {
            let data = read.load_page(curr_index_page);
            let to_free = std::cmp::min(nr_pages - collected_data_pages, IDS_PER_INDIRECT_PAGE);

            for i in 0..to_free {
                let start = i * 4;
                let id = decode_pid(u32::from_le_bytes(
                    data[start..start + 4].try_into().unwrap(),
                ));
                freed.push((id.get(), 1));
            }
            collected_data_pages += to_free;

            freed.push((curr_index_page.get(), 1));
            if collected_data_pages < nr_pages {
                let next =
                    u32::from_le_bytes(data[OFFSET_NEXT_INDIRECT..PAGE_SIZE].try_into().unwrap());
                curr_index_page = decode_pid(next);
            }
        }
    }
}

fn write_index_chain(ctx: &mut TreeWriteContext, pages: &[DataPid]) -> StoreResult<DataPid> {
    let nr_index_pages = pages.len().div_ceil(IDS_PER_INDIRECT_PAGE);
    let index_page_ids = ctx.alloc_pages(nr_index_pages as u32)?;
    let mut index_data = vec![0u8; nr_index_pages * PAGE_SIZE];

    for i in 0..nr_index_pages {
        let start_idx = i * IDS_PER_INDIRECT_PAGE;
        let end_idx = std::cmp::min(start_idx + IDS_PER_INDIRECT_PAGE, pages.len());
        let page = &mut index_data[i * PAGE_SIZE..(i + 1) * PAGE_SIZE];

        for (j, &pid) in pages[start_idx..end_idx].iter().enumerate() {
            let off = j * 4;
            page[off..off + 4].copy_from_slice(&pid.get().to_le_bytes());
        }

        if i + 1 < nr_index_pages {
            let next_pid = index_page_ids[i + 1];
            page[OFFSET_NEXT_INDIRECT..PAGE_SIZE].copy_from_slice(&next_pid.get().to_le_bytes());
        }
    }
    ctx.write_pages(&index_page_ids, &index_data)?;

    Ok(index_page_ids[0])
}

fn write_value_pages(
    ctx: &mut TreeWriteContext,
    pages: &[DataPid],
    value: &[u8],
) -> StoreResult<()> {
    ctx.write_pages(pages, value)
}

fn make_overflow_slot(ctx: &mut TreeWriteContext, value: &[u8]) -> StoreResult<Slot> {
    let nr_blocks = value.len().div_ceil(PAGE_SIZE);
    let pages = ctx.alloc_pages(nr_blocks as u32)?;
    let mut slot = Slot {
        pos: 0,
        klen: 0,
        vlen: value.len() as u32,
        page_id: [0; NR_INLINE_PAGE],
    };

    if nr_blocks <= NR_INLINE_PAGE {
        write_value_pages(ctx, &pages, value)?;
        for (i, &pid) in pages.iter().enumerate() {
            slot.page_id[i] = pid.get();
        }
    } else {
        let first_index_pid = write_index_chain(ctx, &pages)?;
        write_value_pages(ctx, &pages, value)?;
        slot.page_id[0] = first_index_pid.get();
    }
    Ok(slot)
}

fn make_leaf_slot(
    ctx: &mut TreeWriteContext,
    current_offset: u32,
    key_len: usize,
    value: &[u8],
) -> StoreResult<(u32, Slot)> {
    let inline = value_inline_in_slot(key_len, value.len());
    let data_len = if inline {
        key_len + value.len()
    } else {
        key_len
    };
    let data_offset = current_offset - data_len as u32;

    let mut slot = make_value_slot(ctx, key_len, value, inline)?;
    slot.pos = data_offset;
    Ok((data_offset, slot))
}

fn value_inline_in_slot(key_len: usize, value_len: usize) -> bool {
    key_len + value_len <= MAX_INLINE_LEN
}

fn make_inline_slot(key_len: usize, value_len: usize) -> Slot {
    Slot {
        pos: 0,
        klen: key_len as u32,
        vlen: value_len as u32,
        page_id: [0; NR_INLINE_PAGE],
    }
}

fn make_value_slot(
    ctx: &mut TreeWriteContext,
    key_len: usize,
    value: &[u8],
    inline: bool,
) -> StoreResult<Slot> {
    if inline {
        return Ok(make_inline_slot(key_len, value.len()));
    }

    let mut slot = make_overflow_slot(ctx, value)?;
    slot.klen = key_len as u32;
    Ok(slot)
}

fn load_value_pages(read: &TreeReadContext, pages: &[DataPid], value_len: usize) -> Vec<u8> {
    read.load_data(pages, value_len)
}

fn collect_page_ids(read: &TreeReadContext, slot: &Slot) -> Vec<DataPid> {
    let nr_pages = slot.nr_pages() as usize;
    if nr_pages <= NR_INLINE_PAGE {
        return slot.page_id[0..nr_pages]
            .iter()
            .map(|raw| decode_pid(*raw))
            .collect();
    }

    let mut pages = Vec::with_capacity(nr_pages);
    let mut curr_index_page = decode_pid(slot.page_id[0]);

    while pages.len() < nr_pages {
        let data = read.load_page(curr_index_page);
        let to_read = std::cmp::min(nr_pages - pages.len(), IDS_PER_INDIRECT_PAGE);
        for i in 0..to_read {
            let start = i * 4;
            let raw = u32::from_le_bytes(data[start..start + 4].try_into().unwrap());
            pages.push(decode_pid(raw));
        }
        if pages.len() < nr_pages {
            let next =
                u32::from_le_bytes(data[OFFSET_NEXT_INDIRECT..PAGE_SIZE].try_into().unwrap());
            curr_index_page = decode_pid(next);
        }
    }
    pages
}

fn load_overflow_value(read: &TreeReadContext, slot: &Slot) -> Vec<u8> {
    debug_assert!(!slot.is_inline());
    let nr_pages = slot.nr_pages() as usize;
    if nr_pages == 0 {
        return Vec::new();
    }
    if nr_pages <= NR_INLINE_PAGE {
        let mut pages = [decode_pid(slot.page_id[0]); NR_INLINE_PAGE];
        for (index, page_id) in pages.iter_mut().enumerate().take(nr_pages).skip(1) {
            *page_id = decode_pid(slot.page_id[index]);
        }
        return load_value_pages(read, &pages[..nr_pages], slot.value_len());
    }
    let pages = collect_page_ids(read, slot);
    load_value_pages(read, &pages, slot.value_len())
}

#[cfg(test)]
fn collect_slot_storage_ids(read: &TreeReadContext, slot: &Slot) -> Vec<DataPid> {
    if slot.is_inline() {
        return Vec::new();
    }
    let nr_pages = slot.nr_pages() as usize;
    if nr_pages <= NR_INLINE_PAGE {
        return slot.page_id[..nr_pages]
            .iter()
            .map(|raw| decode_pid(*raw))
            .collect();
    }

    let mut all = Vec::new();
    let mut data_pages = 0usize;
    let mut current = decode_pid(slot.page_id[0]);
    while data_pages < nr_pages {
        all.push(current);
        let page = read.load_page(current);
        let count = (nr_pages - data_pages).min(IDS_PER_INDIRECT_PAGE);
        for index in 0..count {
            let offset = index * 4;
            let raw = u32::from_le_bytes(page[offset..offset + 4].try_into().unwrap());
            all.push(decode_pid(raw));
        }
        data_pages += count;
        if data_pages < nr_pages {
            let next =
                u32::from_le_bytes(page[OFFSET_NEXT_INDIRECT..PAGE_SIZE].try_into().unwrap());
            current = decode_pid(next);
        }
    }
    all
}

// ---------------------------------------------------------------------------
// PlainNode: the original 12-byte-header layout. Each slot stores the full key.
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub(crate) struct PlainNode {
    page: AlignedPage,
}

impl PlainNode {
    fn decode_pid(raw: PageId) -> DataPid {
        decode_pid(raw)
    }

    pub(crate) fn from_aligned_page(
        page: AlignedPage,
    ) -> std::result::Result<Self, NodeDecodeError> {
        let this = Self { page };
        this.validate()?;
        Ok(this)
    }

    #[cfg(test)]
    pub(crate) fn from_raw(data: Vec<u8>) -> std::result::Result<Self, NodeDecodeError> {
        if data.len() != PAGE_SIZE {
            return Err(NodeDecodeError::Corruption);
        }
        Self::from_aligned_page(AlignedPage::from_vec(data))
    }

    pub(crate) fn into_aligned_page(self) -> AlignedPage {
        self.page
    }

    pub(crate) fn finalize(&self) -> &[u8] {
        self.page.as_slice()
    }

    fn header(&self) -> &PlainHeader {
        unsafe { &*self.page.as_ptr().cast::<PlainHeader>() }
    }

    fn header_mut(&mut self) -> &mut PlainHeader {
        unsafe { &mut *self.page.as_mut_ptr().cast::<PlainHeader>() }
    }

    pub(crate) fn is_leaf(&self) -> bool {
        self.header().is_leaf == 1
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.header().elems == 0
    }

    pub(crate) fn num_children(&self) -> usize {
        self.header().elems as usize
    }

    fn validate(&self) -> std::result::Result<(), NodeDecodeError> {
        let header = self.header();
        let max_elems = (PAGE_SIZE - PLAIN_HEADER_SIZE) / SLOT_SIZE;
        if header.is_leaf > 1 || header.elems > max_elems as u32 {
            return Err(NodeDecodeError::Corruption);
        }

        let min_offset = PLAIN_HEADER_SIZE + header.elems as usize * SLOT_SIZE;
        if header.offset < min_offset as u32 || header.offset > PAGE_SIZE as u32 {
            return Err(NodeDecodeError::Corruption);
        }

        Ok(())
    }

    fn new(is_leaf: bool) -> Self {
        let mut this = PlainNode {
            page: AlignedPage::new(),
        };
        let h = this.header_mut();
        h.offset = PAGE_SIZE as u32;
        h.is_leaf = if is_leaf { 1 } else { 0 };
        h.elems = 0;
        this
    }

    pub(crate) fn new_leaf() -> Self {
        Self::new(true)
    }

    pub(crate) fn new_branch() -> Self {
        Self::new(false)
    }

    pub(crate) fn new_branch_root(
        left_page_id: DataPid,
        separator: NonEmptyKey,
        right_page_id: DataPid,
    ) -> Self {
        Self::branch_from_entries(&[(Vec::new(), left_page_id), (separator.0, right_page_id)])
    }

    #[cfg(test)]
    pub(crate) fn new_branch_single(child: DataPid) -> Self {
        Self::branch_from_entries(&[(Vec::new(), child)])
    }

    fn available_space(&self) -> u32 {
        let hdr = self.header();
        let used = PLAIN_HEADER_SIZE as u32 + hdr.elems * SLOT_SIZE as u32;
        hdr.offset.saturating_sub(used)
    }

    fn branch_entries_fit(entries: &[(Vec<u8>, DataPid)]) -> bool {
        let key_bytes: usize = entries.iter().map(|(key, _)| key.len()).sum();
        PLAIN_HEADER_SIZE + entries.len() * SLOT_SIZE + key_bytes <= PAGE_SIZE
    }

    pub(crate) fn slot_at(&self, pos: usize) -> &Slot {
        let slot_off = PLAIN_HEADER_SIZE + pos * SLOT_SIZE;
        unsafe { &*self.page.as_ptr().add(slot_off).cast::<Slot>() }
    }

    fn slot_at_mut(&mut self, pos: usize) -> &mut Slot {
        let slot_off = PLAIN_HEADER_SIZE + pos * SLOT_SIZE;
        unsafe { &mut *self.page.as_mut_ptr().add(slot_off).cast::<Slot>() }
    }

    pub(crate) fn key_at(&self, pos: usize) -> &[u8] {
        let slot = self.slot_at(pos);
        let off = slot.data_offset();
        let len = slot.key_len();
        &self.page.as_slice()[off..off + len]
    }

    pub(crate) fn value_at(&self, pos: usize) -> &[u8] {
        let slot = self.slot_at(pos);
        assert!(slot.is_inline());
        let off = slot.data_offset() + slot.key_len();
        let len = slot.value_len();
        &self.page.as_slice()[off..off + len]
    }

    fn key_at_mut(&mut self, pos: usize) -> &mut [u8] {
        let slot = *self.slot_at(pos);
        let off = slot.data_offset();
        let len = slot.key_len();
        &mut self.page.as_mut_slice()[off..off + len]
    }

    fn value_at_mut(&mut self, pos: usize) -> &mut [u8] {
        let slot = *self.slot_at(pos);
        assert!(slot.is_inline());
        let off = slot.data_offset() + slot.key_len();
        let len = slot.value_len();
        &mut self.page.as_mut_slice()[off..off + len]
    }

    pub(crate) fn full_key(&self, pos: usize, buf: &mut Vec<u8>) {
        buf.clear();
        buf.extend_from_slice(self.key_at(pos));
    }

    pub(crate) fn branch_entries(&self) -> Vec<(Vec<u8>, DataPid)> {
        debug_assert!(!self.is_leaf());
        (0..self.num_children())
            .map(|pos| (self.key_at(pos).to_vec(), self.child_at(pos)))
            .collect()
    }

    fn branch_from_entries(entries: &[(Vec<u8>, DataPid)]) -> Self {
        if !Self::branch_entries_fit(entries) {
            crate::invariant(
                "BRANCH_REBUILD_OVERFLOW",
                "branch entries do not fit a page",
            );
        }

        let mut node = Self::new_branch();
        for (pos, (key, page_id)) in entries.iter().enumerate() {
            node.insert_branch_slot_at_raw(pos, key, *page_id);
        }
        node
    }

    fn valid_branch_split_pivot(entries: &[(Vec<u8>, DataPid)], pivot: usize) -> bool {
        pivot > 0
            && pivot < entries.len()
            && !entries[pivot].0.is_empty()
            && Self::branch_entries_fit(&entries[..pivot])
            && Self::branch_entries_fit(&entries[pivot..])
    }

    fn branch_split_pivot(entries: &[(Vec<u8>, DataPid)]) -> Option<usize> {
        let preferred = entries.len() / 2;
        for delta in 0..entries.len() {
            if let Some(pivot) = preferred.checked_sub(delta)
                && Self::valid_branch_split_pivot(entries, pivot)
            {
                return Some(pivot);
            }

            let pivot = preferred + delta;
            if delta != 0 && pivot < entries.len() && Self::valid_branch_split_pivot(entries, pivot)
            {
                return Some(pivot);
            }
        }
        None
    }

    fn emplace_at(&mut self, pos: usize, slot: &Slot, data: &[u8]) {
        let data_off = {
            let hdr = self.header_mut();
            hdr.offset -= data.len() as u32;
            hdr.offset
        };

        let dst_slot = self.slot_at_mut(pos);
        *dst_slot = *slot;
        dst_slot.pos = data_off;

        self.page.as_mut_slice()[data_off as usize..data_off as usize + data.len()]
            .copy_from_slice(data);
        self.header_mut().elems += 1;
    }

    fn leaf_fits_with_updated_value(
        &self,
        pos: usize,
        replacement_slot: Slot,
        replacement_inline_value_len: usize,
    ) -> bool {
        let elems = self.header().elems as usize;
        let payload_bytes: usize = (0..elems)
            .map(|idx| {
                let slot = if idx == pos {
                    replacement_slot
                } else {
                    *self.slot_at(idx)
                };
                slot.key_len()
                    + if idx == pos {
                        usize::from(slot.is_inline()) * replacement_inline_value_len
                    } else if slot.is_inline() {
                        slot.value_len()
                    } else {
                        0
                    }
            })
            .sum();
        PLAIN_HEADER_SIZE + elems * SLOT_SIZE + payload_bytes <= PAGE_SIZE
    }

    fn rebuild_leaf_with_updated_value(
        &mut self,
        pos: usize,
        replacement_slot: Slot,
        replacement_inline_value: &[u8],
    ) {
        let elems = self.header().elems as usize;
        let mut new_page = AlignedPage::new();
        let src_hdr = *self.header();
        let mut offset = PAGE_SIZE as u32;

        new_page.as_mut_slice()[..PLAIN_HEADER_SIZE]
            .copy_from_slice(&self.page.as_slice()[..PLAIN_HEADER_SIZE]);

        for idx in 0..elems {
            let slot = if idx == pos {
                replacement_slot
            } else {
                *self.slot_at(idx)
            };
            let key = self.key_at(idx);
            let inline_value = if slot.is_inline() {
                if idx == pos {
                    replacement_inline_value
                } else {
                    self.value_at(idx)
                }
            } else {
                &[]
            };
            let data_len = key.len() + inline_value.len();
            offset -= data_len as u32;

            let dst_slot_off = PLAIN_HEADER_SIZE + idx * SLOT_SIZE;
            let dst_slot = unsafe { &mut *new_page.as_mut_ptr().add(dst_slot_off).cast::<Slot>() };
            *dst_slot = slot;
            dst_slot.pos = offset;

            new_page.as_mut_slice()[offset as usize..offset as usize + key.len()]
                .copy_from_slice(key);
            if !inline_value.is_empty() {
                new_page.as_mut_slice()[offset as usize + key.len()..offset as usize + data_len]
                    .copy_from_slice(inline_value);
            }
        }

        let dst_hdr = unsafe { &mut *new_page.as_mut_ptr().cast::<PlainHeader>() };
        *dst_hdr = src_hdr;
        dst_hdr.offset = offset;
        self.page = new_page;
    }

    pub(crate) fn update_leaf_at(
        &mut self,
        ctx: &mut TreeWriteContext,
        pos: usize,
        value: &[u8],
    ) -> StoreResult<()> {
        if !self.is_leaf() {
            crate::invariant("UPDATE_NON_LEAF", "attempted leaf update on branch node");
        }

        let old_slot = *self.slot_at(pos);
        if old_slot.is_inline() && value.len() <= old_slot.value_len() {
            self.value_at_mut(pos)[..value.len()].copy_from_slice(value);
            self.slot_at_mut(pos).update_vlen(value.len() as u32);
            return Ok(());
        }

        let key_len = old_slot.key_len();
        let inline = value_inline_in_slot(key_len, value.len());
        if inline {
            let replacement = make_inline_slot(key_len, value.len());
            if self.leaf_fits_with_updated_value(pos, replacement, value.len()) {
                self.rebuild_leaf_with_updated_value(pos, replacement, value);
                if !old_slot.is_inline() {
                    ctx.free_slot(&old_slot);
                }
                return Ok(());
            }
        }

        let replacement = make_value_slot(ctx, key_len, value, false)?;
        self.rebuild_leaf_with_updated_value(pos, replacement, &[]);
        if !old_slot.is_inline() {
            ctx.free_slot(&old_slot);
        }
        Ok(())
    }

    fn expand_slot(&mut self, pos: usize) -> &mut Slot {
        let elems = self.header().elems;
        let slot_off = PLAIN_HEADER_SIZE + pos * SLOT_SIZE;
        let last_slot_off = PLAIN_HEADER_SIZE + elems as usize * SLOT_SIZE;

        if pos < elems as usize {
            self.page
                .as_mut_slice()
                .copy_within(slot_off..last_slot_off, slot_off + SLOT_SIZE);
        }

        self.header_mut().elems += 1;
        self.slot_at_mut(pos)
    }

    pub(crate) fn shrink_slot(&mut self, pos: usize) -> Slot {
        let elems = self.header().elems;
        let slot = *self.slot_at(pos);
        let slot_off = PLAIN_HEADER_SIZE + pos * SLOT_SIZE;
        let next_slot_off = slot_off + SLOT_SIZE;
        let last_slot_off = PLAIN_HEADER_SIZE + elems as usize * SLOT_SIZE;

        if pos + 1 < elems as usize {
            self.page
                .as_mut_slice()
                .copy_within(next_slot_off..last_slot_off, slot_off);
        }

        self.header_mut().elems -= 1;
        slot
    }

    fn insert_branch_slot_at_raw(&mut self, pos: usize, key: &[u8], page_id: DataPid) {
        debug_assert!(!self.is_leaf());

        if key.len() > MAX_KEY_LEN {
            crate::invariant(
                "BRANCH_KEY_TOO_LARGE",
                "branch separator exceeds MAX_KEY_LEN",
            );
        }

        let total_required = key.len() as u32 + SLOT_SIZE as u32;
        if self.available_space() < total_required {
            self.compact();
            if self.available_space() < total_required {
                crate::invariant(
                    "BRANCH_SLOT_OVERFLOW",
                    "pre-sized branch rewrite overflowed",
                );
            }
        }

        let mut cur_off = self.header().offset;
        cur_off -= key.len() as u32;
        self.header_mut().offset = cur_off;

        let slot = self.expand_slot(pos);
        slot.klen = key.len() as u32;
        slot.vlen = 0;
        slot.page_id = [0; NR_INLINE_PAGE];
        slot.page_id[0] = page_id.get();
        slot.pos = cur_off;

        self.key_at_mut(pos).copy_from_slice(key);
    }

    fn insert_leaf_at(
        &mut self,
        ctx: &mut TreeWriteContext,
        pos: usize,
        key: &[u8],
        value: &[u8],
    ) -> StoreResult<()> {
        if !self.is_leaf() {
            crate::invariant("INSERT_NON_LEAF", "attempted leaf insert on branch node");
        }

        let (cur_off, slot_copy) = make_leaf_slot(ctx, self.header().offset, key.len(), value)?;

        self.header_mut().offset = cur_off;
        let slot = self.expand_slot(pos);
        *slot = slot_copy;

        if slot_copy.is_inline() {
            self.value_at_mut(pos).copy_from_slice(value);
        }
        self.key_at_mut(pos).copy_from_slice(key);
        Ok(())
    }

    fn compact(&mut self) {
        let mut new_page = AlignedPage::new();
        let src_hdr = *self.header();
        let elems = src_hdr.elems as usize;
        let mut offset = PAGE_SIZE as u32;

        new_page.as_mut_slice()[..PLAIN_HEADER_SIZE]
            .copy_from_slice(&self.page.as_slice()[..PLAIN_HEADER_SIZE]);

        for i in 0..elems {
            let src_slot = *self.slot_at(i);
            let k = self.key_at(i);
            let mut kv_len = k.len() as u32;

            let v = if src_hdr.is_leaf == 1 && src_slot.is_inline() {
                let val = self.value_at(i);
                kv_len += val.len() as u32;
                Some(val)
            } else {
                None
            };

            offset -= kv_len;
            let dst_slot_off = PLAIN_HEADER_SIZE + i * SLOT_SIZE;
            let dst_slot = unsafe { &mut *new_page.as_mut_ptr().add(dst_slot_off).cast::<Slot>() };
            *dst_slot = src_slot;
            dst_slot.pos = offset;

            new_page.as_mut_slice()[offset as usize..offset as usize + k.len()].copy_from_slice(k);
            if let Some(val) = v {
                new_page.as_mut_slice()
                    [offset as usize + k.len()..offset as usize + k.len() + val.len()]
                    .copy_from_slice(val);
            }
        }

        let dst_hdr = unsafe { &mut *new_page.as_mut_ptr().cast::<PlainHeader>() };
        dst_hdr.offset = offset;

        self.page = new_page;
    }

    pub(crate) fn put_leaf(
        &mut self,
        ctx: &mut TreeWriteContext,
        key: &[u8],
        value: &[u8],
    ) -> StoreResult<LeafWrite> {
        if !self.is_leaf() {
            crate::invariant("PUT_NON_LEAF", "attempted leaf put on branch node");
        }

        if key.len() > MAX_KEY_LEN {
            crate::invariant("LEAF_KEY_TOO_LARGE", "leaf key exceeds MAX_KEY_LEN");
        }

        match self.search(key) {
            Ok(pos) => {
                self.update_leaf_at(ctx, pos, value)?;
            }
            Err(pos) => {
                let data_len = if value_inline_in_slot(key.len(), value.len()) {
                    key.len() + value.len()
                } else {
                    key.len()
                };
                let total_required = data_len as u32 + SLOT_SIZE as u32;

                if self.available_space() < total_required {
                    self.compact();
                    if self.available_space() < total_required {
                        return Ok(LeafWrite::SplitRequired);
                    }
                }
                self.insert_leaf_at(ctx, pos, key, value)?;
            }
        }
        Ok(LeafWrite::Applied)
    }

    pub(crate) fn get(&self, read: &TreeReadContext, key: &[u8]) -> Option<Vec<u8>> {
        match self.search(key) {
            Ok(pos) => {
                let slot = self.slot_at(pos);
                if slot.is_inline() {
                    Some(self.value_at(pos).to_vec())
                } else {
                    Some(load_overflow_value(read, slot))
                }
            }
            Err(_) => None,
        }
    }

    pub(crate) fn search(&self, key: &[u8]) -> std::result::Result<usize, usize> {
        let mut lo = 0;
        let mut hi = self.header().elems as usize;
        let elems = hi;

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let k = self.key_at(mid);
            match k.cmp(key) {
                Ordering::Less => lo = mid + 1,
                _ => hi = mid,
            }
        }

        if lo < elems && self.key_at(lo) == key {
            Ok(lo)
        } else {
            Err(lo)
        }
    }

    pub(crate) fn child_pos_for_key(&self, key: &[u8]) -> usize {
        debug_assert!(!self.is_leaf());

        let mut lo = 0usize;
        let mut hi = self.header().elems as usize;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match self.key_at(mid).cmp(key) {
                Ordering::Greater => hi = mid,
                _ => lo = mid + 1,
            }
        }
        lo.saturating_sub(1)
    }

    pub(crate) fn delete_leaf_key(&mut self, ctx: &mut TreeWriteContext, key: &[u8]) {
        if !self.is_leaf() {
            crate::invariant("DELETE_NON_LEAF", "attempted leaf delete on branch node");
        }

        match self.search(key) {
            Ok(pos) => {
                let slot = self.shrink_slot(pos);
                if !slot.is_inline() {
                    ctx.free_slot(&slot);
                }
            }
            Err(_) => crate::invariant(
                "DELETE_MISSING_KEY",
                "delete reached a leaf without its key",
            ),
        }
    }

    pub(crate) fn split_leaf(&mut self) -> (NonEmptyKey, Node) {
        if !self.is_leaf() {
            crate::invariant("SPLIT_NON_LEAF", "attempted leaf split on branch node");
        }

        let mid = (self.header().elems / 2) as usize;
        let sep = NonEmptyKey::new(self.key_at(mid).to_vec()).unwrap_or_else(|| {
            crate::invariant(
                "EMPTY_LEAF_SPLIT_SEPARATOR",
                "split leaf contains an empty key",
            )
        });
        let mut node = PlainNode::new_leaf();
        let elems = self.header().elems as usize;
        for pos in mid..elems {
            let slot = *self.slot_at(pos);
            let data = self.data_at(pos);
            node.emplace_at(pos - mid, &slot, data);
        }

        self.header_mut().elems = mid as u32;
        self.compact();

        (sep, Node::Plain(node))
    }

    pub(crate) fn data_at(&self, pos: usize) -> &[u8] {
        let slot = self.slot_at(pos);
        let len = if self.is_leaf() && slot.is_inline() {
            slot.key_len() + slot.value_len()
        } else {
            slot.key_len()
        };
        let off = slot.data_offset();
        &self.page.as_slice()[off..off + len]
    }

    pub(crate) fn child_at(&self, pos: usize) -> DataPid {
        debug_assert!(!self.is_leaf());
        Self::decode_pid(self.slot_at(pos).page_id[0])
    }

    pub(crate) fn update_child_page(&mut self, pos: ChildPos, page_id: DataPid) {
        debug_assert!(!self.is_leaf());
        let slot = self.slot_at_mut(pos.get());
        slot.page_id[0] = page_id.get();
        if !self.is_empty() {
            self.canonicalize_branch_slot_zero();
        }
    }

    pub(crate) fn canonicalize_branch_slot_zero(&mut self) {
        debug_assert!(!self.is_leaf());

        if self.is_empty() {
            return;
        }

        let child = self.slot_at(0).page_id[0];
        let slot = self.slot_at_mut(0);
        *slot = make_inline_slot(0, 0);
        slot.page_id[0] = child;
    }

    pub(crate) fn remove_branch_child(&mut self, pos: ChildPos) -> Slot {
        debug_assert!(!self.is_leaf());

        let slot = self.shrink_slot(pos.get());
        if !self.is_empty() {
            self.canonicalize_branch_slot_zero();
        }
        slot
    }

    pub(crate) fn apply_branch_split_rewrite(
        &mut self,
        pos: ChildPos,
        expected_old: DataPid,
        left_page_id: DataPid,
        separator: NonEmptyKey,
        right_page_id: DataPid,
    ) -> BranchRewrite {
        debug_assert!(!self.is_leaf());
        let pos = pos.get();

        if pos >= self.num_children() {
            crate::invariant(
                "BRANCH_REWRITE_POSITION",
                "parent rewrite position is out of bounds",
            );
        }

        if self.child_at(pos) != expected_old {
            crate::invariant(
                "BRANCH_REWRITE_EXPECTED_CHILD",
                "parent child no longer matches rewrite route",
            );
        }

        let mut entries = self.branch_entries();
        entries[pos].1 = left_page_id;
        entries.insert(pos + 1, (separator.0, right_page_id));
        entries[0].0.clear();

        if Self::branch_entries_fit(&entries) {
            *self = Self::branch_from_entries(&entries);
            return BranchRewrite::Applied;
        }

        let pivot = Self::branch_split_pivot(&entries).unwrap_or_else(|| {
            crate::invariant("BRANCH_SPLIT_PIVOT", "no valid branch split pivot")
        });
        let promoted_separator = NonEmptyKey::new(entries[pivot].0.clone()).unwrap_or_else(|| {
            crate::invariant(
                "EMPTY_BRANCH_PROMOTION_SEPARATOR",
                "branch split pivot must carry a non-empty separator",
            )
        });

        let mut left_entries = entries[..pivot].to_vec();
        let mut right_entries = entries[pivot..].to_vec();
        left_entries[0].0.clear();
        right_entries[0].0.clear();

        let right = Self::branch_from_entries(&right_entries);
        *self = Self::branch_from_entries(&left_entries);
        BranchRewrite::Split {
            separator: promoted_separator,
            right: Node::Plain(right),
        }
    }
}

// ---------------------------------------------------------------------------
// EncodedNode: 16-byte header, shared `prefix` bytes after the header, each
// slot stores the key tail (`key[prefix..]`). Ordering is preserved so binary
// search on the tails stays valid (see `cmp_raw_with_prefixed_tail`).
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
struct EncodedLeafEntry {
    slot: Slot,
    key_start: usize,
    key_len: usize,
    value_start: usize,
    value_len: usize,
}

struct EncodedLeafEntries {
    data: Vec<u8>,
    entries: Vec<EncodedLeafEntry>,
}

impl EncodedLeafEntries {
    fn with_capacity(entries: usize, data: usize) -> Self {
        Self {
            data: Vec::with_capacity(data),
            entries: Vec::with_capacity(entries),
        }
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn key(&self, pos: usize) -> &[u8] {
        let entry = &self.entries[pos];
        &self.data[entry.key_start..entry.key_start + entry.key_len]
    }

    fn append_entry_data(&mut self, key: &[u8], slot: Slot, value: &[u8]) -> EncodedLeafEntry {
        debug_assert!(!slot.is_inline() || slot.value_len() == value.len());
        debug_assert!(slot.is_inline() || value.is_empty());

        let key_start = self.data.len();
        self.data.extend_from_slice(key);
        let value_start = self.data.len();
        if slot.is_inline() {
            self.data.extend_from_slice(value);
        }

        EncodedLeafEntry {
            slot,
            key_start,
            key_len: key.len(),
            value_start,
            value_len: value.len(),
        }
    }

    #[cfg(test)]
    fn push(&mut self, key: &[u8], slot: Slot, value: &[u8]) {
        let entry = self.append_entry_data(key, slot, value);
        self.entries.push(entry);
    }

    fn insert(&mut self, pos: usize, key: &[u8], slot: Slot, value: &[u8]) {
        let entry = self.append_entry_data(key, slot, value);
        self.entries.insert(pos, entry);
    }

    fn common_prefix_len(&self, start: usize, end: usize, extra: Option<&[u8]>) -> usize {
        let Some(first) = self.entries.get(start) else {
            return extra.map_or(0, |key| key.len());
        };

        let mut prefix_len = self.key_at_entry(first).len();
        for entry in &self.entries[start + 1..end] {
            prefix_len = common_prefix_len_pair(
                &self.key_at_entry(first)[..prefix_len],
                self.key_at_entry(entry),
            );
            if prefix_len == 0 {
                return 0;
            }
        }
        if let Some(key) = extra {
            prefix_len = common_prefix_len_pair(&self.key_at_entry(first)[..prefix_len], key);
        }
        prefix_len
    }

    fn key_at_entry(&self, entry: &EncodedLeafEntry) -> &[u8] {
        &self.data[entry.key_start..entry.key_start + entry.key_len]
    }

    fn value_at_entry(&self, entry: &EncodedLeafEntry) -> &[u8] {
        &self.data[entry.value_start..entry.value_start + entry.value_len]
    }
}

fn common_prefix_len_pair(lhs: &[u8], rhs: &[u8]) -> usize {
    let n = lhs.len().min(rhs.len());
    lhs[..n]
        .iter()
        .zip(&rhs[..n])
        .position(|(lhs, rhs)| lhs != rhs)
        .unwrap_or(n)
}

/// Encoded leaf built from full keys and values. The shared prefix is the
/// common prefix of all keys; inline values follow each tail back-to-front.
#[cfg(test)]
fn encoded_leaf_entries_from_full_entries(entries: &[(Vec<u8>, Vec<u8>)]) -> EncodedLeafEntries {
    assert!(!entries.is_empty());
    let data_capacity = entries
        .iter()
        .map(|(key, value)| key.len() + value.len())
        .sum();
    let mut collected = EncodedLeafEntries::with_capacity(entries.len(), data_capacity);
    for (key, value) in entries {
        collected.push(
            key,
            Slot {
                pos: 0,
                klen: 0,
                vlen: value.len() as u32,
                page_id: [0; NR_INLINE_PAGE],
            },
            value,
        );
    }
    collected
}

#[cfg(test)]
fn encoded_leaf_from_full_entries(entries: &[(Vec<u8>, Vec<u8>)]) -> EncodedNode {
    let entries = encoded_leaf_entries_from_full_entries(entries);
    let mut node = EncodedNode::new_encoded_leaf();
    node.rebuild_leaf_from_entries(&entries, 0..entries.len());
    node
}

#[derive(Clone, Copy)]
struct EncodedPageEntry<'a> {
    slot: Slot,
    tail: &'a [u8],
    value: &'a [u8],
}

fn write_encoded_page<'a, I>(
    page: &mut AlignedPage,
    kind: u32,
    prefix: &[u8],
    elems: usize,
    entries: I,
) where
    I: IntoIterator<Item = EncodedPageEntry<'a>>,
{
    unsafe {
        let ptr = page.as_mut_ptr();
        let h = &mut *ptr.cast::<EncodedHeader>();
        h.kind = kind;
        h.elems = elems as u32;
        h.offset = PAGE_SIZE as u32;
        h.prefix_len = prefix.len() as u32;
        if !prefix.is_empty() {
            std::ptr::copy_nonoverlapping(
                prefix.as_ptr(),
                ptr.add(ENCODED_HEADER_SIZE),
                prefix.len(),
            );
        }

        let slot_base = (ENCODED_HEADER_SIZE + prefix.len() + 3) & !3;
        let mut offset = PAGE_SIZE;
        for (i, entry) in entries.into_iter().enumerate() {
            offset -= entry.tail.len() + entry.value.len();
            let slot = &mut *ptr.add(slot_base + i * SLOT_SIZE).cast::<Slot>();
            *slot = entry.slot;
            slot.pos = offset as u32;
            slot.klen = entry.tail.len() as u32;
            if !entry.tail.is_empty() {
                std::ptr::copy_nonoverlapping(
                    entry.tail.as_ptr(),
                    ptr.add(offset),
                    entry.tail.len(),
                );
            }
            if !entry.value.is_empty() {
                std::ptr::copy_nonoverlapping(
                    entry.value.as_ptr(),
                    ptr.add(offset + entry.tail.len()),
                    entry.value.len(),
                );
            }
        }
        let h = &mut *ptr.cast::<EncodedHeader>();
        h.offset = offset as u32;
    }
}

/// Encoded branch built from full separator keys. Slot 0 is the empty
/// sentinel; real separators are encoded as `prefix + tail`.
fn encoded_branch_from_full_entries(entries: &[(Vec<u8>, DataPid)]) -> EncodedNode {
    debug_assert!(entries.len() >= 2);
    let real_keys: Vec<&[u8]> = entries[1..].iter().map(|(k, _)| k.as_slice()).collect();
    let prefix_len = common_prefix_len(&real_keys);
    let prefix = &entries[1].0[..prefix_len];
    let mut node = EncodedNode::new_encoded_branch();
    write_encoded_page(
        &mut node.page,
        ENCODED_BRANCH,
        prefix,
        entries.len(),
        entries.iter().enumerate().map(|(i, (key, pid))| {
            let tail = if i == 0 { &[] } else { &key[prefix_len..] };
            EncodedPageEntry {
                slot: Slot {
                    pos: 0,
                    klen: 0,
                    vlen: 0,
                    page_id: [pid.get(), 0, 0, 0, 0],
                },
                tail,
                value: &[],
            }
        }),
    );
    node
}

/// Whether full branch entries fit an encoded page after computing the shared
/// prefix (prefix bytes + aligned slot array + tails).
fn encoded_branch_entries_fit(entries: &[(Vec<u8>, DataPid)]) -> bool {
    if entries.is_empty() {
        return false;
    }
    let real_keys: Vec<&[u8]> = entries[1..].iter().map(|(k, _)| k.as_slice()).collect();
    let prefix_len = if real_keys.is_empty() {
        0
    } else {
        common_prefix_len(&real_keys)
    };
    let slot_base = (ENCODED_HEADER_SIZE + prefix_len + 3) & !3;
    let tail_bytes: usize = entries
        .iter()
        .enumerate()
        .map(|(i, (k, _))| if i == 0 { 0 } else { k.len() - prefix_len })
        .sum();
    slot_base + entries.len() * SLOT_SIZE + tail_bytes <= PAGE_SIZE
}

#[derive(Clone)]
pub(crate) struct EncodedNode {
    page: AlignedPage,
}

impl EncodedNode {
    pub(crate) fn from_aligned_page(
        page: AlignedPage,
    ) -> std::result::Result<Self, NodeDecodeError> {
        let this = Self { page };
        this.validate()?;
        Ok(this)
    }

    #[cfg(test)]
    pub(crate) fn from_raw(data: Vec<u8>) -> std::result::Result<Self, NodeDecodeError> {
        if data.len() != PAGE_SIZE {
            return Err(NodeDecodeError::Corruption);
        }
        Self::from_aligned_page(AlignedPage::from_vec(data))
    }

    pub(crate) fn into_aligned_page(self) -> AlignedPage {
        self.page
    }

    pub(crate) fn finalize(&self) -> &[u8] {
        self.page.as_slice()
    }

    fn header(&self) -> &EncodedHeader {
        unsafe { &*self.page.as_ptr().cast::<EncodedHeader>() }
    }

    fn header_mut(&mut self) -> &mut EncodedHeader {
        unsafe { &mut *self.page.as_mut_ptr().cast::<EncodedHeader>() }
    }

    pub(crate) fn is_leaf(&self) -> bool {
        self.header().kind == ENCODED_LEAF
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.header().elems == 0
    }

    pub(crate) fn num_children(&self) -> usize {
        self.header().elems as usize
    }

    fn validate(&self) -> std::result::Result<(), NodeDecodeError> {
        let header = self.header();
        if header.kind != ENCODED_BRANCH && header.kind != ENCODED_LEAF {
            return Err(NodeDecodeError::Corruption);
        }
        let prefix_len = header.prefix_len as usize;
        if ENCODED_HEADER_SIZE + prefix_len > PAGE_SIZE {
            return Err(NodeDecodeError::Corruption);
        }
        let slot_base = (ENCODED_HEADER_SIZE + prefix_len + 3) & !3;
        if slot_base > PAGE_SIZE {
            return Err(NodeDecodeError::Corruption);
        }
        let max_elems = (PAGE_SIZE - slot_base) / SLOT_SIZE;
        if header.elems > max_elems as u32 {
            return Err(NodeDecodeError::Corruption);
        }
        let min_offset = slot_base + header.elems as usize * SLOT_SIZE;
        if header.offset < min_offset as u32 || header.offset > PAGE_SIZE as u32 {
            return Err(NodeDecodeError::Corruption);
        }
        Ok(())
    }

    fn new_encoded(is_leaf: bool) -> Self {
        let mut this = EncodedNode {
            page: AlignedPage::new(),
        };
        let h = this.header_mut();
        h.kind = if is_leaf {
            ENCODED_LEAF
        } else {
            ENCODED_BRANCH
        };
        h.elems = 0;
        h.offset = PAGE_SIZE as u32;
        h.prefix_len = 0;
        this
    }

    pub(crate) fn new_encoded_leaf() -> Self {
        Self::new_encoded(true)
    }

    pub(crate) fn new_encoded_branch() -> Self {
        Self::new_encoded(false)
    }

    pub(crate) fn new_encoded_branch_root(
        left_page_id: DataPid,
        separator: NonEmptyKey,
        right_page_id: DataPid,
    ) -> Self {
        encoded_branch_from_full_entries(&[
            (Vec::new(), left_page_id),
            (separator.0, right_page_id),
        ])
    }

    fn prefix(&self) -> &[u8] {
        let len = self.header().prefix_len as usize;
        &self.page.as_slice()[ENCODED_HEADER_SIZE..ENCODED_HEADER_SIZE + len]
    }

    /// Sets the shared prefix of an empty encoded leaf to `key` so the first
    /// slot stores an empty tail. Without this the initial empty prefix matches
    /// every key and the shared prefix is never computed until a split.
    fn establish_prefix_for_empty(&mut self, key: &[u8]) {
        debug_assert!(self.is_empty());
        let prefix_len = key.len();
        unsafe {
            let ptr = self.page.as_mut_ptr();
            let h = &mut *ptr.cast::<EncodedHeader>();
            h.prefix_len = prefix_len as u32;
            h.offset = PAGE_SIZE as u32;
            if prefix_len > 0 {
                std::ptr::copy_nonoverlapping(
                    key.as_ptr(),
                    ptr.add(ENCODED_HEADER_SIZE),
                    prefix_len,
                );
            }
        }
    }

    fn slot_base(&self) -> usize {
        let raw = ENCODED_HEADER_SIZE + self.header().prefix_len as usize;
        (raw + 3) & !3
    }

    fn slot_at(&self, pos: usize) -> &Slot {
        let base = self.slot_base();
        unsafe {
            &*self
                .page
                .as_ptr()
                .add(base)
                .add(pos * SLOT_SIZE)
                .cast::<Slot>()
        }
    }

    fn slot_at_mut(&mut self, pos: usize) -> &mut Slot {
        let base = self.slot_base();
        unsafe {
            &mut *self
                .page
                .as_mut_ptr()
                .add(base)
                .add(pos * SLOT_SIZE)
                .cast::<Slot>()
        }
    }

    /// Stored tail bytes (`key[prefix..]`) for the slot.
    fn tail_at(&self, pos: usize) -> &[u8] {
        let slot = self.slot_at(pos);
        let off = slot.pos as usize;
        let len = slot.klen as usize;
        &self.page.as_slice()[off..off + len]
    }

    /// Inline value bytes: the payload follows the stored tail.
    fn value_at(&self, pos: usize) -> &[u8] {
        let slot = self.slot_at(pos);
        assert!(slot.is_inline());
        let off = slot.pos as usize + slot.klen as usize;
        &self.page.as_slice()[off..off + slot.vlen as usize]
    }

    fn tail_at_mut(&mut self, pos: usize) -> &mut [u8] {
        let slot = *self.slot_at(pos);
        let off = slot.pos as usize;
        let len = slot.klen as usize;
        &mut self.page.as_mut_slice()[off..off + len]
    }

    fn value_at_mut(&mut self, pos: usize) -> &mut [u8] {
        let slot = *self.slot_at(pos);
        let off = slot.pos as usize + slot.klen as usize;
        &mut self.page.as_mut_slice()[off..off + slot.vlen as usize]
    }

    fn available_space(&self) -> u32 {
        let header = self.header();
        let used = self.slot_base() as u32 + header.elems * SLOT_SIZE as u32;
        header.offset.saturating_sub(used)
    }

    pub(crate) fn full_key(&self, pos: usize, buf: &mut Vec<u8>) {
        buf.clear();
        buf.extend_from_slice(self.prefix());
        buf.extend_from_slice(self.tail_at(pos));
    }

    /// Collects every entry into one scratch buffer. Full keys are needed to
    /// compute a new prefix, but keeping keys and inline values in one buffer
    /// avoids a pair of heap allocations per existing entry on rebuilds.
    fn collect_slot_entries(&self) -> EncodedLeafEntries {
        let count = self.num_children();
        let prefix = self.prefix();
        let data_capacity = count * prefix.len()
            + (0..count)
                .map(|pos| {
                    let slot = self.slot_at(pos);
                    slot.key_len() + usize::from(slot.is_inline()) * slot.value_len()
                })
                .sum::<usize>();
        let mut entries = EncodedLeafEntries::with_capacity(count, data_capacity);

        for pos in 0..count {
            let slot = *self.slot_at(pos);
            let key_start = entries.data.len();
            entries.data.extend_from_slice(prefix);
            entries.data.extend_from_slice(self.tail_at(pos));
            let key_len = entries.data.len() - key_start;
            let value_start = entries.data.len();
            if slot.is_inline() {
                entries.data.extend_from_slice(self.value_at(pos));
            }
            let value_len = entries.data.len() - value_start;
            entries.entries.push(EncodedLeafEntry {
                slot,
                key_start,
                key_len,
                value_start,
                value_len,
            });
        }
        entries
    }

    /// Rebuilds this leaf from full-key entries, recomputing the shared
    /// prefix. `slot.page_id` is preserved for overflow values; inline payload
    /// bytes follow the (possibly shorter) new tail.
    fn rebuild_leaf_from_entries(
        &mut self,
        entries: &EncodedLeafEntries,
        range: std::ops::Range<usize>,
    ) {
        assert!(!range.is_empty());
        let prefix_len = entries.common_prefix_len(range.start, range.end, None);
        self.rebuild_leaf_with_prefix(entries, range, prefix_len);
    }

    /// Whether `entries` plus one newly inserted entry fit one encoded leaf
    /// under the shared prefix they would compute. The new entry is described
    /// by its stored tail length and inline/overflow choice rather than a slot,
    /// so the check runs before any allocation. `new_prefix_len` is the common
    /// prefix of every existing key plus the new key (rebuild re-encodes all
    /// tails against it).
    fn rebuild_leaf_fits(
        entries: &EncodedLeafEntries,
        range: std::ops::Range<usize>,
        new_prefix_len: usize,
        new_tail_len: usize,
        new_inline: bool,
        new_vlen: usize,
    ) -> bool {
        let slot_base = (ENCODED_HEADER_SIZE + new_prefix_len + 3) & !3;
        let data: usize = entries.entries[range.clone()]
            .iter()
            .map(|entry| {
                if entry.slot.is_inline() {
                    entry.key_len - new_prefix_len + entry.slot.value_len()
                } else {
                    entry.key_len - new_prefix_len
                }
            })
            .sum();
        let new_data = if new_inline {
            new_tail_len + new_vlen
        } else {
            new_tail_len
        };
        slot_base + (range.len() + 1) * SLOT_SIZE + data + new_data <= PAGE_SIZE
    }

    /// Rebuilds this leaf from `entries` so it stores exactly `new_prefix_len`
    /// prefix bytes, re-encoding every tail. The caller has already verified
    /// the result fits a page; this is the encoded equivalent of the plain
    /// leaf's `compact`.
    fn rebuild_leaf_with_prefix(
        &mut self,
        entries: &EncodedLeafEntries,
        range: std::ops::Range<usize>,
        new_prefix_len: usize,
    ) {
        debug_assert!(!range.is_empty());
        let prefix = &entries.key(range.start)[..new_prefix_len];
        let start = range.start;
        let end = range.end;
        write_encoded_page(
            &mut self.page,
            ENCODED_LEAF,
            prefix,
            range.len(),
            (start..end).map(|pos| {
                let entry = &entries.entries[pos];
                let key = entries.key_at_entry(entry);
                let value = if entry.slot.is_inline() {
                    entries.value_at_entry(entry)
                } else {
                    &[]
                };
                EncodedPageEntry {
                    slot: entry.slot,
                    tail: &key[new_prefix_len..],
                    value,
                }
            }),
        );
    }

    fn encoded_leaf_entries_fit_with_prefix(
        entries: &EncodedLeafEntries,
        range: std::ops::Range<usize>,
        prefix_len: usize,
    ) -> bool {
        if range.is_empty() {
            return false;
        }

        let slot_base = (ENCODED_HEADER_SIZE + prefix_len + 3) & !3;
        let data_bytes: usize = entries.entries[range.clone()]
            .iter()
            .map(|entry| {
                let tail_len = entry.key_len - prefix_len;
                if entry.slot.is_inline() {
                    tail_len + entry.slot.value_len()
                } else {
                    tail_len
                }
            })
            .sum();
        slot_base + range.len() * SLOT_SIZE + data_bytes <= PAGE_SIZE
    }

    fn updated_leaf_entries(
        &self,
        pos: usize,
        replacement_slot: Slot,
        replacement_inline_value: &[u8],
    ) -> EncodedLeafEntries {
        let count = self.num_children();
        let prefix = self.prefix();
        let data_capacity = count * prefix.len()
            + (0..count)
                .map(|idx| {
                    let slot = if idx == pos {
                        replacement_slot
                    } else {
                        *self.slot_at(idx)
                    };
                    slot.key_len()
                        + if idx == pos {
                            usize::from(slot.is_inline()) * replacement_inline_value.len()
                        } else if slot.is_inline() {
                            slot.value_len()
                        } else {
                            0
                        }
                })
                .sum::<usize>();
        let mut entries = EncodedLeafEntries::with_capacity(count, data_capacity);
        let mut key = Vec::new();
        for idx in 0..count {
            self.full_key(idx, &mut key);
            let slot = if idx == pos {
                replacement_slot
            } else {
                *self.slot_at(idx)
            };
            let inline_value = if slot.is_inline() {
                if idx == pos {
                    replacement_inline_value
                } else {
                    self.value_at(idx)
                }
            } else {
                &[]
            };
            let entry = entries.append_entry_data(&key, slot, inline_value);
            entries.entries.push(entry);
        }
        entries
    }

    pub(crate) fn put_leaf(
        &mut self,
        ctx: &mut TreeWriteContext,
        key: &[u8],
        value: &[u8],
    ) -> StoreResult<LeafWrite> {
        if !self.is_leaf() {
            crate::invariant("PUT_NON_LEAF", "attempted leaf put on branch node");
        }
        if key.len() > MAX_KEY_LEN {
            crate::invariant("LEAF_KEY_TOO_LARGE", "leaf key exceeds MAX_KEY_LEN");
        }

        if self.is_empty() {
            // First entry: the empty shared prefix matches every key, which
            // would keep every slot at full length until the leaf splits.
            // Establish the prefix as this key so a single-key leaf stores no
            // per-slot key bytes and the next key that shares bytes triggers a
            // rebuild that computes the real common prefix.
            self.establish_prefix_for_empty(key);
        }

        if key.starts_with(self.prefix()) {
            let tail = &key[self.prefix().len()..];
            match self.search(key) {
                Ok(pos) => {
                    self.update_leaf_at(ctx, pos, value)?;
                }
                Err(pos) => {
                    let data_len = if tail.len() + value.len() <= MAX_INLINE_LEN {
                        tail.len() + value.len()
                    } else {
                        tail.len()
                    };
                    let total_required = data_len as u32 + SLOT_SIZE as u32;
                    if self.available_space() < total_required {
                        // Compact first: an insert/delete cycle can leave dead
                        // payload bytes that available_space() counts as used.
                        // Compacting frees them; only then decide to split.
                        let mut entries = self.collect_slot_entries();
                        let insert_at = entries
                            .entries
                            .partition_point(|entry| entries.key_at_entry(entry) < key);
                        let new_prefix_len = entries.common_prefix_len(0, entries.len(), Some(key));
                        let new_tail_len = key.len() - new_prefix_len;
                        let inline = value_inline_in_slot(new_tail_len, value.len());
                        if Self::rebuild_leaf_fits(
                            &entries,
                            0..entries.len(),
                            new_prefix_len,
                            new_tail_len,
                            inline,
                            value.len(),
                        ) {
                            let slot = self.make_entry_slot(ctx, value, inline)?;
                            entries.insert(insert_at, key, slot, if inline { value } else { &[] });
                            self.rebuild_leaf_with_prefix(
                                &entries,
                                0..entries.len(),
                                new_prefix_len,
                            );
                        } else {
                            return Ok(LeafWrite::SplitRequired);
                        }
                    } else {
                        self.insert_leaf_at_tail(ctx, pos, tail, value)?;
                    }
                }
            }
        } else {
            // The new key leaves the shared-prefix domain: rebuild the whole
            // leaf with a fresh prefix. Decide the new prefix, the stored tail
            // length, and whether the entry fits BEFORE allocating any overflow
            // pages, so a split trigger never leaks pages (the plain tail path
            // checks available_space before allocating for the same reason).
            let mut entries = self.collect_slot_entries();
            let insert_at = entries
                .entries
                .partition_point(|entry| entries.key_at_entry(entry) < key);
            let new_prefix_len = entries.common_prefix_len(0, entries.len(), Some(key));
            let new_tail_len = key.len() - new_prefix_len;
            let new_inline = new_tail_len + value.len() <= MAX_INLINE_LEN;
            if !Self::rebuild_leaf_fits(
                &entries,
                0..entries.len(),
                new_prefix_len,
                new_tail_len,
                new_inline,
                value.len(),
            ) {
                return Ok(LeafWrite::SplitRequired);
            }
            let slot = self.make_entry_slot(ctx, value, new_inline)?;
            entries.insert(insert_at, key, slot, if new_inline { value } else { &[] });
            self.rebuild_leaf_from_entries(&entries, 0..entries.len());
        }
        Ok(LeafWrite::Applied)
    }

    /// Builds the slot for a freshly inserted entry. Overflow values are
    /// written to newly allocated pages and the slot carries their page ids.
    /// The caller has already decided `inline` from the actual stored tail
    /// length (not the full key).
    fn make_entry_slot(
        &mut self,
        ctx: &mut TreeWriteContext,
        value: &[u8],
        inline: bool,
    ) -> StoreResult<Slot> {
        make_value_slot(ctx, 0, value, inline)
    }

    fn insert_leaf_at_tail(
        &mut self,
        ctx: &mut TreeWriteContext,
        pos: usize,
        tail: &[u8],
        value: &[u8],
    ) -> StoreResult<()> {
        let (cur_off, slot_copy) = make_leaf_slot(ctx, self.header().offset, tail.len(), value)?;

        self.header_mut().offset = cur_off;
        let slot = self.expand_slot(pos);
        *slot = slot_copy;

        if slot_copy.is_inline() {
            self.value_at_mut(pos).copy_from_slice(value);
        }
        if !tail.is_empty() {
            self.tail_at_mut(pos).copy_from_slice(tail);
        }
        Ok(())
    }

    pub(crate) fn get(&self, read: &TreeReadContext, key: &[u8]) -> Option<Vec<u8>> {
        match self.search(key) {
            Ok(pos) => {
                let slot = self.slot_at(pos);
                if slot.is_inline() {
                    Some(self.value_at(pos).to_vec())
                } else {
                    Some(load_overflow_value(read, slot))
                }
            }
            Err(_) => None,
        }
    }

    pub(crate) fn search(&self, key: &[u8]) -> std::result::Result<usize, usize> {
        let prefix = self.prefix();
        // Leaves have no sentinel; branches skip slot 0, whose decoded full
        // key is the empty sentinel `""` rather than `prefix`, so comparing it
        // against `key` via `cmp_raw_with_prefixed_tail` would be wrong.
        let mut lo = if self.is_leaf() { 0usize } else { 1usize };
        let mut hi = self.num_children();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match cmp_raw_with_prefixed_tail(key, prefix, self.tail_at(mid)) {
                Ordering::Greater => lo = mid + 1,
                _ => hi = mid,
            }
        }
        if lo < self.num_children()
            && cmp_raw_with_prefixed_tail(key, prefix, self.tail_at(lo)) == Ordering::Equal
        {
            Ok(lo)
        } else {
            Err(lo)
        }
    }

    pub(crate) fn child_pos_for_key(&self, key: &[u8]) -> usize {
        debug_assert!(!self.is_leaf());
        // Slot 0 is the empty sentinel (always <= key); only real separators
        // at 1..elems participate, so a key below every separator returns 0
        // without a comparator special case.
        let prefix = self.prefix();
        let mut lo = 1usize;
        let mut hi = self.num_children();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match cmp_raw_with_prefixed_tail(key, prefix, self.tail_at(mid)) {
                Ordering::Less => hi = mid,
                _ => lo = mid + 1,
            }
        }
        lo.saturating_sub(1)
    }

    pub(crate) fn update_leaf_at(
        &mut self,
        ctx: &mut TreeWriteContext,
        pos: usize,
        value: &[u8],
    ) -> StoreResult<()> {
        if !self.is_leaf() {
            crate::invariant("UPDATE_NON_LEAF", "attempted leaf update on branch node");
        }

        let old_slot = *self.slot_at(pos);
        if old_slot.is_inline() && value.len() <= old_slot.value_len() {
            self.value_at_mut(pos)[..value.len()].copy_from_slice(value);
            self.slot_at_mut(pos).update_vlen(value.len() as u32);
            return Ok(());
        }

        let tail_len = old_slot.key_len();
        let inline = value_inline_in_slot(tail_len, value.len());
        if inline {
            let replacement = make_inline_slot(tail_len, value.len());
            let entries = self.updated_leaf_entries(pos, replacement, value);
            let prefix_len = self.prefix().len();
            if Self::encoded_leaf_entries_fit_with_prefix(&entries, 0..entries.len(), prefix_len) {
                self.rebuild_leaf_with_prefix(&entries, 0..entries.len(), prefix_len);
                if !old_slot.is_inline() {
                    ctx.free_slot(&old_slot);
                }
                return Ok(());
            }
        }

        let replacement = self.make_entry_slot(ctx, value, false)?;
        let entries = self.updated_leaf_entries(pos, replacement, &[]);
        self.rebuild_leaf_with_prefix(&entries, 0..entries.len(), self.prefix().len());
        if !old_slot.is_inline() {
            ctx.free_slot(&old_slot);
        }
        Ok(())
    }

    fn expand_slot(&mut self, pos: usize) -> &mut Slot {
        let elems = self.header().elems;
        let base = self.slot_base();
        let slot_off = base + pos * SLOT_SIZE;
        let last_slot_off = base + elems as usize * SLOT_SIZE;

        if pos < elems as usize {
            self.page
                .as_mut_slice()
                .copy_within(slot_off..last_slot_off, slot_off + SLOT_SIZE);
        }

        self.header_mut().elems += 1;
        self.slot_at_mut(pos)
    }

    fn shrink_slot(&mut self, pos: usize) -> Slot {
        let elems = self.header().elems;
        let slot = *self.slot_at(pos);
        let base = self.slot_base();
        let slot_off = base + pos * SLOT_SIZE;
        let next_slot_off = slot_off + SLOT_SIZE;
        let last_slot_off = base + elems as usize * SLOT_SIZE;

        if pos + 1 < elems as usize {
            self.page
                .as_mut_slice()
                .copy_within(next_slot_off..last_slot_off, slot_off);
        }

        self.header_mut().elems -= 1;
        slot
    }

    pub(crate) fn delete_leaf_key(&mut self, ctx: &mut TreeWriteContext, key: &[u8]) {
        if !self.is_leaf() {
            crate::invariant("DELETE_NON_LEAF", "attempted leaf delete on branch node");
        }

        match self.search(key) {
            Ok(pos) => {
                let slot = self.shrink_slot(pos);
                if !slot.is_inline() {
                    ctx.free_slot(&slot);
                }
                // Deleting never shrinks the shared prefix: the old prefix is
                // still a prefix of every remaining key.
            }
            Err(_) => crate::invariant(
                "DELETE_MISSING_KEY",
                "delete reached a leaf without its key",
            ),
        }
    }

    #[cfg(test)]
    pub(crate) fn split_leaf(&mut self) -> (NonEmptyKey, Node) {
        if !self.is_leaf() {
            crate::invariant("SPLIT_NON_LEAF", "attempted leaf split on branch node");
        }

        // Split by full keys; both halves re-encode tails against their own
        // (strictly shorter) prefix. LeanStore's Split copies the two ranges
        // into fresh pages and re-encodes each half against its new fences;
        // the same structure is used here, so a near-full leaf splits into two
        // self-consistent halves for the existing entries. The write path uses
        // `split_leaf_for_insert` so a pending key that changes the prefix is
        // included in the pivot decision as well.
        let entries = self.collect_slot_entries();
        let mid = entries.len() / 2;
        let sep = NonEmptyKey::new(entries.key(mid).to_vec()).unwrap_or_else(|| {
            crate::invariant(
                "EMPTY_LEAF_SPLIT_SEPARATOR",
                "split leaf contains an empty key",
            )
        });
        let mut left = EncodedNode::new_encoded_leaf();
        left.rebuild_leaf_from_entries(&entries, 0..mid);
        let mut right = EncodedNode::new_encoded_leaf();
        right.rebuild_leaf_from_entries(&entries, mid..entries.len());
        *self = left;
        (sep, Node::Encoded(right))
    }

    fn encoded_leaf_entries_fit(
        entries: &EncodedLeafEntries,
        range: std::ops::Range<usize>,
    ) -> bool {
        if range.is_empty() {
            return false;
        }

        let prefix_len = entries.common_prefix_len(range.start, range.end, None);
        Self::encoded_leaf_entries_fit_with_prefix(entries, range, prefix_len)
    }

    fn prefix_len_with_insert(
        entries: &EncodedLeafEntries,
        range: std::ops::Range<usize>,
        key: &[u8],
    ) -> usize {
        entries.common_prefix_len(range.start, range.end, Some(key))
    }

    fn encoded_leaf_fits_with_insert(
        entries: &EncodedLeafEntries,
        range: std::ops::Range<usize>,
        key: &[u8],
        value_len: usize,
    ) -> bool {
        let new_prefix_len = Self::prefix_len_with_insert(entries, range.clone(), key);
        let new_tail_len = key.len() - new_prefix_len;
        Self::rebuild_leaf_fits(
            entries,
            range,
            new_prefix_len,
            new_tail_len,
            new_tail_len + value_len <= MAX_INLINE_LEN,
            value_len,
        )
    }

    /// Chooses a pivot for the complete post-insert entry set. The candidate
    /// containing the pending key is checked with its actual re-encoded prefix
    /// before any value pages are allocated, so the result is always two leaf
    /// siblings rather than a recursive branch subtree.
    fn split_pivot_for_insert(
        entries: &EncodedLeafEntries,
        insert_at: usize,
        key: &[u8],
        value_len: usize,
    ) -> Option<usize> {
        let total = entries.len() + 1;
        let preferred = total / 2;

        for delta in 0..total {
            for pivot in [preferred.checked_sub(delta), preferred.checked_add(delta)] {
                let Some(pivot) = pivot else {
                    continue;
                };
                if pivot == 0 || pivot >= total {
                    continue;
                }

                let new_in_left = pivot > insert_at;
                let left_existing_len = if new_in_left { pivot - 1 } else { pivot };
                let left_fits = if new_in_left {
                    Self::encoded_leaf_fits_with_insert(
                        entries,
                        0..left_existing_len,
                        key,
                        value_len,
                    )
                } else {
                    Self::encoded_leaf_entries_fit(entries, 0..left_existing_len)
                };
                if !left_fits {
                    continue;
                }

                let right_fits = if new_in_left {
                    Self::encoded_leaf_entries_fit(entries, left_existing_len..entries.len())
                } else {
                    Self::encoded_leaf_fits_with_insert(
                        entries,
                        left_existing_len..entries.len(),
                        key,
                        value_len,
                    )
                };
                if right_fits {
                    return Some(pivot);
                }
            }
        }
        None
    }

    /// Splits an overflowing encoded leaf around the pending insert. The
    /// pending key participates in prefix and inline-size calculations before
    /// allocation, which keeps both returned children at the same tree level.
    pub(crate) fn split_leaf_for_insert(
        &mut self,
        ctx: &mut TreeWriteContext,
        key: &[u8],
        value: &[u8],
    ) -> StoreResult<(NonEmptyKey, Node)> {
        if !self.is_leaf() {
            crate::invariant(
                "SPLIT_NON_LEAF",
                "attempted encoded leaf split on a branch node",
            );
        }

        let mut entries = self.collect_slot_entries();
        let insert_at = entries
            .entries
            .partition_point(|entry| entries.key_at_entry(entry) < key);
        let pivot = Self::split_pivot_for_insert(&entries, insert_at, key, value.len())
            .unwrap_or_else(|| {
                crate::invariant(
                    "ENCODED_LEAF_SPLIT_PIVOT",
                    "no valid encoded leaf split pivot",
                )
            });
        let new_in_left = pivot > insert_at;
        let left_existing_len = if new_in_left { pivot - 1 } else { pivot };
        let target_range = if new_in_left {
            0..left_existing_len
        } else {
            left_existing_len..entries.len()
        };
        let new_prefix_len = Self::prefix_len_with_insert(&entries, target_range, key);
        let new_inline = key.len() - new_prefix_len + value.len() <= MAX_INLINE_LEN;
        let slot = self.make_entry_slot(ctx, value, new_inline)?;
        entries.insert(insert_at, key, slot, if new_inline { value } else { &[] });

        let separator = NonEmptyKey::new(entries.key(pivot).to_vec()).unwrap_or_else(|| {
            crate::invariant(
                "EMPTY_LEAF_SPLIT_SEPARATOR",
                "encoded leaf split contains an empty key",
            )
        });
        debug_assert!(Self::encoded_leaf_entries_fit(&entries, 0..pivot));
        debug_assert!(Self::encoded_leaf_entries_fit(
            &entries,
            pivot..entries.len()
        ));

        let mut left = EncodedNode::new_encoded_leaf();
        left.rebuild_leaf_from_entries(&entries, 0..pivot);
        let mut right = EncodedNode::new_encoded_leaf();
        right.rebuild_leaf_from_entries(&entries, pivot..entries.len());
        *self = left;
        Ok((separator, Node::Encoded(right)))
    }

    pub(crate) fn child_at(&self, pos: usize) -> DataPid {
        debug_assert!(!self.is_leaf());
        decode_pid(self.slot_at(pos).page_id[0])
    }

    pub(crate) fn branch_entries(&self) -> Vec<(Vec<u8>, DataPid)> {
        debug_assert!(!self.is_leaf());
        (0..self.num_children())
            .map(|pos| {
                let key = if pos == 0 {
                    Vec::new()
                } else {
                    let mut full =
                        Vec::with_capacity(self.prefix().len() + self.slot_at(pos).klen as usize);
                    full.extend_from_slice(self.prefix());
                    full.extend_from_slice(self.tail_at(pos));
                    full
                };
                (key, self.child_at(pos))
            })
            .collect()
    }

    pub(crate) fn update_child_page(&mut self, pos: ChildPos, page_id: DataPid) {
        debug_assert!(!self.is_leaf());
        let slot = self.slot_at_mut(pos.get());
        slot.page_id[0] = page_id.get();
        if !self.is_empty() {
            self.canonicalize_branch_slot_zero();
        }
    }

    pub(crate) fn canonicalize_branch_slot_zero(&mut self) {
        debug_assert!(!self.is_leaf());
        if self.is_empty() {
            return;
        }
        let child = self.slot_at(0).page_id[0];
        let slot = self.slot_at_mut(0);
        *slot = make_inline_slot(0, 0);
        slot.page_id[0] = child;
    }

    pub(crate) fn remove_branch_child(&mut self, pos: ChildPos) -> Slot {
        debug_assert!(!self.is_leaf());
        let slot = self.shrink_slot(pos.get());
        if !self.is_empty() {
            self.canonicalize_branch_slot_zero();
        }
        slot
    }

    pub(crate) fn apply_branch_split_rewrite(
        &mut self,
        pos: ChildPos,
        expected_old: DataPid,
        left_page_id: DataPid,
        separator: NonEmptyKey,
        right_page_id: DataPid,
    ) -> BranchRewrite {
        debug_assert!(!self.is_leaf());
        let pos = pos.get();

        if pos >= self.num_children() {
            crate::invariant(
                "BRANCH_REWRITE_POSITION",
                "parent rewrite position is out of bounds",
            );
        }
        if self.child_at(pos) != expected_old {
            crate::invariant(
                "BRANCH_REWRITE_EXPECTED_CHILD",
                "parent child no longer matches rewrite route",
            );
        }

        let mut entries = self.branch_entries();
        entries[pos].1 = left_page_id;
        entries.insert(pos + 1, (separator.0, right_page_id));
        entries[0].0.clear();

        if encoded_branch_entries_fit(&entries) {
            *self = encoded_branch_from_full_entries(&entries);
            return BranchRewrite::Applied;
        }

        let pivot = encoded_branch_split_pivot(&entries).unwrap_or_else(|| {
            crate::invariant("BRANCH_SPLIT_PIVOT", "no valid encoded branch split pivot")
        });
        let promoted_separator = NonEmptyKey::new(entries[pivot].0.clone()).unwrap_or_else(|| {
            crate::invariant(
                "EMPTY_BRANCH_PROMOTION_SEPARATOR",
                "branch split pivot must carry a non-empty separator",
            )
        });

        let mut left_entries = entries[..pivot].to_vec();
        let mut right_entries = entries[pivot..].to_vec();
        left_entries[0].0.clear();
        right_entries[0].0.clear();

        let right = encoded_branch_from_full_entries(&right_entries);
        *self = encoded_branch_from_full_entries(&left_entries);
        BranchRewrite::Split {
            separator: promoted_separator,
            right: Node::Encoded(right),
        }
    }
}

fn encoded_branch_split_pivot(entries: &[(Vec<u8>, DataPid)]) -> Option<usize> {
    let preferred = entries.len() / 2;
    for delta in 0..entries.len() {
        if let Some(pivot) = preferred.checked_sub(delta)
            && pivot > 0
            && pivot < entries.len()
            && !entries[pivot].0.is_empty()
            && encoded_branch_entries_fit(&entries[..pivot])
            && encoded_branch_entries_fit(&entries[pivot..])
        {
            return Some(pivot);
        }
        let pivot = preferred + delta;
        if delta != 0
            && pivot < entries.len()
            && pivot > 0
            && !entries[pivot].0.is_empty()
            && encoded_branch_entries_fit(&entries[..pivot])
            && encoded_branch_entries_fit(&entries[pivot..])
        {
            return Some(pivot);
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Node: the shared handle. Reads and writes dispatch on the page's first u32
// by matching the node class, so no `is_encoded()` branch appears in the tree
// logic.
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub(crate) enum Node {
    Plain(PlainNode),
    Encoded(EncodedNode),
}

impl Node {
    /// Interprets a page that may be either node class. A first u32 of 0/1 is
    /// a plain node (`is_leaf`); 2/3 is an encoded node (`kind`).
    pub(crate) fn from_aligned_page(
        page: AlignedPage,
    ) -> std::result::Result<Self, NodeDecodeError> {
        let first = unsafe { page.as_ptr().cast::<u32>().read_unaligned() };
        if first >= ENCODED_BRANCH {
            Ok(Node::Encoded(EncodedNode::from_aligned_page(page)?))
        } else {
            Ok(Node::Plain(PlainNode::from_aligned_page(page)?))
        }
    }

    #[cfg(test)]
    pub(crate) fn from_raw(data: Vec<u8>) -> std::result::Result<Self, NodeDecodeError> {
        assert!(data.len() >= PLAIN_HEADER_SIZE);
        let page = AlignedPage::from_vec(data.clone());
        let first = unsafe { page.as_ptr().cast::<u32>().read_unaligned() };
        if first >= ENCODED_BRANCH {
            EncodedNode::from_raw(data).map(Node::Encoded)
        } else {
            PlainNode::from_raw(data).map(Node::Plain)
        }
    }

    pub(crate) fn into_aligned_page(self) -> AlignedPage {
        match self {
            Node::Plain(node) => node.into_aligned_page(),
            Node::Encoded(node) => node.into_aligned_page(),
        }
    }

    pub(crate) fn finalize(&self) -> &[u8] {
        match self {
            Node::Plain(node) => node.finalize(),
            Node::Encoded(node) => node.finalize(),
        }
    }

    pub(crate) fn is_leaf(&self) -> bool {
        match self {
            Node::Plain(node) => node.is_leaf(),
            Node::Encoded(node) => node.is_leaf(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        match self {
            Node::Plain(node) => node.is_empty(),
            Node::Encoded(node) => node.is_empty(),
        }
    }

    pub(crate) fn num_children(&self) -> usize {
        match self {
            Node::Plain(node) => node.num_children(),
            Node::Encoded(node) => node.num_children(),
        }
    }

    pub(crate) fn new_leaf() -> Self {
        Node::Plain(PlainNode::new_leaf())
    }

    pub(crate) fn new_branch_root(
        left_page_id: DataPid,
        separator: NonEmptyKey,
        right_page_id: DataPid,
    ) -> Self {
        Node::Plain(PlainNode::new_branch_root(
            left_page_id,
            separator,
            right_page_id,
        ))
    }

    #[cfg(test)]
    pub(crate) fn new_branch_single(child: DataPid) -> Self {
        Node::Plain(PlainNode::new_branch_single(child))
    }

    pub(crate) fn new_encoded_leaf() -> Self {
        Node::Encoded(EncodedNode::new_encoded_leaf())
    }

    pub(crate) fn new_encoded_branch_root(
        left_page_id: DataPid,
        separator: NonEmptyKey,
        right_page_id: DataPid,
    ) -> Self {
        Node::Encoded(EncodedNode::new_encoded_branch_root(
            left_page_id,
            separator,
            right_page_id,
        ))
    }

    pub(crate) fn search(&self, key: &[u8]) -> std::result::Result<usize, usize> {
        match self {
            Node::Plain(node) => node.search(key),
            Node::Encoded(node) => node.search(key),
        }
    }

    pub(crate) fn child_pos_for_key(&self, key: &[u8]) -> usize {
        match self {
            Node::Plain(node) => node.child_pos_for_key(key),
            Node::Encoded(node) => node.child_pos_for_key(key),
        }
    }

    pub(crate) fn full_key(&self, pos: usize, buf: &mut Vec<u8>) {
        match self {
            Node::Plain(node) => node.full_key(pos, buf),
            Node::Encoded(node) => node.full_key(pos, buf),
        }
    }

    pub(crate) fn put_leaf(
        &mut self,
        ctx: &mut TreeWriteContext,
        key: &[u8],
        value: &[u8],
    ) -> StoreResult<LeafWrite> {
        match self {
            Node::Plain(node) => node.put_leaf(ctx, key, value),
            Node::Encoded(node) => node.put_leaf(ctx, key, value),
        }
    }

    pub(crate) fn get(&self, read: &TreeReadContext, key: &[u8]) -> Option<Vec<u8>> {
        match self {
            Node::Plain(node) => node.get(read, key),
            Node::Encoded(node) => node.get(read, key),
        }
    }

    pub(crate) fn delete_leaf_key(&mut self, ctx: &mut TreeWriteContext, key: &[u8]) {
        match self {
            Node::Plain(node) => node.delete_leaf_key(ctx, key),
            Node::Encoded(node) => node.delete_leaf_key(ctx, key),
        }
    }

    /// Splits an overflowing leaf while keeping both returned children at the
    /// same tree level. Encoded leaves choose the pivot using the pending
    /// insert's actual prefix and storage shape; plain leaves use the original
    /// midpoint split because their key layout does not change.
    pub(crate) fn split_leaf_for_insert(
        &mut self,
        ctx: &mut TreeWriteContext,
        key: &[u8],
        value: &[u8],
    ) -> StoreResult<(NonEmptyKey, Node)> {
        match self {
            Node::Plain(node) => {
                let (separator, mut right) = node.split_leaf();
                if key < separator.as_slice() {
                    if !matches!(node.put_leaf(ctx, key, value)?, LeafWrite::Applied) {
                        crate::invariant(
                            "PLAIN_SPLIT_OVERFLOW",
                            "a plain split half must accept its pending write",
                        );
                    }
                } else if !matches!(right.put_leaf(ctx, key, value)?, LeafWrite::Applied) {
                    crate::invariant(
                        "PLAIN_SPLIT_OVERFLOW",
                        "a plain split half must accept its pending write",
                    );
                }
                Ok((separator, right))
            }
            Node::Encoded(node) => node.split_leaf_for_insert(ctx, key, value),
        }
    }

    pub(crate) fn slot_at(&self, pos: usize) -> &Slot {
        match self {
            Node::Plain(node) => node.slot_at(pos),
            Node::Encoded(node) => node.slot_at(pos),
        }
    }

    /// Stored key bytes for the slot. For an encoded node this is only the
    /// tail (`key[prefix..]`), not the full key; use [`Node::full_key`] for the
    /// logical key.
    #[cfg(test)]
    pub(crate) fn stored_key_at(&self, pos: usize) -> &[u8] {
        match self {
            Node::Plain(node) => node.key_at(pos),
            Node::Encoded(node) => node.tail_at(pos),
        }
    }

    pub(crate) fn value_at(&self, pos: usize) -> &[u8] {
        match self {
            Node::Plain(node) => node.value_at(pos),
            Node::Encoded(node) => node.value_at(pos),
        }
    }

    #[allow(dead_code)] // exposed for layout tests that read raw node bytes
    pub(crate) fn child_at(&self, pos: usize) -> DataPid {
        match self {
            Node::Plain(node) => node.child_at(pos),
            Node::Encoded(node) => node.child_at(pos),
        }
    }

    pub(crate) fn update_child_page(&mut self, pos: ChildPos, page_id: DataPid) {
        match self {
            Node::Plain(node) => node.update_child_page(pos, page_id),
            Node::Encoded(node) => node.update_child_page(pos, page_id),
        }
    }

    pub(crate) fn canonicalize_branch_slot_zero(&mut self) {
        match self {
            Node::Plain(node) => node.canonicalize_branch_slot_zero(),
            Node::Encoded(node) => node.canonicalize_branch_slot_zero(),
        }
    }

    pub(crate) fn remove_branch_child(&mut self, pos: ChildPos) -> Slot {
        match self {
            Node::Plain(node) => node.remove_branch_child(pos),
            Node::Encoded(node) => node.remove_branch_child(pos),
        }
    }

    pub(crate) fn apply_branch_split_rewrite(
        &mut self,
        pos: ChildPos,
        expected_old: DataPid,
        left_page_id: DataPid,
        separator: NonEmptyKey,
        right_page_id: DataPid,
    ) -> BranchRewrite {
        match self {
            Node::Plain(node) => node.apply_branch_split_rewrite(
                pos,
                expected_old,
                left_page_id,
                separator,
                right_page_id,
            ),
            Node::Encoded(node) => node.apply_branch_split_rewrite(
                pos,
                expected_old,
                left_page_id,
                separator,
                right_page_id,
            ),
        }
    }

    pub(crate) fn free_slot_pages(
        &self,
        read: &TreeReadContext,
        slot: &Slot,
        freed: &mut Vec<(PageId, u32)>,
    ) {
        free_slot_pages_for(read, slot, freed);
    }

    pub(crate) fn load_overflow_value(&self, read: &TreeReadContext, slot: &Slot) -> Vec<u8> {
        load_overflow_value(read, slot)
    }

    pub(crate) fn update_leaf_at(
        &mut self,
        ctx: &mut TreeWriteContext,
        pos: usize,
        value: &[u8],
    ) -> StoreResult<()> {
        match self {
            Node::Plain(node) => node.update_leaf_at(ctx, pos, value),
            Node::Encoded(node) => node.update_leaf_at(ctx, pos, value),
        }
    }

    #[cfg(test)]
    pub(crate) fn collect_slot_storage_ids(
        &self,
        read: &TreeReadContext,
        slot: &Slot,
    ) -> Vec<DataPid> {
        collect_slot_storage_ids(read, slot)
    }

    #[cfg(test)]
    pub(crate) fn header_mut(&mut self) -> &mut PlainHeader {
        match self {
            Node::Plain(node) => node.header_mut(),
            // The first three u32 of an encoded header overlap `NodeHeader`;
            // only used by validation tests that mutate plain pages.
            Node::Encoded(node) => unsafe { &mut *node.page.as_mut_ptr().cast::<PlainHeader>() },
        }
    }

    /// Encoded leaf shared-prefix length (0 for plain or a leaf whose keys
    /// share no prefix). Test-only hook for prefix-establishment assertions.
    #[cfg(test)]
    pub(crate) fn test_encoded_prefix_len(&self) -> usize {
        match self {
            Node::Plain(_) => 0,
            Node::Encoded(node) => node.header().prefix_len as usize,
        }
    }
}

#[cfg(test)]
mod validation_tests {
    use super::*;
    use crate::{
        BTreeRuntime, Layout as NodeLayout, OpenOptions, Store, TreeReadContext, TreeWriteContext,
    };
    use std::collections::HashSet;
    use std::sync::Arc;

    fn leaf_entries(entries: &[(Vec<u8>, Slot, Vec<u8>)]) -> EncodedLeafEntries {
        let mut collected = EncodedLeafEntries::with_capacity(entries.len(), 0);
        for (key, slot, value) in entries {
            collected.push(key, *slot, value);
        }
        collected
    }

    fn test_read(layout: NodeLayout) -> TreeReadContext {
        let dir = tempfile::TempDir::new().unwrap();
        let store = Arc::new(
            Store::open(dir.path().join("node-tests.db"), &OpenOptions::default()).unwrap(),
        );
        let runtime = BTreeRuntime::new(store, OpenOptions::default().cache_capacity);
        TreeReadContext::new(runtime).with_layout(layout)
    }

    #[test]
    fn common_prefix_len_returns_zero_for_empty_input() {
        assert_eq!(common_prefix_len(&[]), 0);
    }

    #[test]
    fn plain_branch_slot_zero_canonicalizes_all_bytes_but_preserves_child() {
        let left = DataPid::new(11).unwrap();
        let right = DataPid::new(12).unwrap();
        let mut node =
            PlainNode::new_branch_root(left, NonEmptyKey::new(b"sep".to_vec()).unwrap(), right);
        let slot = node.slot_at_mut(0);
        slot.pos = 123;
        slot.klen = 7;
        slot.vlen = 9;
        slot.page_id[1] = 88;

        node.canonicalize_branch_slot_zero();

        let slot = node.slot_at(0);
        assert_eq!(slot.page_id[0], left.get());
        assert_eq!(slot.page_id[1], 0);
        assert_eq!(slot.pos, 0);
        assert_eq!(slot.klen, 0);
        assert_eq!(slot.vlen, 0);
    }

    #[test]
    fn encoded_branch_slot_zero_canonicalizes_all_bytes_but_preserves_child() {
        let left = DataPid::new(21).unwrap();
        let right = DataPid::new(22).unwrap();
        let mut node = EncodedNode::new_encoded_branch_root(
            left,
            NonEmptyKey::new(b"prefix/sep".to_vec()).unwrap(),
            right,
        );
        let slot = node.slot_at_mut(0);
        slot.pos = 321;
        slot.klen = 5;
        slot.vlen = 7;
        slot.page_id[1] = 66;

        node.canonicalize_branch_slot_zero();

        let slot = node.slot_at(0);
        assert_eq!(slot.page_id[0], left.get());
        assert_eq!(slot.page_id[1], 0);
        assert_eq!(slot.pos, 0);
        assert_eq!(slot.klen, 0);
        assert_eq!(slot.vlen, 0);
    }

    #[test]
    fn plain_leaf_overwrite_can_grow_inline_without_split() {
        let read = test_read(NodeLayout::Plain);
        let mut leaf = PlainNode::new_leaf();
        let target_key = b"target-key";
        let filler_value = b"x";

        let mut freed = Vec::new();
        let mut alloc = HashSet::new();
        {
            let mut ctx = TreeWriteContext::new(&read, &mut freed, &mut alloc);
            assert!(matches!(
                leaf.put_leaf(&mut ctx, target_key, filler_value).unwrap(),
                LeafWrite::Applied
            ));
            let mut i = 0u32;
            while leaf.available_space() >= 96 {
                let key = format!("filler-{i:03}").into_bytes();
                assert!(matches!(
                    leaf.put_leaf(&mut ctx, &key, filler_value).unwrap(),
                    LeafWrite::Applied
                ));
                i += 1;
            }
            let extra = leaf.available_space() as usize;
            let new_value = vec![b'v'; filler_value.len() + extra];
            assert!(value_inline_in_slot(target_key.len(), new_value.len()));
            assert!(
                leaf.available_space() < (target_key.len() + new_value.len() + SLOT_SIZE) as u32
            );
            assert!(matches!(
                leaf.put_leaf(&mut ctx, target_key, &new_value).unwrap(),
                LeafWrite::Applied
            ));
            assert_eq!(
                leaf.get(&read, target_key).as_deref(),
                Some(new_value.as_slice())
            );
            assert!(leaf.slot_at(leaf.search(target_key).unwrap()).is_inline());
        }
        assert!(freed.is_empty());
    }

    #[test]
    fn encoded_leaf_update_reclaims_overflow_when_value_fits_inline() {
        let read = test_read(NodeLayout::Prefix);
        let mut leaf = EncodedNode::new_encoded_leaf();
        let key = b"user/0001";
        let large = vec![b'L'; MAX_INLINE_LEN + 64];
        let small = b"tiny-inline".to_vec();

        let mut freed = Vec::new();
        let mut alloc = HashSet::new();
        {
            let mut ctx = TreeWriteContext::new(&read, &mut freed, &mut alloc);
            assert!(matches!(
                leaf.put_leaf(&mut ctx, key, &large).unwrap(),
                LeafWrite::Applied
            ));
            let old_slot = *leaf.slot_at(leaf.search(key).unwrap());
            assert!(!old_slot.is_inline());

            leaf.update_leaf_at(&mut ctx, leaf.search(key).unwrap(), &small)
                .unwrap();
        }

        let slot = leaf.slot_at(leaf.search(key).unwrap());
        assert!(slot.is_inline());
        assert_eq!(leaf.get(&read, key).as_deref(), Some(small.as_slice()));
        assert!(!freed.is_empty(), "old overflow storage must be retired");
    }

    #[test]
    fn node_validation_preserves_header_bounds() {
        assert_eq!(std::mem::size_of::<PlainHeader>(), 12);
        assert_eq!(std::mem::size_of::<EncodedHeader>(), 16);

        let assert_rejected = |node: Node| {
            let page = node.finalize().to_vec();
            assert_eq!(
                Node::from_raw(page).err(),
                Some(NodeDecodeError::Corruption)
            );
        };

        let mut invalid_leaf = Node::new_leaf();
        invalid_leaf.header_mut().is_leaf = 7;
        assert_rejected(invalid_leaf);

        let mut invalid_count = Node::new_leaf();
        invalid_count.header_mut().elems = ((PAGE_SIZE - PLAIN_HEADER_SIZE) / SLOT_SIZE + 1) as u32;
        assert_rejected(invalid_count);

        let mut invalid_low_offset = Node::new_leaf();
        invalid_low_offset.header_mut().offset = (PLAIN_HEADER_SIZE - 1) as u32;
        assert_rejected(invalid_low_offset);

        let mut invalid_high_offset = Node::new_leaf();
        invalid_high_offset.header_mut().offset = (PAGE_SIZE + 1) as u32;
        assert_rejected(invalid_high_offset);
    }

    #[test]
    fn from_aligned_page_distinguishes_plain_and_encoded_classes() {
        let plain = Node::new_leaf();
        let encoded = Node::new_encoded_leaf();

        assert!(matches!(
            Node::from_aligned_page(plain.clone().into_aligned_page()),
            Ok(Node::Plain(_))
        ));
        assert!(matches!(
            Node::from_aligned_page(encoded.clone().into_aligned_page()),
            Ok(Node::Encoded(_))
        ));

        // Round-trip through the shared validator.
        let plain2 = Node::from_aligned_page(plain.clone().into_aligned_page()).unwrap();
        let encoded2 = Node::from_aligned_page(encoded.clone().into_aligned_page()).unwrap();
        assert!(plain2.is_leaf());
        assert!(encoded2.is_leaf());
    }

    #[test]
    fn indirect_layout_has_no_identity_header() {
        assert_eq!(IDS_PER_INDIRECT_PAGE, (PAGE_SIZE - 4) / 4);

        let mut page = [0u8; PAGE_SIZE];
        page[0..4].copy_from_slice(&11u32.to_le_bytes());
        page[4..8].copy_from_slice(&22u32.to_le_bytes());
        page[8..12].copy_from_slice(&33u32.to_le_bytes());
        page[12..16].copy_from_slice(&44u32.to_le_bytes());
        page[OFFSET_NEXT_INDIRECT..].copy_from_slice(&55u32.to_le_bytes());
        assert_eq!(
            u32::from_le_bytes(page[OFFSET_NEXT_INDIRECT..].try_into().unwrap()),
            55
        );
    }

    #[test]
    fn overflow_page_count_uses_full_pages() {
        let mut slot = Slot {
            pos: 0,
            klen: 1,
            vlen: PAGE_SIZE as u32,
            page_id: [2; NR_INLINE_PAGE],
        };
        assert_eq!(slot.nr_pages(), 1);
        slot.vlen += 1;
        assert_eq!(slot.nr_pages(), 2);
    }

    #[test]
    fn encoded_leaf_round_trip_preserves_full_keys() {
        let entries: Vec<(Vec<u8>, Vec<u8>)> = vec![
            (b"user/001/age".to_vec(), b"30".to_vec()),
            (b"user/001/name".to_vec(), b"alice".to_vec()),
            (b"user/002/name".to_vec(), b"bob".to_vec()),
        ];
        let node = encoded_leaf_from_full_entries(&entries);
        assert!(node.is_leaf());
        assert_eq!(node.header().prefix_len as usize, b"user/00".len());

        for (i, (k, v)) in entries.iter().enumerate() {
            assert_eq!(node.search(k), Ok(i), "search {k:?}");
            assert_eq!(node.value_at(i), v.as_slice());
            let mut full = Vec::new();
            node.full_key(i, &mut full);
            assert_eq!(full, *k);
        }
        assert_eq!(node.search(b"user/001/zz"), Err(2));
        assert_eq!(node.search(b"zzz"), Err(3));

        // The shared handle dispatches to the encoded implementation.
        let handle = Node::Encoded(node);
        for (i, (k, _)) in entries.iter().enumerate() {
            assert_eq!(handle.search(k), Ok(i), "dispatch search {k:?}");
        }
    }

    #[test]
    fn encoded_node_validation_rejects_bad_prefix_len() {
        let mut node = encoded_leaf_from_full_entries(&[(b"ab/1".to_vec(), b"v".to_vec())]);
        unsafe {
            let h = &mut *node.page.as_mut_ptr().cast::<EncodedHeader>();
            h.prefix_len = (PAGE_SIZE + 1) as u32;
        }
        assert_eq!(
            Node::from_raw(node.finalize().to_vec()).err(),
            Some(NodeDecodeError::Corruption)
        );

        let node = encoded_leaf_from_full_entries(&[(b"ab/1".to_vec(), b"v".to_vec())]);
        assert!(Node::from_raw(node.finalize().to_vec()).is_ok());
    }

    #[test]
    fn encoded_rebuild_fits_rejects_an_entry_that_would_not_fit() {
        // The cross-prefix path must decide whether a rebuild fits BEFORE
        // allocating any overflow pages. Build a near-full inline leaf and
        // confirm the fit check rejects an additional entry (so the caller
        // returns SplitRequired without allocating).
        let slot = |vlen: usize| Slot {
            pos: 0,
            klen: 0,
            vlen: vlen as u32,
            page_id: [0; NR_INLINE_PAGE],
        };
        let near_full = leaf_entries(
            &(0..16u32)
                .map(|i| (format!("user/{i:03}").into_bytes(), slot(200), vec![0; 200]))
                .collect::<Vec<_>>(),
        );
        // Under a shared prefix of 0 (cross-prefix rebuild), each 9-byte key
        // with a 200-byte inline value plus the new entry exceeds one page.
        assert!(
            !EncodedNode::rebuild_leaf_fits(&near_full, 0..near_full.len(), 0, 9, true, 200),
            "a near-full leaf plus a cross-prefix inline entry must not fit"
        );

        // A sparse leaf leaves room.
        let sparse = leaf_entries(
            &(0..4u32)
                .map(|i| (format!("user/{i:03}").into_bytes(), slot(200), vec![0; 200]))
                .collect::<Vec<_>>(),
        );
        assert!(
            EncodedNode::rebuild_leaf_fits(&sparse, 0..sparse.len(), 0, 9, true, 200),
            "a sparse leaf must fit the new entry"
        );

        // An overflow entry stores only its tail, so it is far cheaper than
        // an inline entry of the same value length.
        assert!(
            EncodedNode::rebuild_leaf_fits(&near_full, 0..near_full.len(), 0, 9, false, 300),
            "overflow tails must be cheap enough to fit a near-full leaf"
        );
    }

    #[test]
    fn encoded_cross_prefix_rebuild_preserves_entries() {
        // A leaf whose prefix no longer covers an incoming key must rebuild
        // with a fresh prefix without losing existing entries.
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (96..100u32)
            .map(|i| {
                (
                    format!("user/{i:03}/profile").into_bytes(),
                    vec![i as u8; 128],
                )
            })
            .collect();
        let mut node = encoded_leaf_from_full_entries(&entries);
        assert_eq!(node.prefix(), b"user/09");

        let new_key = b"user/100/profile".to_vec();
        let mut collected = node.collect_slot_entries();
        let insert_at = collected
            .entries
            .partition_point(|entry| collected.key_at_entry(entry) < new_key.as_slice());
        collected.insert(
            insert_at,
            &new_key,
            Slot {
                pos: 0,
                klen: 0,
                vlen: 0,
                page_id: [0; NR_INLINE_PAGE],
            },
            &[],
        );
        node.rebuild_leaf_from_entries(&collected, 0..collected.len());

        // The new prefix is the common prefix of all five keys.
        assert_eq!(node.prefix(), b"user/");
        assert_eq!(node.num_children(), 5);

        let mut keys = Vec::new();
        #[allow(clippy::needless_range_loop)]
        for pos in 0..node.num_children() {
            let mut k = Vec::new();
            node.full_key(pos, &mut k);
            keys.push(k);
            let v = node.value_at(pos);
            if pos < entries.len() {
                assert_eq!(v, entries[pos].1.as_slice(), "entry {pos} value");
            } else {
                // The unit test inserts the new entry with an empty inline
                // payload; it must not disturb existing values.
                assert!(v.is_empty(), "new entry inline value must be preserved");
            }
        }
        assert_eq!(
            keys,
            vec![
                b"user/096/profile".to_vec(),
                b"user/097/profile".to_vec(),
                b"user/098/profile".to_vec(),
                b"user/099/profile".to_vec(),
                new_key,
            ]
        );
    }

    #[test]
    fn encoded_split_leaf_rebuilds_both_halves_with_correct_prefixes() {
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..20u32)
            .map(|i| {
                (
                    format!("user/{i:03}/profile").into_bytes(),
                    vec![i as u8; 128],
                )
            })
            .collect();
        let mut node = encoded_leaf_from_full_entries(&entries);
        let (sep, right) = node.split_leaf();
        let left = node;
        let right = match right {
            Node::Encoded(n) => n,
            Node::Plain(_) => panic!("right must be encoded"),
        };

        let read = |n: &EncodedNode| -> Vec<Vec<u8>> {
            (0..n.num_children())
                .map(|pos| {
                    let mut k = Vec::new();
                    n.full_key(pos, &mut k);
                    k
                })
                .collect()
        };
        let left_keys = read(&left);
        let right_keys = read(&right);
        let mut all: Vec<Vec<u8>> = entries.iter().map(|(k, _)| k.clone()).collect();
        all.sort();
        let mid = all.len() / 2;
        let expect_left = all[..mid].to_vec();
        let expect_right = all[mid..].to_vec();
        let expect_sep = expect_right[0].clone();
        assert_eq!(
            sep.as_slice(),
            expect_sep.as_slice(),
            "separator must be the first right key"
        );
        assert_eq!(left_keys, expect_left, "left half keys");
        assert_eq!(right_keys, expect_right, "right half keys");

        // Values survive the structural rebuild.
        for (pos, key) in right_keys.iter().enumerate() {
            let original = entries.iter().find(|(k, _)| k == key).unwrap().1.clone();
            assert_eq!(
                right.value_at(pos),
                original.as_slice(),
                "right value {key:?}"
            );
        }
        for (pos, key) in left_keys.iter().enumerate() {
            let original = entries.iter().find(|(k, _)| k == key).unwrap().1.clone();
            assert_eq!(
                left.value_at(pos),
                original.as_slice(),
                "left value {key:?}"
            );
        }
    }

    #[test]
    fn encoded_branch_child_pos_and_entries() {
        let pid = |n: u32| DataPid::new(n).unwrap();
        let entries: Vec<(Vec<u8>, DataPid)> = vec![
            (Vec::new(), pid(10)),
            (b"user/a".to_vec(), pid(11)),
            (b"user/m".to_vec(), pid(12)),
            (b"user/z".to_vec(), pid(13)),
        ];
        let node = encoded_branch_from_full_entries(&entries);
        assert!(!node.is_leaf());

        assert_eq!(node.child_pos_for_key(b"abc"), 0);
        assert_eq!(node.child_pos_for_key(b"user/a"), 1);
        assert_eq!(node.child_pos_for_key(b"user/m"), 2);
        assert_eq!(node.child_pos_for_key(b"user/z"), 3);
        assert_eq!(node.child_pos_for_key(b"user/zz"), 3);

        assert_eq!(node.branch_entries(), entries);
    }

    #[test]
    fn encoded_branch_split_pivot_rejects_empty_partitions() {
        let pid = |n: u32| DataPid::new(n).unwrap();
        let entries = vec![
            (Vec::new(), pid(2)),
            (vec![b'x'; PAGE_SIZE], pid(3)),
            (vec![b'y'; PAGE_SIZE], pid(4)),
        ];

        assert!(!encoded_branch_entries_fit(&[]));
        assert_eq!(encoded_branch_split_pivot(&entries), None);
    }

    #[test]
    fn cmp_raw_with_prefixed_tail_is_a_valid_ordering() {
        assert_eq!(
            cmp_raw_with_prefixed_tail(b"ab1", b"ab", b"1"),
            Ordering::Equal
        );
        assert_eq!(
            cmp_raw_with_prefixed_tail(b"ab", b"abc", b""),
            Ordering::Less
        );
        assert_eq!(
            cmp_raw_with_prefixed_tail(b"abd", b"ab", b"c"),
            Ordering::Greater
        );
    }
}
