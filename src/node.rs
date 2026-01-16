use crate::{Error, Result, store::Store};
use std::cmp::Ordering;

pub const PAGE_SIZE: usize = 4096;
pub const MAX_INLINE_LEN: usize = 64;
pub const MAX_KEY_LEN: usize = 32;

const OFFSET_CHECKSUM: usize = PAGE_SIZE - 8;
const OFFSET_NEXT_INDIRECT: usize = PAGE_SIZE - 16;
const IDS_PER_INDIRECT_PAGE: usize = OFFSET_NEXT_INDIRECT / std::mem::size_of::<u64>();

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct NodeHeader {
    pub checksum: u64,
    pub is_leaf: u64,
    pub elems: u64,
    pub offset: u64,
}

const HEADER_SIZE: usize = std::mem::size_of::<NodeHeader>();
const SLOT_SIZE: usize = std::mem::size_of::<Slot>();
const NR_INLINE_PAGE: usize = 3;

pub const fn size_to_pages(size: usize) -> usize {
    size.div_ceil(PAGE_SIZE)
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct Slot {
    pub pos: u32,
    pub klen: u32,
    pub vlen: u32,
    pub page_id: [u64; NR_INLINE_PAGE],
}

impl Slot {
    pub fn is_inline(&self) -> bool {
        self.page_id[0] == 0
    }

    pub fn data_offset(&self) -> usize {
        self.pos as usize
    }

    pub fn key_len(&self) -> usize {
        self.klen as usize
    }

    pub fn value_len(&self) -> usize {
        self.vlen as usize
    }

    pub fn nr_pages(&self) -> u32 {
        if self.vlen == 0 {
            0
        } else {
            size_to_pages(self.vlen as usize) as u32
        }
    }

    fn update_vlen(&mut self, vlen: u32) {
        self.vlen = vlen;
    }
}

#[derive(Clone)]
pub struct Node {
    pub data: Vec<u8>,
    pub dirty: bool,
}

impl Node {
    fn available_space(&self) -> u64 {
        let hdr = self.header();
        let used = HEADER_SIZE as u64 + hdr.elems * SLOT_SIZE as u64;
        hdr.offset.saturating_sub(used)
    }

    pub(crate) fn slot_at(&self, pos: usize) -> &Slot {
        let slot_off = HEADER_SIZE + pos * SLOT_SIZE;
        unsafe { &*self.data.as_ptr().add(slot_off).cast::<Slot>() }
    }

    pub(crate) fn slot_at_mut(&mut self, pos: usize) -> &mut Slot {
        let slot_off = HEADER_SIZE + pos * SLOT_SIZE;
        unsafe { &mut *self.data.as_mut_ptr().add(slot_off).cast::<Slot>() }
    }

    pub fn key_at(&self, pos: usize) -> &[u8] {
        let slot = self.slot_at(pos);
        let off = slot.data_offset();
        let len = slot.key_len();
        &self.data[off..off + len]
    }

    pub fn value_at(&self, pos: usize) -> &[u8] {
        let slot = self.slot_at(pos);
        assert!(slot.is_inline());
        let off = slot.data_offset() + slot.key_len();
        let len = slot.value_len();
        &self.data[off..off + len]
    }

    fn key_at_mut(&mut self, pos: usize) -> &mut [u8] {
        let slot = *self.slot_at(pos);
        let off = slot.data_offset();
        let len = slot.key_len();
        &mut self.data[off..off + len]
    }

    fn value_at_mut(&mut self, pos: usize) -> &mut [u8] {
        let slot = *self.slot_at(pos);
        assert!(slot.is_inline());
        let off = slot.data_offset() + slot.key_len();
        let len = slot.value_len();
        &mut self.data[off..off + len]
    }

    fn emplace_at(&mut self, pos: usize, slot: &Slot, data: &[u8]) {
        let data_off = {
            let hdr = self.header_mut();
            hdr.offset -= data.len() as u64;
            hdr.offset
        };

        let dst_slot = self.slot_at_mut(pos);
        *dst_slot = *slot;
        dst_slot.pos = data_off as u32;

        self.data[data_off as usize..data_off as usize + data.len()].copy_from_slice(data);
        self.header_mut().elems += 1;
    }

    pub(crate) fn collect_page_ids(&self, store: &Store, slot: &Slot) -> Result<Vec<u64>> {
        let nr_pages = slot.nr_pages() as usize;
        if nr_pages <= NR_INLINE_PAGE {
            return Ok(slot.page_id[0..nr_pages].to_vec());
        }

        let mut pages = Vec::with_capacity(nr_pages);
        let mut curr_index_page = slot.page_id[0];

        while pages.len() < nr_pages {
            let data = store.load_page(curr_index_page)?;
            let stored_checksum =
                u64::from_le_bytes(data[OFFSET_CHECKSUM..PAGE_SIZE].try_into().unwrap());
            let computed_checksum = crc32c::crc32c(&data[..OFFSET_CHECKSUM]) as u64;
            if stored_checksum != computed_checksum {
                return Err(Error::Corruption);
            }

            let to_read = std::cmp::min(nr_pages - pages.len(), IDS_PER_INDIRECT_PAGE);
            for i in 0..to_read {
                let start = i * 8;
                pages.push(u64::from_le_bytes(
                    data[start..start + 8].try_into().unwrap(),
                ));
            }
            if pages.len() < nr_pages {
                curr_index_page = u64::from_le_bytes(
                    data[OFFSET_NEXT_INDIRECT..OFFSET_CHECKSUM]
                        .try_into()
                        .unwrap(),
                );
            }
        }
        Ok(pages)
    }

    fn write_index_chain(&self, store: &Store, pages: &[u64]) -> Result<u64> {
        let nr_index_pages = pages.len().div_ceil(IDS_PER_INDIRECT_PAGE);
        let index_page_ids = store.alloc_pages(nr_index_pages as u32)?;

        for i in 0..nr_index_pages {
            let mut index_data = vec![0u8; PAGE_SIZE];
            let start_idx = i * IDS_PER_INDIRECT_PAGE;
            let end_idx = std::cmp::min(start_idx + IDS_PER_INDIRECT_PAGE, pages.len());

            for (j, &pid) in pages[start_idx..end_idx].iter().enumerate() {
                let off = j * 8;
                index_data[off..off + 8].copy_from_slice(&pid.to_le_bytes());
            }

            if i + 1 < nr_index_pages {
                let next_pid = index_page_ids[i + 1];
                index_data[OFFSET_NEXT_INDIRECT..OFFSET_CHECKSUM]
                    .copy_from_slice(&next_pid.to_le_bytes());
            }

            let checksum = crc32c::crc32c(&index_data[..OFFSET_CHECKSUM]) as u64;
            index_data[OFFSET_CHECKSUM..PAGE_SIZE].copy_from_slice(&checksum.to_le_bytes());

            store.write_page(index_page_ids[i], &index_data)?;
        }

        Ok(index_page_ids[0])
    }

    fn update_at(
        &mut self,
        store: &Store,
        pos: usize,
        value: &[u8],
        freed: &mut Vec<(u64, u32)>,
    ) -> Result<()> {
        if !self.is_leaf() {
            if value.len() != 8 {
                return Err(Error::Corruption);
            }
            let page_id = u64::from_le_bytes(value.try_into().unwrap());
            let slot = self.slot_at_mut(pos);
            slot.page_id[0] = page_id;
            return Ok(());
        }

        let (is_inline, old_vlen) = {
            let slot = self.slot_at(pos);
            (slot.is_inline(), slot.value_len())
        };

        if value.len() <= old_vlen && is_inline {
            let dst = self.value_at_mut(pos);
            dst[0..value.len()].copy_from_slice(value);
            let slot = self.slot_at_mut(pos);
            slot.update_vlen(value.len() as u32);
        } else {
            let nr_blocks = size_to_pages(value.len());
            let pages = store.alloc_pages(nr_blocks as u32)?;

            if nr_blocks <= NR_INLINE_PAGE {
                store.write_data(&pages, value)?;
                if !is_inline {
                    let slot_copy = *self.slot_at(pos);
                    self.free_slot_pages(store, &slot_copy, freed)?;
                }
                let slot = self.slot_at_mut(pos);
                for (i, &pid) in pages.iter().enumerate() {
                    slot.page_id[i] = pid;
                }
                for i in nr_blocks..NR_INLINE_PAGE {
                    slot.page_id[i] = 0;
                }
            } else {
                let first_index_pid = self.write_index_chain(store, &pages)?;
                store.write_data(&pages, value)?;

                if !is_inline {
                    let slot_copy = *self.slot_at(pos);
                    self.free_slot_pages(store, &slot_copy, freed)?;
                }
                let slot = self.slot_at_mut(pos);
                slot.page_id[0] = first_index_pid;
                for i in 1..NR_INLINE_PAGE {
                    slot.page_id[i] = 0;
                }
            }

            let slot = self.slot_at_mut(pos);
            slot.update_vlen(value.len() as u32);
        }
        Ok(())
    }

    pub(crate) fn free_slot_pages(
        &self,
        store: &Store,
        slot: &Slot,
        freed: &mut Vec<(u64, u32)>,
    ) -> Result<()> {
        let nr_pages = slot.nr_pages() as usize;
        if nr_pages == 0 {
            return Ok(());
        }

        if nr_pages <= NR_INLINE_PAGE {
            for i in 0..nr_pages {
                freed.push((slot.page_id[i], 1));
            }
        } else {
            let mut curr_index_page = slot.page_id[0];
            let mut collected_data_pages = 0;

            while collected_data_pages < nr_pages {
                let data = store.load_page(curr_index_page)?;
                let stored_checksum =
                    u64::from_le_bytes(data[OFFSET_CHECKSUM..PAGE_SIZE].try_into().unwrap());
                let computed_checksum = crc32c::crc32c(&data[..OFFSET_CHECKSUM]) as u64;
                if stored_checksum != computed_checksum {
                    return Err(Error::Corruption);
                }

                let to_free = std::cmp::min(nr_pages - collected_data_pages, IDS_PER_INDIRECT_PAGE);

                for i in 0..to_free {
                    let pid = u64::from_le_bytes(data[i * 8..i * 8 + 8].try_into().unwrap());
                    freed.push((pid, 1));
                }
                collected_data_pages += to_free;

                let next_index_page = if collected_data_pages < nr_pages {
                    u64::from_le_bytes(
                        data[OFFSET_NEXT_INDIRECT..OFFSET_CHECKSUM]
                            .try_into()
                            .unwrap(),
                    )
                } else {
                    0
                };

                freed.push((curr_index_page, 1));
                curr_index_page = next_index_page;
            }
        }
        Ok(())
    }

    fn expand_slot(&mut self, pos: usize) -> &mut Slot {
        let elems = self.header().elems;
        let slot_off = HEADER_SIZE + pos * SLOT_SIZE;
        let last_slot_off = HEADER_SIZE + elems as usize * SLOT_SIZE;

        if pos < elems as usize {
            self.data
                .copy_within(slot_off..last_slot_off, slot_off + SLOT_SIZE);
        }

        self.header_mut().elems += 1;
        self.slot_at_mut(pos)
    }

    pub fn shrink_slot(&mut self, pos: usize) -> Slot {
        let elems = self.header().elems;
        let slot = *self.slot_at(pos);
        let slot_off = HEADER_SIZE + pos * SLOT_SIZE;
        let next_slot_off = slot_off + SLOT_SIZE;
        let last_slot_off = HEADER_SIZE + elems as usize * SLOT_SIZE;

        if pos + 1 < elems as usize {
            self.data
                .copy_within(next_slot_off..last_slot_off, slot_off);
        }

        self.header_mut().elems -= 1;
        slot
    }

    fn insert_at(&mut self, store: &Store, pos: usize, key: &[u8], value: &[u8]) -> Result<()> {
        let is_leaf = self.is_leaf();
        let mut cur_off = self.header().offset;

        if !is_leaf {
            if value.len() != 8 {
                return Err(Error::Corruption);
            }
            let page_id = u64::from_le_bytes(value.try_into().unwrap());

            cur_off -= key.len() as u64;
            self.header_mut().offset = cur_off;

            let slot = self.expand_slot(pos);
            slot.klen = key.len() as u32;
            slot.vlen = 0;
            slot.page_id[0] = page_id;
            slot.pos = cur_off as u32;

            self.key_at_mut(pos).copy_from_slice(key);
            return Ok(());
        }

        let mut slot_copy = Slot {
            pos: 0,
            klen: key.len() as u32,
            vlen: value.len() as u32,
            page_id: [0; NR_INLINE_PAGE],
        };

        if slot_copy.key_len() + slot_copy.value_len() <= MAX_INLINE_LEN {
            cur_off -= (key.len() + value.len()) as u64;
            slot_copy.pos = cur_off as u32;
        } else {
            cur_off -= key.len() as u64;
            slot_copy.pos = cur_off as u32;
            let nr_blocks = size_to_pages(value.len());
            let pages = store.alloc_pages(nr_blocks as u32)?;

            if nr_blocks <= NR_INLINE_PAGE {
                store.write_data(&pages, value)?;
                for (i, &pid) in pages.iter().enumerate() {
                    slot_copy.page_id[i] = pid;
                }
            } else {
                let first_index_pid = self.write_index_chain(store, &pages)?;
                store.write_data(&pages, value)?;
                slot_copy.page_id[0] = first_index_pid;
            }
        }

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
        let mut new_data = vec![0; PAGE_SIZE];
        let src_hdr = *self.header();
        let elems = src_hdr.elems as usize;
        let mut offset = PAGE_SIZE as u64;

        // Copy header
        new_data[..HEADER_SIZE].copy_from_slice(&self.data[..HEADER_SIZE]);

        for i in 0..elems {
            let src_slot = *self.slot_at(i);
            let k = self.key_at(i);
            let mut kv_len = k.len() as u64;

            let v = if src_hdr.is_leaf == 1 && src_slot.is_inline() {
                let val = self.value_at(i);
                kv_len += val.len() as u64;
                Some(val)
            } else {
                None
            };

            offset -= kv_len;
            let dst_slot_off = HEADER_SIZE + i * SLOT_SIZE;
            let dst_slot = unsafe { &mut *new_data.as_mut_ptr().add(dst_slot_off).cast::<Slot>() };
            *dst_slot = src_slot;
            dst_slot.pos = offset as u32;

            new_data[offset as usize..offset as usize + k.len()].copy_from_slice(k);
            if let Some(val) = v {
                new_data[offset as usize + k.len()..offset as usize + k.len() + val.len()]
                    .copy_from_slice(val);
            }
        }

        let dst_hdr = unsafe { &mut *new_data.as_mut_ptr().cast::<NodeHeader>() };
        dst_hdr.offset = offset;

        self.data = new_data;
        self.update_checksum();
    }

    pub fn from_raw(data: Vec<u8>) -> Result<Self> {
        if data.len() != PAGE_SIZE {
            return Err(Error::Corruption);
        }
        let this = Self { data, dirty: false };
        this.validate()?;
        Ok(this)
    }

    fn validate(&self) -> Result<()> {
        let stored_checksum = self.header().checksum;
        if stored_checksum == 0 {
            return Ok(());
        }

        let computed = self.calc_checksum() as u64;
        if stored_checksum != computed {
            return Err(Error::Corruption);
        }
        Ok(())
    }

    fn new(is_leaf: bool) -> Self {
        let mut this = Node {
            data: vec![0; PAGE_SIZE],
            dirty: true,
        };
        let h = this.header_mut();
        h.offset = PAGE_SIZE as u64;
        h.is_leaf = if is_leaf { 1 } else { 0 };
        this
    }

    pub fn new_leaf() -> Self {
        Self::new(true)
    }

    pub fn new_branch() -> Self {
        Self::new(false)
    }

    pub fn finalize(&mut self) -> &[u8] {
        if self.dirty {
            self.update_checksum();
            self.dirty = false;
        }
        &self.data
    }

    pub fn header(&self) -> &NodeHeader {
        unsafe { &*self.data.as_ptr().cast::<NodeHeader>() }
    }

    pub fn header_mut(&mut self) -> &mut NodeHeader {
        unsafe { &mut *self.data.as_mut_ptr().cast::<NodeHeader>() }
    }

    pub fn is_leaf(&self) -> bool {
        self.header().is_leaf == 1
    }

    pub fn is_empty(&self) -> bool {
        self.header().elems == 0
    }

    pub fn put(
        &mut self,
        store: &Store,
        key: &[u8],
        value: &[u8],
        freed: &mut Vec<(u64, u32)>,
    ) -> Result<()> {
        if key.len() > MAX_KEY_LEN {
            return Err(Error::TooLarge);
        }

        let data_len = if !self.is_leaf() {
            key.len()
        } else if key.len() + value.len() <= MAX_INLINE_LEN {
            key.len() + value.len()
        } else {
            key.len()
        };
        let total_required = data_len as u64 + SLOT_SIZE as u64;

        if self.available_space() < total_required {
            self.compact();
            if self.available_space() < total_required {
                return Err(Error::NoSpace);
            }
        }

        match self.search(key) {
            Ok(pos) => {
                self.update_at(store, pos, value, freed)?;
            }
            Err(pos) => self.insert_at(store, pos, key, value)?,
        }
        self.dirty = true;
        Ok(())
    }

    pub fn get(&self, store: &Store, key: &[u8]) -> Result<Vec<u8>> {
        match self.search(key) {
            Ok(pos) => {
                let slot = self.slot_at(pos);
                if slot.is_inline() {
                    Ok(self.value_at(pos).to_vec())
                } else {
                    let pages = self.collect_page_ids(store, slot)?;
                    store.load_data(&pages, slot.value_len())
                }
            }
            Err(_) => Err(Error::NotFound),
        }
    }

    pub fn search(&self, key: &[u8]) -> std::result::Result<usize, usize> {
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

    pub fn del(&mut self, store: &Store, key: &[u8], freed: &mut Vec<(u64, u32)>) -> Result<()> {
        match self.search(key) {
            Ok(pos) => {
                let slot = self.shrink_slot(pos);
                if !slot.is_inline() {
                    self.free_slot_pages(store, &slot, freed)?;
                }
                self.dirty = true;
                Ok(())
            }
            Err(_) => Err(Error::NotFound),
        }
    }

    pub fn split(&mut self) -> Result<(Vec<u8>, Node)> {
        let mid = (self.header().elems / 2) as usize;
        let sep = self.key_at(mid).to_vec();
        let is_leaf = self.is_leaf();

        let mut node = Node::new(is_leaf);
        let elems = self.header().elems as usize;
        for pos in mid..elems {
            let slot = *self.slot_at(pos);
            let data = self.data_at(pos);
            node.emplace_at(pos - mid, &slot, data);
        }

        self.header_mut().elems = mid as u64;
        self.compact();

        Ok((sep, node))
    }

    pub fn update_checksum(&mut self) {
        self.header_mut().checksum = 0;
        let real = self.calc_checksum();
        self.header_mut().checksum = real as u64;
    }

    fn calc_checksum(&self) -> u32 {
        let mut data_copy = self.data.clone();
        unsafe {
            let hdr = &mut *data_copy.as_mut_ptr().cast::<NodeHeader>();
            hdr.checksum = 0;
        }
        crc32c::crc32c(&data_copy)
    }

    pub fn data_at(&self, pos: usize) -> &[u8] {
        let slot = self.slot_at(pos);
        let len = if self.is_leaf() && slot.is_inline() {
            slot.key_len() + slot.value_len()
        } else {
            slot.key_len()
        };
        let off = slot.data_offset();
        &self.data[off..off + len]
    }

    pub fn child_at(&self, pos: usize) -> u64 {
        debug_assert!(!self.is_leaf());
        self.slot_at(pos).page_id[0]
    }

    pub fn update_child_page(&mut self, pos: usize, page_id: u64) {
        debug_assert!(!self.is_leaf());
        let slot = self.slot_at_mut(pos);
        slot.page_id[0] = page_id;
        self.dirty = true;
    }

    pub fn num_children(&self) -> usize {
        self.header().elems as usize
    }
}
