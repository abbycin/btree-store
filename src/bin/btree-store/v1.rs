//! Frozen decoder for the version-1 on-disk format.
//!
//! This module is a *frozen reader*: it keeps its own `V1_*` constants and never
//! imports the current format's capacities: once a version is released its
//! decoding is frozen. It
//! reads a v1 database through a descriptor the caller already opened read-only
//! and locked; it never writes and never repairs. Every violation becomes a
//! `(code, pid, check)` diagnostic.
//!
//! Layout it understands (little-endian, 4096-byte pages):
//!
//! ```text
//! meta slot (40 bytes): magic u64 | seq u64 | version u32 | catalog u32
//!                       | next_page_id u32 | reusable u32 | retired u32 | crc u32
//! plain node:   12-byte header {is_leaf, elems, offset} | slot[elems] | payload
//! encoded node: 16-byte header {kind, elems, offset, prefix_len} | prefix
//!               | slot[elems] (4-byte aligned) | payload
//! slot (32 bytes): pos u32 | klen u32 | vlen u32 | page_id[5] u32
//! extent page:  {next u32, count u32} | extent[count] {page_id u32, nr_pages u32}
//! ```
//!
//! A slot is inline when `page_id[0] == 0`; otherwise its value occupies
//! `ceil(vlen / 4096)` pages, referenced directly (at most five pages) or through
//! an indirect chain whose first page is `page_id[0]`.

use std::collections::HashSet;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

pub(crate) const V1_PAGE_SIZE: usize = 4096;
const V1_META_SIZE: usize = 40;
const V1_MAGIC: u64 = 0x636f_7762_7472_6565; // "cowbtree"
const V1_FORMAT_VERSION: u32 = 1;
const V1_PLAIN_HEADER: usize = 12;
const V1_ENCODED_HEADER: usize = 16;
const V1_SLOT_SIZE: usize = 32;
const V1_SLOT_PIDS: usize = 5;
const V1_ENCODED_BRANCH: u32 = 2;
const V1_ENCODED_LEAF: u32 = 3;
const V1_OFFSET_NEXT_INDIRECT: usize = V1_PAGE_SIZE - 4;
const V1_IDS_PER_INDIRECT: usize = V1_OFFSET_NEXT_INDIRECT / 4;
const V1_EXTENT_HEADER: usize = 8;
const V1_EXTENT_SIZE: usize = 8;
const V1_EXTENT_PER_PAGE: usize = (V1_PAGE_SIZE - V1_EXTENT_HEADER) / V1_EXTENT_SIZE;
const V1_MAX_VALUE_LEN: u64 = 2 << 30;

/// A diagnostic from a frozen-format read. `pid` is `None` only when the failure
/// belongs to the file as a whole (selection, magic, extent lists).
#[derive(Debug)]
pub(crate) struct V1Error {
    pub(crate) code: &'static str,
    pub(crate) pid: Option<u32>,
    pub(crate) check: &'static str,
    pub(crate) detail: String,
}

impl V1Error {
    fn corrupt(
        code: &'static str,
        pid: u32,
        check: &'static str,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            code,
            pid: Some(pid),
            check,
            detail: detail.into(),
        }
    }

    fn file(code: &'static str, check: &'static str, detail: impl Into<String>) -> Self {
        Self {
            code,
            pid: None,
            check,
            detail: detail.into(),
        }
    }

    fn io(action: &str, error: std::io::Error) -> Self {
        Self::file(
            "V1_IO",
            "reading the source database",
            format!("{action}: {error}"),
        )
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct V1Meta {
    pub(crate) seq: u64,
    pub(crate) catalog_root: u32,
    pub(crate) next_page_id: u32,
    pub(crate) reusable_root: u32,
    pub(crate) retired_root: u32,
}

impl V1Meta {
    fn decode(page: &[u8]) -> Option<Self> {
        if page.len() < V1_META_SIZE {
            return None;
        }
        if u64::from_le_bytes(page[0..8].try_into().ok()?) != V1_MAGIC {
            return None;
        }
        let stored = u32::from_le_bytes(page[36..40].try_into().ok()?);
        if stored != crc32c::crc32c(&page[..36]) {
            return None;
        }
        if u32::from_le_bytes(page[16..20].try_into().ok()?) != V1_FORMAT_VERSION {
            return None;
        }
        Some(Self {
            seq: u64::from_le_bytes(page[8..16].try_into().ok()?),
            catalog_root: u32::from_le_bytes(page[20..24].try_into().ok()?),
            next_page_id: u32::from_le_bytes(page[24..28].try_into().ok()?),
            reusable_root: u32::from_le_bytes(page[28..32].try_into().ok()?),
            retired_root: u32::from_le_bytes(page[32..36].try_into().ok()?),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct V1Bucket {
    pub(crate) name: String,
    /// 0 means an empty bucket.
    pub(crate) root: u32,
    pub(crate) flags: u32,
}

impl V1Bucket {
    pub(crate) fn prefix_encoded(&self) -> bool {
        self.flags & 1 == 1
    }
}

#[derive(Clone, Debug)]
enum V1ValueRef {
    Inline(Vec<u8>),
    Overflow {
        direct: [u32; V1_SLOT_PIDS],
        vlen: u32,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct V1Entry {
    pub(crate) key: Vec<u8>,
    value: V1ValueRef,
}

#[derive(Default, Debug)]
pub(crate) struct V1Walk {
    pub(crate) nodes: HashSet<u32>,
    pub(crate) values: HashSet<u32>,
    pub(crate) indirect: HashSet<u32>,
}

struct DecodedEntry {
    key: Vec<u8>,
    value: V1ValueRef,
    child: Option<u32>,
}

type EntryVisitor<'a> = dyn FnMut(&mut V1Reader, &V1Entry) -> Result<(), V1Error> + 'a;

type ExtentChain = (Vec<(u32, u32)>, Vec<u32>);

struct DecodedNode {
    leaf: bool,
    entries: Vec<DecodedEntry>,
}

pub(crate) struct V1Reader {
    file: File,
    page: Vec<u8>,
    pub(crate) meta: V1Meta,
    /// Complete pages the source file actually holds. This is the second bound
    /// every referenced pid is held to, after `next_page_id`, and the only one that
    /// does not come from the bytes under test (which is what stops one 8-byte
    /// extent entry from claiming the whole id space).
    ///
    /// An intact source keeps `next_page_id` inside the file it wrote: a meta record
    /// is published only after the pages its generation allocated are on disk, and a
    /// rolled-back allocation restores `next_page_id` before anything is published.
    /// A constructed or truncated source may still claim an id space the file does
    /// not hold, and this bound is what refuses it.
    file_pages: u64,
    /// A slot that failed to decode was skipped while selecting the generation.
    /// The CLI reports whether an invalid candidate was ignored - a torn slot
    /// carries no readable `seq`, so "invalid" is all the decoder knows; it cannot
    /// say whether the skipped candidate was newer.
    pub(crate) ignored_invalid_candidate: bool,
}

impl V1Reader {
    /// Reads and selects the generation to migrate.
    ///
    /// A slot is usable when it decodes (magic, checksum, version 1). The highest
    /// `seq` wins; a tie keeps slot A. A torn candidate is skipped, not repaired.
    pub(crate) fn open(file: File) -> Result<Self, V1Error> {
        let file_pages = file
            .metadata()
            .map_err(|error| V1Error::io("statting the source", error))?
            .len()
            / V1_PAGE_SIZE as u64;
        let mut reader = Self {
            file,
            page: vec![0u8; V1_PAGE_SIZE],
            file_pages,
            meta: V1Meta {
                seq: 0,
                catalog_root: 0,
                next_page_id: 0,
                reusable_root: 0,
                retired_root: 0,
            },
            ignored_invalid_candidate: false,
        };

        let slot_a = reader.slot_meta(0)?;
        let slot_b = reader.slot_meta(V1_PAGE_SIZE as u64)?;
        let meta = match (slot_a, slot_b) {
            (Some(a), Some(b)) => {
                if a.seq >= b.seq {
                    a
                } else {
                    b
                }
            }
            (Some(a), None) => a,
            (None, Some(b)) => b,
            (None, None) => {
                return Err(V1Error::file(
                    "V1_NO_VALID_META",
                    "neither v1 meta slot is valid",
                    "the source is not a readable version-1 database",
                ));
            }
        };
        if meta.next_page_id < 2 {
            return Err(V1Error::file(
                "V1_INVALID_NEXT_PAGE_ID",
                "next_page_id must reserve the two meta slots",
                format!("next_page_id = {}", meta.next_page_id),
            ));
        }
        reader.meta = meta;
        reader.ignored_invalid_candidate = slot_a.is_none() || slot_b.is_none();
        Ok(reader)
    }

    fn slot_meta(&mut self, offset: u64) -> Result<Option<V1Meta>, V1Error> {
        match self.read_exact(offset, V1_META_SIZE) {
            Ok(()) => Ok(V1Meta::decode(&self.page[..V1_META_SIZE])),
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
            Err(error) => Err(V1Error::io("reading a meta slot", error)),
        }
    }

    fn read_exact(&mut self, offset: u64, len: usize) -> std::io::Result<()> {
        if self.page.len() < len {
            self.page.resize(len, 0);
        }
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.read_exact(&mut self.page[..len])
    }

    /// Holds one reference to both bounds a page id must satisfy: inside the id
    /// space the source *declares*, and inside the file it actually *holds*.
    /// `next_page_id` is the source's own claim, so it cannot bound a reference by
    /// itself — a page inside it but past the last page the file contains is still
    /// unusable, and only `file_pages` says so (`check_ownership` then requires every
    /// page of the declared id space to have an owner, so such a source is refused
    /// even when nothing references the missing pages). Checking both here is what
    /// lets phase 1 refuse a bad graph before anything is created: graph checking
    /// reads no data page (only the catalog visitor materializes bucket metadata),
    /// so a later read would be the first to notice.
    fn check_pid(&self, pid: u32, code: &'static str, check: &'static str) -> Result<(), V1Error> {
        if pid < 2 || pid >= self.meta.next_page_id {
            return Err(V1Error::corrupt(
                code,
                pid,
                check,
                format!("page {pid} is outside [2, {})", self.meta.next_page_id),
            ));
        }
        if u64::from(pid) >= self.file_pages {
            return Err(V1Error::corrupt(
                "V1_PAGE_BEYOND_EOF",
                pid,
                "every reachable page must exist inside the source file",
                format!(
                    "page {pid} lies past the end of the file, which holds {} pages",
                    self.file_pages
                ),
            ));
        }
        Ok(())
    }

    fn read_page(&mut self, pid: u32) -> Result<(), V1Error> {
        self.check_pid(
            pid,
            "V1_PAGE_OUT_OF_RANGE",
            "a referenced page id must lie in [2, next_page_id)",
        )?;
        let offset = u64::from(pid) * V1_PAGE_SIZE as u64;
        // `check_pid` has already bounded the pid by the file's statted length, so
        // a short read here means the file shrank under us: still past the end,
        // but only the read itself can discover it.
        self.read_exact(offset, V1_PAGE_SIZE).map_err(|error| {
            if error.kind() == std::io::ErrorKind::UnexpectedEof {
                V1Error::corrupt(
                    "V1_PAGE_BEYOND_EOF",
                    pid,
                    "every reachable page must exist inside the source file",
                    format!("page {pid} lies past the end of the file"),
                )
            } else {
                V1Error::io("reading a page", error)
            }
        })
    }

    fn decode_node(&mut self, pid: u32) -> Result<DecodedNode, V1Error> {
        self.read_page(pid)?;
        let first = u32::from_le_bytes(self.page[0..4].try_into().expect("4 bytes"));
        let (encoded, leaf, elems, offset, prefix_len, slot_base) = match first {
            0 | 1 => (
                false,
                first == 1,
                u32::from_le_bytes(self.page[4..8].try_into().expect("4 bytes")) as usize,
                u32::from_le_bytes(self.page[8..12].try_into().expect("4 bytes")) as usize,
                0usize,
                V1_PLAIN_HEADER,
            ),
            V1_ENCODED_BRANCH | V1_ENCODED_LEAF => {
                let elems =
                    u32::from_le_bytes(self.page[4..8].try_into().expect("4 bytes")) as usize;
                let offset =
                    u32::from_le_bytes(self.page[8..12].try_into().expect("4 bytes")) as usize;
                let prefix_len =
                    u32::from_le_bytes(self.page[12..16].try_into().expect("4 bytes")) as usize;
                (
                    true,
                    first == V1_ENCODED_LEAF,
                    elems,
                    offset,
                    prefix_len,
                    (V1_ENCODED_HEADER + prefix_len + 3) & !3,
                )
            }
            other => {
                return Err(V1Error::corrupt(
                    "V1_UNKNOWN_NODE_KIND",
                    pid,
                    "the node discriminant must be 0/1 (plain) or 2/3 (encoded)",
                    format!("first word {other:#x}"),
                ));
            }
        };

        if encoded && V1_ENCODED_HEADER + prefix_len > V1_PAGE_SIZE {
            return Err(V1Error::corrupt(
                "V1_PREFIX_OVERFLOW",
                pid,
                "the shared prefix must fit in the page",
                format!("prefix_len {prefix_len}"),
            ));
        }
        let max_elems = (V1_PAGE_SIZE - slot_base) / V1_SLOT_SIZE;
        if elems > max_elems {
            return Err(V1Error::corrupt(
                "V1_NODE_ELEMS_OVERFLOW",
                pid,
                "the slot array must fit in the page",
                format!("{elems} slots with slot_base {slot_base}"),
            ));
        }
        let min_offset = slot_base + elems * V1_SLOT_SIZE;
        if offset < min_offset || offset > V1_PAGE_SIZE {
            return Err(V1Error::corrupt(
                "V1_NODE_OFFSET_OUT_OF_RANGE",
                pid,
                "the payload offset must lie between the slot array and the page end",
                format!("offset {offset}, slot array ends at {min_offset}"),
            ));
        }
        let prefix = if encoded {
            self.page[V1_ENCODED_HEADER..V1_ENCODED_HEADER + prefix_len].to_vec()
        } else {
            Vec::new()
        };

        let mut entries = Vec::with_capacity(elems);
        for index in 0..elems {
            let at = slot_base + index * V1_SLOT_SIZE;
            let slot = &self.page[at..at + V1_SLOT_SIZE];
            let pos = u32::from_le_bytes(slot[0..4].try_into().expect("4 bytes")) as usize;
            let klen = u32::from_le_bytes(slot[4..8].try_into().expect("4 bytes")) as usize;
            let vlen = u32::from_le_bytes(slot[8..12].try_into().expect("4 bytes")) as usize;
            let mut direct = [0u32; V1_SLOT_PIDS];
            for (i, pid) in direct.iter_mut().enumerate() {
                let at = 12 + i * 4;
                *pid = u32::from_le_bytes(slot[at..at + 4].try_into().expect("4 bytes"));
            }

            // A branch's sentinel slot names no bytes at all, so the payload-area
            // test only applies to slots that actually name bytes. What the
            // sentinel's `pos` holds is not derivable from the node class: the
            // frozen fixture writes `0` for its encoded branches and both `0` and
            // `V1_PAGE_SIZE` for its plain ones, so the zero-length test is the
            // only safe way to recognise it. `pos` itself is still bounded for
            // *every* slot: a torn sector can zero the length fields while leaving
            // a garbage offset behind.
            if pos > V1_PAGE_SIZE
                || ((klen > 0 || vlen > 0) && (pos < min_offset || pos + klen > V1_PAGE_SIZE))
            {
                return Err(V1Error::corrupt(
                    "V1_SLOT_KEY_OUT_OF_RANGE",
                    pid,
                    "a slot's key must lie inside the payload area",
                    format!("slot {index}: pos {pos}, klen {klen}"),
                ));
            }
            let inline = direct[0] == 0;
            if inline && klen + vlen > 0 && pos < min_offset {
                return Err(V1Error::corrupt(
                    "V1_SLOT_VALUE_OUT_OF_RANGE",
                    pid,
                    "an inline value must lie inside the payload area",
                    format!("slot {index}: pos {pos}, klen {klen}, vlen {vlen}"),
                ));
            }
            if inline && pos + klen + vlen > V1_PAGE_SIZE {
                return Err(V1Error::corrupt(
                    "V1_SLOT_VALUE_OUT_OF_RANGE",
                    pid,
                    "an inline value must lie inside the payload area",
                    format!("slot {index}: pos {pos}, klen {klen}, vlen {vlen}"),
                ));
            }
            if u64::from(vlen as u32) > V1_MAX_VALUE_LEN {
                return Err(V1Error::corrupt(
                    "V1_VALUE_TOO_LARGE",
                    pid,
                    "a v1 value may not exceed 2 GiB",
                    format!("slot {index}: vlen {vlen}"),
                ));
            }

            let mut key = prefix.clone();
            key.extend_from_slice(&self.page[pos..pos + klen]);
            if !leaf && index == 0 {
                // A branch's first slot is the sentinel: the writer stores no key
                // for it, so it must decode to the empty key, never to the shared
                // prefix.
                key.clear();
            }

            let value = if inline {
                V1ValueRef::Inline(self.page[pos + klen..pos + klen + vlen].to_vec())
            } else {
                V1ValueRef::Overflow {
                    direct,
                    vlen: vlen as u32,
                }
            };
            if leaf && key.is_empty() {
                return Err(V1Error::corrupt(
                    "V1_LEAF_KEY_EMPTY",
                    pid,
                    "a leaf key must be non-empty (the empty key is a branch sentinel)",
                    format!("slot {index}"),
                ));
            }
            let child = (!leaf).then_some(direct[0]);
            entries.push(DecodedEntry { key, value, child });
        }

        Ok(DecodedNode { leaf, entries })
    }

    /// Walks a tree in key order, validating structure and recording pages.
    ///
    /// `visit` receives each leaf entry in ascending key order; the caller decides
    /// whether to materialize the value (the rebuild does, the ownership pass does
    /// not).
    pub(crate) fn walk(
        &mut self,
        root: u32,
        walk: &mut V1Walk,
        mut visit: Option<&mut EntryVisitor<'_>>,
    ) -> Result<(), V1Error> {
        if root == 0 {
            return Ok(());
        }
        self.check_pid(
            root,
            "V1_ROOT_OUT_OF_RANGE",
            "a tree root must lie in [2, next_page_id)",
        )?;

        let mut stack = vec![(root, 0usize)];
        let mut seen = HashSet::new();
        let mut leaf_depth: Option<usize> = None;
        let mut previous_key: Option<Vec<u8>> = None;

        while let Some((pid, depth)) = stack.pop() {
            if !seen.insert(pid) {
                return Err(V1Error::corrupt(
                    "V1_NODE_REVISITED",
                    pid,
                    "a page may be reached only once per tree",
                    format!("page {pid} appears twice"),
                ));
            }
            walk.nodes.insert(pid);
            let node = self.decode_node(pid)?;

            let mut last: Option<&[u8]> = None;
            for entry in &node.entries {
                if entry.key.is_empty() && !node.leaf {
                    continue;
                }
                if let Some(previous) = last
                    && previous >= entry.key.as_slice()
                {
                    return Err(V1Error::corrupt(
                        "V1_KEYS_NOT_STRICTLY_INCREASING",
                        pid,
                        "keys and separators must strictly increase inside a node",
                        format!("key {:?} does not follow {:?}", entry.key, previous),
                    ));
                }
                last = Some(&entry.key);
            }

            if node.leaf {
                match leaf_depth {
                    None => leaf_depth = Some(depth),
                    Some(expected) if expected == depth => {}
                    Some(expected) => {
                        return Err(V1Error::corrupt(
                            "V1_LEAF_DEPTH_MISMATCH",
                            pid,
                            "every leaf of a tree must sit at the same depth",
                            format!("leaf at depth {depth}, earlier leaves at {expected}"),
                        ));
                    }
                }
                for entry in &node.entries {
                    if let Some(previous) = &previous_key
                        && previous.as_slice() >= entry.key.as_slice()
                    {
                        return Err(V1Error::corrupt(
                            "V1_KEYS_NOT_STRICTLY_INCREASING",
                            pid,
                            "leaf keys must strictly increase across the tree",
                            format!("key {:?} does not follow {:?}", entry.key, previous),
                        ));
                    }
                    previous_key = Some(entry.key.clone());
                    if matches!(entry.value, V1ValueRef::Overflow { .. }) {
                        let entry = V1Entry {
                            key: entry.key.clone(),
                            value: entry.value.clone(),
                        };
                        let pages = self.resolve_value_pages(&entry, Some(walk))?;
                        for page in pages {
                            walk.values.insert(page);
                        }
                        if let Some(visit) = visit.as_mut() {
                            visit(self, &entry)?;
                        }
                    } else if let Some(visit) = visit.as_mut() {
                        let entry = V1Entry {
                            key: entry.key.clone(),
                            value: entry.value.clone(),
                        };
                        visit(self, &entry)?;
                    }
                }
            } else {
                // Children are visited left to right: push them in reverse, and check
                // that each child id is usable before descending.
                let mut children = Vec::with_capacity(node.entries.len());
                for entry in &node.entries {
                    let child = entry.child.expect("branch entries carry a child");
                    self.check_pid(
                        child,
                        "V1_CHILD_OUT_OF_RANGE",
                        "a branch child must lie in [2, next_page_id)",
                    )?;
                    children.push(child);
                }
                for child in children.into_iter().rev() {
                    stack.push((child, depth + 1));
                }
            }
        }

        Ok(())
    }

    /// Resolves the physical data pages of an overflow value, validating the
    /// direct/indirect rule, the chain's count, range and termination.
    fn resolve_value_pages(
        &mut self,
        entry: &V1Entry,
        mut chain: Option<&mut V1Walk>,
    ) -> Result<Vec<u32>, V1Error> {
        let V1ValueRef::Overflow { direct, vlen } = &entry.value else {
            return Ok(Vec::new());
        };
        let needed = u64::from(*vlen).div_ceil(V1_PAGE_SIZE as u64) as usize;
        if needed == 0 {
            return Ok(Vec::new());
        }
        // A value of `vlen` bytes occupies `needed` *distinct* data pages, and every
        // page it names must exist inside the file, so a source claiming more data
        // pages than the file holds cannot be honoured by any v1 file. The count
        // needs a check of its own: a corrupt chain can name the same existing page
        // `needed` times, which leaves every reference in range while `read_value`
        // sizes a buffer from `vlen` - bounded by the 2 GiB format maximum, not by
        // the source. Refusing it here is what keeps that buffer from ever being
        // allocated: no staging file exists yet and nothing has been sized.
        if needed as u64 > self.file_pages {
            return Err(V1Error::corrupt(
                "V1_VALUE_PAGES_OUT_OF_RANGE",
                direct[0],
                "a value's data pages must all fit inside the source file",
                format!(
                    "the value whose pages start at page {} needs {needed} data pages; \
                     the file holds {} pages in total",
                    direct[0], self.file_pages
                ),
            ));
        }
        if needed <= V1_SLOT_PIDS {
            let pages = direct[..needed].to_vec();
            for page in &pages {
                self.check_pid(
                    *page,
                    "V1_VALUE_PID_OUT_OF_RANGE",
                    "a direct value page must lie in [2, next_page_id)",
                )?;
            }
            return Ok(pages);
        }

        let mut pages = Vec::with_capacity(needed);
        let mut current = direct[0];
        let mut seen = HashSet::new();
        while pages.len() < needed {
            if !seen.insert(current) {
                return Err(V1Error::corrupt(
                    "V1_INDIRECT_CYCLE",
                    current,
                    "an indirect chain must be acyclic",
                    format!("page {current} is linked twice"),
                ));
            }
            // No separate depth cap: the loop consumes at least one data page per
            // index page and stops as soon as `needed` pages are collected, a
            // repeated index page is caught by `seen`, and a chain that stops
            // early is caught below. A cap here could only reject legal values
            // (a 2 GiB value legitimately needs 513 index pages).
            self.read_page(current)?;
            if let Some(walk) = chain.as_deref_mut() {
                walk.indirect.insert(current);
            }
            let here = (needed - pages.len()).min(V1_IDS_PER_INDIRECT);
            for index in 0..here {
                let at = index * 4;
                let pid = u32::from_le_bytes(self.page[at..at + 4].try_into().expect("4 bytes"));
                self.check_pid(
                    pid,
                    "V1_INDIRECT_PID_OUT_OF_RANGE",
                    "an indirect entry must reference a data page",
                )?;
                pages.push(pid);
            }
            if pages.len() < needed {
                let next = u32::from_le_bytes(
                    self.page[V1_OFFSET_NEXT_INDIRECT..V1_PAGE_SIZE]
                        .try_into()
                        .expect("4 bytes"),
                );
                if next == 0 {
                    return Err(V1Error::corrupt(
                        "V1_INDIRECT_TRUNCATED",
                        current,
                        "a partial indirect chain must name its successor",
                        format!("needed {needed} data pages, found {}", pages.len()),
                    ));
                }
                current = next;
            }
        }
        Ok(pages)
    }

    /// Reads an entry's value bytes.
    pub(crate) fn read_value(&mut self, entry: &V1Entry) -> Result<Vec<u8>, V1Error> {
        match &entry.value {
            V1ValueRef::Inline(bytes) => Ok(bytes.clone()),
            V1ValueRef::Overflow { vlen, .. } => {
                let pages = self.resolve_value_pages(entry, None)?;
                let mut out = vec![0u8; *vlen as usize];
                let mut copied = 0usize;
                for pid in pages {
                    self.read_page(pid)?;
                    let take = (out.len() - copied).min(V1_PAGE_SIZE);
                    out[copied..copied + take].copy_from_slice(&self.page[..take]);
                    copied += take;
                }
                Ok(out)
            }
        }
    }

    /// The catalog, in key order. Non-UTF-8 bucket names are refused: the target
    /// API takes `&str`, so accepting them would silently drop a bucket.
    pub(crate) fn catalog(&mut self, walk: &mut V1Walk) -> Result<Vec<V1Bucket>, V1Error> {
        let root = self.meta.catalog_root;
        let mut buckets = Vec::new();
        let collect = |reader: &mut Self, entry: &V1Entry| {
            let name = std::str::from_utf8(&entry.key).map_err(|_| {
                V1Error::corrupt(
                    "V1_BUCKET_NAME_NOT_UTF8",
                    root,
                    "bucket names must be valid UTF-8",
                    format!("name bytes {:?}", entry.key),
                )
            })?;
            let value = reader.read_value(entry)?;
            if value.len() != 8 {
                return Err(V1Error::corrupt(
                    "V1_BUCKET_METADATA_LEN",
                    root,
                    "bucket metadata is a 4-byte root plus a 4-byte flag word",
                    format!("bucket {name:?} metadata is {} bytes", value.len()),
                ));
            }
            let bucket_root = u32::from_le_bytes(value[0..4].try_into().expect("4 bytes"));
            let flags = u32::from_le_bytes(value[4..8].try_into().expect("4 bytes"));
            if flags & !1 != 0 {
                return Err(V1Error::corrupt(
                    "V1_BUCKET_FLAGS_UNKNOWN",
                    root,
                    "only the prefix-encoding flag is defined",
                    format!("bucket {name:?} flags {flags:#x}"),
                ));
            }
            if bucket_root != 0 {
                reader.check_pid(
                    bucket_root,
                    "V1_BUCKET_ROOT_OUT_OF_RANGE",
                    "a bucket root must lie in [2, next_page_id)",
                )?;
            }
            buckets.push(V1Bucket {
                name: name.to_string(),
                root: bucket_root,
                flags,
            });
            Ok(())
        };
        let mut collect = collect;
        self.walk(root, walk, Some(&mut collect))?;
        Ok(buckets)
    }

    /// Reads and validates one allocator extent chain.
    pub(crate) fn extent_chain(&mut self, root: u32) -> Result<ExtentChain, V1Error> {
        if root == 0 {
            return Ok((Vec::new(), Vec::new()));
        }
        let mut extents = Vec::new();
        let mut pages = Vec::new();
        let mut seen = HashSet::new();
        let mut current = root;
        let mut previous_end = 0u64;
        while current != 0 {
            self.check_pid(
                current,
                "V1_EXTENT_LIST_PAGE_OUT_OF_RANGE",
                "an allocator list page must lie in [2, next_page_id)",
            )?;
            if !seen.insert(current) {
                return Err(V1Error::corrupt(
                    "V1_EXTENT_CYCLE",
                    current,
                    "an extent chain must be acyclic",
                    format!("page {current} repeats"),
                ));
            }
            pages.push(current);
            self.read_page(current)?;
            let next = u32::from_le_bytes(self.page[0..4].try_into().expect("4 bytes"));
            let count = u32::from_le_bytes(self.page[4..8].try_into().expect("4 bytes")) as usize;
            if count > V1_EXTENT_PER_PAGE {
                return Err(V1Error::corrupt(
                    "V1_EXTENT_COUNT_OVERFLOW",
                    current,
                    "an extent page cannot hold more entries than its capacity",
                    format!("count {count}"),
                ));
            }
            for index in 0..count {
                let at = V1_EXTENT_HEADER + index * V1_EXTENT_SIZE;
                let page_id =
                    u32::from_le_bytes(self.page[at..at + 4].try_into().expect("4 bytes"));
                let nr_pages =
                    u32::from_le_bytes(self.page[at + 4..at + 8].try_into().expect("4 bytes"));
                if page_id == 0 || nr_pages == 0 {
                    return Err(V1Error::corrupt(
                        "V1_EXTENT_ENTRY_ZERO",
                        current,
                        "extent entries must be non-zero",
                        format!("entry {index} is ({page_id}, {nr_pages})"),
                    ));
                }
                let start = u64::from(page_id);
                let end = start + u64::from(nr_pages);
                if page_id < 2 || end > u64::from(self.meta.next_page_id) || end > self.file_pages {
                    return Err(V1Error::corrupt(
                        "V1_EXTENT_OUT_OF_RANGE",
                        current,
                        "an extent must lie inside [2, next_page_id) and inside the source file",
                        format!(
                            "entry {index} covers [{start}, {end}); the file holds {} pages",
                            self.file_pages
                        ),
                    ));
                }
                if start < previous_end {
                    return Err(V1Error::corrupt(
                        "V1_EXTENT_UNSORTED",
                        current,
                        "extents inside one chain must be sorted and disjoint",
                        format!("entry {index} starts at {start}, previous end {previous_end}"),
                    ));
                }
                previous_end = end;
                extents.push((page_id, nr_pages));
            }
            current = next;
        }
        Ok((extents, pages))
    }

    /// Checks the five ownership classes over `[2, next_page_id)`.
    ///
    /// Every page must belong to exactly one of: reachable (nodes, values,
    /// indirect pages), reusable extent, retired extent, a reusable-list page, a
    /// retired-list page.
    pub(crate) fn check_ownership(&mut self, walk: &V1Walk) -> Result<(), V1Error> {
        let reusable_root = self.meta.reusable_root;
        let retired_root = self.meta.retired_root;
        let (reusable, reusable_pages) = self.extent_chain(reusable_root)?;
        let (retired, retired_pages) = self.extent_chain(retired_root)?;

        let mut class: HashSet<u32> = HashSet::new();
        let mut mark = |pid: u32, what: &'static str| -> Result<(), V1Error> {
            if !class.insert(pid) {
                return Err(V1Error::file(
                    "V1_OWNERSHIP_OVERLAP",
                    "a page must belong to exactly one ownership class",
                    format!("page {pid} is claimed twice ({what})"),
                ));
            }
            Ok(())
        };

        for pid in walk.nodes.iter().chain(&walk.values).chain(&walk.indirect) {
            mark(*pid, "reachable")?;
        }
        for (start, count) in reusable.iter().chain(retired.iter()) {
            for pid in *start..(*start + *count) {
                mark(pid, "allocator extent")?;
            }
        }
        for pid in reusable_pages.iter().chain(&retired_pages) {
            mark(*pid, "allocator list page")?;
        }

        for pid in 2..self.meta.next_page_id {
            if !class.contains(&pid) {
                return Err(V1Error::file(
                    "V1_OWNERSHIP_UNACCOUNTED",
                    "every page in [2, next_page_id) must have an owner",
                    format!("page {pid} has no ownership class"),
                ));
            }
        }
        Ok(())
    }
}
