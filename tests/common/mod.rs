//! Shared helpers for integration tests. Each test binary compiles this
//! module separately, so helpers used by only one binary look dead to the
//! others.
#![allow(dead_code)]

use std::path::Path;
use std::process::Command;

fn parse_runner(value: &str) -> Vec<String> {
    value
        .split_whitespace()
        .map(str::to_owned)
        .collect::<Vec<_>>()
}

fn detect_cargo_runner() -> Option<Vec<String>> {
    let arch = std::env::consts::ARCH
        .to_ascii_uppercase()
        .replace('-', "_");
    let mut preferred = Vec::new();
    let mut all = Vec::new();

    for (key, value) in std::env::vars() {
        if !key.starts_with("CARGO_TARGET_") || !key.ends_with("_RUNNER") {
            continue;
        }
        if value.trim().is_empty() {
            continue;
        }

        all.push((key.clone(), value.clone()));
        if key.contains(&format!("_{arch}_")) {
            preferred.push((key, value));
        }
    }

    preferred.sort_by(|a, b| a.0.cmp(&b.0));
    all.sort_by(|a, b| a.0.cmp(&b.0));

    if let Some((_, value)) = preferred.into_iter().next() {
        return Some(parse_runner(&value)).filter(|parts| !parts.is_empty());
    }
    if all.len() == 1 {
        return Some(parse_runner(&all[0].1)).filter(|parts| !parts.is_empty());
    }
    None
}

pub fn child_test_command(exe: &Path) -> Command {
    match detect_cargo_runner() {
        Some(parts) => {
            let mut it = parts.into_iter();
            let mut cmd = Command::new(it.next().expect("runner must not be empty"));
            cmd.args(it);
            cmd.arg(exe);
            cmd
        }
        None => Command::new(exe),
    }
}

/// A physical page is always this many bytes.
pub const PAGE_SIZE: usize = 4096;
/// Bytes a headerless page (indirect or value) holds before its trailer.
pub const TRAILER_CONTENT_SIZE: usize = PAGE_SIZE - 4;
/// Offset of a headerless page's trailer.
pub const TRAILER_CRC_OFFSET: usize = TRAILER_CONTENT_SIZE;
/// Offset of an indirect page's next-page pointer.
pub const INDIRECT_NEXT_OFFSET: usize = TRAILER_CRC_OFFSET - 4;
/// Allocator page header: checksum, next, count.
pub const EXTENT_HEADER_SIZE: usize = 12;
/// One allocator extent entry.
pub const EXTENT_ENTRY_SIZE: usize = 8;

fn crc_of(segments: &[&[u8]]) -> u32 {
    let mut crc = 0u32;
    for segment in segments {
        crc = crc32c::crc32c_append(crc, segment);
    }
    crc
}

/// The checksum field of a header-carrying page (node, allocator): the header's
/// first four bytes.
pub const HEADER_CRC_OFFSET: usize = 0;

/// The documented page checksum rule, re-implemented independently of the library so a
/// test that reseals a page checks the library against the document: the four-byte
/// field at `field` is read as zero, *every* other byte of the page and the expected
/// page id are hashed, and the result belongs in that field.
pub fn page_crc(page: &[u8], field: usize, pid: u32) -> u32 {
    assert_eq!(page.len(), PAGE_SIZE);
    assert!(field == HEADER_CRC_OFFSET || field == TRAILER_CRC_OFFSET);
    crc_of(&[
        &page[..field],
        &[0u8; 4],
        &pid.to_le_bytes(),
        &page[field + 4..],
    ])
}

/// Seals a node or allocator page: its checksum field is the header's first field.
pub fn seal_page(page: &mut [u8], pid: u32) {
    assert_eq!(page.len(), PAGE_SIZE);
    let crc = page_crc(page, HEADER_CRC_OFFSET, pid);
    page[..4].copy_from_slice(&crc.to_le_bytes());
}

/// Reads the header checksum field of a node or allocator page.
pub fn header_crc_field(page: &[u8]) -> u32 {
    u32::from_le_bytes(page[..4].try_into().unwrap())
}

/// Reads a headerless page's trailer checksum field.
pub fn trailer_crc_field(page: &[u8]) -> u32 {
    u32::from_le_bytes(page[TRAILER_CRC_OFFSET..].try_into().unwrap())
}
