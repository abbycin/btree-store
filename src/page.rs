use crate::node::PAGE_SIZE;

/// The checksum field of a header-carrying page (node, allocator): the first
/// field of its header.
pub(crate) const HEADER_CRC_OFFSET: usize = 0;
/// Bytes a headerless page can carry before its checksum field.
pub(crate) const TRAILER_CONTENT_SIZE: usize = PAGE_SIZE - 4;
/// The checksum field of a headerless page (indirect, value): its trailer.
pub(crate) const TRAILER_CRC_OFFSET: usize = TRAILER_CONTENT_SIZE;
/// The four zero bytes standing in for a page's own checksum field while its
/// checksum is being computed.
pub(crate) const CHECKSUM_PLACEHOLDER: [u8; 4] = [0; 4];

/// A page whose stored checksum does not match the page it covers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CrcMismatch {
    pub expected: u32,
    pub actual: u32,
}

/// CRC32C over the concatenation of `segments`, in order.
pub(crate) fn crc_of(segments: &[&[u8]]) -> u32 {
    let mut crc = 0u32;
    for segment in segments {
        crc = crc32c::crc32c_append(crc, segment);
    }
    crc
}

/// Reads the little-endian checksum stored at `offset`.
pub(crate) fn load_crc(page: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(
        page[offset..offset + 4]
            .try_into()
            .expect("a checksum field is exactly four bytes"),
    )
}

/// Stores `crc` at `offset`.
pub(crate) fn store_crc(page: &mut [u8], offset: usize, crc: u32) {
    page[offset..offset + 4].copy_from_slice(&crc.to_le_bytes());
}

/// Compares a stored checksum against the one just computed.
pub(crate) fn compare(stored: u32, computed: u32) -> Result<(), CrcMismatch> {
    if stored == computed {
        Ok(())
    } else {
        Err(CrcMismatch {
            expected: computed,
            actual: stored,
        })
    }
}

/// Every page class uses one rule: the four-byte checksum field at `field` is read
/// as zero, *every* other byte of the page is hashed, the expected page id `pid` is
/// hashed with them, and the result belongs in that field. Node and allocator pages
/// keep the field in the header ([`HEADER_CRC_OFFSET`]), indirect and value pages in
/// their trailer ([`TRAILER_CRC_OFFSET`]). Nothing is excluded but the field itself,
/// so bytes a writer never wrote are covered as they happen to be.
pub(crate) fn page_crc(page: &[u8], field: usize, pid: u32) -> u32 {
    debug_assert!(field == HEADER_CRC_OFFSET || field == TRAILER_CRC_OFFSET);
    debug_assert_eq!(page.len(), PAGE_SIZE);
    crc_of(&[
        &page[..field],
        &CHECKSUM_PLACEHOLDER,
        &pid.to_le_bytes(),
        &page[field + 4..],
    ])
}

/// Computes `page`'s checksum and stores it in the field at `field`.
pub(crate) fn seal_page(page: &mut [u8], field: usize, pid: u32) -> u32 {
    let crc = page_crc(page, field, pid);
    store_crc(page, field, crc);
    crc
}

/// Verifies the checksum stored in the field at `field` against the page and `pid`.
pub(crate) fn verify_page(page: &[u8], field: usize, pid: u32) -> Result<(), CrcMismatch> {
    compare(load_crc(page, field), page_crc(page, field, pid))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Independent anchor: the standard CRC32C (Castagnoli) check value. Locks
    /// the algorithm, so a custom init/final-xor variant cannot slip in.
    #[test]
    fn crc32c_matches_the_standard_check_value() {
        assert_eq!(crc32c::crc32c(b"123456789"), 0xE306_9283);
    }

    #[test]
    fn segments_are_one_stream() {
        let whole: Vec<u8> = (0..64u8).collect();
        let (head, tail) = whole.split_at(17);
        assert_eq!(crc_of(&[&whole]), crc32c::crc32c(&whole));
        assert_eq!(crc_of(&[head, tail]), crc32c::crc32c(&whole));
    }

    /// One rule, both field positions: the field read as zero, the whole page
    /// hashed, the result stored in the field. Every byte outside the field is
    /// covered - including bytes a writer never wrote - and the expected page id is
    /// part of the checksum.
    #[test]
    fn the_whole_page_is_covered_apart_from_the_field() {
        for field in [HEADER_CRC_OFFSET, TRAILER_CRC_OFFSET] {
            let mut page = vec![0u8; PAGE_SIZE];
            for (i, byte) in page.iter_mut().enumerate() {
                *byte = (i % 251) as u8;
            }
            seal_page(&mut page, field, 7);
            assert!(verify_page(&page, field, 7).is_ok());

            for offset in [0usize, 3, 4, 12, 4_000, PAGE_SIZE - 1] {
                if (field..field + 4).contains(&offset) {
                    continue; // the field is the checksum, not covered content
                }
                let mut damaged = page.clone();
                damaged[offset] ^= 1;
                assert!(
                    verify_page(&damaged, field, 7).is_err(),
                    "field {field}: a flipped byte at {offset} must fail verification"
                );
            }

            let covered = page_crc(&page, field, 7);
            assert_ne!(
                page_crc(&page, field, 8),
                covered,
                "the expected page id is part of the checksum"
            );
            page[field..field + 4].copy_from_slice(&0u32.to_le_bytes());
            assert_eq!(
                page_crc(&page, field, 7),
                covered,
                "the field is read as zero, so its own bytes cannot change the result"
            );
        }
    }
}
