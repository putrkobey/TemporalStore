// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! CRC32C (Castagnoli) for on-disk record integrity.
//!
//! The append-only logs originally carried a truncated SHA-256 per record, and the block
//! store a full 32-byte SHA-256 per page record. Both sit on the synchronous write path, so
//! their cost is paid per write, before the durability barrier -- and a cryptographic digest
//! is the wrong tool for the job. Nothing here is defending against a forged record; the
//! threat is accidental corruption of a committed record (a flipped bit that still parses),
//! which is exactly what a CRC is for. CRC32C is the same choice this design
//! makes: a per-record CRC32C in the record header plus a running per-block CRC32C in the
//! block footer.
//!
//! Implemented table-driven and dependency-free, matching how `crc64_jones` is already
//! hand-rolled for routing. The table is built at compile time, so there is no lazy-init
//! check on the hot path.
//!
//! [`crc32c_update`] takes a seed so a checksum can be accumulated across several buffers
//! without concatenating them first.

/// Castagnoli polynomial, bit-reversed (the normal form for a reflected CRC).
const CRC32C_POLYNOMIAL: u32 = 0x82f6_3b78;

/// Byte-at-a-time lookup table, generated at compile time.
const CRC32C_TABLE: [u32; 256] = build_crc32c_table();

const fn build_crc32c_table() -> [u32; 256] {
    let mut table = [0_u32; 256];
    let mut index = 0_usize;
    while index < 256 {
        let mut entry = index as u32;
        let mut bit = 0;
        while bit < 8 {
            entry = if entry & 1 == 1 {
                (entry >> 1) ^ CRC32C_POLYNOMIAL
            } else {
                entry >> 1
            };
            bit += 1;
        }
        table[index] = entry;
        index += 1;
    }
    table
}

/// CRC32C of `bytes`.
pub fn crc32c(bytes: &[u8]) -> u32 {
    crc32c_update(0, bytes)
}

/// Continue a CRC32C over `bytes`, starting from `seed` (0 for a fresh checksum).
///
/// Seeding lets a caller checksum a header and a payload separately, or accumulate across a
/// block, without building a combined buffer first.
pub fn crc32c_update(seed: u32, bytes: &[u8]) -> u32 {
    // CRC32C is the one checksum with an instruction behind it, and it is the checksum every
    // record pays on the way to disk and again on the way back. The table below is the portable
    // answer; where the processor has the instruction it takes eight bytes at a time instead of
    // one, which is worth a branch to find out.
    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("sse4.2") {
            // SAFETY: reached only when the runtime check above says the instruction exists.
            return unsafe { crc32c_update_sse42(seed, bytes) };
        }
    }
    crc32c_update_table(seed, bytes)
}

/// The portable CRC32C, a byte at a time through the table.
///
/// Its own function rather than inlined into the dispatch, so the tests can hold it against the
/// accelerated one on a machine that would otherwise only ever run the fast path.
fn crc32c_update_table(seed: u32, bytes: &[u8]) -> u32 {
    // The reflected algorithm pre- and post-inverts; carrying the inverted form across calls
    // is what makes seeding compose exactly like one pass over the concatenation.
    let mut crc = !seed;
    for byte in bytes {
        let index = ((crc ^ u32::from(*byte)) & 0xff) as usize;
        crc = (crc >> 8) ^ CRC32C_TABLE[index];
    }
    !crc
}

