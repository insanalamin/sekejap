//! Compressed postings list for BM25.
//!
//! Uses delta encoding + variable-length integers (varints) for compression.
//!
//! Format per posting: [doc_id_delta (varint)] [term_freq (varint)]
//!
//! Doc IDs are stored as deltas from the previous doc_id, not absolute values.
//! This dramatically reduces storage for postings with clustered documents.
//!
//! Example:
//! - Absolute doc IDs: [1000, 1005, 1010, 2000]
//! - Deltas: [1000, 5, 5, 990]
//! - Varint encoding: [0xE8, 0x07, 0x05, 0x7E, 0x0F] vs 16 bytes for absolute

use std::io::{Read, Write};

const VARINT_CONTINUATION_BIT: u8 = 0x80;
const VARINT_MASK: u8 = 0x7F;

/// Encode a u64 as varint bytes.
pub fn encode_varint(mut value: u64) -> Vec<u8> {
    let mut bytes = Vec::new();
    loop {
        let byte = (value & 0x7F) as u8;
        value >>= 7;
        if value == 0 {
            bytes.push(byte);
            break;
        }
        bytes.push(byte | VARINT_CONTINUATION_BIT);
    }
    bytes
}

/// Decode a varint from a byte slice. Returns (value, bytes_consumed).
pub fn decode_varint(bytes: &[u8]) -> Option<(u64, usize)> {
    let mut value: u64 = 0;
    let mut shift = 0;
    let mut i = 0;

    loop {
        if i >= bytes.len() {
            return None;
        }
        let byte = bytes[i];
        i += 1;
        value |= ((byte & 0x7F) as u64) << shift;
        if byte & VARINT_CONTINUATION_BIT == 0 {
            return Some((value, i));
        }
        shift += 7;
        if shift > 63 {
            return None;
        }
    }
}

/// A single posting entry: doc_id delta + term frequency.
#[derive(Clone, Debug, PartialEq)]
pub struct Posting {
    pub doc_id: u64,
    pub freq: u32,
}

/// Encode postings with delta encoding.
pub fn encode_postings(postings: &[Posting]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(postings.len() * 4);
    let mut prev_doc_id: u64 = 0;

    for posting in postings {
        let delta = posting.doc_id - prev_doc_id;
        bytes.extend_from_slice(&encode_varint(delta));
        bytes.extend_from_slice(&encode_varint(posting.freq as u64));
        prev_doc_id = posting.doc_id;
    }

    bytes
}

/// Decode postings from bytes (with delta decoding).
pub fn decode_postings(bytes: &[u8]) -> Vec<Posting> {
    let mut postings = Vec::new();
    let mut prev_doc_id: u64 = 0;
    let mut offset = 0;

    while offset < bytes.len() {
        let (delta, consumed) = decode_varint(&bytes[offset..]).unwrap_or((0, 0));
        offset += consumed;

        let (freq, freq_consumed) = decode_varint(&bytes[offset..]).unwrap_or((0, 0));
        offset += freq_consumed;

        let doc_id = prev_doc_id + delta;
        postings.push(Posting {
            doc_id,
            freq: freq as u32,
        });
        prev_doc_id = doc_id;
    }

    postings
}

/// postings.bin layout:
/// [num_postings (u32)] [posting_bytes...]
pub fn encode_postings_to_file(postings: &[Posting]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(4 + postings.len() * 4);
    bytes.extend_from_slice(&(postings.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&encode_postings(postings));
    bytes
}

pub fn decode_postings_from_bytes(bytes: &[u8]) -> Vec<Posting> {
    if bytes.len() < 4 {
        return Vec::new();
    }
    let mut offset = 4;
    let num_postings = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    let mut postings = Vec::with_capacity(num_postings);
    let mut prev_doc_id: u64 = 0;

    while offset < bytes.len() {
        let (delta, consumed) = decode_varint(&bytes[offset..]).unwrap_or((0, 0));
        offset += consumed;

        let (freq, freq_consumed) = decode_varint(&bytes[offset..]).unwrap_or((0, 0));
        offset += freq_consumed;

        let doc_id = prev_doc_id + delta;
        postings.push(Posting {
            doc_id,
            freq: freq as u32,
        });
        prev_doc_id = doc_id;
    }

    postings
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_varint_roundtrip() {
        let values = [0u64, 127, 128, 255, 256, 1000, 1000000, u64::MAX];
        for &v in &values {
            let encoded = encode_varint(v);
            let (decoded, consumed) = decode_varint(&encoded).unwrap();
            assert_eq!(decoded, v, "varint roundtrip failed for {v}");
            assert_eq!(consumed, encoded.len());
        }
    }

    #[test]
    fn test_postings_encoding() {
        let postings = vec![
            Posting {
                doc_id: 100,
                freq: 3,
            },
            Posting {
                doc_id: 105,
                freq: 1,
            },
            Posting {
                doc_id: 110,
                freq: 2,
            },
            Posting {
                doc_id: 200,
                freq: 5,
            },
        ];
        let encoded = encode_postings(&postings);
        let decoded = decode_postings(&encoded);
        assert_eq!(postings, decoded);
    }
}
