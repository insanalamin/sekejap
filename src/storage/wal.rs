//! Append-only WAL with CRC32-framed JSON records.
//!
//! # Frame layout
//! ```text
//! ┌──────────┬──────────┬──────────────────────────────┐
//! │ CRC32    │ length   │ JSON payload                 │
//! │ 4 bytes  │ 4 bytes  │ N bytes                      │
//! └──────────┴──────────┴──────────────────────────────┘
//! ```
//! CRC32 is computed over `[length_bytes || payload_bytes]`.
//!
//! On replay, a bad CRC stops the reader at that frame — everything
//! before it is intact. The JSON payload is human-readable so the WAL
//! can be inspected (or repaired) with any text tool.

use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;

// ── WAL entry ─────────────────────────────────────────────────────────────────

/// A single mutation recorded in the WAL.
///
/// The `op` tag is used as a discriminant in JSON: `{"op":"put",...}`.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub(crate) enum WalEntry {
    Put {
        slug: String,
        payload: String,
    },
    Remove {
        slug: String,
    },
    Link {
        from: String,
        to: String,
        edge_type: String,
        strength: f32,
    },
    LinkMeta {
        from: String,
        to: String,
        edge_type: String,
        strength: f32,
        meta: String,
    },
    Unlink {
        from: String,
        to: String,
        edge_type: String,
    },
    CreateTable {
        collection: String,
        schema_json: String,
    },
    PutVector {
        slug: String,
        field: String,
        data: Vec<f32>,
    },
    CreateIndex {
        collection: String,
        method: String,
        fields: Vec<String>,
    },
}

// ── CRC helper ────────────────────────────────────────────────────────────────

fn crc32(data: &[u8]) -> u32 {
    let mut h = crc32fast::Hasher::new();
    h.update(data);
    h.finalize()
}

// ── Writer ────────────────────────────────────────────────────────────────────

pub(crate) struct WalWriter {
    inner: BufWriter<File>,
}

impl WalWriter {
    /// Open (or create) a WAL file in append mode.
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            inner: BufWriter::new(file),
        })
    }

    /// Append one entry. Flushes to OS after every write.
    /// Call `sync()` if you need fsync-level durability.
    pub fn append(&mut self, entry: &WalEntry) -> io::Result<()> {
        let json =
            serde_json::to_vec(entry).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let len_bytes = (json.len() as u32).to_le_bytes();

        // CRC32 over [length(4) || payload(N)]
        let mut crc_input = Vec::with_capacity(4 + json.len());
        crc_input.extend_from_slice(&len_bytes);
        crc_input.extend_from_slice(&json);
        let checksum = crc32(&crc_input).to_le_bytes();

        self.inner.write_all(&checksum)?;
        self.inner.write_all(&len_bytes)?;
        self.inner.write_all(&json)?;
        self.inner.flush()
    }

    /// fsync — call after a batch of writes when you need
    /// guaranteed on-disk durability.
    pub fn sync(&mut self) -> io::Result<()> {
        self.inner.flush()?;
        self.inner.get_ref().sync_data()
    }
}

// ── Reader ────────────────────────────────────────────────────────────────────

pub(crate) struct WalReader {
    inner: BufReader<File>,
}

impl WalReader {
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = File::open(path)?;
        Ok(Self {
            inner: BufReader::new(file),
        })
    }

    /// Read every valid frame from the WAL.
    ///
    /// Stops at the first bad CRC, truncated frame, or oversized payload.
    /// Returns `(entries, had_corruption)` — if `had_corruption` is true
    /// the last frame was incomplete or corrupted (everything before it is fine).
    pub fn read_all(mut self) -> (Vec<WalEntry>, bool) {
        let mut entries = Vec::new();
        let mut corrupted = false;

        loop {
            // Read the 8-byte frame header [crc32(4) | length(4)]
            let mut header = [0u8; 8];
            match self.inner.read_exact(&mut header) {
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break, // clean end
                Err(_) => {
                    corrupted = true;
                    break;
                }
                Ok(_) => {}
            }

            let stored_crc = u32::from_le_bytes(header[..4].try_into().unwrap());
            let len = u32::from_le_bytes(header[4..].try_into().unwrap()) as usize;

            // Guard against corrupted length causing OOM
            if len > 64 << 20 {
                corrupted = true;
                break;
            }

            let mut payload = vec![0u8; len];
            match self.inner.read_exact(&mut payload) {
                Err(_) => {
                    corrupted = true;
                    break;
                }
                Ok(_) => {}
            }

            // Verify CRC over [length_bytes || payload]
            let mut crc_input = Vec::with_capacity(4 + len);
            crc_input.extend_from_slice(&(len as u32).to_le_bytes());
            crc_input.extend_from_slice(&payload);
            if crc32(&crc_input) != stored_crc {
                corrupted = true;
                break;
            }

            match serde_json::from_slice::<WalEntry>(&payload) {
                Ok(entry) => entries.push(entry),
                Err(_) => {
                    corrupted = true;
                    break;
                }
            }
        }

        (entries, corrupted)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn roundtrip(entries: Vec<WalEntry>) -> (Vec<WalEntry>, bool) {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        drop(tmp); // close so WalWriter can open it

        let mut w = WalWriter::open(&path).unwrap();
        for e in &entries {
            w.append(e).unwrap();
        }
        drop(w);

        WalReader::open(&path).unwrap().read_all()
    }

    #[test]
    fn write_and_read_put() {
        let (entries, corrupted) = roundtrip(vec![WalEntry::Put {
            slug: "alice".into(),
            payload: r#"{"name":"Alice"}"#.into(),
        }]);
        assert!(!corrupted);
        assert_eq!(entries.len(), 1);
        assert!(matches!(&entries[0], WalEntry::Put { slug, .. } if slug == "alice"));
    }

    #[test]
    fn write_multiple_ops() {
        let (entries, corrupted) = roundtrip(vec![
            WalEntry::Put {
                slug: "alice".into(),
                payload: "{}".into(),
            },
            WalEntry::Link {
                from: "alice".into(),
                to: "bob".into(),
                edge_type: "follows".into(),
                strength: 1.0,
            },
            WalEntry::Remove {
                slug: "alice".into(),
            },
        ]);
        assert!(!corrupted);
        assert_eq!(entries.len(), 3);
    }

    #[test]
    fn bad_crc_stops_replay() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        drop(tmp);

        // Write one good entry
        let mut w = WalWriter::open(&path).unwrap();
        w.append(&WalEntry::Put {
            slug: "a".into(),
            payload: "{}".into(),
        })
        .unwrap();
        drop(w);

        // Corrupt the middle of the file (flip a byte in the JSON payload area)
        let mut data = std::fs::read(&path).unwrap();
        let mid = data.len() / 2;
        data[mid] ^= 0xff;
        std::fs::write(&path, &data).unwrap();

        let (entries, corrupted) = WalReader::open(&path).unwrap().read_all();
        assert!(corrupted);
        assert_eq!(entries.len(), 0); // the only entry was corrupted
    }

    #[test]
    fn partial_frame_detected() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        drop(tmp);

        let mut w = WalWriter::open(&path).unwrap();
        w.append(&WalEntry::Put {
            slug: "x".into(),
            payload: "{}".into(),
        })
        .unwrap();
        drop(w);

        // Truncate to half the file to simulate a partial write
        let data = std::fs::read(&path).unwrap();
        std::fs::write(&path, &data[..data.len() / 2]).unwrap();

        let (entries, corrupted) = WalReader::open(&path).unwrap().read_all();
        assert!(corrupted);
        assert_eq!(entries.len(), 0);
    }

    #[test]
    fn good_entries_before_corruption_are_preserved() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        drop(tmp);

        // Write two good entries
        let mut w = WalWriter::open(&path).unwrap();
        w.append(&WalEntry::Put {
            slug: "a".into(),
            payload: "{}".into(),
        })
        .unwrap();
        w.append(&WalEntry::Put {
            slug: "b".into(),
            payload: "{}".into(),
        })
        .unwrap();
        drop(w);

        let _good_data = std::fs::read(&path).unwrap();

        // Append a corrupted frame manually
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(&[
            0xde, 0xad, 0xbe, 0xef, 0x05, 0x00, 0x00, 0x00, b'b', b'a', b'd', 0x00, 0x00,
        ])
        .unwrap();
        drop(f);

        let (entries, corrupted) = WalReader::open(&path).unwrap().read_all();
        assert!(corrupted);
        assert_eq!(entries.len(), 2); // two good entries preserved
    }
}
