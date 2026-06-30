//! S3 segment sync — upload/download database files to/from object storage.
//!
//! Segment files are transferred in 8 MB chunks — never loaded fully into RAM.
//! CRC32 checksums are computed incrementally during transfer.
//!
//! - **Upload**: small files (< 8 MB) use a single `put()`; large files use
//!   `put_multipart()` with 8 MB parts.
//! - **Download**: `get_range()` in 8 MB chunks, written directly to a temp
//!   file, verified by CRC32, then atomic-renamed into place.
//!
//! Credentials are passed via [`S3Credentials`] so downstream systems (e.g.
//! zebflow) can integrate their own credential management.
//!
//! Gated behind `#[cfg(feature = "s3")]`.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, PutPayload};
use tokio::runtime::Runtime;

use super::manifest::{Manifest, Segment};

const MULTIPART_THRESHOLD: usize = 8 * 1024 * 1024;
const CHUNK_SIZE: usize = 8 * 1024 * 1024;

/// S3-compatible credentials passed by the caller.
#[derive(Clone, Debug)]
pub struct S3Credentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub region: String,
    pub endpoint: Option<String>,
    pub allow_http: bool,
}

impl S3Credentials {
    pub fn new(access_key_id: &str, secret_access_key: &str, region: &str) -> Self {
        Self {
            access_key_id: access_key_id.to_string(),
            secret_access_key: secret_access_key.to_string(),
            region: region.to_string(),
            endpoint: None,
            allow_http: false,
        }
    }

    pub fn endpoint(mut self, url: &str) -> Self {
        self.endpoint = Some(url.to_string());
        self
    }

    pub fn allow_http(mut self, allow: bool) -> Self {
        self.allow_http = allow;
        self
    }
}

/// Syncs local database segments to/from an S3-compatible object store.
pub struct RemoteSync {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    runtime: Runtime,
}

impl RemoteSync {
    /// Create from an S3 URL and explicit credentials.
    pub fn from_url(url: &str, creds: &S3Credentials) -> Result<Self, String> {
        let rest = url
            .strip_prefix("s3://")
            .ok_or_else(|| format!("expected s3:// URL, got: {url}"))?;
        let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));

        use object_store::aws::AmazonS3Builder;
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_access_key_id(&creds.access_key_id)
            .with_secret_access_key(&creds.secret_access_key)
            .with_region(&creds.region);
        if let Some(ep) = &creds.endpoint {
            builder = builder.with_endpoint(ep);
        }
        if creds.allow_http {
            builder = builder.with_allow_http(true);
        }
        let store = builder.build().map_err(|e| format!("S3 init: {e}"))?;

        Self::from_store(Arc::new(store), prefix)
    }

    /// Create from a pre-configured object store (for testing with `InMemory`).
    pub fn from_store(store: Arc<dyn ObjectStore>, prefix: &str) -> Result<Self, String> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("tokio init: {e}"))?;

        Ok(Self {
            store,
            prefix: prefix.to_string(),
            runtime,
        })
    }

    fn obj_path(&self, name: &str) -> ObjPath {
        if self.prefix.is_empty() {
            ObjPath::from(name)
        } else {
            ObjPath::from(format!("{}/{}", self.prefix, name))
        }
    }

    /// Upload all segment files to S3 and write a new manifest.
    pub fn sync_to_remote(&self, db_dir: &Path) -> Result<(), String> {
        self.runtime.block_on(self.upload_async(db_dir))
    }

    /// Download missing or stale segments from S3 using the manifest.
    pub fn sync_from_remote(&self, db_dir: &Path) -> Result<(), String> {
        self.runtime.block_on(self.download_async(db_dir))
    }

    /// Return a clone of the underlying object store.
    pub fn store(&self) -> Arc<dyn ObjectStore> {
        self.store.clone()
    }

    /// Return the S3 prefix for this remote.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Fetch the remote manifest. Returns `None` if no manifest exists.
    pub fn get_manifest(&self) -> Result<Option<super::manifest::Manifest>, String> {
        self.runtime.block_on(async {
            let manifest_path = self.obj_path("manifest.json");
            match self.store.get(&manifest_path).await {
                Ok(result) => {
                    let bytes = result
                        .bytes()
                        .await
                        .map_err(|e| format!("reading manifest: {e}"))?;
                    let m: super::manifest::Manifest = serde_json::from_slice(&bytes)
                        .map_err(|e| format!("parsing manifest: {e}"))?;
                    Ok(Some(m))
                }
                Err(object_store::Error::NotFound { .. }) => Ok(None),
                Err(e) => Err(format!("checking manifest: {e}")),
            }
        })
    }

    /// Fetch raw bytes of a remote file by name.
    pub fn fetch_file(&self, name: &str) -> Result<Vec<u8>, String> {
        let path = self.obj_path(name);
        self.runtime.block_on(async {
            let result = self
                .store
                .get(&path)
                .await
                .map_err(|e| format!("fetching {name}: {e}"))?;
            let bytes = result
                .bytes()
                .await
                .map_err(|e| format!("reading {name}: {e}"))?;
            Ok(bytes.to_vec())
        })
    }

    /// Check the remote manifest generation (0 if no manifest exists).
    pub fn latest_generation(&self) -> Result<u64, String> {
        self.runtime.block_on(async {
            let manifest_path = self.obj_path("manifest.json");
            match self.store.get(&manifest_path).await {
                Ok(result) => {
                    let bytes = result
                        .bytes()
                        .await
                        .map_err(|e| format!("reading manifest: {e}"))?;
                    let m: Manifest = serde_json::from_slice(&bytes)
                        .map_err(|e| format!("parsing manifest: {e}"))?;
                    Ok(m.generation)
                }
                Err(object_store::Error::NotFound { .. }) => Ok(0),
                Err(e) => Err(format!("checking manifest: {e}")),
            }
        })
    }

    // ── Upload (streaming) ───────────────────────────────────────────────────

    async fn upload_async(&self, db_dir: &Path) -> Result<(), String> {
        let files =
            list_segment_files(db_dir).map_err(|e| format!("listing segments: {e}"))?;
        if files.is_empty() {
            return Ok(());
        }

        let manifest_path = self.obj_path("manifest.json");
        let prev_gen = match self.store.get(&manifest_path).await {
            Ok(result) => {
                let bytes = result
                    .bytes()
                    .await
                    .map_err(|e| format!("reading manifest: {e}"))?;
                serde_json::from_slice::<Manifest>(&bytes)
                    .map(|m| m.generation)
                    .unwrap_or(0)
            }
            Err(_) => 0,
        };

        let mut segments = Vec::new();
        for file_path in &files {
            let name = file_path
                .file_name()
                .and_then(|n| n.to_str())
                .ok_or("invalid segment filename")?
                .to_string();

            let seg = self.upload_file(file_path, &name).await?;
            segments.push(seg);
        }

        let manifest = Manifest::new(prev_gen + 1, segments);
        let json = serde_json::to_vec_pretty(&manifest)
            .map_err(|e| format!("serializing manifest: {e}"))?;
        self.store
            .put(&manifest_path, PutPayload::from(json))
            .await
            .map_err(|e| format!("writing manifest: {e}"))?;

        Ok(())
    }

    async fn upload_file(&self, file_path: &Path, name: &str) -> Result<Segment, String> {
        let file_len = std::fs::metadata(file_path)
            .map_err(|e| format!("stat {name}: {e}"))?
            .len();
        let obj_path = self.obj_path(name);

        if file_len < MULTIPART_THRESHOLD as u64 {
            let bytes =
                std::fs::read(file_path).map_err(|e| format!("reading {name}: {e}"))?;
            let crc = crc32fast::hash(&bytes);
            self.store
                .put(&obj_path, PutPayload::from(bytes))
                .await
                .map_err(|e| format!("uploading {name}: {e}"))?;
            return Ok(Segment {
                name: name.to_string(),
                size: file_len,
                crc32: crc,
            });
        }

        // Large file: multipart upload with streaming CRC.
        let mut upload = self
            .store
            .put_multipart(&obj_path)
            .await
            .map_err(|e| format!("starting multipart {name}: {e}"))?;

        let mut file = std::io::BufReader::new(
            std::fs::File::open(file_path).map_err(|e| format!("opening {name}: {e}"))?,
        );
        let mut hasher = crc32fast::Hasher::new();
        let mut buf = vec![0u8; CHUNK_SIZE];

        loop {
            let filled = read_full(&mut file, &mut buf)
                .map_err(|e| format!("reading {name}: {e}"))?;
            if filled == 0 {
                break;
            }
            hasher.update(&buf[..filled]);
            upload
                .put_part(PutPayload::from(buf[..filled].to_vec()))
                .await
                .map_err(|e| format!("uploading part {name}: {e}"))?;
        }

        upload
            .complete()
            .await
            .map_err(|e| format!("completing multipart {name}: {e}"))?;

        Ok(Segment {
            name: name.to_string(),
            size: file_len,
            crc32: hasher.finalize(),
        })
    }

    // ── Download (streaming) ─────────────────────────────────────────────────

    async fn download_async(&self, db_dir: &Path) -> Result<(), String> {
        let manifest_path = self.obj_path("manifest.json");
        let result = match self.store.get(&manifest_path).await {
            Ok(r) => r,
            Err(object_store::Error::NotFound { .. }) => return Ok(()),
            Err(e) => return Err(format!("reading manifest: {e}")),
        };
        let bytes = result
            .bytes()
            .await
            .map_err(|e| format!("reading manifest bytes: {e}"))?;
        let manifest: Manifest =
            serde_json::from_slice(&bytes).map_err(|e| format!("parsing manifest: {e}"))?;

        std::fs::create_dir_all(db_dir).map_err(|e| format!("creating db dir: {e}"))?;

        for segment in &manifest.segments {
            let local_path = db_dir.join(&segment.name);

            // Fast skip: check size first (no I/O beyond stat), then streaming CRC.
            if local_path.exists() {
                if let Ok(meta) = std::fs::metadata(&local_path) {
                    if meta.len() == segment.size {
                        if let Ok(crc) = stream_crc32(&local_path) {
                            if crc == segment.crc32 {
                                continue;
                            }
                        }
                    }
                }
            }

            self.download_file(segment, &local_path).await?;
        }

        Ok(())
    }

    async fn download_file(&self, segment: &Segment, local_path: &Path) -> Result<(), String> {
        let obj_path = self.obj_path(&segment.name);
        let tmp_path = local_path.with_extension("s3tmp");
        let total = segment.size as usize;

        let mut file = std::fs::File::create(&tmp_path)
            .map_err(|e| format!("creating tmp {}: {e}", segment.name))?;
        let mut hasher = crc32fast::Hasher::new();
        let mut offset = 0usize;

        while offset < total {
            let end = std::cmp::min(offset + CHUNK_SIZE, total);
            let bytes = self
                .store
                .get_range(&obj_path, offset..end)
                .await
                .map_err(|e| format!("downloading {}: {e}", segment.name))?;
            hasher.update(&bytes);
            std::io::Write::write_all(&mut file, &bytes)
                .map_err(|e| format!("writing {}: {e}", segment.name))?;
            offset = end;
        }

        let crc = hasher.finalize();
        if crc != segment.crc32 {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(format!("checksum mismatch for {}", segment.name));
        }

        file.sync_all()
            .map_err(|e| format!("syncing {}: {e}", segment.name))?;
        drop(file);
        std::fs::rename(&tmp_path, local_path)
            .map_err(|e| format!("renaming {}: {e}", segment.name))?;

        Ok(())
    }
}

/// Read until `buf` is full or EOF. Returns number of bytes filled.
fn read_full(reader: &mut impl std::io::Read, buf: &mut [u8]) -> io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        let n = reader.read(&mut buf[filled..])?;
        if n == 0 {
            break;
        }
        filled += n;
    }
    Ok(filled)
}

/// Compute CRC32 of a file by streaming through it (64 KB at a time).
fn stream_crc32(path: &Path) -> io::Result<u32> {
    use std::io::Read;
    let mut file = std::fs::File::open(path)?;
    let mut hasher = crc32fast::Hasher::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize())
}

/// List segment files in a database directory that should be synced.
fn list_segment_files(db_dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut files = Vec::new();

    for name in &[
        "snapshot.json",
        "payloads.bin",
        "gin.bin",
        "search.bin",
        "edges.bin",
        "edge_meta.bin",
    ] {
        let p = db_dir.join(name);
        if p.exists() {
            files.push(p);
        }
    }

    if db_dir.is_dir() {
        for entry in std::fs::read_dir(db_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let s = name.to_string_lossy();
            if s.starts_with("vectors_") && s.ends_with(".bin") {
                files.push(entry.path());
            }
        }
    }

    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shared_mem_store() -> Arc<dyn ObjectStore> {
        Arc::new(object_store::memory::InMemory::new())
    }

    #[test]
    fn test_list_segment_files_empty_dir() {
        let dir = tempfile::tempdir().unwrap();
        let files = list_segment_files(dir.path()).unwrap();
        assert!(files.is_empty());
    }

    #[test]
    fn test_list_segment_files_with_segments() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("snapshot.json"), b"{}").unwrap();
        std::fs::write(dir.path().join("payloads.bin"), b"\x00").unwrap();
        std::fs::write(dir.path().join("wal.log"), b"").unwrap();
        std::fs::write(dir.path().join("db.lock"), b"").unwrap();
        std::fs::write(dir.path().join("vectors_emb.bin"), b"\x00").unwrap();

        let files = list_segment_files(dir.path()).unwrap();
        let names: Vec<&str> = files
            .iter()
            .map(|p| p.file_name().unwrap().to_str().unwrap())
            .collect();
        assert!(names.contains(&"snapshot.json"));
        assert!(names.contains(&"payloads.bin"));
        assert!(names.contains(&"vectors_emb.bin"));
        assert!(!names.contains(&"wal.log"));
        assert!(!names.contains(&"db.lock"));
    }

    #[test]
    fn test_stream_crc32() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        let data = b"hello world streaming crc32";
        std::fs::write(&path, data).unwrap();
        let crc = stream_crc32(&path).unwrap();
        assert_eq!(crc, crc32fast::hash(data));
    }

    #[test]
    fn test_roundtrip_sync() {
        let store = shared_mem_store();
        let remote = RemoteSync::from_store(store, "testdb").unwrap();

        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("snapshot.json"), b"{\"nodes\":[]}").unwrap();
        std::fs::write(dir.path().join("payloads.bin"), b"\x00\x01\x02").unwrap();

        remote.sync_to_remote(dir.path()).unwrap();

        let dir2 = tempfile::tempdir().unwrap();
        remote.sync_from_remote(dir2.path()).unwrap();

        assert_eq!(
            std::fs::read(dir2.path().join("snapshot.json")).unwrap(),
            b"{\"nodes\":[]}"
        );
        assert_eq!(
            std::fs::read(dir2.path().join("payloads.bin")).unwrap(),
            b"\x00\x01\x02"
        );

        // Second sync should skip (files match).
        remote.sync_from_remote(dir2.path()).unwrap();
    }

    #[test]
    fn test_download_no_manifest() {
        let store = shared_mem_store();
        let remote = RemoteSync::from_store(store, "empty").unwrap();
        let dir = tempfile::tempdir().unwrap();
        remote.sync_from_remote(dir.path()).unwrap();
    }

    #[test]
    fn test_generation_increments() {
        let store = shared_mem_store();
        let remote = RemoteSync::from_store(store, "gen").unwrap();
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("snapshot.json"), b"{}").unwrap();

        remote.sync_to_remote(dir.path()).unwrap();
        remote.sync_to_remote(dir.path()).unwrap();
        remote.sync_to_remote(dir.path()).unwrap();

        assert_eq!(remote.latest_generation().unwrap(), 3);
    }

    #[test]
    fn test_latest_generation_no_manifest() {
        let store = shared_mem_store();
        let remote = RemoteSync::from_store(store, "none").unwrap();
        assert_eq!(remote.latest_generation().unwrap(), 0);
    }

    #[test]
    fn test_writer_reader_shared_store() {
        let store = shared_mem_store();

        let writer = RemoteSync::from_store(store.clone(), "shared").unwrap();
        let w_dir = tempfile::tempdir().unwrap();
        std::fs::write(w_dir.path().join("snapshot.json"), b"{\"v\":1}").unwrap();
        writer.sync_to_remote(w_dir.path()).unwrap();

        let reader = RemoteSync::from_store(store, "shared").unwrap();
        let r_dir = tempfile::tempdir().unwrap();
        reader.sync_from_remote(r_dir.path()).unwrap();

        assert_eq!(
            std::fs::read(r_dir.path().join("snapshot.json")).unwrap(),
            b"{\"v\":1}"
        );
        assert_eq!(reader.latest_generation().unwrap(), 1);
    }

    #[test]
    fn test_large_file_multipart() {
        let store = shared_mem_store();
        let remote = RemoteSync::from_store(store, "large").unwrap();

        let dir = tempfile::tempdir().unwrap();
        // Create a file larger than MULTIPART_THRESHOLD (8 MB).
        let big = vec![0xABu8; MULTIPART_THRESHOLD + 1024];
        std::fs::write(dir.path().join("payloads.bin"), &big).unwrap();
        // Small snapshot for manifest.
        std::fs::write(dir.path().join("snapshot.json"), b"{}").unwrap();

        remote.sync_to_remote(dir.path()).unwrap();

        // Download into fresh dir.
        let dir2 = tempfile::tempdir().unwrap();
        remote.sync_from_remote(dir2.path()).unwrap();

        let downloaded = std::fs::read(dir2.path().join("payloads.bin")).unwrap();
        assert_eq!(downloaded.len(), big.len());
        assert_eq!(downloaded, big);
    }
}
