use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Pointer to a blob in BlobStore
/// Uses file_id, offset, and length for efficient storage and retrieval
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BlobPtr {
    pub file_id: u64,
    pub offset: u64,
    pub length: u64,
}

impl BlobPtr {
    pub fn new(file_id: u64, offset: u64, length: u64) -> Self {
        Self {
            file_id,
            offset,
            length,
        }
    }
}

/// Simple file-based BlobStore for testing (hyperminimalist approach)
/// In production, this would use NVMe optimization and proper WAL
pub struct BlobStore {
    base_dir: PathBuf,
    current_file_id: u64,
    current_file: std::fs::File,
    current_offset: u64,
}

impl BlobStore {
    pub fn new(base_dir: PathBuf) -> std::io::Result<Self> {
        std::fs::create_dir_all(&base_dir)?;

        let file_id = 0;
        let path = base_dir.join(format!("blob_{}.dat", file_id));
        let current_file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&path)?;

        Ok(Self {
            base_dir,
            current_file_id: file_id,
            current_file,
            current_offset: 0,
        })
    }

    /// Write bytes to blob store and return pointer
    pub fn write(&mut self, data: &[u8]) -> std::io::Result<BlobPtr> {
        let ptr = BlobPtr::new(self.current_file_id, self.current_offset, data.len() as u64);

        use std::io::Write;
        self.current_file.write_all(data)?;
        self.current_file.flush()?;

        self.current_offset += data.len() as u64;
        Ok(ptr)
    }

    /// Read bytes from blob store using pointer
    pub fn read(&self, ptr: BlobPtr) -> std::io::Result<Vec<u8>> {
        let path = self.base_dir.join(format!("blob_{}.dat", ptr.file_id));
        let mut file = std::fs::File::open(&path)?;

        use std::io::{Read, Seek};
        file.seek(std::io::SeekFrom::Start(ptr.offset))?;

        let mut buffer = vec![0u8; ptr.length as usize];
        file.read_exact(&mut buffer)?;

        Ok(buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_blob_write_read() {
        let temp_dir = TempDir::new().unwrap();
        let mut store = BlobStore::new(temp_dir.path().to_path_buf()).unwrap();

        let data = b"Hello, Sekejap-DB!";
        let ptr = store.write(data).unwrap();

        let read_data = store.read(ptr).unwrap();
        assert_eq!(data.to_vec(), read_data);
    }

    #[test]
    fn test_multiple_writes() {
        let temp_dir = TempDir::new().unwrap();
        let mut store = BlobStore::new(temp_dir.path().to_path_buf()).unwrap();

        let data1 = vec![1u8, 2, 3];
        let data2 = vec![4u8, 5, 6];

        let ptr1 = store.write(&data1).unwrap();
        let ptr2 = store.write(&data2).unwrap();

        let read1 = store.read(ptr1).unwrap();
        let read2 = store.read(ptr2).unwrap();

        assert_eq!(data1, read1);
        assert_eq!(data2, read2);
    }
}
