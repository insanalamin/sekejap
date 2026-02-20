use memmap2::MmapMut;
use std::fs::{File, OpenOptions};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

#[repr(C)]
pub struct ArenaHeader {
    pub magic: u64,
    pub committed_count: AtomicU64,
    pub durable_count: AtomicU64,
    pub capacity: u64,
    pub _pad: [u64; 4],
}

// NOTE: DurableArena cannot be Clone'd - cloning would duplicate mmap/file handles
// which is undefined behavior. Use references instead.
pub struct DurableArena<T> {
    mmap: MmapMut,
    pub _file: File,
    pub write_head: AtomicU64,
    pub slot_size: usize,
    _marker: std::marker::PhantomData<T>,
}

impl<T: Copy + Default> DurableArena<T> {
    pub fn new(path: &Path, capacity: usize) -> std::io::Result<Self> {
        let slot_size = std::mem::size_of::<T>();
        let file_size = (64 + capacity * slot_size).max(64);
        let file = OpenOptions::new().read(true).write(true).create(true).open(path)?;

        // Only grow — never truncate an existing arena (preserves data on reopen)
        if file.metadata()?.len() < file_size as u64 {
            file.set_len(file_size as u64)?;
        }

        let mut mmap = unsafe { MmapMut::map_mut(&file)? };

        let header_ptr = mmap.as_ptr() as *mut ArenaHeader;
        unsafe {
            if (*header_ptr).magic != 0x5345_4B45 {
                (*header_ptr).magic = 0x5345_4B45;
                (*header_ptr).capacity = capacity as u64;
            }
        }

        let committed = unsafe { (*header_ptr).committed_count.load(Ordering::Acquire) };

        Ok(Self {
            mmap,
            _file: file,
            write_head: AtomicU64::new(committed),
            slot_size,
            _marker: std::marker::PhantomData,
        })
    }

    /// Number of slots currently mapped (may be larger than original capacity on reopen).
    pub fn capacity(&self) -> usize {
        let mapped = self.mmap.len();
        if mapped <= 64 { 0 } else { (mapped - 64) / self.slot_size }
    }

    /// Grow the arena to hold at least `new_capacity` slots.
    /// Re-mmaps if the file needs to grow. No-op if already large enough.
    /// Requires `&mut self` — callers must hold exclusive access (e.g. RwLock write guard).
    pub fn resize(&mut self, new_capacity: usize) -> std::io::Result<()> {
        let needed = 64 + new_capacity * self.slot_size;
        if self.mmap.len() < needed {
            self._file.set_len(needed as u64)?;
            self.mmap = unsafe { MmapMut::map_mut(&self._file)? };
        }
        Ok(())
    }

    #[inline(always)]
    pub fn write_at(&self, idx: u64, item: &T) {
        let offset = 64 + (idx as usize * self.slot_size);
        unsafe {
            let dest = self.mmap.as_ptr().add(offset);
            std::ptr::copy_nonoverlapping(item as *const T as *const u8, dest as *mut u8, self.slot_size);
        }
    }

    #[inline(always)]
    pub fn read_at(&self, idx: u64) -> T {
        let offset = 64 + (idx as usize * self.slot_size);
        unsafe {
            let src = self.mmap.as_ptr().add(offset) as *const T;
            std::ptr::read_volatile(src)
        }
    }

    pub fn commit(&self, new_count: u64) {
        let header_ptr = self.mmap.as_ptr() as *const ArenaHeader;
        unsafe {
            (*header_ptr).committed_count.store(new_count, Ordering::Release);
        }
    }

    pub fn flush(&self) -> std::io::Result<()> {
        self.mmap.flush()
    }

    pub fn flush_written(&self) -> std::io::Result<()> {
        let count = self.write_head.load(Ordering::Acquire);
        let len = 64 + (count as usize * self.slot_size);
        self.mmap.flush_range(0, len)
    }

    pub fn get_mmap_ptr(&self) -> *const u8 {
        self.mmap.as_ptr()
    }
}

pub struct BlobArena {
    mmap: MmapMut,
    pub _file: File,
    pub write_offset: AtomicU64,
}

impl BlobArena {
    pub fn new(path: &Path, size_mb: usize) -> std::io::Result<Self> {
        let size = size_mb * 1024 * 1024;
        let file = OpenOptions::new().read(true).write(true).create(true).open(path)?;
        // Only grow — never truncate existing blob data on reopen
        if file.metadata()?.len() < size as u64 {
            file.set_len(size as u64)?;
        }
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };
        
        unsafe {
            let magic = *(mmap.as_ptr() as *const u64);
            if magic != 0x424C_4F42 {
                *(mmap.as_mut_ptr() as *mut u64) = 0x424C_4F42;
                *(mmap.as_mut_ptr().add(8) as *mut u64) = 64;
            }
        }

        let committed_offset = unsafe { *(mmap.as_ptr().add(8) as *const u64) };

        Ok(Self { 
            mmap, 
            _file: file,
            write_offset: AtomicU64::new(committed_offset) 
        })
    }

    #[inline(always)]
    pub fn append(&self, data: &[u8]) -> (u64, u32) {
        let len = data.len() as u32;
        let offset = self.write_offset.fetch_add(len as u64, Ordering::Relaxed);
        unsafe {
            let dest = self.mmap.as_ptr().add(offset as usize);
            std::ptr::copy_nonoverlapping(data.as_ptr(), dest as *mut u8, data.len());
        }
        (offset, len)
    }

    #[inline(always)]
    pub fn read(&self, offset: u64, len: u32) -> &[u8] {
        &self.mmap[offset as usize..(offset as usize + len as usize)]
    }

    pub fn commit(&self) {
        let offset = self.write_offset.load(Ordering::Acquire);
        unsafe {
            *(self.mmap.as_ptr().add(8) as *mut u64) = offset;
        }
    }

    pub fn flush(&self) -> std::io::Result<()> {
        self.mmap.flush()
    }

    pub fn flush_written(&self) -> std::io::Result<()> {
        let offset = self.write_offset.load(Ordering::Acquire);
        self.mmap.flush_range(0, offset as usize)
    }
}
