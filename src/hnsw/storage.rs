use crate::arena::DurableArena;
use crate::types::VectorSlot;
use std::sync::atomic::Ordering;

/// Arena-backed vector store using raw pointers (not Arc clone)
/// This avoids the undefined behavior of cloning mmap handles.
pub struct ArenaVectorStore {
    ptr: *const u8,                    // raw pointer to mmap base
    slot_size: usize,                   // size of VectorSlot
    len_ptr: *const std::sync::atomic::AtomicU64,  // pointer to write_head
    dim: usize,
}

impl ArenaVectorStore {
    pub fn new(arena: &DurableArena<VectorSlot>, dim: usize) -> Self {
        Self {
            ptr: arena.get_mmap_ptr(),
            slot_size: arena.slot_size,
            len_ptr: &arena.write_head as *const _ as *const std::sync::atomic::AtomicU64,
            dim,
        }
    }

    #[inline(always)]
    pub fn get(&self, idx: u32) -> &[f32] {
        let offset = 64 + (idx as usize * self.slot_size);
        unsafe {
            let ptr = self.ptr.add(offset) as *const f32;
            std::slice::from_raw_parts(ptr, self.dim)
        }
    }

    pub fn len(&self) -> usize {
        unsafe { (*self.len_ptr).load(Ordering::Acquire) as usize }
    }
}

// Safety: ArenaVectorStore is safe to send between threads because:
// 1. The underlying mmap is memory-mapped file, which is thread-safe
// 2. All accesses are through raw pointers that point to valid mmap data
// 3. The mmap lives as long as the SekejapDB which owns the arena
unsafe impl Send for ArenaVectorStore {}
unsafe impl Sync for ArenaVectorStore {}
