use std::sync::Mutex;

/// Buffered write accumulator for batching SQL mutations.
///
/// Collects SQL statements in a thread-safe buffer and signals when the
/// configured threshold is reached. The caller (typically [`Engine`](super::Engine))
/// then drains the buffer and applies all statements in a single lock
/// acquisition, amortizing the cost of exclusive access.
///
/// # Example
///
/// ```rust,ignore
/// let buf = WriteBuffer::new(50);
///
/// for sql in statements {
///     if buf.push(sql) {
///         // Threshold reached — drain and apply
///         let batch = buf.drain();
///         apply_all(&batch);
///     }
/// }
/// // Drain any remaining
/// let remainder = buf.drain();
/// ```
pub struct WriteBuffer {
    pending: Mutex<Vec<String>>,
    threshold: usize,
}

impl WriteBuffer {
    /// Create a buffer that signals flush at `threshold` pending statements.
    pub fn new(threshold: usize) -> Self {
        Self {
            pending: Mutex::new(Vec::with_capacity(threshold)),
            threshold,
        }
    }

    /// Push a SQL statement into the buffer.
    ///
    /// Returns `true` if the buffer has reached the flush threshold,
    /// signaling the caller to drain and apply.
    pub fn push(&self, sql: String) -> bool {
        let mut buf = self.pending.lock().expect("WriteBuffer poisoned");
        buf.push(sql);
        buf.len() >= self.threshold
    }

    /// Drain all buffered statements, returning them in insertion order.
    ///
    /// The buffer is empty after this call.
    pub fn drain(&self) -> Vec<String> {
        let mut buf = self.pending.lock().expect("WriteBuffer poisoned");
        std::mem::take(&mut *buf)
    }

    /// Number of pending (unflushed) statements.
    pub fn len(&self) -> usize {
        self.pending.lock().expect("WriteBuffer poisoned").len()
    }

    /// Returns `true` if no statements are buffered.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
