# TC-5.2: BlobStore Read

Goal: Read payload by BlobPtr with zero-copy semantics.
Given: A valid BlobPtr.
When: Reading from BlobStore.
Then: Payload is returned without copy (rkyv or mmap slice).

Preconditions:
- BlobPtr obtained from TC-5.1.

Steps:
1. Read payload by BlobPtr.
2. Compare with original payload.

Expected:
- Payload matches byte-for-byte.
- Read path uses zero-copy or mmap slice.
