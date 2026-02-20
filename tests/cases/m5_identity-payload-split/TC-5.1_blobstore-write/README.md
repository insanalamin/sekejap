# TC-5.1: BlobStore Write

Goal: Verify BlobStore write returns a valid pointer and persists data.
Given: Large JSON payload (~10KB).
When: Writing to BlobStore.
Then: Return BlobPtr and data is durable.

Preconditions:
- BlobStore configured.

Test Data:
- Payload size: 10KB JSON blob.

Steps:
1. Write payload to BlobStore.
2. Record BlobPtr (file_id, offset, length).

Expected:
- BlobPtr values are non-zero.
- Payload is present on disk after flush.
