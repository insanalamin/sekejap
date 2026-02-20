# TC-5.3: NodeHeader with Payload Reference

Goal: Verify NodeHeader and BlobStore integration.
Given: NodeHeader with payload_ptr.
When: Reading full node.
Then: Header from index and payload from BlobStore are retrieved.

Preconditions:
- NodeHeader schema includes payload_ptr.

Steps:
1. Insert node with payload stored in BlobStore.
2. Read node by slug.

Expected:
- Header fields loaded from index structure.
- Payload loaded from BlobStore via pointer.
