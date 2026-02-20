# TC-2.1: Simple Node Promotion

Goal: Verify atomic promotion from Tier 1 to Tier 2 with new epoch.
Given: A node exists in Tier 1 ingestion buffer.
When: Promoting to Tier 2.
Then: The node is visible in Tier 2 and epoch is updated.

Preconditions:
- Tier 1 and Tier 2 enabled.
- Node exists in Tier 1 only.

Test Data:
- Slug: events/flood-2024-01
- Payload:
```json
{"_id":"events/flood-2024-01","summary":"Flood report","epoch":1}
```

Steps:
1. Insert node into Tier 1.
2. Promote node to Tier 2.
3. Read node from Tier 2 by slug.

Expected:
- Node is visible in Tier 2.
- Tier 2 epoch value is greater than Tier 1 epoch.
- Tier 1 no longer serves the node if configured for exclusive promotion.
