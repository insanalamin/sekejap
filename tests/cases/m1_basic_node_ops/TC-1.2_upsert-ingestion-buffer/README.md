# TC-1.2: Upsert to Ingestion Buffer

Goal: Ensure upsert updates an existing node rather than creating a duplicate.
Given: An existing node with slug "events/jakarta-crime-2024" in Tier 1.
When: Upserting a new version with updated fields.
Then: The buffer updates the existing node and keeps a single logical record.

Preconditions:
- Tier 1 ingestion buffer enabled.
- Existing node present with slug events/jakarta-crime-2024.

Test Data:
- Initial payload:
```json
{"_id":"events/jakarta-crime-2024","summary":"Initial report","version":1}
```
- Updated payload:
```json
{"_id":"events/jakarta-crime-2024","summary":"Update with new details","version":2}
```

Steps:
1. Insert initial payload into Tier 1.
2. Upsert the updated payload using the same slug.
3. Fetch by slug and count nodes with that slug.

Expected:
- Only one node exists for the slug.
- Payload reflects version 2 and updated summary.
- No duplicate index entry is created.
