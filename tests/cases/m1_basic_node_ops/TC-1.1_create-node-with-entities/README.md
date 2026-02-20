# TC-1.1: Create Node with Entities

Goal: Verify entity-rich node creation persists ID, timestamp, payload, and indexes.
Given: An ingestion agent extracts entities from a news story.
When: Creating a node with Who/Where/When fields and coordinates.
Then: The node is stored with proper ID and timestamp and is discoverable via spatial lookup.

Preconditions:
- Empty DB or isolated namespace for events.
- System clock available for timestamp assignment.

Test Data:
- Slug: events/jakarta-crime-2024-04-12
- Payload:
```json
{"_id":"events/jakarta-crime-2024-04-12","who":["Police","Mayor"],"where":"Jakarta","when":"2024-04-12T09:30:00Z","coordinates":{"lat":-6.2088,"lon":106.8456},"summary":"Morning incident near downtown"}
```

Steps:
1. Insert the node with the slug and payload.
2. Read the node by slug.
3. Run a spatial query centered at (-6.2088, 106.8456) with radius 1km.

Expected:
- Read returns the same payload content.
- slug_index contains a mapping for the slug hash to the node ID.
- Node header timestamp is set within 2 seconds of insert time.
- Spatial query includes the node.
