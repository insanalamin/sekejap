# TC-1.3: Spatial Index Registration

Goal: Ensure inserting a node with coordinates registers it in the spatial index.
Given: A node with GPS coordinates.
When: Inserting the node.
Then: The spatial index returns it for nearby queries.

Preconditions:
- Spatial index enabled.

Test Data:
- Slug: locations/monas
- Payload:
```json
{"_id":"locations/monas","name":"Monas","coordinates":{"lat":-6.1754,"lon":106.8272}}
```

Steps:
1. Insert node with coordinates.
2. Run a spatial query centered at (-6.1754, 106.8272) with radius 0.5km.

Expected:
- The spatial query includes the node.
- Node is excluded if radius is reduced to 0.01km.
