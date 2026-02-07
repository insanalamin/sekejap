# Data Processing Examples (Python)

This directory contains Python examples showing how to perform common data processing operations with Sekejap-DB.

## Overview

Sekejap follows a **composable atom** philosophy rather than providing high-level built-in functions. Each example demonstrates how to combine basic operations to implement complex operations similar to SQL queries.

## Examples

### [filters.py](./filters.py)
Demonstrates WHERE clause equivalents:
- **Simple Filter** - Single condition
- **Compound AND Filter** - Multiple conditions with AND
- **Compound OR Filter** - Multiple conditions with OR
- **Negation Filter** - NOT condition
- **Range Filter** - BETWEEN clause
- **Pattern Filter** - LIKE clause
- **IN Filter** - Multiple values
- **Edge Filter** - Filter by edge relationships

**Run:**
```bash
python examples/python/data-processing/filters.py
```

### [aggregations.py](./aggregations.py)
Demonstrates GROUP BY and aggregation functions:
- **COUNT** - Count items per group
- **SUM** - Sum values
- **AVG** - Average values
- **GROUP BY** - Single column grouping
- **Multi-Column GROUP BY** - Multiple grouping keys

**Run:**
```bash
python examples/python/data-processing/aggregations.py
```

### [joins.py](./joins.py)
Demonstrates SQL-like JOIN operations:
- **Inner Join** - Only matching pairs
- **Left Join** - Include NULL matches
- **Self Join** - Join table with itself
- **Multi-Way Join** - Join multiple tables

**Run:**
```bash
python examples/python/data-processing/joins.py
```

## Philosophy

### Why Not Built-In Functions?

SQL databases provide built-in functions like `JOIN`, `COUNT`, `GROUP BY` because they hide complexity. Sekejap takes a different approach:

1. **Composability** - Operations combine naturally
2. **Transparency** - No hidden magic, clear data flow
3. **Flexibility** - Adapt patterns to any use case
4. **LLM-Friendly** - Easy for AI to understand and compose

### Example: Inner Join

Instead of:
```sql
SELECT r.*, rev.*
FROM restaurants r
INNER JOIN reviews rev ON r.id = rev.restaurant_id
```

Python with Sekejap:
```python
# Get all restaurants
restaurants = db.read("restaurant-1")

# Get reviews for each restaurant
reviews = []
edges = db.get_edges_from("restaurant-1")
for edge in edges:
    if edge.type == "reviews":
        review = db.read(edge.target)
        reviews.append(review)
```

## Common Patterns

### Pattern 1: Filter-Transform
```python
results = [item for item in items if condition(item)]
```

### Pattern 2: Group By
```python
from collections import defaultdict
groups = defaultdict(list)
for item in items:
    key = extract_key(item)
    groups[key].append(item)
```

### Pattern 3: Join with Lookup
```python
results = []
edges = db.get_edges_from(source)
for edge in edges:
    target = db.read(edge.target)
    results.append((source, target))
```

## Performance Tips

1. **Bulk operations** - Fetch all needed data first
2. **Limit early** - Apply filters before expensive operations
3. **Use WriteOptions** - Use `publish_now=True` for immediate reads

## Next Steps

1. **Study these examples** to understand composition patterns
2. **Apply to your use cases** - adapt patterns as needed
3. **Check Rust examples** for more advanced patterns in `examples/rust/data-processing/`

---

**Sekejap** - Composable Multi-Modal Graph Database
