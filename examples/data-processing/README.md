# Data Processing Examples

This directory contains examples showing how to compose Sekejap atoms for common data processing operations.

## Overview

Sekejap follows a **composable atom** philosophy rather than providing high-level built-in functions. Each example demonstrates how to combine basic atoms to implement complex operations similar to SQL queries.

## Examples

### [joins.rs](./joins.rs)
Demonstrates SQL-like JOIN operations:
- **Inner Join** - Only matching pairs
- **Left Join** - Include NULL matches
- **Self Join** - Join table with itself
- **Multi-Way Join** - Join multiple tables
- **Join with Filter** - WHERE clause on joined results
- **Join with Aggregation** - GROUP BY on joins

**Run:**
```bash
cargo run --example joins
```

### [aggregations.rs](./aggregations.rs)
Demonstrates GROUP BY and aggregation functions:
- **COUNT** - Count items per group
- **SUM** - Sum values
- **AVG** - Average values
- **GROUP BY** - Single column grouping
- **GROUP BY + HAVING** - Filter aggregated results
- **Multi-Column GROUP BY** - Multiple grouping keys
- **Aggregation with JOIN** - Join then aggregate

**Run:**
```bash
cargo run --example aggregations
```

### [filters.rs](./filters.rs)
Demonstrates WHERE clause equivalents:
- **Simple Filter** - Single condition
- **Compound AND Filter** - Multiple conditions with AND
- **Compound OR Filter** - Multiple conditions with OR
- **Negation Filter** - NOT condition
- **Range Filter** - BETWEEN clause
- **Pattern Filter** - LIKE clause
- **IN Filter** - Multiple values
- **Edge Filter** - Filter by edge relationships
- **Multi-Step Filter** - Traverse then filter

**Run:**
```bash
cargo run --example filters
```

## Philosophy

### Why Not Built-In Functions?

SQL databases provide built-in functions like `JOIN`, `COUNT`, `GROUP BY` because they hide complexity. Sekejap takes a different approach:

1. **Composability** - Atoms combine naturally like LEGO blocks
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

Compose atoms:
```rust
let restaurants = get_nodes_by_edge_type(db, EdgeType::AvailableOn);

for restaurant in restaurants {
    let reviews = get_edges_from(db, &restaurant.slug)
        .into_iter()
        .filter(|e| e.edge_type == EdgeType::Reviews)
        .map(|e| get_node(db, &e.target_slug))
        .collect();

    // Use (restaurant, reviews) pair
}
```

### Benefits

- **No learning curve** - Same atoms for all operations
- **Type-safe** - Compile-time checking
- **Zero-overhead** - No abstraction penalty
- **Extensible** - Easily add custom logic

## Common Patterns

### Pattern 1: Filter-Transform-Aggregate
```rust
results
    .into_iter()
    .filter(|x| condition(x))     // WHERE
    .map(|x| transform(x))         // SELECT
    .reduce(|a, b| aggregate(a, b)) // AGGREGATE
```

### Pattern 2: Join with Lookup
```rust
for item in items {
    let related = get_edges_from(db, &item.slug)
        .into_iter()
        .filter_map(|e| get_node(db, &e.target_slug))
        .collect();
    // Process (item, related) pair
}
```

### Pattern 3: Group By
```rust
let mut groups = HashMap::new();
for item in items {
    let key = extract_key(item);
    groups.entry(key).or_insert(Vec::new()).push(item);
}
// Now groups contains HashMap<key, Vec<item>>
```

## Performance Tips

1. **Bulk operations** - Fetch all needed data first, then process
2. **Limit early** - Apply filters before expensive operations
3. **Index usage** - Use `get_nodes_by_edge_type` instead of linear scan when possible
4. **Avoid allocations** - Use iterators instead of collecting to Vec

## Next Steps

1. **Study these examples** to understand composition patterns
2. **Apply to your use cases** - adapt patterns as needed
3. **Explore SekejapQL** - Query engine for ad-hoc queries (coming soon)
4. **Read the atoms documentation** - `src/atoms.rs`

## Feedback

Found a better pattern? Want to contribute an example?
- Open an issue or PR
- Include SQL equivalent for clarity
- Explain the composition strategy

---

**Sekejap** - Composable Multi-Modal Graph Database