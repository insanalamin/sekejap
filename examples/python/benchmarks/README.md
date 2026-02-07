# Benchmark Examples (Python)

This directory contains Python benchmark examples comparing Sekejap-DB performance against SQLite.

## Examples

### [write_benchmark.py](./write_benchmark.py)
Bulk write performance benchmarks:
- **Single writes** - Individual node insertions
- **Batch writes** - Bulk insertions with WriteOptions
- **Publish modes** - Tier 1 only vs Tier 1+2

**Run:**
```bash
python examples/python/benchmarks/write_benchmark.py
```

### [sqlite_vs_sekejap.py](./sqlite_vs_sekejap.py)
Comprehensive comparison benchmarks:
- **Insert operations** - Single vs bulk inserts
- **Point lookup** - Read by ID
- **Filtering** - Query by type/department
- **Traversal** - Multi-hop graph traversal
- **Aggregation** - COUNT, SUM, AVG operations
- **Join operations** - Multi-table queries

**Run:**
```bash
python examples/python/benchmarks/sqlite_vs_sekejap.py
```

## Company Hierarchy Dataset

The benchmarks use a company hierarchy dataset:
- ~1000 nodes across 6 levels (Company → Department → Team → Project → Task → Employee)
- ~50 companies with varying sizes
- Parent-child relationships for traversal testing

## Performance Tips for Python

1. **Use WriteOptions** - `publish_now=True` for immediate reads
2. **Batch operations** - Group writes together
3. **Context manager** - Automatic cleanup
4. **Reuse DB connection** - Keep DB open during bulk operations

## Results Format

```
Operation              | SQLite (ms) | Sekejap (ms) | Winner
----------------------+-------------+--------------+--------
Insert Companies      |      12.34 |        8.21  | Sekejap (1.5x)
Insert Nodes (1000)   |     234.56 |       45.67  | Sekejap (5.1x)
Point Lookup (100x)   |       5.23 |        2.14  | Sekejap (2.4x)
...
```

## Comparison Notes

- **Sekejap** excels at graph operations (traversal, joins)
- **SQLite** may be faster for simple indexed lookups
- **Sekejap** provides ACID compliance and MVCC
- **Sekejap** supports multi-modal data (vectors, geo, etc.)

---

**Sekejap** - Composable Multi-Modal Graph Database
