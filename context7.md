# Sekejap

Graph-first, embedded multimodel database engine for Rust and Python.

Core retrieval domains:
- graph
- exact and vague time
- spatial
- vector
- full-text

Recommended common-user path:
- SQL first

Lower-level control remains available through the Atomic / fluent API.

## Main Docs

- [README.md](README.md): Main entry point, installation, Rust/Python quick starts
- [docs/user-guide.md](docs/user-guide.md): Practical usage guide
- [docs/sekejap-sql.md](docs/sekejap-sql.md): Short SQL reference
- [docs/python-api.md](docs/python-api.md): Proposed Pythonic API with `sekejap.open(...)` and `db.df.*`

## Core Source

- [src/db.rs](src/db.rs): `SekejapDB` core, query/mutate/flush/describe, schema persistence across reopen
- [src/set.rs](src/set.rs): Query pipeline execution
- [src/sql/](src/sql): SQL parser, lowering, executor
- [src/sekejapql.rs](src/sekejapql.rs): SekejapQL parser/compiler
- [src/types.rs](src/types.rs): Core types, steps, outcomes, traces, collection schema
- [src/index/](src/index): Hash, range, and temporal indexes
- [src/hnsw/](src/hnsw): Vector index
- [src/geometry.rs](src/geometry.rs): Spatial and geometry logic
- [src/fulltext/](src/fulltext): Full-text adapters

## Wrappers

- [wrappers/python/src/lib.rs](wrappers/python/src/lib.rs): Python PyO3 bindings
- [wrappers/python/python/sekejap/__main__.py](wrappers/python/python/sekejap/__main__.py): Python CLI entry point
- [wrappers/python/pyproject.toml](wrappers/python/pyproject.toml): Python packaging and console script wiring
- [wrappers/browser/src/lib.rs](wrappers/browser/src/lib.rs): Browser/WASM bindings

## CLI

- [skcli/src/main.rs](skcli/src/main.rs): Rust CLI source
- Rust CLI package name: `sekejap-cli`
- Rust CLI binary command: `sekejap`
- Python CLI command: `sekejap`

## Packaging

- Rust library: `cargo add sekejap`
- Rust CLI: `cargo install sekejap-cli`
- Python library and CLI: `pip install sekejap`

## Release Workflow

- [.github/workflows/release.yml](.github/workflows/release.yml)

Release workflow now:
- publishes `sekejap` to crates.io
- publishes `sekejap-cli` to crates.io
- builds Python wheels for Linux and Windows
- builds Python sdist
- publishes Python distributions to PyPI

## Benchmarks

Benchmarks are organized under:
- `sekejap-benchmark/cases`
- `sekejap-benchmark/techniques`

Implemented case suites:
- causal investigation
- memory time space
- research network

Technique suites include:
- graph traversal
- parser
- spatial ops
- vector ops
- text ops

## Current Strengths

- anchored graph traversal
- exact-time filtering
- vector retrieval
- point-centric spatial filtering

## Current Weakest Area

- vague time and vague-time-heavy hybrid planning
