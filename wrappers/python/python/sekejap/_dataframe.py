"""
db.df — pandas/polars dataframe integration for sekejap.

pandas is optional. Import only when actually used so the core DB
works without any dataframe dependency installed.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pandas as pd


def _require_pandas():
    try:
        import pandas as pd
        return pd
    except ImportError:
        raise ImportError(
            "pandas is required for db.df — install it with: pip install pandas"
        )


class DataFrameAccessor:
    """
    Dataframe integration namespace.  Access via ``db.df``.

    All methods are pandas-optional — pandas is only imported when a
    method that needs it is actually called.
    """

    def __init__(self, db):
        self._db = db

    # ── Query → DataFrame ─────────────────────────────────────────────────────

    def query(self, sql: str, *, index_col: str | None = None) -> "pd.DataFrame":
        """
        Run a SQL query and return a ``pandas.DataFrame``.

        Each result row is the parsed JSON payload of a matched node.
        If a node has no payload, a row with only ``_slug`` is emitted.

        Args:
            sql:        Any SELECT, MATCH … RETURN, or SELECT … FROM MATCH query.
            index_col:  If provided, set this column as the DataFrame index.

        Supported query forms::

            # Standard SELECT
            db.df.query("SELECT * FROM venues LIMIT 100")

            # MATCH aggregate — RETURN form
            db.df.query(\"\"\"
                MATCH (a:bands)-[r:played_at]->(b:venues)
                RETURN b._key AS venue, COUNT(a) AS plays
                GROUP BY b._key ORDER BY plays DESC
            \"\"\")

            # SELECT FROM MATCH — SQL-first form (identical result)
            db.df.query(\"\"\"
                SELECT b._key AS venue, COUNT(a) AS plays
                FROM MATCH (a:bands)-[r:played_at]->(b:venues)
                GROUP BY b._key ORDER BY plays DESC
            \"\"\")

            # PATH_* aggregates
            db.df.query(\"\"\"
                MATCH (a:venues)-[r:route_to*1..3]->(b:venues)
                WHERE a._key = 'melbourne_cbd'
                RETURN b._key AS dest, PATH_PRODUCT(r._path_strength) AS reliability
            \"\"\")

            # Vector search
            db.df.query(
                "SELECT * FROM venues WHERE VECTOR_NEAR(embedding, [...], 20)",
                index_col="_key",
            )
        """
        pd = _require_pandas()
        hits = self._db.query(sql)
        rows = []
        for hit in hits:
            if hit.payload:
                row = json.loads(hit.payload)
            else:
                row = {}
            row.setdefault("_slug", hit.slug)
            rows.append(row)
        df = pd.DataFrame(rows)
        if index_col and index_col in df.columns:
            df = df.set_index(index_col)
        return df

    # ── Load nodes from DataFrame ─────────────────────────────────────────────

    def load_nodes(
        self,
        df: "pd.DataFrame",
        collection: str,
        *,
        id_col: str = "_key",
        mapping: dict[str, str] | None = None,
        batch_size: int = 1000,
    ) -> int:
        """
        Load rows from a pandas DataFrame into a sekejap collection.

        The collection schema (if declared via ``CREATE TABLE``) is the
        source of truth for field names.  ``mapping`` renames DataFrame
        columns to schema field names before insertion.

        Args:
            df:          Source DataFrame.
            collection:  Target collection name (e.g. ``"researchers"``).
            id_col:      Column used as ``_key``.  Defaults to ``"_key"``.
            mapping:     ``{df_column: schema_field}`` rename map.
            batch_size:  Rows per ``put_many`` call (unused — kept for API
                         compatibility; rows are inserted individually for now).

        Returns:
            Number of nodes inserted.

        Example::

            db.df.load_nodes(df, "researchers", id_col="researcher_id")

            db.df.load_nodes(
                df, "researchers",
                id_col="researcher_id",
                mapping={"full_name": "name", "joined_at": "created_at"},
            )
        """
        mapping = mapping or {}
        count = 0
        for _, row in df.iterrows():
            record: dict[str, Any] = {}
            for col, val in row.items():
                field = mapping.get(str(col), str(col))
                record[field] = _coerce(val)

            key_field = mapping.get(id_col, id_col)
            raw_key = record.get(key_field) or record.get(id_col)
            if raw_key is None:
                continue
            key = str(raw_key)

            record["_collection"] = collection
            record["_key"] = key
            slug = f"{collection}/{key}"

            self._db.put(slug, json.dumps(record))
            count += 1
        return count

    # ── Load edges from DataFrame ─────────────────────────────────────────────

    def load_edges(
        self,
        df: "pd.DataFrame",
        *,
        source_col: str,
        target_col: str,
        edge_type: str | None = None,
        edge_type_col: str | None = None,
        source_collection: str | None = None,
        target_collection: str | None = None,
        weight_col: str | None = None,
        meta_col: str | None = None,
        batch_size: int = 1000,
    ) -> int:
        """
        Load edge rows from a pandas DataFrame.

        Either ``edge_type`` (fixed for all rows) or ``edge_type_col``
        (per-row from a DataFrame column) must be provided.

        Args:
            df:                   Source DataFrame.
            source_col:           Column with source node key.
            target_col:           Column with target node key.
            edge_type:            Fixed edge type for all rows.
            edge_type_col:        Column name for per-row edge type.
            source_collection:    Prefix slugs with this collection name.
            target_collection:    Prefix slugs with this collection name.
            weight_col:           Column for edge weight (default 1.0).
            meta_col:             Column with JSON metadata string.
            batch_size:           Kept for API compatibility.

        Returns:
            Number of edges inserted.

        Example::

            # fixed edge type
            db.df.load_edges(
                df_edges,
                source_col="researcher_id",
                target_col="topic_id",
                edge_type="works_on",
                source_collection="researchers",
                target_collection="topics",
                weight_col="weight",
            )

            # per-row edge type
            db.df.load_edges(
                df_edges,
                source_col="source",
                target_col="target",
                edge_type_col="relation",
                weight_col="weight",
            )
        """
        if edge_type is None and edge_type_col is None:
            raise ValueError("provide either edge_type or edge_type_col")

        count = 0
        for _, row in df.iterrows():
            src = str(row[source_col])
            tgt = str(row[target_col])

            if source_collection and "/" not in src:
                src = f"{source_collection}/{src}"
            if target_collection and "/" not in tgt:
                tgt = f"{target_collection}/{tgt}"

            etype = edge_type if edge_type else str(row[edge_type_col])
            weight = float(row[weight_col]) if weight_col and weight_col in row else 1.0

            if meta_col and meta_col in row and row[meta_col]:
                self._db.link_meta(src, tgt, etype, weight, str(row[meta_col]))
            else:
                self._db.link(src, tgt, etype, weight)

            count += 1
        return count

    # ── Create collection from field spec ─────────────────────────────────────

    def create_collection(
        self,
        name: str,
        fields: dict[str, str],
        *,
        hash_index: list[str] | None = None,
        range_index: list[str] | None = None,
        spatial_index: list[str] | None = None,
        vector_index: list[str] | None = None,
        fulltext_index: list[str] | None = None,
    ) -> None:
        """
        Create a typed collection from a Python field spec.

        Compiles to ``CREATE TABLE ... WITH (...)`` SQL.

        Args:
            name:            Collection name.
            fields:          ``{field_name: sql_type}`` dict.
            hash_index:      Fields to hash-index.
            range_index:     Fields to range-index (B-tree).
            spatial_index:   Fields to spatial-index.
            vector_index:    Fields to vector-index.
            fulltext_index:  Fields to full-text-index.

        Example::

            db.df.create_collection(
                "researchers",
                fields={
                    "_key": "TEXT PRIMARY KEY",
                    "name": "TEXT",
                    "campus": "TEXT",
                    "embedding": "VECTOR(128)",
                    "geometry": "GEOMETRY",
                },
                hash_index=["_key", "campus"],
                vector_index=["embedding"],
                spatial_index=["geometry"],
            )
        """
        field_defs = ", ".join(f"{k} {v}" for k, v in fields.items())
        opts = []
        if hash_index:
            cols = ", ".join(hash_index)
            opts.append(f"hash_index = [{cols}]")
        if range_index:
            cols = ", ".join(range_index)
            opts.append(f"range_index = [{cols}]")
        if spatial_index:
            cols = ", ".join(spatial_index)
            opts.append(f"spatial_index = [{cols}]")
        if vector_index:
            cols = ", ".join(vector_index)
            opts.append(f"vector_index = [{cols}]")
        if fulltext_index:
            cols = ", ".join(fulltext_index)
            opts.append(f"fulltext_index = [{cols}]")

        sql = f"CREATE TABLE {name} ({field_defs})"
        if opts:
            sql += " WITH (" + ", ".join(opts) + ")"

        self._db.execute(sql)


# ── helpers ───────────────────────────────────────────────────────────────────

def _coerce(val: Any) -> Any:
    """Convert pandas/numpy scalar types to plain Python for JSON."""
    try:
        import numpy as np
        if isinstance(val, (np.integer,)):
            return int(val)
        if isinstance(val, (np.floating,)):
            return float(val)
        if isinstance(val, np.ndarray):
            return val.tolist()
        if isinstance(val, np.bool_):
            return bool(val)
    except ImportError:
        pass
    try:
        import pandas as pd
        if pd.isna(val):
            return None
    except (ImportError, TypeError, ValueError):
        pass
    return val
