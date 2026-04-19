"""
sekejap Python REPL / one-shot runner.

Usage::

    python -m sekejap                     # in-memory REPL
    python -m sekejap ./data              # persistent REPL
    python -m sekejap ./data "SELECT..."  # one-shot query, print, exit
"""

from __future__ import annotations

import argparse
import json
import sys
import time

from . import DB, Hit


# ── Helpers ───────────────────────────────────────────────────────────────────

_MAX_COL = 52
_MIN_COL = 4


def _fmt_duration(ns: int) -> str:
    if ns < 1_000:
        return f"{ns} ns"
    elif ns < 1_000_000:
        return f"{ns / 1_000:.2f} µs"
    elif ns < 1_000_000_000:
        return f"{ns / 1_000_000:.2f} ms"
    else:
        return f"{ns / 1_000_000_000:.3f} s"


def _cell(v: object) -> str:
    if isinstance(v, str):
        return v
    if v is None:
        return ""
    return json.dumps(v, ensure_ascii=False)


def _trunc(s: str, maxw: int) -> str:
    if len(s) <= maxw:
        return s
    return s[:maxw - 1] + "…"


def _print_table(hits: list[Hit], elapsed_ns: int) -> None:
    timing = _fmt_duration(elapsed_ns)
    n = len(hits)

    if not hits:
        print(f"(0 rows)  [{timing}]")
        return

    # Collect columns from first hit's payload
    columns: list[str] = []
    payloads: list[dict] = []
    for hit in hits:
        if hit.payload:
            try:
                d = json.loads(hit.payload)
                if isinstance(d, dict):
                    for k in d:
                        if k not in columns:
                            columns.append(k)
                    payloads.append(d)
                else:
                    payloads.append({})
            except Exception:
                payloads.append({})
        else:
            payloads.append({})

    if not columns:
        # Slug-only fallback
        slug_w = max((len(h.slug) for h in hits), default=5)
        slug_w = min(max(slug_w, len("_slug")), _MAX_COL)
        line = "─" * (slug_w + 2)
        print(f"┌{line}┐")
        print(f"│ {'_slug':<{slug_w}} │")
        print(f"├{line}┤")
        for hit in hits:
            print(f"│ {_trunc(hit.slug, _MAX_COL):<{slug_w}} │")
        print(f"└{line}┘")
        row_word = "row" if n == 1 else "rows"
        print(f"{n} {row_word}  [{timing}]")
        return

    # Column widths
    widths = [max(len(c), _MIN_COL) for c in columns]
    for row in payloads:
        for i, col in enumerate(columns):
            v = _cell(row.get(col))
            w = min(len(v), _MAX_COL)
            if w > widths[i]:
                widths[i] = w

    top = "┬".join("─" * (w + 2) for w in widths)
    mid = "┼".join("─" * (w + 2) for w in widths)
    bot = "┴".join("─" * (w + 2) for w in widths)

    print(f"┌{top}┐")
    hdr = "│".join(f" {c:<{w}} " for c, w in zip(columns, widths))
    print(f"│{hdr}│")
    print(f"├{mid}┤")

    for row in payloads:
        cells = "│".join(
            f" {_trunc(_cell(row.get(col)), _MAX_COL):<{w}} "
            for col, w in zip(columns, widths)
        )
        print(f"│{cells}│")

    print(f"└{bot}┘")
    row_word = "row" if n == 1 else "rows"
    print(f"{n} {row_word}  [{timing}]")


def _run_sql(db: DB, sql: str) -> None:
    first = sql.split()[0].upper() if sql.split() else ""
    t0 = time.perf_counter_ns()
    if first in ("SELECT", "MATCH"):
        try:
            hits = db.query(sql)
            elapsed = time.perf_counter_ns() - t0
            _print_table(hits, elapsed)
        except Exception as e:
            print(f"error: {e}", file=sys.stderr)
    elif first in ("INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "REINDEX"):
        try:
            n = db.execute(sql)
            elapsed = time.perf_counter_ns() - t0
            timing = _fmt_duration(elapsed)
            if n == 0:
                print(f"ok  [{timing}]")
            elif n == 1:
                print(f"ok — 1 row affected  [{timing}]")
            else:
                print(f"ok — {n} rows affected  [{timing}]")
        except Exception as e:
            print(f"error: {e}", file=sys.stderr)
    elif first == "SHOW":
        try:
            hits = db.show(sql)
            elapsed = time.perf_counter_ns() - t0
            _print_table(hits, elapsed)
        except Exception as e:
            print(f"error: {e}", file=sys.stderr)
    else:
        print(
            "unknown statement — supported: SELECT MATCH SHOW INSERT UPDATE DELETE"
            " CREATE DROP ALTER REINDEX",
            file=sys.stderr,
        )


def _run_dot(db: DB, label: list[str], line: str) -> bool:
    """Handle a .command. Returns False if the user wants to quit."""
    parts = line.split(maxsplit=1)
    cmd = parts[0].lower()

    if cmd in (".quit", ".q", ".exit"):
        return False

    elif cmd == ".help":
        print("""
sekejap dot commands
────────────────────
.open <path>        open (or create) a persistent DB
.tables             list all collections
.schema [name]      show CREATE TABLE DDL
.compact            flush snapshot, truncate WAL
.stats              show node / edge / collection counts
.edges              show full graph schema
.edges <col>        show distinct edge types leaving a collection
.help               show this help
.quit / .q / .exit  exit

SQL — standard (end each statement with ;)
──────────────────────────────────────────
SELECT * FROM collection [WHERE ...] [ORDER BY ...] [LIMIT n] [OFFSET n];
INSERT INTO collection (_key, field, ...) VALUES ('key', val, ...);
UPDATE collection SET field = val [WHERE ...];
DELETE FROM collection [WHERE ...];
CREATE TABLE collection (_key TEXT PRIMARY KEY, field TYPE);
ALTER TABLE collection ADD COLUMN field TYPE;
ALTER TABLE collection DROP COLUMN field;
ALTER TABLE collection RENAME COLUMN old TO new;
ALTER TABLE collection RENAME TO new_name;
REINDEX ON collection USING method (field);

SQL — graph aggregate — RETURN form
────────────────────────────────────
MATCH (a:col)-[r:rel]->(b:col)
    [WHERE a._key = 'val']
    RETURN expr AS alias [, ...]
    [GROUP BY col] [ORDER BY col [DESC]] [LIMIT n];

SQL — graph aggregate — SELECT FROM MATCH form
───────────────────────────────────────────────
SELECT expr AS alias [, ...]
FROM MATCH (a:col)-[r:rel]->(b:col)
    [WHERE a._key = 'val']
    [GROUP BY col] [ORDER BY col [DESC]] [LIMIT n];

Return expressions
──────────────────
var._key, var.field         node / edge field
COUNT(var)                  number of paths
SUM(var.field)              numeric sum
AVG(var.field)              numeric mean
MIN(var.field)              minimum
MAX(var.field)              maximum
r._depth                    hop depth of edge bind
r._path_keys                JSON array of slug keys along path
r._path_strength            JSON array of edge strengths along path
PATH_PRODUCT(r._path_strength)   product of all values in array
PATH_AVG / PATH_SUM / PATH_MIN / PATH_MAX(r.field)
PATH_FIRST(r._path_keys)    first element of path array
PATH_LAST(r._path_keys)     last element of path array
JSON_ARRAY_LENGTH(r._path_keys)  length of a path array
CASE WHEN var.field = val THEN lit ELSE lit END
NOW()                       current Unix timestamp (integer)
AGE_DAYS(var.field)         days since epoch field
AGE_HOURS(var.field)        hours since epoch field

Shortest path
─────────────
MATCH SHORTEST (a)-[r*]->(b)
    WHERE a._key = 'col/key1' AND b._key = 'col/key2';

Introspection
─────────────
SHOW TABLES;
SHOW EDGES;
SHOW EDGES FROM collection;
SHOW EDGES FROM col1 TO col2;
SHOW <collection>;

Filters: =  !=  >  <  >=  <=  BETWEEN n AND n  IN (...)  LIKE  IS NULL
Spatial: ST_DWithin  ST_Contains  ST_Within  ST_Intersects  ST_DISTANCE_KM(a,b)
Vector:  WHERE VECTOR_NEAR(field, [f32, ...], k)
         VECTOR_COSINE(a,b)  VECTOR_L2(a,b)  VECTOR_DOT(a,b)  VECTOR_L1(a,b)
         a <=> b  a <-> b  a <#> b  a <+> b
""")

    elif cmd == ".open":
        path = parts[1].strip() if len(parts) > 1 else ""
        if not path:
            print("usage: .open <path>", file=sys.stderr)
        else:
            try:
                new_db = DB(path)
                # Can't rebind outer db (PyO3 class) — swap internals via close+reopen trick.
                # For simplicity, inform the user they need to restart.
                print(f"note: .open not supported after startup in Python REPL — "
                      f"restart with: python -m sekejap {path}")
            except Exception as e:
                print(f"error: {e}", file=sys.stderr)

    elif cmd == ".tables":
        hits = db.show("SHOW TABLES")
        if hits:
            print(f"{'name':<30} {'count'}")
            print("-" * 38)
            for h in hits:
                if h.payload:
                    import json as _json
                    d = _json.loads(h.payload)
                    print(f"{d.get('name', ''):<30} {d.get('count', 0)}")
        else:
            print("(no collections)")

    elif cmd == ".schema":
        target = parts[1].strip() if len(parts) > 1 else ""
        names = [target] if target else db.collection_names()
        found = False
        for name in names:
            hits = db.show(f"SHOW {name}")
            if hits:
                import json as _json
                print(f"-- {name}")
                print(f"{'field':<25} {'type':<15} {'source'}")
                print("-" * 50)
                for h in hits:
                    if h.payload:
                        d = _json.loads(h.payload)
                        pk = " (PK)" if d.get("primary_key") else ""
                        print(f"{d.get('field',''):<25} {d.get('type',''):<15} {d.get('source','')}{pk}")
                print()
                found = True
            elif target:
                print(f"-- no data found for '{name}'")
                found = True
        if not found:
            print("(no collections found)")

    elif cmd == ".compact":
        try:
            db.compact()
            print("compacted")
        except Exception as e:
            print(f"error: {e}", file=sys.stderr)

    elif cmd == ".stats":
        print(f"nodes       : {db.node_count()}")
        print(f"edges       : {db.edge_count()}")
        print(f"collections : {len(db.collection_names())}")

    elif cmd == ".edges":
        import json as _json
        arg = parts[1].strip() if len(parts) > 1 else ""
        sql = f"SHOW EDGES FROM {arg}" if arg else "SHOW EDGES"
        hits = db.show(sql)
        if not hits:
            print("(no edges)")
        elif arg:
            print(f"{'type':<20} {'count'}")
            print("-" * 28)
            for h in hits:
                if h.payload:
                    d = _json.loads(h.payload)
                    print(f"{d.get('type',''):<20} {d.get('count', 0)}")
        else:
            print(f"{'from':<25} {'type':<20} {'to':<25} {'count'}")
            print("-" * 78)
            for h in hits:
                if h.payload:
                    d = _json.loads(h.payload)
                    print(f"{d.get('from',''):<25} {d.get('type',''):<20} {d.get('to',''):<25} {d.get('count',0)}")

    else:
        print(f"unknown command: {cmd}  (try .help)", file=sys.stderr)

    return True


# ── Script runner ─────────────────────────────────────────────────────────────

def _run_script(db: DB, script: str) -> None:
    label: list[str] = []
    buf = ""
    in_str = False
    str_char = "\0"

    for line in script.splitlines():
        trimmed = line.strip()

        if not in_str and not buf.strip() and trimmed.startswith("."):
            if not _run_dot(db, label, trimmed):
                return
            continue

        if not in_str and (not trimmed or trimmed.startswith("--")):
            continue

        for ch in trimmed:
            if not in_str and ch in ("'", '"'):
                in_str = True
                str_char = ch
                buf += ch
            elif in_str and ch == str_char:
                in_str = False
                buf += ch
            elif ch == ";" and not in_str:
                stmt = buf.strip()
                buf = ""
                if stmt:
                    _run_sql(db, stmt)
            else:
                buf += ch

        if buf.strip():
            buf += " "

    stmt = buf.strip()
    if stmt:
        _run_sql(db, stmt)


# ── REPL ──────────────────────────────────────────────────────────────────────

def _repl(db: DB, label: str) -> None:
    try:
        import readline  # noqa: F401 — enables line editing on Unix
    except ImportError:
        pass

    print(f"sekejap  —  {label}")
    print("type .help for commands, .quit to exit\n")

    buf = ""
    label_list: list[str] = [label]

    while True:
        prompt = "sekejap> " if not buf.strip() else "      ...> "
        try:
            line = input(prompt)
        except EOFError:
            print()
            break
        except KeyboardInterrupt:
            print()
            buf = ""
            continue

        trimmed = line.strip()
        if not trimmed:
            continue

        if trimmed.startswith("."):
            buf = ""
            if not _run_dot(db, label_list, trimmed):
                break
            continue

        if buf:
            buf += " "
        buf += trimmed

        if buf.rstrip().endswith(";"):
            sql = buf.rstrip().rstrip(";").strip()
            buf = ""
            if sql:
                _run_sql(db, sql)


# ── Entry point ───────────────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m sekejap",
        description="sekejap embedded DB — REPL and one-shot runner",
    )
    parser.add_argument("path", nargs="?", default=None,
                        help="DB directory path (omit for in-memory)")
    parser.add_argument("sql", nargs="?", default=None,
                        help="SQL statement or script to run and exit")
    args = parser.parse_args(argv)

    db = DB(args.path)
    label = args.path or ":memory:"

    try:
        if args.sql:
            _run_script(db, args.sql)
            return 0

        import sys as _sys
        if not _sys.stdin.isatty():
            script = _sys.stdin.read()
            _run_script(db, script)
            return 0

        _repl(db, label)
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
