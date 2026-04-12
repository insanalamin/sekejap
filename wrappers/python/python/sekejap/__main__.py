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

from . import DB, Hit


# ── Helpers ───────────────────────────────────────────────────────────────────

def _print_hits(hits: list[Hit]) -> None:
    for hit in hits:
        if hit.payload:
            try:
                print(json.dumps(json.loads(hit.payload), indent=2))
            except Exception:
                print(hit.payload)
        else:
            print(hit.slug)
    n = len(hits)
    print(f"── {n} row{'s' if n != 1 else ''} ──")


def _run_sql(db: DB, sql: str) -> None:
    first = sql.split()[0].upper() if sql.split() else ""
    if first in ("SELECT", "MATCH"):
        try:
            _print_hits(db.query(sql))
        except Exception as e:
            print(f"error: {e}", file=sys.stderr)
    elif first in ("INSERT", "UPDATE", "DELETE", "CREATE", "DROP"):
        try:
            n = db.execute(sql)
            if n == 0:
                print("ok")
            elif n == 1:
                print("ok — 1 row affected")
            else:
                print(f"ok — {n} rows affected")
        except Exception as e:
            print(f"error: {e}", file=sys.stderr)
    elif first == "SHOW":
        try:
            _print_hits(db.show(sql))
        except Exception as e:
            print(f"error: {e}", file=sys.stderr)
    else:
        print(
            f"unknown statement — supported: SELECT MATCH SHOW INSERT UPDATE DELETE CREATE DROP",
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

SQL (end each statement with ;)
────────────────────────────────
SELECT * FROM collection [WHERE ...] [ORDER BY ...] [LIMIT n] [OFFSET n];
INSERT INTO collection (_key, field, ...) VALUES ('key', val, ...);
UPDATE collection SET field = val [WHERE ...];
DELETE FROM collection [WHERE ...];
CREATE TABLE collection (_key TEXT PRIMARY KEY, field TYPE);
MATCH (a:col)-[:rel]->(b:col) WHERE a._key = 'x' RETURN b;

Filters: =  !=  >  <  >=  <=  BETWEEN n AND n  IN (...)  LIKE  IS NULL
Spatial: ST_DWithin  ST_Contains  ST_Within  ST_Intersects
Vector:  WHERE VECTOR_NEAR(field, [f32, ...], k)
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
        names = db.collection_names()
        if names:
            for n in names:
                print(n)
        else:
            print("(no collections)")

    elif cmd == ".schema":
        target = parts[1].strip() if len(parts) > 1 else ""
        names = [target] if target else db.collection_names()
        found = False
        for name in names:
            ddl = db.schema_ddl(name)
            if ddl:
                print(f"{ddl};")
                found = True
            elif target:
                print(f"-- no CREATE TABLE for '{name}'")
                found = True
        if not found:
            print("(no schemas declared — use CREATE TABLE to add one)")

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
        arg = parts[1].strip() if len(parts) > 1 else ""
        if arg:
            types = db.edge_types_from_collection(arg)
            if types:
                for t in types:
                    print(t)
            else:
                print(f"(no outgoing edges from '{arg}')")
        else:
            schema = db.edge_schema()
            if schema:
                print(f"{'from':<25} {'type':<20} {'to'}")
                print("-" * 65)
                for from_col, kind, to_col in schema:
                    print(f"{from_col:<25} {kind:<20} {to_col}")
            else:
                print("(no edges)")

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
