import argparse
import json
import shlex
import sys

from . import SekejapDB


def looks_like_sql_mutation(line: str) -> bool:
    upper = line.lstrip().upper()
    return (
        upper.startswith("CREATE COLLECTION ")
        or upper.startswith("INSERT INTO ")
        or upper.startswith("RELATE ")
        or upper.startswith("UPDATE ")
        or upper.startswith("DELETE FROM ")
        or upper.startswith("UNRELATE ")
    )


def is_internal_command(line: str) -> bool:
    cmd = line.split(maxsplit=1)[0].lstrip(".\\").lower() if line.strip() else ""
    return cmd in {"help", "quit", "exit", "tables", "ls", "describe", "flush"}


def print_help() -> None:
    print("sekejap")
    print()
    print("Internal commands:")
    print("  .help")
    print("  .tables")
    print("  .describe [collection]")
    print("  .flush")
    print("  .quit")
    print()
    print("SQL examples:")
    print("  CREATE COLLECTION cases (id TEXT PRIMARY KEY, title TEXT) WITH (hash_index = [id]);")
    print("  INSERT INTO cases (id, title) VALUES ('c1', 'hello');")
    print("  SELECT id, title FROM cases WHERE id = 'c1';")
    print("  RELATE cases/c1 -> caused_by -> causes/wet_road_00001;")


def print_json(value) -> None:
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            print(json.dumps(parsed, indent=2))
            return
        except Exception:
            print(value)
            return
    print(json.dumps(value, indent=2))


def list_collections(db: SekejapDB) -> None:
    desc = json.loads(db.describe())
    for collection in desc.get("collections", []):
        schema = collection.get("schema", {})
        name = schema.get("name") or collection.get("name") or f"hash:{collection.get('hash')}"
        count = collection.get("count", 0)
        print(f"{name}\t{count}")


def handle_command(line: str, db: SekejapDB) -> bool:
    parts = shlex.split(line)
    if not parts:
        return True
    cmd = parts[0].lstrip(".\\").lower()
    if cmd == "help":
        print_help()
    elif cmd in {"quit", "exit"}:
        return False
    elif cmd in {"tables", "ls"}:
        list_collections(db)
    elif cmd == "describe":
        if len(parts) > 1:
            print_json(db.describe_collection(parts[1]))
        else:
            print_json(db.describe())
    elif cmd == "flush":
        db.flush()
        print("flushed")
    else:
        print(f"unknown command: {cmd}")
    return True


def handle_query(line: str, db: SekejapDB) -> None:
    line = line.strip().rstrip(";").strip()
    if not line:
        return
    if looks_like_sql_mutation(line):
        print_json(db.mutate(line))
        return
    if line.lower().startswith("count "):
        print(db.count(line[6:].strip()))
        return
    if line.lower().startswith("explain "):
        print_json(db.explain(line[8:].strip()))
        return
    print_json(db.query(line))


def repl(db: SekejapDB) -> int:
    print("sekejap")
    print("Connected. Type .help for help.")
    while True:
        try:
            line = input("sekejap> ")
        except EOFError:
            print()
            break
        except KeyboardInterrupt:
            print()
            continue
        line = line.strip()
        if not line:
            continue
        if is_internal_command(line):
            if not handle_command(line, db):
                break
        else:
            try:
                handle_query(line, db)
            except Exception as exc:
                print(f"error: {exc}", file=sys.stderr)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="sekejap", description="Sekejap SQL-first CLI")
    parser.add_argument("path", nargs="?", default="./sekejap_data")
    parser.add_argument("input", nargs="?")
    parser.add_argument("--capacity", type=int, default=1_000_000)
    args = parser.parse_args(argv)

    db = SekejapDB(args.path, capacity=args.capacity)
    try:
        if args.input and args.input.strip():
            line = args.input.strip()
            if is_internal_command(line):
                handle_command(line, db)
            else:
                handle_query(line, db)
            return 0
        return repl(db)
    finally:
        try:
            db.flush()
        except Exception:
            pass
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
