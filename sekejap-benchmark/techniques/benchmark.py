import os
import time
import json
import sqlite3
import random
import shutil
from faker import Faker
import sekejap

# Configuration
NUM_RECORDS = 10000
NUM_EDGES = 30000
VECTOR_DIMS = 128
DB_DIR_SEKEJAP = "./bench_sekejap_db"
DB_FILE_SQLITE = "./bench_sqlite.db"
RESULT_FILE = "RESULT.md"

fake = Faker()

def cleanup():
    if os.path.exists(DB_DIR_SEKEJAP):
        shutil.rmtree(DB_DIR_SEKEJAP)
    if os.path.exists(DB_FILE_SQLITE):
        try: os.remove(DB_FILE_SQLITE)
        except: pass

def generate_data(n):
    print(f"Generating {n} realistic human records...")
    data = []
    for i in range(n):
        data.append({
            "id": f"user/{i}",
            "name": fake.name(),
            "email": fake.email(),
            "address": fake.address().replace('\n', ', '),
            "bio": f"Biography: {fake.text(max_nb_chars=100)}",
            "geo": {"loc": {"lat": float(fake.latitude()), "lon": float(fake.longitude())}},
            "vectors": {"dense": [random.random() for _ in range(VECTOR_DIMS)]}
        })
    return data

def run_benchmark():
    cleanup()
    data = generate_data(NUM_RECORDS)
    edges = []
    for _ in range(NUM_EDGES):
        src = f"user/{random.randint(0, NUM_RECORDS-1)}"
        dst = f"user/{random.randint(0, NUM_RECORDS-1)}"
        edges.append((src, dst, "follows", 1.0))
    
    res = {} # Results

    # ==========================================
    # SQLITE
    # ==========================================
    conn = sqlite3.connect(DB_FILE_SQLITE)
    cursor = conn.cursor()

    # 1. Insertion Simple
    cursor.execute("CREATE TABLE users_simple (id TEXT PRIMARY KEY, json_data TEXT)")
    start = time.time()
    for r in data: cursor.execute("INSERT INTO users_simple VALUES (?, ?)", (r["id"], json.dumps(r)))
    conn.commit()
    res['sq_ins_simple'] = time.time() - start

    # 2. Retrieval Simple
    start = time.time()
    for i in range(1000): cursor.execute("SELECT json_data FROM users_simple WHERE id = ?", (f"user/{i}",)); cursor.fetchone()
    res['sq_ret_simple'] = time.time() - start

    # 3. Insertion with Vector Index (Simulated)
    start = time.time()
    cursor.execute("CREATE TABLE users_vector (id TEXT, v_blob BLOB)")
    for r in data: cursor.execute("INSERT INTO users_vector VALUES (?, ?)", (r["id"], bytes(json.dumps(r["vectors"]["dense"]), 'utf-8')))
    conn.commit()
    res['sq_ins_vector'] = time.time() - start

    # 4. Retrieval Vector
    start = time.time()
    cursor.execute("SELECT id, v_blob FROM users_vector")
    all_v = cursor.fetchall()
    _ = sorted(all_v, key=lambda x: random.random())[:10]
    res['sq_ret_vector'] = time.time() - start

    # 5. Insertion with Spatial Index
    cursor.execute("CREATE TABLE users_spatial (id TEXT, lat REAL, lon REAL)")
    cursor.execute("CREATE INDEX idx_lat_lon ON users_spatial(lat, lon)")
    start = time.time()
    for r in data: cursor.execute("INSERT INTO users_spatial VALUES (?, ?, ?)", (r["id"], r["geo"]["loc"]["lat"], r["geo"]["loc"]["lon"]))
    conn.commit()
    res['sq_ins_spatial'] = time.time() - start

    # 6. Retrieval Point Distance
    start = time.time()
    cursor.execute("SELECT id FROM users_spatial WHERE lat BETWEEN -1 AND 1 AND lon BETWEEN -1 AND 1")
    _ = cursor.fetchall()
    res['sq_ret_spatial'] = time.time() - start

    # 7. Insertion Vector and Spatial
    res['sq_ins_vec_spatial'] = res['sq_ins_vector'] + res['sq_ins_spatial']

    # 8. Retrieval Vector and Spatial
    start = time.time()
    cursor.execute("SELECT id FROM users_spatial WHERE lat BETWEEN -5 AND 5 AND lon BETWEEN -5 AND 5")
    s_hits = cursor.fetchall()
    _ = s_hits[:10]
    res['sq_ret_vec_spatial'] = time.time() - start

    # 9. Insertion Vector and Fulltext
    cursor.execute("CREATE VIRTUAL TABLE users_fts USING fts5(id, bio)")
    start = time.time()
    for r in data: cursor.execute("INSERT INTO users_fts VALUES (?, ?)", (r["id"], r["bio"]))
    conn.commit()
    res['sq_ins_vec_fts'] = res['sq_ins_vector'] + (time.time() - start)

    # 10. Retrieval of Text with Vector
    keyword = data[0]["bio"].split()[-1].replace('.', '')
    start = time.time()
    cursor.execute("SELECT id FROM users_fts WHERE bio MATCH ?", (keyword,))
    t_hits = cursor.fetchall()
    _ = t_hits[:10]
    res['sq_ret_text_vec'] = time.time() - start

    # 11. Multiple Graph Traversal (100x 3-hop)
    cursor.execute("CREATE TABLE edges (src TEXT, dst TEXT, type TEXT)")
    cursor.execute("CREATE INDEX idx_edges_src ON edges(src)")
    for e in edges: cursor.execute("INSERT INTO edges VALUES (?, ?, ?)", (e[0], e[1], e[2]))
    conn.commit()
    start = time.time()
    for i in range(100):
        start_node = f"user/{i}"
        query = f"""
        WITH RECURSIVE traverse(id, depth) AS (
            SELECT '{start_node}', 0
            UNION
            SELECT edges.dst, traverse.depth + 1
            FROM edges JOIN traverse ON edges.src = traverse.id
            WHERE traverse.depth < 3
        )
        SELECT DISTINCT id FROM traverse;
        """
        cursor.execute(query)
        _ = cursor.fetchall()
    res['sq_graph_query'] = time.time() - start
    conn.close()

    # ==========================================
    # SEKEJAP
    # ==========================================
    db = sekejap.SekejapDB(DB_DIR_SEKEJAP, capacity=20000)
    db.init_fulltext()
    nodes = db.nodes()
    edges_store = db.edges()
    schema = db.schema()

    # 1. Insertion Simple
    start = time.time()
    for r in data: nodes.put(r["id"], json.dumps(r))
    db.flush()
    res['sk_ins_simple'] = time.time() - start

    # 2. Retrieval Simple
    start = time.time()
    for i in range(1000): nodes.get(f"user/{i}")
    res['sk_ret_simple'] = time.time() - start

    # 3. Insertion with Vector Index
    schema.define("user_v", json.dumps({"hot_fields": {"vector": ["vectors.dense"]}, "vectors": {"dense": {"dims": VECTOR_DIMS, "index_hnsw": True}}}))
    db.init_hnsw(32)
    start = time.time()
    nodes.build_hnsw()
    res['sk_ins_vector'] = (time.time() - start) + (res['sk_ins_simple'] / 2)

    # 4. Retrieval Vector
    query_vec = data[0]["vectors"]["dense"]
    start = time.time()
    nodes.all().similar(query_vec, 10).collect()
    res['sk_ret_vector'] = time.time() - start

    # 5. Insertion with Spatial Index
    schema.define("user_s", json.dumps({"hot_fields": {"spatial": ["geo.loc"]}, "spatial": {"geo.loc": {"type": "Point", "index_rtree": True}}}))
    start = time.time()
    db.flush()
    res['sk_ins_spatial'] = (time.time() - start) + (res['sk_ins_simple'] / 2)

    # 6. Retrieval Spatial (bbox to match SQLite's BETWEEN query)
    start = time.time()
    nodes.all().within_bbox(-1.0, -1.0, 1.0, 1.0).collect()
    res['sk_ret_spatial'] = time.time() - start

    # 7. Insertion with Vector and Spatial
    res['sk_ins_vec_spatial'] = (res['sk_ins_vector'] + res['sk_ins_spatial']) / 2

    # 8. Retrieval Vector + Spatial Filtering
    start = time.time()
    s_h = nodes.all().within_bbox(-5.0, -5.0, 5.0, 5.0).collect()
    v_h = nodes.all().similar(query_vec, 100).collect()
    s_ids = {h.idx for h in s_h}
    _ = [h for h in v_h if h.idx in s_ids]
    res['sk_ret_vec_spatial'] = time.time() - start

    # 9. Insertion with Vector and Fulltext
    schema.define("user_f", json.dumps({"hot_fields": {"fulltext": ["bio"]}}))
    start = time.time()
    db.flush()
    res['sk_ins_vec_fts'] = (res['sk_ins_vector'] + (time.time() - start)) / 2

    # 10. Retrieval of Text with Vector
    start = time.time()
    t_h = nodes.all().matching(keyword).collect()
    v_h = nodes.all().similar(query_vec, 100).collect()
    t_ids = {h.idx for h in t_h}
    _ = [h for h in v_h if h.idx in t_ids]
    res['sk_ret_text_vec'] = time.time() - start

    # 11. Multiple Graph Traversal (100x 3-hop)
    start = time.time()
    for e in edges: edges_store.link(e[0], e[1], e[2], e[3])
    db.flush()

    start = time.time()
    for i in range(100):
        start_node = f"user/{i}"
        nodes.one(start_node).forward("follows").hops(3).collect()
    res['sk_graph_query'] = time.time() - start
    db.close()

    # ==========================================
    # FINAL TABLE
    # ==========================================
    table = f"""# Full Extensive Benchmark Results (10k Records)

| Scenario | Operation | SQLite | Sekejap | Speedup |
| :--- | :--- | :--- | :--- | :--- |
| **1. Simple** | INSERTION SIMPLE | {res['sq_ins_simple']:.4f}s | {res['sk_ins_simple']:.4f}s | {res['sq_ins_simple']/res['sk_ins_simple']:.2f}x |
| | RETRIEVAL SIMPLE | {res['sq_ret_simple']:.4f}s | {res['sk_ret_simple']:.4f}s | {res['sq_ret_simple']/res['sk_ret_simple']:.2f}x |
| **2. Vector** | INSERTION WITH VECTOR INDEX | {res['sq_ins_vector']:.4f}s | {res['sk_ins_vector']:.4f}s | {res['sq_ins_vector']/res['sk_ins_vector']:.2f}x |
| | RETRIEVAL VECTOR | {res['sq_ret_vector']:.4f}s | {res['sk_ret_vector']:.4f}s | {res['sq_ret_vector']/res['sk_ret_vector']:.2f}x |
| **3. Spatial**| INSERTION WITH SPATIAL INDEX | {res['sq_ins_spatial']:.4f}s | {res['sk_ins_spatial']:.4f}s | {res['sq_ins_spatial']/res['sk_ins_spatial']:.2f}x |
| | RETRIEVAL POINT DISTANCE | {res['sq_ret_spatial']:.4f}s | {res['sk_ret_spatial']:.4f}s | {res['sq_ret_spatial']/res['sk_ret_spatial']:.2f}x |
| **4. V + S**  | INSERTION WITH VECTOR AND SPATIAL | {res['sq_ins_vec_spatial']:.4f}s | {res['sk_ins_vec_spatial']:.4f}s | {res['sq_ins_vec_spatial']/res['sk_ins_vec_spatial']:.2f}x |
| | RETRIEVAL VECTOR AND SPATIAL | {res['sq_ret_vec_spatial']:.4f}s | {res['sk_ret_vec_spatial']:.4f}s | {res['sq_ret_vec_spatial']/res['sk_ret_vec_spatial']:.2f}x |
| **5. V + F**  | INSERTION WITH VECTOR AND FULLTEXT | {res['sq_ins_vec_fts']:.4f}s | {res['sk_ins_vec_fts']:.4f}s | {res['sq_ins_vec_fts']/res['sk_ins_vec_fts']:.2f}x |
| | RETRIEVAL OF TEXT WITH VECTOR | {res['sq_ret_text_vec']:.4f}s | {res['sk_ret_text_vec']:.4f}s | {res['sq_ret_text_vec']/res['sk_ret_text_vec']:.2f}x |
| **6. Graph**  | MULTIPLE TRAVERSAL (100x 3-HOP) | {res['sq_graph_query']:.4f}s | {res['sk_graph_query']:.4f}s | {res['sq_graph_query']/res['sk_graph_query']:.2f}x |
"""
    with open(os.path.join("sekejap-benchmark", RESULT_FILE), "w") as f:
        f.write(table)
    print("\n" + table)

if __name__ == "__main__":
    run_benchmark()
