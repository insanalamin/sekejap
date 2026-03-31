use chrono::NaiveDateTime;
use sekejap::SekejapDB;
use serde_json::json;
use std::fs;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempdir()?;
    let base = temp.path().join("exact_time_probe");
    fs::create_dir_all(&base)?;

    let db = SekejapDB::new(&base, 4096)?;
    db.schema().define(
        "memories",
        &json!({
            "hot_fields": {
                "hash_index": ["id"],
                "range_index": ["createdEpochMicros"]
            }
        })
        .to_string(),
    )?;

    for i in 0..2000usize {
        let year = 2013 + (i % 5) as i32;
        let month = 1 + (i % 12) as u32;
        let day = 1 + (i % 28) as u32;
        let hour = 8 + ((i * 7) % 12) as u32;
        let minute = ((i * 13) % 60) as u32;
        let second = ((i * 17) % 60) as u32;
        let created_at = format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
            year, month, day, hour, minute, second
        );
        let epoch = parse_timestamp_to_micros(&created_at);
        let payload = json!({
            "_id": format!("memories/memory_{i:05}"),
            "id": format!("memory_{i:05}"),
            "created_at": created_at,
            "createdEpochMicros": epoch,
        })
        .to_string();
        db.nodes()
            .put_json(&payload)?;
    }

    let describe = db.describe_collection("memories");
    println!("describe_collection(memories):\n{describe}");

    let start = parse_timestamp_to_micros("2014-01-10 00:00:00") as f64;
    let end = parse_timestamp_to_micros("2014-01-20 23:59:59") as f64;

    let atomic = db
        .nodes()
        .collection("memories")
        .where_between("createdEpochMicros", start, end)
        .take(200)
        .count()?;
    println!("atomic_count={}", atomic.data);
    println!("atomic_trace={:?}", atomic.trace);

    let sql_db = SekejapDB::new(&base.join("sql"), 4096)?;
    sql_db.mutate(
        "CREATE COLLECTION memories (\
            id UUID PRIMARY KEY DEFAULT uuidv4(),\
            created_at TIMESTAMP\
        ) WITH (\
            hash_index = [id],\
            range_index = [created_at]\
        )",
    )?;
    for i in 0..2000usize {
        let year = 2013 + (i % 5) as i32;
        let month = 1 + (i % 12) as u32;
        let day = 1 + (i % 28) as u32;
        let hour = 8 + ((i * 7) % 12) as u32;
        let minute = ((i * 13) % 60) as u32;
        let second = ((i * 17) % 60) as u32;
        let created_at = format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
            year, month, day, hour, minute, second
        );
        let insert = format!(
            "INSERT INTO memories (id, created_at) VALUES ('memory_{i:05}', TIMESTAMP '{created_at}')"
        );
        sql_db.mutate(&insert)?;
    }
    let sql_describe = sql_db.describe_collection("memories");
    println!("sql_describe_collection(memories):\n{sql_describe}");
    let sql_steps = sql_db.explain(
        "SELECT id FROM memories WHERE created_at >= TIMESTAMP '2014-01-10 00:00:00' AND created_at <= TIMESTAMP '2014-01-20 23:59:59' LIMIT 200",
    )?;
    println!("sql_steps={sql_steps:?}");
    let sql_outcome = sql_db.count(
        "SELECT id FROM memories WHERE created_at >= TIMESTAMP '2014-01-10 00:00:00' AND created_at <= TIMESTAMP '2014-01-20 23:59:59' LIMIT 200",
    )?;
    println!("sql_count={}", sql_outcome.data);
    println!("sql_trace={:?}", sql_outcome.trace);

    Ok(())
}

fn parse_timestamp_to_micros(value: &str) -> i64 {
    let dt = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S").unwrap();
    dt.and_utc().timestamp_micros()
}
