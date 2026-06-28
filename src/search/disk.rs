use std::collections::HashMap;
use std::io::{self, Read, Write};
use roaring::RoaringBitmap;
use super::index::SearchIndex;

const MAGIC: &[u8; 8] = b"SKSRCH02";
pub const SEARCH_INDEX_VERSION: u32 = 2;

impl SearchIndex {
    pub fn write_binary<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_all(MAGIC)?;
        w.write_all(&SEARCH_INDEX_VERSION.to_le_bytes())?;

        // Fields
        w.write_all(&(self.fields.len() as u16).to_le_bytes())?;
        for f in &self.fields {
            let bytes = f.as_bytes();
            w.write_all(&(bytes.len() as u16).to_le_bytes())?;
            w.write_all(bytes)?;
        }

        // ID map
        w.write_all(&self.doc_count.to_le_bytes())?;
        for &hash in &self.id_map {
            w.write_all(&hash.to_le_bytes())?;
        }

        // Doc field lengths
        for lengths in &self.doc_field_lengths {
            for &l in lengths {
                w.write_all(&l.to_le_bytes())?;
            }
        }

        // FST data blob
        w.write_all(&(self.fst_data.len() as u64).to_le_bytes())?;
        w.write_all(&self.fst_data)?;

        // Postings data blob
        w.write_all(&(self.postings_data.len() as u64).to_le_bytes())?;
        w.write_all(&self.postings_data)?;

        // Term-field bitmaps
        w.write_all(&(self.term_field_bitmaps.len() as u32).to_le_bytes())?;
        for ((term, field_idx), bm) in &self.term_field_bitmaps {
            let bytes = term.as_bytes();
            w.write_all(&(bytes.len() as u16).to_le_bytes())?;
            w.write_all(bytes)?;
            w.write_all(&[*field_idx])?;
            write_bitmap(w, bm)?;
        }

        // Term-position bitmaps
        w.write_all(&(self.term_position_bitmaps.len() as u32).to_le_bytes())?;
        for ((term, bucket), bm) in &self.term_position_bitmaps {
            let bytes = term.as_bytes();
            w.write_all(&(bytes.len() as u16).to_le_bytes())?;
            w.write_all(bytes)?;
            w.write_all(&bucket.to_le_bytes())?;
            write_bitmap(w, bm)?;
        }

        Ok(())
    }

    pub fn read_binary<R: Read>(r: &mut R) -> io::Result<Self> {
        let mut magic = [0u8; 8];
        r.read_exact(&mut magic)?;
        if &magic != MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "bad search index magic"));
        }

        let mut ver = [0u8; 4];
        r.read_exact(&mut ver)?;
        let version = u32::from_le_bytes(ver);
        if version != SEARCH_INDEX_VERSION {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "search index version mismatch"));
        }

        // Fields
        let num_fields = read_u16(r)? as usize;
        let mut fields = Vec::with_capacity(num_fields);
        for _ in 0..num_fields {
            fields.push(read_string(r)?);
        }

        // ID map
        let doc_count = read_u32(r)?;
        let mut id_map = Vec::with_capacity(doc_count as usize);
        let mut id_to_slot = HashMap::with_capacity(doc_count as usize);
        for slot in 0..doc_count {
            let hash = read_u64(r)?;
            id_to_slot.insert(hash, slot);
            id_map.push(hash);
        }

        // Doc field lengths
        let mut doc_field_lengths = Vec::with_capacity(doc_count as usize);
        for _ in 0..doc_count {
            let mut lengths = Vec::with_capacity(num_fields);
            for _ in 0..num_fields {
                lengths.push(read_u16(r)?);
            }
            doc_field_lengths.push(lengths);
        }

        // FST data blob
        let fst_len = read_u64(r)? as usize;
        let mut fst_data = vec![0u8; fst_len];
        r.read_exact(&mut fst_data)?;

        // Postings data blob
        let postings_len = read_u64(r)? as usize;
        let mut postings_data = vec![0u8; postings_len];
        r.read_exact(&mut postings_data)?;

        // Term-field bitmaps
        let tf_count = read_u32(r)? as usize;
        let mut term_field_bitmaps = HashMap::with_capacity(tf_count);
        for _ in 0..tf_count {
            let term = read_string(r)?;
            let mut fi = [0u8; 1];
            r.read_exact(&mut fi)?;
            let bm = read_bitmap(r)?;
            term_field_bitmaps.insert((term, fi[0]), bm);
        }

        // Term-position bitmaps
        let tp_count = read_u32(r)? as usize;
        let mut term_position_bitmaps = HashMap::with_capacity(tp_count);
        for _ in 0..tp_count {
            let term = read_string(r)?;
            let bucket = read_u16(r)?;
            let bm = read_bitmap(r)?;
            term_position_bitmaps.insert((term, bucket), bm);
        }

        Ok(SearchIndex {
            fields,
            id_map,
            id_to_slot,
            doc_count,
            doc_field_lengths,
            fst_data,
            postings_data,
            term_field_bitmaps,
            term_position_bitmaps,
        })
    }
}

fn write_bitmap<W: Write>(w: &mut W, bm: &RoaringBitmap) -> io::Result<()> {
    let mut buf = Vec::new();
    bm.serialize_into(&mut buf)?;
    w.write_all(&(buf.len() as u32).to_le_bytes())?;
    w.write_all(&buf)
}

fn read_bitmap<R: Read>(r: &mut R) -> io::Result<RoaringBitmap> {
    let len = read_u32(r)? as usize;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    RoaringBitmap::deserialize_from(&buf[..])
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

fn read_u16<R: Read>(r: &mut R) -> io::Result<u16> {
    let mut buf = [0u8; 2];
    r.read_exact(&mut buf)?;
    Ok(u16::from_le_bytes(buf))
}

fn read_u32<R: Read>(r: &mut R) -> io::Result<u32> {
    let mut buf = [0u8; 4];
    r.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

fn read_u64<R: Read>(r: &mut R) -> io::Result<u64> {
    let mut buf = [0u8; 8];
    r.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}

fn read_string<R: Read>(r: &mut R) -> io::Result<String> {
    let len = read_u16(r)? as usize;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    String::from_utf8(buf)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search::index::DocFields;

    #[test]
    fn roundtrip() {
        let idx = SearchIndex::build(
            vec!["title".into(), "body".into()],
            vec![
                DocFields { hash: 1, field_values: vec!["hello world".into(), "rust programming".into()] },
                DocFields { hash: 2, field_values: vec!["python guide".into(), "easy language".into()] },
            ].into_iter(),
        );

        let mut buf = Vec::new();
        idx.write_binary(&mut buf).unwrap();

        let mut cursor = io::Cursor::new(&buf);
        let loaded = SearchIndex::read_binary(&mut cursor).unwrap();

        assert_eq!(loaded.fields, idx.fields);
        assert_eq!(loaded.id_map, idx.id_map);
        assert_eq!(loaded.doc_count, idx.doc_count);
        assert_eq!(loaded.doc_field_lengths, idx.doc_field_lengths);
        assert_eq!(loaded.fst_data, idx.fst_data);
        assert_eq!(loaded.postings_data, idx.postings_data);

        // Verify search still works after roundtrip
        let results = loaded.search("rust");
        assert!(results.contains(0));
        assert!(!results.contains(1));
    }

    #[test]
    fn roundtrip_fuzzy() {
        let idx = SearchIndex::build(
            vec!["title".into()],
            vec![
                DocFields { hash: 1, field_values: vec!["Rust Programming Language".into()] },
                DocFields { hash: 2, field_values: vec!["Python Scripting".into()] },
            ].into_iter(),
        );

        let mut buf = Vec::new();
        idx.write_binary(&mut buf).unwrap();

        let mut cursor = io::Cursor::new(&buf);
        let loaded = SearchIndex::read_binary(&mut cursor).unwrap();

        // Fuzzy match should work after roundtrip
        let results = loaded.search("programing");
        assert!(results.contains(0), "fuzzy should work after disk roundtrip");
    }
}
