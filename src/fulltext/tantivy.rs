//! Tantivy FullText Adapter
//!
//! Mature full-text search engine with rich ecosystem.
//! Larger binary (~15MB) but more features.

use super::{FullTextAdapter, SearchHit};
use tantivy::schema::*;
use tantivy::{Index, IndexReader, IndexWriter, TantivyDocument};
use tantivy::query::QueryParser;
use tantivy::collector::TopDocs;
use std::path::Path;
use std::sync::{Arc, Mutex};

/// Tantivy-based fulltext index adapter
pub struct TantivyAdapter {
    index: Arc<Index>,
    reader: Mutex<IndexReader>,
    writer: Mutex<IndexWriter>,
    title_field: Field,
    content_field: Field,
    id_field: Field,
}

impl TantivyAdapter {
    pub fn new(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let mut schema_builder = Schema::builder();
        let id_field = schema_builder.add_u64_field("id", STORED | INDEXED);
        let title_field = schema_builder.add_text_field("title", TEXT | STORED);
        let content_field = schema_builder.add_text_field("content", TEXT);
        let schema = schema_builder.build();

        let index = if path.exists() {
            Index::open_in_dir(path)?
        } else {
            std::fs::create_dir_all(path)?;
            Index::create_in_dir(path, schema)?
        };

        let reader = index.reader()?;
        let writer = index.writer(50_000_000)?;

        Ok(Self {
            index: Arc::new(index),
            reader: Mutex::new(reader),
            writer: Mutex::new(writer),
            title_field,
            content_field,
            id_field,
        })
    }
}

impl FullTextAdapter for TantivyAdapter {
    fn add_document(&self, title: &str, content: &str, id: u64) -> Result<(), Box<dyn std::error::Error>> {
        let mut writer = self.writer.lock().unwrap();
        let mut doc = TantivyDocument::default();
        doc.add_u64(self.id_field, id);
        doc.add_text(self.title_field, title);
        doc.add_text(self.content_field, content);
        writer.add_document(doc)?;
        Ok(())
    }

    fn search(&self, query: &str, limit: usize) -> Result<Vec<SearchHit>, Box<dyn std::error::Error>> {
        let reader = self.reader.lock().unwrap();
        let searcher = reader.searcher();

        let query_parser = QueryParser::for_index(
            &self.index,
            vec![self.title_field, self.content_field]
        );

        let parsed_query = query_parser.parse_query(query)?;
        let top_docs = searcher.search(&parsed_query, &TopDocs::with_limit(limit))?;

        let hits: Vec<SearchHit> = top_docs.into_iter().filter_map(|(score, doc_address)| {
            searcher.doc::<TantivyDocument>(doc_address).ok().and_then(|doc| {
                doc.get_first(self.id_field).and_then(|v| v.as_u64()).map(|id| SearchHit {
                    id,
                    score: score as f32,
                })
            })
        }).collect();

        Ok(hits)
    }

    fn commit(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut writer = self.writer.lock().unwrap();
        writer.commit()?;
        let mut reader = self.reader.lock().unwrap();
        reader.reload()?;
        Ok(())
    }
}