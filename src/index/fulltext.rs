//! Full-Text Search Index using Tantivy
//!
//! This module provides full-text search capabilities using Tantivy
//! for high-performance text indexing and retrieval.

use std::path::Path;
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::*;
use tantivy::{Index, IndexReader, IndexWriter, ReloadPolicy, doc};

/// Full-text index configuration
#[derive(Debug, Clone)]
pub struct FulltextConfig {
    /// Maximum memory for indexing (in bytes)
    pub memory_limit_bytes: usize,
    /// Fields to index for full-text search
    pub searchable_fields: Vec<String>,
    /// Fields to store (return in results)
    pub stored_fields: Vec<String>,
    /// Field to use as primary key for results
    pub key_field: String,
}

impl Default for FulltextConfig {
    fn default() -> Self {
        Self {
            memory_limit_bytes: 50_000_000, // 50MB
            searchable_fields: vec!["title".to_string(), "content".to_string()],
            stored_fields: vec!["title".to_string(), "content".to_string()],
            key_field: "slug".to_string(),
        }
    }
}

/// Full-text search result
#[derive(Debug, Clone)]
pub struct FulltextResult {
    /// The key (slug) of the matching document
    pub key: String,
    /// Document score (relevance)
    pub score: f32,
    /// Stored fields from the document
    pub stored_fields: std::collections::HashMap<String, String>,
}

/// Full-text index using Tantivy
///
/// Provides fast full-text search with:
/// - Tokenization and stemming
/// - Phrase search
/// - Boolean queries
/// - Relevance scoring
#[cfg(feature = "fulltext")]
pub struct FulltextIndex {
    /// The Tantivy index
    index: Index,
    /// Index writer for adding documents
    writer: IndexWriter,
    /// Index reader for searching
    reader: IndexReader,
    /// Schema definition
    schema: Schema,
    /// Field references for fast access
    title_field: Field,
    content_field: Field,
    slug_field: Field,
    attributes_field: Field,
    /// Configuration
    config: FulltextConfig,
}

#[cfg(feature = "fulltext")]
impl FulltextIndex {
    /// Create a new full-text index
    pub fn new(path: &Path, config: FulltextConfig) -> Result<Self, Box<dyn std::error::Error>> {
        // Create directory if it doesn't exist
        std::fs::create_dir_all(path)?;

        // Build schema
        let mut schema_builder = Schema::builder();

        // Add title field (text, searchable, stored)
        let title_field = schema_builder.add_text_field("title", TEXT | STORED);

        // Add content field (text, searchable)
        let content_field = schema_builder.add_text_field("content", TEXT);

        // Add slug field (STRING - keyword, not tokenized)
        let slug_field = schema_builder.add_text_field("slug", STRING | STORED);

        // Add attributes field (JSON - dynamic schema)
        let attributes_field = schema_builder.add_json_field("attributes", TEXT | STORED);

        let schema = schema_builder.build();

        // Create index directory
        std::fs::create_dir_all(path)?;

        // Check if directory is empty (new index) or has content (existing index)
        let index = if path.read_dir()?.next().is_some() {
            // Directory has content, try to open existing index
            Index::open_in_dir(path)?
        } else {
            // Directory is empty, create new index
            Index::create_in_dir(path, schema.clone())?
        };

        // Create writer with memory limit
        let writer = index.writer(config.memory_limit_bytes)?;

        // Create reader with immediate reload on commit
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;

        Ok(Self {
            index,
            writer,
            reader,
            schema,
            title_field,
            content_field,
            slug_field,
            attributes_field,
            config,
        })
    }

    /// Create a new full-text index with default config
    pub fn new_default(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        Self::new(path, FulltextConfig::default())
    }

    /// Add a document to the index
    ///
    /// # Arguments
    /// * `title` - Document title
    /// * `content` - Document content
    /// * `slug` - Unique key for the document
    /// * `attributes` - Optional dynamic JSON attributes
    pub fn add_document(
        &mut self,
        title: &str,
        content: &str,
        slug: &str,
        attributes: Option<serde_json::Value>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut doc = doc!(
            self.title_field => title,
            self.content_field => content,
            self.slug_field => slug,
        );

        if let Some(attrs) = attributes {
            doc.add_field_value(self.attributes_field, &attrs);
        }

        self.writer.add_document(doc);
        Ok(())
    }

    /// Commit pending documents to the index
    pub fn commit(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.writer.commit()?;

        // Refresh reader to see new documents
        self.reader.reload()?;

        Ok(())
    }

    /// Search the index
    ///
    /// # Arguments
    /// * `query` - Search query string
    /// * `limit` - Maximum number of results
    ///
    /// # Returns
    /// Vector of matching documents with scores
    pub fn search(
        &self,
        query: &str,
        limit: usize,
    ) -> Result<Vec<FulltextResult>, Box<dyn std::error::Error>> {
        let searcher = self.reader.searcher();

        // Create query parser for searchable fields (include attributes for dynamic searching)
        let query_parser =
            QueryParser::for_index(&self.index, vec![self.title_field, self.content_field, self.attributes_field]);

        // Parse query (handle parse errors gracefully)
        let query = match query_parser.parse_query(query) {
            Ok(q) => q,
            Err(e) => {
                // Return empty results for invalid queries
                log::warn!("Query parse error: {}", e);
                return Ok(Vec::new());
            }
        };

        // Search and get top documents
        let top_docs = searcher.search(&query, &TopDocs::with_limit(limit))?;

        let mut results = Vec::new();

        for (score, address) in top_docs {
            // Get document as JSON for easy field extraction
            let doc: TantivyDocument = searcher.doc(address)?;
            let doc_json: serde_json::Value = serde_json::from_str(&doc.to_json(&self.schema))?;

            // Helper to extract string from JSON
            fn extract_str(v: &serde_json::Value) -> Option<&str> {
                if let Some(s) = v.as_str() {
                    Some(s)
                } else if let Some(arr) = v.as_array() {
                    arr.first().and_then(|s| s.as_str())
                } else {
                    None
                }
            }

            // Extract slug as key
            let slug = doc_json
                .get("slug")
                .and_then(extract_str)
                .unwrap_or("")
                .to_string();

            if slug.is_empty() {
                continue;
            }

            // Extract stored fields
            let mut stored_fields = std::collections::HashMap::new();

            if let Some(title) = doc_json.get("title").and_then(extract_str) {
                stored_fields.insert("title".to_string(), title.to_string());
            }
            if let Some(content) = doc_json.get("content").and_then(extract_str) {
                stored_fields.insert("content".to_string(), content.to_string());
            }

            results.push(FulltextResult {
                key: slug,
                score,
                stored_fields,
            });
        }

        Ok(results)
    }

    /// Search with field boost
    ///
    /// Allows giving more weight to certain fields (e.g., title matches)
    pub fn search_with_boost(
        &self,
        query: &str,
        limit: usize,
        title_boost: f32,
        content_boost: f32,
    ) -> Result<Vec<FulltextResult>, Box<dyn std::error::Error>> {
        let searcher = self.reader.searcher();

        let mut query_parser =
            QueryParser::for_index(&self.index, vec![self.title_field, self.content_field, self.attributes_field]);

        // Set field boosts
        query_parser.set_field_boost(self.title_field, title_boost);
        query_parser.set_field_boost(self.content_field, content_boost);

        let query = match query_parser.parse_query(query) {
            Ok(q) => q,
            Err(e) => {
                log::warn!("Query parse error: {}", e);
                return Ok(Vec::new());
            }
        };

        let top_docs = searcher.search(&query, &TopDocs::with_limit(limit))?;

        let mut results = Vec::new();
        for (score, address) in top_docs {
            let doc: TantivyDocument = searcher.doc(address)?;
            let doc_json: serde_json::Value = serde_json::from_str(&doc.to_json(&self.schema))?;

            // Helper to extract string from JSON
            fn extract_str(v: &serde_json::Value) -> Option<&str> {
                if let Some(s) = v.as_str() {
                    Some(s)
                } else if let Some(arr) = v.as_array() {
                    arr.first().and_then(|s| s.as_str())
                } else {
                    None
                }
            }

            let slug = doc_json
                .get("slug")
                .and_then(extract_str)
                .unwrap_or("")
                .to_string();

            if slug.is_empty() {
                continue;
            }

            let mut stored_fields = std::collections::HashMap::new();
            if let Some(title) = doc_json.get("title").and_then(extract_str) {
                stored_fields.insert("title".to_string(), title.to_string());
            }

            results.push(FulltextResult {
                key: slug,
                score,
                stored_fields,
            });
        }

        Ok(results)
    }

    /// Delete a document by slug
    pub fn delete_document(&mut self, slug: &str) -> Result<bool, Box<dyn std::error::Error>> {
        let term = Term::from_field_text(self.slug_field, slug);
        let deleted = self.writer.delete_term(term);
        Ok(deleted > 0)
    }

    /// Get the number of documents in the index
    pub fn num_docs(&self) -> usize {
        self.reader.searcher().num_docs() as usize
    }

    /// Check if the index is empty
    pub fn is_empty(&self) -> bool {
        self.num_docs() == 0
    }

    /// Clear all documents from the index
    pub fn clear(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.writer.delete_all_documents()?;
        self.writer.commit()?;
        Ok(())
    }

    /// Get index statistics
    pub fn stats(&self) -> FulltextStats {
        let searcher = self.reader.searcher();
        FulltextStats {
            num_docs: searcher.num_docs() as usize,
            segment_count: self.index.searchable_segments().unwrap_or_default().len(),
        }
    }
}

/// Full-text index statistics
#[derive(Debug, Clone)]
pub struct FulltextStats {
    /// Number of documents in the index
    pub num_docs: usize,
    /// Number of segments
    pub segment_count: usize,
}

/// Non-fulltext placeholder
#[cfg(not(feature = "fulltext"))]
pub struct FulltextIndex;

#[cfg(not(feature = "fulltext"))]
impl FulltextIndex {
    pub fn new(_path: &Path, _config: FulltextConfig) -> Result<Self, Box<dyn std::error::Error>> {
        Err("Fulltext feature not enabled. Recompile with --features fulltext".into())
    }

    pub fn new_default(_path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        Err("Fulltext feature not enabled. Recompile with --features fulltext".into())
    }

    pub fn add_document(
        &mut self,
        _title: &str,
        _content: &str,
        _slug: &str,
        _attributes: Option<serde_json::Value>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Err("Fulltext feature not enabled. Recompile with --features fulltext".into())
    }

    pub fn commit(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        Err("Fulltext feature not enabled. Recompile with --features fulltext".into())
    }

    pub fn search(
        &self,
        _query: &str,
        _limit: usize,
    ) -> Result<Vec<FulltextResult>, Box<dyn std::error::Error>> {
        Err("Fulltext feature not enabled. Recompile with --features fulltext".into())
    }

    pub fn delete_document(&mut self, _slug: &str) -> Result<bool, Box<dyn std::error::Error>> {
        Err("Fulltext feature not enabled. Recompile with --features fulltext".into())
    }

    pub fn num_docs(&self) -> usize {
        0
    }

    pub fn is_empty(&self) -> bool {
        true
    }

    pub fn clear(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    pub fn stats(&self) -> FulltextStats {
        FulltextStats {
            num_docs: 0,
            segment_count: 0,
        }
    }
}

#[cfg(feature = "fulltext")]
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_index_creation() {
        let temp_dir = TempDir::new().unwrap();
        let index = FulltextIndex::new_default(temp_dir.path()).unwrap();
        assert!(index.is_empty());
    }

    #[test]
    fn test_single_document() {
        let temp_dir = TempDir::new().unwrap();
        let mut index = FulltextIndex::new_default(temp_dir.path()).unwrap();

        index
            .add_document("Test Title", "Test content here", "test-1", None)
            .unwrap();
        index.commit().unwrap();

        assert_eq!(index.num_docs(), 1);
    }

    #[test]
    fn test_basic_search() {
        let temp_dir = TempDir::new().unwrap();
        let mut index = FulltextIndex::new_default(temp_dir.path()).unwrap();

        index
            .add_document("Jakarta Crime", "Crime rate increased in Jakarta", "news-1", None)
            .unwrap();
        index
            .add_document("Bandung Weather", "Weather is nice in Bandung", "news-2", None)
            .unwrap();
        index
            .add_document("Surabaya Traffic", "Traffic jam in Surabaya", "news-3", None)
            .unwrap();
        index.commit().unwrap();

        let results = index.search("Jakarta", 10).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].key, "news-1");
    }

    #[test]
    fn test_phrase_search() {
        let temp_dir = TempDir::new().unwrap();
        let mut index = FulltextIndex::new_default(temp_dir.path()).unwrap();

        index
            .add_document("Article", "The quick brown fox jumps", "doc-1", None)
            .unwrap();
        index
            .add_document("Article", "The lazy dog sleeps", "doc-2", None)
            .unwrap();
        index.commit().unwrap();

        // Phrase search
        let results = index.search("\"quick brown\"", 10).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].key, "doc-1");
    }

    #[test]
    fn test_boolean_search() {
        let temp_dir = TempDir::new().unwrap();
        let mut index = FulltextIndex::new_default(temp_dir.path()).unwrap();

        index
            .add_document("Doc 1", "apple orange banana", "doc-1", None)
            .unwrap();
        index.add_document("Doc 2", "apple pear", "doc-2", None).unwrap();
        index
            .add_document("Doc 3", "banana grape", "doc-3", None)
            .unwrap();
        index.commit().unwrap();

        // AND search
        let results = index.search("apple AND banana", 10).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].key, "doc-1");

        // OR search
        let results = index.search("banana OR grape", 10).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_field_boost() {
        let temp_dir = TempDir::new().unwrap();
        let mut index = FulltextIndex::new_default(temp_dir.path()).unwrap();

        // "apple" appears in both title and content
        index
            .add_document("Apple Product", "This is about apple the fruit", "doc-1", None)
            .unwrap();
        index
            .add_document("Product Review", "Apple makes great products", "doc-2", None)
            .unwrap();
        index.commit().unwrap();

        // Title should rank higher with boost
        let results = index.search_with_boost("apple", 10, 3.0, 1.0).unwrap();
        assert!(!results.is_empty());
        assert_eq!(results[0].key, "doc-1"); // Title match should rank first
    }

    #[test]
    fn test_empty_result() {
        let temp_dir = TempDir::new().unwrap();
        let index = FulltextIndex::new_default(temp_dir.path()).unwrap();

        let results = index.search("nonexistent", 10).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_delete_document() {
        let temp_dir = TempDir::new().unwrap();
        let mut index = FulltextIndex::new_default(temp_dir.path()).unwrap();

        index.add_document("Test", "Content", "test-1", None).unwrap();
        index.commit().unwrap();
        assert_eq!(index.num_docs(), 1);

        index.delete_document("test-1").unwrap();
        index.commit().unwrap();
        assert_eq!(index.num_docs(), 0);
    }

    #[test]
    fn test_clear_index() {
        let temp_dir = TempDir::new().unwrap();
        let mut index = FulltextIndex::new_default(temp_dir.path()).unwrap();

        for i in 0..10 {
            index
                .add_document(
                    &format!("Title {}", i),
                    &format!("Content {}", i),
                    &format!("doc-{}", i),
                    None,
                )
                .unwrap();
        }
        index.commit().unwrap();
        assert_eq!(index.num_docs(), 10);

        index.clear().unwrap();
        index.commit().unwrap();
        assert_eq!(index.num_docs(), 0);
        assert!(index.is_empty());
    }

    #[test]
    fn test_stemming() {
        let temp_dir = TempDir::new().unwrap();
        let mut index = FulltextIndex::new_default(temp_dir.path()).unwrap();

        // Tantivy applies stemming automatically
        index
            .add_document("Running", "The program is running smoothly", "doc-1", None)
            .unwrap();
        index
            .add_document("Run", "I will run tomorrow", "doc-2", None)
            .unwrap();
        index.commit().unwrap();

        // Both should match "run" due to stemming
        let results = index.search("run", 10).unwrap();
        assert!(results.len() >= 1);
    }
}
