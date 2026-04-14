//! Hand-rolled SQL parser and compiler for sekejap.
//!
//! Parses a subset of SELECT SQL and compiles it to a `Vec<Step>` pipeline
//! that the existing executor can run without any extra overhead.
//!
//! # Grammar
//! ```text
//! SELECT [* | field, ...]
//! FROM   collection_name | ALL
//! [WHERE field OP value [AND field OP value]*]
//! [ORDER BY field [ASC|DESC]]
//! [LIMIT n]
//! [OFFSET n]
//!
//! -- SELECT … FROM MATCH (unified graph aggregate form)
//! SELECT expr AS alias [, ...]
//! FROM MATCH (start)-[edge]->(node) [...]
//! [WHERE start._key = 'val']
//! [GROUP BY var.field]
//! [ORDER BY alias [ASC|DESC]]
//! [LIMIT n]
//!
//! INSERT INTO collection (_key, field, ...) VALUES ('key', val, ...)
//! INSERT ('from')-[:KIND {strength: n, key: val}]->('to')
//! UPDATE collection SET field = val [, ...] [WHERE ...]
//! DELETE FROM collection | ALL [WHERE ...]
//! DELETE ('from')-[:KIND]->('to')
//! MATCH (node)-[edge]->(node) [WHERE ...] RETURN vars [LIMIT n]   -- simple traversal
//! SELECT expr AS alias [, ...] FROM MATCH (a)-[r]->(b) [WHERE ...] [GROUP BY] [ORDER BY] [LIMIT]
//! SELECT expr AS alias [, ...] FROM MATCH SHORTEST (a)-[r*]->(b) WHERE a._key='x' AND b._key='y'
//! SELECT expr FROM MATCH (a)-[:e]->(b), collection AS alias   -- multi-FROM cross-join
//!
//! CREATE TABLE collection (field type, ...)
//!     [_key TEXT PRIMARY KEY, ...]
//!     [field TIMESTAMPTZ DEFAULT NOW(), ...]
//! WITH (hash: ['_key'], range: ['age'], fulltext: ['name'], bm25: ['bio'], spatial: ['location'])
//! DROP TABLE [IF EXISTS] collection
//! DROP INDEX [IF EXISTS] ON collection USING method (field)
//!
//! ALTER TABLE collection ADD [COLUMN] name type [PRIMARY KEY] [NOT NULL]
//! ALTER TABLE collection DROP [COLUMN] [IF EXISTS] name
//! ALTER TABLE collection RENAME COLUMN old TO new
//! ALTER TABLE collection RENAME TO new_name
//! ALTER TABLE collection ALTER [COLUMN] name TYPE new_type
//!
//! SHOW TABLES
//! SHOW EDGES [FROM collection] [TO collection]
//! SHOW CREATE TABLE collection
//! SHOW collection
//!
//! -- SELECT list expressions (SELECT … FROM MATCH)
//! expr ::= var.field
//!        | COUNT(*) | SUM(math) | AVG(math) | MIN(math) | MAX(math)
//!        | PATH_AVG(var.field) | PATH_SUM(var.field) | PATH_MIN(var.field)
//!        | PATH_MAX(var.field) | PATH_PRODUCT(var.field)
//!        | PATH_FIRST(var.field) | PATH_LAST(var.field)
//!        | CASE WHEN var.field op literal THEN literal
//!               [WHEN ...] [ELSE literal] END
//!        | AGE_DAYS(var.field) | AGE_HOURS(var.field)
//!        | NOW()
//!        | JSON_ARRAY_LENGTH(var.field)
//!
//! OP    ::= = | != | <> | > | < | >= | <= | <=> | <-> | <#> | <+>
//!         | BETWEEN n AND n
//!         | IN (val, ...)
//!         | LIKE 'pat' | ILIKE 'pat'
//! value ::= 'string' | number | true | false | null
//!         | '{"type":"Point",...}'  (auto-parsed JSON)
//! ```

use crate::query::{ScoreExpr, Step};
use crate::sk_hash;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;

// ── Error ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum SqlError {
    UnexpectedToken { expected: &'static str, got: String },
    UnexpectedEnd { expected: &'static str },
    InvalidNumber(String),
    MissingField { field: &'static str },
    FieldValueCountMismatch { fields: usize, values: usize },
    InvalidValue(String),
    /// The in-memory GIN index was declared in the schema but not built.
    /// Returned by `query()` when a step that needs the index would silently
    /// produce wrong or degraded results (e.g. ILIKE on a field with no gin_index entry).
    IndexNotBuilt {
        collection: String,
        method: String,
        field: String,
    },
    /// An explicit index build failed (e.g. HNSW with no stored vectors).
    IndexBuildFailed {
        collection: String,
        method: String,
        field: String,
        reason: String,
    },
}

impl fmt::Display for SqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SqlError::UnexpectedToken { expected, got } => {
                write!(f, "expected {expected}, got `{got}`")
            }
            SqlError::UnexpectedEnd { expected } => {
                write!(f, "unexpected end of input, expected {expected}")
            }
            SqlError::InvalidNumber(s) => write!(f, "invalid number: `{s}`"),
            SqlError::MissingField { field } => write!(f, "INSERT requires a `{field}` field"),
            SqlError::FieldValueCountMismatch { fields, values } => write!(
                f,
                "field count ({fields}) does not match value count ({values})"
            ),
            SqlError::InvalidValue(s) => write!(f, "invalid value: {s}"),
            SqlError::IndexNotBuilt { collection, method, field } => write!(
                f,
                "{method} index on {collection}.{field} is declared but not built.\n  Hint: REINDEX ON {collection} USING {method} ({field})"
            ),
            SqlError::IndexBuildFailed { collection, method, field, reason } => write!(
                f,
                "cannot build {method} index on {collection}.{field}: {reason}.\n  Hint: once data is ready, run: REINDEX ON {collection} USING {method} ({field})"
            ),
        }
    }
}

impl std::error::Error for SqlError {}

// ── Tokens ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Kw(Kw),
    Ident(String),
    Str(String),
    Num(f64),
    Star,
    Comma,
    Eq,  // =
    Neq, // != or <>
    Gt,  // >
    Lt,  // <
    Gte, // >=
    Lte, // <=
    LParen,
    RParen,
    LBrace,    // {
    RBrace,    // }
    Arrow,     // ->
    LongArrow, // ->>
    BackArrow, // <-
    Colon,     // :
    LBracket,  // [
    RBracket,  // ]
    Dot,       // .
    DotDot,    // ..
    Dash,      // -
    Plus,      // +
    Slash,     // /
    VecCosineOp, // <=>  cosine distance
    VecL2Op,     // <->  Euclidean (L2) distance
    VecDotOp,    // <#>  inner product
    VecL1Op,     // <+>  Manhattan (L1) distance
    Eof,
}

#[derive(Debug, Clone, PartialEq)]
enum Kw {
    Select,
    From,
    Where,
    And,
    Order,
    By,
    Asc,
    Desc,
    Limit,
    Offset,
    All,
    Between,
    In,
    Like,
    ILike,
    Null,
    True,
    False,
    Insert,
    Into,
    Values,
    Delete,
    Update,
    Set,
    Match,
    Return,
    Union,
    Create,
    Table,
    Index,
    On,
    Using,
    Primary,
    Key,
    With,
    VectorNear,
    // Logical / null check
    Not,
    Or,
    Is,
    // Projection alias
    As,
    // Aggregate functions
    Count,
    Sum,
    Avg,
    Min,
    Max,
    // Grouping / dedup
    Distinct,
    Group,
    Having,
    // Schema introspection
    Show,
    // DDL lifecycle
    Drop,
    If,
    Exists,
    // ALTER TABLE
    Alter,
    Column,
    Rename,
    To,
    Add,
    // Index rebuild
    Reindex,
    // CASE expression
    Case,
    When,
    Then,
    Else,
    End,
    // Path predicate quantifiers (ANY/ALL already: All, add Any/None_/Single)
    Any,
    None_,
    Single,
}

fn kw_to_str(kw: &Kw) -> &'static str {
    match kw {
        Kw::Select => "select",
        Kw::From => "from",
        Kw::Where => "where",
        Kw::And => "and",
        Kw::Order => "order",
        Kw::By => "by",
        Kw::Asc => "asc",
        Kw::Desc => "desc",
        Kw::Limit => "limit",
        Kw::Offset => "offset",
        Kw::All => "all",
        Kw::Between => "between",
        Kw::In => "in",
        Kw::Like => "like",
        Kw::ILike => "ilike",
        Kw::Null => "null",
        Kw::True => "true",
        Kw::False => "false",
        Kw::Insert => "insert",
        Kw::Into => "into",
        Kw::Values => "values",
        Kw::Delete => "delete",
        Kw::Update => "update",
        Kw::Set => "set",
        Kw::Match => "match",
        Kw::Return => "return",
        Kw::Union => "union",
        Kw::Create => "create",
        Kw::Table => "table",
        Kw::Index => "index",
        Kw::On => "on",
        Kw::Using => "using",
        Kw::Primary => "primary",
        Kw::Key => "key",
        Kw::With => "with",
        Kw::VectorNear => "vector_near",
        Kw::Not => "not",
        Kw::Or => "or",
        Kw::Is => "is",
        Kw::As => "as",
        Kw::Count => "count",
        Kw::Sum => "sum",
        Kw::Avg => "avg",
        Kw::Min => "min",
        Kw::Max => "max",
        Kw::Distinct => "distinct",
        Kw::Group => "group",
        Kw::Having => "having",
        Kw::Show => "show",
        Kw::Drop => "drop",
        Kw::If => "if",
        Kw::Exists => "exists",
        Kw::Alter => "alter",
        Kw::Column => "column",
        Kw::Rename => "rename",
        Kw::To => "to",
        Kw::Add => "add",
        Kw::Reindex => "reindex",
        Kw::Case => "case",
        Kw::When => "when",
        Kw::Then => "then",
        Kw::Else => "else",
        Kw::End => "end",
        Kw::Any => "any",
        Kw::None_ => "none",
        Kw::Single => "single",
    }
}

fn keyword(s: &str) -> Option<Kw> {
    match s.to_ascii_uppercase().as_str() {
        "SELECT" => Some(Kw::Select),
        "FROM" => Some(Kw::From),
        "WHERE" => Some(Kw::Where),
        "AND" => Some(Kw::And),
        "ORDER" => Some(Kw::Order),
        "BY" => Some(Kw::By),
        "ASC" => Some(Kw::Asc),
        "DESC" => Some(Kw::Desc),
        "LIMIT" => Some(Kw::Limit),
        "OFFSET" => Some(Kw::Offset),
        "ALL" => Some(Kw::All),
        "BETWEEN" => Some(Kw::Between),
        "IN" => Some(Kw::In),
        "LIKE" => Some(Kw::Like),
        "ILIKE" => Some(Kw::ILike),
        "NULL" => Some(Kw::Null),
        "TRUE" => Some(Kw::True),
        "FALSE" => Some(Kw::False),
        "INSERT" => Some(Kw::Insert),
        "INTO" => Some(Kw::Into),
        "VALUES" => Some(Kw::Values),
        "DELETE" => Some(Kw::Delete),
        "UPDATE" => Some(Kw::Update),
        "SET" => Some(Kw::Set),
        "MATCH" => Some(Kw::Match),
        "RETURN" => Some(Kw::Return),
        "UNION" => Some(Kw::Union),
        "CREATE" => Some(Kw::Create),
        "TABLE" => Some(Kw::Table),
        "INDEX" => Some(Kw::Index),
        "ON" => Some(Kw::On),
        "USING" => Some(Kw::Using),
        "PRIMARY" => Some(Kw::Primary),
        "KEY" => Some(Kw::Key),
        "WITH" => Some(Kw::With),
        "VECTOR_NEAR" => Some(Kw::VectorNear),
        "NOT" => Some(Kw::Not),
        "OR" => Some(Kw::Or),
        "IS" => Some(Kw::Is),
        "AS" => Some(Kw::As),
        "COUNT" => Some(Kw::Count),
        "SUM" => Some(Kw::Sum),
        "AVG" => Some(Kw::Avg),
        "MIN" => Some(Kw::Min),
        "MAX" => Some(Kw::Max),
        "DISTINCT" => Some(Kw::Distinct),
        "GROUP" => Some(Kw::Group),
        "HAVING" => Some(Kw::Having),
        "SHOW" => Some(Kw::Show),
        "DROP" => Some(Kw::Drop),
        "IF" => Some(Kw::If),
        "EXISTS" => Some(Kw::Exists),
        "ALTER" => Some(Kw::Alter),
        "COLUMN" => Some(Kw::Column),
        "RENAME" => Some(Kw::Rename),
        "TO" => Some(Kw::To),
        "ADD" => Some(Kw::Add),
        "REINDEX" => Some(Kw::Reindex),
        "CASE" => Some(Kw::Case),
        "WHEN" => Some(Kw::When),
        "THEN" => Some(Kw::Then),
        "ELSE" => Some(Kw::Else),
        "END" => Some(Kw::End),
        "ANY" => Some(Kw::Any),
        "NONE" => Some(Kw::None_),
        "SINGLE" => Some(Kw::Single),
        _ => None,
    }
}

// ── Tokenizer ─────────────────────────────────────────────────────────────────

fn tokenize(sql: &str) -> Result<Vec<Tok>, SqlError> {
    let chars: Vec<char> = sql.chars().collect();
    let len = chars.len();
    let mut i = 0;
    let mut tokens = Vec::new();

    while i < len {
        let c = chars[i];

        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }

        match c {
            '*' => {
                tokens.push(Tok::Star);
                i += 1;
            }
            ',' => {
                tokens.push(Tok::Comma);
                i += 1;
            }
            '(' => {
                tokens.push(Tok::LParen);
                i += 1;
            }
            ')' => {
                tokens.push(Tok::RParen);
                i += 1;
            }
            '{' => {
                tokens.push(Tok::LBrace);
                i += 1;
            }
            '}' => {
                tokens.push(Tok::RBrace);
                i += 1;
            }
            '[' => {
                tokens.push(Tok::LBracket);
                i += 1;
            }
            ']' => {
                tokens.push(Tok::RBracket);
                i += 1;
            }
            ':' => {
                tokens.push(Tok::Colon);
                i += 1;
            }
            '=' => {
                tokens.push(Tok::Eq);
                i += 1;
            }
            '.' => {
                if i + 1 < len && chars[i + 1] == '.' {
                    tokens.push(Tok::DotDot);
                    i += 2;
                } else {
                    tokens.push(Tok::Dot);
                    i += 1;
                }
            }
            '>' => {
                if i + 1 < len && chars[i + 1] == '=' {
                    tokens.push(Tok::Gte);
                    i += 2;
                } else {
                    tokens.push(Tok::Gt);
                    i += 1;
                }
            }
            '-' if i + 2 < len && chars[i + 1] == '>' && chars[i + 2] == '>' => {
                tokens.push(Tok::LongArrow);
                i += 3;
            }
            '-' if i + 1 < len && chars[i + 1] == '>' => {
                tokens.push(Tok::Arrow);
                i += 2;
            }
            '-' if i + 1 < len && chars[i + 1].is_ascii_digit() => {
                let start = i;
                i += 1; // skip '-'
                while i < len
                    && (chars[i].is_ascii_digit()
                        || (chars[i] == '.' && !(i + 1 < len && chars[i + 1] == '.')))
                {
                    i += 1;
                }
                let s: String = chars[start..i].iter().collect();
                let n = s.parse::<f64>().map_err(|_| SqlError::InvalidNumber(s))?;
                tokens.push(Tok::Num(n));
            }
            '-' => {
                tokens.push(Tok::Dash);
                i += 1;
            }
            '+' => {
                tokens.push(Tok::Plus);
                i += 1;
            }
            '/' => {
                tokens.push(Tok::Slash);
                i += 1;
            }
            '<' => {
                // 3-char vector operators — must be checked before 2-char operators.
                if i + 2 < len && chars[i + 1] == '=' && chars[i + 2] == '>' {
                    tokens.push(Tok::VecCosineOp); // <=>
                    i += 3;
                } else if i + 2 < len && chars[i + 1] == '-' && chars[i + 2] == '>' {
                    tokens.push(Tok::VecL2Op); // <->
                    i += 3;
                } else if i + 2 < len && chars[i + 1] == '#' && chars[i + 2] == '>' {
                    tokens.push(Tok::VecDotOp); // <#>
                    i += 3;
                } else if i + 2 < len && chars[i + 1] == '+' && chars[i + 2] == '>' {
                    tokens.push(Tok::VecL1Op); // <+>
                    i += 3;
                } else if i + 1 < len && chars[i + 1] == '=' {
                    tokens.push(Tok::Lte); // <=
                    i += 2;
                } else if i + 1 < len && chars[i + 1] == '>' {
                    tokens.push(Tok::Neq); // <>
                    i += 2;
                } else if i + 1 < len && chars[i + 1] == '-' {
                    tokens.push(Tok::BackArrow); // <-
                    i += 2;
                } else {
                    tokens.push(Tok::Lt); // <
                    i += 1;
                }
            }
            '!' => {
                if i + 1 < len && chars[i + 1] == '=' {
                    tokens.push(Tok::Neq);
                    i += 2;
                } else {
                    return Err(SqlError::UnexpectedToken {
                        expected: "=",
                        got: if i + 1 < len {
                            chars[i + 1].to_string()
                        } else {
                            "end".into()
                        },
                    });
                }
            }
            '\'' | '"' => {
                let quote = c;
                i += 1;
                let start = i;
                while i < len && chars[i] != quote {
                    i += 1;
                }
                let s: String = chars[start..i].iter().collect();
                if i < len {
                    i += 1;
                } // consume closing quote
                tokens.push(Tok::Str(s));
            }
            c if c.is_ascii_digit() => {
                let start = i;
                while i < len
                    && (chars[i].is_ascii_digit()
                        || (chars[i] == '.' && !(i + 1 < len && chars[i + 1] == '.')))
                {
                    i += 1;
                }
                let s: String = chars[start..i].iter().collect();
                let n = s.parse::<f64>().map_err(|_| SqlError::InvalidNumber(s))?;
                tokens.push(Tok::Num(n));
            }
            c if c.is_alphabetic() || c == '_' || c == '/' => {
                let start = i;
                while i < len && (chars[i].is_alphanumeric() || chars[i] == '_' || chars[i] == '/')
                {
                    i += 1;
                }
                let s: String = chars[start..i].iter().collect();
                match keyword(&s) {
                    Some(kw) => tokens.push(Tok::Kw(kw)),
                    None => tokens.push(Tok::Ident(s)),
                }
            }
            ';' => {
                i += 1;
            } // trailing semicolons are ignored
            other => {
                return Err(SqlError::UnexpectedToken {
                    expected: "valid SQL token",
                    got: other.to_string(),
                });
            }
        }
    }

    tokens.push(Tok::Eof);
    Ok(tokens)
}

// ── AST ───────────────────────────────────────────────────────────────────────

enum Source {
    Collection(String),
    All,
}

#[derive(Clone, Copy)]
enum CompareOp {
    Eq,
    Neq,
    Gt,
    Lt,
    Gte,
    Lte,
}

enum CondExpr {
    Compare {
        field: String,
        op: CompareOp,
        value: Value,
    },
    Between {
        field: String,
        lo: f64,
        hi: f64,
    },
    In {
        field: String,
        values: Vec<Value>,
    },
    Like {
        field: String,
        pattern: String,
        case_insensitive: bool,
    },
    StDWithin {
        lat: f64,
        lon: f64,
        distance_km: f64,
    },
    StContainsPoint {
        lat: f64,
        lon: f64,
    },
    StWithin {
        ring: Vec<[f64; 2]>,
    },
    StContains {
        ring: Vec<[f64; 2]>,
    },
    StIntersects {
        ring: Vec<[f64; 2]>,
    },
    StDistance {
        field: String,
        lat: f64,
        lon: f64,
        max_km: f64,
    },
    StLength {
        field: String,
        min_km: f64,
    },
    StArea {
        field: String,
        min_km2: f64,
    },
    Bm25Func {
        field: String,
        query: String,
    },
    Bm25 {
        field: String,
        query: String,
        min_score: f64,
    },
    VectorNear {
        field: String,
        query: Vec<f32>,
        k: usize,
    },
    /// `field IS NULL` or `field IS NOT NULL`.
    IsNull {
        field: String,
        negated: bool, // true = IS NOT NULL
    },
    /// `NOT <inner_cond>`
    Not(Box<CondExpr>),
    /// `cond_group OR cond_group [OR …]`
    /// Each inner Vec is one AND-group.
    Or(Vec<Vec<CondExpr>>),
}

enum OrderKey {
    /// One or more `field [ASC|DESC]` columns, evaluated left-to-right.
    Fields(Vec<(String, bool)>),
    Bm25(String, String, bool),
    /// `field <op> [f32, ...]` — sort by vector distance (nearest first, Dot negated).
    Vector { field: String, query: Vec<f32>, metric: crate::query::VecMetric },
    /// Arithmetic score expression, e.g. `BM25(title,'q') * 0.7 + popularity * 0.3`.
    Expr(ScoreExpr, bool),
}

struct SelectStmt {
    fields: Vec<String>,
    source: Source,
    conditions: Vec<CondExpr>,
    group_by: Vec<String>,
    having: Vec<CondExpr>,
    distinct: bool,
    order_by: Option<OrderKey>,
    limit: Option<usize>,
    offset: Option<usize>,
    bm25_scores: Vec<(String, String)>,
}

struct InsertStmt {
    collection: String,
    fields: Vec<String>,
    values: Vec<Value>,
}

struct DeleteStmt {
    source: Source,
    conditions: Vec<CondExpr>,
}

struct UpdateStmt {
    collection: String,
    updates: Vec<(String, Value)>, // SET field = value pairs
    conditions: Vec<CondExpr>,
}

/// A single edge parsed from INSERT ('a')-[:KIND {props}]->('b').
#[derive(Debug, Clone)]
pub struct EdgeInsert {
    pub from: String,
    pub to: String,
    pub edge_type: String,
    pub strength: f32,
    pub props_json: Option<String>,
}

/// A single edge parsed from DELETE ('a')-[:KIND]->('b').
#[derive(Debug, Clone)]
pub struct EdgeDelete {
    pub from: String,
    pub to: String,
    pub edge_type: String,
}

/// The result of compiling a mutation SQL statement.
#[derive(Debug)]
pub enum CompiledMutation {
    /// A single node to insert, plus any vector fields.
    Insert {
        /// The collection this insert targets (for schema lookup).
        collection: String,
        slug: String,
        payload_json: String,
        /// Vector fields separated from the JSON payload: (field_name, f32 data).
        vectors: Vec<(String, Vec<f32>)>,
    },
    /// Steps that select the set of nodes to delete.
    Delete(Vec<Step>),
    /// Steps that select nodes to update, plus the field→value patches.
    Update {
        steps: Vec<Step>,
        updates: Vec<(String, Value)>,
    },
    /// Create one or more directed edges via Cypher pattern.
    InsertEdge(Vec<EdgeInsert>),
    /// Remove one or more directed edges via Cypher pattern.
    DeleteEdge(Vec<EdgeDelete>),
    /// MATCH ... INSERT: select nodes via MATCH, then insert edges.
    MatchInsert {
        match_steps: Vec<Step>,
        target: String,
        edge_type: String,
        strength: f32,
        props: Option<String>,
    },
    /// CREATE TABLE: define schema for a collection.
    CreateTable {
        collection: String,
        schema: TableSchema,
    },
    /// CREATE INDEX: build an index on a collection field (PostgreSQL style).
    CreateIndex {
        /// Optional index name (ignored at runtime, stored for introspection).
        name: Option<String>,
        collection: String,
        method: IndexMethod,
        fields: Vec<String>,
    },
    /// DROP TABLE [IF EXISTS]: delete schema + all nodes + cascade edges.
    DropTable {
        collection: String,
        /// If true, silently succeed when the collection does not exist.
        if_exists: bool,
    },
    /// DROP INDEX [IF EXISTS] ON collection USING method (field): remove a specific index.
    DropIndex {
        collection: String,
        method: IndexMethod,
        field: String,
        if_exists: bool,
    },
    /// ALTER TABLE: modify an existing schema (add/drop/rename columns, rename table).
    AlterTable {
        collection: String,
        op: AlterTableOp,
    },
    /// REINDEX: rebuild an existing index on a collection field.
    /// Does not write a WAL entry — it is a rebuild, not a schema mutation.
    Reindex {
        collection: String,
        method: IndexMethod,
        fields: Vec<String>,
    },
}

/// The specific alteration to apply in an `ALTER TABLE` statement.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum AlterTableOp {
    /// `ALTER TABLE t ADD COLUMN name type`
    AddColumn { def: FieldDef },
    /// `ALTER TABLE t DROP COLUMN [IF EXISTS] name`
    DropColumn { name: String, if_exists: bool },
    /// `ALTER TABLE t RENAME COLUMN old TO new`
    RenameColumn { old_name: String, new_name: String },
    /// `ALTER TABLE t RENAME TO new_name`
    RenameTable { new_name: String },
    /// `ALTER TABLE t ALTER COLUMN name TYPE new_type` (schema-only; no data coercion)
    AlterColumnType { name: String, ty: FieldType },
}

// ── MATCH AST ─────────────────────────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq)]
enum EdgeDir {
    Forward,
    Backward,
}

struct MatchNode {
    var: Option<String>,         // (a:artist) → var="a"
    label: Option<String>,       // (a:artist) → label="artist"
    props: Vec<(String, Value)>, // (:genre {_key: 'rock'}) → inline WHERE
}

struct MatchEdge {
    var: Option<String>,  // [r:has_genre] → var="r"
    kind: Option<String>, // [r:has_genre] → kind="has_genre"
    dir: EdgeDir,
    depth: Option<(u32, u32)>, // *1..5 → Some((1,5))
}

struct MatchStmt {
    start: MatchNode,
    edge: MatchEdge,
    end: MatchNode,
    conditions: Vec<MatchCond>,
    #[allow(dead_code)]
    return_vars: Vec<String>, // kept for future RETURN projection
    limit: Option<usize>,
}

enum MatchCond {
    NodeField {
        var: String,
        field: String,
        op: CompareOp,
        value: Value,
    },
}

// ── CREATE INDEX ──────────────────────────────────────────────────────────────

/// Index method — mirrors PostgreSQL's `USING <method>` clause.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IndexMethod {
    /// B-tree: range and equality on ordered fields (price, age, …).
    Btree,
    /// Hash: fast exact-match lookups.
    Hash,
    /// GIN trigram: ILIKE / full-text search (exact, no false positives).
    Gin,
    /// GiST trigram: ILIKE / full-text search (lossy, lower memory).
    Gist,
    /// BM25: relevance-ranked full-text search.
    Bm25,
    /// Spatial grid: ST_DWITHIN, ST_WITHIN, ST_CONTAINS, …
    Spatial,
    /// HNSW: approximate nearest-neighbour vector search (Phase 2).
    Hnsw,
}

impl std::fmt::Display for IndexMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Btree   => "btree",
            Self::Hash    => "hash",
            Self::Gin     => "gin",
            Self::Gist    => "gist",
            Self::Bm25    => "bm25",
            Self::Spatial => "spatial",
            Self::Hnsw    => "hnsw",
        };
        f.write_str(s)
    }
}

// ── CREATE TABLE AST ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FieldType {
    Text,
    Integer,
    Real,
    Timestamptz,
    Geo,
    Vector,
    Json,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldDef {
    pub name: String,
    pub ty: FieldType,
    pub is_primary_key: bool,
    pub is_timestamptz: bool,
    pub default_now: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexHint {
    pub hash: Vec<String>,
    pub range: Vec<String>,
    pub fulltext: Vec<String>,
    pub bm25: Vec<String>,
    pub spatial: Vec<String>,
    /// Vector fields — populated automatically from VECTOR-typed columns,
    /// or explicitly via WITH (vector: ['field']). Reserved for Phase 2 HNSW.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub vector: Vec<String>,
    /// Version at which each index was last built.
    /// Key: `"method:field"` — e.g. `"gin:name"`, `"btree:price"`.
    /// Absent key (or stored 0) means built before versioning was introduced → rebuild.
    #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub build_versions: std::collections::HashMap<String, u32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableSchema {
    pub collection: String,
    pub fields: Vec<FieldDef>,
    pub indexes: IndexHint,
}

impl Default for IndexHint {
    fn default() -> Self {
        Self {
            hash: vec!["_key".to_string()],
            range: Vec::new(),
            fulltext: Vec::new(),
            bm25: Vec::new(),
            spatial: Vec::new(),
            vector: Vec::new(),
            build_versions: std::collections::HashMap::new(),
        }
    }
}

// ── Parser ────────────────────────────────────────────────────────────────────

enum FieldOrBm25 {
    Field(String),
    Bm25 { field: String, query: String },
}

struct Parser {
    tokens: Vec<Tok>,
    pos: usize,
}

impl Parser {
    fn new(tokens: Vec<Tok>) -> Self {
        Self { tokens, pos: 0 }
    }

    fn peek(&self) -> &Tok {
        &self.tokens[self.pos]
    }

    fn advance(&mut self) -> Tok {
        let t = self.tokens[self.pos].clone();
        if self.pos + 1 < self.tokens.len() {
            self.pos += 1;
        }
        t
    }

    fn expect_kw(&mut self, expected: Kw, name: &'static str) -> Result<(), SqlError> {
        match self.peek() {
            Tok::Kw(k) if *k == expected => {
                self.advance();
                Ok(())
            }
            Tok::Eof => Err(SqlError::UnexpectedEnd { expected: name }),
            other => Err(SqlError::UnexpectedToken {
                expected: name,
                got: format!("{other:?}"),
            }),
        }
    }

    fn expect_str(&mut self) -> Result<String, SqlError> {
        match self.advance() {
            Tok::Str(s) => Ok(s),
            Tok::Eof => Err(SqlError::UnexpectedEnd {
                expected: "string literal",
            }),
            other => Err(SqlError::UnexpectedToken {
                expected: "string literal",
                got: format!("{other:?}"),
            }),
        }
    }

    fn expect_num(&mut self) -> Result<f64, SqlError> {
        match self.advance() {
            Tok::Num(n) => Ok(n),
            Tok::Eof => Err(SqlError::UnexpectedEnd { expected: "number" }),
            other => Err(SqlError::UnexpectedToken {
                expected: "number",
                got: format!("{other:?}"),
            }),
        }
    }

    fn expect_comma(&mut self) -> Result<(), SqlError> {
        match self.peek() {
            Tok::Comma => {
                self.advance();
                Ok(())
            }
            Tok::Eof => Err(SqlError::UnexpectedEnd { expected: "," }),
            other => Err(SqlError::UnexpectedToken {
                expected: ",",
                got: format!("{other:?}"),
            }),
        }
    }

    fn expect_lparen(&mut self) -> Result<(), SqlError> {
        match self.peek() {
            Tok::LParen => {
                self.advance();
                Ok(())
            }
            Tok::Eof => Err(SqlError::UnexpectedEnd { expected: "(" }),
            other => Err(SqlError::UnexpectedToken {
                expected: "(",
                got: format!("{other:?}"),
            }),
        }
    }

    fn expect_rparen(&mut self) -> Result<(), SqlError> {
        match self.peek() {
            Tok::RParen => {
                self.advance();
                Ok(())
            }
            Tok::Eof => Err(SqlError::UnexpectedEnd { expected: ")" }),
            other => Err(SqlError::UnexpectedToken {
                expected: ")",
                got: format!("{other:?}"),
            }),
        }
    }

    fn expect_lbracket(&mut self) -> Result<(), SqlError> {
        match self.peek() {
            Tok::LBracket => {
                self.advance();
                Ok(())
            }
            Tok::Eof => Err(SqlError::UnexpectedEnd { expected: "[" }),
            other => Err(SqlError::UnexpectedToken {
                expected: "[",
                got: format!("{other:?}"),
            }),
        }
    }

    fn expect_rbracket(&mut self) -> Result<(), SqlError> {
        match self.peek() {
            Tok::RBracket => {
                self.advance();
                Ok(())
            }
            Tok::Eof => Err(SqlError::UnexpectedEnd { expected: "]" }),
            other => Err(SqlError::UnexpectedToken {
                expected: "]",
                got: format!("{other:?}"),
            }),
        }
    }

    fn expect_dot(&mut self) -> Result<(), SqlError> {
        match self.peek() {
            Tok::Dot => {
                self.advance();
                Ok(())
            }
            Tok::Eof => Err(SqlError::UnexpectedEnd { expected: "." }),
            other => Err(SqlError::UnexpectedToken {
                expected: ".",
                got: format!("{other:?}"),
            }),
        }
    }

    fn expect_colon(&mut self) -> Result<(), SqlError> {
        match self.peek() {
            Tok::Colon => {
                self.advance();
                Ok(())
            }
            Tok::Eof => Err(SqlError::UnexpectedEnd { expected: ":" }),
            other => Err(SqlError::UnexpectedToken {
                expected: ":",
                got: format!("{other:?}"),
            }),
        }
    }

    fn expect_ident(&mut self) -> Result<String, SqlError> {
        match self.advance() {
            Tok::Ident(s) => Ok(s),
            Tok::Str(s) => Ok(s), // allow quoted identifiers
            // Allow keywords as identifiers (e.g. r.strength, collection named "set")
            Tok::Kw(kw) => Ok(kw_to_str(&kw).to_string()),
            Tok::Eof => Err(SqlError::UnexpectedEnd {
                expected: "identifier",
            }),
            other => Err(SqlError::UnexpectedToken {
                expected: "identifier",
                got: format!("{other:?}"),
            }),
        }
    }

    fn parse_value(&mut self) -> Result<Value, SqlError> {
        // Vector literal: [f32, f32, ...] — used in INSERT/UPDATE for vector fields.
        if matches!(self.peek(), Tok::LBracket) {
            let floats = self.parse_f32_array()?;
            let arr = floats
                .into_iter()
                .map(|f| {
                    serde_json::Number::from_f64(f as f64)
                        .map(Value::Number)
                        .unwrap_or(Value::Null)
                })
                .collect();
            return Ok(Value::Array(arr));
        }
        match self.advance() {
            Tok::Str(s) => Ok(Value::String(s)),
            Tok::Num(n) => {
                let num = serde_json::Number::from_f64(n)
                    .ok_or_else(|| SqlError::InvalidNumber(n.to_string()))?;
                Ok(Value::Number(num))
            }
            Tok::Kw(Kw::True) => Ok(Value::Bool(true)),
            Tok::Kw(Kw::False) => Ok(Value::Bool(false)),
            Tok::Kw(Kw::Null) => Ok(Value::Null),
            Tok::Eof => Err(SqlError::UnexpectedEnd { expected: "value" }),
            other => Err(SqlError::UnexpectedToken {
                expected: "value",
                got: format!("{other:?}"),
            }),
        }
    }

    // ── JSON path tail ────────────────────────────────────────────────────────

    /// After reading a root identifier, consume any `->` / `->>` JSON path
    /// operators and return an encoded field key:
    ///
    /// - `col->>'key'`             → `__JP_TEXT__col__key`
    /// - `col->'nested'->>'key'`   → `__JP_TEXT__col__nested__key`
    /// - `col->'nested'`           → `__JP_OBJ__col__nested`
    ///
    /// If no path operator follows, the original `root` is returned unchanged.
    ///
    /// **Note**: key segments are joined with `__`. Keys that themselves contain
    /// `__` will be mis-parsed; this mirrors the existing `__FUNC__` convention.
    fn parse_json_path_tail(&mut self, root: String) -> String {
        // Quick check: is there a path operator next?
        if !matches!(self.peek(), Tok::Arrow | Tok::LongArrow) {
            return root;
        }

        let mut segments = vec![root];
        let mut is_text = false;

        loop {
            match self.peek() {
                Tok::LongArrow => {
                    self.advance();
                    let key = self.read_path_key();
                    segments.push(key);
                    is_text = true;
                    break; // ->> is always the terminal operator
                }
                Tok::Arrow => {
                    self.advance();
                    let key = self.read_path_key();
                    segments.push(key);
                    is_text = false;
                    // continue — may chain further ->/->>'s
                }
                _ => break,
            }
        }

        if segments.len() == 1 {
            return segments.into_iter().next().unwrap();
        }

        let prefix = if is_text { "__JP_TEXT__" } else { "__JP_OBJ__" };
        format!("{}{}", prefix, segments.join("__"))
    }

    /// Read the next token as a path segment key (string literal, identifier, or keyword).
    fn read_path_key(&mut self) -> String {
        match self.peek().clone() {
            Tok::Str(s) => { self.advance(); s }
            Tok::Ident(s) => { self.advance(); s }
            Tok::Kw(kw) => { self.advance(); kw_to_str(&kw).to_string() }
            _ => String::new(), // malformed — return empty, parser will error on next token
        }
    }

    // ── Top-level parse ───────────────────────────────────────────────────────

    fn parse(&mut self) -> Result<SelectStmt, SqlError> {
        self.expect_kw(Kw::Select, "SELECT")?;

        let distinct = if matches!(self.peek(), Tok::Kw(Kw::Distinct)) {
            self.advance();
            true
        } else {
            false
        };

        let (fields, bm25_scores) = self.parse_field_list()?;

        self.expect_kw(Kw::From, "FROM")?;

        let source = self.parse_source()?;

        let mut conditions = Vec::new();
        if matches!(self.peek(), Tok::Kw(Kw::Where)) {
            self.advance();
            conditions = self.parse_conditions()?;
        }

        let mut group_by: Vec<String> = Vec::new();
        if matches!(self.peek(), Tok::Kw(Kw::Group)) {
            self.advance();
            self.expect_kw(Kw::By, "BY")?;
            group_by.push(self.expect_ident()?);
            while matches!(self.peek(), Tok::Comma) {
                self.advance();
                group_by.push(self.expect_ident()?);
            }
        }

        let mut having: Vec<CondExpr> = Vec::new();
        if matches!(self.peek(), Tok::Kw(Kw::Having)) {
            self.advance();
            having = self.parse_conditions()?;
        }

        let mut order_by = None;
        if matches!(self.peek(), Tok::Kw(Kw::Order)) {
            self.advance();
            self.expect_kw(Kw::By, "BY")?;

            // Special case: `field <op> [vec]` — vector distance sort (all 4 metrics).
            // Peek ahead: if first token is a plain ident and the next is a vec operator,
            // use the fast vector path.
            let vec_op = if matches!(self.peek(), Tok::Ident(_)) {
                let saved = self.pos;
                self.advance(); // consume ident temporarily
                let op = match self.peek() {
                    Tok::VecCosineOp => Some(crate::query::VecMetric::Cosine),
                    Tok::VecL2Op     => Some(crate::query::VecMetric::L2),
                    Tok::VecDotOp    => Some(crate::query::VecMetric::Dot),
                    Tok::VecL1Op     => Some(crate::query::VecMetric::L1),
                    _ => None,
                };
                self.pos = saved; // roll back
                op
            } else {
                None
            };

            if let Some(metric) = vec_op {
                let field = self.expect_ident()?;
                self.advance(); // consume operator
                let query = self.parse_f32_array()?;
                order_by = Some(OrderKey::Vector { field, query, metric });
            } else {
                // Arithmetic score expression (handles plain fields, BM25, VECTOR_SIM,
                // and any combination with +, -, *, /, parentheses).
                let expr = self.parse_score_expr()?;

                // Read optional direction. Score expressions default to DESC
                // (highest score first); plain field sorts default to ASC.
                let is_plain_field = matches!(&expr, ScoreExpr::Field(_));
                let ascending = match self.peek() {
                    Tok::Kw(Kw::Desc) => { self.advance(); false }
                    Tok::Kw(Kw::Asc)  => { self.advance(); true  }
                    _ => is_plain_field, // field → true (ASC), score → false (DESC)
                };

                // Classify the result:
                match expr {
                    // Plain field(s) — use the fast multi-column sort path.
                    ScoreExpr::Field(name) if matches!(self.peek(), Tok::Comma) => {
                        // Multi-column: `field1 [ASC|DESC], field2 [ASC|DESC], ...`
                        let mut cols = vec![(name, ascending)];
                        while matches!(self.peek(), Tok::Comma) {
                            self.advance(); // consume comma
                            let next_ident = self.expect_ident()?;
                            let next_field = self.parse_json_path_tail(next_ident);
                            let next_asc = match self.peek() {
                                Tok::Kw(Kw::Desc) => { self.advance(); false }
                                Tok::Kw(Kw::Asc)  => { self.advance(); true  }
                                _ => true,
                            };
                            cols.push((next_field, next_asc));
                        }
                        order_by = Some(OrderKey::Fields(cols));
                    }
                    ScoreExpr::Field(name) => {
                        // Single-column plain field sort.
                        order_by = Some(OrderKey::Fields(vec![(name, ascending)]));
                    }
                    // Everything else — arithmetic score expression.
                    other => {
                        order_by = Some(OrderKey::Expr(other, ascending));
                    }
                }
            }
        }

        let mut limit = None;
        let mut offset = None;
        loop {
            match self.peek() {
                Tok::Kw(Kw::Limit) => {
                    self.advance();
                    limit = Some(self.expect_num()? as usize);
                }
                Tok::Kw(Kw::Offset) => {
                    self.advance();
                    offset = Some(self.expect_num()? as usize);
                }
                _ => break,
            }
        }

        Ok(SelectStmt {
            fields,
            source,
            conditions,
            group_by,
            having,
            distinct,
            order_by,
            limit,
            offset,
            bm25_scores,
        })
    }

    fn parse_field_list(&mut self) -> Result<(Vec<String>, Vec<(String, String)>), SqlError> {
        if matches!(self.peek(), Tok::Star) {
            self.advance();
            return Ok((vec![], vec![]));
        }
        let mut fields = Vec::new();
        let mut bm25_scores = Vec::new();
        loop {
            let field = self.parse_field_or_bm25()?;
            match field {
                FieldOrBm25::Field(f) => fields.push(f),
                FieldOrBm25::Bm25 { field, query } => bm25_scores.push((field, query)),
            }
            if matches!(self.peek(), Tok::Comma) {
                self.advance();
            } else {
                break;
            }
        }
        Ok((fields, bm25_scores))
    }

    fn parse_field_or_bm25(&mut self) -> Result<FieldOrBm25, SqlError> {
        let ident = self.expect_ident()?;
        if ident.to_uppercase() == "BM25" && matches!(self.peek(), Tok::LParen) {
            self.advance();
            let field = self.expect_ident()?;
            self.expect_comma()?;
            let query = self.expect_str()?;
            self.expect_rparen()?;
            return Ok(FieldOrBm25::Bm25 { field, query });
        }
        if matches!(self.peek(), Tok::LParen) && ident.to_uppercase() == "ST_CENTROID" {
            self.advance();
            let field = self.expect_ident()?;
            self.expect_rparen()?;
            return Ok(FieldOrBm25::Field(format!("__ST_Centroid__{}", field)));
        }
        if matches!(self.peek(), Tok::LParen) {
            let func_upper = ident.to_uppercase();
            if matches!(
                func_upper.as_str(),
                "LENGTH" | "LEN" | "LOWER" | "UPPER" | "TRIM" | "LTRIM" | "RTRIM"
            ) {
                self.advance();
                let arg = self.expect_ident()?;
                self.expect_rparen()?;
                return Ok(FieldOrBm25::Field(format!(
                    "__FUNC__{}__{}",
                    func_upper, arg
                )));
            }
            if func_upper == "SUBSTRING" {
                self.advance();
                let arg1 = self.expect_ident()?;
                self.expect_comma()?;
                let start = self.expect_num()?;
                self.expect_comma()?;
                let len = self.expect_num()?;
                self.expect_rparen()?;
                return Ok(FieldOrBm25::Field(format!(
                    "__FUNC__SUBSTRING__{}__{}_{}",
                    arg1, start as usize, len as usize
                )));
            }
            if func_upper == "REPLACE" {
                self.advance();
                let arg1 = self.expect_ident()?;
                self.expect_comma()?;
                let old = self.expect_str()?;
                self.expect_comma()?;
                let new = self.expect_str()?;
                self.expect_rparen()?;
                return Ok(FieldOrBm25::Field(format!(
                    "__FUNC__REPLACE__{}__{}__{}",
                    arg1, old, new
                )));
            }
            if func_upper == "CONCAT" {
                self.advance();
                let arg1 = self.expect_ident()?;
                self.expect_comma()?;
                let arg2 = self.expect_str()?;
                self.expect_rparen()?;
                return Ok(FieldOrBm25::Field(format!(
                    "__FUNC__CONCAT__{}__{}",
                    arg1, arg2
                )));
            }
            if func_upper == "NOW" {
                self.advance();
                self.expect_rparen()?;
                return Ok(FieldOrBm25::Field("__FUNC__NOW__".to_string()));
            }
            if matches!(func_upper.as_str(), "YEAR" | "MONTH" | "DAY") {
                self.advance();
                let arg = self.expect_ident()?;
                self.expect_rparen()?;
                return Ok(FieldOrBm25::Field(format!(
                    "__FUNC__{}__{}",
                    func_upper, arg
                )));
            }
            if func_upper == "UUIDV4" {
                self.advance();
                self.expect_rparen()?;
                return Ok(FieldOrBm25::Field("__FUNC__UUIDV4__".to_string()));
            }
            if func_upper == "UUIDV5" {
                self.advance();
                let namespace = self.expect_str()?;
                self.expect_comma()?;
                let name = self.expect_str()?;
                self.expect_rparen()?;
                return Ok(FieldOrBm25::Field(format!(
                    "__FUNC__UUIDV5__{}__{}",
                    namespace, name
                )));
            }
            // Aggregate functions: COUNT(*|field), SUM(field), AVG(field), MIN(field), MAX(field)
            if matches!(
                func_upper.as_str(),
                "COUNT" | "SUM" | "AVG" | "MIN" | "MAX"
            ) {
                self.advance(); // consume (
                let arg = if func_upper == "COUNT" && matches!(self.peek(), Tok::Star) {
                    self.advance(); // consume *
                    "*".to_string()
                } else {
                    self.expect_ident()?
                };
                self.expect_rparen()?;
                let expr = format!("__AGG__{}__{}", func_upper, arg);
                // Check for AS alias
                let expr = if matches!(self.peek(), Tok::Kw(Kw::As)) {
                    self.advance();
                    let alias = self.expect_ident()?;
                    format!("__AS__{}\x01{}", alias, expr)
                } else {
                    expr
                };
                return Ok(FieldOrBm25::Field(expr));
            }
        }
        // Check for JSON path operators (-> / ->>) in SELECT field list.
        let ident = self.parse_json_path_tail(ident);
        // Check for AS alias
        let ident = if matches!(self.peek(), Tok::Kw(Kw::As)) {
            self.advance();
            let alias = self.expect_ident()?;
            format!("__AS__{}\x01{}", alias, ident)
        } else {
            ident
        };
        Ok(FieldOrBm25::Field(ident))
    }

    fn parse_source(&mut self) -> Result<Source, SqlError> {
        match self.peek().clone() {
            Tok::Kw(Kw::All) => {
                self.advance();
                Ok(Source::All)
            }
            Tok::Ident(name) => {
                self.advance();
                Ok(Source::Collection(name))
            }
            Tok::Eof => Err(SqlError::UnexpectedEnd {
                expected: "collection name or ALL",
            }),
            other => Err(SqlError::UnexpectedToken {
                expected: "collection name or ALL",
                got: format!("{other:?}"),
            }),
        }
    }

    fn parse_conditions(&mut self) -> Result<Vec<CondExpr>, SqlError> {
        // Parse: and_group (OR and_group)*
        // where and_group = condition (AND condition)*
        let first_group = self.parse_and_group()?;

        if !matches!(self.peek(), Tok::Kw(Kw::Or)) {
            // No OR — return conditions directly (most common path)
            return Ok(first_group);
        }

        // OR present — collect groups and wrap in a single Or CondExpr.
        let mut groups = vec![first_group];
        while matches!(self.peek(), Tok::Kw(Kw::Or)) {
            self.advance();
            groups.push(self.parse_and_group()?);
        }
        Ok(vec![CondExpr::Or(groups)])
    }

    fn parse_and_group(&mut self) -> Result<Vec<CondExpr>, SqlError> {
        let mut conds = vec![self.parse_condition()?];
        while matches!(self.peek(), Tok::Kw(Kw::And)) {
            self.advance();
            conds.push(self.parse_condition()?);
        }
        Ok(conds)
    }

    fn parse_condition(&mut self) -> Result<CondExpr, SqlError> {
        // Parenthesized group: (a OR b) AND c, (a AND b), etc.
        if matches!(self.peek(), Tok::LParen) {
            self.advance(); // consume (
            let mut inner = self.parse_conditions()?;
            self.expect_rparen()?;
            return Ok(match inner.len() {
                0 => return Err(SqlError::UnexpectedToken {
                    expected: "condition inside parens",
                    got: "empty parens".to_string(),
                }),
                1 => inner.remove(0),
                // Multiple AND conditions — wrap as single-branch OR (acts as AND)
                _ => CondExpr::Or(vec![inner]),
            });
        }

        // NOT prefix: NOT <condition>
        if matches!(self.peek(), Tok::Kw(Kw::Not)) {
            self.advance();
            let inner = self.parse_condition()?;
            return Ok(CondExpr::Not(Box::new(inner)));
        }

        let field = self.expect_ident()?;

        // IS NULL / IS NOT NULL
        if matches!(self.peek(), Tok::Kw(Kw::Is)) {
            self.advance(); // consume IS
            let negated = if matches!(self.peek(), Tok::Kw(Kw::Not)) {
                self.advance(); // consume NOT
                true
            } else {
                false
            };
            self.expect_kw(Kw::Null, "NULL")?;
            return Ok(CondExpr::IsNull {
                field: self.parse_json_path_tail(field),
                negated,
            });
        }

        // Aggregate functions in HAVING: COUNT(*) > 5, SUM(price) < 100, etc.
        // expect_ident() returns lowercase keyword names ("count", "sum", …)
        let upper = field.to_uppercase();
        if matches!(upper.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX")
            && matches!(self.peek(), Tok::LParen)
        {
            self.advance(); // consume (
            let arg = if upper == "COUNT" && matches!(self.peek(), Tok::Star) {
                self.advance();
                "*".to_string()
            } else {
                self.expect_ident()?
            };
            self.expect_rparen()?;
            let agg_key = format!("__AGG__{}__{}", upper, arg);
            // Parse comparison operator + value (same logic as normal field below)
            return match self.peek().clone() {
                Tok::Gt => {
                    self.advance();
                    let v = self.parse_value()?;
                    Ok(CondExpr::Compare { field: agg_key, op: CompareOp::Gt, value: v })
                }
                Tok::Lt => {
                    self.advance();
                    let v = self.parse_value()?;
                    Ok(CondExpr::Compare { field: agg_key, op: CompareOp::Lt, value: v })
                }
                Tok::Gte => {
                    self.advance();
                    let v = self.parse_value()?;
                    Ok(CondExpr::Compare { field: agg_key, op: CompareOp::Gte, value: v })
                }
                Tok::Lte => {
                    self.advance();
                    let v = self.parse_value()?;
                    Ok(CondExpr::Compare { field: agg_key, op: CompareOp::Lte, value: v })
                }
                Tok::Eq => {
                    self.advance();
                    let v = self.parse_value()?;
                    Ok(CondExpr::Compare { field: agg_key, op: CompareOp::Eq, value: v })
                }
                Tok::Neq => {
                    self.advance();
                    let v = self.parse_value()?;
                    Ok(CondExpr::Compare { field: agg_key, op: CompareOp::Neq, value: v })
                }
                other => Err(SqlError::UnexpectedToken {
                    expected: "comparison operator after aggregate function",
                    got: format!("{other:?}"),
                }),
            };
        }

        // Spatial function calls: ST_DWithin(...), ST_Contains(...), etc.
        if matches!(
            upper.as_str(),
            "ST_DWITHIN"
                | "ST_CONTAINS"
                | "ST_WITHIN"
                | "ST_INTERSECTS"
                | "ST_DISTANCE"
                | "ST_LENGTH"
                | "ST_AREA"
        ) {
            return self.parse_spatial_function(&upper);
        }

        // VECTOR_NEAR similarity search: VECTOR_NEAR(field, [f32, ...], k)
        if upper == "VECTOR_NEAR" {
            return self.parse_vector_near_function();
        }

        // BM25 full-text search: BM25(field, 'query') > min_score
        if upper == "BM25" {
            let bm25_expr = self.parse_bm25_function()?;
            // After BM25(field, 'query'), expect > min_score
            match self.peek().clone() {
                Tok::Gt => {
                    self.advance();
                    let min_score = self.expect_num()?;
                    if let CondExpr::Bm25Func { field, query } = bm25_expr {
                        Ok(CondExpr::Bm25 {
                            field,
                            query,
                            min_score,
                        })
                    } else {
                        unreachable!()
                    }
                }
                Tok::Gte => {
                    self.advance();
                    let min_score = self.expect_num()?;
                    if let CondExpr::Bm25Func { field, query } = bm25_expr {
                        Ok(CondExpr::Bm25 {
                            field,
                            query,
                            min_score, // For >=, we use the value directly (user can adjust)
                        })
                    } else {
                        unreachable!()
                    }
                }
                other => Err(SqlError::UnexpectedToken {
                    expected: "> or >= after BM25(...)",
                    got: format!("{other:?}"),
                }),
            }
        } else {
            // Resolve any JSON path operators (-> / ->>) after the field name.
            let field = self.parse_json_path_tail(field);

            match self.peek().clone() {
                Tok::Eq => {
                    self.advance();
                    let v = self.parse_value()?;
                    Ok(CondExpr::Compare {
                        field,
                        op: CompareOp::Eq,
                        value: v,
                    })
                }
                Tok::Neq => {
                    self.advance();
                    let v = self.parse_value()?;
                    Ok(CondExpr::Compare {
                        field,
                        op: CompareOp::Neq,
                        value: v,
                    })
                }
                Tok::Gt => {
                    self.advance();
                    let v = self.parse_value()?;
                    Ok(CondExpr::Compare {
                        field,
                        op: CompareOp::Gt,
                        value: v,
                    })
                }
                Tok::Lt => {
                    self.advance();
                    let v = self.parse_value()?;
                    Ok(CondExpr::Compare {
                        field,
                        op: CompareOp::Lt,
                        value: v,
                    })
                }
                Tok::Gte => {
                    self.advance();
                    let v = self.parse_value()?;
                    Ok(CondExpr::Compare {
                        field,
                        op: CompareOp::Gte,
                        value: v,
                    })
                }
                Tok::Lte => {
                    self.advance();
                    let v = self.parse_value()?;
                    Ok(CondExpr::Compare {
                        field,
                        op: CompareOp::Lte,
                        value: v,
                    })
                }
                Tok::Kw(Kw::Between) => {
                    self.advance();
                    let lo = self.expect_num()?;
                    self.expect_kw(Kw::And, "AND")?;
                    let hi = self.expect_num()?;
                    Ok(CondExpr::Between { field, lo, hi })
                }
                Tok::Kw(Kw::In) => {
                    self.advance();
                    self.expect_lparen()?;
                    let mut values = vec![self.parse_value()?];
                    while matches!(self.peek(), Tok::Comma) {
                        self.advance();
                        values.push(self.parse_value()?);
                    }
                    self.expect_rparen()?;
                    Ok(CondExpr::In { field, values })
                }
                Tok::Kw(Kw::Not) => {
                    self.advance(); // consume NOT
                    self.expect_kw(Kw::In, "IN")?;
                    self.expect_lparen()?;
                    let mut values = vec![self.parse_value()?];
                    while matches!(self.peek(), Tok::Comma) {
                        self.advance();
                        values.push(self.parse_value()?);
                    }
                    self.expect_rparen()?;
                    Ok(CondExpr::Not(Box::new(CondExpr::In { field, values })))
                }
                Tok::Kw(Kw::Like) => {
                    self.advance();
                    let pattern = self.expect_str()?;
                    Ok(CondExpr::Like {
                        field,
                        pattern,
                        case_insensitive: false,
                    })
                }
                Tok::Kw(Kw::ILike) => {
                    self.advance();
                    let pattern = self.expect_str()?;
                    Ok(CondExpr::Like {
                        field,
                        pattern,
                        case_insensitive: true,
                    })
                }
                Tok::Eof => Err(SqlError::UnexpectedEnd {
                    expected: "comparison operator",
                }),
                other => Err(SqlError::UnexpectedToken {
                    expected:
                        "comparison operator (=, !=, <>, >, <, >=, <=, BETWEEN, IN, NOT IN, LIKE, ILIKE)",
                    got: format!("{other:?}"),
                }),
            }
        }
    }

    // ── Spatial parsing ─────────────────────────────────────────────────

    /// Parse a spatial function call after the function name has been consumed.
    fn parse_spatial_function(&mut self, func: &str) -> Result<CondExpr, SqlError> {
        self.expect_lparen()?;
        let _geom_field = self.expect_ident()?; // "geometry" — consumed but ignored
        self.expect_comma()?;

        match func {
            "ST_DWITHIN" => {
                // ST_DWithin(geometry, POINT(lon lat), distance)
                let (lon, lat) = self.parse_point_literal()?;
                self.expect_comma()?;
                let distance = self.expect_num()?;
                self.expect_rparen()?;
                Ok(CondExpr::StDWithin {
                    lat,
                    lon,
                    distance_km: distance,
                })
            }
            "ST_CONTAINS" => {
                // POINT → StContainsPoint, POLYGON → StContains
                let next = self.expect_ident()?;
                match next.to_uppercase().as_str() {
                    "POINT" => {
                        self.expect_lparen()?;
                        let lon = self.expect_num()?;
                        let lat = self.expect_num()?;
                        self.expect_rparen()?;
                        self.expect_rparen()?;
                        Ok(CondExpr::StContainsPoint { lat, lon })
                    }
                    "POLYGON" => {
                        let ring = self.parse_polygon_literal()?;
                        self.expect_rparen()?;
                        Ok(CondExpr::StContains { ring })
                    }
                    _ => Err(SqlError::UnexpectedToken {
                        expected: "POINT or POLYGON",
                        got: next,
                    }),
                }
            }
            "ST_WITHIN" => {
                let ring = self.parse_polygon_with_keyword()?;
                self.expect_rparen()?;
                Ok(CondExpr::StWithin { ring })
            }
            "ST_INTERSECTS" => {
                let ring = self.parse_polygon_with_keyword()?;
                self.expect_rparen()?;
                Ok(CondExpr::StIntersects { ring })
            }
            "ST_DISTANCE" => {
                // ST_Distance(geometry, POINT(lon lat), max_km)
                let field = self.expect_ident()?;
                self.expect_comma()?;
                let (lon, lat) = self.parse_point_literal()?;
                self.expect_comma()?;
                let max_km = self.expect_num()?;
                self.expect_rparen()?;
                Ok(CondExpr::StDistance {
                    field,
                    lat,
                    lon,
                    max_km,
                })
            }
            "ST_LENGTH" => {
                // ST_Length(geometry) < min_km
                let field = self.expect_ident()?;
                self.expect_comma()?;
                let min_km = self.expect_num()?;
                self.expect_rparen()?;
                Ok(CondExpr::StLength { field, min_km })
            }
            "ST_AREA" => {
                // ST_Area(geometry) > min_km2
                let field = self.expect_ident()?;
                self.expect_comma()?;
                let min_km2 = self.expect_num()?;
                self.expect_rparen()?;
                Ok(CondExpr::StArea { field, min_km2 })
            }
            _ => Err(SqlError::UnexpectedToken {
                expected: "spatial function",
                got: func.to_string(),
            }),
        }
    }

    /// Parse BM25(field, 'query') → CondExpr::Bm25Func.
    /// The BM25 keyword has already been consumed by parse_condition.
    fn parse_bm25_function(&mut self) -> Result<CondExpr, SqlError> {
        self.expect_lparen()?;
        let field = self.expect_ident()?;
        self.expect_comma()?;
        let query = self.expect_str()?;
        self.expect_rparen()?;
        Ok(CondExpr::Bm25Func { field, query })
    }

    /// Parse `VECTOR_NEAR(field, [f32, ...], k)`.
    fn parse_vector_near_function(&mut self) -> Result<CondExpr, SqlError> {
        self.expect_lparen()?;
        let field = self.expect_ident()?;
        self.expect_comma()?;
        let query = self.parse_f32_array()?;
        self.expect_comma()?;
        let k = self.expect_num()? as usize;
        self.expect_rparen()?;
        Ok(CondExpr::VectorNear { field, query, k })
    }

    /// Parse a JSON-style f32 array literal: `[ num, num, ... ]`.
    fn parse_f32_array(&mut self) -> Result<Vec<f32>, SqlError> {
        self.expect_lbracket()?;
        let mut values = Vec::new();
        loop {
            match self.peek().clone() {
                Tok::RBracket => {
                    self.advance();
                    break;
                }
                Tok::Comma => {
                    self.advance();
                }
                Tok::Num(n) => {
                    self.advance();
                    values.push(n as f32);
                }
                Tok::Dash => {
                    self.advance();
                    match self.advance() {
                        Tok::Num(n) => values.push(-(n as f32)),
                        other => {
                            return Err(SqlError::UnexpectedToken {
                                expected: "number after -",
                                got: format!("{other:?}"),
                            })
                        }
                    }
                }
                other => {
                    return Err(SqlError::UnexpectedToken {
                        expected: "number or ]",
                        got: format!("{other:?}"),
                    })
                }
            }
        }
        Ok(values)
    }

    // ── Score expression parser (arithmetic ORDER BY) ─────────────────────────

    /// Entry point: parse a score expression with `+` / `-` at the top level.
    ///
    /// Grammar:
    /// ```text
    /// score_expr  = score_mul ( ('+' | '-') score_mul )*
    /// score_mul   = score_unary ( ('*' | '/') score_unary )*
    /// score_unary = '-' score_unary | score_atom
    /// score_atom  = '(' score_expr ')'
    ///             | BM25 '(' ident ',' string ')'
    ///             | VECTOR_COSINE '(' ident ',' f32_array ')'
    ///             | VECTOR_L2 '(' ident ',' f32_array ')'
    ///             | VECTOR_DOT '(' ident ',' f32_array ')'
    ///             | VECTOR_L1 '(' ident ',' f32_array ')'
    ///             | ST_DISTANCE_KM '(' ident ',' POINT '(' lon lat ')' ')'
    ///             | ident [ json_path_tail ]
    ///             | number
    /// ```
    fn parse_score_expr(&mut self) -> Result<ScoreExpr, SqlError> {
        let mut left = self.parse_score_mul()?;
        loop {
            match self.peek() {
                Tok::Plus => {
                    self.advance();
                    let right = self.parse_score_mul()?;
                    left = ScoreExpr::Add(Box::new(left), Box::new(right));
                }
                Tok::Dash => {
                    // Guard: don't consume '->' or '->>' (Arrow / LongArrow are
                    // their own tokens, so plain Dash here is always subtraction).
                    self.advance();
                    let right = self.parse_score_mul()?;
                    left = ScoreExpr::Sub(Box::new(left), Box::new(right));
                }
                _ => break,
            }
        }
        Ok(left)
    }

    fn parse_score_mul(&mut self) -> Result<ScoreExpr, SqlError> {
        let mut left = self.parse_score_unary()?;
        loop {
            match self.peek() {
                Tok::Star => {
                    self.advance();
                    let right = self.parse_score_unary()?;
                    left = ScoreExpr::Mul(Box::new(left), Box::new(right));
                }
                Tok::Slash => {
                    self.advance();
                    let right = self.parse_score_unary()?;
                    left = ScoreExpr::Div(Box::new(left), Box::new(right));
                }
                _ => break,
            }
        }
        Ok(left)
    }

    fn parse_score_unary(&mut self) -> Result<ScoreExpr, SqlError> {
        if matches!(self.peek(), Tok::Dash) {
            self.advance();
            let inner = self.parse_score_unary()?;
            return Ok(ScoreExpr::Neg(Box::new(inner)));
        }
        self.parse_score_atom()
    }

    fn parse_score_atom(&mut self) -> Result<ScoreExpr, SqlError> {
        match self.peek().clone() {
            Tok::Num(n) => {
                self.advance();
                Ok(ScoreExpr::Lit(n))
            }
            Tok::LParen => {
                self.advance(); // consume '('
                let inner = self.parse_score_expr()?;
                self.expect_rparen()?;
                Ok(inner)
            }
            Tok::Ident(name) => {
                self.advance();
                match name.to_ascii_uppercase().as_str() {
                    "BM25" => {
                        self.expect_lparen()?;
                        let field = self.expect_ident()?;
                        self.expect_comma()?;
                        let query = self.expect_str()?;
                        self.expect_rparen()?;
                        Ok(ScoreExpr::Bm25 { field, query })
                    }
                    "VECTOR_COSINE" => {
                        self.expect_lparen()?;
                        let field = self.expect_ident()?;
                        self.expect_comma()?;
                        let query = self.parse_f32_array()?;
                        self.expect_rparen()?;
                        Ok(ScoreExpr::VectorCosine { field, query })
                    }
                    "VECTOR_L2" => {
                        self.expect_lparen()?;
                        let field = self.expect_ident()?;
                        self.expect_comma()?;
                        let query = self.parse_f32_array()?;
                        self.expect_rparen()?;
                        Ok(ScoreExpr::VectorL2 { field, query })
                    }
                    "VECTOR_DOT" => {
                        self.expect_lparen()?;
                        let field = self.expect_ident()?;
                        self.expect_comma()?;
                        let query = self.parse_f32_array()?;
                        self.expect_rparen()?;
                        Ok(ScoreExpr::VectorDot { field, query })
                    }
                    "VECTOR_L1" => {
                        self.expect_lparen()?;
                        let field = self.expect_ident()?;
                        self.expect_comma()?;
                        let query = self.parse_f32_array()?;
                        self.expect_rparen()?;
                        Ok(ScoreExpr::VectorL1 { field, query })
                    }
                    "ST_DISTANCE_KM" => {
                        // ST_DISTANCE_KM(field, POINT(lon lat))
                        self.expect_lparen()?;
                        let field = self.expect_ident()?;
                        self.expect_comma()?;
                        let (lon, lat) = self.parse_point_literal()?;
                        self.expect_rparen()?;
                        Ok(ScoreExpr::StDistance { field, lat, lon })
                    }
                    _ => {
                        // Plain field name, with optional JSON path (col->'key'->>'leaf').
                        let field = self.parse_json_path_tail(name);
                        Ok(ScoreExpr::Field(field))
                    }
                }
            }
            other => Err(SqlError::UnexpectedToken {
                expected: "score expression (number, field, BM25, VECTOR_COSINE, VECTOR_L2, VECTOR_DOT, VECTOR_L1, ST_DISTANCE_KM, or parentheses)",
                got: format!("{other:?}"),
            }),
        }
    }

    /// Parse `POINT(lon lat)` → `(lon, lat)`.
    fn parse_point_literal(&mut self) -> Result<(f64, f64), SqlError> {
        let kw = self.expect_ident()?;
        if kw.to_uppercase() != "POINT" {
            return Err(SqlError::UnexpectedToken {
                expected: "POINT",
                got: kw,
            });
        }
        self.expect_lparen()?;
        let lon = self.expect_num()?;
        let lat = self.expect_num()?;
        self.expect_rparen()?;
        Ok((lon, lat))
    }

    /// Parse `((lon1 lat1, lon2 lat2, ...))` → `Vec<[lat, lon]>`.
    fn parse_polygon_literal(&mut self) -> Result<Vec<[f64; 2]>, SqlError> {
        self.expect_lparen()?; // outer (
        self.expect_lparen()?; // inner (
        let mut ring = Vec::new();
        loop {
            let lon = self.expect_num()?;
            let lat = self.expect_num()?;
            ring.push([lat, lon]); // internal format: [lat, lon]
            if matches!(self.peek(), Tok::Comma) {
                self.advance();
            } else {
                break;
            }
        }
        self.expect_rparen()?; // inner )
        self.expect_rparen()?; // outer )
        Ok(ring)
    }

    /// Parse `POLYGON((lon1 lat1, ...))` → `Vec<[lat, lon]>`.
    fn parse_polygon_with_keyword(&mut self) -> Result<Vec<[f64; 2]>, SqlError> {
        let kw = self.expect_ident()?;
        if kw.to_uppercase() != "POLYGON" {
            return Err(SqlError::UnexpectedToken {
                expected: "POLYGON",
                got: kw,
            });
        }
        self.parse_polygon_literal()
    }

    /// Parse: INSERT INTO collection (_key, field, ...) VALUES ('key', val, ...)
    /// Called after INSERT has already been consumed.
    fn parse_insert_node(&mut self) -> Result<InsertStmt, SqlError> {
        self.expect_kw(Kw::Into, "INTO")?;
        let collection = self.expect_ident()?;
        self.expect_lparen()?;
        let mut fields = vec![self.expect_ident()?];
        while matches!(self.peek(), Tok::Comma) {
            self.advance();
            fields.push(self.expect_ident()?);
        }
        self.expect_rparen()?;
        self.expect_kw(Kw::Values, "VALUES")?;
        self.expect_lparen()?;
        let mut values = vec![self.parse_value()?];
        while matches!(self.peek(), Tok::Comma) {
            self.advance();
            values.push(self.parse_value()?);
        }
        self.expect_rparen()?;
        Ok(InsertStmt {
            collection,
            fields,
            values,
        })
    }

    /// Parse: ('from')-[:KIND {strength: n, key: val}]->('to') [, ...]
    /// Called after INSERT has already been consumed.
    fn parse_insert_edge(&mut self) -> Result<Vec<EdgeInsert>, SqlError> {
        let mut edges = Vec::new();
        loop {
            // (from)
            self.expect_lparen()?;
            let from = self.expect_str()?;
            self.expect_rparen()?;
            // -[
            match self.peek() {
                Tok::Dash => {
                    self.advance();
                }
                Tok::Eof => return Err(SqlError::UnexpectedEnd { expected: "-" }),
                other => {
                    return Err(SqlError::UnexpectedToken {
                        expected: "-",
                        got: format!("{other:?}"),
                    })
                }
            }
            match self.peek() {
                Tok::LBracket => {
                    self.advance();
                }
                Tok::Eof => return Err(SqlError::UnexpectedEnd { expected: "[" }),
                other => {
                    return Err(SqlError::UnexpectedToken {
                        expected: "[",
                        got: format!("{other:?}"),
                    })
                }
            }
            // :KIND
            match self.peek() {
                Tok::Colon => {
                    self.advance();
                }
                Tok::Eof => return Err(SqlError::UnexpectedEnd { expected: ":" }),
                other => {
                    return Err(SqlError::UnexpectedToken {
                        expected: ":",
                        got: format!("{other:?}"),
                    })
                }
            }
            let edge_type = self.expect_ident()?;
            // Optional {props}
            let mut strength = 1.0f32;
            let mut props_json = None;
            if matches!(self.peek(), Tok::LBrace) {
                self.advance();
                let mut map = serde_json::Map::new();
                while !matches!(self.peek(), Tok::RBrace | Tok::Eof) {
                    let key = self.expect_ident()?;
                    match self.peek() {
                        Tok::Colon => {
                            self.advance();
                        }
                        Tok::Eof => return Err(SqlError::UnexpectedEnd { expected: ":" }),
                        other => {
                            return Err(SqlError::UnexpectedToken {
                                expected: ":",
                                got: format!("{other:?}"),
                            })
                        }
                    }
                    let val = self.parse_value()?;
                    map.insert(key, val);
                    if matches!(self.peek(), Tok::Comma) {
                        self.advance();
                    }
                }
                match self.peek() {
                    Tok::RBrace => {
                        self.advance();
                    }
                    _ => return Err(SqlError::UnexpectedEnd { expected: "}" }),
                }
                // Extract strength from props, default 1.0
                if let Some(s) = map.remove("strength") {
                    strength = s.as_f64().unwrap_or(1.0) as f32;
                }
                if !map.is_empty() {
                    props_json = Some(
                        serde_json::to_string(&Value::Object(map))
                            .map_err(|e| SqlError::InvalidValue(e.to_string()))?,
                    );
                }
            }
            // ]->
            match self.peek() {
                Tok::RBracket => {
                    self.advance();
                }
                Tok::Eof => return Err(SqlError::UnexpectedEnd { expected: "]" }),
                other => {
                    return Err(SqlError::UnexpectedToken {
                        expected: "]",
                        got: format!("{other:?}"),
                    })
                }
            }
            match self.peek() {
                Tok::Arrow => {
                    self.advance();
                }
                Tok::Eof => return Err(SqlError::UnexpectedEnd { expected: "->" }),
                other => {
                    return Err(SqlError::UnexpectedToken {
                        expected: "->",
                        got: format!("{other:?}"),
                    })
                }
            }
            // (to)
            self.expect_lparen()?;
            let to = self.expect_str()?;
            self.expect_rparen()?;

            edges.push(EdgeInsert {
                from,
                to,
                edge_type,
                strength,
                props_json,
            });

            if matches!(self.peek(), Tok::Comma) {
                self.advance();
            } else {
                break;
            }
        }
        Ok(edges)
    }

    /// Parse: DELETE FROM collection|ALL [WHERE ...]
    /// Called after DELETE has already been consumed.
    fn parse_delete_node(&mut self) -> Result<DeleteStmt, SqlError> {
        self.expect_kw(Kw::From, "FROM")?;
        let source = self.parse_source()?;
        let mut conditions = Vec::new();
        if matches!(self.peek(), Tok::Kw(Kw::Where)) {
            self.advance();
            conditions = self.parse_conditions()?;
        }
        Ok(DeleteStmt { source, conditions })
    }

    /// Parse: ('from')-[:KIND]->('to') [, ...]
    /// Called after DELETE has already been consumed.
    fn parse_delete_edge(&mut self) -> Result<Vec<EdgeDelete>, SqlError> {
        let mut edges = Vec::new();
        loop {
            // (from)
            self.expect_lparen()?;
            let from = self.expect_str()?;
            self.expect_rparen()?;
            // -[
            match self.peek() {
                Tok::Dash => {
                    self.advance();
                }
                Tok::Eof => return Err(SqlError::UnexpectedEnd { expected: "-" }),
                other => {
                    return Err(SqlError::UnexpectedToken {
                        expected: "-",
                        got: format!("{other:?}"),
                    })
                }
            }
            match self.peek() {
                Tok::LBracket => {
                    self.advance();
                }
                Tok::Eof => return Err(SqlError::UnexpectedEnd { expected: "[" }),
                other => {
                    return Err(SqlError::UnexpectedToken {
                        expected: "[",
                        got: format!("{other:?}"),
                    })
                }
            }
            // :KIND
            match self.peek() {
                Tok::Colon => {
                    self.advance();
                }
                Tok::Eof => return Err(SqlError::UnexpectedEnd { expected: ":" }),
                other => {
                    return Err(SqlError::UnexpectedToken {
                        expected: ":",
                        got: format!("{other:?}"),
                    })
                }
            }
            let edge_type = self.expect_ident()?;
            // ]->
            match self.peek() {
                Tok::RBracket => {
                    self.advance();
                }
                Tok::Eof => return Err(SqlError::UnexpectedEnd { expected: "]" }),
                other => {
                    return Err(SqlError::UnexpectedToken {
                        expected: "]",
                        got: format!("{other:?}"),
                    })
                }
            }
            match self.peek() {
                Tok::Arrow => {
                    self.advance();
                }
                Tok::Eof => return Err(SqlError::UnexpectedEnd { expected: "->" }),
                other => {
                    return Err(SqlError::UnexpectedToken {
                        expected: "->",
                        got: format!("{other:?}"),
                    })
                }
            }
            // (to)
            self.expect_lparen()?;
            let to = self.expect_str()?;
            self.expect_rparen()?;

            edges.push(EdgeDelete {
                from,
                to,
                edge_type,
            });

            if matches!(self.peek(), Tok::Comma) {
                self.advance();
            } else {
                break;
            }
        }
        Ok(edges)
    }

    fn parse_update(&mut self) -> Result<UpdateStmt, SqlError> {
        self.expect_kw(Kw::Update, "UPDATE")?;
        let collection = self.expect_ident()?;
        self.expect_kw(Kw::Set, "SET")?;
        // Parse one or more  field = value  pairs separated by commas
        let mut updates = Vec::new();
        loop {
            let field = self.expect_ident()?;
            match self.peek() {
                Tok::Eq => {
                    self.advance();
                }
                Tok::Eof => return Err(SqlError::UnexpectedEnd { expected: "=" }),
                other => {
                    return Err(SqlError::UnexpectedToken {
                        expected: "=",
                        got: format!("{other:?}"),
                    })
                }
            }
            let value = self.parse_value()?;
            updates.push((field, value));
            if !matches!(self.peek(), Tok::Comma) {
                break;
            }
            self.advance(); // consume comma
        }
        let mut conditions = Vec::new();
        if matches!(self.peek(), Tok::Kw(Kw::Where)) {
            self.advance();
            conditions = self.parse_conditions()?;
        }
        Ok(UpdateStmt {
            collection,
            updates,
            conditions,
        })
    }

    fn parse_create_table(&mut self) -> Result<TableSchema, SqlError> {
        self.expect_kw(Kw::Table, "TABLE")?;
        let collection = self.expect_ident()?;
        self.expect_lparen()?;

        let mut fields = Vec::new();
        loop {
            let field_name = self.expect_ident()?;
            let ty = self.parse_type()?;
            let is_primary_key =
                if field_name == "_key" && matches!(self.peek(), Tok::Kw(Kw::Primary)) {
                    self.expect_kw(Kw::Primary, "PRIMARY")?;
                    self.expect_kw(Kw::Key, "KEY")?;
                    true
                } else {
                    false
                };
            let is_timestamptz = field_name.ends_with("_at") || field_name.ends_with("_time");
            let default_now = is_timestamptz;
            fields.push(FieldDef {
                name: field_name,
                ty,
                is_primary_key,
                is_timestamptz,
                default_now,
            });
            if matches!(self.peek(), Tok::Comma) {
                self.advance();
            } else {
                break;
            }
        }
        self.expect_rparen()?;

        let mut schema = TableSchema {
            collection,
            fields,
            indexes: IndexHint::default(),
        };

        schema.fields.push(FieldDef {
            name: "_created_unix".to_string(),
            ty: FieldType::Integer,
            is_primary_key: false,
            is_timestamptz: false,
            default_now: true,
        });
        schema.fields.push(FieldDef {
            name: "_updated_unix".to_string(),
            ty: FieldType::Integer,
            is_primary_key: false,
            is_timestamptz: false,
            default_now: true,
        });

        Ok(schema)
    }

    /// Parse: ALTER TABLE collection <op>
    /// Called after ALTER has already been consumed by parse_mutation.
    ///
    /// Supported forms:
    /// - `ADD [COLUMN] name type [PRIMARY KEY] [NOT NULL]`
    /// - `DROP [COLUMN] [IF EXISTS] name`
    /// - `RENAME COLUMN old TO new`
    /// - `RENAME TO new_name`
    /// - `ALTER [COLUMN] name TYPE new_type`
    fn parse_alter_table(&mut self) -> Result<CompiledMutation, SqlError> {
        self.expect_kw(Kw::Table, "TABLE")?;
        let collection = self.expect_ident()?;

        match self.peek().clone() {
            // ADD [COLUMN] name type [PRIMARY KEY] [NOT NULL]
            Tok::Kw(Kw::Add) => {
                self.advance(); // consume ADD
                if matches!(self.peek(), Tok::Kw(Kw::Column)) {
                    self.advance(); // optional COLUMN
                }
                let col_name = self.expect_ident()?;
                let ty = self.parse_type()?;
                let mut is_primary_key = false;
                loop {
                    match self.peek().clone() {
                        Tok::Kw(Kw::Primary) => {
                            self.advance();
                            self.expect_kw(Kw::Key, "KEY")?;
                            is_primary_key = true;
                        }
                        Tok::Kw(Kw::Not) => {
                            self.advance();
                            // consume NULL — NOT NULL is noted but we don't track nullability yet
                            if matches!(self.peek(), Tok::Kw(Kw::Null)) {
                                self.advance();
                            }
                        }
                        _ => break,
                    }
                }
                let is_timestamptz = col_name.ends_with("_at") || col_name.ends_with("_time");
                let def = FieldDef {
                    name: col_name,
                    ty,
                    is_primary_key,
                    is_timestamptz,
                    default_now: is_timestamptz,
                };
                Ok(CompiledMutation::AlterTable {
                    collection,
                    op: AlterTableOp::AddColumn { def },
                })
            }

            // DROP [COLUMN] [IF EXISTS] name
            Tok::Kw(Kw::Drop) => {
                self.advance(); // consume DROP
                if matches!(self.peek(), Tok::Kw(Kw::Column)) {
                    self.advance(); // optional COLUMN
                }
                let if_exists = if matches!(self.peek(), Tok::Kw(Kw::If)) {
                    self.advance();
                    self.expect_kw(Kw::Exists, "EXISTS")?;
                    true
                } else {
                    false
                };
                let name = self.expect_ident()?;
                Ok(CompiledMutation::AlterTable {
                    collection,
                    op: AlterTableOp::DropColumn { name, if_exists },
                })
            }

            // RENAME COLUMN old TO new  |  RENAME TO new_name
            Tok::Kw(Kw::Rename) => {
                self.advance(); // consume RENAME
                match self.peek().clone() {
                    Tok::Kw(Kw::Column) => {
                        self.advance(); // consume COLUMN
                        let old_name = self.expect_ident()?;
                        self.expect_kw(Kw::To, "TO")?;
                        let new_name = self.expect_ident()?;
                        Ok(CompiledMutation::AlterTable {
                            collection,
                            op: AlterTableOp::RenameColumn { old_name, new_name },
                        })
                    }
                    Tok::Kw(Kw::To) => {
                        self.advance(); // consume TO
                        let new_name = self.expect_ident()?;
                        Ok(CompiledMutation::AlterTable {
                            collection,
                            op: AlterTableOp::RenameTable { new_name },
                        })
                    }
                    other => Err(SqlError::UnexpectedToken {
                        expected: "COLUMN or TO",
                        got: format!("{other:?}"),
                    }),
                }
            }

            // ALTER [COLUMN] name TYPE new_type
            Tok::Kw(Kw::Alter) => {
                self.advance(); // consume ALTER
                if matches!(self.peek(), Tok::Kw(Kw::Column)) {
                    self.advance(); // optional COLUMN
                }
                let col_name = self.expect_ident()?;
                // TYPE is not a registered keyword — consumed as ident
                let type_kw = self.expect_ident()?;
                if type_kw.to_ascii_uppercase() != "TYPE" {
                    return Err(SqlError::UnexpectedToken {
                        expected: "TYPE",
                        got: type_kw,
                    });
                }
                let ty = self.parse_type()?;
                Ok(CompiledMutation::AlterTable {
                    collection,
                    op: AlterTableOp::AlterColumnType { name: col_name, ty },
                })
            }

            other => Err(SqlError::UnexpectedToken {
                expected: "ADD, DROP, RENAME, or ALTER",
                got: format!("{other:?}"),
            }),
        }
    }

    /// Parse: CREATE INDEX [name] ON collection USING method (field [, ...])
    /// Called after CREATE has already been consumed by parse_mutation.
    fn parse_create_index(&mut self) -> Result<CompiledMutation, SqlError> {
        self.expect_kw(Kw::Index, "INDEX")?;

        // Optional index name — if the next token is NOT `ON`, it's a name.
        let name = if !matches!(self.peek(), Tok::Kw(Kw::On)) {
            Some(self.expect_ident()?)
        } else {
            None
        };

        self.expect_kw(Kw::On, "ON")?;
        let collection = self.expect_ident()?;
        self.expect_kw(Kw::Using, "USING")?;
        let method_str = self.expect_ident()?;
        let method = match method_str.to_lowercase().as_str() {
            "btree"   => IndexMethod::Btree,
            "hash"    => IndexMethod::Hash,
            "gin"     => IndexMethod::Gin,
            "gist"    => IndexMethod::Gist,
            "bm25"    => IndexMethod::Bm25,
            "spatial" => IndexMethod::Spatial,
            "hnsw"    => IndexMethod::Hnsw,
            other => return Err(SqlError::UnexpectedToken {
                expected: "btree, hash, gin, gist, bm25, spatial, or hnsw",
                got: other.to_string(),
            }),
        };

        self.expect_lparen()?;
        let mut fields = vec![self.expect_ident()?];
        while matches!(self.peek(), Tok::Comma) {
            self.advance();
            fields.push(self.expect_ident()?);
        }
        self.expect_rparen()?;

        Ok(CompiledMutation::CreateIndex { name, collection, method, fields })
    }

    fn parse_type(&mut self) -> Result<FieldType, SqlError> {
        let ident = self.expect_ident()?;
        match ident.to_uppercase().as_str() {
            "TEXT" => Ok(FieldType::Text),
            "INTEGER" => Ok(FieldType::Integer),
            "REAL" => Ok(FieldType::Real),
            "TIMESTAMPTZ" => Ok(FieldType::Timestamptz),
            "GEO" => Ok(FieldType::Geo),
            "VECTOR" => Ok(FieldType::Vector),
            "JSON" => Ok(FieldType::Json),
            _ => Err(SqlError::UnexpectedToken {
                expected: "TEXT, INTEGER, REAL, TIMESTAMPTZ, GEO, VECTOR, or JSON",
                got: ident,
            }),
        }
    }

    fn parse_with_options(&mut self) -> Result<IndexHint, SqlError> {
        self.expect_lparen()?;
        let mut hints = IndexHint::default();
        loop {
            let ident = self.expect_ident()?;
            match ident.to_lowercase().as_str() {
                "hash" => {
                    self.expect_colon()?;
                    hints.hash = self.parse_string_list()?;
                }
                "range" => {
                    self.expect_colon()?;
                    hints.range = self.parse_string_list()?;
                }
                "fulltext" => {
                    self.expect_colon()?;
                    hints.fulltext = self.parse_string_list()?;
                }
                "bm25" => {
                    self.expect_colon()?;
                    hints.bm25 = self.parse_string_list()?;
                }
                "spatial" => {
                    self.expect_colon()?;
                    hints.spatial = self.parse_string_list()?;
                }
                "vector" => {
                    self.expect_colon()?;
                    hints.vector = self.parse_string_list()?;
                }
                _ => {
                    return Err(SqlError::UnexpectedToken {
                        expected: "hash, range, fulltext, bm25, spatial, or vector",
                        got: ident,
                    })
                }
            }
            if matches!(self.peek(), Tok::Comma) {
                self.advance();
            } else {
                break;
            }
        }
        self.expect_rparen()?;
        Ok(hints)
    }

    fn parse_string_list(&mut self) -> Result<Vec<String>, SqlError> {
        self.expect_lbracket()?;
        let mut items = Vec::new();
        loop {
            let s = self.expect_str()?;
            items.push(s);
            if matches!(self.peek(), Tok::Comma) {
                self.advance();
            } else {
                break;
            }
        }
        self.expect_rbracket()?;
        Ok(items)
    }

    // ── MATCH parser ─────────────────────────────────────────────────────────

    /// Parse: MATCH (node)-[edge]->(node) [WHERE ...] RETURN vars [LIMIT n]
    fn parse_match(&mut self) -> Result<MatchStmt, SqlError> {
        self.expect_kw(Kw::Match, "MATCH")?;

        let start = self.parse_match_node()?;
        let edge = self.parse_match_edge()?;
        let end = self.parse_match_node()?;

        let mut conditions = Vec::new();
        if matches!(self.peek(), Tok::Kw(Kw::Where)) {
            self.advance();
            conditions = self.parse_match_conditions()?;
        }

        self.expect_kw(Kw::Return, "RETURN")?;
        let return_vars = self.parse_return_list()?;

        let mut limit = None;
        if matches!(self.peek(), Tok::Kw(Kw::Limit)) {
            self.advance();
            limit = Some(self.expect_num()? as usize);
        }

        Ok(MatchStmt {
            start,
            edge,
            end,
            conditions,
            return_vars,
            limit,
        })
    }

    /// Parse: MATCH (src) [WHERE conditions] INSERT (src)-[:kind]->(tgt)
    fn parse_match_insert(&mut self) -> Result<CompiledMutation, SqlError> {
        self.expect_kw(Kw::Match, "MATCH")?;

        // Parse source node with variable
        let src_node = self.parse_match_node()?;
        if src_node.var.is_none() {
            return Err(SqlError::UnexpectedToken {
                expected: "named node like (p:people) in MATCH source",
                got: format!("{:?}", src_node.label),
            });
        }

        // Parse optional WHERE conditions
        let mut conditions = Vec::new();
        if matches!(self.peek(), Tok::Kw(Kw::Where)) {
            self.advance();
            conditions = self.parse_match_conditions()?;
        }

        // Expect INSERT keyword
        self.expect_kw(Kw::Insert, "INSERT")?;

        // Parse edge pattern: (src)-[:kind {props}]->(tgt)
        // parse_match_node handles its own parentheses
        let _ = self.parse_match_node()?;

        // Consume dash before edge bracket
        if matches!(self.peek(), Tok::Dash) {
            self.advance();
        } else {
            return Err(SqlError::UnexpectedToken {
                expected: "-",
                got: format!("{:?}", self.peek()),
            });
        }

        // Parse edge type
        self.expect_lbracket()?;
        // Consume optional colon before edge kind (e.g. [:member_of])
        if matches!(self.peek(), Tok::Colon) {
            self.advance();
        }
        let edge_kind = self.expect_ident()?;
        // Optional edge props
        let mut strength = 1.0;
        let mut props_json = None;
        if matches!(self.peek(), Tok::LBrace) {
            self.advance();
            let mut props_map = serde_json::Map::new();
            loop {
                if matches!(self.peek(), Tok::RBrace) {
                    self.advance();
                    break;
                }
                let key = self.expect_ident()?;
                self.expect_colon()?;
                let value = self.parse_value()?;
                if key == "strength" {
                    if let Some(n) = value.as_f64() {
                        strength = n as f32;
                    }
                } else {
                    props_map.insert(key, value);
                }
                if matches!(self.peek(), Tok::Comma) {
                    self.advance();
                }
            }
            if !props_map.is_empty() {
                props_json = Some(serde_json::to_string(&props_map).unwrap());
            }
        }
        self.expect_rbracket()?;

        // Arrow
        if matches!(self.peek(), Tok::Arrow) {
            self.advance();
        }

        // Target node — parse_match_node handles its own parentheses
        let tgt_node = self.parse_match_node()?;

        // Build target slug from node
        let target_slug = if let Some(ref v) = tgt_node.var {
            if v.contains('/') {
                // Direct slug reference like classroom/A
                v.clone()
            } else if let Some(ref label) = tgt_node.label {
                let key = tgt_node
                    .props
                    .iter()
                    .find(|(k, _)| k == "_key")
                    .map(|(_, v)| v.as_str().unwrap_or(""))
                    .unwrap_or("");
                format!("{}/{}", label, key)
            } else {
                return Err(SqlError::UnexpectedToken {
                    expected: "named target like (classroom/C) or (:label {_key: val})",
                    got: format!("variable '{}' without label or slug", v),
                });
            }
        } else if let Some(ref label) = tgt_node.label {
            let key = tgt_node
                .props
                .iter()
                .find(|(k, _)| k == "_key")
                .map(|(_, v)| v.as_str().unwrap_or(""))
                .unwrap_or("");
            format!("{}/{}", label, key)
        } else {
            return Err(SqlError::UnexpectedToken {
                expected: "named target like (classroom/C)",
                got: "unnamed node".into(),
            });
        };

        // Build match_steps from conditions
        let mut match_steps = Vec::new();
        if let Some(label) = &src_node.label {
            match_steps.push(Step::Collection(sk_hash(label)));
        }
        for cond in conditions {
            match cond {
                MatchCond::NodeField {
                    var: _,
                    field,
                    op,
                    value,
                } => match op {
                    CompareOp::Eq => match_steps.push(Step::WhereEq(field, value)),
                    CompareOp::Gt => {
                        if let Some(n) = value.as_f64() {
                            match_steps.push(Step::WhereGt(field, n));
                        }
                    }
                    CompareOp::Gte => {
                        if let Some(n) = value.as_f64() {
                            match_steps.push(Step::WhereGte(field, n));
                        }
                    }
                    CompareOp::Lt => {
                        if let Some(n) = value.as_f64() {
                            match_steps.push(Step::WhereLt(field, n));
                        }
                    }
                    CompareOp::Lte => {
                        if let Some(n) = value.as_f64() {
                            match_steps.push(Step::WhereLte(field, n));
                        }
                    }
                    CompareOp::Neq => match_steps.push(Step::WhereNeq(field, value)),
                    _ => {}
                },
            }
        }

        Ok(CompiledMutation::MatchInsert {
            match_steps,
            target: target_slug,
            edge_type: edge_kind,
            strength,
            props: props_json,
        })
    }

    /// Parse: (var:label {key: val, ...})
    fn parse_match_node(&mut self) -> Result<MatchNode, SqlError> {
        self.expect_lparen()?;

        let mut var = None;
        let mut label = None;
        let mut props = Vec::new();

        // Check for empty node ()
        if matches!(self.peek(), Tok::RParen) {
            self.advance();
            return Ok(MatchNode { var, label, props });
        }

        // Colon first means no var, just :label
        if matches!(self.peek(), Tok::Colon) {
            self.advance();
            label = Some(self.expect_ident()?);
        } else if matches!(self.peek(), Tok::Ident(_) | Tok::Str(_)) {
            let name = self.expect_ident()?;
            if matches!(self.peek(), Tok::Colon) {
                // var:label
                self.advance();
                var = Some(name);
                label = Some(self.expect_ident()?);
            } else {
                // just var, no label
                var = Some(name);
            }
        }

        // Optional inline props {key: val, ...}
        if matches!(self.peek(), Tok::LBrace) {
            self.advance();
            while !matches!(self.peek(), Tok::RBrace | Tok::Eof) {
                let key = self.expect_ident()?;
                match self.peek() {
                    Tok::Colon => {
                        self.advance();
                    }
                    Tok::Eof => return Err(SqlError::UnexpectedEnd { expected: ":" }),
                    other => {
                        return Err(SqlError::UnexpectedToken {
                            expected: ":",
                            got: format!("{other:?}"),
                        })
                    }
                }
                let val = self.parse_value()?;
                props.push((key, val));
                if matches!(self.peek(), Tok::Comma) {
                    self.advance();
                }
            }
            match self.peek() {
                Tok::RBrace => {
                    self.advance();
                }
                _ => return Err(SqlError::UnexpectedEnd { expected: "}" }),
            }
        }

        self.expect_rparen()?;
        Ok(MatchNode { var, label, props })
    }

    /// Parse edge pattern + direction:
    ///   Forward:  -[var:kind *min..max]->
    ///   Backward: <-[var:kind]-
    fn parse_match_edge(&mut self) -> Result<MatchEdge, SqlError> {
        let dir;
        // Detect direction by looking at first token
        if matches!(self.peek(), Tok::BackArrow) {
            // <-[...]- (backward)
            dir = EdgeDir::Backward;
            self.advance(); // consume <-
        } else if matches!(self.peek(), Tok::Dash) {
            // -[...]-> (forward)
            dir = EdgeDir::Forward;
            self.advance(); // consume -
        } else {
            return Err(SqlError::UnexpectedToken {
                expected: "- or <-",
                got: format!("{:?}", self.peek()),
            });
        }

        // [var:kind *min..max]
        match self.peek() {
            Tok::LBracket => {
                self.advance();
            }
            Tok::Eof => return Err(SqlError::UnexpectedEnd { expected: "[" }),
            other => {
                return Err(SqlError::UnexpectedToken {
                    expected: "[",
                    got: format!("{other:?}"),
                })
            }
        }

        let mut var = None;
        let mut kind = None;
        let mut depth = None;

        // Empty edge []
        if !matches!(self.peek(), Tok::RBracket) {
            // Optional var or :kind
            if matches!(self.peek(), Tok::Colon) {
                self.advance();
                kind = Some(self.expect_ident()?);
            } else if matches!(self.peek(), Tok::Ident(_) | Tok::Str(_)) {
                let name = self.expect_ident()?;
                if matches!(self.peek(), Tok::Colon) {
                    self.advance();
                    var = Some(name);
                    kind = Some(self.expect_ident()?);
                } else {
                    // just var, no kind
                    var = Some(name);
                }
            }

            // Optional depth: *min..max
            if matches!(self.peek(), Tok::Star) {
                self.advance();
                let min = self.expect_num()? as u32;
                match self.peek() {
                    Tok::DotDot => {
                        self.advance();
                    }
                    Tok::Eof => return Err(SqlError::UnexpectedEnd { expected: ".." }),
                    other => {
                        return Err(SqlError::UnexpectedToken {
                            expected: "..",
                            got: format!("{other:?}"),
                        })
                    }
                }
                let max = self.expect_num()? as u32;
                depth = Some((min, max));
            }
        }

        match self.peek() {
            Tok::RBracket => {
                self.advance();
            }
            Tok::Eof => return Err(SqlError::UnexpectedEnd { expected: "]" }),
            other => {
                return Err(SqlError::UnexpectedToken {
                    expected: "]",
                    got: format!("{other:?}"),
                })
            }
        }

        // Consume trailing direction marker
        if dir == EdgeDir::Forward {
            // expect ->
            match self.peek() {
                Tok::Arrow => {
                    self.advance();
                }
                Tok::Eof => return Err(SqlError::UnexpectedEnd { expected: "->" }),
                other => {
                    return Err(SqlError::UnexpectedToken {
                        expected: "->",
                        got: format!("{other:?}"),
                    })
                }
            }
        } else {
            // backward: expect trailing -
            match self.peek() {
                Tok::Dash => {
                    self.advance();
                }
                Tok::Eof => return Err(SqlError::UnexpectedEnd { expected: "-" }),
                other => {
                    return Err(SqlError::UnexpectedToken {
                        expected: "-",
                        got: format!("{other:?}"),
                    })
                }
            }
        }

        Ok(MatchEdge {
            var,
            kind,
            dir,
            depth,
        })
    }

    /// Parse WHERE conditions: var.field OP value [AND var.field OP value]*
    fn parse_match_conditions(&mut self) -> Result<Vec<MatchCond>, SqlError> {
        let mut conds = vec![self.parse_match_cond()?];
        while matches!(self.peek(), Tok::Kw(Kw::And)) {
            self.advance();
            conds.push(self.parse_match_cond()?);
        }
        Ok(conds)
    }

    /// Parse: var.field OP value
    fn parse_match_cond(&mut self) -> Result<MatchCond, SqlError> {
        let var = self.expect_ident()?;
        match self.peek() {
            Tok::Dot => {
                self.advance();
            }
            Tok::Eof => return Err(SqlError::UnexpectedEnd { expected: "." }),
            other => {
                return Err(SqlError::UnexpectedToken {
                    expected: ".",
                    got: format!("{other:?}"),
                })
            }
        }
        let field = self.expect_ident()?;
        let op = match self.peek() {
            Tok::Eq => {
                self.advance();
                CompareOp::Eq
            }
            Tok::Neq => {
                self.advance();
                CompareOp::Neq
            }
            Tok::Gt => {
                self.advance();
                CompareOp::Gt
            }
            Tok::Lt => {
                self.advance();
                CompareOp::Lt
            }
            Tok::Gte => {
                self.advance();
                CompareOp::Gte
            }
            Tok::Lte => {
                self.advance();
                CompareOp::Lte
            }
            Tok::Eof => {
                return Err(SqlError::UnexpectedEnd {
                    expected: "comparison operator",
                })
            }
            other => {
                return Err(SqlError::UnexpectedToken {
                    expected: "comparison operator",
                    got: format!("{other:?}"),
                })
            }
        };
        let value = self.parse_value()?;
        Ok(MatchCond::NodeField {
            var,
            field,
            op,
            value,
        })
    }

    /// Parse: RETURN a, g, r
    fn parse_return_list(&mut self) -> Result<Vec<String>, SqlError> {
        let mut vars = vec![self.expect_ident()?];
        while matches!(self.peek(), Tok::Comma) {
            self.advance();
            vars.push(self.expect_ident()?);
        }
        Ok(vars)
    }

    // ── Aggregate MATCH parser ────────────────────────────────────────────────

    /// Parse a MATCH path in aggregate mode and build a [`MatchAggStmt`].
    ///
    /// The MATCH keyword must already have been consumed by the caller.
    ///
    /// Grammar:
    /// ```text
    /// (start)-[:edge]->(bind) [-[:edge]->(bind)]*
    /// RETURN expr AS alias [, ...]
    /// [GROUP BY var.field]
    /// [ORDER BY alias [ASC|DESC]]
    /// [LIMIT n]
    /// ```
    fn parse_match_agg_path(&mut self) -> Result<crate::query::MatchAggStmt, SqlError> {
        use crate::query::{HopSpec, MatchAggStart, MatchAggStmt};

        // ── Start node ────────────────────────────────────────────────────
        let start_node = self.parse_match_node()?;
        let start_var:   Option<String> = start_node.var.clone();
        let start_label: Option<String> = start_node.label.clone();
        let mut start = match start_node.label {
            Some(ref lbl) => MatchAggStart::Collection(sk_hash(lbl)),
            None => match start_node.var {
                Some(ref v) => MatchAggStart::Slug(sk_hash(v)),
                None => MatchAggStart::All,
            },
        };

        // ── Hop chain ─────────────────────────────────────────────────────
        // Edge pattern forms:
        //   -[r:edge_type]->   edge_bind="r", type=edge_type
        //   -[:edge_type]->    edge_bind=None, type=edge_type
        //   -[r*]->            edge_bind="r", type=any (0)
        //   -[r*1..3]->        edge_bind="r", type=any (range ignored by executor for now)
        //   -[*]->             edge_bind=None, type=any
        let mut hops: Vec<HopSpec> = Vec::new();
        while matches!(self.peek(), Tok::Dash) {
            self.advance(); // consume '-'
            self.expect_lbracket()?;

            // Determine edge_bind and edge_type_hash from the pattern inside [...]
            let mut edge_bind: Option<String> = None;
            let mut edge_type_hash: u64 = 0; // 0 = any

            match self.peek().clone() {
                Tok::Ident(name) => {
                    self.advance(); // consume ident (potential bind name)
                    match self.peek() {
                        Tok::Colon => {
                            // r:edge_type
                            self.advance(); // consume ':'
                            let et = self.expect_ident()?;
                            edge_bind = Some(name);
                            edge_type_hash = sk_hash(&et);
                        }
                        _ => {
                            // r* or r alone — edge bind, any type
                            edge_bind = Some(name);
                        }
                    }
                }
                Tok::Colon => {
                    // :edge_type — no bind name
                    self.advance(); // consume ':'
                    let et = self.expect_ident()?;
                    edge_type_hash = sk_hash(&et);
                }
                _ => { /* * or ] — anonymous, any type */ }
            }

            // Consume remaining tokens in [...] (*, ranges, etc.)
            loop {
                match self.peek() {
                    Tok::RBracket | Tok::Eof => break,
                    _ => { self.advance(); }
                }
            }
            self.expect_rbracket()?;

            if !matches!(self.peek(), Tok::Arrow) {
                return Err(SqlError::UnexpectedToken {
                    expected: "->",
                    got: format!("{:?}", self.peek()),
                });
            }
            self.advance(); // consume '->'
            self.expect_lparen()?;
            let node_bind = self.expect_ident()?;
            if matches!(self.peek(), Tok::Colon) { self.advance(); self.expect_ident()?; } // skip :col
            self.expect_rparen()?;
            hops.push(HopSpec { edge_type_hash, node_bind, edge_bind });
        }

        // ── WHERE clause (optional) ───────────────────────────────────────
        // Supported: WHERE start_var._key = 'slug'  — refines Collection → Slug.
        // Multiple AND conditions are parsed but only _key on the start var acts.
        if matches!(self.peek(), Tok::Kw(Kw::Where)) {
            self.advance(); // consume WHERE
            loop {
                // Parse one condition: var.field = value
                let cond_var = self.expect_ident()?;
                self.expect_dot()?;
                let cond_field = self.expect_ident()?;
                if !matches!(self.peek(), Tok::Eq) {
                    return Err(SqlError::UnexpectedToken {
                        expected: "=",
                        got: format!("{:?}", self.peek()),
                    });
                }
                self.advance(); // consume '='
                let cond_val = self.parse_value()?;

                // _key on the start var upgrades MatchAggStart to Slug.
                // Reconstruct full slug: if start has a collection label,
                // slug = "label/key_value", else use the value as-is.
                if cond_field == "_key" {
                    if let Some(ref sv) = start_var {
                        if *sv == cond_var {
                            if let Some(key_val) = cond_val.as_str() {
                                let full_slug = match start_label {
                                    Some(ref lbl) => format!("{}/{}", lbl, key_val),
                                    None => key_val.to_string(),
                                };
                                start = MatchAggStart::Slug(sk_hash(&full_slug));
                            }
                        }
                    }
                }

                if matches!(self.peek(), Tok::Kw(Kw::And)) {
                    self.advance(); // consume AND, continue to next condition
                } else {
                    break;
                }
            }
        }

        // ── RETURN clause ─────────────────────────────────────────────────
        self.expect_kw(Kw::Return, "RETURN")?;
        let returns = self.parse_agg_return_list()?;

        // ── GROUP BY ──────────────────────────────────────────────────────
        let group_by = if matches!(self.peek(), Tok::Kw(Kw::Group)) {
            self.advance(); // GROUP
            self.expect_kw(Kw::By, "BY")?;
            let var = self.expect_ident()?;
            self.expect_dot()?;
            let field = self.expect_ident()?;
            Some((var, field))
        } else {
            None
        };

        // ── ORDER BY ──────────────────────────────────────────────────────
        let order_by = if matches!(self.peek(), Tok::Kw(Kw::Order)) {
            self.advance(); // ORDER
            self.expect_kw(Kw::By, "BY")?;
            let alias = self.expect_ident()?;
            let ascending = if matches!(self.peek(), Tok::Kw(Kw::Desc)) {
                self.advance();
                false
            } else {
                if matches!(self.peek(), Tok::Kw(Kw::Asc)) {
                    self.advance();
                }
                true
            };
            Some((alias, ascending))
        } else {
            None
        };

        // ── LIMIT ─────────────────────────────────────────────────────────
        let limit = if matches!(self.peek(), Tok::Kw(Kw::Limit)) {
            self.advance();
            Some(self.expect_num()? as usize)
        } else {
            None
        };

        Ok(MatchAggStmt { start, hops, returns, group_by, order_by, limit })
    }

    /// Parse `SELECT return_list FROM MATCH (start)-[edge]->(node)... [WHERE ...] [GROUP BY ...] [ORDER BY ...] [LIMIT n]`
    ///
    /// The SELECT list acts as the RETURN clause; no RETURN keyword is present.
    fn parse_select_from_match(&mut self) -> Result<crate::query::MatchAggStmt, SqlError> {
        use crate::query::{HopSpec, MatchAggStart, MatchAggStmt};

        self.expect_kw(Kw::Select, "SELECT")?;
        let returns = self.parse_agg_return_list()?;

        self.expect_kw(Kw::From, "FROM")?;
        self.expect_kw(Kw::Match, "MATCH")?;

        // ── Start node ────────────────────────────────────────────────────
        let start_node = self.parse_match_node()?;
        let start_var:   Option<String> = start_node.var.clone();
        let start_label: Option<String> = start_node.label.clone();
        let mut start = match start_node.label {
            Some(ref lbl) => MatchAggStart::Collection(sk_hash(lbl)),
            None => match start_node.var {
                Some(ref v) => MatchAggStart::Slug(sk_hash(v)),
                None => MatchAggStart::All,
            },
        };

        // ── Hop chain (same as parse_match_agg_path) ──────────────────────
        let mut hops: Vec<HopSpec> = Vec::new();
        while matches!(self.peek(), Tok::Dash) {
            self.advance(); // consume '-'
            self.expect_lbracket()?;

            let mut edge_bind: Option<String> = None;
            let mut edge_type_hash: u64 = 0;

            match self.peek().clone() {
                Tok::Ident(name) => {
                    self.advance();
                    match self.peek() {
                        Tok::Colon => {
                            self.advance();
                            let et = self.expect_ident()?;
                            edge_bind = Some(name);
                            edge_type_hash = sk_hash(&et);
                        }
                        _ => { edge_bind = Some(name); }
                    }
                }
                Tok::Colon => {
                    self.advance();
                    let et = self.expect_ident()?;
                    edge_type_hash = sk_hash(&et);
                }
                _ => {}
            }

            loop {
                match self.peek() {
                    Tok::RBracket | Tok::Eof => break,
                    _ => { self.advance(); }
                }
            }
            self.expect_rbracket()?;

            if !matches!(self.peek(), Tok::Arrow) {
                return Err(SqlError::UnexpectedToken {
                    expected: "->",
                    got: format!("{:?}", self.peek()),
                });
            }
            self.advance();
            self.expect_lparen()?;
            let node_bind = self.expect_ident()?;
            if matches!(self.peek(), Tok::Colon) { self.advance(); self.expect_ident()?; }
            self.expect_rparen()?;
            hops.push(HopSpec { edge_type_hash, node_bind, edge_bind });
        }

        // ── WHERE (same as parse_match_agg_path) ──────────────────────────
        if matches!(self.peek(), Tok::Kw(Kw::Where)) {
            self.advance();
            loop {
                let cond_var = self.expect_ident()?;
                self.expect_dot()?;
                let cond_field = self.expect_ident()?;
                if !matches!(self.peek(), Tok::Eq) {
                    return Err(SqlError::UnexpectedToken {
                        expected: "=",
                        got: format!("{:?}", self.peek()),
                    });
                }
                self.advance();
                let cond_val = self.parse_value()?;

                if cond_field == "_key" {
                    if let Some(ref sv) = start_var {
                        if *sv == cond_var {
                            if let Some(key_val) = cond_val.as_str() {
                                let full_slug = match start_label {
                                    Some(ref lbl) => format!("{}/{}", lbl, key_val),
                                    None => key_val.to_string(),
                                };
                                start = MatchAggStart::Slug(sk_hash(&full_slug));
                            }
                        }
                    }
                }

                if matches!(self.peek(), Tok::Kw(Kw::And)) {
                    self.advance();
                } else {
                    break;
                }
            }
        }

        // ── GROUP BY ──────────────────────────────────────────────────────
        let group_by = if matches!(self.peek(), Tok::Kw(Kw::Group)) {
            self.advance();
            self.expect_kw(Kw::By, "BY")?;
            let var = self.expect_ident()?;
            self.expect_dot()?;
            let field = self.expect_ident()?;
            Some((var, field))
        } else {
            None
        };

        // ── ORDER BY ──────────────────────────────────────────────────────
        let order_by = if matches!(self.peek(), Tok::Kw(Kw::Order)) {
            self.advance();
            self.expect_kw(Kw::By, "BY")?;
            let alias = self.expect_ident()?;
            let ascending = if matches!(self.peek(), Tok::Kw(Kw::Desc)) {
                self.advance();
                false
            } else {
                if matches!(self.peek(), Tok::Kw(Kw::Asc)) { self.advance(); }
                true
            };
            Some((alias, ascending))
        } else {
            None
        };

        // ── LIMIT ─────────────────────────────────────────────────────────
        let limit = if matches!(self.peek(), Tok::Kw(Kw::Limit)) {
            self.advance();
            Some(self.expect_num()? as usize)
        } else {
            None
        };

        Ok(MatchAggStmt { start, hops, returns, group_by, order_by, limit })
    }

    /// Parse `SELECT return_list FROM MATCH SHORTEST (a[:col])-[r*]->(b[:col])
    ///   WHERE a._key = 'x' AND b._key = 'y'
    ///   [AND ANY(n IN nodes(r) WHERE n.field op val)]
    ///   [ORDER BY alias [ASC|DESC]] [LIMIT n]`
    fn parse_select_from_match_shortest(
        &mut self,
    ) -> Result<crate::query::ShortestSelectStmt, SqlError> {
        use crate::query::{PathPredicate, ShortestSelectStmt, SimpleCond};

        self.expect_kw(Kw::Select, "SELECT")?;
        let returns = self.parse_agg_return_list()?;

        self.expect_kw(Kw::From, "FROM")?;
        self.expect_kw(Kw::Match, "MATCH")?;
        // SHORTEST ident (already confirmed by is_select_from_match_shortest)
        self.expect_ident()?; // consume "SHORTEST"

        // (start_bind[:col])
        self.expect_lparen()?;
        let start_bind = self.expect_ident()?;
        if matches!(self.peek(), Tok::Colon) { self.advance(); self.expect_ident()?; }
        self.expect_rparen()?;

        // -[path_bind*]->
        if !matches!(self.peek(), Tok::Dash) {
            return Err(SqlError::UnexpectedToken { expected: "-", got: format!("{:?}", self.peek()) });
        }
        self.advance();
        self.expect_lbracket()?;
        // Parse optional path_bind name
        let path_bind = match self.peek().clone() {
            Tok::Ident(name) => {
                self.advance();
                Some(name)
            }
            _ => None,
        };
        // Consume everything inside [...] (*, ranges, type labels)
        loop {
            match self.peek() {
                Tok::RBracket | Tok::Eof => break,
                _ => { self.advance(); }
            }
        }
        self.expect_rbracket()?;
        if !matches!(self.peek(), Tok::Arrow) {
            return Err(SqlError::UnexpectedToken { expected: "->", got: format!("{:?}", self.peek()) });
        }
        self.advance(); // consume ->

        // (end_bind[:col])
        self.expect_lparen()?;
        let end_bind = self.expect_ident()?;
        if matches!(self.peek(), Tok::Colon) { self.advance(); self.expect_ident()?; }
        self.expect_rparen()?;

        // WHERE — parse _key conditions for start/end, and optional path predicates
        self.expect_kw(Kw::Where, "WHERE")?;

        let mut from_slug: Option<String> = None;
        let mut to_slug:   Option<String> = None;
        let mut predicates: Vec<PathPredicate> = Vec::new();

        loop {
            // Path predicate: ANY/ALL/NONE/SINGLE (...)
            match self.peek().clone() {
                Tok::Kw(Kw::Any) | Tok::Kw(Kw::All) | Tok::Kw(Kw::None_) | Tok::Kw(Kw::Single) => {
                    let quantifier = self.advance();
                    self.expect_lparen()?;
                    // n IN nodes(path_bind) WHERE n.field op val
                    let _node_var = self.expect_ident()?; // the iteration variable name
                    self.expect_kw(Kw::In, "IN")?;
                    // nodes(path_bind_name)
                    let func_name = self.expect_ident()?;
                    if func_name.to_ascii_uppercase() != "NODES" {
                        return Err(SqlError::UnexpectedToken {
                            expected: "nodes(path_var)",
                            got: func_name,
                        });
                    }
                    self.expect_lparen()?;
                    let path_var = self.expect_ident()?;
                    self.expect_rparen()?;
                    self.expect_kw(Kw::Where, "WHERE")?;
                    // n.field op val
                    let _n_var = self.expect_ident()?; // should match node_var
                    self.expect_dot()?;
                    let field = self.expect_ident()?;
                    let op = self.parse_cmp_op()?;
                    let val = self.parse_value()?;
                    self.expect_rparen()?;
                    let cond = SimpleCond { field, op, val };
                    let pred = match quantifier {
                        Tok::Kw(Kw::Any)    => PathPredicate::Any    { var: path_var, cond },
                        Tok::Kw(Kw::All)    => PathPredicate::All    { var: path_var, cond },
                        Tok::Kw(Kw::None_)  => PathPredicate::None_  { var: path_var, cond },
                        Tok::Kw(Kw::Single) => PathPredicate::Single { var: path_var, cond },
                        _ => unreachable!(),
                    };
                    predicates.push(pred);
                }
                _ => {
                    // var._key = 'value' condition
                    let binding = self.expect_ident()?;
                    self.expect_dot()?;
                    let field = self.expect_ident()?;
                    if !matches!(self.peek(), Tok::Eq) {
                        return Err(SqlError::UnexpectedToken {
                            expected: "=",
                            got: format!("{:?}", self.peek()),
                        });
                    }
                    self.advance();
                    let value = self.parse_value()?;
                    let slug_val = value.as_str().unwrap_or("").to_string();
                    if binding == start_bind && field == "_key" {
                        from_slug = Some(slug_val);
                    } else if binding == end_bind && field == "_key" {
                        to_slug = Some(slug_val);
                    }
                    // Other conditions (e.g. _collection) are parsed but not acted on
                }
            }

            if matches!(self.peek(), Tok::Kw(Kw::And)) {
                self.advance();
            } else {
                break;
            }
        }

        let from_slug = from_slug.ok_or(SqlError::UnexpectedEnd { expected: "start node _key condition" })?;
        let to_slug   = to_slug  .ok_or(SqlError::UnexpectedEnd { expected: "end node _key condition" })?;

        // ORDER BY
        let order_by = if matches!(self.peek(), Tok::Kw(Kw::Order)) {
            self.advance();
            self.expect_kw(Kw::By, "BY")?;
            let alias = self.expect_ident()?;
            let ascending = if matches!(self.peek(), Tok::Kw(Kw::Desc)) {
                self.advance(); false
            } else {
                if matches!(self.peek(), Tok::Kw(Kw::Asc)) { self.advance(); }
                true
            };
            Some((alias, ascending))
        } else { None };

        // LIMIT
        let limit = if matches!(self.peek(), Tok::Kw(Kw::Limit)) {
            self.advance();
            Some(self.expect_num()? as usize)
        } else { None };

        Ok(ShortestSelectStmt { from_slug, to_slug, start_bind, end_bind, path_bind, returns, predicates, order_by, limit })
    }

    /// Parse `SELECT return_list FROM source1, source2, … [ORDER BY alias] [LIMIT n]`
    ///
    /// Each source is:
    /// - `MATCH SHORTEST (a)-[r*]->(b) WHERE a._key = 'x' AND b._key = 'y'`
    /// - `MATCH (a:col)-[r:edge]->(b:col) [WHERE …]`
    /// - `collection_name [AS alias]`
    fn parse_select_multi_from(
        &mut self,
    ) -> Result<crate::query::MultiFromStmt, SqlError> {
        use crate::query::{FromSource, MatchAggStart, MatchAggStmt, MultiFromStmt};

        self.expect_kw(Kw::Select, "SELECT")?;
        let returns = self.parse_agg_return_list()?;

        self.expect_kw(Kw::From, "FROM")?;

        // Parse each source
        let mut sources: Vec<FromSource> = Vec::new();
        loop {
            let src = match self.peek().clone() {
                Tok::Kw(Kw::Match) => {
                    self.advance(); // consume MATCH
                    // Check for SHORTEST
                    if matches!(self.peek(), Tok::Ident(ref name) if name.eq_ignore_ascii_case("SHORTEST")) {
                        // Delegate to a sub-parser for the SHORTEST pattern
                        // Re-use parse_select_from_match_shortest logic inline:
                        self.advance(); // consume SHORTEST
                        self.expect_lparen()?;
                        let start_bind = self.expect_ident()?;
                        if matches!(self.peek(), Tok::Colon) { self.advance(); self.expect_ident()?; }
                        self.expect_rparen()?;
                        self.advance(); // consume '-'
                        self.expect_lbracket()?;
                        let path_bind = match self.peek().clone() {
                            Tok::Ident(name) => { self.advance(); Some(name) }
                            _ => None,
                        };
                        loop {
                            match self.peek() {
                                Tok::RBracket | Tok::Eof => break,
                                _ => { self.advance(); }
                            }
                        }
                        self.expect_rbracket()?;
                        self.advance(); // consume ->
                        self.expect_lparen()?;
                        let end_bind = self.expect_ident()?;
                        if matches!(self.peek(), Tok::Colon) { self.advance(); self.expect_ident()?; }
                        self.expect_rparen()?;

                        self.expect_kw(Kw::Where, "WHERE")?;
                        let mut from_slug: Option<String> = None;
                        let mut to_slug: Option<String> = None;
                        loop {
                            let binding = self.expect_ident()?;
                            self.expect_dot()?;
                            let field = self.expect_ident()?;
                            if !matches!(self.peek(), Tok::Eq) {
                                return Err(SqlError::UnexpectedToken { expected: "=", got: format!("{:?}", self.peek()) });
                            }
                            self.advance();
                            let value = self.parse_value()?;
                            let slug_val = value.as_str().unwrap_or("").to_string();
                            if binding == start_bind && field == "_key" { from_slug = Some(slug_val); }
                            else if binding == end_bind && field == "_key" { to_slug = Some(slug_val); }
                            if matches!(self.peek(), Tok::Kw(Kw::And)) {
                                // Check if next is another _key condition (not a predicate)
                                if matches!(self.peek(), Tok::Kw(Kw::And)) {
                                    self.advance();
                                    if matches!(self.peek(), Tok::Kw(Kw::Any | Kw::All | Kw::None_ | Kw::Single)) {
                                        // predicates — skip for multi-from simplicity
                                        break;
                                    }
                                }
                            } else { break; }
                        }
                        let from_slug = from_slug.ok_or(SqlError::UnexpectedEnd { expected: "start _key" })?;
                        let to_slug   = to_slug  .ok_or(SqlError::UnexpectedEnd { expected: "end _key" })?;
                        FromSource::Shortest(crate::query::ShortestSelectStmt {
                            from_slug, to_slug, start_bind, end_bind, path_bind,
                            returns: vec![], predicates: vec![], order_by: None, limit: None,
                        })
                    } else {
                        // Regular MATCH hop chain
                        let start_node = self.parse_match_node()?;
                        let start_var = start_node.var.clone();
                        let start_label = start_node.label.clone();
                        let mut start = match start_node.label {
                            Some(ref lbl) => MatchAggStart::Collection(sk_hash(lbl)),
                            None => match start_node.var {
                                Some(ref v) => MatchAggStart::Slug(sk_hash(v)),
                                None => MatchAggStart::All,
                            },
                        };
                        let mut hops = Vec::new();
                        while matches!(self.peek(), Tok::Dash) {
                            self.advance();
                            self.expect_lbracket()?;
                            let mut edge_bind: Option<String> = None;
                            let mut edge_type_hash: u64 = 0;
                            match self.peek().clone() {
                                Tok::Ident(name) => {
                                    self.advance();
                                    match self.peek() {
                                        Tok::Colon => { self.advance(); let et = self.expect_ident()?; edge_bind = Some(name); edge_type_hash = sk_hash(&et); }
                                        _ => { edge_bind = Some(name); }
                                    }
                                }
                                Tok::Colon => { self.advance(); let et = self.expect_ident()?; edge_type_hash = sk_hash(&et); }
                                _ => {}
                            }
                            loop { match self.peek() { Tok::RBracket | Tok::Eof => break, _ => { self.advance(); } } }
                            self.expect_rbracket()?;
                            if !matches!(self.peek(), Tok::Arrow) {
                                return Err(SqlError::UnexpectedToken { expected: "->", got: format!("{:?}", self.peek()) });
                            }
                            self.advance();
                            self.expect_lparen()?;
                            let node_bind = self.expect_ident()?;
                            if matches!(self.peek(), Tok::Colon) { self.advance(); self.expect_ident()?; }
                            self.expect_rparen()?;
                            hops.push(crate::query::HopSpec { edge_type_hash, node_bind, edge_bind });
                        }
                        // Optional WHERE (only _key on start var acted on)
                        if matches!(self.peek(), Tok::Kw(Kw::Where)) {
                            self.advance();
                            loop {
                                let cond_var = self.expect_ident()?;
                                self.expect_dot()?;
                                let cond_field = self.expect_ident()?;
                                if !matches!(self.peek(), Tok::Eq) {
                                    return Err(SqlError::UnexpectedToken { expected: "=", got: format!("{:?}", self.peek()) });
                                }
                                self.advance();
                                let cond_val = self.parse_value()?;
                                if cond_field == "_key" {
                                    if let Some(ref sv) = start_var {
                                        if *sv == cond_var {
                                            if let Some(key_val) = cond_val.as_str() {
                                                let full_slug = match start_label {
                                                    Some(ref lbl) => format!("{}/{}", lbl, key_val),
                                                    None => key_val.to_string(),
                                                };
                                                start = MatchAggStart::Slug(sk_hash(&full_slug));
                                            }
                                        }
                                    }
                                }
                                if matches!(self.peek(), Tok::Kw(Kw::And)) {
                                    self.advance();
                                } else { break; }
                            }
                        }
                        FromSource::Match(MatchAggStmt {
                            start, hops, returns: vec![], group_by: None, order_by: None, limit: None,
                        })
                    }
                }
                Tok::Ident(name) => {
                    let col_name = name.clone();
                    self.advance();
                    // Optional AS alias
                    let alias = if matches!(self.peek(), Tok::Kw(Kw::As)) {
                        self.advance();
                        self.expect_ident()?
                    } else {
                        col_name.clone()
                    };
                    FromSource::Collection { alias, name_hash: sk_hash(&col_name) }
                }
                other => return Err(SqlError::UnexpectedToken {
                    expected: "MATCH or collection name",
                    got: format!("{other:?}"),
                }),
            };
            sources.push(src);

            // Continue if there's a comma
            if matches!(self.peek(), Tok::Comma) {
                self.advance();
            } else {
                break;
            }
        }

        // ORDER BY
        let order_by = if matches!(self.peek(), Tok::Kw(Kw::Order)) {
            self.advance();
            self.expect_kw(Kw::By, "BY")?;
            let alias = self.expect_ident()?;
            let ascending = if matches!(self.peek(), Tok::Kw(Kw::Desc)) {
                self.advance(); false
            } else {
                if matches!(self.peek(), Tok::Kw(Kw::Asc)) { self.advance(); }
                true
            };
            Some((alias, ascending))
        } else { None };

        // LIMIT
        let limit = if matches!(self.peek(), Tok::Kw(Kw::Limit)) {
            self.advance();
            Some(self.expect_num()? as usize)
        } else { None };

        Ok(MultiFromStmt { sources, returns, order_by, limit })
    }

    /// Parse the aggregate RETURN list: `expr AS alias [, expr AS alias ...]`
    fn parse_agg_return_list(
        &mut self,
    ) -> Result<Vec<(crate::query::MatchAggReturn, String)>, SqlError> {
        let mut items = vec![self.parse_agg_return_item()?];
        while matches!(self.peek(), Tok::Comma) {
            self.advance();
            items.push(self.parse_agg_return_item()?);
        }
        Ok(items)
    }

    /// Parse a single aggregate return item: `var.field AS alias` or `SUM(math) AS alias`.
    fn parse_agg_return_item(
        &mut self,
    ) -> Result<(crate::query::MatchAggReturn, String), SqlError> {
        use crate::query::MatchAggReturn;

        let expr: MatchAggReturn = match self.peek().clone() {
            Tok::Kw(Kw::Count) => {
                self.advance();
                self.expect_lparen()?;
                // Accept `*` or a var.field argument — result is always COUNT
                match self.peek() {
                    Tok::Star => { self.advance(); }
                    _ => {
                        let _ = self.expect_ident()?;
                        if matches!(self.peek(), Tok::Dot) {
                            self.advance();
                            let _ = self.expect_ident()?;
                        }
                    }
                }
                self.expect_rparen()?;
                MatchAggReturn::Count
            }
            Tok::Kw(ref kw) if matches!(kw, Kw::Sum | Kw::Avg | Kw::Min | Kw::Max) => {
                let func = kw.clone();
                self.advance();
                self.expect_lparen()?;
                let math = self.parse_math_expr()?;
                self.expect_rparen()?;
                match func {
                    Kw::Sum => MatchAggReturn::Sum(math),
                    Kw::Avg => MatchAggReturn::Avg(math),
                    Kw::Min => MatchAggReturn::Min(math),
                    Kw::Max => MatchAggReturn::Max(math),
                    _ => unreachable!(),
                }
            }
            Tok::Ident(ref name_peek) => {
                let upper = name_peek.to_ascii_uppercase();
                if upper.starts_with("PATH_")
                    || matches!(upper.as_str(), "AGE_DAYS" | "AGE_HOURS" | "JSON_ARRAY_LENGTH")
                {
                    let func_name = self.expect_ident()?.to_ascii_uppercase();
                    self.expect_lparen()?;
                    let var = self.expect_ident()?;
                    self.expect_dot()?;
                    let field = self.expect_ident()?;
                    self.expect_rparen()?;
                    path_func_variant(&func_name, var, field)?
                } else if upper == "NOW" {
                    self.advance(); // consume NOW
                    self.expect_lparen()?;
                    self.expect_rparen()?;
                    MatchAggReturn::Now
                } else {
                    let var = self.expect_ident()?;
                    self.expect_dot()?;
                    let field = self.expect_ident()?;
                    MatchAggReturn::Field { var, field }
                }
            }
            Tok::Kw(Kw::Case) => {
                use crate::query::{CaseCond, CmpOp};
                self.advance(); // consume CASE
                let mut branches: Vec<(CaseCond, Value)> = Vec::new();
                let mut else_val = Value::Null;
                loop {
                    match self.peek().clone() {
                        Tok::Kw(Kw::When) => {
                            self.advance(); // consume WHEN
                            let var = self.expect_ident()?;
                            self.expect_dot()?;
                            let field = self.expect_ident()?;
                            let op: CmpOp = self.parse_cmp_op()?;
                            let val = self.parse_value()?;
                            self.expect_kw(Kw::Then, "THEN")?;
                            let then_val = self.parse_value()?;
                            branches.push((CaseCond { var, field, op, val }, then_val));
                        }
                        Tok::Kw(Kw::Else) => {
                            self.advance(); // consume ELSE
                            else_val = self.parse_value()?;
                        }
                        _ => break,
                    }
                }
                self.expect_kw(Kw::End, "END")?;
                MatchAggReturn::Case { branches, else_val }
            }
            other => {
                return Err(SqlError::UnexpectedToken {
                    expected: "field expression or aggregate function (SUM, AVG, MIN, MAX, COUNT, PATH_*, CASE, NOW, AGE_DAYS, AGE_HOURS, JSON_ARRAY_LENGTH)",
                    got: format!("{other:?}"),
                })
            }
        };

        // Optional AS alias
        let default_alias = match &expr {
            MatchAggReturn::Field { var, field } => format!("{}.{}", var, field),
            MatchAggReturn::Sum(_) => "sum".to_string(),
            MatchAggReturn::Count => "count".to_string(),
            MatchAggReturn::Avg(_) => "avg".to_string(),
            MatchAggReturn::Min(_) => "min".to_string(),
            MatchAggReturn::Max(_) => "max".to_string(),
            MatchAggReturn::PathAvg { .. } => "path_avg".to_string(),
            MatchAggReturn::PathSum { .. } => "path_sum".to_string(),
            MatchAggReturn::PathMin { .. } => "path_min".to_string(),
            MatchAggReturn::PathMax { .. } => "path_max".to_string(),
            MatchAggReturn::PathProduct { .. } => "path_product".to_string(),
            MatchAggReturn::PathFirst { .. } => "path_first".to_string(),
            MatchAggReturn::PathLast { .. } => "path_last".to_string(),
            MatchAggReturn::Case { .. } => "case".to_string(),
            MatchAggReturn::AgeDays { .. } => "age_days".to_string(),
            MatchAggReturn::AgeHours { .. } => "age_hours".to_string(),
            MatchAggReturn::Now => "now".to_string(),
            MatchAggReturn::JsonArrayLen { .. } => "json_array_length".to_string(),
        };
        let alias = if matches!(self.peek(), Tok::Kw(Kw::As)) {
            self.advance();
            self.expect_ident()?
        } else {
            default_alias
        };

        Ok((expr, alias))
    }

    /// Parse a math expression: `primary (* | + | - | /) primary ...` (left-assoc).
    fn parse_math_expr(&mut self) -> Result<crate::query::MathExpr, SqlError> {
        let mut left = self.parse_math_primary()?;
        loop {
            match self.peek() {
                Tok::Star => {
                    self.advance();
                    let right = self.parse_math_primary()?;
                    left = crate::query::MathExpr::Mul(Box::new(left), Box::new(right));
                }
                Tok::Plus => {
                    self.advance();
                    let right = self.parse_math_primary()?;
                    left = crate::query::MathExpr::Add(Box::new(left), Box::new(right));
                }
                Tok::Dash => {
                    self.advance();
                    let right = self.parse_math_primary()?;
                    left = crate::query::MathExpr::Sub(Box::new(left), Box::new(right));
                }
                Tok::Slash => {
                    self.advance();
                    let right = self.parse_math_primary()?;
                    left = crate::query::MathExpr::Div(Box::new(left), Box::new(right));
                }
                _ => break,
            }
        }
        Ok(left)
    }

    /// Parse a math primary: `var.field`, numeric literal, or `(expr)`.
    fn parse_math_primary(&mut self) -> Result<crate::query::MathExpr, SqlError> {
        match self.peek().clone() {
            Tok::Num(n) => {
                self.advance();
                Ok(crate::query::MathExpr::Literal(n))
            }
            Tok::LParen => {
                self.advance();
                let inner = self.parse_math_expr()?;
                self.expect_rparen()?;
                Ok(inner)
            }
            Tok::Ident(_) => {
                let var = self.expect_ident()?;
                self.expect_dot()?;
                let field = self.expect_ident()?;
                Ok(crate::query::MathExpr::VarField { var, field })
            }
            other => Err(SqlError::UnexpectedToken {
                expected: "number, var.field, or (expr)",
                got: format!("{other:?}"),
            }),
        }
    }

    /// Parse a comparison operator token into a [`CmpOp`].
    fn parse_cmp_op(&mut self) -> Result<crate::query::CmpOp, SqlError> {
        use crate::query::CmpOp;
        match self.peek() {
            Tok::Eq  => { self.advance(); Ok(CmpOp::Eq) }
            Tok::Neq => { self.advance(); Ok(CmpOp::Neq) }
            Tok::Lt  => { self.advance(); Ok(CmpOp::Lt) }
            Tok::Gt  => { self.advance(); Ok(CmpOp::Gt) }
            Tok::Lte => { self.advance(); Ok(CmpOp::Lte) }
            Tok::Gte => { self.advance(); Ok(CmpOp::Gte) }
            other => Err(SqlError::UnexpectedToken {
                expected: "comparison operator (=, !=, <>, <, >, <=, >=)",
                got: format!("{other:?}"),
            }),
        }
    }

    // ── PIPELINE parser (MATCH + WITH) ───────────────────────────────────────

    /// Parse a `MATCH + WITH` pipeline statement.
    ///
    /// ```text
    /// MATCH start_pattern [WITH output_list MATCH start_pattern]* RETURN output_list [ORDER BY alias] [LIMIT n]
    /// ```
    pub fn parse_pipeline(&mut self) -> Result<crate::pipeline::Pipeline, SqlError> {
        use crate::pipeline::{Pipeline, PipelineStage, PipeProjectStage};

        let mut stages = Vec::new();

        // First MATCH (keyword already consumed by caller or to be consumed here)
        self.expect_kw(Kw::Match, "MATCH")?;
        stages.push(PipelineStage::Match(self.parse_pipe_match_stage()?));

        loop {
            match self.peek().clone() {
                Tok::Kw(Kw::With) => {
                    self.advance(); // consume WITH
                    let outputs = self.parse_pipe_output_list()?;
                    stages.push(PipelineStage::Project(PipeProjectStage {
                        outputs,
                        order_by: None,
                        limit: None,
                    }));
                    // Next must be MATCH (continue pipeline) or RETURN (fall through)
                    if matches!(self.peek(), Tok::Kw(Kw::Match)) {
                        self.advance(); // consume MATCH
                        stages.push(PipelineStage::Match(self.parse_pipe_match_stage()?));
                    } else if !matches!(self.peek(), Tok::Kw(Kw::Return)) {
                        return Err(SqlError::UnexpectedToken {
                            expected: "MATCH or RETURN after WITH",
                            got: format!("{:?}", self.peek()),
                        });
                    }
                    // If peek is Return, next loop iteration handles it
                }
                Tok::Kw(Kw::Return) => {
                    self.advance(); // consume RETURN
                    let outputs = self.parse_pipe_output_list()?;
                    let order_by = self.parse_pipe_opt_order_by()?;
                    let limit = self.parse_pipe_opt_limit()?;
                    stages.push(PipelineStage::Project(PipeProjectStage {
                        outputs,
                        order_by,
                        limit,
                    }));
                    break;
                }
                Tok::Eof => {
                    return Err(SqlError::UnexpectedEnd { expected: "WITH or RETURN" })
                }
                other => {
                    return Err(SqlError::UnexpectedToken {
                        expected: "WITH or RETURN",
                        got: format!("{other:?}"),
                    })
                }
            }
        }

        Ok(Pipeline { stages })
    }

    /// Parse a MATCH pattern (start node + hop chain).
    /// Caller has already consumed the MATCH keyword.
    fn parse_pipe_match_stage(&mut self) -> Result<crate::pipeline::PipeMatchStage, SqlError> {
        use crate::pipeline::{PipeHop, PipeMatchStage};

        let start = self.parse_pipe_match_start()?;
        let mut hops = Vec::new();

        // Each hop: -[:edge_type]->(bind[:col])
        while matches!(self.peek(), Tok::Dash) {
            self.advance(); // consume -
            self.expect_lbracket()?;
            if matches!(self.peek(), Tok::Colon) {
                self.advance();
            }
            let edge_type = self.expect_ident()?;
            self.expect_rbracket()?;
            self.expect_arrow()?;
            let (bind, col_filter) = self.parse_pipe_hop_node()?;
            hops.push(PipeHop {
                edge_type_hash: sk_hash(&edge_type),
                bind,
                col_filter,
            });
        }

        Ok(PipeMatchStage { start, hops })
    }

    /// Parse: `('slug')` or `(var:collection [WHERE ...])`
    fn parse_pipe_match_start(&mut self) -> Result<crate::pipeline::PipeMatchStart, SqlError> {
        use crate::pipeline::PipeMatchStart;

        self.expect_lparen()?;
        match self.peek().clone() {
            Tok::Str(slug) => {
                self.advance();
                self.expect_rparen()?;
                Ok(PipeMatchStart::Slug(sk_hash(&slug)))
            }
            Tok::Ident(_) | Tok::Kw(_) => {
                let name = self.expect_ident()?;
                match self.peek().clone() {
                    Tok::Colon => {
                        self.advance(); // consume :
                        let col = self.expect_ident()?;
                        let mut filters = vec![];
                        if matches!(self.peek(), Tok::Kw(Kw::Where)) {
                            self.advance(); // consume WHERE
                            filters = self.parse_pipe_where_conds()?;
                        }
                        self.expect_rparen()?;
                        Ok(PipeMatchStart::Collection {
                            bind: Some(name),
                            col_hash: sk_hash(&col),
                            filters,
                        })
                    }
                    Tok::RParen => {
                        self.advance(); // consume )
                        // Bare `(name)` — collection scan with bind, no filter
                        // Treat as all-nodes bind (useful for pipeline starts that reference prior result)
                        // We require a collection label for meaningful pipeline traversal
                        Err(SqlError::UnexpectedToken {
                            expected: ":collection after variable name in MATCH start",
                            got: format!(") (use ({}:collection) syntax)", name),
                        })
                    }
                    other => Err(SqlError::UnexpectedToken {
                        expected: ": or ) after variable name in MATCH start",
                        got: format!("{other:?}"),
                    }),
                }
            }
            other => Err(SqlError::UnexpectedToken {
                expected: "string literal or (var:collection) in MATCH start",
                got: format!("{other:?}"),
            }),
        }
    }

    /// Parse hop endpoint: `(bind[:collection])`
    fn parse_pipe_hop_node(&mut self) -> Result<(String, Option<u64>), SqlError> {
        self.expect_lparen()?;
        let bind = self.expect_ident()?;
        let col_filter = if matches!(self.peek(), Tok::Colon) {
            self.advance(); // consume :
            let col = self.expect_ident()?;
            Some(sk_hash(&col))
        } else {
            None
        };
        self.expect_rparen()?;
        Ok((bind, col_filter))
    }

    /// Parse WHERE conditions: `field = value_or_ref [AND ...]`
    fn parse_pipe_where_conds(&mut self) -> Result<Vec<crate::pipeline::PipeWhere>, SqlError> {
        let mut conds = vec![self.parse_pipe_where_cond()?];
        while matches!(self.peek(), Tok::Kw(Kw::And)) {
            self.advance();
            conds.push(self.parse_pipe_where_cond()?);
        }
        Ok(conds)
    }

    /// Parse: `field <op> literal_or_row_ref`
    /// Op: `=`  `!=`  `<>`  `<`  `<=`  `>`  `>=`
    fn parse_pipe_where_cond(&mut self) -> Result<crate::pipeline::PipeWhere, SqlError> {
        use crate::pipeline::{CmpOp, PipeWhere};

        let field = self.expect_ident()?;

        let op = match self.peek().clone() {
            Tok::Eq  => { self.advance(); CmpOp::Eq  }
            Tok::Neq => { self.advance(); CmpOp::Ne  }
            Tok::Lt  => { self.advance(); CmpOp::Lt  }
            Tok::Lte => { self.advance(); CmpOp::Lte }
            Tok::Gt  => { self.advance(); CmpOp::Gt  }
            Tok::Gte => { self.advance(); CmpOp::Gte }
            other => return Err(SqlError::UnexpectedToken {
                expected: "comparison operator (= != <> < <= > >=) in pipeline WHERE",
                got: format!("{other:?}"),
            }),
        };

        match self.peek().clone() {
            Tok::Str(s) => {
                self.advance();
                Ok(PipeWhere::FieldCmpLiteral { field, op, value: Value::String(s) })
            }
            Tok::Num(n) => {
                self.advance();
                let num = serde_json::Number::from_f64(n)
                    .map(Value::Number)
                    .unwrap_or(Value::Null);
                Ok(PipeWhere::FieldCmpLiteral { field, op, value: num })
            }
            Tok::Kw(Kw::True) => {
                self.advance();
                Ok(PipeWhere::FieldCmpLiteral { field, op, value: Value::Bool(true) })
            }
            Tok::Kw(Kw::False) => {
                self.advance();
                Ok(PipeWhere::FieldCmpLiteral { field, op, value: Value::Bool(false) })
            }
            Tok::Kw(Kw::Null) => {
                self.advance();
                Ok(PipeWhere::FieldCmpLiteral { field, op, value: Value::Null })
            }
            Tok::Ident(row_key) => {
                self.advance();
                Ok(PipeWhere::FieldCmpRowRef { field, op, row_key })
            }
            other => Err(SqlError::UnexpectedToken {
                expected: "literal or row reference in pipeline WHERE",
                got: format!("{other:?}"),
            }),
        }
    }

    /// Parse WITH/RETURN output list: `expr AS alias [, expr AS alias]*`
    fn parse_pipe_output_list(
        &mut self,
    ) -> Result<Vec<(crate::pipeline::PipeOutExpr, String)>, SqlError> {
        let mut items = vec![self.parse_pipe_output_item()?];
        while matches!(self.peek(), Tok::Comma) {
            self.advance();
            items.push(self.parse_pipe_output_item()?);
        }
        Ok(items)
    }

    fn parse_pipe_output_item(
        &mut self,
    ) -> Result<(crate::pipeline::PipeOutExpr, String), SqlError> {
        use crate::pipeline::{PipeExpr, PipeOutExpr};
        let expr = self.parse_pipe_out_expr()?;

        // `AS alias` is optional — derive alias from expression when absent
        let alias = if matches!(self.peek(), Tok::Kw(Kw::As)) {
            self.advance(); // consume AS
            self.expect_ident()?
        } else {
            match &expr {
                PipeOutExpr::Scalar(PipeExpr::RowKey(key)) => key.clone(),
                PipeOutExpr::Scalar(PipeExpr::RowField { field, .. }) => field.clone(),
                _ => {
                    return Err(SqlError::UnexpectedToken {
                        expected: "AS alias (required for aggregate expressions)",
                        got: format!("{:?}", self.peek()),
                    });
                }
            }
        };
        Ok((expr, alias))
    }

    fn parse_pipe_out_expr(&mut self) -> Result<crate::pipeline::PipeOutExpr, SqlError> {
        use crate::pipeline::PipeOutExpr;
        match self.peek().clone() {
            Tok::Kw(Kw::Sum) => {
                self.advance();
                Ok(PipeOutExpr::Sum(self.parse_pipe_paren_math_expr()?))
            }
            Tok::Kw(Kw::Avg) => {
                self.advance();
                Ok(PipeOutExpr::Avg(self.parse_pipe_paren_math_expr()?))
            }
            Tok::Kw(Kw::Min) => {
                self.advance();
                Ok(PipeOutExpr::Min(self.parse_pipe_paren_math_expr()?))
            }
            Tok::Kw(Kw::Max) => {
                self.advance();
                Ok(PipeOutExpr::Max(self.parse_pipe_paren_math_expr()?))
            }
            Tok::Kw(Kw::Count) => {
                self.advance();
                self.expect_lparen()?;
                if matches!(self.peek(), Tok::Star) {
                    self.advance();
                }
                self.expect_rparen()?;
                Ok(PipeOutExpr::Count)
            }
            _ => Ok(PipeOutExpr::Scalar(self.parse_pipe_math_expr()?)),
        }
    }

    fn parse_pipe_paren_math_expr(&mut self) -> Result<crate::pipeline::PipeExpr, SqlError> {
        self.expect_lparen()?;
        let expr = self.parse_pipe_math_expr()?;
        self.expect_rparen()?;
        Ok(expr)
    }

    fn parse_pipe_math_expr(&mut self) -> Result<crate::pipeline::PipeExpr, SqlError> {
        use crate::pipeline::PipeExpr;
        let mut left = self.parse_pipe_math_term()?;
        loop {
            match self.peek() {
                Tok::Plus => {
                    self.advance();
                    let right = self.parse_pipe_math_term()?;
                    left = PipeExpr::Add(Box::new(left), Box::new(right));
                }
                Tok::Dash => {
                    self.advance();
                    let right = self.parse_pipe_math_term()?;
                    left = PipeExpr::Sub(Box::new(left), Box::new(right));
                }
                _ => break,
            }
        }
        Ok(left)
    }

    fn parse_pipe_math_term(&mut self) -> Result<crate::pipeline::PipeExpr, SqlError> {
        use crate::pipeline::PipeExpr;
        let mut left = self.parse_pipe_math_primary()?;
        loop {
            match self.peek() {
                Tok::Star => {
                    self.advance();
                    let right = self.parse_pipe_math_primary()?;
                    left = PipeExpr::Mul(Box::new(left), Box::new(right));
                }
                Tok::Slash => {
                    self.advance();
                    let right = self.parse_pipe_math_primary()?;
                    left = PipeExpr::Div(Box::new(left), Box::new(right));
                }
                _ => break,
            }
        }
        Ok(left)
    }

    fn parse_pipe_math_primary(&mut self) -> Result<crate::pipeline::PipeExpr, SqlError> {
        use crate::pipeline::PipeExpr;
        match self.peek().clone() {
            Tok::Num(n) => {
                self.advance();
                Ok(PipeExpr::Literal(n))
            }
            Tok::LParen => {
                self.advance();
                let expr = self.parse_pipe_math_expr()?;
                self.expect_rparen()?;
                Ok(expr)
            }
            Tok::Ident(_) | Tok::Kw(_) => {
                let name = self.expect_ident()?;
                if matches!(self.peek(), Tok::Dot) {
                    self.advance(); // consume .
                    let field = self.expect_ident()?;
                    Ok(PipeExpr::RowField { var: name, field })
                } else {
                    Ok(PipeExpr::RowKey(name))
                }
            }
            other => Err(SqlError::UnexpectedToken {
                expected: "number, identifier, or ( in expression",
                got: format!("{other:?}"),
            }),
        }
    }

    fn parse_pipe_opt_order_by(&mut self) -> Result<Option<(String, bool)>, SqlError> {
        if !matches!(self.peek(), Tok::Kw(Kw::Order)) {
            return Ok(None);
        }
        self.advance(); // ORDER
        self.expect_kw(Kw::By, "BY")?;
        let field = self.expect_ident()?;
        let asc = !matches!(self.peek(), Tok::Kw(Kw::Desc));
        if !asc {
            self.advance(); // consume DESC
        } else if matches!(self.peek(), Tok::Kw(Kw::Asc)) {
            self.advance(); // consume optional ASC
        }
        Ok(Some((field, asc)))
    }

    fn parse_pipe_opt_limit(&mut self) -> Result<Option<usize>, SqlError> {
        if !matches!(self.peek(), Tok::Kw(Kw::Limit)) {
            return Ok(None);
        }
        self.advance(); // LIMIT
        let n = self.expect_num()?;
        Ok(Some(n as usize))
    }

    fn expect_arrow(&mut self) -> Result<(), SqlError> {
        if matches!(self.peek(), Tok::Arrow) {
            self.advance();
            Ok(())
        } else {
            Err(SqlError::UnexpectedToken {
                expected: "->",
                got: format!("{:?}", self.peek()),
            })
        }
    }
}

// ── Compiler ──────────────────────────────────────────────────────────────────

fn compile_match(stmt: MatchStmt) -> Vec<Step> {
    let mut steps: Vec<Step> = Vec::new();

    // 1. Starter — check if WHERE has _key on start var for O(1) lookup
    let start_var = stmt.start.var.as_deref().unwrap_or("");
    let key_cond_idx = stmt.conditions.iter().position(|c| {
        let MatchCond::NodeField {
            var,
            field,
            op,
            value,
        } = c;
        var == start_var && field == "_key" && matches!(op, CompareOp::Eq) && value.is_string()
    });

    if let Some(idx) = key_cond_idx {
        let MatchCond::NodeField { value, .. } = &stmt.conditions[idx];
        let key = value.as_str().unwrap();
        // Construct full slug: collection/_key if label present, else just _key
        let slug = match &stmt.start.label {
            Some(label) => format!("{}/{}", label, key),
            None => key.to_string(),
        };
        steps.push(Step::One(sk_hash(&slug)));
    } else if let Some(label) = &stmt.start.label {
        steps.push(Step::Collection(sk_hash(label)));
    } else {
        steps.push(Step::All);
    }

    // 2. Start node filters from WHERE (skip the _key condition already used)
    for (i, cond) in stmt.conditions.iter().enumerate() {
        if Some(i) == key_cond_idx {
            continue;
        }
        let MatchCond::NodeField {
            var,
            field,
            op,
            value,
        } = cond;
        if var == start_var && field != "strength" {
            let step = match op {
                CompareOp::Eq => Step::WhereEq(field.clone(), value.clone()),
                CompareOp::Neq => Step::WhereNeq(field.clone(), value.clone()),
                CompareOp::Gt => Step::WhereGt(field.clone(), value.as_f64().unwrap_or(0.0)),
                CompareOp::Lt => Step::WhereLt(field.clone(), value.as_f64().unwrap_or(0.0)),
                CompareOp::Gte => Step::WhereGte(field.clone(), value.as_f64().unwrap_or(0.0)),
                CompareOp::Lte => Step::WhereLte(field.clone(), value.as_f64().unwrap_or(0.0)),
            };
            steps.push(step);
        }
    }

    // 3. Start inline props → WhereEq
    for (key, val) in &stmt.start.props {
        steps.push(Step::WhereEq(key.clone(), val.clone()));
    }

    // 4. Traversal
    if let Some(depth) = stmt.edge.depth {
        // Multi-hop typed BFS
        if let Some(kind) = &stmt.edge.kind {
            steps.push(Step::HopsTyped {
                type_hash: sk_hash(kind),
                min_depth: depth.0,
                max_depth: depth.1,
            });
        } else {
            // Untyped multi-hop → use regular Hops
            steps.push(Step::Hops(depth.1));
        }
    } else if let Some(kind) = &stmt.edge.kind {
        // Single hop, typed
        match stmt.edge.dir {
            EdgeDir::Forward => steps.push(Step::Forward(sk_hash(kind))),
            EdgeDir::Backward => steps.push(Step::Backward(sk_hash(kind))),
        }
    }

    // 5. Edge filters (r.strength >= N)
    let edge_var = stmt.edge.var.as_deref().unwrap_or("");
    for cond in &stmt.conditions {
        let MatchCond::NodeField {
            var,
            field,
            op,
            value,
        } = cond;
        if var == edge_var && field == "strength" {
            if let (CompareOp::Gte, Some(n)) = (op, value.as_f64()) {
                steps.push(Step::MinStrength(n as f32));
            }
        }
    }

    // 6-8. End node — build Intersect with all filters inside.
    //
    // Moving end filters inside the Intersect lets `btree_seed` fire on the inner
    // Collection step (same optimisation that already applies to SELECT … WHERE).
    // When the end node has `_key = 'X'` we go one step further and replace the
    // Collection with a O(1) `One()` lookup, mirroring the start-node path above.
    let end_var = stmt.end.var.as_deref().unwrap_or("");

    // _key condition on end var (WHERE b._key = 'X')
    let end_key_cond_idx = stmt.conditions.iter().position(|c| {
        let MatchCond::NodeField { var, field, op, value } = c;
        !end_var.is_empty()
            && var == end_var
            && field == "_key"
            && matches!(op, CompareOp::Eq)
            && value.is_string()
    });

    // _key in end inline props  (:genre {_key: 'X'})
    let end_inline_key: Option<String> = stmt.end.props.iter()
        .find(|(k, _)| k == "_key")
        .and_then(|(_, v)| v.as_str().map(str::to_string));

    let has_end_label   = stmt.end.label.is_some();
    let has_end_starter = has_end_label
        || end_key_cond_idx.is_some()
        || end_inline_key.is_some();

    if has_end_starter {
        let mut end_steps: Vec<Step> = Vec::new();

        // ── Starter ───────────────────────────────────────────────────
        if let Some(idx) = end_key_cond_idx {
            let MatchCond::NodeField { value, .. } = &stmt.conditions[idx];
            let key = value.as_str().unwrap();
            let slug = match &stmt.end.label {
                Some(label) => format!("{}/{}", label, key),
                None        => key.to_string(),
            };
            end_steps.push(Step::One(sk_hash(&slug)));
        } else if let Some(ref key) = end_inline_key {
            let slug = match &stmt.end.label {
                Some(label) => format!("{}/{}", label, key),
                None        => key.clone(),
            };
            end_steps.push(Step::One(sk_hash(&slug)));
        } else if let Some(label) = &stmt.end.label {
            end_steps.push(Step::Collection(sk_hash(label)));
        }

        // ── End WHERE filters (skip _key already consumed above) ──────
        for (i, cond) in stmt.conditions.iter().enumerate() {
            if Some(i) == end_key_cond_idx { continue; }
            let MatchCond::NodeField { var, field, op, value } = cond;
            if !end_var.is_empty() && var == end_var {
                let step = match op {
                    CompareOp::Eq  => Step::WhereEq(field.clone(), value.clone()),
                    CompareOp::Neq => Step::WhereNeq(field.clone(), value.clone()),
                    CompareOp::Gt  => Step::WhereGt(field.clone(), value.as_f64().unwrap_or(0.0)),
                    CompareOp::Lt  => Step::WhereLt(field.clone(), value.as_f64().unwrap_or(0.0)),
                    CompareOp::Gte => Step::WhereGte(field.clone(), value.as_f64().unwrap_or(0.0)),
                    CompareOp::Lte => Step::WhereLte(field.clone(), value.as_f64().unwrap_or(0.0)),
                };
                end_steps.push(step);
            }
        }

        // ── End inline props (skip _key already consumed above) ───────
        for (key, val) in &stmt.end.props {
            if end_inline_key.is_some() && key == "_key" { continue; }
            end_steps.push(Step::WhereEq(key.clone(), val.clone()));
        }

        steps.push(Step::Intersect(end_steps));
    } else {
        // No label and no _key on end var — Intersect without a Collection/One
        // starter would just be an O(N) full scan with no benefit.  Fall back to
        // plain filter steps appended after the traversal.
        for cond in &stmt.conditions {
            let MatchCond::NodeField { var, field, op, value } = cond;
            if !end_var.is_empty() && var == end_var {
                let step = match op {
                    CompareOp::Eq  => Step::WhereEq(field.clone(), value.clone()),
                    CompareOp::Neq => Step::WhereNeq(field.clone(), value.clone()),
                    CompareOp::Gt  => Step::WhereGt(field.clone(), value.as_f64().unwrap_or(0.0)),
                    CompareOp::Lt  => Step::WhereLt(field.clone(), value.as_f64().unwrap_or(0.0)),
                    CompareOp::Gte => Step::WhereGte(field.clone(), value.as_f64().unwrap_or(0.0)),
                    CompareOp::Lte => Step::WhereLte(field.clone(), value.as_f64().unwrap_or(0.0)),
                };
                steps.push(step);
            }
        }
        for (key, val) in &stmt.end.props {
            steps.push(Step::WhereEq(key.clone(), val.clone()));
        }
    }

    // 9. LIMIT
    if let Some(n) = stmt.limit {
        steps.push(Step::Take(n));
    }

    steps
}

/// Convert a single CondExpr to a Step.
fn compile_cond(cond: CondExpr) -> Step {
    match cond {
        CondExpr::Compare { field, op, value } => match op {
            CompareOp::Eq => Step::WhereEq(field, value),
            CompareOp::Neq => Step::WhereNeq(field, value),
            CompareOp::Gt => Step::WhereGt(field, value.as_f64().unwrap_or(0.0)),
            CompareOp::Lt => Step::WhereLt(field, value.as_f64().unwrap_or(0.0)),
            CompareOp::Gte => Step::WhereGte(field, value.as_f64().unwrap_or(0.0)),
            CompareOp::Lte => Step::WhereLte(field, value.as_f64().unwrap_or(0.0)),
        },
        CondExpr::Between { field, lo, hi } => Step::WhereBetween(field, lo, hi),
        CondExpr::In { field, values } => Step::WhereIn(field, values),
        CondExpr::Like {
            field,
            pattern,
            case_insensitive,
        } => Step::Like(field, pattern, case_insensitive),
        CondExpr::StDWithin {
            lat,
            lon,
            distance_km,
        } => Step::StDWithin(lat, lon, distance_km),
        CondExpr::StContainsPoint { lat, lon } => Step::StContainsPoint(lat, lon),
        CondExpr::StWithin { ring } => Step::StWithin(ring),
        CondExpr::StContains { ring } => Step::StContains(ring),
        CondExpr::StIntersects { ring } => Step::StIntersects(ring),
        CondExpr::StDistance {
            field,
            lat,
            lon,
            max_km,
        } => Step::StDistance(field, lat, lon, max_km),
        CondExpr::StLength { field, min_km } => Step::StLength(field, min_km),
        CondExpr::StArea { field, min_km2 } => Step::StArea(field, min_km2),
        CondExpr::Bm25 {
            field,
            query,
            min_score,
        } => Step::Bm25Filter(field, query, min_score),
        CondExpr::Bm25Func { .. } => unreachable!("Bm25Func should not reach compile_cond"),
        CondExpr::VectorNear { field, query, k } => Step::VectorNear { field, query, k },
        CondExpr::IsNull { field, negated } => Step::WhereIsNull(field, negated),
        CondExpr::Not(inner) => Step::WhereNot(Box::new(compile_cond(*inner))),
        CondExpr::Or(groups) => Step::WhereOr(
            groups
                .into_iter()
                .map(|group| group.into_iter().map(compile_cond).collect())
                .collect(),
        ),
    }
}

/// Append ORDER BY / OFFSET / LIMIT / SELECT steps to a pipeline.
fn append_tail(
    steps: &mut Vec<Step>,
    order_by: Option<OrderKey>,
    offset: Option<usize>,
    limit: Option<usize>,
    fields: Vec<String>,
    bm25_scores: Vec<(String, String)>,
) {
    if let Some(order_key) = order_by {
        match order_key {
            OrderKey::Fields(cols) => {
                steps.push(Step::Sort(cols));
            }
            OrderKey::Bm25(field, query, ascending) => {
                steps.push(Step::Bm25Sort(field, query, ascending));
            }
            OrderKey::Vector { field, query, metric } => {
                steps.push(Step::SortByVector { field, query, metric });
            }
            OrderKey::Expr(expr, ascending) => {
                steps.push(Step::SortByExpr { expr, ascending });
            }
        }
    }
    if let Some(n) = offset {
        steps.push(Step::Skip(n));
    }
    if let Some(n) = limit {
        steps.push(Step::Take(n));
    }
    for (field, query) in bm25_scores {
        steps.push(Step::Bm25Score(field, query));
    }
    if !fields.is_empty() {
        steps.push(Step::Select(fields));
    }
}

/// Returns true for any spatial CondExpr variant.
fn is_spatial_cond(c: &CondExpr) -> bool {
    matches!(
        c,
        CondExpr::StDWithin { .. }
            | CondExpr::StContainsPoint { .. }
            | CondExpr::StWithin { .. }
            | CondExpr::StContains { .. }
            | CondExpr::StIntersects { .. }
            | CondExpr::StDistance { .. }
            | CondExpr::StLength { .. }
            | CondExpr::StArea { .. }
    )
}

fn compile(stmt: SelectStmt) -> Vec<Step> {
    let SelectStmt {
        fields,
        source,
        conditions,
        group_by,
        having,
        distinct,
        order_by,
        limit,
        offset,
        bm25_scores,
    } = stmt;
    let mut steps: Vec<Step> = Vec::new();

    // ── Fast-path 1: Collection + WHERE _key = 'val' → O(1) One(hash) ───────────
    //
    // Instead of loading every node in the collection and scanning payloads,
    // compute the slug hash directly and emit a single-node lookup.
    if let Source::Collection(ref name) = source {
        let key_pos = conditions.iter().position(|c| {
            matches!(c, CondExpr::Compare {
                field,
                op: CompareOp::Eq,
                value: Value::String(_),
            } if field == "_key")
        });
        if let Some(pos) = key_pos {
            let key_val = match &conditions[pos] {
                CondExpr::Compare {
                    value: Value::String(s),
                    ..
                } => s.clone(),
                _ => unreachable!(),
            };
            steps.push(Step::One(sk_hash(&format!("{}/{}", name, key_val))));
            for (i, cond) in conditions.into_iter().enumerate() {
                if i != pos {
                    steps.push(compile_cond(cond));
                }
            }
            push_group_having_distinct(&mut steps, group_by, having, distinct);
            append_tail(&mut steps, order_by, offset, limit, fields, bm25_scores);
            return steps;
        }
    }

    // ── Fast-path 2: Collection + spatial WHERE → grid starter ───────────────────
    //
    // When the spatial index is available, we want the grid to produce the initial
    // candidate list (~tens of nodes) rather than loading the entire collection
    // (~thousands) and then filtering.  Emit spatial steps first with an empty
    // candidates list so they act as starters, then gate on _collection membership.
    if let Source::Collection(ref name) = source {
        if conditions.iter().any(is_spatial_cond) {
            let coll_name = name.clone();
            // Partition once: spatial goes first (as starters), rest follows
            let (spatial_conds, other_conds): (Vec<CondExpr>, Vec<CondExpr>) =
                conditions.into_iter().partition(|c| is_spatial_cond(c));
            for cond in spatial_conds {
                steps.push(compile_cond(cond));
            }
            // Gate on collection membership (nodes without _collection are excluded)
            steps.push(Step::WhereEq(
                "_collection".to_string(),
                Value::String(coll_name),
            ));
            for cond in other_conds {
                steps.push(compile_cond(cond));
            }
            push_group_having_distinct(&mut steps, group_by, having, distinct);
            append_tail(&mut steps, order_by, offset, limit, fields, bm25_scores);
            return steps;
        }
    }

    // ── Default path ──────────────────────────────────────────────────────────────
    match source {
        Source::Collection(name) => steps.push(Step::Collection(sk_hash(&name))),
        Source::All => steps.push(Step::All),
    }
    for cond in conditions {
        steps.push(compile_cond(cond));
    }
    push_group_having_distinct(&mut steps, group_by, having, distinct);
    append_tail(&mut steps, order_by, offset, limit, fields, bm25_scores);
    steps
}

/// Emit GROUP BY / HAVING / DISTINCT steps between conditions and the tail.
fn push_group_having_distinct(
    steps: &mut Vec<Step>,
    group_by: Vec<String>,
    having: Vec<CondExpr>,
    distinct: bool,
) {
    if !group_by.is_empty() {
        steps.push(Step::GroupBy(group_by));
    }
    if !having.is_empty() {
        let having_steps: Vec<Step> = having.into_iter().map(compile_cond).collect();
        steps.push(Step::Having(having_steps));
    }
    if distinct {
        steps.push(Step::Distinct);
    }
}

/// If a `Value::Array` contains only numbers, return them as `Vec<f32>`.
/// Used to detect vector literals in INSERT/UPDATE values.
fn value_as_f32_vec(v: &Value) -> Option<Vec<f32>> {
    let arr = v.as_array()?;
    if arr.is_empty() {
        return None;
    }
    arr.iter()
        .map(|x| x.as_f64().map(|f| f as f32))
        .collect()
}

/// If a string value looks like JSON (`{...}` or `[...]`), parse it into a
/// native JSON object/array. This enables inserting geometry and nested data.
fn maybe_parse_json_string(value: Value) -> Value {
    if let Value::String(ref s) = value {
        let trimmed = s.trim();
        if (trimmed.starts_with('{') && trimmed.ends_with('}'))
            || (trimmed.starts_with('[') && trimmed.ends_with(']'))
        {
            if let Ok(parsed) = serde_json::from_str::<Value>(trimmed) {
                return parsed;
            }
        }
    }
    value
}

fn compile_insert(stmt: InsertStmt) -> Result<CompiledMutation, SqlError> {
    if stmt.fields.len() != stmt.values.len() {
        return Err(SqlError::FieldValueCountMismatch {
            fields: stmt.fields.len(),
            values: stmt.values.len(),
        });
    }
    let key_idx = stmt
        .fields
        .iter()
        .position(|f| f == "_key")
        .ok_or(SqlError::MissingField { field: "_key" })?;
    let slug = match &stmt.values[key_idx] {
        Value::String(s) => format!("{}/{}", stmt.collection, s),
        other => {
            return Err(SqlError::InvalidValue(format!(
                "_key must be a string, got {other}"
            )))
        }
    };
    let mut obj = serde_json::Map::new();
    obj.insert("_collection".into(), Value::String(stmt.collection.clone()));
    obj.insert("_id".into(), Value::String(slug.clone()));
    let mut vectors: Vec<(String, Vec<f32>)> = Vec::new();
    for (field, value) in stmt.fields.into_iter().zip(stmt.values) {
        if let Some(floats) = value_as_f32_vec(&value) {
            // Array-of-numbers → vector field, not stored in the JSON payload
            vectors.push((field, floats));
        } else {
            obj.insert(field, maybe_parse_json_string(value));
        }
    }
    let payload_json =
        serde_json::to_string(&obj).map_err(|e| SqlError::InvalidValue(e.to_string()))?;
    Ok(CompiledMutation::Insert { collection: stmt.collection, slug, payload_json, vectors })
}

fn compile_delete(stmt: DeleteStmt) -> Vec<Step> {
    let mut steps: Vec<Step> = Vec::new();
    match stmt.source {
        Source::Collection(name) => steps.push(Step::Collection(sk_hash(&name))),
        Source::All => steps.push(Step::All),
    }
    for cond in stmt.conditions {
        steps.push(compile_cond(cond));
    }
    steps
}

fn compile_update(stmt: UpdateStmt) -> CompiledMutation {
    let mut steps: Vec<Step> = vec![Step::Collection(sk_hash(&stmt.collection))];
    for cond in stmt.conditions {
        steps.push(compile_cond(cond));
    }
    CompiledMutation::Update {
        steps,
        updates: stmt.updates,
    }
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Parse a `MATCH + WITH` pipeline statement.
///
/// # Errors
/// Returns [`SqlError`] if the statement is syntactically invalid.
pub fn parse_pipeline(sql: &str) -> Result<crate::pipeline::Pipeline, SqlError> {
    let tokens = tokenize(sql)?;
    let mut parser = Parser::new(tokens);
    parser.parse_pipeline()
}

/// The result of parsing a MATCH statement — either a compiled step pipeline
/// (simple graph traversal) or an aggregate statement.
pub enum MatchOrAgg {
    /// Standard MATCH — compiled to a Vec<Step> pipeline.
    Steps(Vec<Step>),
    /// Aggregate MATCH — must be executed via `execute_match_agg`.
    Agg(crate::query::MatchAggStmt),
    /// `SELECT … FROM MATCH SHORTEST` — must be executed via `execute_shortest_select`.
    Shortest(crate::query::ShortestSelectStmt),
    /// `SELECT … FROM source1, source2, …` — must be executed via `execute_multi_from`.
    MultiFrom(crate::query::MultiFromStmt),
}

/// Parse a MATCH statement and determine whether it is a simple graph query
/// or an aggregate (multi-hop with RETURN var.field / aggregate functions).
///
/// This is the unified entry-point called by `db.query()`.
///
/// # Errors
/// Returns [`SqlError`] if the statement is syntactically invalid.
pub fn parse_match_or_agg(sql: &str) -> Result<MatchOrAgg, SqlError> {
    let tokens = tokenize(sql)?;

    // Multi-FROM: SELECT … FROM source1, source2, … (comma between FROM sources)
    if is_multi_from(&tokens) {
        let stmt = Parser::new(tokens).parse_select_multi_from()?;
        return Ok(MatchOrAgg::MultiFrom(stmt));
    }

    // SELECT … FROM MATCH [SHORTEST] — check before regular SELECT routing.
    if is_select_from_match(&tokens) {
        if is_select_from_match_shortest(&tokens) {
            let stmt = Parser::new(tokens).parse_select_from_match_shortest()?;
            return Ok(MatchOrAgg::Shortest(stmt));
        }
        let stmt = Parser::new(tokens).parse_select_from_match()?;
        return Ok(MatchOrAgg::Agg(stmt));
    }

    // Non-MATCH SQL goes through the regular pipeline.
    if !matches!(tokens.first(), Some(Tok::Kw(Kw::Match))) {
        let mut parser = Parser::new(tokens);
        let stmt = parser.parse()?;
        return Ok(MatchOrAgg::Steps(compile(stmt)));
    }

    // Simple MATCH — compile to Steps.
    let mut parser = Parser::new(tokens);
    let stmt = parser.parse_match()?;
    let mut all_steps = compile_match(stmt);
    while matches!(parser.peek(), Tok::Kw(Kw::Union)) {
        parser.advance();
        let next_stmt = parser.parse_match()?;
        let next_steps = compile_match(next_stmt);
        all_steps.push(Step::Union(next_steps));
    }
    Ok(MatchOrAgg::Steps(all_steps))
}

/// Return `true` when the SQL is `SELECT … FROM MATCH (…)-[…]->(…) …`.
fn is_select_from_match(tokens: &[Tok]) -> bool {
    if !matches!(tokens.first(), Some(Tok::Kw(Kw::Select))) {
        return false;
    }
    tokens.windows(2).any(|w| {
        matches!(w, [Tok::Kw(Kw::From), Tok::Kw(Kw::Match)])
    })
}

/// Return `true` when the SQL is `SELECT … FROM MATCH SHORTEST (…)-[…]->(…) …`.
fn is_select_from_match_shortest(tokens: &[Tok]) -> bool {
    // Look for FROM MATCH SHORTEST (3-token sequence)
    tokens.windows(3).any(|w| {
        matches!(w[0], Tok::Kw(Kw::From))
        && matches!(w[1], Tok::Kw(Kw::Match))
        && matches!(&w[2], Tok::Ident(name) if name.eq_ignore_ascii_case("SHORTEST"))
    })
}

/// Return `true` when the SQL has multiple comma-separated FROM sources.
///
/// Detects a top-level comma after the FROM keyword (paren depth = 0).
fn is_multi_from(tokens: &[Tok]) -> bool {
    if !matches!(tokens.first(), Some(Tok::Kw(Kw::Select))) {
        return false;
    }
    // Find the FROM keyword position
    let from_pos = match tokens.iter().position(|t| matches!(t, Tok::Kw(Kw::From))) {
        Some(p) => p,
        None => return false,
    };
    // Scan after FROM for a top-level comma
    let mut depth: usize = 0;
    for tok in &tokens[from_pos + 1..] {
        match tok {
            Tok::LParen => depth += 1,
            Tok::RParen => { if depth > 0 { depth -= 1; } }
            Tok::Comma if depth == 0 => return true,
            // Stop scanning at ORDER/LIMIT/WHERE/GROUP (these appear after all FROM sources)
            Tok::Kw(Kw::Where | Kw::Order | Kw::Limit | Kw::Group) if depth == 0 => break,
            _ => {}
        }
    }
    false
}

/// Map an uppercase PATH_* / time function name + (var, field) to a `MatchAggReturn`.
fn path_func_variant(
    name: &str,
    var: String,
    field: String,
) -> Result<crate::query::MatchAggReturn, SqlError> {
    use crate::query::MatchAggReturn;
    match name {
        "PATH_AVG"     => Ok(MatchAggReturn::PathAvg { var, field }),
        "PATH_SUM"     => Ok(MatchAggReturn::PathSum { var, field }),
        "PATH_MIN"     => Ok(MatchAggReturn::PathMin { var, field }),
        "PATH_MAX"     => Ok(MatchAggReturn::PathMax { var, field }),
        "PATH_PRODUCT" => Ok(MatchAggReturn::PathProduct { var, field }),
        "PATH_FIRST"   => Ok(MatchAggReturn::PathFirst { var, field }),
        "PATH_LAST"    => Ok(MatchAggReturn::PathLast { var, field }),
        "AGE_DAYS"     => Ok(MatchAggReturn::AgeDays { var, field }),
        "AGE_HOURS"    => Ok(MatchAggReturn::AgeHours { var, field }),
        "JSON_ARRAY_LENGTH" => Ok(MatchAggReturn::JsonArrayLen { var, field }),
        _ => Err(SqlError::UnexpectedToken {
            expected: "known PATH_* or time function",
            got: name.to_string(),
        }),
    }
}

/// Look at the token stream to determine if a MATCH statement has an
/// aggregate RETURN clause (var.field or SUM/AVG/MIN/MAX/COUNT).
fn is_agg_match(tokens: &[Tok]) -> bool {
    // Find the RETURN keyword position.
    let return_pos = tokens.iter().position(|t| matches!(t, Tok::Kw(Kw::Return)));
    let pos = match return_pos {
        Some(p) => p + 1,
        None => return false,
    };
    // Skip to the token after RETURN.
    match tokens.get(pos) {
        // COUNT/SUM/AVG/MIN/MAX → aggregate
        Some(Tok::Kw(Kw::Count))
        | Some(Tok::Kw(Kw::Sum))
        | Some(Tok::Kw(Kw::Avg))
        | Some(Tok::Kw(Kw::Min))
        | Some(Tok::Kw(Kw::Max)) => true,
        // CASE WHEN → aggregate
        Some(Tok::Kw(Kw::Case)) => true,
        // identifier followed by `.` → var.field (aggregate field ref)
        // identifier that is a PATH_* or time function → aggregate
        Some(Tok::Ident(name)) => {
            let upper = name.to_ascii_uppercase();
            matches!(tokens.get(pos + 1), Some(Tok::Dot))
                || upper.starts_with("PATH_")
                || matches!(upper.as_str(), "AGE_DAYS" | "AGE_HOURS" | "JSON_ARRAY_LENGTH" | "NOW")
        }
        _ => false,
    }
}

/// Parse an SQL SELECT string and compile it to a `Vec<Step>`.
///
/// This is a pure function — no DB access required.
/// You can call it to validate SQL ahead of time, or pass the result to
/// [`Set::from_steps`](crate::Set::from_steps).
///
/// # Errors
/// Returns [`SqlError`] if the SQL is syntactically invalid.
pub fn parse_and_compile(sql: &str) -> Result<Vec<Step>, SqlError> {
    match parse_match_or_agg(sql)? {
        MatchOrAgg::Steps(steps) => Ok(steps),
        MatchOrAgg::Agg(_) => Err(SqlError::UnexpectedToken {
            expected: "simple MATCH or SELECT (not aggregate MATCH)",
            got: "aggregate SELECT FROM MATCH".into(),
        }),
        MatchOrAgg::Shortest(_) => Err(SqlError::UnexpectedToken {
            expected: "simple MATCH or SELECT (not MATCH SHORTEST)",
            got: "SELECT FROM MATCH SHORTEST".into(),
        }),
        MatchOrAgg::MultiFrom(_) => Err(SqlError::UnexpectedToken {
            expected: "simple MATCH or SELECT (not multi-FROM)",
            got: "SELECT FROM multiple sources".into(),
        }),
    }
}

/// Parse a mutation SQL statement and compile it to a [`CompiledMutation`].
///
/// # Node INSERT
/// ```text
/// INSERT INTO collection (_key, field, ...) VALUES ('key', val, ...)
/// ```
///
/// # Edge INSERT (Cypher-style)
/// ```text
/// INSERT ('from')-[:KIND {strength: n, key: val}]->('to')
/// ```
///
/// # Node DELETE
/// ```text
/// DELETE FROM collection [WHERE field OP value [AND ...]]
/// DELETE FROM ALL [WHERE ...]
/// ```
///
/// # Edge DELETE (Cypher-style)
/// ```text
/// DELETE ('from')-[:KIND]->('to')
/// ```
///
/// # Errors
/// Returns [`SqlError`] if the SQL is syntactically invalid, or if an INSERT
/// is missing a `_key` field or has mismatched field/value counts.
pub fn parse_mutation(sql: &str) -> Result<CompiledMutation, SqlError> {
    let tokens = tokenize(sql)?;
    let mut parser = Parser::new(tokens);
    match parser.peek().clone() {
        Tok::Kw(Kw::Insert) => {
            parser.advance(); // consume INSERT
            match parser.peek() {
                Tok::Kw(Kw::Into) => {
                    let stmt = parser.parse_insert_node()?;
                    compile_insert(stmt)
                }
                Tok::LParen => {
                    let edges = parser.parse_insert_edge()?;
                    Ok(CompiledMutation::InsertEdge(edges))
                }
                Tok::Eof => Err(SqlError::UnexpectedEnd {
                    expected: "INTO or (",
                }),
                other => Err(SqlError::UnexpectedToken {
                    expected: "INTO or (",
                    got: format!("{other:?}"),
                }),
            }
        }
        Tok::Kw(Kw::Delete) => {
            parser.advance(); // consume DELETE
            match parser.peek() {
                Tok::Kw(Kw::From) => {
                    let stmt = parser.parse_delete_node()?;
                    Ok(CompiledMutation::Delete(compile_delete(stmt)))
                }
                Tok::LParen => {
                    let edges = parser.parse_delete_edge()?;
                    Ok(CompiledMutation::DeleteEdge(edges))
                }
                Tok::Eof => Err(SqlError::UnexpectedEnd {
                    expected: "FROM or (",
                }),
                other => Err(SqlError::UnexpectedToken {
                    expected: "FROM or (",
                    got: format!("{other:?}"),
                }),
            }
        }
        Tok::Kw(Kw::Update) => {
            let stmt = parser.parse_update()?;
            Ok(compile_update(stmt))
        }
        Tok::Kw(Kw::Match) => parser.parse_match_insert(),
        Tok::Kw(Kw::Create) => {
            parser.advance(); // consume CREATE
            match parser.peek() {
                Tok::Kw(Kw::Index) => parser.parse_create_index(),
                _ => {
                    let schema = parser.parse_create_table()?;
                    Ok(CompiledMutation::CreateTable {
                        collection: schema.collection.clone(),
                        schema,
                    })
                }
            }
        }
        Tok::Kw(Kw::Drop) => {
            parser.advance(); // consume DROP
            match parser.peek().clone() {
                // DROP TABLE [IF EXISTS] collection
                Tok::Kw(Kw::Table) => {
                    parser.advance(); // consume TABLE
                    let if_exists = if matches!(parser.peek(), Tok::Kw(Kw::If)) {
                        parser.advance();
                        match parser.peek().clone() {
                            Tok::Kw(Kw::Exists) => { parser.advance(); true }
                            other => return Err(SqlError::UnexpectedToken {
                                expected: "EXISTS",
                                got: format!("{other:?}"),
                            }),
                        }
                    } else { false };
                    let collection = parser.expect_ident()?;
                    Ok(CompiledMutation::DropTable { collection, if_exists })
                }
                // DROP INDEX [IF EXISTS] ON collection USING method (field)
                Tok::Kw(Kw::Index) => {
                    parser.advance(); // consume INDEX
                    let if_exists = if matches!(parser.peek(), Tok::Kw(Kw::If)) {
                        parser.advance();
                        match parser.peek().clone() {
                            Tok::Kw(Kw::Exists) => { parser.advance(); true }
                            other => return Err(SqlError::UnexpectedToken {
                                expected: "EXISTS",
                                got: format!("{other:?}"),
                            }),
                        }
                    } else { false };
                    parser.expect_kw(Kw::On, "ON")?;
                    let collection = parser.expect_ident()?;
                    parser.expect_kw(Kw::Using, "USING")?;
                    let method_str = parser.expect_ident()?;
                    let method = match method_str.to_ascii_uppercase().as_str() {
                        "BTREE"   => IndexMethod::Btree,
                        "HASH"    => IndexMethod::Hash,
                        "GIN"     => IndexMethod::Gin,
                        "GIST"    => IndexMethod::Gist,
                        "BM25"    => IndexMethod::Bm25,
                        "SPATIAL" => IndexMethod::Spatial,
                        "HNSW"    => IndexMethod::Hnsw,
                        other => return Err(SqlError::UnexpectedToken {
                            expected: "BTREE, HASH, GIN, GIST, BM25, SPATIAL, or HNSW",
                            got: other.to_string(),
                        }),
                    };
                    parser.expect_lparen()?;
                    let field = parser.expect_ident()?;
                    parser.expect_rparen()?;
                    Ok(CompiledMutation::DropIndex { collection, method, field, if_exists })
                }
                other => Err(SqlError::UnexpectedToken {
                    expected: "TABLE or INDEX",
                    got: format!("{other:?}"),
                }),
            }
        }
        Tok::Kw(Kw::Alter) => {
            parser.advance(); // consume ALTER
            parser.parse_alter_table()
        }
        Tok::Kw(Kw::Reindex) => {
            parser.advance(); // consume REINDEX
            parser.expect_kw(Kw::On, "ON")?;
            let collection = parser.expect_ident()?;
            parser.expect_kw(Kw::Using, "USING")?;
            let method_str = parser.expect_ident()?;
            let method = match method_str.to_lowercase().as_str() {
                "btree"   => IndexMethod::Btree,
                "hash"    => IndexMethod::Hash,
                "gin"     => IndexMethod::Gin,
                "gist"    => IndexMethod::Gist,
                "bm25"    => IndexMethod::Bm25,
                "spatial" => IndexMethod::Spatial,
                "hnsw"    => IndexMethod::Hnsw,
                other => return Err(SqlError::UnexpectedToken {
                    expected: "btree, hash, gin, gist, bm25, spatial, or hnsw",
                    got: other.to_string(),
                }),
            };
            parser.expect_lparen()?;
            let mut fields = vec![parser.expect_ident()?];
            while matches!(parser.peek(), Tok::Comma) {
                parser.advance();
                fields.push(parser.expect_ident()?);
            }
            parser.expect_rparen()?;
            Ok(CompiledMutation::Reindex { collection, method, fields })
        }
        Tok::Eof => Err(SqlError::UnexpectedEnd {
            expected: "INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, or REINDEX",
        }),
        other => Err(SqlError::UnexpectedToken {
            expected: "INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, or REINDEX",
            got: format!("{other:?}"),
        }),
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn step_names(steps: &[Step]) -> Vec<&'static str> {
        steps
            .iter()
            .map(|s| match s {
                Step::One(_) => "One",
                Step::Many(_) => "Many",
                Step::Collection(_) => "Collection",
                Step::All => "All",
                Step::Forward(_) => "Forward",
                Step::Backward(_) => "Backward",
                Step::Hops(_) => "Hops",
                Step::HopsTyped { .. } => "HopsTyped",
                Step::MinStrength(_) => "MinStrength",
                Step::Leaves => "Leaves",
                Step::Roots => "Roots",
                Step::WhereEq(..) => "WhereEq",
                Step::WhereNeq(..) => "WhereNeq",
                Step::WhereGt(..) => "WhereGt",
                Step::WhereLt(..) => "WhereLt",
                Step::WhereGte(..) => "WhereGte",
                Step::WhereLte(..) => "WhereLte",
                Step::WhereBetween(..) => "WhereBetween",
                Step::WhereIn(..) => "WhereIn",
                Step::Like(..) => "Like",
                Step::StDWithin(..) => "StDWithin",
                Step::StContainsPoint(..) => "StContainsPoint",
                Step::StWithin(..) => "StWithin",
                Step::StContains(..) => "StContains",
                Step::StIntersects(..) => "StIntersects",
                Step::StDistance(..) => "StDistance",
                Step::StLength(..) => "StLength",
                Step::StArea(..) => "StArea",
                Step::VectorNear { .. } => "VectorNear",
                Step::Bm25Filter(..) => "Bm25Filter",
                Step::Bm25Sort(..) => "Bm25Sort",
                Step::Bm25Score(..) => "Bm25Score",
                Step::Intersect(_) => "Intersect",
                Step::Union(_) => "Union",
                Step::Subtract(_) => "Subtract",
                Step::WhereIsNull(..) => "WhereIsNull",
                Step::WhereNot(_) => "WhereNot",
                Step::WhereOr(_) => "WhereOr",
                Step::GroupBy(_) => "GroupBy",
                Step::Having(_) => "Having",
                Step::Distinct => "Distinct",
                Step::Sort(..) => "Sort",
                Step::SortByVector { .. } => "SortByVector",
                Step::SortByExpr { .. } => "SortByExpr",
                Step::Skip(_) => "Skip",
                Step::Take(_) => "Take",
                Step::Select(_) => "Select",
            })
            .collect()
    }

    #[test]
    fn parse_select_star_from_collection() {
        let steps = parse_and_compile("SELECT * FROM products").unwrap();
        assert_eq!(step_names(&steps), ["Collection"]);
    }

    #[test]
    fn parse_where_eq() {
        let steps = parse_and_compile("SELECT * FROM products WHERE category = 'cat3'").unwrap();
        assert_eq!(step_names(&steps), ["Collection", "WhereEq"]);
    }

    #[test]
    fn parse_where_neq() {
        let steps = parse_and_compile("SELECT * FROM products WHERE category != 'cat0'").unwrap();
        assert_eq!(step_names(&steps), ["Collection", "WhereNeq"]);
    }

    #[test]
    fn parse_where_neq_diamond_operator() {
        let steps = parse_and_compile("SELECT * FROM products WHERE status <> 'inactive'").unwrap();
        assert_eq!(step_names(&steps), ["Collection", "WhereNeq"]);
    }

    #[test]
    fn parse_range_and_order_limit() {
        let steps = parse_and_compile(
            "SELECT name, price FROM products WHERE price > 50 AND price <= 150 ORDER BY price ASC LIMIT 20"
        ).unwrap();
        assert_eq!(
            step_names(&steps),
            [
                "Collection",
                "WhereGt",
                "WhereLte",
                "Sort",
                "Take",
                "Select"
            ]
        );
    }

    #[test]
    fn parse_between() {
        let steps =
            parse_and_compile("SELECT * FROM items WHERE price BETWEEN 10 AND 100").unwrap();
        assert_eq!(step_names(&steps), ["Collection", "WhereBetween"]);
    }

    #[test]
    fn parse_in() {
        let steps =
            parse_and_compile("SELECT * FROM items WHERE status IN ('active', 'pending')").unwrap();
        assert_eq!(step_names(&steps), ["Collection", "WhereIn"]);
    }

    #[test]
    fn parse_like() {
        let steps = parse_and_compile("SELECT * FROM users WHERE name LIKE 'ali'").unwrap();
        assert_eq!(step_names(&steps), ["Collection", "Like"]);
    }

    #[test]
    fn parse_from_all() {
        let steps = parse_and_compile("SELECT * FROM ALL WHERE active = true").unwrap();
        assert_eq!(step_names(&steps), ["All", "WhereEq"]);
    }

    #[test]
    fn parse_offset_limit() {
        let steps = parse_and_compile("SELECT * FROM products LIMIT 10 OFFSET 5").unwrap();
        assert_eq!(step_names(&steps), ["Collection", "Skip", "Take"]);
    }

    #[test]
    fn parse_order_desc() {
        let steps = parse_and_compile("SELECT * FROM products ORDER BY price DESC").unwrap();
        assert_eq!(step_names(&steps), ["Collection", "Sort"]);
        if let Step::Sort(cols) = &steps[1] {
            assert_eq!(cols.len(), 1);
            assert!(!cols[0].1, "DESC means ascending=false");
        } else {
            panic!("expected Sort step");
        }
    }

    #[test]
    fn error_unexpected_token() {
        let err = parse_and_compile("INSERT INTO foo").unwrap_err();
        assert!(matches!(err, SqlError::UnexpectedToken { .. }));
    }

    #[test]
    fn error_unexpected_end() {
        let err = parse_and_compile("SELECT * FROM").unwrap_err();
        assert!(matches!(err, SqlError::UnexpectedEnd { .. }));
    }

    #[test]
    fn roundtrip_where_eq_bool() {
        let steps = parse_and_compile("SELECT * FROM products WHERE in_stock = true").unwrap();
        assert_eq!(step_names(&steps), ["Collection", "WhereEq"]);
        if let Step::WhereEq(_, v) = &steps[1] {
            assert_eq!(v, &serde_json::Value::Bool(true));
        } else {
            panic!();
        }
    }

    #[test]
    fn roundtrip_where_null() {
        let steps = parse_and_compile("SELECT * FROM items WHERE deleted_at = null").unwrap();
        if let Step::WhereEq(_, v) = &steps[1] {
            assert_eq!(v, &serde_json::Value::Null);
        } else {
            panic!();
        }
    }

    // ── INSERT / DELETE ───────────────────────────────────────────────────────

    #[test]
    fn parse_insert_basic() {
        let m = parse_mutation("INSERT INTO users (_key, name, age) VALUES ('alice', 'Alice', 30)")
            .unwrap();
        match m {
            CompiledMutation::Insert { slug, payload_json, .. } => {
                assert_eq!(slug, "users/alice");
                let v: serde_json::Value = serde_json::from_str(&payload_json).unwrap();
                assert_eq!(v["_collection"], "users");
                assert_eq!(v["_id"], "users/alice");
                assert_eq!(v["_key"], "alice");
                assert_eq!(v["name"], "Alice");
                assert_eq!(v["age"].as_f64(), Some(30.0));
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn parse_insert_with_bool_and_null() {
        let m =
            parse_mutation("INSERT INTO items (_key, active, deleted_at) VALUES ('x', true, null)")
                .unwrap();
        match m {
            CompiledMutation::Insert { payload_json, .. } => {
                let v: serde_json::Value = serde_json::from_str(&payload_json).unwrap();
                assert_eq!(v["active"], true);
                assert_eq!(v["deleted_at"], serde_json::Value::Null);
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn parse_delete_from_collection() {
        let m = parse_mutation("DELETE FROM users WHERE name = 'alice'").unwrap();
        match m {
            CompiledMutation::Delete(steps) => {
                assert_eq!(step_names(&steps), ["Collection", "WhereEq"])
            }
            _ => panic!("expected Delete"),
        }
    }

    #[test]
    fn parse_delete_from_all() {
        let m = parse_mutation("DELETE FROM ALL").unwrap();
        match m {
            CompiledMutation::Delete(steps) => assert_eq!(step_names(&steps), ["All"]),
            _ => panic!("expected Delete"),
        }
    }

    #[test]
    fn parse_delete_multi_condition() {
        let m =
            parse_mutation("DELETE FROM products WHERE price < 1 AND in_stock = false").unwrap();
        match m {
            CompiledMutation::Delete(steps) => {
                assert_eq!(step_names(&steps), ["Collection", "WhereLt", "WhereEq"])
            }
            _ => panic!("expected Delete"),
        }
    }

    #[test]
    fn insert_missing_key_errors() {
        let err = parse_mutation("INSERT INTO users (name, age) VALUES ('Alice', 30)").unwrap_err();
        assert!(matches!(err, SqlError::MissingField { field: "_key" }));
    }

    #[test]
    fn insert_field_value_mismatch_errors() {
        let err = parse_mutation("INSERT INTO users (slug, name) VALUES ('alice')").unwrap_err();
        assert!(matches!(
            err,
            SqlError::FieldValueCountMismatch {
                fields: 2,
                values: 1
            }
        ));
    }

    #[test]
    fn insert_non_string_key_errors() {
        let err =
            parse_mutation("INSERT INTO users (_key, name) VALUES (42, 'Alice')").unwrap_err();
        assert!(matches!(err, SqlError::InvalidValue(_)));
    }

    #[test]
    fn parse_mutation_rejects_select() {
        let err = parse_mutation("SELECT * FROM users").unwrap_err();
        assert!(matches!(err, SqlError::UnexpectedToken { .. }));
    }

    // ── MATCH unit tests ─────────────────────────────────────────────────────

    #[test]
    fn tokenize_match_forward() {
        let tokens = tokenize(
            "MATCH (a:artist)-[:has_genre]->(g:genre) WHERE a._key = 'the-vines' RETURN g",
        )
        .unwrap();
        // Should contain: Match LParen Ident Colon Ident RParen Dash LBracket Colon Ident RBracket Arrow ...
        assert!(tokens.iter().any(|t| matches!(t, Tok::Kw(Kw::Match))));
        assert!(tokens.iter().any(|t| matches!(t, Tok::LBracket)));
        assert!(tokens.iter().any(|t| matches!(t, Tok::RBracket)));
        assert!(tokens.iter().any(|t| matches!(t, Tok::Arrow)));
        assert!(tokens.iter().any(|t| matches!(t, Tok::Dot)));
    }

    #[test]
    fn tokenize_backarrow() {
        let tokens = tokenize("<-[:follows]-").unwrap();
        assert_eq!(tokens[0], Tok::BackArrow);
        assert_eq!(tokens[1], Tok::LBracket);
    }

    #[test]
    fn tokenize_dotdot() {
        let tokens = tokenize("*1..5").unwrap();
        assert_eq!(tokens[0], Tok::Star);
        assert_eq!(tokens[1], Tok::Num(1.0));
        assert_eq!(tokens[2], Tok::DotDot);
        assert_eq!(tokens[3], Tok::Num(5.0));
    }

    #[test]
    fn match_forward_with_key() {
        let steps = parse_and_compile(
            "MATCH (a:artist)-[:has_genre]->(g:genre) WHERE a._key = 'the-vines' RETURN g",
        )
        .unwrap();
        assert_eq!(step_names(&steps), ["One", "Forward", "Intersect"]);
    }

    #[test]
    fn match_backward_with_key() {
        let steps = parse_and_compile(
            "MATCH (g:genre)<-[:has_genre]-(a:artist) WHERE g._key = 'garage-rock' RETURN a",
        )
        .unwrap();
        // Backward from genre, then Intersect to filter by artist collection
        assert_eq!(step_names(&steps), ["One", "Backward", "Intersect"]);
    }

    #[test]
    fn match_typed_multihop() {
        let steps = parse_and_compile(
            "MATCH (e:event)-[:caused_by*1..5]->(root) WHERE e._key = 'maribyrnong-flood' RETURN root"
        ).unwrap();
        assert_eq!(step_names(&steps), ["One", "HopsTyped"]);
    }

    #[test]
    fn match_edge_strength_filter() {
        let steps = parse_and_compile(
            "MATCH (a:artist)-[r:has_genre]->(g:genre) WHERE a._key = 'the-vines' AND r.strength >= 7 RETURN g"
        ).unwrap();
        assert_eq!(
            step_names(&steps),
            ["One", "Forward", "MinStrength", "Intersect"]
        );
    }

    #[test]
    fn match_inline_props() {
        let steps = parse_and_compile(
            "MATCH (a:artist)-[:has_genre]->(:genre {_key: 'garage-rock'}) RETURN a",
        )
        .unwrap();
        // {_key: 'garage-rock'} inline prop → One() inside the Intersect (no outer WhereEq)
        assert_eq!(
            step_names(&steps),
            ["Collection", "Forward", "Intersect"]
        );
    }

    #[test]
    fn match_collection_scan_start() {
        let steps = parse_and_compile("MATCH (a:artist)-[:has_genre]->(g:genre) RETURN g").unwrap();
        assert_eq!(step_names(&steps), ["Collection", "Forward", "Intersect"]);
    }

    /// End-node _key in WHERE → One() inside Intersect (O(1) end-node lookup).
    #[test]
    fn match_end_node_key_becomes_one() {
        let steps = parse_and_compile(
            "MATCH (a:artist)-[:has_genre]->(g:genre) WHERE a._key = 'the-vines' AND g._key = 'garage-rock' RETURN g",
        ).unwrap();
        // Start: One (a._key), Forward, Intersect([One(g._key)])
        assert_eq!(step_names(&steps), ["One", "Forward", "Intersect"]);
        // Verify the sub-step inside Intersect is also a One
        if let Step::Intersect(sub) = &steps[2] {
            assert_eq!(step_names(sub), ["One"]);
        } else {
            panic!("expected Intersect");
        }
    }

    /// End-node WHERE filters move inside Intersect (enables btree_seed on Collection).
    #[test]
    fn match_end_node_filter_inside_intersect() {
        let steps = parse_and_compile(
            "MATCH (a:artist)-[:has_genre]->(g:genre) WHERE g.name = 'Garage Rock' RETURN g",
        ).unwrap();
        // Collection(artist), Forward, Intersect([Collection(genre), WhereEq(name)])
        assert_eq!(step_names(&steps), ["Collection", "Forward", "Intersect"]);
        if let Step::Intersect(sub) = &steps[2] {
            assert_eq!(step_names(sub), ["Collection", "WhereEq"]);
        } else {
            panic!("expected Intersect");
        }
    }

    /// End-node without a label but with plain filters: fall back to outer WhereEq.
    #[test]
    fn match_end_no_label_filter_stays_outer() {
        let steps = parse_and_compile(
            "MATCH (a:artist)-[:rel]->(b) WHERE a._key = 'x' AND b.price > 10 RETURN b",
        ).unwrap();
        // One, Forward — then WhereGt outside (no Collection to hang an Intersect on)
        assert_eq!(step_names(&steps), ["One", "Forward", "WhereGt"]);
    }

    #[test]
    fn match_union() {
        let steps = parse_and_compile(
            "MATCH (a:artist)-[:has_genre]->(g:genre) WHERE a._key = 'the-vines' RETURN g \
             UNION \
             MATCH (a:artist)-[:origin]->(c:city) WHERE a._key = 'the-vines' RETURN c",
        )
        .unwrap();
        assert!(step_names(&steps).contains(&"Union"));
    }

    #[test]
    fn parse_ilike() {
        let steps = parse_and_compile("SELECT * FROM artist WHERE name ILIKE 'vines'").unwrap();
        assert_eq!(step_names(&steps), ["Collection", "Like"]);
        if let Step::Like(_, _, case_insensitive) = &steps[1] {
            assert!(*case_insensitive, "ILIKE should set case_insensitive=true");
        } else {
            panic!("expected Like step");
        }
    }

    #[test]
    fn match_with_limit() {
        let steps = parse_and_compile(
            "MATCH (a:artist)-[:has_genre]->(g:genre) WHERE a._key = 'the-vines' RETURN g LIMIT 10",
        )
        .unwrap();
        assert_eq!(step_names(&steps), ["One", "Forward", "Intersect", "Take"]);
    }

    // ── Spatial SQL tests ────────────────────────────────────────────────────

    #[test]
    fn parse_st_dwithin() {
        // Spatial-first optimisation: grid starter + collection gate, no Collection step
        let steps = parse_and_compile(
            "SELECT * FROM places WHERE ST_DWithin(geometry, POINT(144.9631 -37.8136), 5.0)",
        )
        .unwrap();
        assert_eq!(step_names(&steps), ["StDWithin", "WhereEq"]);
    }

    #[test]
    fn parse_st_contains_point() {
        let steps = parse_and_compile(
            "SELECT * FROM zones WHERE ST_Contains(geometry, POINT(144.9631 -37.8136))",
        )
        .unwrap();
        assert_eq!(step_names(&steps), ["StContainsPoint", "WhereEq"]);
    }

    #[test]
    fn parse_st_within_polygon() {
        let steps = parse_and_compile(
            "SELECT * FROM places WHERE ST_Within(geometry, POLYGON((144.95 -37.80, 144.98 -37.80, 144.98 -37.83, 144.95 -37.83, 144.95 -37.80)))"
        ).unwrap();
        assert_eq!(step_names(&steps), ["StWithin", "WhereEq"]);
    }

    #[test]
    fn parse_st_intersects_polygon() {
        let steps = parse_and_compile(
            "SELECT * FROM routes WHERE ST_Intersects(geometry, POLYGON((144.95 -37.80, 144.98 -37.80, 144.98 -37.83, 144.95 -37.83, 144.95 -37.80)))"
        ).unwrap();
        assert_eq!(step_names(&steps), ["StIntersects", "WhereEq"]);
    }

    #[test]
    fn parse_st_contains_polygon() {
        let steps = parse_and_compile(
            "SELECT * FROM zones WHERE ST_Contains(geometry, POLYGON((144.96 -37.81, 144.97 -37.81, 144.97 -37.82, 144.96 -37.82, 144.96 -37.81)))"
        ).unwrap();
        assert_eq!(step_names(&steps), ["StContains", "WhereEq"]);
    }

    #[test]
    fn parse_st_dwithin_with_other_filter() {
        // Spatial step first (starter), then collection gate, then non-spatial filter
        let steps = parse_and_compile(
            "SELECT * FROM places WHERE ST_DWithin(geometry, POINT(144.9631 -37.8136), 5.0) AND category = 'landmark'"
        ).unwrap();
        assert_eq!(step_names(&steps), ["StDWithin", "WhereEq", "WhereEq"]);
    }

    #[test]
    fn parse_bm25_filter() {
        let steps =
            parse_and_compile("SELECT * FROM articles WHERE BM25(body, 'rust tutorial') > 0.5")
                .unwrap();
        assert_eq!(step_names(&steps), ["Collection", "Bm25Filter"]);
    }

    #[test]
    fn parse_bm25_order_by() {
        let steps =
            parse_and_compile("SELECT * FROM articles ORDER BY BM25(body, 'rust tutorial') DESC")
                .unwrap();
        // Simple BM25 now compiles to SortByExpr (arithmetic expression path).
        assert_eq!(step_names(&steps), ["Collection", "SortByExpr"]);
    }

    #[test]
    fn parse_bm25_order_by_asc() {
        let steps =
            parse_and_compile("SELECT * FROM articles ORDER BY BM25(body, 'rust tutorial') ASC")
                .unwrap();
        assert_eq!(step_names(&steps), ["Collection", "SortByExpr"]);
    }

    #[test]
    fn parse_bm25_select_score() {
        let steps =
            parse_and_compile("SELECT title, BM25(body, 'rust tutorial') FROM articles").unwrap();
        assert_eq!(step_names(&steps), ["Collection", "Bm25Score", "Select"]);
    }

    #[test]
    fn parse_bm25_filter_and_order() {
        let steps = parse_and_compile(
            "SELECT * FROM articles WHERE category = 'tech' ORDER BY BM25(body, 'rust') DESC LIMIT 10",
        )
        .unwrap();
        assert_eq!(
            step_names(&steps),
            ["Collection", "WhereEq", "SortByExpr", "Take"]
        );
    }

    #[test]
    fn insert_json_string_auto_parsed() {
        let m = parse_mutation(
            r#"INSERT INTO places (_key, geometry) VALUES ('p1', '{"type":"Point","coordinates":[144.96,-37.81]}')"#
        ).unwrap();
        match m {
            CompiledMutation::Insert { payload_json, .. } => {
                let v: Value = serde_json::from_str(&payload_json).unwrap();
                assert!(
                    v["geometry"].is_object(),
                    "geometry should be parsed object"
                );
                assert_eq!(v["geometry"]["type"], "Point");
            }
            other => panic!("expected Insert, got {other:?}"),
        }
    }

    // ── INSERT edge parse tests ─────────────────────────────────────────────

    #[test]
    fn insert_edge_simple() {
        let m = parse_mutation("INSERT ('a')-[:KNOWS]->('b')").unwrap();
        match m {
            CompiledMutation::InsertEdge(edges) => {
                assert_eq!(edges.len(), 1);
                assert_eq!(edges[0].from, "a");
                assert_eq!(edges[0].to, "b");
                assert_eq!(edges[0].edge_type, "KNOWS");
                assert_eq!(edges[0].strength, 1.0);
                assert!(edges[0].props_json.is_none());
            }
            other => panic!("expected InsertEdge, got {other:?}"),
        }
    }

    #[test]
    fn insert_edge_with_strength() {
        let m = parse_mutation("INSERT ('a')-[:KNOWS {strength: 10}]->('b')").unwrap();
        match m {
            CompiledMutation::InsertEdge(edges) => {
                assert_eq!(edges[0].strength, 10.0);
                assert!(
                    edges[0].props_json.is_none(),
                    "strength should be extracted, not in props"
                );
            }
            other => panic!("expected InsertEdge, got {other:?}"),
        }
    }

    #[test]
    fn insert_edge_with_props() {
        let m = parse_mutation("INSERT ('a')-[:KNOWS {strength: 5, since: 2024}]->('b')").unwrap();
        match m {
            CompiledMutation::InsertEdge(edges) => {
                assert_eq!(edges[0].strength, 5.0);
                let props: Value =
                    serde_json::from_str(edges[0].props_json.as_ref().unwrap()).unwrap();
                assert_eq!(props["since"], 2024.0);
                assert!(
                    props.get("strength").is_none(),
                    "strength should not be in props"
                );
            }
            other => panic!("expected InsertEdge, got {other:?}"),
        }
    }

    #[test]
    fn insert_edge_multiple() {
        let m = parse_mutation("INSERT ('a')-[:X]->('b'), ('b')-[:Y]->('c')").unwrap();
        match m {
            CompiledMutation::InsertEdge(edges) => {
                assert_eq!(edges.len(), 2);
                assert_eq!(edges[0].from, "a");
                assert_eq!(edges[0].edge_type, "X");
                assert_eq!(edges[0].to, "b");
                assert_eq!(edges[1].from, "b");
                assert_eq!(edges[1].edge_type, "Y");
                assert_eq!(edges[1].to, "c");
            }
            other => panic!("expected InsertEdge, got {other:?}"),
        }
    }

    #[test]
    fn insert_edge_default_strength() {
        let m = parse_mutation("INSERT ('x')-[:links]->('y')").unwrap();
        match m {
            CompiledMutation::InsertEdge(edges) => {
                assert_eq!(edges[0].strength, 1.0);
            }
            other => panic!("expected InsertEdge, got {other:?}"),
        }
    }

    // ── DELETE edge parse tests ─────────────────────────────────────────────

    #[test]
    fn delete_edge_parses() {
        let m = parse_mutation("DELETE ('a')-[:KNOWS]->('b')").unwrap();
        match m {
            CompiledMutation::DeleteEdge(edges) => {
                assert_eq!(edges.len(), 1);
                assert_eq!(edges[0].from, "a");
                assert_eq!(edges[0].to, "b");
                assert_eq!(edges[0].edge_type, "KNOWS");
            }
            other => panic!("expected DeleteEdge, got {other:?}"),
        }
    }
}

#[cfg(test)]
mod debug_tests {
    use super::*;

    #[test]
    fn debug_tokenize_match_insert() {
        let sql = "MATCH (p:people) WHERE p.grade < 80 INSERT (p)-[:member_of]->(classroom/A)";
        let tokens = tokenize(sql).unwrap();
        assert!(!tokens.is_empty());

        let result = parse_mutation(sql).expect("MATCH INSERT should parse successfully");
        match result {
            CompiledMutation::MatchInsert {
                match_steps,
                target,
                edge_type,
                strength,
                props,
            } => {
                assert_eq!(target, "classroom/A");
                assert_eq!(edge_type, "member_of");
                assert!((strength - 1.0).abs() < f32::EPSILON);
                assert!(props.is_none());
                // Should have Collection step + WhereLt step
                assert_eq!(match_steps.len(), 2);
            }
            other => panic!("expected MatchInsert, got {other:?}"),
        }
    }

    #[test]
    fn match_insert_with_edge_props() {
        let sql = r#"MATCH (p:people) WHERE p.grade < 80 INSERT (p)-[:member_of {strength: 0.8, semester: "fall"}]->(classroom/A)"#;
        let result = parse_mutation(sql).expect("MATCH INSERT with edge props should parse");
        match result {
            CompiledMutation::MatchInsert {
                target,
                edge_type,
                strength,
                props,
                ..
            } => {
                assert_eq!(target, "classroom/A");
                assert_eq!(edge_type, "member_of");
                assert!((strength - 0.8).abs() < f32::EPSILON);
                assert!(props.is_some());
                let p: serde_json::Value = serde_json::from_str(props.as_ref().unwrap()).unwrap();
                assert_eq!(p["semester"], "fall");
            }
            other => panic!("expected MatchInsert, got {other:?}"),
        }
    }

    #[test]
    fn match_insert_without_where() {
        let sql = "MATCH (p:people) INSERT (p)-[:member_of]->(classroom/A)";
        let result = parse_mutation(sql).expect("MATCH INSERT without WHERE should parse");
        match result {
            CompiledMutation::MatchInsert {
                match_steps,
                target,
                edge_type,
                ..
            } => {
                assert_eq!(target, "classroom/A");
                assert_eq!(edge_type, "member_of");
                // Only Collection step, no WHERE
                assert_eq!(match_steps.len(), 1);
            }
            other => panic!("expected MatchInsert, got {other:?}"),
        }
    }

    #[test]
    fn match_insert_with_label_target() {
        let sql = "MATCH (p:people) INSERT (p)-[:member_of]->(:group {_key: 'alpha'})";
        let result = parse_mutation(sql).expect("MATCH INSERT with label target should parse");
        match result {
            CompiledMutation::MatchInsert {
                target, edge_type, ..
            } => {
                assert_eq!(target, "group/alpha");
                assert_eq!(edge_type, "member_of");
            }
            other => panic!("expected MatchInsert, got {other:?}"),
        }
    }

    #[test]
    fn match_insert_multiple_where() {
        let sql = "MATCH (p:people) WHERE p.grade >= 50 AND p.grade < 80 INSERT (p)-[:member_of]->(classroom/A)";
        let result = parse_mutation(sql).expect("MATCH INSERT with multiple WHERE should parse");
        match result {
            CompiledMutation::MatchInsert {
                match_steps,
                target,
                ..
            } => {
                assert_eq!(target, "classroom/A");
                // Collection + WhereGte + WhereLt
                assert_eq!(match_steps.len(), 3);
            }
            other => panic!("expected MatchInsert, got {other:?}"),
        }
    }
}

// ── SHOW ──────────────────────────────────────────────────────────────────────

/// Parsed form of a `SHOW EDGES` clause.
pub struct ShowEdgesStmt {
    /// `SHOW EDGES FROM <collection>` — filter source collection.
    pub from_col: Option<String>,
    /// `SHOW EDGES FROM x TO <collection>` — also filter target collection.
    pub to_col: Option<String>,
}

/// Parsed form of any `SHOW …` statement.
pub enum ShowStmt {
    /// `SHOW TABLES` — all collections with row counts.
    Tables,
    /// `SHOW EDGES [FROM col] [TO col]` — graph schema with edge counts.
    Edges(ShowEdgesStmt),
    /// `SHOW <collection>` — field structure for one collection.
    Collection(String),
    /// `SHOW CREATE TABLE <collection>` — DDL that would recreate this collection.
    CreateTable(String),
}

/// Parse any `SHOW` statement:
///
/// ```text
/// SHOW TABLES
/// SHOW EDGES [FROM collection] [TO collection]
/// SHOW CREATE TABLE <collection>
/// SHOW <collection_name>
/// ```
pub fn parse_show(sql: &str) -> Result<ShowStmt, SqlError> {
    let tokens = tokenize(sql)?;
    let mut p = Parser::new(tokens);

    p.expect_kw(Kw::Show, "SHOW")?;

    match p.peek().clone() {
        // SHOW TABLES (TABLE alone = list of tables)
        Tok::Ident(s) if s.to_ascii_uppercase() == "TABLES" => {
            p.advance();
            Ok(ShowStmt::Tables)
        }
        // SHOW CREATE TABLE <collection>  — TABLE keyword triggers the branch
        Tok::Kw(Kw::Table) => {
            p.advance();
            // If next token is a collection name it's a bare SHOW TABLE (= SHOW TABLES)
            // If preceded by CREATE it's SHOW CREATE TABLE — but CREATE is consumed before
            // we get here, so we check: is the next token an ident?
            match p.peek().clone() {
                Tok::Ident(_) => {
                    let name = p.expect_ident()?;
                    Ok(ShowStmt::CreateTable(name))
                }
                _ => Ok(ShowStmt::Tables),
            }
        }
        // SHOW CREATE TABLE <collection>
        Tok::Kw(Kw::Create) => {
            p.advance();
            // expect TABLE
            match p.peek().clone() {
                Tok::Kw(Kw::Table) => { p.advance(); }
                other => return Err(SqlError::UnexpectedToken {
                    expected: "TABLE",
                    got: format!("{other:?}"),
                }),
            }
            let name = p.expect_ident()?;
            Ok(ShowStmt::CreateTable(name))
        }
        // SHOW EDGES [FROM col] [TO col]
        Tok::Ident(s) if s.to_ascii_uppercase() == "EDGES" => {
            p.advance();

            let from_col = if matches!(p.peek(), Tok::Kw(Kw::From)) {
                p.advance();
                Some(p.expect_ident()?)
            } else {
                None
            };

            let to_col = match p.peek().clone() {
                Tok::Kw(Kw::To) => {
                    p.advance();
                    Some(p.expect_ident()?)
                }
                _ => None,
            };

            Ok(ShowStmt::Edges(ShowEdgesStmt { from_col, to_col }))
        }
        // SHOW <collection> — field structure
        Tok::Ident(_) => {
            let name = p.expect_ident()?;
            Ok(ShowStmt::Collection(name))
        }
        other => Err(SqlError::UnexpectedToken {
            expected: "TABLES, CREATE TABLE, EDGES, or a collection name",
            got: format!("{other:?}"),
        }),
    }
}

