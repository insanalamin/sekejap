#[derive(Clone, Debug)]
pub enum SqlStatement {
    Select(SelectStatement),
    CreateCollection(CreateCollectionStatement),
    Insert(InsertStatement),
    Relate(RelateStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    Unrelate(UnrelateStatement),
}

#[derive(Clone, Debug)]
pub struct SelectStatement {
    pub projection: Vec<SelectItem>,
    pub from: TableRef,
    pub traverse: Option<TraverseClause>,
    pub selection: Option<Expr>,
    pub order_by: Vec<OrderByItem>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub raw: String,
}

#[derive(Clone, Debug)]
pub enum SelectItem {
    Wildcard,
    Field(String),
    FunctionCall { name: String, args: Vec<Expr> },
}

#[derive(Clone, Debug)]
pub struct TableRef {
    pub name: String,
    pub alias: Option<String>,
}

#[derive(Clone, Debug)]
pub struct TraverseClause {
    pub direction: TraverseDirection,
    pub edge_type: String,
    pub target: TableRef,
    pub hops: Option<u32>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TraverseDirection {
    Forward,
    Backward,
}

#[derive(Clone, Debug)]
pub struct OrderByItem {
    pub field: String,
    pub ascending: bool,
}

#[derive(Clone, Debug)]
pub struct CreateCollectionStatement {
    pub name: String,
    pub fields: Vec<ColumnDef>,
    pub options: CollectionOptions,
    pub raw: String,
}

#[derive(Clone, Debug)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: SqlType,
    pub primary_key: bool,
    pub default: Option<DefaultExpr>,
}

#[derive(Clone, Debug, Default)]
pub struct CollectionOptions {
    pub hash_index: Vec<String>,
    pub range_index: Vec<String>,
    pub temporal_index: Vec<String>,
    pub spatial_index: Vec<String>,
    pub vector_index: Vec<String>,
    pub fulltext_index: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct InsertStatement {
    pub table: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Expr>>,
    pub raw: String,
}

#[derive(Clone, Debug)]
pub struct RelateStatement {
    pub edges: Vec<RelateEdge>,
    pub raw: String,
}

#[derive(Clone, Debug)]
pub struct UpdateStatement {
    pub table: String,
    pub assignments: Vec<(String, Expr)>,
    pub selection: Option<Expr>,
    pub raw: String,
}

#[derive(Clone, Debug)]
pub struct DeleteStatement {
    pub table: String,
    pub selection: Option<Expr>,
    pub raw: String,
}

#[derive(Clone, Debug)]
pub struct UnrelateStatement {
    pub source: String,
    pub edge_type: String,
    pub target: String,
    pub raw: String,
}

#[derive(Clone, Debug)]
pub struct RelateEdge {
    pub source: String,
    pub edge_type: String,
    pub target: String,
    pub weight: f32,
    pub meta_json: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SqlType {
    Text,
    Int,
    Float,
    Bool,
    Json,
    Timestamp,
    VagueTime,
    Geometry,
    Uuid,
    Vector(usize),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DefaultExpr {
    UuidV4,
}

#[derive(Clone, Debug)]
pub enum Expr {
    Identifier(String),
    StringLiteral(String),
    TimestampLiteral(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Null,
    ArrayLiteral(Vec<Expr>),
    FunctionCall { name: String, args: Vec<Expr> },
    Raw(String),
}
