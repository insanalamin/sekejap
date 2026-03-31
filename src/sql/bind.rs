use serde_json::Value;

#[derive(Clone, Debug)]
pub enum SqlBind {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Json(Value),
}

#[derive(Clone, Debug, Default)]
pub struct SqlBindSet {
    pub values: Vec<SqlBind>,
}

impl SqlBindSet {
    pub fn new(values: Vec<SqlBind>) -> Self {
        Self { values }
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}
