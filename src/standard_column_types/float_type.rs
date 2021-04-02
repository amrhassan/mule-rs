use crate::column_types::ColumnType;
use std::hash::Hash;

#[derive(Hash)]
struct FloatType;

impl ColumnType for FloatType {
    fn name() -> &'static str {
        "Float"
    }
}
