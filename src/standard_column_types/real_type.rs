use crate::column_types::ColumnType;
use std::hash::Hash;

#[derive(Hash)]
struct RealType;

impl ColumnType for RealType {
    fn name() -> &'static str {
        "Real"
    }
}
