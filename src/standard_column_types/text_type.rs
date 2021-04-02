use crate::column_types::ColumnType;
use std::hash::Hash;

#[derive(Hash)]
struct TextType;

impl ColumnType for TextType {
    fn name() -> &'static str {
        "Text"
    }
}
