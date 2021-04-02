use std::hash::Hash;

pub trait ColumnType: Hash {
    fn name() -> &'static str;
}
