use super::raw_parser::RawValue;
use std::hash::Hash;

#[derive(Clone, Copy, Hash, PartialEq, Eq, Debug)]
pub enum ColumnValue<A> {
    Invalid,
    Missing,
    Some(A),
}

/// Infer the type of a raw value
pub trait Typer {
    type TypedValue: Clone;
    type TypeTag: Hash + Eq + Copy;

    fn new() -> Self;

    /// Parse a raw value into a specialized typed value or None if missing
    fn type_raw_value(&self, value: &RawValue) -> Option<Self::TypedValue>;

    fn tag_typed_value(&self, typed_value: &Self::TypedValue) -> Self::TypeTag;

    /// Parse a raw value into a specific type
    fn type_raw_value_as(
        &self,
        value: &RawValue,
        tag: Self::TypeTag,
    ) -> ColumnValue<Self::TypedValue>;
}
