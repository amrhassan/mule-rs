use super::raw_parser::RawValue;
use std::hash::Hash;

/// Infer the type of a raw value
pub trait Typer {
    type TypedValue;
    type TypeTag: Hash + Eq;

    /// Parse a raw value into a specialized typed value
    fn type_raw_value(&self, value: &RawValue) -> Self::TypedValue;

    fn tag_typed_value(&self, typed_value: &Self::TypedValue) -> Self::TypeTag;
}
