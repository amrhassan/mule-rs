use super::raw_parser::{ColumnValue, RawValue};
use std::fmt::{Debug, Display};
use std::hash::Hash;

/// Infers the type of a raw value
pub trait Typer: Default + 'static {
    /// Uniquely-identifying tag type for typed values
    type TypeTag: Display + Hash + Eq + Copy;

    /// The type of a fully-typed single value
    type TypedValue: Debug + Clone;

    type TypedColumn;

    /// The tags of supported types ordered by parsing priority. The earlier type tags will be attempted first.
    const TYPES: &'static [Self::TypeTag];

    /// Parse a raw value into a specific type.
    fn type_value_as(&self, value: &RawValue, tag: Self::TypeTag) -> ColumnValue<Self::TypedValue>;

    /// Determine a tag value that identifies the type of the value
    fn tag_type(&self, value: &Self::TypedValue) -> Self::TypeTag;

    fn type_value(&self, value: &RawValue) -> ColumnValue<Self::TypedValue> {
        Self::TYPES
            .iter()
            .map(|tag| self.type_value_as(value, *tag))
            .find(|v| v.is_some())
            .unwrap_or(ColumnValue::Invalid)
    }

    fn type_column(
        &self,
        tag: Self::TypeTag,
        values: Vec<ColumnValue<Self::TypedValue>>,
    ) -> Self::TypedColumn;
}
