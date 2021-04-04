use super::raw_parser::{ColumnValue, RawValue};
use std::fmt::{Debug, Display};
use std::hash::Hash;

/// Infer the type of a raw value
pub trait Typer: Default {
    /// Uniquely-identifying tag type for typed values
    type TypeTag: Display + Hash + Eq + Copy;

    /// The type of a fully-typed single value
    type Output: TypedValue<Self::TypeTag>;

    /// Parse a raw value into a specialized typed value
    ///
    /// This method should never fail. Your type should have a fallback variant
    /// to be used when no appropriate concrete was detected (like a Text variant).
    fn type_value(&self, value: &RawValue) -> Self::Output;

    /// Parse a raw value into a specific type. This should fail by parsing into `ColumnValue::Invalid`
    /// when the specified type tag could not be used to parse the raw value.
    fn type_value_as(&self, value: &RawValue, tag: Self::TypeTag) -> ColumnValue<Self::Output>;
}

/// A value of a concrete type
pub trait TypedValue<T>: Clone + Debug {
    /// Determine a tag value that identifies the type of the value
    fn tag(&self) -> T;
}
