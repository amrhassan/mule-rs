use crate::value_parsing::{Parsed, RawValue};
use std::fmt::{Debug, Display};
use std::hash::Hash;

pub trait Typer: Default + Clone + 'static {
    type ColumnType: Display + Hash + Eq + Copy + Debug + Send + Default;
    type DatasetValue: DatasetValue<Self::ColumnType>;

    const COLUMN_TYPES: &'static [Self::ColumnType];

    fn parse_as(&self, value: &RawValue, tag: Self::ColumnType) -> Parsed<Self::DatasetValue>;

    fn parse(&self, value: &RawValue) -> Parsed<Self::DatasetValue> {
        Self::COLUMN_TYPES
            .iter()
            .map(|tag| self.parse_as(value, *tag))
            .find(|v| v.is_some())
            .unwrap_or(Parsed::Invalid)
    }
}

pub trait DatasetValue<C>: Debug + Clone + PartialEq {
    fn get_column_type(&self) -> C;
}
