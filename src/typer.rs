use crate::value_parsing::{Parsed, RawValue};
use std::fmt::{Debug, Display};
use std::hash::Hash;

pub trait Typer: Default + Clone + 'static {
    type ColumnType: Display + Hash + Eq + Copy + Debug + Default;
    type DatasetValue: DatasetValue<Self::ColumnType>;
    type Column: Default;

    const COLUMN_TYPES: &'static [Self::ColumnType];

    fn parse_as(&self, value: &RawValue, tag: Self::ColumnType) -> Parsed<Self::DatasetValue>;

    fn determine_type(&self, value: &RawValue) -> Option<Self::ColumnType> {
        Self::COLUMN_TYPES
            .iter()
            .copied()
            .flat_map(|column_type| self.parse_as(value, column_type).get())
            .map(|v| v.get_column_type())
            .next()
    }

    fn parse(&self, value: &RawValue) -> Parsed<Self::DatasetValue> {
        Self::COLUMN_TYPES
            .iter()
            .map(|tag| self.parse_as(value, *tag))
            .find(|v| v.is_some())
            .unwrap_or(Parsed::Invalid)
    }

    fn parse_column(
        &self,
        column_type: Self::ColumnType,
        values: Vec<Parsed<Self::DatasetValue>>,
    ) -> Self::Column;
}

pub trait DatasetValue<C>: Debug + Clone + PartialEq {
    fn get_column_type(&self) -> C;
}
