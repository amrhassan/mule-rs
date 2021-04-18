use crate::typer::{DatasetValue, Typer};
use crate::value_parsing::{Parsed, RawValue};
use derive_more::Display;

/// Fully typed value
#[derive(Clone, PartialEq, Debug)]
pub enum Value {
    Boolean(bool),
    Int(i64),
    Float(f64),
    Text(String),
}

impl DatasetValue<ColumnType> for Value {
    fn get_column_type(&self) -> ColumnType {
        match self {
            Value::Boolean(_) => ColumnType::Boolean,
            Value::Int(_) => ColumnType::Int,
            Value::Float(_) => ColumnType::Float,
            Value::Text(_) => ColumnType::Text,
        }
    }
}

/// Tag of typed values
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Display)]
pub enum ColumnType {
    Boolean,
    Int,
    Float,
    Text,
    Unknown,
}

impl Default for ColumnType {
    fn default() -> Self {
        ColumnType::Unknown
    }
}

/// Column of typed values
#[derive(Clone, Debug, PartialEq)]
pub enum Column {
    Boolean(Vec<Parsed<bool>>),
    Int(Vec<Parsed<i64>>),
    Float(Vec<Parsed<f64>>),
    Text(Vec<Parsed<String>>),
    Unknown,
}

impl Default for Column {
    fn default() -> Self {
        Column::Unknown
    }
}

/// Default typing scheme
#[derive(Default, Debug, PartialEq, Eq)]
pub struct DefaultTyper;

impl DefaultTyper {
    fn as_int(&self, value: &RawValue) -> Parsed<Value> {
        value.parse_i64().map(Value::Int)
    }

    fn as_float(&self, value: &RawValue) -> Parsed<Value> {
        value.parse_f64().map(Value::Float)
    }

    fn as_bool(&self, value: &RawValue) -> Parsed<Value> {
        value.parse_bool().map(Value::Boolean)
    }

    fn as_text(&self, value: &RawValue) -> Value {
        Value::Text(value.0.to_string())
    }

    fn as_bool_column(&self, values: Vec<Parsed<Value>>) -> Column {
        let vs = values
            .into_iter()
            .map(|v| match v {
                Parsed::Some(Value::Boolean(x)) => Parsed::Some(x),
                Parsed::Invalid => Parsed::Invalid,
                Parsed::Missing => Parsed::Missing,
                _ => Parsed::Invalid,
            })
            .collect();
        Column::Boolean(vs)
    }

    fn as_int_column(&self, values: Vec<Parsed<Value>>) -> Column {
        let vs = values
            .into_iter()
            .map(|v| match v {
                Parsed::Some(Value::Int(x)) => Parsed::Some(x),
                Parsed::Invalid => Parsed::Invalid,
                Parsed::Missing => Parsed::Missing,
                _ => Parsed::Invalid,
            })
            .collect();
        Column::Int(vs)
    }

    fn as_float_column(&self, values: Vec<Parsed<Value>>) -> Column {
        let vs = values
            .into_iter()
            .map(|v| match v {
                Parsed::Some(Value::Float(x)) => Parsed::Some(x),
                Parsed::Invalid => Parsed::Invalid,
                Parsed::Missing => Parsed::Missing,
                _ => Parsed::Invalid,
            })
            .collect();
        Column::Float(vs)
    }

    fn as_text_column(&self, values: Vec<Parsed<Value>>) -> Column {
        let vs = values
            .into_iter()
            .map(|v| match v {
                Parsed::Some(Value::Text(x)) => Parsed::Some(x),
                Parsed::Invalid => Parsed::Invalid,
                Parsed::Missing => Parsed::Missing,
                _ => Parsed::Invalid,
            })
            .collect();
        Column::Text(vs)
    }
}

impl Typer for DefaultTyper {
    type ColumnType = ColumnType;
    type DatasetValue = Value;
    type Column = Column;

    const COLUMN_TYPES: &'static [Self::ColumnType] = &[
        ColumnType::Boolean,
        ColumnType::Int,
        ColumnType::Float,
        ColumnType::Text,
    ];

    fn parse_as(&self, value: &RawValue, tag: Self::ColumnType) -> Parsed<Self::DatasetValue> {
        match tag {
            ColumnType::Boolean => self.as_bool(value),
            ColumnType::Int => self.as_int(value),
            ColumnType::Float => self.as_float(value),
            ColumnType::Text => Parsed::Some(self.as_text(value)),
            ColumnType::Unknown => Parsed::Invalid,
        }
    }

    fn parse_column(
        &self,
        tag: Self::ColumnType,
        values: Vec<Parsed<Self::DatasetValue>>,
    ) -> Self::Column {
        match tag {
            ColumnType::Boolean => self.as_bool_column(values),
            ColumnType::Int => self.as_int_column(values),
            ColumnType::Float => self.as_float_column(values),
            ColumnType::Text => self.as_text_column(values),
            ColumnType::Unknown => Self::Column::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use core::f64;
    use std::cmp::Ordering;

    use super::*;

    #[test]
    fn test_typing_bool() {
        let values = vec![
            ("true", true),
            (" false", false),
            ("   TRUE ", true),
            ("FALSE", false),
            (" 1", true),
            ("0", false),
            ("t", true),
            ("T", true),
            ("f", false),
            ("F", false),
        ];
        for (raw, expected) in values {
            assert_eq!(
                DefaultTyper.parse(&raw.into()),
                Parsed::Some(Value::Boolean(expected)),
                "{} failed the test",
                raw
            );
        }
    }

    #[test]
    fn test_typing_int() {
        let values = vec![("4", 4), ("8", 8), ("-15", -15), ("23", 23), ("  42", 42)];
        for (raw, expected) in values {
            assert_eq!(
                DefaultTyper.parse(&raw.into()),
                Parsed::Some(Value::Int(expected)),
                "{} failed the test",
                raw
            );
        }
    }

    #[test]
    fn test_typing_float() {
        let values = vec![
            ("4.", 4.0),
            ("8.12342", 8.12342),
            ("-15.234", -15.234),
            (".23", 0.23),
            ("  42e13", 42e13),
            ("NAN", f64::NAN),
            ("nan", f64::NAN),
            ("INF", f64::INFINITY),
            ("inf", f64::INFINITY),
            ("-inf", f64::NEG_INFINITY),
            ("-INF", f64::NEG_INFINITY),
        ];
        for (raw, expected) in values {
            let determined = DefaultTyper.parse(&raw.into());
            let is_equal = if let Parsed::Some(Value::Float(parsed)) = determined {
                parsed.is_nan() && expected.is_nan()
                    || parsed.partial_cmp(&expected) == Some(Ordering::Equal)
            } else {
                false
            };
            assert!(is_equal, "{} != {}", raw, expected);
        }
    }
}
