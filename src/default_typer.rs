use crate::raw_parser::{ColumnValue, RawValue, ValueParser};
use crate::typer::Typer;
use derive_more::Display;

/// Fully typed value
#[derive(Clone, PartialEq, Debug)]
pub enum Value {
    Boolean(bool),
    Int(i64),
    Float(f64),
    Text(String),
}

/// Tag of typed values
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Display)]
pub enum ValueType {
    Boolean,
    Int,
    Float,
    Text,
}

/// Column of typed values
#[derive(Clone, Debug, PartialEq)]
pub enum Column {
    Boolean(Vec<ColumnValue<bool>>),
    Int(Vec<ColumnValue<i64>>),
    Float(Vec<ColumnValue<f64>>),
    Text(Vec<ColumnValue<String>>),
}

/// Default typing scheme
#[derive(Default, Debug)]
pub struct DefaultTyper;

impl DefaultTyper {
    fn as_int(&self, value: &RawValue) -> ColumnValue<Value> {
        value.parse_csv().map(Value::Int)
    }

    fn as_float(&self, value: &RawValue) -> ColumnValue<Value> {
        value.parse_csv().map(Value::Float)
    }

    fn as_bool(&self, value: &RawValue) -> ColumnValue<Value> {
        value.parse_csv().map(Value::Boolean)
    }

    fn as_text(&self, value: &RawValue) -> Value {
        Value::Text(value.0.to_string())
    }

    fn as_bool_column(&self, values: Vec<ColumnValue<Value>>) -> Column {
        let vs = values
            .into_iter()
            .map(|v| match v {
                ColumnValue::Some(Value::Boolean(x)) => ColumnValue::Some(x),
                ColumnValue::Invalid => ColumnValue::Invalid,
                ColumnValue::Missing => ColumnValue::Missing,
                _ => ColumnValue::Invalid,
            })
            .collect();
        Column::Boolean(vs)
    }

    fn as_int_column(&self, values: Vec<ColumnValue<Value>>) -> Column {
        let vs = values
            .into_iter()
            .map(|v| match v {
                ColumnValue::Some(Value::Int(x)) => ColumnValue::Some(x),
                ColumnValue::Invalid => ColumnValue::Invalid,
                ColumnValue::Missing => ColumnValue::Missing,
                _ => ColumnValue::Invalid,
            })
            .collect();
        Column::Int(vs)
    }

    fn as_float_column(&self, values: Vec<ColumnValue<Value>>) -> Column {
        let vs = values
            .into_iter()
            .map(|v| match v {
                ColumnValue::Some(Value::Float(x)) => ColumnValue::Some(x),
                ColumnValue::Invalid => ColumnValue::Invalid,
                ColumnValue::Missing => ColumnValue::Missing,
                _ => ColumnValue::Invalid,
            })
            .collect();
        Column::Float(vs)
    }

    fn as_text_column(&self, values: Vec<ColumnValue<Value>>) -> Column {
        let vs = values
            .into_iter()
            .map(|v| match v {
                ColumnValue::Some(Value::Text(x)) => ColumnValue::Some(x),
                ColumnValue::Invalid => ColumnValue::Invalid,
                ColumnValue::Missing => ColumnValue::Missing,
                _ => ColumnValue::Invalid,
            })
            .collect();
        Column::Text(vs)
    }
}

impl Typer for DefaultTyper {
    type TypeTag = ValueType;
    type TypedValue = Value;
    type TypedColumn = Column;

    const TYPES: &'static [Self::TypeTag] = &[
        ValueType::Boolean,
        ValueType::Int,
        ValueType::Float,
        ValueType::Text,
    ];

    fn type_value_as(&self, value: &RawValue, tag: Self::TypeTag) -> ColumnValue<Self::TypedValue> {
        match tag {
            ValueType::Boolean => self.as_bool(value),
            ValueType::Int => self.as_int(value),
            ValueType::Float => self.as_float(value),
            ValueType::Text => ColumnValue::Some(self.as_text(value)),
        }
    }

    fn tag_type(&self, value: &Self::TypedValue) -> ValueType {
        match value {
            Value::Boolean(_) => ValueType::Boolean,
            Value::Int(_) => ValueType::Int,
            Value::Float(_) => ValueType::Float,
            Value::Text(_) => ValueType::Text,
        }
    }

    fn type_column(
        &self,
        tag: Self::TypeTag,
        values: Vec<ColumnValue<Self::TypedValue>>,
    ) -> Self::TypedColumn {
        match tag {
            ValueType::Boolean => self.as_bool_column(values),
            ValueType::Int => self.as_int_column(values),
            ValueType::Float => self.as_float_column(values),
            ValueType::Text => self.as_text_column(values),
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
                DefaultTyper.type_value(&raw.into()),
                ColumnValue::Some(Value::Boolean(expected)),
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
                DefaultTyper.type_value(&raw.into()),
                ColumnValue::Some(Value::Int(expected)),
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
            let determined = DefaultTyper.type_value(&raw.into());
            let is_equal = if let ColumnValue::Some(Value::Float(parsed)) = determined {
                parsed.is_nan() && expected.is_nan()
                    || parsed.partial_cmp(&expected) == Some(Ordering::Equal)
            } else {
                false
            };
            assert!(is_equal, "{} != {}", raw, expected);
        }
    }
}
