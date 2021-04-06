use crate::raw_parser::{ColumnValue, RawValue, ValueParser};
use crate::typer::Typer;
use derive_more::Display;

/// A fully typed value
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
}

impl Typer for DefaultTyper {
    type TypeTag = ValueType;
    type TypedValue = Value;

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
