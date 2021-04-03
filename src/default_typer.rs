use super::raw_parser::RawValue;
use super::typer::Typer;
use super::values::{Value, ValueType};

#[derive(Default)]
pub struct DefaultTyper;

impl DefaultTyper {
    fn as_int(&self, value: &RawValue) -> Option<Value> {
        Some(Value::Int(value.0.trim().parse().ok()?))
    }

    fn as_float(&self, value: &RawValue) -> Option<Value> {
        let s = value.0.trim().to_lowercase();
        if s == "nan" {
            Some(Value::Float(f64::NAN))
        } else {
            Some(Value::Float(s.parse().ok()?))
        }
    }

    fn as_bool(&self, value: &RawValue) -> Option<Value> {
        let s = value.0.trim().to_lowercase();
        if s == "1" || s == "t" {
            Some(Value::Boolean(true))
        } else if s == "0" || s == "f" {
            Some(Value::Boolean(false))
        } else {
            Some(Value::Boolean(s.parse().ok()?))
        }
    }

    fn as_text(&self, value: &RawValue) -> Value {
        Value::Text(value.0.clone())
    }
}

impl Typer for DefaultTyper {
    type TypedValue = Value;
    type TypeTag = ValueType;

    fn type_raw_value(&self, value: &RawValue) -> Self::TypedValue {
        self.as_bool(value)
            .or_else(|| self.as_int(value))
            .or_else(|| self.as_float(value))
            .unwrap_or_else(|| self.as_text(value))
    }

    fn tag_typed_value(&self, typed_value: &Self::TypedValue) -> Self::TypeTag {
        typed_value.value_type()
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
                DefaultTyper.type_raw_value(&raw.into()),
                Value::Boolean(expected),
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
                DefaultTyper.type_raw_value(&raw.into()),
                Value::Int(expected),
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
            let determined = DefaultTyper.type_raw_value(&raw.into());
            let is_equal = if let Value::Float(parsed) = determined {
                parsed.is_nan() && expected.is_nan()
                    || parsed.partial_cmp(&expected) == Some(Ordering::Equal)
            } else {
                false
            };
            assert!(is_equal, "{} != {}", raw, expected);
        }
    }
}
