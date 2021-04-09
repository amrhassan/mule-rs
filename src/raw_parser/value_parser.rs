use super::RawValue;

/// A single value in a dataset's column
#[derive(Clone, Copy, Hash, PartialEq, Eq, Debug)]
pub enum ColumnValue<A> {
    Invalid,
    Missing,
    Some(A),
}

impl<T> ColumnValue<T> {
    pub fn or_else(self, op: impl FnOnce() -> ColumnValue<T>) -> ColumnValue<T> {
        match self {
            ColumnValue::Missing => op(),
            ColumnValue::Invalid => op(),
            some => some,
        }
    }

    pub fn unwrap_or_else(self, op: impl FnOnce() -> T) -> T {
        match self {
            ColumnValue::Missing => op(),
            ColumnValue::Invalid => op(),
            ColumnValue::Some(t) => t,
        }
    }

    pub fn map<U>(self, op: impl FnOnce(T) -> U) -> ColumnValue<U> {
        match self {
            ColumnValue::Missing => ColumnValue::Missing,
            ColumnValue::Invalid => ColumnValue::Invalid,
            ColumnValue::Some(t) => ColumnValue::Some(op(t)),
        }
    }

    pub fn is_some(&self) -> bool {
        matches!(self, ColumnValue::Some(_))
    }
}

/// Parsing of CSV raw values into primitive types
pub trait ValueParser<T> {
    fn parse_csv(&self) -> ColumnValue<T>;
}

impl ValueParser<bool> for RawValue {
    fn parse_csv(&self) -> ColumnValue<bool> {
        match self.0.trim().to_lowercase().as_ref() {
            "" => ColumnValue::Missing,
            "1" | "t" => ColumnValue::Some(true),
            "0" | "f" => ColumnValue::Some(false),
            otherwise => otherwise
                .parse()
                .map(ColumnValue::Some)
                .unwrap_or(ColumnValue::Invalid),
        }
    }
}

impl ValueParser<i64> for RawValue {
    fn parse_csv(&self) -> ColumnValue<i64> {
        match self.0.trim() {
            "" => ColumnValue::Missing,
            otherwise => otherwise
                .parse()
                .map(ColumnValue::Some)
                .unwrap_or(ColumnValue::Invalid),
        }
    }
}

impl ValueParser<f64> for RawValue {
    fn parse_csv(&self) -> ColumnValue<f64> {
        match self.0.trim().to_lowercase().as_ref() {
            "" => ColumnValue::Missing,
            "nan" => ColumnValue::Some(f64::NAN),
            otherwise => otherwise
                .parse()
                .map(ColumnValue::Some)
                .unwrap_or(ColumnValue::Invalid),
        }
    }
}

impl ValueParser<String> for RawValue {
    fn parse_csv(&self) -> ColumnValue<String> {
        ColumnValue::Some(self.0.to_string())
    }
}
