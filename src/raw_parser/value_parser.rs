use super::RawValue;

#[derive(Clone, Copy, Hash, PartialEq, Eq, Debug)]
pub enum Parsed<A> {
    Invalid,
    Missing,
    Some(A),
}

impl<T> Parsed<T> {
    pub fn get(self) -> Option<T> {
        match self {
            Parsed::Missing => None,
            Parsed::Invalid => None,
            Parsed::Some(x) => Some(x),
        }
    }

    pub fn or_else(self, op: impl FnOnce() -> Parsed<T>) -> Parsed<T> {
        match self {
            Parsed::Missing => op(),
            Parsed::Invalid => op(),
            some => some,
        }
    }

    pub fn unwrap_or_else(self, op: impl FnOnce() -> T) -> T {
        match self {
            Parsed::Missing => op(),
            Parsed::Invalid => op(),
            Parsed::Some(t) => t,
        }
    }

    pub fn map<U>(self, op: impl FnOnce(T) -> U) -> Parsed<U> {
        match self {
            Parsed::Missing => Parsed::Missing,
            Parsed::Invalid => Parsed::Invalid,
            Parsed::Some(t) => Parsed::Some(op(t)),
        }
    }

    pub fn is_some(&self) -> bool {
        matches!(self, Parsed::Some(_))
    }
}

/// Parsing of CSV raw values into primitive types
pub trait ValueParser<T> {
    fn parse_csv(&self) -> Parsed<T>;
}

impl ValueParser<bool> for RawValue {
    fn parse_csv(&self) -> Parsed<bool> {
        match self.0.trim().to_lowercase().as_ref() {
            "" => Parsed::Missing,
            "1" | "t" => Parsed::Some(true),
            "0" | "f" => Parsed::Some(false),
            otherwise => otherwise
                .parse()
                .map(Parsed::Some)
                .unwrap_or(Parsed::Invalid),
        }
    }
}

impl ValueParser<i64> for RawValue {
    fn parse_csv(&self) -> Parsed<i64> {
        match self.0.trim() {
            "" => Parsed::Missing,
            otherwise => otherwise
                .parse()
                .map(Parsed::Some)
                .unwrap_or(Parsed::Invalid),
        }
    }
}

impl ValueParser<f64> for RawValue {
    fn parse_csv(&self) -> Parsed<f64> {
        match self.0.trim().to_lowercase().as_ref() {
            "" => Parsed::Missing,
            "nan" => Parsed::Some(f64::NAN),
            otherwise => otherwise
                .parse()
                .map(Parsed::Some)
                .unwrap_or(Parsed::Invalid),
        }
    }
}

impl ValueParser<String> for RawValue {
    fn parse_csv(&self) -> Parsed<String> {
        Parsed::Some(self.0.to_string())
    }
}
