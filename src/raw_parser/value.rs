use derive_more::{Display, From, Into};

/// A CSV value
#[derive(Debug, Clone, Hash, PartialEq, Eq, From, Into, Display)]
pub struct RawValue(pub String);

impl From<&str> for RawValue {
    fn from(s: &str) -> Self {
        s.to_string().into()
    }
}
