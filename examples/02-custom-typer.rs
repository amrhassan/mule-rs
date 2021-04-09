use derive_more::Display;
use mule::{ColumnValue, Dataset, RawValue, ReadingOptions, Result, Typer, ValueParser};
use std::env;

#[tokio::main]
pub async fn main() -> Result<()> {
    let file_path = env::args()
        .skip(1)
        .next()
        .unwrap_or_else(|| "datasets/sales-10.tsv".to_string());
    let options = ReadingOptions::default();
    let typer = CustomTyper::default();
    let dataset = Dataset::read_file(file_path, options, &typer).await?;

    println!("Got dataset: {:#?}", dataset);

    Ok(())
}

#[derive(Clone, PartialEq, Debug)]
pub enum CustomValue {
    Maybe(YayNay),
    Int(i64),
    Float(f64),
    Text(String),
}

#[derive(PartialEq, Debug, Eq, Hash, Clone, Copy)]
pub enum YayNay {
    Yay,
    Nay,
    Nah,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Display)]
pub enum CustomValueType {
    Maybe,
    Int,
    Float,
    Text,
}

#[derive(Default, Debug)]
pub struct CustomTyper;

impl CustomTyper {
    fn as_int(&self, value: &RawValue) -> ColumnValue<CustomValue> {
        value.parse_csv().map(CustomValue::Int)
    }

    fn as_float(&self, value: &RawValue) -> ColumnValue<CustomValue> {
        value.parse_csv().map(CustomValue::Float)
    }

    fn as_text(&self, value: &RawValue) -> CustomValue {
        CustomValue::Text(value.0.to_string())
    }

    fn as_maybe(&self, value: &RawValue) -> ColumnValue<CustomValue> {
        match value.0.to_lowercase().trim() {
            "" => ColumnValue::Missing,
            "yay" => ColumnValue::Some(CustomValue::Maybe(YayNay::Yay)),
            "nay" => ColumnValue::Some(CustomValue::Maybe(YayNay::Nay)),
            _ => ColumnValue::Invalid,
        }
    }
}

impl Typer for CustomTyper {
    type TypeTag = CustomValueType;
    type TypedValue = CustomValue;

    const TYPES: &'static [Self::TypeTag] = &[
        CustomValueType::Maybe,
        CustomValueType::Int,
        CustomValueType::Float,
        CustomValueType::Text,
    ];

    fn type_value_as(&self, value: &RawValue, tag: Self::TypeTag) -> ColumnValue<Self::TypedValue> {
        match tag {
            CustomValueType::Maybe => self.as_maybe(value),
            CustomValueType::Int => self.as_int(value),
            CustomValueType::Float => self.as_float(value),
            CustomValueType::Text => ColumnValue::Some(self.as_text(value)),
        }
    }

    fn tag_type(&self, value: &Self::TypedValue) -> CustomValueType {
        match value {
            CustomValue::Maybe(_) => CustomValueType::Maybe,
            CustomValue::Int(_) => CustomValueType::Int,
            CustomValue::Float(_) => CustomValueType::Float,
            CustomValue::Text(_) => CustomValueType::Text,
        }
    }
}
