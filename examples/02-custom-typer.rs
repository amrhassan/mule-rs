use derive_more::Display;
use mule::{Dataset, DatasetValue, Parsed, RawValue, ReadingOptions, Result, Typer, ValueParser};
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

impl DatasetValue<CustomColumnType> for CustomValue {
    fn get_column_type(&self) -> CustomColumnType {
        match self {
            CustomValue::Maybe(_) => CustomColumnType::Maybe,
            CustomValue::Int(_) => CustomColumnType::Int,
            CustomValue::Float(_) => CustomColumnType::Float,
            CustomValue::Text(_) => CustomColumnType::Text,
        }
    }
}

#[derive(PartialEq, Debug, Eq, Hash, Clone, Copy)]
pub enum YayNay {
    Yay,
    Nay,
    Nah,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Display)]
pub enum CustomColumnType {
    Maybe,
    Int,
    Float,
    Text,
    Unknown,
}

impl Default for CustomColumnType {
    fn default() -> Self {
        CustomColumnType::Unknown
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum CustomColumn {
    Maybe(Vec<Parsed<YayNay>>),
    Int(Vec<Parsed<i64>>),
    Float(Vec<Parsed<f64>>),
    Text(Vec<Parsed<String>>),
    Unknown,
}

impl Default for CustomColumn {
    fn default() -> Self {
        CustomColumn::Unknown
    }
}

#[derive(Default, Debug)]
pub struct CustomTyper;

impl CustomTyper {
    fn as_int(&self, value: &RawValue) -> Parsed<CustomValue> {
        value.parse_csv().map(CustomValue::Int)
    }

    fn as_float(&self, value: &RawValue) -> Parsed<CustomValue> {
        value.parse_csv().map(CustomValue::Float)
    }

    fn as_text(&self, value: &RawValue) -> CustomValue {
        CustomValue::Text(value.0.to_string())
    }

    fn as_maybe(&self, value: &RawValue) -> Parsed<CustomValue> {
        match value.0.to_lowercase().trim() {
            "" => Parsed::Missing,
            "yay" => Parsed::Some(CustomValue::Maybe(YayNay::Yay)),
            "nay" => Parsed::Some(CustomValue::Maybe(YayNay::Nay)),
            _ => Parsed::Invalid,
        }
    }

    fn as_maybe_column(&self, values: Vec<Parsed<CustomValue>>) -> CustomColumn {
        let vs = values
            .into_iter()
            .map(|v| match v {
                Parsed::Some(CustomValue::Maybe(x)) => Parsed::Some(x),
                Parsed::Invalid => Parsed::Invalid,
                Parsed::Missing => Parsed::Missing,
                _ => Parsed::Invalid,
            })
            .collect();
        CustomColumn::Maybe(vs)
    }

    fn as_int_column(&self, values: Vec<Parsed<CustomValue>>) -> CustomColumn {
        let vs = values
            .into_iter()
            .map(|v| match v {
                Parsed::Some(CustomValue::Int(x)) => Parsed::Some(x),
                Parsed::Invalid => Parsed::Invalid,
                Parsed::Missing => Parsed::Missing,
                _ => Parsed::Invalid,
            })
            .collect();
        CustomColumn::Int(vs)
    }

    fn as_float_column(&self, values: Vec<Parsed<CustomValue>>) -> CustomColumn {
        let vs = values
            .into_iter()
            .map(|v| match v {
                Parsed::Some(CustomValue::Float(x)) => Parsed::Some(x),
                Parsed::Invalid => Parsed::Invalid,
                Parsed::Missing => Parsed::Missing,
                _ => Parsed::Invalid,
            })
            .collect();
        CustomColumn::Float(vs)
    }

    fn as_text_column(&self, values: Vec<Parsed<CustomValue>>) -> CustomColumn {
        let vs = values
            .into_iter()
            .map(|v| match v {
                Parsed::Some(CustomValue::Text(x)) => Parsed::Some(x),
                Parsed::Invalid => Parsed::Invalid,
                Parsed::Missing => Parsed::Missing,
                _ => Parsed::Invalid,
            })
            .collect();
        CustomColumn::Text(vs)
    }
}

impl Typer for CustomTyper {
    type ColumnType = CustomColumnType;
    type DatasetValue = CustomValue;
    type Column = CustomColumn;

    const COLUMN_TYPES: &'static [Self::ColumnType] = &[
        CustomColumnType::Maybe,
        CustomColumnType::Int,
        CustomColumnType::Float,
        CustomColumnType::Text,
    ];

    fn parse_as(&self, value: &RawValue, tag: Self::ColumnType) -> Parsed<Self::DatasetValue> {
        match tag {
            CustomColumnType::Maybe => self.as_maybe(value),
            CustomColumnType::Int => self.as_int(value),
            CustomColumnType::Float => self.as_float(value),
            CustomColumnType::Text => Parsed::Some(self.as_text(value)),
            CustomColumnType::Unknown => Parsed::Invalid,
        }
    }

    fn parse_column(
        &self,
        tag: Self::ColumnType,
        values: Vec<Parsed<Self::DatasetValue>>,
    ) -> Self::Column {
        match tag {
            CustomColumnType::Maybe => self.as_maybe_column(values),
            CustomColumnType::Int => self.as_int_column(values),
            CustomColumnType::Float => self.as_float_column(values),
            CustomColumnType::Text => self.as_text_column(values),
            CustomColumnType::Unknown => Self::Column::default(),
        }
    }
}
