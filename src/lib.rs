#![deny(warnings)]
#![deny(clippy::all)]

mod dataset;
mod default_typer;
mod errors;
mod raw_parser;
mod schema_inference;
mod typer;

pub use dataset::{
    read_file, Dataset, ReadingOptions, SchemaInferenceDepth, Separator, TypedDataset,
};
pub use default_typer::{DefaultTyper, Value, ValueType};
pub use errors::Result;
pub use raw_parser::{ColumnValue, RawValue, ValueParser};
pub use typer::Typer;
