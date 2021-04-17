#![deny(warnings)]
#![deny(clippy::all)]

mod dataset;
mod default_typer;
mod errors;
mod file;
mod raw_parser;
mod schema_inference;
mod typer;

pub use dataset::{
    read_file, Dataset, ReadingOptions, SchemaInferenceDepth, Separator, TypedDataset,
};
pub use default_typer::{Column, ColumnType, DefaultTyper, Value};
pub use errors::Result;
pub use raw_parser::{Parsed, RawValue, ValueParser};
pub use schema_inference::{infer_schema, infer_separator};
pub use typer::{DatasetValue, Typer};
