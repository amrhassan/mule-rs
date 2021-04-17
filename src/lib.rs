#![deny(warnings)]
#![deny(clippy::all)]

mod dataset;
mod default_typer;
mod errors;
mod file;
mod raw_parser;
mod schema;
mod schema_inference;
mod typer;

pub use dataset::{read_file, Dataset, ReadingOptions, Separator, TypedDataset};
pub use default_typer::{Column, ColumnType, DefaultTyper, Value};
pub use errors::Result;
pub use raw_parser::{Parsed, ParsingOptions, RawValue, ValueParser};
pub use schema::Schema;
pub use schema_inference::{infer_file_schema, SchemaInferenceDepth};
pub use typer::{DatasetValue, Typer};
