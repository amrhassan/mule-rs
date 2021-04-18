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
mod value_parsing;

pub use dataset::{read_file, Dataset, ReadingOptions, Separator, TypedDataset};
pub use default_typer::{Column, ColumnType, DefaultTyper, Value};
pub use errors::Result;
pub use raw_parser::ParsingOptions;
pub use schema::Schema;
pub use schema_inference::{infer_file_schema, SchemaInferenceDepth};
pub use typer::{DatasetValue, Typer};
pub use value_parsing::{Parsed, RawValue};
