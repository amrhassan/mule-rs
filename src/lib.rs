#![deny(warnings)]
#![deny(clippy::all)]

mod column_parsing;
mod dataset;
mod default_typer;
mod errors;
mod file;
mod header_parsing;
mod line_parsing;
mod schema;
mod schema_inference;
mod typer;
mod defaults;
mod value_parsing;
mod separator_inference;

pub use defaults::read_file;
pub use column_parsing::{Column, Columns};
pub use dataset::{Dataset, ReadingOptions, Separator, TypedDataset};
pub use default_typer::{Column as CC, ColumnType, DefaultTyper, Value};
pub use errors::Result;
pub use line_parsing::LineParsingOptions;
pub use schema::{Schema, SchemaInferenceDepth};
pub use typer::{DatasetValue, Typer};
pub use value_parsing::{Parsed, RawValue};
