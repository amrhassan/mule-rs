// #![deny(warnings)]
// #![deny(clippy::all)]

mod column_parsing;
mod dataset;
mod dataset_batch;
mod dataset_file;
mod default_typer;
mod defaults;
mod errors;
mod header_parsing;
mod lexer;
mod record_parsing;
mod schema;
mod separator_inference;
mod typer;
mod value_parsing;

pub use column_parsing::{Column, Columns};
pub use dataset::{Dataset, ReadingOptions, Separator, TypedDataset};
pub use default_typer::{ColumnType, DefaultTyper, Value};
pub use defaults::read_file;
pub use errors::Result;
pub use record_parsing::RecordParsingOptions;
pub use schema::{Schema, SchemaInferenceDepth};
pub use typer::{DatasetValue, Typer};
pub use value_parsing::{Parsed, RawValue};
