mod column_type;
mod errors;
mod parser;
mod schema_inference;

pub use errors::Result;
pub use parser::{CsvParser, Value};
pub use schema_inference::detect_separator;
