mod file_parser;
mod line_parser;
mod value;
mod value_parser;

pub use file_parser::{read_file_column_names, read_file_data};
pub use line_parser::LineParser;
pub use value::RawValue;
pub use value_parser::{Parsed, ValueParser};

pub struct ParsingOptions {
    pub separator: String,
    pub text_quote: String,
    pub text_quote_escape: String,
}

impl Default for ParsingOptions {
    fn default() -> Self {
        ParsingOptions {
            separator: ",".to_string(),
            text_quote: "\"".to_string(),
            text_quote_escape: "\\".to_string(),
        }
    }
}
