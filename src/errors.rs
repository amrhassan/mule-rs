use derive_more::Display;
use thiserror::Error;

use crate::lexer::RecordLexerError;

pub type Result<T> = std::result::Result<T, MuleError>;

#[derive(Error, Debug, Display)]
pub enum MuleError {
    Io(#[from] std::io::Error),
    RecordLexer(#[from] RecordLexerError),
    SchemaInference(String),
}
