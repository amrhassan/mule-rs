use thiserror::Error;
use derive_more::Display;

pub type Result<T> = std::result::Result<T, MuleError>;

#[derive(Error, Debug, Display)]
pub enum MuleError {
    Io(#[from] std::io::Error),
}
