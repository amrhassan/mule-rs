use super::dataset::{Dataset, ReadingOptions};
use super::default_typer::DefaultTyper;
use super::errors::Result;
use std::path::Path;

/// Opens and reads the dataset at the specified file using the default options and type system.
pub async fn read_file(file_path: impl AsRef<Path> + Clone) -> Result<Dataset<DefaultTyper>> {
    let typer = DefaultTyper::default();
    let options = ReadingOptions::default();
    let ds = Dataset::read_file(file_path, options, &typer).await?;
    Ok(ds)
}
