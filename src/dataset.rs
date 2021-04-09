use crate::default_typer::DefaultTyper;
use crate::errors::Result;
use crate::file;
use crate::raw_parser::{read_file_column_names, read_file_data, ColumnValue, ParsingOptions};
use crate::schema_inference::{infer_schema, infer_separator};
use crate::typer::Typer;
use std::path::Path;

/// Strongly-typed columnar dataset
#[derive(Debug, Clone)]
pub struct Dataset<T: Typer> {
    pub column_names: Option<Vec<String>>,
    pub schema: Vec<T::TypeTag>,
    pub data: Vec<Vec<ColumnValue<T::TypedValue>>>,
    pub row_count: usize,
}

/// Opens and reads the dataset at the specified file using the default options and type system.
pub async fn read_file(file_path: impl AsRef<Path> + Clone) -> Result<Dataset<DefaultTyper>> {
    let typer = DefaultTyper::default();
    let options = ReadingOptions::default();
    let ds = Dataset::read_file(file_path, options, &typer).await?;
    Ok(ds)
}

impl<T: Typer> Dataset<T> {
    pub async fn read_file(
        file_path: impl AsRef<Path> + Clone,
        options: ReadingOptions,
        typer: &T,
    ) -> Result<Dataset<T>> {
        let line_count = file::count_lines(file_path.clone()).await?;
        let schema_inference_line_count = match options.schema_inference_depth {
            SchemaInferenceDepth::Lines(n) => n,
            SchemaInferenceDepth::Percentage(x) => (x.min(1.0) * line_count as f32).ceil() as usize,
        };

        let separator = match options.separator {
            Separator::Value(value) => value,
            Separator::Infer => infer_separator(file_path.clone()).await?,
        };

        let parsing_options = ParsingOptions {
            text_quote: options.text_quote,
            text_quote_escape: options.text_quote_escape,
            separator,
        };

        let column_names = if options.read_header {
            read_file_column_names(file_path.clone(), &parsing_options).await?
        } else {
            None
        };

        let skip_first_line = column_names.is_some();
        let row_count = if skip_first_line {
            line_count - 1
        } else {
            line_count
        };
        let schema = infer_schema(
            file_path.clone(),
            skip_first_line,
            schema_inference_line_count,
            &parsing_options,
            T::default(),
        )
        .await?;

        let data = read_file_data(
            file_path.clone(),
            &schema,
            &parsing_options,
            line_count,
            skip_first_line,
            typer,
        )
        .await?;

        Ok(Dataset {
            column_names,
            schema,
            row_count,
            data,
        })
    }
}

pub type TypedDataset = Dataset<DefaultTyper>;

/// Dataset separator used while reading
#[derive(Clone, Debug)]
pub enum Separator {
    Value(String),
    Infer,
}

/// Number of lines to read while inferring the dataset schema
#[derive(Copy, Clone, Debug)]
pub enum SchemaInferenceDepth {
    /// Percentage of total number of lines
    Percentage(f32),
    /// Absolute number of lines
    Lines(usize),
}

#[derive(Clone, Debug)]
pub struct ReadingOptions {
    pub read_header: bool,
    pub schema_inference_depth: SchemaInferenceDepth,
    pub separator: Separator,
    pub text_quote: String,
    pub text_quote_escape: String,
}

impl Default for ReadingOptions {
    fn default() -> Self {
        ReadingOptions {
            read_header: true,
            schema_inference_depth: SchemaInferenceDepth::Percentage(0.01),
            separator: Separator::Infer,
            text_quote: "\"".to_string(),
            text_quote_escape: "\\".to_string(),
        }
    }
}
