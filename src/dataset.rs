use crate::default_typer::DefaultTyper;
use crate::errors::Result;
use crate::raw_parser::{ColumnValue, LineParser};
use crate::schema_inference::{
    count_lines, infer_column_types, infer_separator, read_column_names,
};
use crate::typer::Typer;
use std::path::Path;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};

#[derive(Debug, Clone)]
pub struct Dataset<T: Typer> {
    pub column_names: Option<Vec<String>>,
    pub column_types: Vec<T::TypeTag>,
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
        let line_count = count_lines(File::open(file_path.clone()).await?).await?;
        let schema_inference_depth =
            (options.schema_inference_percentage.min(1.0) * line_count as f64).ceil() as usize;

        let separator = match options.separator {
            Separator::Value(value) => value,
            Separator::Infer => infer_separator(File::open(file_path.clone()).await?).await?,
        };

        let column_names = if options.read_header {
            read_column_names(
                File::open(file_path.clone()).await?,
                &separator,
                &options.text_quote,
                &options.text_quote_escape,
            )
            .await?
        } else {
            None
        };

        let skip_first_row = column_names.is_some();
        let row_count = if skip_first_row {
            line_count - 1
        } else {
            line_count
        };
        let column_types = infer_column_types(
            File::open(file_path.clone()).await?,
            skip_first_row,
            schema_inference_depth,
            &separator,
            &options.text_quote,
            &options.text_quote_escape,
            T::default(),
        )
        .await?;

        let data = read_data(
            file_path.clone(),
            &column_types,
            skip_first_row,
            &separator,
            &options.text_quote,
            &options.text_quote_escape,
            typer,
        )
        .await?;

        Ok(Dataset {
            column_names,
            column_types,
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

#[derive(Clone, Debug)]
pub struct ReadingOptions {
    pub read_header: bool,
    /// A value between 0.0 and 1.0 indicating the percentage of rows to read for schema inference
    pub schema_inference_percentage: f64,
    pub separator: Separator,
    pub text_quote: String,
    pub text_quote_escape: String,
}

impl Default for ReadingOptions {
    fn default() -> Self {
        ReadingOptions {
            read_header: true,
            schema_inference_percentage: 0.01,
            separator: Separator::Infer,
            text_quote: "\"".to_string(),
            text_quote_escape: "\\".to_string(),
        }
    }
}

async fn read_data<T: Typer>(
    file_path: impl AsRef<Path>,
    column_types: &[T::TypeTag],
    skip_first_row: bool,
    separator: &str,
    text_quote: &str,
    text_quote_escape: &str,
    typer: &T,
) -> Result<Vec<Vec<ColumnValue<T::TypedValue>>>> {
    let mut data: Vec<Vec<ColumnValue<T::TypedValue>>> = vec![vec![]; column_types.len()];
    let mut lines = BufReader::new(File::open(file_path).await?).lines();

    if skip_first_row {
        let _ = lines.next_line().await?;
    }

    while let Some(line) = lines.next_line().await? {
        let line_values = LineParser::new(line, separator, text_quote, text_quote_escape);
        for (col_ix, (value, column_type)) in line_values.zip(column_types.iter()).enumerate() {
            let column_value = typer.type_value_as(&value, *column_type);
            data[col_ix].push(column_value);
        }
    }

    Ok(data)
}
