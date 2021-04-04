use super::default_typer::DefaultTyper;
use super::errors::Result;
use super::raw_parser::LineParser;
use super::schema_inference::{count_lines, infer_column_types, read_column_names};
use super::typer::{ColumnValue, Typer};
use std::path::Path;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};

pub struct Dataset<T: Typer> {
    pub column_names: Option<Vec<String>>,
    pub column_types: Vec<T::TypeTag>,
    pub data: Vec<Vec<ColumnValue<T::TypedValue>>>,
}

impl<T: Typer> Dataset<T> {
    pub async fn read_file(
        file_path: impl AsRef<Path> + Clone,
        options: ReadingOptions<T>,
    ) -> Result<Dataset<T>> {
        let line_count = count_lines(File::open(file_path.clone()).await?).await?;
        let schema_inference_depth =
            (options.schema_inference_percentage.min(1.0) * line_count as f64).ceil() as usize;

        let column_names = if options.read_header {
            read_column_names(
                File::open(file_path.clone()).await?,
                &options.separator,
                &options.text_quote,
                &options.text_quote_escape,
            )
            .await?
        } else {
            None
        };

        let skip_first_row = column_names.is_some();
        let column_types = infer_column_types(
            File::open(file_path.clone()).await?,
            skip_first_row,
            schema_inference_depth,
            &options.separator,
            &options.text_quote,
            &options.text_quote_escape,
            T::new(),
        )
        .await?;

        let mut data: Vec<Vec<ColumnValue<T::TypedValue>>> = vec![vec![]; column_types.len()];
        let mut lines = BufReader::new(File::open(file_path).await?).lines();

        if skip_first_row {
            let _ = lines.next_line().await?;
        }

        while let Some(line) = lines.next_line().await? {
            let line_values = LineParser::new(
                line,
                &options.separator,
                &options.text_quote,
                &options.text_quote_escape,
            );
            for (col_ix, (value, column_type)) in line_values.zip(column_types.iter()).enumerate() {
                let column_value = options.typer.type_raw_value_as(&value, *column_type);
                data[col_ix].push(column_value);
            }
        }

        Ok(Dataset {
            column_names,
            column_types,
            data,
        })
    }
}

pub type TypedDataset = Dataset<DefaultTyper>;

#[derive(Clone, Debug)]
pub struct ReadingOptions<T> {
    pub read_header: bool,
    /// A value between 0.0 and 1.0 indicating the percentage of rows to read for schema inference
    pub schema_inference_percentage: f64,
    pub separator: String,
    pub text_quote: String,
    pub text_quote_escape: String,
    pub typer: T,
}

impl Default for ReadingOptions<DefaultTyper> {
    fn default() -> Self {
        ReadingOptions {
            read_header: true,
            schema_inference_percentage: 0.01,
            separator: ",".to_string(),
            text_quote: "\"".to_string(),
            text_quote_escape: "\\".to_string(),
            typer: DefaultTyper::new(),
        }
    }
}

pub type DefaultTypedReadingOptions = ReadingOptions<DefaultTyper>;
