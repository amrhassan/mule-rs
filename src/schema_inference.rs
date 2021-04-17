use crate::default_typer::DefaultTyper;
use crate::raw_parser::{LineParser, Parsed, ParsingOptions};
use crate::schema::Schema;
use crate::typer::{DatasetValue, Typer};
use crate::{errors::Result, file};
use itertools::Itertools;
use maplit::hashmap;
use std::collections::HashMap;
use std::path::Path;
use tokio_stream::StreamExt;

static COMMON_SEPARATORS: [&str; 3] = [",", "\t", "|"];

/// Infer the separator as the most commonly used separator in the file
pub async fn infer_separator(path: impl AsRef<Path>) -> Result<String> {
    let mut counts: HashMap<&str, usize> = HashMap::default();

    let mut lines = file::read_lines(path).await?;
    while let Some(line_res) = lines.next().await {
        let line = line_res?;
        for sep in COMMON_SEPARATORS.iter() {
            *counts.entry(sep).or_default() += line.clone().matches(sep).count();
        }
    }
    let sep = counts
        .into_iter()
        .sorted_by_key(|(_, v)| *v)
        .last()
        .map(|(k, _)| k)
        .unwrap_or(",");
    Ok(sep.to_string())
}

/// Number of lines to read while inferring the dataset schema
#[derive(Copy, Clone, Debug)]
pub enum SchemaInferenceDepth {
    /// Percentage of total number of lines
    Percentage(f32),
    /// Absolute number of lines
    Lines(usize),
}

impl Default for SchemaInferenceDepth {
    fn default() -> Self {
        SchemaInferenceDepth::Percentage(0.1)
    }
}

/// Opens and infers the schema of the dataset at the specified path using the default options and type system.
pub async fn infer_file_schema(
    file_path: impl AsRef<Path> + Clone,
    inference_depth: &SchemaInferenceDepth,
    parsing_options: &ParsingOptions,
) -> Result<Schema<DefaultTyper>> {
    let typer = DefaultTyper::default();
    let skip_header = true;
    let schema = infer_schema(
        file_path,
        skip_header,
        inference_depth,
        parsing_options,
        typer,
    )
    .await?;
    Ok(schema)
}

/// Infer the schema of a file by determining the type of each column as the one that most of
/// the column values can be parsed into.
pub async fn infer_schema<T: Typer>(
    file_path: impl AsRef<Path>,
    skip_header: bool,
    inference_depth: &SchemaInferenceDepth,
    parsing_options: &ParsingOptions,
    typer: T,
) -> Result<Schema<T>> {
    let lines_to_skip = if skip_header { 1 } else { 0 };
    let lines_to_read = match inference_depth {
        SchemaInferenceDepth::Lines(n) => *n,
        SchemaInferenceDepth::Percentage(percentage) => {
            let line_count = file::count_lines(&file_path).await?;
            ((*percentage).min(1.0) * (line_count as f32)).ceil() as usize
        }
    };

    let mut lines = file::read_lines(file_path)
        .await?
        .skip(lines_to_skip)
        .take(lines_to_read);

    let mut column_freqs: Vec<HashMap<T::ColumnType, usize>> = Vec::new();

    while let Some(line_res) = lines.next().await {
        let line_values = LineParser::new(line_res?, &parsing_options);
        for (ix, val) in line_values.enumerate() {
            if let Parsed::Some(parsed) = typer.parse(&val) {
                let column_type = parsed.get_column_type();
                match column_freqs.get_mut(ix) {
                    Some(counts) => *counts.entry(parsed.get_column_type()).or_default() += 1,
                    None => column_freqs.push(hashmap! { column_type => 1 }),
                }
            }
        }
    }

    let column_types = column_freqs
        .into_iter()
        .map(|types| {
            types
                .into_iter()
                .sorted_by_key(|(_, count)| *count)
                .last()
                .map(|(column_type, _)| column_type)
                .unwrap_or(T::ColumnType::default())
        })
        .collect();

    Ok(Schema { column_types })
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{ColumnType, DefaultTyper};

    #[tokio::test]
    pub async fn test_separator_inference() -> Result<()> {
        assert_eq!(infer_separator("datasets/sales-100.tsv").await?, "\t");

        assert_eq!(
            infer_separator("datasets/sales-100.csv").await.unwrap(),
            ","
        );

        Ok(())
    }

    #[tokio::test]
    pub async fn test_infer_column_types_sales_100() -> Result<()> {
        let typer = DefaultTyper::default();
        let parsing_options = ParsingOptions {
            separator: ",".to_string(),
            text_quote: "\"".to_string(),
            text_quote_escape: "\\".to_string(),
        };
        let schema = infer_schema(
            "datasets/sales-100.csv",
            true,
            &SchemaInferenceDepth::Percentage(0.1),
            &parsing_options,
            typer,
        )
        .await?;

        let expected_schema = Schema::<DefaultTyper> {
            column_types: vec![
                ColumnType::Text,
                ColumnType::Text,
                ColumnType::Text,
                ColumnType::Text,
                ColumnType::Text,
                ColumnType::Text,
                ColumnType::Int,
                ColumnType::Text,
                ColumnType::Int,
                ColumnType::Float,
                ColumnType::Float,
                ColumnType::Float,
                ColumnType::Float,
                ColumnType::Float,
            ],
        };

        assert_eq!(schema, expected_schema);

        Ok(())
    }
}
