use crate::errors::MuleError;
use crate::raw_parser::{LineParser, Parsed, ParsingOptions};
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

/// Infer the schema of a file by determining the type of each column as the one that most of
/// the column values can be parsed into.
pub async fn infer_schema<T: Typer>(
    file_path: impl AsRef<Path>,
    skip_first_line: bool,
    inference_depth: usize,
    options: &ParsingOptions,
    typer: T,
) -> Result<Vec<T::ColumnType>> {
    let lines_to_skip = if skip_first_line { 1 } else { 0 };
    let mut lines = file::read_lines(file_path)
        .await?
        .skip(lines_to_skip)
        .take(inference_depth);

    let mut column_freqs: Vec<HashMap<T::ColumnType, usize>> = Vec::new();

    while let Some(line_res) = lines.next().await {
        let line_values = LineParser::new(line_res?, &options);
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

    let mut output = vec![];

    for (col_ix, col_freq) in column_freqs.into_iter().enumerate() {
        let type_tag = col_freq
            .into_iter()
            .sorted_by_key(|(_, count)| *count)
            .last()
            .map(|(tag, _)| tag)
            .ok_or_else(|| {
                MuleError::ColumnTyping(format!(
                    "Failed to find at least a single matching type for column {}",
                    col_ix
                ))
            })?;
        output.push(type_tag);
    }

    Ok(output)
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
        let column_types =
            infer_schema("datasets/sales-100.csv", true, 200, &parsing_options, typer).await?;

        let expected = vec![
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
        ];

        assert_eq!(expected, column_types);

        Ok(())
    }
}
