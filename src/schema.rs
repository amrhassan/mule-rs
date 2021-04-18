use crate::errors::Result;
use crate::file;
use crate::line_parsing::{LineParser, LineParsingOptions};
use crate::typer::{DatasetValue, Typer};
use crate::value_parsing::Parsed;
use itertools::Itertools;
use maplit::hashmap;
use std::collections::HashMap;
use std::path::Path;
use tokio_stream::StreamExt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema<T: Typer> {
    pub column_types: Vec<T::ColumnType>,
}

impl<T: Typer> Schema<T> {
    pub async fn infer(
        file_path: impl AsRef<Path>,
        skip_header: bool,
        inference_depth: &SchemaInferenceDepth,
        parsing_options: &LineParsingOptions,
        typer: T,
    ) -> Result<Schema<T>> {
        let lines_to_skip = if skip_header { 1 } else { 0 };

        let lines_to_read = match inference_depth {
            SchemaInferenceDepth::Lines(n) => *n,
            SchemaInferenceDepth::Percentage(percentage) => {
                let line_count = file::count_lines(&file_path).await?;
                ((*percentage).min(1.0) * (line_count as f64)).ceil() as usize
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
}

/// Number of lines to read while inferring the dataset schema
#[derive(Copy, Clone, Debug)]
pub enum SchemaInferenceDepth {
    /// Percentage of total number of lines
    Percentage(f64),
    /// Absolute number of lines
    Lines(usize),
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{ColumnType, DefaultTyper};

    #[tokio::test]
    pub async fn test_infer_schema_sales_100() -> Result<()> {
        let typer = DefaultTyper::default();
        let parsing_options = LineParsingOptions::default();
        let schema_inference_depth = SchemaInferenceDepth::default();
        let skip_first_line = true;
        let schema = Schema::infer(
            "datasets/sales-100.csv",
            skip_first_line,
            &schema_inference_depth,
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
