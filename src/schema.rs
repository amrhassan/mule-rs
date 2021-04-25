use crate::dataset_file::{DatasetFile, LinesToRead};
use crate::errors::Result;
use crate::line_parsing::{LineParser, LineParsingOptions};
use crate::typer::{DatasetValue, Typer};
use crate::value_parsing::Parsed;
use futures_core::TryStream;
use itertools::Itertools;
use maplit::hashmap;
use rayon::current_num_threads;
use rayon::prelude::*;
use std::collections::HashMap;
use std::path::Path;
use tokio::task;
use tokio_stream::StreamExt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema<T: Typer> {
    pub column_types: Vec<T::ColumnType>,
}

impl<T: Typer + Send + Sync> Schema<T> {
    pub async fn infer(
        file_path: impl AsRef<Path>,
        skip_header: bool,
        inference_depth: &SchemaInferenceDepth,
        parsing_options: &LineParsingOptions,
        typer: &T,
    ) -> Result<Schema<T>> {
        let lines_to_read = match inference_depth {
            SchemaInferenceDepth::Lines(n) => LinesToRead::Absolute(*n),
            SchemaInferenceDepth::Percentage(percentage) => LinesToRead::Percentage(*percentage),
        };

        let own_file_path = file_path.as_ref().to_owned();
        let own_typer = typer.clone();
        let own_parsing_options = parsing_options.clone();
        let column_type_counts = task::spawn_blocking(move || {
            count_file_column_types_blocking(
                own_file_path,
                skip_header,
                lines_to_read,
                &own_parsing_options,
                &own_typer,
            )
        })
        .await
        .expect("Failed to join on a blocking task")?;

        let column_types = column_type_counts
            .0
            .into_iter()
            .map(|types| {
                types
                    .into_iter()
                    .sorted_by_key(|(_, count)| *count)
                    .last()
                    .map(|(column_type, _)| column_type)
                    .unwrap_or_default()
            })
            .collect();

        Ok(Schema { column_types })
    }
}

fn count_file_column_types_blocking<T: Typer + Send + Sync>(
    file_path: impl AsRef<Path> + Clone + Sized,
    skip_header: bool,
    lines_to_read: LinesToRead,
    parsing_options: &LineParsingOptions,
    typer: &T,
) -> Result<ColumnTypeCounts<T>> {
    let batch_count = current_num_threads();
    let dataset_file = DatasetFile::new(file_path);
    let line_batches = dataset_file.batches_blocking(skip_header, lines_to_read, batch_count)?;

    let batch_column_type_counts: Vec<Result<ColumnTypeCounts<T>>> = line_batches
        .into_par_iter()
        .map(|batch| match batch.read_lines_blocking() {
            Ok(lines) => count_column_types(lines, typer, parsing_options),
            Err(err) => Err(err),
        })
        .collect();

    let mut output = ColumnTypeCounts::default();
    for column_type_counts in batch_column_type_counts.into_iter() {
        output.update_with(column_type_counts?)
    }

    Ok(output)
}

#[tokio::main(flavor = "current_thread")]
async fn count_column_types<T: Typer>(
    mut lines: impl TryStream<Item = Result<String>> + Unpin,
    typer: &T,
    parsing_options: &LineParsingOptions,
) -> Result<ColumnTypeCounts<T>> {
    let mut column_type_counts: Vec<HashMap<T::ColumnType, usize>> = Vec::new();
    while let Some(line_res) = lines.next().await {
        let line_values = LineParser::new(line_res?, &parsing_options);
        for (ix, val) in line_values.enumerate() {
            if let Parsed::Some(parsed) = typer.parse(&val) {
                let column_type = parsed.get_column_type();
                match column_type_counts.get_mut(ix) {
                    Some(counts) => *counts.entry(parsed.get_column_type()).or_default() += 1,
                    None => column_type_counts.push(hashmap! { column_type => 1 }),
                }
            }
        }
    }
    Ok(ColumnTypeCounts(column_type_counts))
}

/// A mapping of each parsable column types to how prevalent it is in each column, ordered by column order.
#[derive(Default, Debug)]
struct ColumnTypeCounts<T: Typer>(Vec<HashMap<T::ColumnType, usize>>);

impl<T: Typer> ColumnTypeCounts<T> {
    fn update_with(&mut self, other: Self) {
        for (col_ix, rhs_col) in other.0.into_iter().enumerate() {
            for (t, t_counts) in rhs_col.into_iter() {
                let type_counts = match self.0.get_mut(col_ix) {
                    Some(counts) => counts,
                    None => {
                        self.0.push(HashMap::new());
                        self.0.get_mut(col_ix).unwrap()
                    }
                };
                *type_counts.entry(t).or_default() += t_counts;
            }
        }
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

impl Default for SchemaInferenceDepth {
    fn default() -> Self {
        SchemaInferenceDepth::Percentage(0.1)
    }
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
            &typer,
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
