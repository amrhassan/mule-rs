use crate::default_typer::DefaultTyper;
use crate::errors::Result;
use crate::file;
use crate::header_parsing::Header;
use crate::line_parsing::LineParsingOptions;
use crate::raw_parser::read_file_data;
use crate::schema::Schema;
use crate::schema::SchemaInferenceDepth;
use crate::schema_inference::{infer_schema, infer_separator};
use crate::typer::Typer;
use std::path::Path;

/// Strongly-typed columnar dataset
#[derive(Debug, Clone)]
pub struct Dataset<T: Typer> {
    pub header: Option<Header>,
    pub schema: Schema<T>,
    pub data: Vec<T::Column>,
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
            SchemaInferenceDepth::Percentage(x) => (x.min(1.0) * line_count as f64).ceil() as usize,
        };

        let separator = match options.separator {
            Separator::Value(value) => value,
            Separator::Infer => infer_separator(file_path.clone()).await?,
        };

        let parsing_options = LineParsingOptions {
            text_quote: options.text_quote,
            text_quote_escape: options.text_quote_escape,
            separator,
        };

        let header = if options.read_header {
            Header::parse(file_path.clone(), &parsing_options).await?
        } else {
            None
        };

        let skip_first_line = header.is_some();
        let row_count = if skip_first_line {
            line_count - 1
        } else {
            line_count
        };
        let schema = infer_schema(
            &file_path,
            skip_first_line,
            &SchemaInferenceDepth::Lines(schema_inference_line_count),
            &parsing_options,
            T::default(),
        )
        .await?;

        let data = read_file_data(
            &file_path,
            &schema,
            &parsing_options,
            line_count,
            skip_first_line,
            typer,
        )
        .await?;

        Ok(Dataset {
            header,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Column, ColumnType};
    use itertools::Itertools;

    #[tokio::test]
    pub async fn test_dataset_read_sales_10_weird() -> Result<()> {
        let options = ReadingOptions::default();
        let typer = DefaultTyper::default();
        let dataset = Dataset::read_file("datasets/sales-10-weird.csv", options, &typer).await?;

        let schema = dataset.schema;
        let header = dataset.header;
        let data = dataset.data;

        let expected_schema = Schema {
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

        let expected_header = Some(Header {
            column_names: vec![
                "Region",
                "Country",
                "Item Type",
                "Sales Channel",
                "",
                "Order Date",
                "Order ID",
                "\"Ship\" Date",
                "Units Sold",
                "Unit Price",
                "Unit Cost",
                "Total Revenue",
                "Total Cost",
                "Total Profit",
            ]
            .into_iter()
            .map_into()
            .collect_vec(),
        });

        assert_eq!(schema, expected_schema);
        assert_eq!(schema.column_types.len(), 14);
        assert_eq!(header, expected_header);
        assert_eq!(header.map(|h| h.column_names.len()), Some(14));
        assert_eq!(data.len(), 14);

        for column in data {
            let all_good = match &column {
                Column::Boolean(vs) => vs.iter().all(|v| v.is_some()),
                Column::Int(vs) => vs.iter().all(|v| v.is_some()),
                Column::Float(vs) => vs.iter().all(|v| v.is_some()),
                Column::Text(vs) => vs.iter().all(|v| v.is_some()),
                Column::Unknown => panic!(),
            };
            assert!(all_good, "The column has invalid values! {:?}", column)
        }

        Ok(())
    }
}
