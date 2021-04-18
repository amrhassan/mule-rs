use crate::column_parsing::Columns;
use crate::default_typer::DefaultTyper;
use crate::errors::Result;
use crate::file;
use crate::header_parsing::Header;
use crate::line_parsing::LineParsingOptions;
use crate::schema::{Schema, SchemaInferenceDepth};
use crate::separator_inference::infer_separator;
use crate::typer::Typer;
use std::path::Path;

/// Strongly-typed columnar dataset
#[derive(Debug, Clone)]
pub struct Dataset<T: Typer> {
    pub header: Option<Header>,
    pub schema: Schema<T>,
    pub columns: Columns<T>,
}

impl<T: Typer> Dataset<T> {
    pub async fn read_file(
        file_path: impl AsRef<Path> + Clone,
        options: ReadingOptions,
        typer: &T,
    ) -> Result<Dataset<T>> {
        let line_count = file::count_lines(file_path.clone()).await?;

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

        let skip_first_line = options.read_header;
        let schema = Schema::infer(
            &file_path,
            skip_first_line,
            &options.schema_inference_depth,
            &parsing_options,
            typer,
        )
        .await?;

        let columns = Columns::parse(
            &file_path,
            &schema,
            &parsing_options,
            line_count,
            skip_first_line,
            &typer,
        )
        .await?;

        Ok(Dataset {
            header,
            schema,
            columns,
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
            schema_inference_depth: SchemaInferenceDepth::default(),
            separator: Separator::Infer,
            text_quote: "\"".to_string(),
            text_quote_escape: "\\".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ColumnType;
    use itertools::Itertools;

    #[tokio::test]
    pub async fn test_dataset_read_sales_10_weird() -> Result<()> {
        let options = ReadingOptions::default();
        let typer = DefaultTyper::default();
        let dataset = Dataset::read_file("datasets/sales-10-weird.csv", options, &typer).await?;

        let schema = dataset.schema;
        let header = dataset.header;
        let columns = dataset.columns;

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
        assert_eq!(columns.columns.len(), 14);

        for column in columns.columns {
            let all_good = column.values.iter().all(|v| v.is_some());
            assert!(all_good, "The column has invalid values! {:?}", column)
        }

        Ok(())
    }
}
