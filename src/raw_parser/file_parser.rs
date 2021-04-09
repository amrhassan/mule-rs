use super::line_parser::LineParser;
use super::value_parser::ColumnValue;
use super::ParsingOptions;
use crate::errors::Result;
use crate::file;
use crate::Typer;
use itertools::Itertools;
use std::path::Path;
use tokio_stream::StreamExt;

pub async fn read_file_data<T: Typer>(
    file_path: impl AsRef<Path>,
    schema: &[T::TypeTag],
    options: &ParsingOptions,
    line_count: usize,
    skip_first_line: bool,
    typer: &T,
) -> Result<Vec<Vec<ColumnValue<T::TypedValue>>>> {
    let mut data: Vec<Vec<ColumnValue<T::TypedValue>>> =
        vec![Vec::with_capacity(line_count); schema.len()];
    let lines_to_skip = if skip_first_line { 1 } else { 0 };
    let mut lines = file::read_lines(file_path).await?.skip(lines_to_skip);
    while let Some(line_res) = lines.next().await {
        let line = line_res?;
        let line_values = LineParser::new(line, options);
        for (col_ix, (value, column_type)) in line_values.zip(schema.iter()).enumerate() {
            let column_value = typer.type_value_as(&value, *column_type);
            data[col_ix].push(column_value);
        }
    }
    Ok(data)
}

pub async fn read_file_column_names(
    path: impl AsRef<Path>,
    options: &ParsingOptions,
) -> Result<Option<Vec<String>>> {
    let names = file::read_lines(path).await?.try_next().await?.map(|line| {
        let names = LineParser::new(line, options);
        names.map_into().collect_vec()
    });

    Ok(names)
}

#[cfg(test)]
mod test {
    use super::*;
    use itertools::Itertools;

    #[tokio::test]
    pub async fn test_read_colum_names_sales_100() -> Result<()> {
        let options = ParsingOptions {
            text_quote: "\"".to_string(),
            separator: ",".to_string(),
            text_quote_escape: "\\".to_string(),
        };
        let column_names = read_file_column_names("datasets/sales-100.csv", &options).await?;

        let expected = vec![
            "Region",
            "Country",
            "Item Type",
            "Sales Channel",
            "Order Priority",
            "Order Date",
            "Order ID",
            "Ship Date",
            "Units Sold",
            "Unit Price",
            "Unit Cost",
            "Total Revenue",
            "Total Cost",
            "Total Profit",
        ]
        .into_iter()
        .map_into()
        .collect_vec();

        assert_eq!(Some(expected), column_names);

        Ok(())
    }
}
