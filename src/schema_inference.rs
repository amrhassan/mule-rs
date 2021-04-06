use super::errors::Result;
use super::raw_parser::{ColumnValue, LineParser};
use super::typer::Typer;
use crate::errors::MuleError;
use itertools::Itertools;
use maplit::hashmap;
use std::collections::HashMap;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};

static COMMON_SEPARATORS: [&str; 3] = [",", "\t", "|"];

/// Detects the separator used the most in the given reader from the common separators
pub async fn infer_separator(reader: impl AsyncRead + Unpin) -> Result<String> {
    let mut counts: HashMap<&str, usize> = HashMap::default();

    let mut lines = BufReader::new(reader).lines();
    while let Some(line) = lines.next_line().await? {
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

pub async fn read_column_names(
    reader: impl AsyncRead + Unpin,
    separator: &str,
    text_quote: &str,
    text_quote_escape: &str,
) -> Result<Option<Vec<String>>> {
    let names = BufReader::new(reader)
        .lines()
        .next_line()
        .await?
        .map(|line| {
            let names = LineParser::new(line, separator, text_quote, text_quote_escape);
            names.map_into().collect_vec()
        });

    Ok(names)
}

pub async fn count_lines(reader: impl AsyncRead + Unpin) -> Result<usize> {
    let mut count = 0;
    let mut lines = BufReader::new(reader).lines();
    while let Some(_) = lines.next_line().await? {
        count += 1;
    }
    Ok(count)
}

pub async fn infer_column_types<T: Typer>(
    reader: impl AsyncRead + Unpin,
    skip_first_row: bool,
    inference_depth: usize,
    separator: &str,
    text_quote: &str,
    text_quote_escape: &str,
    typer: T,
) -> Result<Vec<T::TypeTag>> {
    let mut lines = BufReader::new(reader).lines();

    if skip_first_row {
        let _ = lines.next_line().await?;
    }

    let mut column_freqs: Vec<HashMap<T::TypeTag, usize>> = Vec::new();

    let mut count = 0;
    while let Some(line) = lines.next_line().await? {
        if count > inference_depth {
            break;
        }
        count += 1;
        let line_values = LineParser::new(line, separator, text_quote, text_quote_escape);
        for (ix, val) in line_values.enumerate() {
            if let ColumnValue::Some(typed_val) = typer.type_value(&val) {
                let type_tag = typer.tag_type(&typed_val);
                match column_freqs.get_mut(ix) {
                    Some(counts) => *counts.entry(type_tag).or_default() += 1,
                    None => column_freqs.push(hashmap! {type_tag => 1}),
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
    use crate::{DefaultTyper, ValueType};
    use itertools::Itertools;
    use tokio::fs::File;

    #[tokio::test]
    pub async fn test_separator_inference() -> Result<()> {
        let tsv_file = File::open("datasets/sales-100.tsv").await?;
        assert_eq!(infer_separator(tsv_file).await?, "\t");

        let csv_file = File::open("datasets/sales-100.csv").await?;
        assert_eq!(infer_separator(csv_file).await.unwrap(), ",");

        Ok(())
    }

    #[tokio::test]
    pub async fn test_infer_column_types_sales_100() -> Result<()> {
        let csv_file = File::open("datasets/sales-100.csv").await?;

        let typer = DefaultTyper::default();
        let column_types = infer_column_types(csv_file, true, 200, ",", "\"", "\\", typer).await?;

        let expected = vec![
            ValueType::Text,
            ValueType::Text,
            ValueType::Text,
            ValueType::Text,
            ValueType::Text,
            ValueType::Text,
            ValueType::Int,
            ValueType::Text,
            ValueType::Int,
            ValueType::Float,
            ValueType::Float,
            ValueType::Float,
            ValueType::Float,
            ValueType::Float,
        ];

        assert_eq!(expected, column_types);

        Ok(())
    }

    #[tokio::test]
    pub async fn test_read_colum_names_sales_100() -> Result<()> {
        let csv_file = File::open("datasets/sales-100.csv").await?;

        let column_names = read_column_names(csv_file, ",", "\"", "\\").await?;

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
