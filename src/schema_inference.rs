use crate::errors::MuleError;

use super::errors::Result;
use super::raw_parser::LineParser;
use super::typer::Typer;
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

pub async fn infer_column_types<T: Typer>(
    reader: impl AsyncRead + Unpin,
    skip_first_row: bool,
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

    while let Some(line) = lines.next_line().await? {
        let line_values = LineParser::new(line, separator, text_quote, text_quote_escape);
        for (ix, val) in line_values.enumerate() {
            let typed_value = typer.type_raw_value(&val);
            let type_tag = typer.tag_typed_value(&typed_value);

            match column_freqs.get_mut(ix) {
                Some(counts) => *counts.entry(type_tag).or_default() += 1,
                None => column_freqs.push(hashmap! {type_tag => 1}),
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
