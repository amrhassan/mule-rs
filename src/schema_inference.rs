use crate::errors::Result;
use itertools::Itertools;
use std::collections::HashMap;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};

/// Detects the separator used the most in the given reader from the common separators
pub async fn detect_separator(reader: impl AsyncRead + Unpin) -> Result<String> {
    static COMMON_SEPARATORS: [&'static str; 3] = [",", "\t", "|"];

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
