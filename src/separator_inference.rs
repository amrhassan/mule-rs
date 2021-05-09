use crate::dataset_file::DatasetFile;
use crate::errors::Result;
use itertools::Itertools;
use std::collections::HashMap;
use std::path::Path;
use tokio_stream::StreamExt;

static COMMON_SEPARATORS: [&str; 3] = [",", "\t", "|"];

/// Infer the separator as the most commonly used separator in the file
pub async fn infer_separator(path: impl AsRef<Path>) -> Result<String> {
    let mut counts: HashMap<&str, usize> = HashMap::default();
    let mut records = DatasetFile::new(path).read_records().await?;
    while let Some(record_res) = records.next().await {
        let record = record_res?;
        for sep in COMMON_SEPARATORS.iter() {
            *counts.entry(sep).or_default() += record.as_ref().matches(sep).count();
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

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    pub async fn test_separator_inference() -> Result<()> {
        assert_eq!(infer_separator("datasets/sales-100.tsv").await?, "\t");
        assert_eq!(
            infer_separator("datasets/sales-100.csv").await.unwrap(),
            ","
        );

        Ok(())
    }
}
