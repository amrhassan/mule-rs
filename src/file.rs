use crate::errors::Result;
use futures_core::stream::TryStream;
use std::path::Path;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio_stream::wrappers::LinesStream;
use tokio_stream::StreamExt;

/// Open and count the lines in a file
pub async fn count_lines(path: impl AsRef<Path>) -> Result<usize> {
    let lines = read_lines(path).await?;
    let count = lines.fold(0, |acc, _| acc + 1).await;
    Ok(count)
}

pub async fn read_lines(path: impl AsRef<Path>) -> Result<impl TryStream<Item = Result<String>>> {
    let reader = File::open(path).await?;
    let buff = BufReader::new(reader);
    let stream = LinesStream::new(buff.lines());
    Ok(stream.map(|res| Ok(res?)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_count_lines() -> Result<()> {
        assert_eq!(count_lines("datasets/sales-10.csv").await?, 10);
        assert_eq!(count_lines("datasets/sales-100.tsv").await?, 100);
        Ok(())
    }
}
