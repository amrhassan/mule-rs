use crate::errors::Result;
use futures_core::stream::TryStream;
use itertools::Itertools;
use std::path::{Path, PathBuf};
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

/// A batch of lines from a local file
pub struct LineBatch {
    file_path: PathBuf,
    skip_lines: usize,
    take_lines: usize,
}

impl LineBatch {
    /// Determine the batches of lines from a local file
    pub fn prepare_line_batches(
        file_path: impl AsRef<Path>,
        lines_to_skip: usize,
        lines_to_read: usize,
        batch_count: usize,
    ) -> Vec<LineBatch> {
        let batch_size = lines_to_read / batch_count;
        let start = lines_to_skip;
        let stop = lines_to_skip + lines_to_read;
        (start..stop)
            .chunks(batch_size)
            .into_iter()
            .map(move |mut chunk| {
                let start_inc = chunk.next().unwrap_or(0);
                let stop_inc = chunk.last().unwrap_or(usize::MAX);
                let skip_lines = start_inc;
                let take_lines = stop_inc - start_inc;
                LineBatch {
                    file_path: file_path.as_ref().to_owned(),
                    skip_lines,
                    take_lines,
                }
            })
            .collect()
    }

    /// Read lines from this batch
    pub async fn read_lines(self) -> Result<impl TryStream<Item = Result<String>>> {
        let s = read_lines(self.file_path)
            .await?
            .skip(self.skip_lines)
            .take(self.take_lines);
        Ok(s)
    }

    #[tokio::main(flavor = "current_thread")]
    pub async fn read_lines_blocking(self) -> Result<impl TryStream<Item = Result<String>>> {
        let l = self.read_lines().await?;
        Ok(l)
    }
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
