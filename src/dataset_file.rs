use crate::{dataset_batch::DatasetBatch, errors::Result};
use futures_core::stream::TryStream;
use itertools::Itertools;
use std::path::{Path, PathBuf};
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio_stream::wrappers::LinesStream;
use tokio_stream::StreamExt;

#[derive(Clone)]
pub struct DatasetFile {
    path: PathBuf,
}

impl DatasetFile {
    pub fn new(path: impl AsRef<Path>) -> DatasetFile {
        DatasetFile {
            path: path.as_ref().to_owned(),
        }
    }

    /// Open and count the lines in a file
    pub async fn count_lines(&self) -> Result<usize> {
        let lines = self.read_lines().await?;
        let count = lines.fold(0, |acc, _| acc + 1).await;
        Ok(count)
    }

    #[tokio::main(flavor = "current_thread")]
    pub async fn count_lines_blocking(&self) -> Result<usize> {
        self.count_lines().await
    }

    pub async fn read_lines(&self) -> Result<impl TryStream<Item = Result<String>>> {
        let reader = File::open(&self.path).await?;
        let buff = BufReader::new(reader);
        let stream = LinesStream::new(buff.lines());
        Ok(stream.map(|res| Ok(res?)))
    }

    #[tokio::main(flavor = "current_thread")]
    pub async fn read_lines_blocking(&self) -> Result<impl TryStream<Item = Result<String>>> {
        self.read_lines().await
    }

    /// Break the range of file content into batches
    pub async fn batches(
        &self,
        skip_header: bool,
        lines_to_read: LinesToRead,
        batch_count: usize,
    ) -> Result<Vec<DatasetBatch>> {
        let lines_to_skip = if skip_header { 1 } else { 0 };

        let line_count = match lines_to_read {
            LinesToRead::All => self.count_lines().await?,
            LinesToRead::Absolute(n) => n,
            LinesToRead::Percentage(p) => (self.count_lines().await? as f64 * p).floor() as usize,
        };

        let batch_size = line_count / batch_count;

        let start = lines_to_skip;
        let stop = line_count;

        if (stop - start) < 2 {
            return Ok(vec![DatasetBatch::new(&self.path, start..=stop)]);
        }

        let batches = (start..stop)
            .chunks(batch_size)
            .into_iter()
            .map(|mut chunk| {
                let start = chunk.next().unwrap_or(lines_to_skip);
                let stop = chunk.last().unwrap_or(start);
                DatasetBatch::new(&self.path, start..=stop)
            })
            .collect();

        Ok(batches)
    }

    /// Break the range of file content into batches
    #[tokio::main(flavor = "current_thread")]
    pub async fn batches_blocking(
        &self,
        skip_header: bool,
        lines_to_read: LinesToRead,
        batch_count: usize,
    ) -> Result<Vec<DatasetBatch>> {
        self.batches(skip_header, lines_to_read, batch_count).await
    }
}

pub enum LinesToRead {
    All,
    Absolute(usize),
    Percentage(f64),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_count_lines() -> Result<()> {
        assert_eq!(
            DatasetFile::new("datasets/sales-10.csv")
                .count_lines()
                .await?,
            10
        );
        assert_eq!(
            DatasetFile::new("datasets/sales-100.tsv")
                .count_lines()
                .await?,
            100
        );
        Ok(())
    }
}
