use crate::errors::Result;
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
    pub fn batches(
        &self,
        skip_header: bool,
        lines_to_read: usize,
        batch_count: usize,
    ) -> Vec<LineBatch> {
        let lines_to_skip = if skip_header { 1 } else { 0 };
        let batch_size = lines_to_read / batch_count;
        let start = lines_to_skip;
        let stop = lines_to_skip + lines_to_read;

        if (stop - start) < 2 {
            return vec![LineBatch {
                file_path: self.path.clone(),
                skip_lines: lines_to_skip,
                take_lines: 0,
            }];
        }

        (start..stop)
            .chunks(batch_size)
            .into_iter()
            .map(|mut chunk| {
                let start_inc = chunk.next().unwrap_or(0);
                let stop_inc = chunk.last().unwrap_or(start_inc);
                let skip_lines = start_inc;
                let take_lines = stop_inc - start_inc;
                LineBatch {
                    file_path: self.path.clone(),
                    skip_lines,
                    take_lines,
                }
            })
            .collect()
    }
}

/// A batch of lines from a local file
pub struct LineBatch {
    file_path: PathBuf,
    skip_lines: usize,
    take_lines: usize,
}

impl LineBatch {
    /// Read lines from this batch
    pub async fn read_lines(&self) -> Result<impl TryStream<Item = Result<String>>> {
        let s = DatasetFile::new(&self.file_path)
            .read_lines()
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

    pub fn get_row_count(&self) -> usize {
        self.take_lines
    }
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
