use crate::{
    dataset_batch::DatasetBatch,
    errors::Result,
    lexer::{Record, RecordLexer},
};
use futures_core::stream::TryStream;
use itertools::Itertools;
use std::path::{Path, PathBuf};
use tokio::fs::File;
use tokio::io::BufReader;
use tokio_stream::StreamExt;
use tokio_util::codec::FramedRead;

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

    /// Open and count the records in a file
    pub async fn count_records(&self) -> Result<usize> {
        let records = self.read_records().await?;
        let count = records.fold(0, |acc, _| acc + 1).await;
        Ok(count)
    }

    #[tokio::main(flavor = "current_thread")]
    pub async fn count_records_blocking(&self) -> Result<usize> {
        self.count_records().await
    }

    pub async fn read_records(&self) -> Result<impl TryStream<Item = Result<Record>>> {
        let reader = File::open(&self.path).await?;
        let buff = BufReader::new(reader);
        let record_decoder = RecordLexer::new(crate::lexer::TextEncoding::Utf8);
        let stream = FramedRead::new(buff, record_decoder);
        Ok(stream.map(|res| Ok(res?)))
    }

    #[tokio::main(flavor = "current_thread")]
    pub async fn read_records_blocking(&self) -> Result<impl TryStream<Item = Result<Record>>> {
        self.read_records().await
    }

    /// Break the range of file content into batches
    pub async fn batches(
        &self,
        skip_header: bool,
        records_to_read: RecordsToRead,
        batch_count: usize,
    ) -> Result<Vec<DatasetBatch>> {
        let records_to_skip = if skip_header { 1 } else { 0 };

        let record_count = match records_to_read {
            RecordsToRead::All => self.count_records().await?,
            RecordsToRead::Absolute(n) => n,
            RecordsToRead::Percentage(p) => {
                (self.count_records().await? as f64 * p).floor() as usize
            }
        };

        let batch_size = record_count / batch_count;

        let start = records_to_skip;
        let stop = record_count;

        if (stop - start) < 2 {
            return Ok(vec![DatasetBatch::new(&self.path, start..=stop)]);
        }

        let batches = (start..stop)
            .chunks(batch_size)
            .into_iter()
            .map(|mut chunk| {
                let start = chunk.next().unwrap_or(records_to_skip);
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
        records_to_read: RecordsToRead,
        batch_count: usize,
    ) -> Result<Vec<DatasetBatch>> {
        self.batches(skip_header, records_to_read, batch_count)
            .await
    }
}

pub enum RecordsToRead {
    All,
    Absolute(usize),
    Percentage(f64),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_count_records() -> Result<()> {
        assert_eq!(
            DatasetFile::new("datasets/sales-10.csv")
                .count_records()
                .await?,
            10
        );
        assert_eq!(
            DatasetFile::new("datasets/sales-100.tsv")
                .count_records()
                .await?,
            100
        );
        Ok(())
    }
}
