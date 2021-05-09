use crate::errors::Result;
use crate::{dataset_file::DatasetFile, lexer::Record};
use futures_core::stream::TryStream;
use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};
use tokio_stream::StreamExt;

pub struct DatasetBatch {
    file_path: PathBuf,
    records: RangeInclusive<usize>,
}

impl DatasetBatch {
    pub fn new(file_path: impl AsRef<Path>, record_range: RangeInclusive<usize>) -> DatasetBatch {
        DatasetBatch {
            file_path: file_path.as_ref().to_path_buf(),
            records: record_range,
        }
    }

    /// Read records from this batch
    pub async fn read_records(&self) -> Result<impl TryStream<Item = Result<Record>>> {
        let s = DatasetFile::new(&self.file_path)
            .read_records()
            .await?
            .skip(*self.records.start())
            .take(self.get_row_count());
        Ok(s)
    }

    #[tokio::main(flavor = "current_thread")]
    pub async fn read_records_blocking(self) -> Result<impl TryStream<Item = Result<Record>>> {
        let l = self.read_records().await?;
        Ok(l)
    }

    pub fn get_row_count(&self) -> usize {
        self.records.end() - self.records.start() + 1
    }
}
