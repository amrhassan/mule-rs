use crate::dataset_file::DatasetFile;
use crate::errors::Result;
use futures_core::stream::TryStream;
use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};
use tokio_stream::StreamExt;

pub struct DatasetBatch {
    file_path: PathBuf,
    lines: RangeInclusive<usize>,
}

impl DatasetBatch {
    pub fn new(file_path: impl AsRef<Path>, line_range: RangeInclusive<usize>) -> DatasetBatch {
        DatasetBatch {
            file_path: file_path.as_ref().to_path_buf(),
            lines: line_range,
        }
    }

    /// Read lines from this batch
    pub async fn read_lines(&self) -> Result<impl TryStream<Item = Result<String>>> {
        let s = DatasetFile::new(&self.file_path)
            .read_lines()
            .await?
            .skip(*self.lines.start())
            .take(self.get_row_count());
        Ok(s)
    }

    #[tokio::main(flavor = "current_thread")]
    pub async fn read_lines_blocking(self) -> Result<impl TryStream<Item = Result<String>>> {
        let l = self.read_lines().await?;
        Ok(l)
    }

    pub fn get_row_count(&self) -> usize {
        self.lines.end() - self.lines.start() + 1
    }
}
