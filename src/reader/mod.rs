use crate::errors::Result;
use async_stream::try_stream;
use futures_core::stream::Stream;
use std::path::Path;
use tokio::io::{AsyncRead, BufReader};
use tokio::{fs::File, io::AsyncBufReadExt};
use tokio_stream::wrappers::LinesStream;
use tokio_stream::StreamExt;

pub struct CsvReader<R> {
    lines: LinesStream<BufReader<R>>,
}

impl<R: AsyncRead + Unpin> CsvReader<R> {
    pub fn from_reader(reader: R) -> CsvReader<R> {
        let buffer = BufReader::new(reader);
        let lines = LinesStream::new(buffer.lines());
        CsvReader { lines }
    }

    pub async fn from_path(path: impl AsRef<Path>) -> Result<CsvReader<File>> {
        let file = File::open(path).await?;
        Ok(CsvReader::from_reader(file))
    }

    pub fn values(
        mut self,
        separator: String,
        text_quote: String,
        text_quote_escape: String,
    ) -> impl Stream<Item = Result<String>> {
        try_stream! {
            while let Some(line) = self.lines.next().await {
                for value in LineValues::new(line?, &separator, &text_quote, &text_quote_escape) {
                    yield value;
                }
            }
        }
    }
}

struct LineValues {
    line: String,
    separator: String,
    text_quote: String,
    text_quote_escape: String,
    next_start: usize,
    next_continue: usize,
}

impl LineValues {
    fn new(line: String, separator: &str, text_quote: &str, text_quote_escape: &str) -> LineValues {
        LineValues {
            line,
            separator: separator.to_string(),
            text_quote: text_quote.to_string(),
            text_quote_escape: text_quote_escape.to_string(),
            next_start: 0,
            next_continue: 0,
        }
    }
}

impl Iterator for LineValues {
    type Item = String;
    fn next(&mut self) -> Option<Self::Item> {
        if self.next_start > self.line.len() || self.line.is_empty() {
            return None;
        }
        let sub_line = &self.line[self.next_continue..];
        if let Some(sep_ix_rel) = sub_line.find(&self.separator) {
            let sep_ix = sep_ix_rel + self.next_continue;
            let value = self.line[self.next_start..sep_ix].to_string();
            self.next_start = sep_ix + self.separator.len();
            self.next_continue = self.next_start;
            Some(value)
        } else {
            let final_value = self.line[self.next_start..].to_string();
            self.next_start = self.line.len() + 1;
            Some(final_value)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_line_values_1() {
        let line = "first, second,,three,4,,,".to_string();
        let values: Vec<String> = LineValues::new(line, ",", "\"", "\\").collect();

        assert_eq!(
            values,
            vec!["first", " second", "", "three", "4", "", "", ""]
        )
    }

    #[test]
    fn test_line_values_2() {
        let line = "first, second,,three,4,,,five".to_string();
        let values: Vec<String> = LineValues::new(line, ",", "\"", "\\").collect();

        assert_eq!(
            values,
            vec!["first", " second", "", "three", "4", "", "", "five"]
        )
    }

    #[test]
    fn test_line_values_3() {
        let line = "first,, second,,,,three,,4,,,,,,".to_string();
        let values: Vec<String> = LineValues::new(line, ",,", "\"", "\\").collect();

        assert_eq!(
            values,
            vec!["first", " second", "", "three", "4", "", "", ""]
        )
    }

    //     #[test]
    //     fn test_line_values_4() {
    //         let line = "first, second,,three,4,\"\",,five".to_string();
    //         let values: Vec<String> = LineValues::new(line, ",", "\"", "\\").collect();

    //         assert_eq!(
    //             values,
    //             vec!["first", " second", "", "three", "4", "", "", "five"]
    //         )
    //     }
}
