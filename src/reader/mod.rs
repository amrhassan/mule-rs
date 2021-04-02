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

struct Value<'a> {
    raw: String,
    text_quote: &'a str,
}

fn unquoted_value(raw: String, text_quote: &str, text_quote_escape: &str) -> String {
    let quote_l = raw.find(text_quote);
    let quote_r = raw.rfind(text_quote);
    match (quote_l, quote_r) {
        (Some(ix_l), Some(ix_r)) if ix_l < ix_r => raw[ix_l + text_quote.len()..ix_r].replace(text_quote_escape, ""),
        _ => raw,
    }
}

struct LineValues {
    line: String,
    separator: String,
    text_quote: String,
    text_quote_escape: String,
    next_start: usize,
}

impl LineValues {
    fn new(line: String, separator: &str, text_quote: &str, text_quote_escape: &str) -> LineValues {
        LineValues {
            line,
            separator: separator.to_string(),
            text_quote: text_quote.to_string(),
            text_quote_escape: text_quote_escape.to_string(),
            next_start: 0,
        }
    }
}

impl LineValues {
    fn remaining(&self) -> &str {
        self.start_from(self.next_start)
    }

    fn start_from(&self, ix: usize) -> &str {
        &self.line[ix..]
    }

    fn next_separator_ix(&self) -> Option<usize> {
        self.remaining()
            .find(&self.separator)
            .map(|ix| ix + self.next_start)
    }

    fn next_quote_ix(&self) -> Option<usize> {
        self.remaining()
            .find(&self.text_quote)
            .map(|ix| ix + self.next_start)
    }

    fn subsequent_qoute_ix(&self, first_quote_ix: usize) -> Option<usize> {
        self.start_from(first_quote_ix + self.text_quote.len())
            .find(&self.text_quote)
            .map(|ix| ix + first_quote_ix + self.text_quote.len())
    }

    fn parse_unquoted(&self) -> Option<(String, usize)> {
        let end = self.next_separator_ix().unwrap_or(self.line.len());
        self.parse_to(end)
    }

    fn parse_quoted(&self) -> Option<(String, usize)> {
        let quote_l = match self.next_quote_ix() {
            Some(ix) => ix,
            None => return self.parse_unquoted(),
        };
        let mut quote_r = match self.subsequent_qoute_ix(quote_l) {
            Some(ix) => ix,
            None => return self.parse_unquoted(),
        };

        while self.line[quote_r - self.text_quote_escape.len()..quote_r] == self.text_quote_escape {
            let new_quote_r = match self.subsequent_qoute_ix(quote_r) {
                Some(ix) => ix,
                None => return self.parse_unquoted(),
            };
            quote_r = dbg!(new_quote_r);
        }

        let end = quote_r + self.text_quote.len();
        self.parse_to(end)
    }

    fn parse_to(&self, end: usize) -> Option<(String, usize)> {
        let value = self.line[self.next_start..end].to_string();
        Some((value, end + self.separator.len()))
    }
}

impl Iterator for LineValues {
    type Item = String;
    fn next(&mut self) -> Option<Self::Item> {
        if self.next_start > self.line.len() || self.line.is_empty() {
            return None;
        }
        let parsed = if self.next_separator_ix() < self.next_quote_ix() {
            self.parse_unquoted()
        } else {
            self.parse_quoted()
        };

        if let Some((value, next_start)) = parsed {
            self.next_start = next_start;
            Some(unquoted_value(
                value,
                &self.text_quote,
                &self.text_quote_escape,
            ))
        } else {
            None
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

    #[test]
    fn test_line_values_4() {
        let line = "first, second,,three,4,\"\",,five".to_string();
        let values: Vec<String> = LineValues::new(line, ",", "\"", "\\").collect();

        assert_eq!(
            values,
            vec!["first", " second", "", "three", "4", "", "", "five"]
        )
    }

    #[test]
    fn test_line_values_5() {
        let line = "first, \"second point five\",,three,4,\"\",,five".to_string();
        let values: Vec<String> = LineValues::new(line, ",", "\"", "\\").collect();

        assert_eq!(
            values,
            vec![
                "first",
                "second point five",
                "",
                "three",
                "4",
                "",
                "",
                "five"
            ]
        )
    }

    #[test]
    fn test_line_values_6() {
        let line = "first, \"second \\\" point five\",,three,4,\"\",,five".to_string();
        let values: Vec<String> = LineValues::new(line, ",", "\"", "\\").collect();

        assert_eq!(
            values,
            vec![
                "first",
                "second \" point five",
                "",
                "three",
                "4",
                "",
                "",
                "five"
            ]
        )
    }

    #[test]
    fn test_line_values_7() {
        let line = "first, \"second \\\" \\\" point five\",,three,4,\"\",,five".to_string();
        let values: Vec<String> = LineValues::new(line, ",", "\"", "\\").collect();

        assert_eq!(
            values,
            vec![
                "first",
                "second \" \" point five",
                "",
                "three",
                "4",
                "",
                "",
                "five"
            ]
        )
    }
}
