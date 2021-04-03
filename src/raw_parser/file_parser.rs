use super::line_parser::LineParser;
use crate::errors::Result;
use std::path::Path;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader, Lines};

/// Parser of CSV byte streams
pub struct CsvParser<'a, R> {
    lines: Lines<BufReader<R>>,
    separator: &'a str,
    text_quote: &'a str,
    text_quote_escape: &'a str,
}

impl<'a> CsvParser<'a, File> {
    pub async fn open_path(
        path: impl AsRef<Path>,
        separator: &'a str,
        text_quote: &'a str,
        text_quote_escape: &'a str,
    ) -> Result<CsvParser<'a, File>> {
        let parser = CsvParser::from_reader(
            File::open(path).await?,
            separator,
            text_quote,
            text_quote_escape,
        )
        .await?;
        Ok(parser)
    }
}

impl<'a, R: AsyncRead + Unpin> CsvParser<'a, R> {
    pub async fn from_reader(
        reader: R,
        separator: &'a str,
        text_quote: &'a str,
        text_quote_escape: &'a str,
    ) -> Result<CsvParser<'a, R>> {
        let lines = BufReader::new(reader).lines();
        Ok(CsvParser {
            lines,
            separator,
            text_quote,
            text_quote_escape,
        })
    }

    pub async fn next_line(&mut self) -> Result<Option<LineParser<'a>>> {
        let parser = self.lines.next_line().await?.map(|line| {
            LineParser::new(
                line,
                self.separator,
                self.text_quote,
                self.text_quote_escape,
            )
        });
        Ok(parser)
    }
}
