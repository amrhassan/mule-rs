use super::line_parser::LineParser;
use crate::errors::Result;
use std::{
    fs::File,
    io::{BufRead, BufReader, Lines, Read},
    path::Path,
};

/// Parser of CSV byte streams
pub struct CsvParser<'a, R> {
    lines: Lines<BufReader<R>>,
    separator: &'a str,
    text_quote: &'a str,
    text_quote_escape: &'a str,
}

impl<'a, R: Read> CsvParser<'a, R> {
    pub fn open_path(
        path: impl AsRef<Path>,
        separator: &'a str,
        text_quote: &'a str,
        text_quote_escape: &'a str,
    ) -> Result<CsvParser<'a, File>> {
        CsvParser::from_reader(File::open(path)?, separator, text_quote, text_quote_escape)
    }

    pub fn from_reader(
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
}

impl<'a, R: Read> Iterator for CsvParser<'a, R> {
    type Item = Result<LineParser<'a>>;
    fn next(&mut self) -> Option<Self::Item> {
        match self.lines.next() {
            Some(Ok(line)) => Some(Ok(LineParser::new(
                line,
                self.separator,
                self.text_quote,
                self.text_quote_escape,
            ))),
            Some(Err(err)) => Some(Err(err.into())),
            None => None,
        }
    }
}
