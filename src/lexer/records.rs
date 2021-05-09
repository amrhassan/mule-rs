use bytes::buf::Buf;
use bytes::BytesMut;
use derive_more::Display;
use std::str::Utf8Error;
use thiserror::Error;
use tokio_util::codec::Decoder;

#[derive(Clone, Debug)]
pub struct Record(String);

impl AsRef<str> for Record {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<&str> for Record {
    fn from(s: &str) -> Self {
        Record(s.to_string())
    }
}

impl Record {
    pub fn len(&self) -> usize {
        self.as_ref().len()
    }

    pub fn is_empty(&self) -> bool {
        self.as_ref().is_empty()
    }
}

#[derive(Display, Error, Debug)]
pub enum RecordLexerError {
    Io(#[from] std::io::Error),
    Encoding(#[from] Utf8Error),
}

pub type Result<T> = std::result::Result<T, RecordLexerError>;

/// A lexer of  RFC-4180 CSV records from UTF-8 bytes
pub struct RecordLexer {
    text_encoding: TextEncoding,
}

impl RecordLexer {
    pub fn new(text_encoding: TextEncoding) -> RecordLexer {
        RecordLexer { text_encoding }
    }
}

#[derive(Clone, Copy)]
pub enum TextEncoding {
    Utf8,
}

impl TextEncoding {
    fn decode(self, bytes: &[u8]) -> Result<&str> {
        match self {
            TextEncoding::Utf8 => Ok(std::str::from_utf8(bytes)?),
        }
    }

    fn char_byte_count(self, c: char) -> usize {
        match self {
            TextEncoding::Utf8 => char::len_utf8(c),
        }
    }

    fn str_byte_count(self, s: &str) -> usize {
        s.chars().map(|c| self.char_byte_count(c)).sum()
    }
}

static DOUBLE_QUOTES: char = '"';
static CR: char = '\r';
static LF: char = '\n';

impl Decoder for RecordLexer {
    type Item = Record;
    type Error = RecordLexerError;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        let text = self.text_encoding.decode(src.as_ref())?;
        let mut double_quote_balance = 0;
        let mut previous_c = '\0';
        for (char_index, c) in text.chars().enumerate() {
            if c == DOUBLE_QUOTES {
                double_quote_balance = (double_quote_balance + 1) % 2;
            } else if c == LF && double_quote_balance == 0 {
                let delimiter = if previous_c == CR { "\r\n" } else { "\n" };
                let delimiter_char_count = delimiter.chars().count();
                let delimiter_byte_count = self.text_encoding.str_byte_count(&delimiter);
                let record_char_count = char_index - (delimiter_char_count - 1);
                let record = Record(text.chars().take(record_char_count).collect());
                let record_byte_count = TextEncoding::Utf8.str_byte_count(&record.0);
                src.advance(record_byte_count + delimiter_byte_count);
                return Ok(Some(record));
            }
            previous_c = c;
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_stream::StreamExt;
    use tokio_util::codec::FramedRead;

    #[tokio::test]
    async fn test_frames_simple_records() -> Result<()> {
        let source = "name,age,gender\nname_1, 3, F \r\n name_2,5,X\n".to_string();
        let decoder = RecordLexer::new(TextEncoding::Utf8);

        let mut framed_reader = FramedRead::new(source.as_bytes(), decoder);

        assert_eq!("name,age,gender", framed_reader.next().await.unwrap()?.0);
        assert_eq!("name_1, 3, F ", framed_reader.next().await.unwrap()?.0);
        assert_eq!(" name_2,5,X", framed_reader.next().await.unwrap()?.0);

        Ok(())
    }

    #[tokio::test]
    async fn test_frames_records_with_quotes() -> Result<()> {
        let source = "name,age,gender\n\"name_1\", 3, F \r\n \"name 2\",5,X\n".to_string();
        let decoder = RecordLexer::new(TextEncoding::Utf8);

        let mut framed_reader = FramedRead::new(source.as_bytes(), decoder);

        assert_eq!("name,age,gender", framed_reader.next().await.unwrap()?.0);
        assert_eq!("\"name_1\", 3, F ", framed_reader.next().await.unwrap()?.0);
        assert_eq!(" \"name 2\",5,X", framed_reader.next().await.unwrap()?.0);

        Ok(())
    }

    #[tokio::test]
    async fn test_frames_records_with_quoted_newrecords() -> Result<()> {
        let source = "name,age,gender\n\"name\n1\", 3, F \r\n \"name \r\n 2\",5,X\n".to_string();
        let decoder = RecordLexer::new(TextEncoding::Utf8);

        let mut framed_reader = FramedRead::new(source.as_bytes(), decoder);

        assert_eq!("name,age,gender", framed_reader.next().await.unwrap()?.0);
        assert_eq!("\"name\n1\", 3, F ", framed_reader.next().await.unwrap()?.0);
        assert_eq!(
            " \"name \r\n 2\",5,X",
            framed_reader.next().await.unwrap()?.0
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_frames_records_with_quoted_quotes() -> Result<()> {
        let source =
            "name,\"age \"\"biological\"\"\",gender\n\"name\n1\", 3, F \r\n \"name \r\n 2\",5,X\n"
                .to_string();
        let decoder = RecordLexer::new(TextEncoding::Utf8);

        let mut framed_reader = FramedRead::new(source.as_bytes(), decoder);

        assert_eq!(
            "name,\"age \"\"biological\"\"\",gender",
            framed_reader.next().await.unwrap()?.0
        );
        assert_eq!("\"name\n1\", 3, F ", framed_reader.next().await.unwrap()?.0);
        assert_eq!(
            " \"name \r\n 2\",5,X",
            framed_reader.next().await.unwrap()?.0
        );

        Ok(())
    }
}
