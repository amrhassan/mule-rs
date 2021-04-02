mod line_parser;

// use crate::errors::Result;
// use async_stream::try_stream;
// use futures_core::stream::Stream;
// use line_parser::LineParser;
// use std::path::Path;
// use tokio::io::{AsyncRead, BufReader};
// use tokio::{fs::File, io::AsyncBufReadExt};
// use tokio_stream::wrappers::LinesStream;
// use tokio_stream::StreamExt;

// pub struct CsvReader<R> {
//     lines: LinesStream<BufReader<R>>,
// }

// impl<R: AsyncRead + Unpin> CsvReader<R> {
//     pub fn from_reader(reader: R) -> CsvReader<R> {
//         let buffer = BufReader::new(reader);
//         let lines = LinesStream::new(buffer.lines());
//         CsvReader { lines }
//     }

//     pub async fn from_path(path: impl AsRef<Path>) -> Result<CsvReader<File>> {
//         let file = File::open(path).await?;
//         Ok(CsvReader::from_reader(file))
//     }

//     // pub fn values(
//     //     mut self,
//     //     separator: String,
//     //     text_quote: String,
//     //     text_quote_escape: String,
//     // ) -> impl Stream<Item = Result<String>> {
//     //     try_stream! {
//     //         while let Some(line) = self.lines.next().await {
//     //             for value in LineParser::new(line?, &separator, &text_quote, &text_quote_escape) {
//     //                 yield value;
//     //             }
//     //         }
//     //     }
//     // }
// }
