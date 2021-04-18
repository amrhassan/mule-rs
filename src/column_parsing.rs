// use crate::errors::Result;
// use crate::file;
// use crate::line_parsing::{LineParser, LineParsingOptions};
// use crate::schema::Schema;
// use crate::value_parsing::Parsed;
// use crate::Typer;
// use std::path::Path;
// use tokio_stream::StreamExt;

// pub struct Columns<T: Typer>(Vec<T::DatasetValue>);

// impl <T: Typer> Columns<T> {
//     pub async fn parse(
//         file_path: impl AsRef<Path>,
//         schema: &Schema<T>,
//         parsing_options: &LineParsingOptions,
//         line_count: usize,
//         skip_first_line: bool,
//         typer: &T,
// )
// }

// pub async fn read_file_data<T: Typer>(
//     file_path: impl AsRef<Path>,
//     schema: &Schema<T>,
//     parsing_options: &LineParsingOptions,
//     line_count: usize,
//     skip_first_line: bool,
//     typer: &T,
// ) -> Result<Vec<T::Column>> {
//     let mut data: Vec<Vec<Parsed<T::DatasetValue>>> =
//         vec![Vec::with_capacity(line_count); schema.column_types.len()];
//     let lines_to_skip = if skip_first_line { 1 } else { 0 };
//     let mut lines = file::read_lines(file_path).await?.skip(lines_to_skip);
//     while let Some(line_res) = lines.next().await {
//         let line = line_res?;
//         let line_values = LineParser::new(line, parsing_options);
//         for (col_ix, (value, column_type)) in
//             line_values.zip(schema.column_types.iter()).enumerate()
//         {
//             let column_value = typer.parse_as(&value, *column_type);
//             data[col_ix].push(column_value);
//         }
//     }
//     let typed_data = data
//         .into_iter()
//         .zip(schema.column_types.iter())
//         .map(|(vals, tag)| typer.parse_column(*tag, vals))
//         .collect();
//     Ok(typed_data)
// }
