use crate::errors::Result;
use crate::file;
use crate::line_parsing::{LineParser, LineParsingOptions};
use crate::schema::Schema;
use crate::value_parsing::Parsed;
use crate::Typer;
use std::path::Path;
use tokio_stream::StreamExt;

#[derive(Clone, Debug, PartialEq)]
pub struct Column<T: Typer> {
    pub values: Vec<Parsed<T::DatasetValue>>,
}

impl<T: Typer> Column<T> {
    pub fn new(n: usize) -> Column<T> {
        Column {
            values: vec![Parsed::Missing; n],
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct Columns<T: Typer> {
    pub columns: Vec<Column<T>>,
}

impl<T: Typer> Columns<T> {
    pub fn new(columns: usize, rows: usize) -> Columns<T> {
        Columns {
            columns: vec![Column::new(rows); columns],
        }
    }
}

impl<T: Typer> Columns<T> {
    pub async fn parse(
        file_path: impl AsRef<Path>,
        schema: &Schema<T>,
        parsing_options: &LineParsingOptions,
        line_count: usize,
        skip_first_line: bool,
        typer: &T,
    ) -> Result<Columns<T>> {
        let lines_to_skip = if skip_first_line { 1 } else { 0 };

        let mut columns: Columns<T> =
            Columns::new(schema.column_types.len(), line_count - lines_to_skip);

        let mut lines = file::read_lines(file_path).await?.skip(lines_to_skip);
        let mut row_ix = 0;

        while let Some(line_res) = lines.next().await {
            let line = line_res?;
            let line_values = LineParser::new(line, parsing_options);
            for (col_ix, (value, column_type)) in
                line_values.zip(schema.column_types.iter()).enumerate()
            {
                let column_value = typer.parse_as(&value, *column_type);
                columns.columns[col_ix].values[row_ix] = column_value;
            }
            row_ix += 1;
        }

        Ok(columns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Value;
    use crate::{ColumnType, DefaultTyper, Schema};

    #[tokio::test]
    pub async fn test_parses_sales_10_weird() -> Result<()> {
        let typer = DefaultTyper::default();
        let parsing_options = LineParsingOptions::default();
        let skip_first_line = true;
        let schema = Schema::<DefaultTyper> {
            column_types: vec![
                ColumnType::Text,
                ColumnType::Text,
                ColumnType::Text,
                ColumnType::Text,
                ColumnType::Text,
                ColumnType::Text,
                ColumnType::Int,
                ColumnType::Text,
                ColumnType::Int,
                ColumnType::Float,
                ColumnType::Float,
                ColumnType::Float,
                ColumnType::Float,
                ColumnType::Float,
            ],
        };

        let line_count = 10;

        let columns = Columns::parse(
            "datasets/sales-10-weird-bad.csv",
            &schema,
            &parsing_options,
            line_count,
            skip_first_line,
            &typer,
        )
        .await?;

        use Parsed::*;
        use Value::*;

        let expected_columns = Columns {
            columns: vec![
                Column {
                    values: vec![
                        Some(Text("Australia and Oceania".to_string())),
                        Some(Text("Central America and the Caribbean".to_string())),
                        Some(Text("Europe".to_string())),
                        Some(Text("Sub-Saharan Africa".to_string())),
                        Some(Text("Sub-Saharan Africa".to_string())),
                        Some(Text("".to_string())),
                        Some(Text("Sub-Saharan Africa".to_string())),
                        Some(Text("Sub-Saharan Africa".to_string())),
                        Some(Text("Sub-Saharan Africa".to_string())),
                    ],
                },
                Column {
                    values: vec![
                        Some(Text("Tuvalu".to_string())),
                        Some(Text("Grenada".to_string())),
                        Some(Text("Russia".to_string())),
                        Some(Text("Sao Tome and Principe".to_string())),
                        Some(Text("Rwanda".to_string())),
                        Some(Text("Solomon Islands".to_string())),
                        Some(Text("Angola".to_string())),
                        Some(Text("Burkina Faso".to_string())),
                        Some(Text("Republic of the Congo".to_string())),
                    ],
                },
                Column {
                    values: vec![
                        Some(Text("Baby Food".to_string())),
                        Some(Text("Cereal".to_string())),
                        Some(Text("Office Supplies".to_string())),
                        Some(Text("Fruits".to_string())),
                        Some(Text("Office Supplies".to_string())),
                        Some(Text("Baby Food".to_string())),
                        Some(Text("Household".to_string())),
                        Some(Text("Vegetables".to_string())),
                        Some(Text("Personal Care".to_string())),
                    ],
                },
                Column {
                    values: vec![
                        Some(Text("Offline".to_string())),
                        Some(Text("Online".to_string())),
                        Some(Text("Offline".to_string())),
                        Some(Text("Online".to_string())),
                        Some(Text("Offline".to_string())),
                        Some(Text("Online".to_string())),
                        Some(Text("Offline".to_string())),
                        Some(Text("Online".to_string())),
                        Some(Text("Offline".to_string())),
                    ],
                },
                Column {
                    values: vec![
                        Some(Text("H".to_string())),
                        Some(Text("C".to_string())),
                        Some(Text("L".to_string())),
                        Some(Text("C".to_string())),
                        Some(Text("L".to_string())),
                        Some(Text("C".to_string())),
                        Some(Text("M".to_string())),
                        Some(Text("H".to_string())),
                        Some(Text("M".to_string())),
                    ],
                },
                Column {
                    values: vec![
                        Some(Text("5/28/2010".to_string())),
                        Some(Text("8/22/2012".to_string())),
                        Some(Text("5/2/2014".to_string())),
                        Some(Text("6/20/2014".to_string())),
                        Some(Text("2/1/2013".to_string())),
                        Some(Text("2-4-2015".to_string())),
                        Some(Text("4/23/2011".to_string())),
                        Some(Text("7/17/2012".to_string())),
                        Some(Text("7/14/2015".to_string())),
                    ],
                },
                Column {
                    values: vec![
                        Some(Int(669165933)),
                        Some(Int(963881480)),
                        Some(Int(341417157)),
                        Some(Int(514321792)),
                        Some(Int(115456712)),
                        Some(Int(547995746)),
                        Some(Int(135425221)),
                        Some(Int(871543967)),
                        Some(Int(770463311)),
                    ],
                },
                Column {
                    values: vec![
                        Some(Text("6/27/2010".to_string())),
                        Some(Text("9/15/2012".to_string())),
                        Some(Text("5/8/2014".to_string())),
                        Some(Text("7/5/2014".to_string())),
                        Some(Text("2/6/2013".to_string())),
                        Some(Text("2/21/2015".to_string())),
                        Some(Text("4/27/2011".to_string())),
                        Some(Text("7/27/2012".to_string())),
                        Some(Text("8/25/2015".to_string())),
                    ],
                },
                Column {
                    values: vec![
                        Some(Int(9925)),
                        Some(Int(2804)),
                        Some(Int(1779)),
                        Some(Int(8102)),
                        Some(Int(5062)),
                        Invalid,
                        Some(Int(4187)),
                        Some(Int(8082)),
                        Some(Int(6070)),
                    ],
                },
                Column {
                    values: vec![
                        Some(Float(255.28)),
                        Some(Float(205.7)),
                        Some(Float(651.21)),
                        Some(Float(9.33)),
                        Some(Float(651.21)),
                        Some(Float(255.28)),
                        Some(Float(668.27)),
                        Some(Float(154.06)),
                        Some(Float(81.73)),
                    ],
                },
                Column {
                    values: vec![
                        Some(Float(159.42)),
                        Some(Float(117.11)),
                        Some(Float(524.96)),
                        Some(Float(6.92)),
                        Some(Float(524.96)),
                        Some(Float(159.42)),
                        Some(Float(502.54)),
                        Some(Float(90.93)),
                        Some(Float(56.67)),
                    ],
                },
                Column {
                    values: vec![
                        Some(Float(2533654.0)),
                        Some(Float(576782.8)),
                        Some(Float(1158502.59)),
                        Some(Float(75591.66)),
                        Some(Float(3296425.02)),
                        Some(Float(0.72)),
                        Some(Float(2798046.49)),
                        Some(Float(1245112.92)),
                        Some(Float(496101.1)),
                    ],
                },
                Column {
                    values: vec![
                        Some(Float(1582243.5)),
                        Some(Float(328376.44)),
                        Some(Float(933903.84)),
                        Some(Float(56065.84)),
                        Some(Float(2657347.52)),
                        Some(Float(474115.08)),
                        Some(Float(2104134.98)),
                        Some(Float(734896.26)),
                        Some(Float(343986.9)),
                    ],
                },
                Column {
                    values: vec![
                        Some(Float(951410.5)),
                        Some(Float(248406.36)),
                        Some(Float(224598.75)),
                        Some(Float(19525.82)),
                        Some(Float(639077.5)),
                        Some(Float(285087.64)),
                        Some(Float(693911.51)),
                        Some(Float(510216.66)),
                        Some(Float(152114.2)),
                    ],
                },
            ],
        };

        assert_eq!(columns, expected_columns);

        Ok(())
    }
}
