use super::typer::Typer;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema<T: Typer> {
    pub column_types: Vec<T::ColumnType>,
}
