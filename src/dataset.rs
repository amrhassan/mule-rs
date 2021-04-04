use super::default_typer::DefaultTyper;
use super::typer::Typer;

pub struct Dataset<T: Typer> {
    pub column_names: Option<Vec<String>>,
    pub column_types: Vec<T::TypeTag>,
    pub data: Vec<Vec<T::TypedValue>>,
}

pub type TypedDataset = Dataset<DefaultTyper>;
