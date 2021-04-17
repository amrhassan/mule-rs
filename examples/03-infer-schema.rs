use std::env;

use mule::{infer_schema, DefaultTyper, ParsingOptions, Result, SchemaInferenceDepth};

#[tokio::main]
pub async fn main() -> Result<()> {
    let file_path = env::args().skip(1).next().expect("Missing argument");
    let schema = infer_schema(
        file_path,
        true,
        &SchemaInferenceDepth::default(),
        &ParsingOptions::default(),
        DefaultTyper::default(),
    )
    .await?;

    println!("Inferred the schema: {:#?}", schema);

    Ok(())
}
