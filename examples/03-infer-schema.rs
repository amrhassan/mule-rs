use std::env;

use mule::{infer_file_schema, LineParsingOptions, Result, SchemaInferenceDepth};

#[tokio::main]
pub async fn main() -> Result<()> {
    let file_path = env::args().skip(1).next().expect("Missing argument");
    let schema = infer_file_schema(
        file_path,
        &SchemaInferenceDepth::default(),
        &LineParsingOptions::default(),
    )
    .await?;

    println!("Inferred the schema: {:#?}", schema);

    Ok(())
}
