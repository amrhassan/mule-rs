use std::env;

use mule::{DefaultTyper, LineParsingOptions, Result, Schema, SchemaInferenceDepth};

#[tokio::main]
pub async fn main() -> Result<()> {
    let file_path = env::args().skip(1).next().expect("Missing argument");
    let skip_header = true;
    let inference_depth = SchemaInferenceDepth::Percentage(1.0);
    let parsing_options = LineParsingOptions::default();
    let typer = DefaultTyper;
    let schema = Schema::infer(
        file_path,
        skip_header,
        &inference_depth,
        &parsing_options,
        &typer,
    )
    .await?;

    println!("Inferred the schema: {:#?}", schema);

    Ok(())
}
