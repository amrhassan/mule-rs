use mule::{Dataset, DefaultTyper, ReadingOptions, Result, SchemaInferenceDepth};
use std::env;

#[tokio::main]
pub async fn main() -> Result<()> {
    let file_path = env::args()
        .skip(1)
        .next()
        .unwrap_or_else(|| "datasets/sales-10.csv".to_string());
    let options = ReadingOptions {
        schema_inference_depth: SchemaInferenceDepth::Percentage(0.5), // Will read 50% of the dataset to infer its schema
        ..ReadingOptions::default()
    };
    let typer = DefaultTyper::default();
    let dataset = Dataset::read_file(file_path, options, &typer).await?;

    println!("Got dataset: {:#?}", dataset);

    Ok(())
}
