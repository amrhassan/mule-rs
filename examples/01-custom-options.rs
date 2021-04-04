use mule::{Dataset, DefaultTypedReadingOptions, ReadingOptions, Result};

#[tokio::main]
pub async fn main() -> Result<()> {
    let file_path = "datasets/sales-10.tsv";
    let options = ReadingOptions {
        schema_inference_percentage: 0.5, // Will read 50% of the dataset to infer its schema
        ..DefaultTypedReadingOptions::default()
    };
    let dataset = Dataset::read_file(file_path, options).await?;

    println!("Got dataset: {:#?}", dataset);

    Ok(())
}
