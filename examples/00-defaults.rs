use std::env;

use mule::{read_file, Result};

#[tokio::main]
pub async fn main() -> Result<()> {
    let file_path = env::args()
        .skip(1)
        .next()
        .unwrap_or_else(|| "datasets/sales-10.csv".to_string());
    let dataset = read_file(file_path).await?;

    println!("Got dataset: {:#?}", dataset);

    Ok(())
}
