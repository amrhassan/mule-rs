use mule::{read_file, Result};

#[tokio::main]
pub async fn main() -> Result<()> {
    let file_path = "datasets/sales-10.csv";
    let dataset = read_file(file_path).await?;

    println!("Got dataset: {:#?}", dataset);

    Ok(())
}
