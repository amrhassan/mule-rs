use mule_rs::{detect_separator, Result};
use tokio::fs::File;

#[tokio::test]
pub async fn test_separator_detection() -> Result<()> {
    let tsv_file = File::open("datasets/sales-100.tsv").await?;
    assert_eq!(detect_separator(tsv_file).await?, "\t");

    let csv_file = File::open("datasets/sales-100.csv").await?;
    assert_eq!(detect_separator(csv_file).await.unwrap(), ",");

    Ok(())
}
