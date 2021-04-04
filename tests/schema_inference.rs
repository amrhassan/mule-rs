use itertools::Itertools;
use mule::{
    infer_column_types, infer_separator, read_column_names, DefaultTyper, Result, ValueType,
};
use tokio::fs::File;

#[tokio::test]
pub async fn test_separator_inference() -> Result<()> {
    let tsv_file = File::open("datasets/sales-100.tsv").await?;
    assert_eq!(infer_separator(tsv_file).await?, "\t");

    let csv_file = File::open("datasets/sales-100.csv").await?;
    assert_eq!(infer_separator(csv_file).await.unwrap(), ",");

    Ok(())
}

#[tokio::test]
pub async fn test_infer_column_types_sales_100() -> Result<()> {
    let csv_file = File::open("datasets/sales-100.csv").await?;

    let typer = DefaultTyper::default();
    let column_types = infer_column_types(csv_file, true, 200, ",", "\"", "\\", typer).await?;

    let expected = vec![
        ValueType::Text,
        ValueType::Text,
        ValueType::Text,
        ValueType::Text,
        ValueType::Text,
        ValueType::Text,
        ValueType::Int,
        ValueType::Text,
        ValueType::Int,
        ValueType::Float,
        ValueType::Float,
        ValueType::Float,
        ValueType::Float,
        ValueType::Float,
    ];

    assert_eq!(expected, column_types);

    Ok(())
}

#[tokio::test]
pub async fn test_read_colum_names_sales_100() -> Result<()> {
    let csv_file = File::open("datasets/sales-100.csv").await?;

    let column_names = read_column_names(csv_file, ",", "\"", "\\").await?;

    let expected = vec![
        "Region",
        "Country",
        "Item Type",
        "Sales Channel",
        "Order Priority",
        "Order Date",
        "Order ID",
        "Ship Date",
        "Units Sold",
        "Unit Price",
        "Unit Cost",
        "Total Revenue",
        "Total Cost",
        "Total Profit",
    ]
    .into_iter()
    .map_into()
    .collect_vec();

    assert_eq!(Some(expected), column_names);

    Ok(())
}
