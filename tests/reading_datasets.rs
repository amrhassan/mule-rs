use itertools::Itertools;
use mule::{Column, ColumnType, Dataset, DefaultTyper, ReadingOptions, Result};

#[tokio::test]
pub async fn test_dataset_read_sales_10_weird() -> Result<()> {
    let options = ReadingOptions::default();
    let typer = DefaultTyper::default();
    let dataset = Dataset::read_file("datasets/sales-10-weird.csv", options, &typer).await?;

    let schema = dataset.schema;
    let column_names = dataset.column_names;
    let data = dataset.data;

    let expected_schema = vec![
        ColumnType::Text,
        ColumnType::Text,
        ColumnType::Text,
        ColumnType::Text,
        ColumnType::Text,
        ColumnType::Text,
        ColumnType::Int,
        ColumnType::Text,
        ColumnType::Int,
        ColumnType::Float,
        ColumnType::Float,
        ColumnType::Float,
        ColumnType::Float,
        ColumnType::Float,
    ];

    let expected_column_names = Some(
        vec![
            "Region",
            "Country",
            "Item Type",
            "Sales Channel",
            "",
            "Order Date",
            "Order ID",
            "\"Ship\" Date",
            "Units Sold",
            "Unit Price",
            "Unit Cost",
            "Total Revenue",
            "Total Cost",
            "Total Profit",
        ]
        .into_iter()
        .map_into()
        .collect_vec(),
    );

    assert_eq!(schema, expected_schema);
    assert_eq!(schema.len(), 14);
    assert_eq!(column_names, expected_column_names);
    assert_eq!(column_names.map(|ns| ns.len()), Some(14));
    assert_eq!(data.len(), 14);

    for column in data {
        let all_good = match &column {
            Column::Boolean(vs) => vs.iter().all(|v| v.is_some()),
            Column::Int(vs) => vs.iter().all(|v| v.is_some()),
            Column::Float(vs) => vs.iter().all(|v| v.is_some()),
            Column::Text(vs) => vs.iter().all(|v| v.is_some()),
        };
        assert!(all_good, "The column has invalid values! {:?}", column)
    }

    Ok(())
}
