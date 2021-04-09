use itertools::Itertools;
use mule::{ColumnValue, Dataset, DefaultTyper, ReadingOptions, Result, Typer, ValueType};

#[tokio::test]
pub async fn test_dataset_read_sales_10_weird() -> Result<()> {
    let options = ReadingOptions::default();
    let typer = DefaultTyper::default();
    let dataset = Dataset::read_file("datasets/sales-10-weird.csv", options, &typer).await?;

    let schema = dataset.schema;
    let column_names = dataset.column_names;
    let data = dataset.data;

    let expected_schema = vec![
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
        let col_types = column
            .iter()
            .flat_map(|v| match v {
                ColumnValue::Invalid => vec![].into_iter(),
                ColumnValue::Missing => vec![].into_iter(),
                ColumnValue::Some(x) => vec![typer.tag_type(&x)].into_iter(),
            })
            .collect_vec();
        assert!(
            col_types.iter().all_equal(),
            "The column has multiple types! {:?}",
            col_types
        )
    }

    Ok(())
}
