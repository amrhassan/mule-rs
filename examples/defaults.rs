use mule::{ColumnValue, Dataset, DefaultTypedReadingOptions, Result};

#[tokio::main]
pub async fn main() -> Result<()> {
    let file_path = "datasets/sales-100.csv";
    let options = DefaultTypedReadingOptions::default();
    let dataset = Dataset::read_file(file_path, options).await?;

    println!("Read {}.", file_path);

    if let Some(col_names) = dataset.column_names {
        println!("Columns found:");
        for (col_name, col_type) in col_names.iter().zip(dataset.column_types) {
            println!("  {}: {}", col_name, col_type)
        }
    }

    println!("First few rows:");
    for row_ix in 0..10 {
        print!("| ");
        for col_ix in 0..dataset.data.len() {
            let v = dataset.data[col_ix]
                .get(row_ix)
                .map(|v| match v {
                    ColumnValue::Some(vv) => format!("{:?}", vv),
                    ColumnValue::Invalid => "Invalid".to_string(),
                    ColumnValue::Missing => "Missing".to_string(),
                })
                .unwrap_or("".to_string());
            print!("{} | ", v)
        }
        println!("")
    }

    Ok(())
}
