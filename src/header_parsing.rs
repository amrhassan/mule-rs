use crate::dataset_file::DatasetFile;
use crate::errors::Result;
use crate::record_parsing::{RecordParser, RecordParsingOptions};
use itertools::Itertools;
use std::path::Path;
use tokio_stream::StreamExt;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Header {
    pub column_names: Vec<String>,
}

impl Header {
    pub async fn parse(
        path: impl AsRef<Path>,
        options: &RecordParsingOptions,
    ) -> Result<Option<Header>> {
        let header = DatasetFile::new(path)
            .read_records()
            .await?
            .try_next()
            .await?
            .map(|record| {
                let names = RecordParser::new(record, options);
                names.map_into().collect_vec()
            })
            .map(|column_names| Header { column_names });
        Ok(header)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    pub async fn test_read_header_sales_100() -> Result<()> {
        let options = RecordParsingOptions::default();
        let header = Header::parse("datasets/sales-100.csv", &options).await?;

        let expected = Some(Header {
            column_names: vec![
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
            .collect_vec(),
        });

        assert_eq!(header, expected);

        Ok(())
    }
}
