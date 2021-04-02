use mule_rs::detect_separator;
use std::fs::File;

#[test]
pub fn test_separator_detection() {
    let tsv_file = File::open("datasets/sales-100.tsv").unwrap();
    assert_eq!(detect_separator(tsv_file).unwrap(), "\t");

    let csv_file = File::open("datasets/sales-100.csv").unwrap();
    assert_eq!(detect_separator(csv_file).unwrap(), ",");
}
