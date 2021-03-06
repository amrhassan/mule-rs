# mule #
Strong-headed (yet flexible) parser of columnar datasets from CSV, TSV and other delimiter-separated datasets

[![Tests](https://github.com/amrhassan/mule-rs/actions/workflows/test.yaml/badge.svg)](https://github.com/amrhassan/mule-rs/actions/workflows/test.yaml)
[![Crates.io](https://img.shields.io/crates/v/mule)](https://crates.io/crates/mule)
[![Documentation](https://docs.rs/mule/badge.svg)](https://docs.rs/mule)
[![Crates.io](https://img.shields.io/crates/l/mule)](LICENSE)

# Usage #
```rust
use mule::{read_file, Result};

#[tokio::main]
pub async fn main() -> Result<()> {
    let file_path = "datasets/sales-10.csv";
    let dataset = read_file(file_path).await?;
    println!("Got dataset: {:#?}", dataset);
    Ok(())
}
```

Other examples are available in the [examples directory](https://github.com/amrhassan/mule-rs/tree/main/examples).
