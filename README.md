# mule #
Strong-headed (yet flexible) parser of columnar datasets from CSV, TSV or other delimiter-separated datasets

# Usage #
Add the dependency to `Cargo.toml`:
```toml
mule = "0.0.0"
```

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
