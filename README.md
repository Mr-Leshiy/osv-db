# osv-db

A Rust library for downloading and querying the [OSV (Open Source Vulnerabilities)](https://osv.dev) database locally.

## Overview

`osv-db` fetches vulnerability data from the [OSV Google Cloud Storage](https://storage.googleapis.com/osv-vulnerabilities) and stores it on disk as JSON files. It supports scoping the database to a single ecosystem (e.g. `crates.io`, `PyPI`, `npm`) or working across all ecosystems at once.
Official [OSV Documentation](https://google.github.io/osv.dev/).

## Features

- **Full download** — fetch the complete OSV archive for one or all ecosystems (`OsvDb::download_latest`)
- **Incremental sync** — download only records modified since the last sync (`OsvDb::sync`)
- **Record lookup** — retrieve a single vulnerability by ID (`OsvDb::get_record`)
- **Stream iteration** — async stream over all stored records (`OsvDb::records_stream`)
- **Typed schema** — strongly-typed `OsvRecord` matching the [OSV schema spec](https://ossf.github.io/osv-schema/)

## Usage

```rust
use osv_db::{OsvDb, OsvGsEcosystems, OsvGsEcosystem};
use tempfile::TempDir;
use futures::TryStreamExt;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let db = OsvDb::new(OsvGsEcosystems::all().add(OsvGsEcosystem::CratesIo), dir.path())?;

    // Download the full database for crates.io
    db.download_latest(10 * 1024 * 1024).await?;

    // Look up a specific record
    if let Some(record) = db.get_record(&"RUSTSEC-2024-0401".to_string())? {
        println!("{}: {:?}", record.id, record.summary);
    }

    // Sync only new/updated records since last download
    let mut stream = db.sync().await.unwrap();
    while let Some(record) = stream.try_next().await? {
        println!("Updated: {}", record.id);
    }

    Ok(())
}
```
