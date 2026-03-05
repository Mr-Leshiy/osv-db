use anyhow::Context;
use chrono::{DateTime, Utc};

use super::{Ecosystem, OsvRecordId};

/// A single entry from a `modified_id.csv` index file.
pub struct OsvModifiedRecord {
    /// Timestamp of the last modification.
    pub modified: DateTime<Utc>,
    /// Ecosystem the record belongs to.
    pub ecosystem: Ecosystem,
    /// Unique vulnerability identifier (e.g. `RUSTSEC-2024-0001`).
    pub id: OsvRecordId,
}

impl OsvModifiedRecord {
    /// The CSV format is `<iso modified date>,<ecosystem_dir>/<id>` for the all-ecosystem
    /// index, or `<iso modified date>,<id>` for a per-ecosystem index.
    /// That handles by the provided `ecosystem` argument, if [`None`] assuming to read as
    /// `<iso modified date>,<ecosystem_dir>/<id>` and `<iso modified date>,<id>`
    /// otherwise.
    pub fn try_from_csv_record(
        record: &csv::StringRecord,
        ecosystem: Option<Ecosystem>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            record.len() == 2,
            "expected 2 columns, got {}",
            record.len()
        );

        let timestamp_str = record.get(0).context("missing timestamp column")?;
        let path = record.get(1).context("missing path column")?;

        let modified: DateTime<Utc> = timestamp_str
            .parse()
            .context("invalid timestamp in modified_id.csv")?;

        if let Some(ecosystem) = ecosystem {
            Ok(Self {
                modified,
                ecosystem,
                id: path.to_string(),
            })
        } else if let Some((ecosystem, id)) = path.split_once('/') {
            Ok(Self {
                modified,
                ecosystem: ecosystem.parse()?,
                id: id.to_string(),
            })
        } else {
            anyhow::bail!("Invalid format, must be <ecosystem_dir>/<id>, provided: {path}")
        }
    }
}
