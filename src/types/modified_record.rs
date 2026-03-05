use anyhow::Context;
use chrono::{DateTime, Utc};

use super::{Ecosystem, OsvRecordId};

/// A single entry from a `modified_id.csv` index file.
///
/// The CSV format is `<iso modified date>,<ecosystem_dir>/<id>` for the all-ecosystem
/// index, or `<iso modified date>,<id>` for a per-ecosystem index (in which case
/// [`ecosystem`](Self::ecosystem) will be [`None`]).
pub struct OsvModifiedRecord {
    /// Timestamp of the last modification.
    pub modified: DateTime<Utc>,
    /// Ecosystem the record belongs to, or [`None`] for per-ecosystem CSV files.
    pub ecosystem: Option<Ecosystem>,
    /// Unique vulnerability identifier (e.g. `RUSTSEC-2024-0001`).
    pub id: OsvRecordId,
}

impl TryFrom<csv::StringRecord> for OsvModifiedRecord {
    type Error = anyhow::Error;

    fn try_from(record: csv::StringRecord) -> Result<Self, Self::Error> {
        anyhow::ensure!(record.len() == 2, "expected 2 columns, got {}", record.len());

        let timestamp_str = record.get(0).context("missing timestamp column")?;
        let path = record.get(1).context("missing path column")?;

        let modified: DateTime<Utc> = timestamp_str
            .parse()
            .context("invalid timestamp in modified_id.csv")?;

        let (ecosystem, id) = match path.split_once('/') {
            Some((eco_str, id_str)) => {
                let ecosystem = eco_str
                    .parse::<Ecosystem>()
                    .with_context(|| format!("unknown ecosystem: {eco_str}"))?;
                (Some(ecosystem), id_str.to_string())
            }
            None => (None, path.to_string()),
        };

        Ok(Self {
            modified,
            ecosystem,
            id,
        })
    }
}
