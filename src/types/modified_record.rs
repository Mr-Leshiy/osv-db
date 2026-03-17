use chrono::{DateTime, Utc};

use super::OsvRecordId;
use crate::{errors::ParseModifiedRecordErr, osv_gs::OsvGsEcosystem};

/// A single entry from a `modified_id.csv` index file.
pub struct OsvModifiedRecord {
    /// Timestamp of the last modification.
    pub modified: DateTime<Utc>,
    /// Ecosystem the record belongs to.
    pub ecosystem: OsvGsEcosystem,
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
        ecosystem: Option<OsvGsEcosystem>,
    ) -> Result<Self, ParseModifiedRecordErr> {
        if record.len() != 2 {
            return Err(ParseModifiedRecordErr::WrongColumnCount(record.len()));
        }

        let timestamp_str = record
            .get(0)
            .ok_or(ParseModifiedRecordErr::MissingTimestamp)?;
        let path = record.get(1).ok_or(ParseModifiedRecordErr::MissingPath)?;

        let modified: DateTime<Utc> = timestamp_str
            .parse()
            .map_err(ParseModifiedRecordErr::ParseTimestamp)?;

        if let Some(ecosystem) = ecosystem {
            Ok(Self {
                modified,
                ecosystem,
                id: path.to_string(),
            })
        } else if let Some((ecosystem_str, id)) = path.split_once('/') {
            Ok(Self {
                modified,
                ecosystem: ecosystem_str
                    .parse()
                    .map_err(ParseModifiedRecordErr::ParseEcosystem)?,
                id: id.to_string(),
            })
        } else {
            Err(ParseModifiedRecordErr::InvalidPathFormat(path.to_string()))
        }
    }
}
