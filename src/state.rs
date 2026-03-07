use std::{fs::File, path::Path};

use anyhow::Context;
use chrono::{DateTime, Utc};

use crate::types::OsvRecord;

#[derive(Debug, PartialEq, Eq)]
pub struct OsvState {
    pub last_modified: DateTime<Utc>,
}

impl OsvState {
    /// Scans all `.json` files in `path`, deserializes them as [`OsvRecord`]s, and builds
    /// an [`OsvState`] with the maximum [`OsvRecord::modified`] timestamp found across
    /// all records.
    ///
    /// Must be called after the OSV archive has already been downloaded and extracted
    /// into `path` (i.e. after [`download_and_extract_osv_archive`] has completed
    /// successfully).
    pub fn build(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        if !path.as_ref().exists() {
            return Ok(Self {
                last_modified: DateTime::<Utc>::MIN_UTC,
            })
        }
        
        let last_modified = std::fs::read_dir(path.as_ref())
            .context("failed to read database directory")?
            .filter_map(|entry| {
                match entry {
                    Ok(entry) => {
                        let path = entry.path();
                        if path.extension()?.to_str()? == super::OSV_RECORD_FILE_EXTENSION {
                            Some(anyhow::Ok(path))
                        } else {
                            None
                        }
                    },
                    Err(err) => Some(Err(err.into())),
                }
            })
            .try_fold(DateTime::<Utc>::MIN_UTC, |max, path| {
                let path = path?;
                let file = File::open(&path)
                    .with_context(|| format!("failed to open {}", path.display()))?;
                let record: OsvRecord = serde_json::from_reader(file)
                    .with_context(|| format!("failed to deserialize {}", path.display()))?;
                anyhow::Ok(max.max(record.modified))
            })?;

        Ok(Self { last_modified })
    }
}
