use std::{collections::HashMap, fs::File, path::Path};

use anyhow::Context;
use chrono::{DateTime, Utc};

use crate::types::{OsvRecord, OsvRecordId, PackageName};

#[derive(Debug, PartialEq, Eq)]
pub struct OsvState {
    pub last_modified: DateTime<Utc>,
    pub affected: HashMap<PackageName, OsvRecordId>,
}

impl OsvState {
    /// Scans all `.json` files in `path`, deserializes them as [`OsvRecord`]s, and builds
    /// an [`OsvState`] with the maximum [`OsvRecord::modified`] timestamp found across
    /// all records, and a map of package names to their [`OsvRecordId`].
    ///
    /// Must be called after the OSV archive has already been downloaded and extracted
    /// into `path` (i.e. after [`download_and_extract_osv_archive`] has completed
    /// successfully).
    pub fn build(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        anyhow::ensure!(
            path.as_ref().is_dir(),
            "Provided `path` {} must be a directory and exists",
            path.as_ref().display()
        );
        let res = Self {
            last_modified: DateTime::<Utc>::MIN_UTC,
            affected: HashMap::new(),
        };
        
        let res = std::fs::read_dir(path.as_ref())
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
            .try_fold(res, |mut res, path| {
                let path = path?;
                let file = File::open(&path)
                    .with_context(|| format!("failed to open {}", path.display()))?;
                let record: OsvRecord = serde_json::from_reader(file)
                    .with_context(|| format!("failed to deserialize {}", path.display()))?;
                if record.modified > res.last_modified {
                    res.last_modified = record.modified;
                }
                if let Some(packages) = &record.affected {
                    for entry in packages {
                        if let Some(package) = &entry.package {
                            res.affected.insert(package.name.clone(), record.id.clone());
                        }
                    }
                }
                anyhow::Ok(res)
            })?;

        Ok(res)
    }

    /// Merges `other` into `self`: all entries from `other.affected` are inserted into
    /// `self.affected` (overwriting on key collision), and `self.last_modified` is updated
    /// to the later of the two timestamps.
    pub fn merge(&mut self, other: OsvState) {
        self.last_modified = self.last_modified.max(other.last_modified);
        self.affected.extend(other.affected);
    }
}
