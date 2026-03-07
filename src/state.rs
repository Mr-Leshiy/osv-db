use std::{
    collections::{HashMap, HashSet},
    fs::File,
    path::Path,
};

use anyhow::Context;
use chrono::{DateTime, Utc};
use rayon::prelude::*;

use crate::types::{OsvRecord, OsvRecordId, PackageName};

#[derive(Debug, PartialEq, Eq)]
pub struct OsvState {
    pub last_modified: DateTime<Utc>,
    pub affected: HashMap<PackageName, HashSet<OsvRecordId>>,
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

        let default_self = || {
            Self {
                last_modified: DateTime::<Utc>::MIN_UTC,
                affected: HashMap::new(),
            }
        };

        std::fs::read_dir(path.as_ref())
            .context("failed to read database directory")?
            .par_bridge()
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
            .map(|path| {
                let path = path?;
                let file = File::open(&path)
                    .with_context(|| format!("failed to open {}", path.display()))?;
                let record: OsvRecord = serde_json::from_reader(file)
                    .with_context(|| format!("failed to deserialize {}", path.display()))?;
                anyhow::Ok(record)
            })
            .try_fold(default_self, |mut res, record| {
                let record = record?;
                if record.modified > res.last_modified {
                    res.last_modified = record.modified;
                }
                if let Some(packages) = &record.affected {
                    for entry in packages {
                        if let Some(package) = &entry.package {
                            res.affected
                                .entry(package.name.clone())
                                .or_default()
                                .insert(record.id.clone());
                        }
                    }
                }
                anyhow::Ok(res)
            })
            .try_reduce(default_self, |mut a, b| {
                a.merge(b);
                anyhow::Ok(a)
            })
    }

    /// Merges `other` into `self`: all record IDs from `other.affected` are merged into
    /// `self.affected` (unioning per-package sets on key collision), and
    /// `self.last_modified` is updated to the later of the two timestamps.
    pub fn merge(
        &mut self,
        other: OsvState,
    ) {
        self.last_modified = self.last_modified.max(other.last_modified);
        for (package, record_ids) in other.affected {
            self.affected.entry(package).or_default().extend(record_ids);
        }
    }
}
