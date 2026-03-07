#![allow(dead_code)]

mod downloader;
mod osv_gs;
pub mod types;

use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use anyhow::Context;
use chrono::{DateTime, Utc};

use crate::{
    downloader::{chuncked_download_to, simple_download_to},
    osv_gs::{osv_archive_url, osv_modified_id_csv_url, osv_record_url},
    types::{Ecosystem, OsvModifiedRecord, OsvRecord, OsvRecordId},
};

const OSV_RECORD_FILE_EXTENSION: &str = "json";
const RECORDS_DIRECTORY: &str = "records";

#[derive(Debug, Clone)]
pub struct OsvDb(Arc<RwLock<OsvDbInner>>);

#[derive(Debug)]
struct OsvDbInner {
    /// On disk location of the OSV data
    location: PathBuf,
    /// Ecosystem this database was initialised for, or [`None`] for all ecosystems
    ecosystem: Option<Ecosystem>,
    /// The latest `modified` timestamp across all records in the database
    last_modified: DateTime<Utc>,
}

impl OsvDb {
    pub fn new(
        ecosystem: Option<Ecosystem>,
        path: impl AsRef<Path>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            path.as_ref().is_dir(),
            "Provided `path` {} must be a directory",
            path.as_ref().display()
        );
        Ok(Self(Arc::new(RwLock::new(OsvDbInner {
            location: path.as_ref().to_path_buf(),
            ecosystem,
            last_modified: DateTime::<Utc>::MIN_UTC,
        }))))
    }

    fn read_inner(&self) -> RwLockReadGuard<'_, OsvDbInner> {
        let inner = self.0.read();
        // dont care about poisoning, get the recovered value
        inner.unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn write_inner(&self) -> RwLockWriteGuard<'_, OsvDbInner> {
        let inner = self.0.write();
        // dont care about poisoning, get the recovered value
        inner.unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Downloads a full, latest OSV database for the provided [`Ecosystem`].
    /// If provided ecosystem is [`None`], initialise for all ecosystems.
    /// - Downloads the latest archive into a temporary subdirectory of `location`
    /// - Moves all downloaded files into `location`, replacing any existing files
    /// - Scans all `.json` files in `location`, deserializes them as [`OsvRecord`]s, and
    ///   updates `self.last_modified` field with the maximum [`OsvRecord::modified`]
    ///   timestamp found across all records.
    pub async fn download_latest(&self) -> anyhow::Result<()> {
        let (tmp_dir, ecosystem) = {
            let read_inner = self.read_inner();
            (read_inner.tmp_dir("osv-download")?, read_inner.ecosystem)
        };

        download_and_extract_osv_archive(ecosystem.as_ref(), &tmp_dir).await?;

        let mut write_inner = self.write_inner();
        // cleans up the current state if its exitsts
        let records_dir = write_inner.records_path();
        if records_dir.exists() {
            std::fs::remove_dir_all(&records_dir)?;
        }
        // replace it with the latest one
        std::fs::rename(&tmp_dir, &records_dir)?;

        write_inner.last_modified = last_modified(&records_dir)?;
        Ok(())
    }

    pub fn get_record(
        &self,
        id: &OsvRecordId,
    ) -> anyhow::Result<Option<OsvRecord>> {
        let read_inner = self.read_inner();
        read_inner.get_record(id)
    }

    /// Sync with the latest OSV data, downloads only the records that have been modified
    /// since [`Self::last_modified`] and updates the local database files
    /// accordingly.
    ///
    /// Fetches the `modified_id.csv` index for the configured ecosystem (or all
    /// ecosystems if [`None`]). The file is sorted in reverse chronological order, so
    /// parsing stops as soon as a timestamp at or before [`Self::last_modified`] is
    /// encountered, avoiding a full re-download. After all new records are saved,
    /// [`Self::last_modified`] is updated to the highest timestamp seen.
    pub async fn sync(&self) -> anyhow::Result<()> {
        let (ecosystem, last_modified, records_dir) = {
            let inner = self.read_inner();
            (inner.ecosystem, inner.last_modified, inner.records_path())
        };

        let client = reqwest::Client::new();

        let csv_text = client
            .get(osv_modified_id_csv_url(ecosystem.as_ref()))
            .send()
            .await?
            .text()
            .await?;

        let mut new_last_modified = last_modified;

        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(csv_text.as_bytes());

        for result in rdr.records() {
            let entry = OsvModifiedRecord::try_from_csv_record(&result?, ecosystem)?;

            if entry.modified <= last_modified {
                break;
            }

            new_last_modified = new_last_modified.max(entry.modified);

            let mut record_path = records_dir.join(&entry.id);
            record_path.add_extension(OSV_RECORD_FILE_EXTENSION);

            simple_download_to(
                &client,
                &osv_record_url(Some(&entry.ecosystem), &entry.id),
                record_path,
            )
            .await?;
        }

        self.write_inner().last_modified = new_last_modified;
        Ok(())
    }
}

impl OsvDbInner {
    /// Returns the on disk location of the database
    #[must_use]
    pub fn location(&self) -> &Path {
        &self.location
    }

    fn records_path(&self) -> PathBuf {
        self.location().join(RECORDS_DIRECTORY)
    }

    fn tmp_dir(
        &self,
        prefix: &str,
    ) -> anyhow::Result<tempfile::TempDir> {
        Ok(tempfile::Builder::new()
            .prefix(prefix)
            .tempdir_in(self.location())?)
    }

    fn get_record(
        &self,
        id: &OsvRecordId,
    ) -> anyhow::Result<Option<OsvRecord>> {
        let records_dir = self.records_path();
        let mut record_path = records_dir.join(id);
        record_path.add_extension(OSV_RECORD_FILE_EXTENSION);
        if !record_path.exists() {
            return Ok(None);
        }
        let osv_record_file = File::open(record_path)?;
        let osv_record = serde_json::from_reader(&osv_record_file)?;
        Ok(Some(osv_record))
    }
}

/// Downloads the OSV archive for the given [`Ecosystem`] (or all ecosystems if [`None`])
/// from <https://storage.googleapis.com/osv-vulnerabilities> and extracts it into `path`.
async fn download_and_extract_osv_archive(
    ecosystem: Option<&Ecosystem>,
    path: impl AsRef<Path>,
) -> anyhow::Result<()> {
    const CHUNK_SIZE: u64 = 1024 * 1024; // 1 MB

    let client = reqwest::Client::new();
    let zip_archive_path = path.as_ref().join("osv.zip");
    let archive = chuncked_download_to(
        &client,
        &osv_archive_url(ecosystem),
        CHUNK_SIZE,
        &zip_archive_path,
    )
    .await?;

    let mut zip_archive = zip::ZipArchive::new(archive)?;
    zip_archive.extract(&path)?;
    std::fs::remove_file(&zip_archive_path)?;

    Ok(())
}

/// Scans all `.json` files in `path`, deserializes them as [`OsvRecord`]s, and returns
/// the maximum [`OsvRecord::modified`] timestamp found across all records.
///
/// Must be called after the OSV archive has already been downloaded and extracted into
/// `path` (i.e. after [`download_and_extract_osv_archive`] has completed successfully).
fn last_modified(path: impl AsRef<Path>) -> anyhow::Result<DateTime<Utc>> {
    std::fs::read_dir(path.as_ref())
        .context("failed to read database directory")?
        .filter_map(|entry| {
            match entry {
                Ok(entry) => {
                    let path = entry.path();
                    if path.extension()?.to_str()? == OSV_RECORD_FILE_EXTENSION {
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
            let file =
                File::open(&path).with_context(|| format!("failed to open {}", path.display()))?;
            let record: OsvRecord = serde_json::from_reader(file)
                .with_context(|| format!("failed to deserialize {}", path.display()))?;
            Ok(max.max(record.modified))
        })
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use chrono::Duration;
    use tempfile::TempDir;

    use super::*;
    use crate::types::{Ecosystem, OsvRecord};

    /// Downloads the latest OSV database, reads `RUSTSEC-2024-0401`, removes all
    /// records modified at or before its `modified` timestamp, then asserts the
    /// record no longer exists. Then calls sync to re-download it and asserts it
    /// is present again.
    #[tokio::test]
    async fn simple_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let osv = OsvDb::new(Some(Ecosystem::CratesIo), tmp.path()).unwrap();

        let record_id = "RUSTSEC-2024-0401".to_string();
        assert!(osv.get_record(&record_id).unwrap().is_none());

        osv.download_latest().await.unwrap();

        let record = osv.get_record(&record_id).unwrap().unwrap();
        let cutoff = record.modified;

        let records_dir = osv.read_inner().records_path();
        for entry in std::fs::read_dir(&records_dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().and_then(|e| e.to_str()) == Some(OSV_RECORD_FILE_EXTENSION) {
                let file = File::open(&path).unwrap();
                let r: OsvRecord = serde_json::from_reader(file).unwrap();
                if r.modified <= cutoff {
                    std::fs::remove_file(&path).unwrap();
                }
            }
        }

        assert!(osv.get_record(&record_id).unwrap().is_none());

        osv.write_inner().last_modified = cutoff - Duration::milliseconds(1);
        osv.sync().await.unwrap();

        assert!(osv.get_record(&record_id).unwrap().is_some());
    }
}
