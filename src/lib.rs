#![allow(dead_code)]

mod downloader;
mod osv_gs;
mod state;
pub mod types;

use std::{
    collections::HashSet,
    fs::File,
    path::{Path, PathBuf},
    sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use chrono::{DateTime, Utc};

pub use crate::osv_gs::OsvGsEcosystem;
use crate::{
    downloader::{chuncked_download_to, simple_download_to},
    osv_gs::{osv_archive_url, osv_modified_id_csv_url, osv_record_url},
    state::OsvState,
    types::{OsvModifiedRecord, OsvRecord, OsvRecordId, PackageName},
};

const OSV_RECORD_FILE_EXTENSION: &str = "json";
const RECORDS_DIRECTORY: &str = "records";

#[derive(Debug, Clone)]
pub struct OsvDb(Arc<OsvDbInner>);

#[derive(Debug)]
struct OsvDbInner {
    /// On disk location of the OSV data
    location: PathBuf,
    /// Ecosystem this database was initialised for, or [`None`] for all ecosystems
    ecosystem: Option<OsvGsEcosystem>,
    /// State of the database
    state: RwLock<OsvState>,
}

impl OsvDb {
    /// Creates a new [`OsvDb`] rooted at `path` for the given `ecosystem`.
    ///
    /// If `ecosystem` is [`None`], the database covers all ecosystems.
    ///
    /// # Errors
    ///
    /// Returns an error if `path` does not point to an existing directory.
    pub fn new(
        ecosystem: Option<OsvGsEcosystem>,
        path: impl AsRef<Path>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            path.as_ref().is_dir(),
            "Provided `path` {} must be a directory and exists",
            path.as_ref().display()
        );
        let records_dir = path.as_ref().join(RECORDS_DIRECTORY);
        if !records_dir.exists() {
            std::fs::create_dir(&records_dir)?;
        }
        let state = OsvState::build(records_dir)?;
        Ok(Self(Arc::new(OsvDbInner {
            location: path.as_ref().to_path_buf(),
            ecosystem,
            state: RwLock::new(state),
        })))
    }

    /// Returns the on disk location of the database
    #[must_use]
    pub fn location(&self) -> &Path {
        &self.0.location
    }

    /// Returns the latest `modified` timestamp seen across all records in the database.
    ///
    /// The value reflects the most recent [`download_latest`](Self::download_latest) or
    /// [`sync`](Self::sync) call. Returns the Unix epoch if the database has not yet
    /// been populated.
    #[must_use]
    pub fn last_modified(&self) -> DateTime<Utc> {
        self.read_state().last_modified
    }

    /// Returns the set of [`OsvRecordId`]s associated with the given package name,
    /// or [`None`] if no records are found for that package.
    #[must_use]
    pub fn get_record_id(
        &self,
        package_name: &PackageName,
    ) -> Option<HashSet<OsvRecordId>> {
        self.read_state().affected.get(package_name).cloned()
    }

    /// Returns a read guard for [`OsvState`].
    ///
    /// Poisoning is ignored — if a thread panicked while holding the write lock, the
    /// state is still returned as-is, since a partially-updated [`OsvState`] is
    /// preferable to propagating the panic across unrelated callers.
    fn read_state(&self) -> RwLockReadGuard<'_, OsvState> {
        self.0.state.read().unwrap_or_else(PoisonError::into_inner)
    }

    /// Returns a write guard for [`OsvState`].
    ///
    /// Poisoning is ignored — if a thread panicked while holding the write lock, the
    /// state is still returned as-is, since a partially-updated [`OsvState`] is
    /// preferable to propagating the panic across unrelated callers.
    fn write_state(&self) -> RwLockWriteGuard<'_, OsvState> {
        self.0.state.write().unwrap_or_else(PoisonError::into_inner)
    }

    fn records_dir(&self) -> PathBuf {
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

    /// Looks up a single OSV record by its [`OsvRecordId`].
    ///
    /// Returns `Ok(None)` if no record matching `id` exists.
    ///
    /// # Errors
    ///
    /// Returns an error if the record file cannot be opened or deserialized.
    pub fn get_record(
        &self,
        id: &OsvRecordId,
    ) -> anyhow::Result<Option<OsvRecord>> {
        let records_dir = self.records_dir();
        let mut record_path = records_dir.join(id);
        record_path.add_extension(OSV_RECORD_FILE_EXTENSION);
        if !record_path.exists() {
            return Ok(None);
        }
        let osv_record_file = File::open(record_path)?;
        let osv_record = serde_json::from_reader(&osv_record_file)?;
        Ok(Some(osv_record))
    }

    /// Downloads a full, latest OSV database for the provided [`OsvGsEcosystem`].
    /// If provided ecosystem is [`None`], initialise for all ecosystems.
    /// - Downloads the latest archive into a temporary subdirectory of `location`
    /// - Moves all downloaded files into `location`, replacing any existing files
    /// - Scans all `.json` files in `location`, deserializes them as [`OsvRecord`]s, and
    ///   updates `self.last_modified` field with the maximum [`OsvRecord::modified`]
    ///   timestamp found across all records.
    pub async fn download_latest(
        &self,
        chunk_size: u64,
    ) -> anyhow::Result<()> {
        let tmp_dir = self.tmp_dir("osv-download")?;
        download_and_extract_osv_archive(self.0.ecosystem.as_ref(), &tmp_dir, chunk_size).await?;

        let records_dir = self.records_dir();
        let new_state = OsvState::build(&tmp_dir)?;
        // acquire lock during all manipulation with the data
        let mut state = self.write_state();
        if records_dir.exists() {
            std::fs::remove_dir_all(&records_dir)?;
        }
        // Replaces current records with the latest one
        std::fs::rename(&tmp_dir, records_dir)?;

        *state = new_state;

        Ok(())
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
        let tmp_dir = self.tmp_dir("osv-sync")?;
        let ecosystem = self.0.ecosystem;
        let last_modified = self.last_modified();

        let client = reqwest::Client::new();

        let csv_text = client
            .get(osv_modified_id_csv_url(ecosystem.as_ref()))
            .send()
            .await?
            .text()
            .await?;

        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(csv_text.as_bytes());

        for result in rdr.records() {
            let entry = OsvModifiedRecord::try_from_csv_record(&result?, ecosystem)?;

            if entry.modified <= last_modified {
                break;
            }

            let mut record_filename = PathBuf::from(&entry.id);
            record_filename.add_extension(OSV_RECORD_FILE_EXTENSION);

            simple_download_to(
                &client,
                &osv_record_url(Some(&entry.ecosystem), &entry.id),
                &tmp_dir.path().join(&record_filename),
            )
            .await?;
        }

        let new_state = OsvState::build(tmp_dir.path())?;

        let mut state = self.write_state();

        let records_dir = self.records_dir();
        for entry in std::fs::read_dir(tmp_dir.path())? {
            let entry = entry?;
            std::fs::rename(entry.path(), records_dir.join(entry.file_name()))?;
        }

        state.merge(new_state);
        Ok(())
    }
}

/// Downloads the OSV archive for the given [`OsvGsEcosystem`] (or all ecosystems if
/// [`None`]) from <https://storage.googleapis.com/osv-vulnerabilities> and extracts it into `path`.
async fn download_and_extract_osv_archive(
    ecosystem: Option<&OsvGsEcosystem>,
    path: impl AsRef<Path>,
    chunk_size: u64,
) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let zip_archive_path = path.as_ref().join("osv.zip");
    let archive = chuncked_download_to(
        &client,
        &osv_archive_url(ecosystem),
        chunk_size,
        &zip_archive_path,
    )
    .await?;

    let mut zip_archive = zip::ZipArchive::new(archive)?;
    zip_archive.extract(&path)?;
    std::fs::remove_file(&zip_archive_path)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use tempfile::TempDir;

    use super::*;

    /// Downloads the latest OSV database, reads `RUSTSEC-2024-0401`, removes all
    /// records modified at or before its `modified` timestamp, then asserts the
    /// record no longer exists. Then calls sync to re-download it and asserts it
    /// is present again.
    #[tokio::test]
    async fn simple_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let osv = OsvDb::new(Some(OsvGsEcosystem::CratesIo), tmp.path()).unwrap();

        let record_id = "RUSTSEC-2024-0401".to_string();
        assert!(osv.get_record(&record_id).unwrap().is_none());

        osv.download_latest(10 * 1024 * 1024).await.unwrap();

        let record = osv.get_record(&record_id).unwrap().unwrap();
        let package_name = record
            .affected
            .as_ref()
            .and_then(|v| v.first())
            .and_then(|a| a.package.as_ref())
            .map(|p| p.name.clone())
            .expect("RUSTSEC-2024-0401 must have at least one affected package");
        assert!(
            osv.get_record_id(&package_name)
                .is_some_and(|ids| ids.contains(&record_id))
        );

        // manipulates internal files, some existing records, to be able to test `sync` method
        let cutoff = record.modified;
        let records_dir = osv.records_dir();
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

        osv.write_state().last_modified = cutoff - chrono::Duration::nanoseconds(1);
        osv.sync().await.unwrap();

        assert!(osv.get_record(&record_id).unwrap().is_some());
    }
}
