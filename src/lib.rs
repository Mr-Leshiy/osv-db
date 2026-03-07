#![doc = include_str!("../README.md")]

mod downloader;
mod osv_gs;
pub mod types;

use std::{
    fs::File,
    io::Cursor,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
};

use anyhow::Context;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::StreamExt;

pub use crate::osv_gs::OsvGsEcosystem;
use crate::{
    downloader::{chuncked_download_to, simple_download_to},
    osv_gs::{osv_archive_url, osv_modified_id_csv_url, osv_record_url},
    types::{OsvModifiedRecord, OsvRecord, OsvRecordId},
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
    /// The most recent `modified` timestamp seen across all records, stored as
    /// nanoseconds since the Unix epoch. Updated atomically after each
    /// [`OsvDb::download_latest`] or [`OsvDb::sync`] call. Defaults to `0` (Unix
    /// epoch) until the database is populated.
    last_modified: AtomicI64,
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
        Ok(Self(Arc::new(OsvDbInner {
            location: path.as_ref().to_path_buf(),
            ecosystem,
            last_modified: AtomicI64::default(),
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
        DateTime::<Utc>::from_timestamp_nanos(self.0.last_modified.load(Ordering::Acquire))
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

    /// Returns an async [`Stream`] over every [`OsvRecord`] stored in the database.
    ///
    /// Files are read and parsed asynchronously using [`tokio::fs`]. Each
    /// record is yielded as `Ok(`[`OsvRecord`]`)`. I/O or parse failures
    /// yield an [`Err`] item without terminating the stream.
    ///
    /// [`Stream`]: futures::Stream
    pub fn records_stream(
        &self
    ) -> anyhow::Result<impl futures::Stream<Item = anyhow::Result<OsvRecord>>> {
        use futures::StreamExt as _;
        let records_dir_content = std::fs::read_dir(self.records_dir())?;
        let stream = futures::stream::iter(records_dir_content)
            .filter_map(|entry| {
                async {
                    Some(entry).filter(|e| {
                        e.as_ref().is_ok_and(|e| {
                            e.path().extension().and_then(|e| e.to_str())
                                == Some(OSV_RECORD_FILE_EXTENSION)
                        })
                    })
                }
            })
            .then(|entry| {
                async move {
                    let entry = entry?;
                    let bytes = tokio::fs::read(entry.path()).await?;
                    let osv_record = serde_json::from_slice(&bytes)?;
                    anyhow::Ok(osv_record)
                }
            });
        Ok(stream)
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
        let client = reqwest::Client::new();
        download_and_extract_osv_archive(&client, self.0.ecosystem.as_ref(), &tmp_dir, chunk_size)
            .await?;

        let mut csv_rdr = download_osv_modified_csv(&client, self.0.ecosystem.as_ref()).await?;
        // The data inside the OSV modified csv file is strictly sorted in reverse chronological
        // order <https://google.github.io/osv.dev/data/#downloading-recent-changes>
        let first_record = csv_rdr
            .records()
            .next()
            .context("OSV modified csv file must have at least one entry")?;
        let first_osv_record =
            OsvModifiedRecord::try_from_csv_record(&first_record?, self.0.ecosystem)?;
        let new_last_modified = first_osv_record.modified;

        let records_dir = self.records_dir();
        if records_dir.exists() {
            std::fs::remove_dir_all(&records_dir)?;
        }
        // Atomically replaces the current records directory with the newly downloaded one.
        // rename(2) is guaranteed to be atomic on POSIX systems — see
        // <https://man7.org/linux/man-pages/man2/rename.2.html>.
        std::fs::rename(&tmp_dir, records_dir)?;

        let new_last_modified_timestamp_nanos = new_last_modified.timestamp_nanos_opt().context(format!("The date must be between 1677-09-21T00:12:43.145224192 and and 2262-04-11T23:47:16.854775807, provided: {new_last_modified}"))?;
        self.0
            .last_modified
            .store(new_last_modified_timestamp_nanos, Ordering::Release);

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
    ///
    /// Returns an async [`Stream`] that yields each newly added or updated [`OsvRecord`].
    ///
    /// [`Stream`]: futures::Stream
    pub async fn sync(
        &self
    ) -> anyhow::Result<impl futures::Stream<Item = anyhow::Result<OsvRecord>>> {
        let tmp_dir = self.tmp_dir("osv-sync")?;
        let ecosystem = self.0.ecosystem;
        let last_modified = self.last_modified();

        let client = reqwest::Client::new();
        let mut csv_rdr = download_osv_modified_csv(&client, ecosystem.as_ref()).await?;
        let mut new_last_modified = last_modified;
        for result in csv_rdr.records() {
            let entry = OsvModifiedRecord::try_from_csv_record(&result?, ecosystem)?;
            if entry.modified <= last_modified {
                break;
            }
            new_last_modified = new_last_modified.max(entry.modified);

            let mut record_filename = PathBuf::from(&entry.id);
            record_filename.add_extension(OSV_RECORD_FILE_EXTENSION);

            simple_download_to(
                &client,
                &osv_record_url(Some(&entry.ecosystem), &entry.id),
                &tmp_dir.path().join(&record_filename),
            )
            .await?;
        }

        let records_dir = self.records_dir();
        let mut new_record_paths = Vec::new();
        for entry in std::fs::read_dir(tmp_dir.path())? {
            let entry = entry?;
            let dest = records_dir.join(entry.file_name());
            // Atomically replaces the current records directory with the newly downloaded one.
            // rename(2) is guaranteed to be atomic on POSIX systems — see
            // <https://man7.org/linux/man-pages/man2/rename.2.html>.
            std::fs::rename(entry.path(), &dest)?;
            new_record_paths.push(dest);
        }

        let new_last_modified_timestamp_nanos = new_last_modified.timestamp_nanos_opt().context(format!("The date must be between 1677-09-21T00:12:43.145224192 and and 2262-04-11T23:47:16.854775807, provided: {new_last_modified}"))?;
        self.0
            .last_modified
            .store(new_last_modified_timestamp_nanos, Ordering::Release);

        let stream = futures::stream::iter(new_record_paths).then(|path| {
            async move {
                let bytes = tokio::fs::read(&path).await?;
                let osv_record = serde_json::from_slice(&bytes)?;
                anyhow::Ok(osv_record)
            }
        });

        Ok(stream)
    }
}

/// Downloads the OSV archive for the given [`OsvGsEcosystem`] (or all ecosystems if
/// [`None`]) from <https://storage.googleapis.com/osv-vulnerabilities> and extracts it into `path`.
async fn download_and_extract_osv_archive(
    client: &reqwest::Client,
    ecosystem: Option<&OsvGsEcosystem>,
    path: impl AsRef<Path>,
    chunk_size: u64,
) -> anyhow::Result<()> {
    let zip_archive_path = path.as_ref().join("osv.zip");
    let archive = chuncked_download_to(
        client,
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

async fn download_osv_modified_csv(
    client: &reqwest::Client,
    ecosystem: Option<&OsvGsEcosystem>,
) -> anyhow::Result<csv::Reader<Cursor<Bytes>>> {
    let csv_bytes = client
        .get(osv_modified_id_csv_url(ecosystem))
        .send()
        .await?
        .bytes()
        .await?;

    Ok(csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(Cursor::new(csv_bytes)))
}

#[cfg(test)]
mod tests {
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
        assert_eq!(record.id, record_id);

        // verify records_stream yields all records including our target
        let ids: Vec<String> = osv
            .records_stream()
            .unwrap()
            .map(|r| r.unwrap().id)
            .collect()
            .await;
        assert!(ids.contains(&record_id));
    }
}
