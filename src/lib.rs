#![doc = include_str!("../README.md")]

mod downloader;
pub mod errors;
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

use bytes::Bytes;
use chrono::{DateTime, Utc};
use tempfile::tempdir_in;

pub use crate::osv_gs::{OsvGsEcosystem, OsvGsEcosystems};
use crate::{
    downloader::{chuncked_download_to, simple_download_to},
    errors::{
        DownloadLatestErr, DownloaderErr, GetRecordErr, OsvDbNewErr, ReadRecordErr,
        RecordsStreamErr, SyncErr,
    },
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
    /// The set of ecosystems this database targets. An empty set means all ecosystems.
    ecosystems: OsvGsEcosystems,
    /// The most recent `modified` timestamp seen across all records, stored as
    /// nanoseconds since the Unix epoch. Updated atomically after each
    /// [`OsvDb::download_latest`] or [`OsvDb::sync`] call. Defaults to `0` (Unix
    /// epoch) until the database is populated.
    last_modified: AtomicI64,
}

impl OsvDb {
    /// Creates a new [`OsvDb`] rooted at `path` targeting the given `ecosystems`.
    ///
    /// Pass [`OsvGsEcosystems::all`] to cover all ecosystems, or build a specific set
    /// with [`OsvGsEcosystems::add`].
    ///
    /// If `path` does not exist it is created (including all parent directories).
    ///
    /// # Errors
    /// - [`OsvDbNewErr`]
    pub fn new(
        ecosystems: OsvGsEcosystems,
        path: impl AsRef<Path>,
    ) -> Result<Self, OsvDbNewErr> {
        std::fs::create_dir_all(&path).map_err(OsvDbNewErr::Io)?;
        Ok(Self(Arc::new(OsvDbInner {
            location: path.as_ref().to_path_buf(),
            ecosystems,
            last_modified: AtomicI64::default(),
        })))
    }

    /// Returns the on disk location of the database.
    #[must_use]
    pub fn location(&self) -> &Path {
        &self.0.location
    }

    /// Returns the set of ecosystems this database targets.
    ///
    /// An empty set (i.e. [`OsvGsEcosystems::is_all`] is `true`) means all ecosystems.
    #[must_use]
    pub fn ecosystems(&self) -> &OsvGsEcosystems {
        &self.0.ecosystems
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
    ) -> Result<tempfile::TempDir, std::io::Error> {
        tempfile::Builder::new()
            .prefix(prefix)
            .tempdir_in(self.location())
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
    ) -> Result<Option<OsvRecord>, GetRecordErr> {
        let records_dir = self.records_dir();
        let mut record_path = records_dir.join(id);
        record_path.add_extension(OSV_RECORD_FILE_EXTENSION);
        if !record_path.exists() {
            return Ok(None);
        }
        let osv_record_file = File::open(record_path).map_err(GetRecordErr::Io)?;
        let osv_record = serde_json::from_reader(&osv_record_file).map_err(GetRecordErr::Json)?;
        Ok(Some(osv_record))
    }

    /// Returns an [`Iterator`] over every [`OsvRecord`] stored in the database.
    ///
    /// Files are read and parsed synchronously. Each record is yielded as
    /// `Ok(`[`OsvRecord`]`)`. I/O or parse failures yield an [`Err`] item
    /// without terminating the iterator.
    pub fn records(
        &self
    ) -> Result<impl Iterator<Item = Result<OsvRecord, ReadRecordErr>>, RecordsStreamErr> {
        let records_dir = self.records_dir();
        if !records_dir.exists() {
            let empty: Box<dyn Iterator<Item = Result<OsvRecord, ReadRecordErr>>> =
                Box::new(std::iter::empty());
            return Ok(empty);
        }

        let records_dir_content =
            std::fs::read_dir(records_dir).map_err(RecordsStreamErr::ReadDir)?;
        Ok(Box::new(
            records_dir_content
                .filter(|entry| {
                    entry.as_ref().is_ok_and(|e| {
                        e.path().extension().and_then(|ext| ext.to_str())
                            == Some(OSV_RECORD_FILE_EXTENSION)
                    })
                })
                .map(|entry| {
                    let entry = entry.map_err(ReadRecordErr::Io)?;
                    let bytes = std::fs::read(entry.path()).map_err(ReadRecordErr::Io)?;
                    let osv_record = serde_json::from_slice(&bytes).map_err(ReadRecordErr::Json)?;
                    Ok(osv_record)
                }),
        ))
    }

    /// Downloads a full, latest OSV database for all configured ecosystems.
    ///
    /// - For each targeted ecosystem (or the global archive when all ecosystems are
    ///   selected), downloads the latest archive into a temporary subdirectory of
    ///   `location` and extracts all records into a single flat directory.
    /// - Atomically replaces the current records directory with the newly downloaded one.
    /// - Updates `self.last_modified` with the maximum `modified` timestamp seen across
    ///   all targeted ecosystems.
    pub async fn download_latest(
        &self,
        chunk_size: u64,
    ) -> Result<(), DownloadLatestErr> {
        let tmp_dir = self
            .tmp_dir("osv-download")
            .map_err(DownloadLatestErr::Io)?;
        let client = reqwest::Client::new();

        let new_last_modified =
            download_latest_archives(&client, &self.0.ecosystems, &tmp_dir, chunk_size).await?;

        let records_dir = self.records_dir();
        if records_dir.exists() {
            std::fs::remove_dir_all(&records_dir).map_err(DownloadLatestErr::Io)?;
        }
        // Atomically replaces the current records directory with the newly downloaded one.
        // rename(2) is guaranteed to be atomic on POSIX systems — see
        // <https://man7.org/linux/man-pages/man2/rename.2.html>.
        std::fs::rename(&tmp_dir, records_dir).map_err(DownloadLatestErr::Io)?;

        let new_last_modified_timestamp_nanos = new_last_modified
            .timestamp_nanos_opt()
            .ok_or(DownloadLatestErr::TimestampOutOfRange(new_last_modified))?;
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
    /// Returns an [`Iterator`] that yields each newly added or updated [`OsvRecord`].
    pub async fn sync(
        &self
    ) -> Result<impl Iterator<Item = Result<OsvRecord, ReadRecordErr>>, SyncErr> {
        let tmp_dir = self.tmp_dir("osv-sync").map_err(SyncErr::Io)?;
        let last_modified = self.last_modified();

        let client = reqwest::Client::new();

        // Collect all records that need to be downloaded before spawning concurrent tasks.
        let (new_last_modified, entries_to_download) =
            collect_modified_entries(&client, &self.0.ecosystems, last_modified).await?;

        // Concurrently download all records.
        let mut tasks = tokio::task::JoinSet::new();
        for entry in entries_to_download {
            let client = client.clone();
            let tmp_path = tmp_dir.path().to_path_buf();
            tasks.spawn(async move {
                let mut record_filename = PathBuf::from(&entry.id);
                record_filename.add_extension(OSV_RECORD_FILE_EXTENSION);
                simple_download_to(
                    &client,
                    &osv_record_url(entry.ecosystem, &entry.id),
                    &tmp_path.join(&record_filename),
                )
                .await
                .map_err(SyncErr::Download)?;
                Ok::<(), SyncErr>(())
            });
        }
        while let Some(res) = tasks.join_next().await {
            res.map_err(SyncErr::Join)??;
        }

        let records_dir = self.records_dir();
        if !records_dir.exists() {
            std::fs::create_dir(&records_dir).map_err(SyncErr::Io)?;
        }
        let mut tasks = tokio::task::JoinSet::new();
        for entry in std::fs::read_dir(tmp_dir.path()).map_err(SyncErr::Io)? {
            let records_dir = records_dir.clone();
            tasks.spawn(async move {
                let entry = entry.map_err(SyncErr::Io)?;
                let dest = records_dir.join(entry.file_name());

                // Atomically replaces the current records directory with the newly
                // downloaded one. rename(2) is guaranteed
                // to be atomic on POSIX systems — see <https://man7.org/linux/man-pages/man2/rename.2.html>.
                tokio::fs::rename(entry.path(), &dest)
                    .await
                    .map_err(SyncErr::Io)?;
                Ok::<PathBuf, SyncErr>(dest)
            });
        }
        // resolve error right now, before modifying `last_modified` field
        let new_record_paths: Vec<PathBuf> = tasks
            .join_all()
            .await
            .into_iter()
            .collect::<Result<_, _>>()?;

        let new_last_modified_timestamp_nanos = new_last_modified
            .timestamp_nanos_opt()
            .ok_or(SyncErr::TimestampOutOfRange(new_last_modified))?;
        self.0
            .last_modified
            .store(new_last_modified_timestamp_nanos, Ordering::Release);

        Ok(new_record_paths.into_iter().map(|path| {
            let bytes = std::fs::read(&path).map_err(ReadRecordErr::Io)?;
            let osv_record = serde_json::from_slice(&bytes).map_err(ReadRecordErr::Json)?;
            Ok::<OsvRecord, ReadRecordErr>(osv_record)
        }))
    }
}

/// Downloads archives for all ecosystems in `ecosystems` into `path` and returns the
/// maximum `modified` timestamp seen across their `modified_id.csv` files.
///
/// When `ecosystems` targets all ecosystems, the single global archive is used.
/// Otherwise each ecosystem's archive is downloaded and extracted into the same
/// directory.
///
/// The `modified_id.csv` files are sorted in reverse chronological order, so the first
/// entry is always the most recently modified record for that ecosystem.
/// <https://google.github.io/osv.dev/data/#downloading-recent-changes>
async fn download_latest_archives(
    client: &reqwest::Client,
    ecosystems: &OsvGsEcosystems,
    path: impl AsRef<Path>,
    chunk_size: u64,
) -> Result<DateTime<Utc>, DownloadLatestErr> {
    if ecosystems.is_all() {
        download_archive_for_ecosystem(client, None, &path, chunk_size).await
    } else {
        let mut tasks = tokio::task::JoinSet::new();
        for eco in ecosystems.iter() {
            let client = client.clone();
            let path = path.as_ref().to_path_buf();
            tasks.spawn(async move {
                download_archive_for_ecosystem(&client, Some(eco), &path, chunk_size).await
            });
        }
        tasks
            .join_all()
            .await
            .into_iter()
            .try_fold(DateTime::<Utc>::MIN_UTC, |last_modified, new_modified| {
                Ok(last_modified.max(new_modified?))
            })
    }
}

/// Downloads and extracts the OSV archive for the given `ecosystem` (or the global
/// archive if [`None`]) into `path`, then reads the first entry of the
/// `modified_id.csv` and returns its `modified` timestamp.
async fn download_archive_for_ecosystem(
    client: &reqwest::Client,
    ecosystem: Option<OsvGsEcosystem>,
    path: impl AsRef<Path>,
    chunk_size: u64,
) -> Result<DateTime<Utc>, DownloadLatestErr> {
    download_and_extract_osv_archive(client, ecosystem, &path, chunk_size).await?;
    let mut csv_rdr = download_osv_modified_csv(client, ecosystem)
        .await
        .map_err(DownloadLatestErr::Download)?;
    let first_record = csv_rdr
        .records()
        .next()
        .ok_or(DownloadLatestErr::EmptyModifiedCsv)?;
    let entry = OsvModifiedRecord::try_from_csv_record(
        &first_record.map_err(DownloadLatestErr::Csv)?,
        ecosystem,
    )
    .map_err(DownloadLatestErr::ParseRecord)?;
    Ok(entry.modified)
}

/// Reads the `modified_id.csv` for each ecosystem in `ecosystems` and collects every
/// entry whose `modified` timestamp is strictly after `since`.
///
/// Each CSV is sorted in reverse chronological order, so reading stops as soon as an
/// entry at or before `since` is encountered.
///
/// Returns the updated maximum `modified` timestamp and the list of entries to download.
async fn collect_modified_entries(
    client: &reqwest::Client,
    ecosystems: &OsvGsEcosystems,
    since: DateTime<Utc>,
) -> Result<(DateTime<Utc>, Vec<OsvModifiedRecord>), SyncErr> {
    if ecosystems.is_all() {
        collect_entries_from_csv(client, None, since).await
    } else {
        let mut tasks = tokio::task::JoinSet::new();
        for eco in ecosystems.iter() {
            let client = client.clone();
            tasks.spawn(async move { collect_entries_from_csv(&client, Some(eco), since).await });
        }
        tasks.join_all().await.into_iter().try_fold(
            (since, Vec::new()),
            |(last_modified, mut entries), res| {
                let (new_modified, new_entries) = res?;
                entries.extend(new_entries);
                Ok((last_modified.max(new_modified), entries))
            },
        )
    }
}

/// Downloads and reads the `modified_id.csv` for the given `ecosystem` (or the global
/// index if [`None`]) and returns every entry whose `modified` timestamp is strictly
/// after `since`, along with the maximum `modified` timestamp seen.
///
/// The CSV is sorted in reverse chronological order, so reading stops as soon as an
/// entry at or before `since` is encountered.
async fn collect_entries_from_csv(
    client: &reqwest::Client,
    ecosystem: Option<OsvGsEcosystem>,
    since: DateTime<Utc>,
) -> Result<(DateTime<Utc>, Vec<OsvModifiedRecord>), SyncErr> {
    let mut new_last_modified = since;
    let mut entries = Vec::new();
    let mut csv_rdr = download_osv_modified_csv(client, ecosystem)
        .await
        .map_err(SyncErr::Download)?;
    for result in csv_rdr.records() {
        let entry =
            OsvModifiedRecord::try_from_csv_record(&result.map_err(SyncErr::Csv)?, ecosystem)
                .map_err(SyncErr::ParseRecord)?;
        if entry.modified <= since {
            break;
        }
        new_last_modified = new_last_modified.max(entry.modified);
        entries.push(entry);
    }
    Ok((new_last_modified, entries))
}

/// Downloads the OSV archive for the given [`OsvGsEcosystem`] (or all ecosystems if
/// [`None`]) from <https://storage.googleapis.com/osv-vulnerabilities> and extracts it into `path`.
async fn download_and_extract_osv_archive(
    client: &reqwest::Client,
    ecosystem: Option<OsvGsEcosystem>,
    path: impl AsRef<Path>,
    chunk_size: u64,
) -> Result<(), DownloadLatestErr> {
    let temp_zip_archive_dir = tempdir_in(&path).map_err(DownloadLatestErr::Io)?;
    let archive = chuncked_download_to(
        client,
        &osv_archive_url(ecosystem),
        chunk_size,
        temp_zip_archive_dir.path().join("osv.zip"),
    )
    .await
    .map_err(DownloadLatestErr::Download)?;
    let mut zip_archive = zip::ZipArchive::new(archive).map_err(DownloadLatestErr::Zip)?;
    zip_archive.extract(&path).map_err(DownloadLatestErr::Zip)?;
    Ok(())
}

async fn download_osv_modified_csv(
    client: &reqwest::Client,
    ecosystem: Option<OsvGsEcosystem>,
) -> Result<csv::Reader<Cursor<Bytes>>, DownloaderErr> {
    let csv_bytes = client
        .get(osv_modified_id_csv_url(ecosystem))
        .send()
        .await
        .map_err(DownloaderErr::Http)?
        .bytes()
        .await
        .map_err(DownloaderErr::Http)?;

    Ok(csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(Cursor::new(csv_bytes)))
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::atomic::Ordering};

    use tempfile::TempDir;

    use super::*;

    /// Downloads the latest OSV database, reads defiend record ids, removes all
    /// records modified at or before its `modified` timestamp, then asserts the
    /// record no longer exists. Then calls sync to re-download it and asserts it
    /// is present again.
    #[tokio::test]
    async fn download_latest_test() {
        let tmp = TempDir::new().unwrap();
        let osv = OsvDb::new(
            OsvGsEcosystems::all()
                .add(OsvGsEcosystem::CratesIo)
                .add(OsvGsEcosystem::Julia),
            tmp.path(),
        )
        .unwrap();

        let record_ids = [
            "RUSTSEC-2024-0401".to_string(),
            "JLSEC-2025-329".to_string(),
        ];

        for record_id in &record_ids {
            assert!(osv.get_record(record_id).unwrap().is_none());
        }

        osv.download_latest(10 * 1024 * 1024).await.unwrap();

        for record_id in &record_ids {
            let record = osv.get_record(record_id).unwrap().unwrap();
            assert_eq!(&record.id, record_id);
        }

        // verify records yields all records including our target
        let ids: HashSet<OsvRecordId> = osv.records().unwrap().map(|r| r.unwrap().id).collect();

        for record_id in &record_ids {
            assert!(ids.contains(record_id));
        }
    }

    /// Initialises an empty database, sets `last_modified` to the date of
    /// `RUSTSEC-2026-0032` (2026-03-05T00:00:00Z), then calls `sync`. Verifies:
    ///
    /// 1. `RUSTSEC-2026-0032` was not present before sync.
    /// 2. `RUSTSEC-2026-0032` exists after sync (it was modified at 2026-03-05T05:53:11Z,
    ///    which is strictly after the `last_modified` cutoff).
    /// 3. Every record returned by the `sync` stream is also present in `records_stream`.
    /// 4. Every record returned by the `sync` stream has `modified >= last_modified`.
    #[tokio::test]
    async fn sync_test() {
        // The date of RUSTSEC-2026-0032 (modified: 2026-03-05T05:53:11Z).
        // Using midnight so the record itself (modified later that day) is captured.
        let last_modified: DateTime<Utc> = "2026-03-05T00:00:00Z".parse().unwrap();

        let tmp = TempDir::new().unwrap();
        let osv = OsvDb::new(
            OsvGsEcosystems::all().add(OsvGsEcosystem::CratesIo),
            tmp.path(),
        )
        .unwrap();

        let record_id = "RUSTSEC-2026-0032".to_string();

        // DB is empty — record must not exist yet.
        assert!(osv.get_record(&record_id).unwrap().is_none());

        // Set last_modified to the date of RUSTSEC-2026-0032.
        osv.0.last_modified.store(
            last_modified.timestamp_nanos_opt().unwrap(),
            Ordering::Release,
        );

        let sync_records: Vec<OsvRecord> = osv.sync().await.unwrap().map(|r| r.unwrap()).collect();

        // RUSTSEC-2026-0032 must be present after sync.
        assert!(
            osv.get_record(&record_id).unwrap().is_some(),
            "RUSTSEC-2026-0032 should exist after sync"
        );

        let stream_ids: HashSet<String> = osv.records().unwrap().map(|r| r.unwrap().id).collect();

        for sync_record in &sync_records {
            assert!(
                stream_ids.contains(&sync_record.id),
                "sync record {} is missing from records",
                sync_record.id
            );
            assert!(
                sync_record.modified >= last_modified,
                "sync record {} has modified {} which is before last_modified {}",
                sync_record.id,
                sync_record.modified,
                last_modified
            );
        }
    }
}
