mod downloader;
mod osv_gs;
pub mod types;

use std::{
    fs::File,
    path::{Path, PathBuf},
};

use anyhow::Context;
use chrono::{DateTime, Utc};

use crate::{
    downloader::{chuncked_download_to, simple_download_to},
    osv_gs::{osv_archive_url, osv_modified_id_csv_url, osv_record_url},
    types::{Ecosystem, OsvModifiedRecord, OsvRecord, OsvRecordId},
};

const OSV_RECORD_FILE_EXTENSION: &str = "json";

pub struct OsvDb {
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
            "Provided `path` must be a directory"
        );
        Ok(Self {
            location: path.as_ref().to_path_buf(),
            ecosystem,
            last_modified: DateTime::<Utc>::MIN_UTC,
        })
    }

    /// Initializes an OSV database for the provided [`Ecosystem`].
    /// If provided ecosystem is [`None`], initialise for all ecosystems.
    /// - Downloads the latest archive from <https://storage.googleapis.com/osv-vulnerabilities>
    /// - Unfolds it to the provided `path`
    pub async fn init(
        ecosystem: Option<Ecosystem>,
        path: impl AsRef<Path>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            path.as_ref().is_dir(),
            "Provided `path` must be a directory"
        );

        download_and_extract_osv_archive(ecosystem.as_ref(), &path).await?;
        let last_modified = last_modified(&path)?;

        Ok(Self {
            location: path.as_ref().to_path_buf(),
            ecosystem,
            last_modified,
        })
    }

    /// Downloads a full, latest OSV database for the provided [`Ecosystem`].
    /// If provided ecosystem is [`None`], initialise for all ecosystems.
    /// - Downloads the latest archive from <https://storage.googleapis.com/osv-vulnerabilities>
    /// - Unfolds it to the provided `path`
    /// - Scans all `.json` files in `path`, deserializes them as [`OsvRecord`]s, and
    ///   updates `self.last_modified` field with the maximum [`OsvRecord::modified`]
    ///   timestamp found across all records.
    pub async fn download_latest(&mut self) -> anyhow::Result<()> {
        download_and_extract_osv_archive(self.ecosystem.as_ref(), &self.location).await?;
        self.last_modified = last_modified(&self.location)?;
        Ok(())
    }

    /// Returns the on disk location of the database
    #[must_use]
    pub fn location(&self) -> &Path {
        &self.location
    }

    pub fn get_record(
        &self,
        id: &OsvRecordId,
    ) -> anyhow::Result<Option<OsvRecord>> {
        let mut record_path = self.location.join(id);
        record_path.add_extension(OSV_RECORD_FILE_EXTENSION);
        if !record_path.exists() {
            return Ok(None);
        }
        let osv_record_file = File::open(record_path)?;
        let osv_record = serde_json::from_reader(&osv_record_file)?;
        Ok(Some(osv_record))
    }

    /// Downloads only the records that have been modified since [`Self::last_modified`]
    /// and updates the local database files accordingly.
    ///
    /// Fetches the `modified_id.csv` index for the configured ecosystem (or all
    /// ecosystems if [`None`]). The file is sorted in reverse chronological order, so
    /// parsing stops as soon as a timestamp at or before [`Self::last_modified`] is
    /// encountered, avoiding a full re-download. After all new records are saved,
    /// [`Self::last_modified`] is updated to the highest timestamp seen.
    pub async fn update(&mut self) -> anyhow::Result<()> {
        let client = reqwest::Client::new();

        let csv_text = client
            .get(osv_modified_id_csv_url(self.ecosystem.as_ref()))
            .send()
            .await?
            .text()
            .await?;

        let mut new_last_modified = self.last_modified;

        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(csv_text.as_bytes());

        for result in rdr.records() {
            let entry = OsvModifiedRecord::try_from_csv_record(&result?, self.ecosystem)?;

            if entry.modified <= self.last_modified {
                break;
            }

            new_last_modified = new_last_modified.max(entry.modified);

            let mut record_path = self.location.join(&entry.id);
            record_path.add_extension(OSV_RECORD_FILE_EXTENSION);

            simple_download_to(
                &client,
                &osv_record_url(self.ecosystem.as_ref(), &entry.id),
                record_path,
            )
            .await?;
        }

        self.last_modified = new_last_modified;
        Ok(())
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
    use tempfile::TempDir;
    use test_case::test_case;

    use super::*;

    #[test_case(Ecosystem::CratesIo)]
    #[tokio::test]
    async fn osv_db_init_test(ecosystem: Ecosystem) {
        let tmp = TempDir::new().unwrap();
        let osv = OsvDb::init(Some(ecosystem), tmp.path()).await.unwrap();

        assert!(
            osv.get_record(&"RUSTSEC-2024-0401".to_string())
                .unwrap()
                .is_some()
        );
    }
}
