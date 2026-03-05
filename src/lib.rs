mod downloader;
pub mod types;

use std::{
    fs::File,
    path::{Path, PathBuf},
};

use anyhow::Context;
use chrono::{DateTime, Utc};

use crate::{
    downloader::chuncked_download_to,
    types::{Ecosystem, OsvModifiedRecord, OsvRecord, OsvRecordId},
};

const OSV_RECORD_FILE_EXTENSION: &str = "json";

pub struct OsvDb {
    /// On disk location of the OSV data
    location: PathBuf,
    /// Ecosystem this database was initialised for, or [`None`] for all ecosystems
    ecosystem: Option<Ecosystem>,
    /// The latest `modified` timestamp across all records in the database
    pub last_modified: DateTime<Utc>,
}

impl OsvDb {
    /// Initializes an OSV database for the provided [`Ecosystem`].
    /// If provided ecosystem is [`None`], initialise for all ecosystems.
    /// - Downloads the latest archive from <https://storage.googleapis.com/osv-vulnerabilities>
    /// - Unfolds it to the provided [`path`]
    pub async fn init(
        ecosystem: Option<Ecosystem>,
        path: impl AsRef<Path>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            path.as_ref().is_dir(),
            "Provided `path` must be a directory"
        );

        download_and_extract_osv_archive(ecosystem.as_ref(), &path).await?;
        let last_modified = index(&path)?;

        Ok(Self {
            location: path.as_ref().to_path_buf(),
            ecosystem,
            last_modified,
        })
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
            .get(modified_id_csv_url(self.ecosystem.as_ref()))
            .send()
            .await?
            .text()
            .await?;

        let mut new_last_modified = self.last_modified;

        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(csv_text.as_bytes());

        for result in rdr.records() {
            let entry = OsvModifiedRecord::try_from(result?)?;

            if entry.modified <= self.last_modified {
                break;
            }

            new_last_modified = new_last_modified.max(entry.modified);

            let mut local_path = self.location.join(&entry.id);
            if let Some(parent) = local_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            local_path.add_extension(OSV_RECORD_FILE_EXTENSION);

            let record_bytes = client
                .get(osv_record_url(self.ecosystem.as_ref(), &entry.id))
                .send()
                .await?
                .bytes()
                .await?;
            std::fs::write(&local_path, &record_bytes)?;
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
fn index(path: impl AsRef<Path>) -> anyhow::Result<DateTime<Utc>> {
    std::fs::read_dir(path.as_ref())
        .context("failed to read database directory")?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.extension()?.to_str()? == OSV_RECORD_FILE_EXTENSION {
                Some(path)
            } else {
                None
            }
        })
        .try_fold(DateTime::<Utc>::MIN_UTC, |max, path| {
            let file =
                File::open(&path).with_context(|| format!("failed to open {}", path.display()))?;
            let record: OsvRecord = serde_json::from_reader(file)
                .with_context(|| format!("failed to deserialize {}", path.display()))?;
            Ok(max.max(record.modified))
        })
}

const OSV_STORAGE_URL: &str = "https://storage.googleapis.com/osv-vulnerabilities";

fn osv_archive_url(ecosystem: Option<&Ecosystem>) -> String {
    match ecosystem {
        Some(ecosystem) => format!("{OSV_STORAGE_URL}/{ecosystem}/all.zip"),
        None => format!("{OSV_STORAGE_URL}/all.zip"),
    }
}

fn modified_id_csv_url(ecosystem: Option<&Ecosystem>) -> String {
    match ecosystem {
        Some(ecosystem) => format!("{OSV_STORAGE_URL}/{ecosystem}/modified_id.csv"),
        None => format!("{OSV_STORAGE_URL}/modified_id.csv"),
    }
}

fn osv_record_url(
    ecosystem: Option<&Ecosystem>,
    record_path: &str,
) -> String {
    match ecosystem {
        Some(ecosystem) => format!("{OSV_STORAGE_URL}/{ecosystem}/{record_path}.json"),
        None => format!("{OSV_STORAGE_URL}/{record_path}.json"),
    }
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;
    use test_case::test_case;

    use super::*;

    #[test_case(Ecosystem::CratesIo)]
    #[tokio::test]
    async fn osv_db_init_test(ecosystem: Ecosystem) {
        let tmp = TempDir::new("osv_downloader").unwrap();
        let osv = OsvDb::init(Some(ecosystem), tmp.path()).await.unwrap();

        assert!(
            osv.get_record(&"RUSTSEC-2024-0401".to_string())
                .unwrap()
                .is_some()
        );
    }
}
