use chrono::{DateTime, Utc};
use thiserror::Error;

/// Error returned by downloader operations (HTTP and I/O).
#[derive(Debug, Error)]
pub enum DownloaderErr {
    /// An HTTP request failed.
    #[error("HTTP request failed: {0}")]
    Http(#[source] reqwest::Error),
    /// A file-system I/O operation failed.
    #[error("I/O error: {0}")]
    Io(#[source] std::io::Error),
}

/// Error returned by `OsvDb::new`.
#[derive(Debug, Error)]
pub enum OsvDbNewErr {
    /// Failed to create or access the database directory.
    #[error("failed to create or access the database directory: {0}")]
    Io(#[source] std::io::Error),
}

/// Error returned by `OsvDb::clear`.
#[derive(Debug, Error)]
pub enum ClearErr {
    /// A file-system I/O operation failed.
    #[error("I/O error: {0}")]
    Io(#[source] std::io::Error),
}

/// Error returned by `OsvDb::get_record`.
#[derive(Debug, Error)]
pub enum GetRecordErr {
    /// Failed to open or read the record file.
    #[error("failed to open or read record file: {0}")]
    Io(#[source] std::io::Error),
    /// Failed to deserialize the record JSON.
    #[error("failed to deserialize OSV record: {0}")]
    Json(#[source] serde_json::Error),
}

/// Error returned by `OsvDb::records`.
#[derive(Debug, Error)]
pub enum RecordsStreamErr {
    /// Failed to open the records directory for reading.
    #[error("failed to read the records directory: {0}")]
    ReadDir(#[source] std::io::Error),
}

/// Error yielded by individual iterator items from `OsvDb::records` and
/// `OsvDb::sync`.
#[derive(Debug, Error)]
pub enum ReadRecordErr {
    /// Failed to read the record file from disk.
    #[error("failed to read record file: {0}")]
    Io(#[source] std::io::Error),
    /// Failed to deserialize the record JSON.
    #[error("failed to deserialize OSV record: {0}")]
    Json(#[source] serde_json::Error),
}

/// Error returned by `OsvModifiedRecord::try_from_csv_record`.
#[derive(Debug, Error)]
pub enum ParseModifiedRecordErr {
    /// The CSV row did not have exactly 2 columns.
    #[error("expected 2 columns, got {0}")]
    WrongColumnCount(usize),
    /// The timestamp column (index 0) was absent.
    #[error("missing timestamp column")]
    MissingTimestamp,
    /// The path column (index 1) was absent.
    #[error("missing path column")]
    MissingPath,
    /// The timestamp string could not be parsed as an RFC 3339 date-time.
    #[error("invalid timestamp: {0}")]
    ParseTimestamp(#[source] chrono::ParseError),
    /// The ecosystem string did not match any known variant.
    #[error("invalid ecosystem: {0}")]
    ParseEcosystem(#[source] strum::ParseError),
    /// The path column did not have the expected `<ecosystem>/<id>` format.
    #[error("invalid path format, expected `<ecosystem>/<id>`, got: `{0}`")]
    InvalidPathFormat(String),
}

/// Error returned by `OsvDb::download_latest`.
#[derive(Debug, Error)]
pub enum DownloadLatestErr {
    /// A file-system I/O operation failed.
    #[error("I/O error: {0}")]
    Io(#[source] std::io::Error),
    /// A network download operation failed.
    #[error("download error: {0}")]
    Download(#[source] DownloaderErr),
    /// A ZIP archive operation failed.
    #[error("ZIP archive error: {0}")]
    Zip(#[source] zip::result::ZipError),
    /// A CSV parsing operation failed.
    #[error("CSV parsing error: {0}")]
    Csv(#[source] csv::Error),
    /// A `modified_id.csv` row could not be parsed.
    #[error("failed to parse modified record: {0}")]
    ParseRecord(#[source] ParseModifiedRecordErr),
    /// The `modified_id.csv` file contained no rows.
    #[error("modified_id.csv contains no entries")]
    EmptyModifiedCsv,
    /// The `modified` timestamp cannot be represented as nanoseconds.
    #[error("timestamp `{0}` is outside the representable nanosecond range")]
    TimestampOutOfRange(DateTime<Utc>),
}

/// Error returned by `OsvDb::sync`.
#[derive(Debug, Error)]
pub enum SyncErr {
    /// A file-system I/O operation failed.
    #[error("I/O error: {0}")]
    Io(#[source] std::io::Error),
    /// A network download operation failed.
    #[error("download error: {0}")]
    Download(#[source] DownloaderErr),
    /// A CSV parsing operation failed.
    #[error("CSV parsing error: {0}")]
    Csv(#[source] csv::Error),
    /// A `modified_id.csv` row could not be parsed.
    #[error("failed to parse modified record: {0}")]
    ParseRecord(#[source] ParseModifiedRecordErr),
    /// A spawned background task panicked.
    #[error("background task panicked: {0}")]
    Join(#[source] tokio::task::JoinError),
    /// The `modified` timestamp cannot be represented as nanoseconds.
    #[error("timestamp `{0}` is outside the representable nanosecond range")]
    TimestampOutOfRange(DateTime<Utc>),
}
