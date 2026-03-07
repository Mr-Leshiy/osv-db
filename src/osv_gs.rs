//! OSV google storage URLs

use crate::types::Ecosystem;

const OSV_STORAGE_URL: &str = "https://storage.googleapis.com/osv-vulnerabilities";

pub fn osv_archive_url(ecosystem: Option<&Ecosystem>) -> String {
    match ecosystem {
        Some(ecosystem) => format!("{OSV_STORAGE_URL}/{ecosystem}/all.zip"),
        None => format!("{OSV_STORAGE_URL}/all.zip"),
    }
}

pub fn osv_modified_id_csv_url(ecosystem: Option<&Ecosystem>) -> String {
    match ecosystem {
        Some(ecosystem) => format!("{OSV_STORAGE_URL}/{ecosystem}/modified_id.csv"),
        None => format!("{OSV_STORAGE_URL}/modified_id.csv"),
    }
}

pub fn osv_record_url(
    ecosystem: Option<&Ecosystem>,
    record_path: &str,
) -> String {
    match ecosystem {
        Some(ecosystem) => format!("{OSV_STORAGE_URL}/{ecosystem}/{record_path}.json"),
        None => format!("{OSV_STORAGE_URL}/{record_path}.json"),
    }
}
