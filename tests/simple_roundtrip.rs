use std::fs::File;

use osv_db::{
    OsvDb,
    types::{Ecosystem, OsvRecord},
};
use tempfile::TempDir;

/// Downloads the latest OSV database, reads `RUSTSEC-2024-0401`, removes all
/// records modified at or before its `modified` timestamp, then asserts the
/// record no longer exists.
#[tokio::test]
async fn simple_roundtrip() {
    let tmp = TempDir::new().unwrap();
    let osv = OsvDb::new(Some(Ecosystem::CratesIo), tmp.path()).unwrap();

    let record_id = "RUSTSEC-2024-0401".to_string();
    assert!(osv.get_record(&record_id).unwrap().is_none());

    osv.download_latest().await.unwrap();

    let record = osv.get_record(&record_id).unwrap().unwrap();
    let cutoff = record.modified;

    let records_dir = tmp.path().join("records");
    for entry in std::fs::read_dir(&records_dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|e| e.to_str()) == Some("json") {
            let file = File::open(&path).unwrap();
            let r: OsvRecord = serde_json::from_reader(file).unwrap();
            if r.modified <= cutoff {
                std::fs::remove_file(&path).unwrap();
            }
        }
    }

    assert!(osv.get_record(&record_id).unwrap().is_none());
}
