//! Integration test demonstrating the theoretical race between two concurrent
//! `download_latest` calls and `get_record`.
//!
//! `get_record` acquires the read lock only to obtain the records path, then
//! **releases it before performing any filesystem I/O**.  Both `download_latest`
//! calls race to swap the `records/` directory (`remove_dir_all` + `rename`).
//! A `get_record` that runs between those two operations will observe a missing
//! directory and return either `Ok(None)` or `Err`, even though the record
//! exists in the newly downloaded data.

#![allow(clippy::unwrap_used, clippy::arithmetic_side_effects)]

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use osv_db::{OsvDb, types::Ecosystem};
use tempfile::TempDir;

/// Two concurrent `download_latest` calls on clones of the same [`OsvDb`]
/// (which share the same `Arc<RwLock<…>>` inner) race to swap the `records/`
/// directory.  A third task continuously calls `get_record` for the entire
/// duration.
///
/// Because `get_record` releases the internal read-guard before any filesystem
/// I/O, it can land inside the `remove_dir_all` → `rename` window and observe
/// an absent or partially-replaced `records/` directory, returning `Ok(None)`
/// or `Err` for a record that should always be present once populated.
#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
async fn get_record_races_with_concurrent_download_latest() {
    let tmp = TempDir::new().unwrap();
    let db = OsvDb::new(Some(Ecosystem::CratesIo), tmp.path()).unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let stop_r = Arc::clone(&stop);

    // Two concurrent download_latest calls: both will race to swap the records
    // directory via remove_dir_all + rename under their respective write-locks.
    let dl1 = tokio::spawn({
        let db = db.clone();
        async move { db.download_latest().await }
    });
    let dl2 = tokio::spawn({
        let db = db.clone();
        async move { db.download_latest().await }
    });

    // Reader: runs for the entire download duration.  Any Ok(None) or Err(_)
    // result for a known record is evidence of the race having been observed.
    let reader = tokio::spawn(async move {
        let record_id = "RUSTSEC-2024-0401".to_string();
        loop {
            if db.get_record(&record_id).is_err() {
                return false;
            }
            if stop_r.load(Ordering::Relaxed) {
                return true;
            }
        }
    });

    let (r1, r2) = tokio::join!(dl1, dl2);
    r1.unwrap().unwrap();
    r2.unwrap().unwrap();

    stop.store(true, Ordering::Relaxed);
    assert!(reader.await.unwrap());
}
