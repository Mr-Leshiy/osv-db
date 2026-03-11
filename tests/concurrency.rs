//! Integration test verifying that concurrent `download_latest` and `get_record`
//! calls are safe under the internal `Arc<RwLock<OsvDbInner>>` guard.
//!
//! `get_record` holds the **read lock** for the entire duration of its
//! filesystem I/O (path resolution, existence check, open, deserialize).
//! `download_latest` holds the **write lock** while atomically swapping the
//! `records/` directory (`remove_dir_all` + `rename`), so the two operations
//! are mutually exclusive — a reader can never observe a partially-replaced or
//! missing directory.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use osv_db::{OsvDb, OsvGsEcosystem, OsvGsEcosystems};
use tempfile::TempDir;

/// Spawns two concurrent `download_latest` tasks on clones of the same [`OsvDb`]
/// (sharing the same `Arc<RwLock<…>>` inner), plus a third task that continuously
/// calls `get_record` for the entire download duration.
///
/// The write lock serializes the directory swap in each `download_latest` call,
/// and the read lock held by `get_record` prevents it from running concurrently
/// with any write.  Therefore `get_record` must never return `Err` — it either
/// finds no database yet (`Ok(None)`) or returns the populated record (`Ok(Some(…))`).
#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
async fn get_record_races_with_concurrent_download_latest() {
    const CHUNK_SIZE: u64 = 10 * 1024 * 1024;

    let tmp = TempDir::new().unwrap();
    let db = OsvDb::new(
        OsvGsEcosystems::all().add(OsvGsEcosystem::CratesIo),
        tmp.path(),
    )
    .unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let stop_r = Arc::clone(&stop);

    // Two concurrent download_latest calls, each serialized by the write-lock
    // around the remove_dir_all + rename swap.
    let dl1 = tokio::spawn({
        let db = db.clone();
        async move { db.download_latest(CHUNK_SIZE).await }
    });
    let dl2 = tokio::spawn({
        let db = db.clone();
        async move { db.download_latest(CHUNK_SIZE).await }
    });

    // Reader: runs for the entire download duration.  An Err(_) result would
    // indicate that the RwLock failed to prevent a torn read.
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
