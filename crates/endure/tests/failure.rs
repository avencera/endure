//! Storage failures injected through a wrapping backend

use std::fs::OpenOptions;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use endure::redb::StorageBackend;
use endure::redb::backends::FileBackend;
use endure::{Durable, DurableMap, Error, UpdateError};
use tempfile::TempDir;

#[derive(Debug)]
struct FlakyBackend {
    inner: FileBackend,
    fail_writes: Arc<AtomicBool>,
}

impl FlakyBackend {
    fn open(path: &Path, fail_writes: Arc<AtomicBool>) -> Self {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .unwrap();
        Self {
            inner: FileBackend::new(file).unwrap(),
            fail_writes,
        }
    }

    fn check(&self) -> io::Result<()> {
        if self.fail_writes.load(Ordering::SeqCst) {
            return Err(io::Error::other("injected write failure"));
        }

        Ok(())
    }
}

impl StorageBackend for FlakyBackend {
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.inner.read(offset, out)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.check()?;
        self.inner.set_len(len)
    }

    fn sync_data(&self) -> io::Result<()> {
        self.check()?;
        self.inner.sync_data()
    }

    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.check()?;
        self.inner.write(offset, data)
    }

    fn close(&self) -> io::Result<()> {
        self.inner.close()
    }
}

#[derive(Durable)]
struct Store {
    entries: DurableMap<u32, String>,
}

fn open(path: &Path, fail_writes: &Arc<AtomicBool>) -> Durable<Store> {
    Store::builder()
        .backend(FlakyBackend::open(path, Arc::clone(fail_writes)))
        .open()
        .unwrap()
}

#[test]
fn failed_commit_leaves_memory_and_disk_in_agreement() -> eyre::Result<()> {
    let dir = TempDir::new()?;
    let path = dir.path().join("flaky.redb");
    let fail_writes = Arc::new(AtomicBool::new(false));

    let store = open(&path, &fail_writes);
    store.update_blocking(|s| s.entries.insert(1, "kept".into()))?;

    fail_writes.store(true, Ordering::SeqCst);
    let result = store.try_update_blocking(|s| {
        s.entries.insert(2, "lost".into());
        Ok::<_, std::convert::Infallible>(())
    });
    assert!(
        matches!(result, Err(UpdateError::Storage(_))),
        "got {result:?}"
    );

    // memory never shows the write that failed
    assert_eq!(
        store.read().entries.keys().copied().collect::<Vec<_>>(),
        [1]
    );

    // once storage recovers the handle either keeps working or asks to be reopened, never
    // silently diverging from disk
    fail_writes.store(false, Ordering::SeqCst);
    match store.update_blocking(|s| s.entries.insert(3, "after".into())) {
        Ok(_) => assert_eq!(
            store.read().entries.keys().copied().collect::<Vec<_>>(),
            [1, 3]
        ),
        Err(Error::Poisoned) => {}
        Err(other) => panic!("unexpected error after recovery: {other:?}"),
    }

    let expected: Vec<u32> = store.read().entries.keys().copied().collect();
    drop(store);

    let reopened = open(&path, &fail_writes);
    let on_disk: Vec<u32> = reopened.read().entries.keys().copied().collect();
    assert!(
        !on_disk.contains(&2),
        "a failed commit must not appear after reopening"
    );
    assert!(on_disk.contains(&1));
    assert!(
        on_disk == expected || on_disk == [1],
        "disk {on_disk:?}, memory had {expected:?}"
    );
    Ok(())
}
