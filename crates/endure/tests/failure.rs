//! Storage failures injected through a wrapping backend

use std::fs::OpenOptions;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use endure::redb::StorageBackend;
use endure::redb::backends::FileBackend;
use endure::{Durable, DurableMap, Error};
use tempfile::TempDir;

#[derive(Debug, Clone, Copy, PartialEq)]
enum Fail {
    Write,
    Sync,
}

// fails the next `remaining` calls of one kind, then behaves normally
#[derive(Debug, Clone)]
struct Faults {
    kind: Fail,
    remaining: Arc<AtomicUsize>,
}

impl Faults {
    fn new(kind: Fail) -> Self {
        Self {
            kind,
            remaining: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn fail_next(&self, count: usize) {
        self.remaining.store(count, Ordering::SeqCst);
    }

    fn check(&self, kind: Fail) -> io::Result<()> {
        if kind == self.kind
            && self
                .remaining
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                    left.checked_sub(1)
                })
                .is_ok()
        {
            return Err(io::Error::other("injected failure"));
        }

        Ok(())
    }
}

#[derive(Debug)]
struct FlakyBackend {
    inner: FileBackend,
    faults: Faults,
}

impl FlakyBackend {
    fn open(path: &Path, faults: Faults) -> Self {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .unwrap();

        Self {
            inner: FileBackend::new(file).unwrap(),
            faults,
        }
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
        self.faults.check(Fail::Write)?;
        self.inner.set_len(len)
    }

    fn sync_data(&self) -> io::Result<()> {
        self.faults.check(Fail::Sync)?;
        self.inner.sync_data()
    }

    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.faults.check(Fail::Write)?;
        self.inner.write(offset, data)
    }

    fn close(&self) -> io::Result<()> {
        self.inner.close()
    }
}

#[derive(Clone, Durable)]
struct Store {
    entries: DurableMap<u32, String>,
    #[durable(skip)]
    cache: Vec<u32>,
}

fn reopenable(path: &Path, faults: &Faults) -> Durable<Store> {
    let path = path.to_path_buf();
    let faults = faults.clone();
    Store::builder()
        .backend_with(move || FlakyBackend::open(&path, faults.clone()))
        .open()
        .unwrap()
}

fn keys(store: &Durable<Store>) -> Vec<u32> {
    store.read().entries.keys().copied().collect()
}

fn keys_on_disk(path: &PathBuf) -> Vec<u32> {
    let store = Store::builder().path(path).open().unwrap();
    keys(&store)
}

fn failed_commit_recovers(kind: Fail) -> eyre::Result<()> {
    let dir = TempDir::new()?;
    let path = dir.path().join("flaky.redb");
    let faults = Faults::new(kind);

    let store = reopenable(&path, &faults);
    store.update_blocking(|s| {
        s.entries.insert(1, "kept".into());
        s.cache.push(7);
    })?;

    faults.fail_next(1);
    let result = store.update_blocking(|s| s.entries.insert(2, "maybe".into()));
    assert!(
        matches!(result, Err(Error::CommitFailed(_))),
        "got {result:?}"
    );

    // the handle reopened its storage instead of giving up, and kept memory-only fields
    assert!(!store.is_poisoned());
    assert_eq!(store.read().cache, vec![7]);

    store.update_blocking(|s| s.entries.insert(3, "after".into()))?;
    let in_memory = keys(&store);
    drop(store);

    assert_eq!(in_memory, keys_on_disk(&path), "memory must match disk");
    assert!(in_memory.contains(&1) && in_memory.contains(&3));
    Ok(())
}

#[test]
fn failed_write_recovers_with_memory_matching_disk() -> eyre::Result<()> {
    failed_commit_recovers(Fail::Write)
}

#[test]
fn failed_sync_recovers_with_memory_matching_disk() -> eyre::Result<()> {
    failed_commit_recovers(Fail::Sync)
}

#[test]
fn single_use_backend_poisons_after_a_failed_commit() -> eyre::Result<()> {
    let dir = TempDir::new()?;
    let path = dir.path().join("flaky.redb");
    let faults = Faults::new(Fail::Write);

    let store = Store::builder()
        .backend(FlakyBackend::open(&path, faults.clone()))
        .open()?;
    store.update_blocking(|s| s.entries.insert(1, "kept".into()))?;

    faults.fail_next(usize::MAX);
    let result = store.update_blocking(|s| s.entries.insert(2, "lost".into()));
    assert!(
        matches!(result, Err(Error::CommitFailed(_))),
        "got {result:?}"
    );
    assert!(store.is_poisoned());

    faults.fail_next(0);
    let next = store.update_blocking(|s| s.entries.insert(3, "refused".into()));
    assert!(matches!(next, Err(Error::Poisoned)), "got {next:?}");
    assert_eq!(keys(&store), [1]);

    drop(store);
    assert_eq!(keys_on_disk(&path), [1]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_failed_commit_recovers_on_the_writer_thread() -> eyre::Result<()> {
    let dir = TempDir::new()?;
    let path = dir.path().join("flaky.redb");
    let faults = Faults::new(Fail::Write);

    let store = reopenable(&path, &faults);
    store
        .update(|s| {
            s.entries.insert(1, "kept".into());
            s.cache.push(7);
        })
        .await?;

    let mut commits = store.subscribe();
    faults.fail_next(1);
    let result = store.update(|s| s.entries.insert(2, "maybe".into())).await;
    assert!(
        matches!(result, Err(Error::CommitFailed(_))),
        "got {result:?}"
    );
    assert!(!store.is_poisoned());
    assert_eq!(store.read().cache, vec![7]);

    // the reload is announced as a change to every field
    assert!(commits.borrow_and_update().changed("entries"));

    store
        .update(|s| s.entries.insert(3, "after".into()))
        .await?;
    let in_memory = keys(&store);
    drop(store);

    assert_eq!(in_memory, keys_on_disk(&path));
    Ok(())
}
