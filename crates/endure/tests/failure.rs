//! Storage failures injected through a wrapping backend

use std::fs::OpenOptions;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use endure::redb::backends::FileBackend;
use endure::redb::{Database, StorageBackend};
use endure::{Durable, DurableCell, DurableMap, Error};
use serde::{Deserialize, Deserializer, Serialize};
use tempfile::TempDir;

#[derive(Debug, Clone, Copy, PartialEq)]
enum Fail {
    Write,
    Sync,
    // the data is synced, then the call reports an error anyway
    AfterSync,
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
    fn open(path: &Path, faults: Faults) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        Ok(Self {
            inner: FileBackend::new(file).map_err(io::Error::other)?,
            faults,
        })
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
        self.inner.sync_data()?;
        self.faults.check(Fail::AfterSync)
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
fn a_commit_that_landed_keeps_its_memory_only_edits() -> eyre::Result<()> {
    let dir = TempDir::new()?;
    let path = dir.path().join("flaky.redb");
    let faults = Faults::new(Fail::AfterSync);

    let store = reopenable(&path, &faults);
    store.update_blocking(|s| s.cache.push(1))?;

    faults.fail_next(1);
    let result = store.update_blocking(|s| {
        s.entries.insert(2, "saved".into());
        s.cache.push(2);
    });

    assert!(
        matches!(result, Err(Error::CommitFailed(_))),
        "got {result:?}"
    );

    // the commit reached storage, so the cache edit from the same closure stands with it
    assert_eq!(keys(&store), [2]);
    assert_eq!(store.read().cache, [1, 2]);

    drop(store);
    assert_eq!(keys_on_disk(&path), [2]);
    Ok(())
}

#[test]
fn single_use_backend_poisons_after_a_failed_commit() -> eyre::Result<()> {
    let dir = TempDir::new()?;
    let path = dir.path().join("flaky.redb");
    let faults = Faults::new(Fail::Write);

    let store = Store::builder()
        .backend(FlakyBackend::open(&path, faults.clone())?)
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

#[test]
fn a_factory_that_fails_to_reopen_poisons_the_handle() -> eyre::Result<()> {
    let dir = TempDir::new()?;
    let path = dir.path().join("flaky.redb");
    let faults = Faults::new(Fail::Write);
    let reopen_allowed = Arc::new(std::sync::atomic::AtomicBool::new(true));

    let store = {
        let path = path.clone();
        let faults = faults.clone();
        let reopen_allowed = Arc::clone(&reopen_allowed);
        Store::builder()
            .backend_with(move || {
                if !reopen_allowed.load(Ordering::SeqCst) {
                    return Err(io::Error::other("storage is gone"));
                }

                FlakyBackend::open(&path, faults.clone())
            })
            .open()?
    };

    store.update_blocking(|s| s.entries.insert(1, "kept".into()))?;

    reopen_allowed.store(false, Ordering::SeqCst);
    faults.fail_next(1);
    let result = store.update_blocking(|s| s.entries.insert(2, "lost".into()));
    assert!(
        matches!(result, Err(Error::CommitFailed(_))),
        "got {result:?}"
    );

    assert!(store.is_poisoned());
    Ok(())
}

// a backend that panics in `sync_data` once, after the data was written
#[derive(Debug)]
struct PanickyBackend {
    inner: FileBackend,
    armed: Arc<std::sync::atomic::AtomicBool>,
}

impl StorageBackend for PanickyBackend {
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.inner.read(offset, out)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.inner.set_len(len)
    }

    fn sync_data(&self) -> io::Result<()> {
        self.inner.sync_data()?;
        if self.armed.swap(false, Ordering::SeqCst) {
            panic!("backend bug");
        }

        Ok(())
    }

    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.inner.write(offset, data)
    }

    fn close(&self) -> io::Result<()> {
        self.inner.close()
    }
}

#[test]
fn a_panicking_backend_fails_the_commit_and_reloads() -> eyre::Result<()> {
    let dir = TempDir::new()?;
    let path = dir.path().join("panicky.redb");
    let armed = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let store = {
        let path = path.clone();
        let armed = Arc::clone(&armed);
        Store::builder()
            .backend_with(move || {
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(&path)?;
                Ok(PanickyBackend {
                    inner: FileBackend::new(file).map_err(io::Error::other)?,
                    armed: Arc::clone(&armed),
                })
            })
            .open()?
    };

    store.update_blocking(|s| s.entries.insert(1, "one".into()))?;

    armed.store(true, Ordering::SeqCst);
    let result = store.update_blocking(|s| s.entries.insert(2, "two".into()));
    assert!(
        matches!(result, Err(Error::CommitFailed(_))),
        "got {result:?}"
    );

    assert!(!store.is_poisoned());

    let in_memory = keys(&store);
    drop(store);
    assert_eq!(in_memory, keys_on_disk(&path), "memory must match disk");
    Ok(())
}

// counts syncs, which every commit needs and a plain open and close does not
#[derive(Debug)]
struct CountingBackend {
    inner: FileBackend,
    syncs: Arc<AtomicUsize>,
}

impl StorageBackend for CountingBackend {
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.inner.read(offset, out)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.inner.set_len(len)
    }

    fn sync_data(&self) -> io::Result<()> {
        self.syncs.fetch_add(1, Ordering::SeqCst);
        self.inner.sync_data()
    }

    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.inner.write(offset, data)
    }

    fn close(&self) -> io::Result<()> {
        self.inner.close()
    }
}

#[test]
fn opening_a_current_file_commits_nothing() -> eyre::Result<()> {
    let dir = TempDir::new()?;
    let path = dir.path().join("counted.redb");
    let syncs = Arc::new(AtomicUsize::new(0));

    let backend = || -> eyre::Result<CountingBackend> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        Ok(CountingBackend {
            inner: FileBackend::new(file)?,
            syncs: Arc::clone(&syncs),
        })
    };

    let synced_by = |open: &dyn Fn() -> eyre::Result<()>| -> eyre::Result<usize> {
        let before = syncs.load(Ordering::SeqCst);
        open()?;
        Ok(syncs.load(Ordering::SeqCst) - before)
    };

    let store = Store::builder().backend(backend()?).open()?;
    store.update_blocking(|s| s.entries.insert(1, "one".into()))?;
    drop(store);

    // redb may sync while opening on its own; endure must add no commit of its own
    let bare = synced_by(&|| {
        drop(Database::builder().create_with_backend(backend()?)?);
        Ok(())
    })?;

    let endure = synced_by(&|| {
        let store = Store::builder().backend(backend()?).open()?;
        assert_eq!(keys(&store), [1]);
        Ok(())
    })?;

    assert_eq!(endure, bare, "opening a current file committed");
    Ok(())
}

// counts down decodes; the one that reaches zero panics
static DECODES_UNTIL_PANIC: AtomicUsize = AtomicUsize::new(0);

// panics in `deserialize` while armed, the way a buggy decoder might during a reload
#[derive(Clone, Default, Serialize)]
struct Touchy(u32);

impl<'de> Deserialize<'de> for Touchy {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let left = DECODES_UNTIL_PANIC.try_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
            left.checked_sub(1)
        });
        assert!(left != Ok(1), "injected decode failure");
        Ok(Self(u32::deserialize(deserializer)?))
    }
}

#[derive(Clone, Durable)]
struct TouchyStore {
    value: DurableCell<Touchy>,
}

#[test]
fn a_panic_while_reloading_poisons_the_handle() -> eyre::Result<()> {
    let dir = TempDir::new()?;
    let path = dir.path().join("touchy.redb");
    let faults = Faults::new(Fail::AfterSync);

    let store = {
        let path = path.clone();
        let faults = faults.clone();
        TouchyStore::builder()
            .backend_with(move || FlakyBackend::open(&path, faults.clone()))
            .open()?
    };
    store.update_blocking(|s| s.value.set(Touchy(1)))?;

    // the commit lands and reports an error; the first decode checks the new value before the
    // write, and the second, during the reload that follows, panics
    faults.fail_next(1);
    DECODES_UNTIL_PANIC.store(2, Ordering::SeqCst);
    let result = store.update_blocking(|s| s.value.set(Touchy(2)));
    DECODES_UNTIL_PANIC.store(0, Ordering::SeqCst);

    assert!(
        matches!(result, Err(Error::CommitFailed(_))),
        "got {result:?}"
    );
    assert!(store.is_poisoned());

    let next = store.update_blocking(|s| s.value.set(Touchy(3)));
    assert!(matches!(next, Err(Error::Poisoned)), "got {next:?}");
    Ok(())
}
