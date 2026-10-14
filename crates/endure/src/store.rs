use std::convert::Infallible;
use std::fmt;
use std::marker::PhantomData;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use arc_swap::ArcSwap;
use redb::{Database, StorageBackend};
use tokio::sync::{Mutex, watch};

use crate::error::{Error, Result, UpdateError};
use crate::field::{self, Changes, Loader, State};

/// A handle to durable state of type `S`
///
/// Cloning is cheap and every clone shares the same state. Reads return committed snapshots;
/// writes go through [`update`](Self::update) and friends, which run one at a time in call order
pub struct Durable<S: State> {
    inner: Arc<Inner<S>>,
}

struct Inner<S: State> {
    db: Database,
    path: Option<PathBuf>,
    current: ArcSwap<S>,
    // serializes writers so commits land in the order their updates ran
    writer: Mutex<()>,
    poisoned: AtomicBool,
    commits: watch::Sender<Commit<S>>,
    hooks: RwLock<Vec<Hook<S>>>,
}

type Hook<S> = Box<dyn Fn(&Commit<S>) + Send + Sync>;

/// A committed, immutable view of the state
///
/// Holding a snapshot never blocks writers; later commits produce new snapshots
pub struct Snapshot<S>(Arc<S>);

impl<S> Deref for Snapshot<S> {
    type Target = S;

    fn deref(&self) -> &S {
        &self.0
    }
}

impl<S> Clone for Snapshot<S> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<S: fmt::Debug> fmt::Debug for Snapshot<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// A notification that an update committed
pub struct Commit<S> {
    /// The state as of this commit
    pub snapshot: Snapshot<S>,
    /// Names of the persisted fields the commit changed
    pub changed: Arc<[&'static str]>,
}

impl<S> Clone for Commit<S> {
    fn clone(&self) -> Self {
        Self {
            snapshot: self.snapshot.clone(),
            changed: Arc::clone(&self.changed),
        }
    }
}

impl<S> Commit<S> {
    /// Returns true if the commit changed the named field
    pub fn changed(&self, field: &str) -> bool {
        self.changed.contains(&field)
    }
}

impl<S: State> Clone for Durable<S> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<S: State> fmt::Debug for Durable<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Durable")
            .field("state", &S::NAME)
            .field("path", &self.inner.path)
            .finish()
    }
}

impl<S: State> Durable<S> {
    /// Opens the state at its default location
    ///
    /// See [`Builder`] for the default location and how to change it
    pub fn open() -> Result<Self> {
        Self::builder().open()
    }

    /// Starts configuring where and how the state is stored
    pub fn builder() -> Builder<S> {
        Builder::new()
    }

    /// Opens a fresh state that lives only in memory, for tests and scratch use
    pub fn memory() -> Self {
        Self::builder()
            .in_memory()
            .open()
            .expect("opening an empty in-memory database cannot fail")
    }

    fn from_db(db: Database, path: Option<PathBuf>) -> Result<Self> {
        let state = load::<S>(&db)?;
        let snapshot = Snapshot(Arc::new(state));
        let (commits, _) = watch::channel(Commit {
            snapshot: snapshot.clone(),
            changed: Arc::from([]),
        });

        Ok(Self {
            inner: Arc::new(Inner {
                db,
                path,
                current: ArcSwap::new(snapshot.0),
                writer: Mutex::new(()),
                poisoned: AtomicBool::new(false),
                commits,
                hooks: RwLock::new(Vec::new()),
            }),
        })
    }

    /// Returns the latest committed snapshot
    ///
    /// This is a cheap pointer load with no I/O
    pub fn read(&self) -> Snapshot<S> {
        Snapshot(self.inner.current.load_full())
    }

    /// Returns the file backing this state, or `None` for in-memory and custom backends
    pub fn path(&self) -> Option<&Path> {
        self.inner.path.as_deref()
    }

    /// Applies `f` to a copy of the state and commits the changes it made
    ///
    /// The returned future resolves once the changes are durable. Other readers see them only
    /// after that point. If the commit fails, the state stays as it was
    pub async fn update<T>(&self, f: impl FnOnce(&mut S) -> T) -> Result<T> {
        self.try_update(|state| Ok::<_, Infallible>(f(state)))
            .await
            .map_err(storage_error)
    }

    /// Like [`update`](Self::update), but `f` can reject the update by returning an error
    ///
    /// On `Err`, nothing is written and the state stays as it was, even if `f` had already
    /// changed its copy
    pub async fn try_update<T, E>(
        &self,
        f: impl FnOnce(&mut S) -> Result<T, E>,
    ) -> Result<T, UpdateError<E>> {
        let _writer = self.inner.writer.lock().await;
        let (output, next, changes) = self.prepare(f)?;

        let (changes, result) = if changes.is_empty() {
            (changes, Ok(()))
        } else {
            commit_async(Arc::clone(&self.inner), changes).await
        };

        self.finish(next, changes, result)?;
        Ok(output)
    }

    /// Blocking form of [`update`](Self::update) for code outside an async runtime
    ///
    /// # Panics
    ///
    /// Panics if called from within an async runtime; use [`update`](Self::update) there
    pub fn update_blocking<T>(&self, f: impl FnOnce(&mut S) -> T) -> Result<T> {
        self.try_update_blocking(|state| Ok::<_, Infallible>(f(state)))
            .map_err(storage_error)
    }

    /// Blocking form of [`try_update`](Self::try_update) for code outside an async runtime
    ///
    /// # Panics
    ///
    /// Panics if called from within an async runtime; use [`try_update`](Self::try_update) there
    pub fn try_update_blocking<T, E>(
        &self,
        f: impl FnOnce(&mut S) -> Result<T, E>,
    ) -> Result<T, UpdateError<E>> {
        let _writer = self.inner.writer.blocking_lock();
        let (output, next, changes) = self.prepare(f)?;

        let result = if changes.is_empty() {
            Ok(())
        } else {
            write(&self.inner.db, &changes)
        };

        self.finish(next, changes, result)?;
        Ok(output)
    }

    /// Subscribes to commits; the receiver always holds the most recent one
    ///
    /// Use this to refresh a UI or wake a task when state changes
    pub fn subscribe(&self) -> watch::Receiver<Commit<S>> {
        self.inner.commits.subscribe()
    }

    /// Runs `hook` synchronously after every commit, in commit order
    ///
    /// Hooks run while the writer lock is held, so keep them short and never call `update`
    /// from inside one
    pub fn on_commit(&self, hook: impl Fn(&Commit<S>) + Send + Sync + 'static) {
        self.inner
            .hooks
            .write()
            .unwrap_or_else(|poison| poison.into_inner())
            .push(Box::new(hook));
    }

    /// Closes the state and deletes its file
    ///
    /// Fails with [`Error::InUse`] if other clones of this handle are alive
    pub fn destroy(self) -> Result<()> {
        let inner = Arc::try_unwrap(self.inner).map_err(|_| Error::InUse)?;
        let path = inner.path.clone();
        drop(inner);

        if let Some(path) = path {
            std::fs::remove_file(path)?;
        }

        Ok(())
    }

    fn prepare<T, E>(
        &self,
        f: impl FnOnce(&mut S) -> Result<T, E>,
    ) -> Result<(T, S, Changes), UpdateError<E>> {
        if self.inner.poisoned.load(Ordering::Acquire) {
            return Err(UpdateError::Storage(Error::Poisoned));
        }

        // the maps and lists inside are structurally shared, so this copy is cheap
        let mut next = S::clone(&self.inner.current.load());
        let output = f(&mut next).map_err(UpdateError::Rejected)?;

        let mut changes = Changes::default();
        next.take_changes(&mut changes)?;
        Ok((output, next, changes))
    }

    fn finish(&self, next: S, changes: Changes, result: Result<()>) -> Result<()> {
        if let Err(error) = result {
            self.recover();
            return Err(error);
        }

        let snapshot = Snapshot(Arc::new(next));
        self.inner.current.store(Arc::clone(&snapshot.0));

        if !changes.is_empty() {
            self.notify(Commit {
                snapshot,
                changed: Arc::from(changes.fields),
            });
        }

        Ok(())
    }

    // a failed commit may or may not have reached disk, so reload rather than guess; if even
    // that fails, refuse further writes until the state is reopened
    fn recover(&self) {
        match load::<S>(&self.inner.db) {
            Ok(state) => {
                let snapshot = Snapshot(Arc::new(state));
                self.inner.current.store(Arc::clone(&snapshot.0));
                let changed: Vec<&'static str> =
                    S::fields().iter().map(|field| field.name).collect();
                self.notify(Commit {
                    snapshot,
                    changed: Arc::from(changed),
                });
            }
            Err(_) => self.inner.poisoned.store(true, Ordering::Release),
        }
    }

    fn notify(&self, commit: Commit<S>) {
        for hook in self
            .inner
            .hooks
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .iter()
        {
            hook(&commit);
        }

        self.inner.commits.send_replace(commit);
    }
}

fn storage_error(error: UpdateError<Infallible>) -> Error {
    match error {
        UpdateError::Storage(error) => error,
        UpdateError::Rejected(never) => match never {},
    }
}

fn load<S: State>(db: &Database) -> Result<S> {
    let txn = db.begin_write()?;
    let state = {
        let mut loader = Loader::new(&txn)?;
        S::load(&mut loader)?
    };

    txn.commit()?;
    Ok(state)
}

fn write(db: &Database, changes: &Changes) -> Result<()> {
    let txn = db.begin_write()?;
    for (name, ops) in &changes.tables {
        let mut table = txn.open_table(field::table(name))?;
        field::apply(&mut table, ops)?;
    }

    txn.commit()?;
    Ok(())
}

#[cfg(feature = "tokio")]
async fn commit_async<S: State>(inner: Arc<Inner<S>>, changes: Changes) -> (Changes, Result<()>) {
    // keep the fsync off the async worker threads when a runtime is available
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => {
            let task = handle.spawn_blocking(move || {
                let result = write(&inner.db, &changes);
                (changes, result)
            });

            match task.await {
                Ok(outcome) => outcome,
                Err(error) => std::panic::resume_unwind(error.into_panic()),
            }
        }
        Err(_) => {
            let result = write(&inner.db, &changes);
            (changes, result)
        }
    }
}

#[cfg(not(feature = "tokio"))]
async fn commit_async<S: State>(inner: Arc<Inner<S>>, changes: Changes) -> (Changes, Result<()>) {
    let result = write(&inner.db, &changes);
    (changes, result)
}

type OpenBackend = Box<dyn FnOnce(&redb::Builder) -> Result<Database, redb::DatabaseError>>;

/// Configures where durable state is stored
///
/// By default the file is `<state dir>/<crate name>/<struct name>.redb`, where the state dir is
/// `$XDG_STATE_HOME` (usually `~/.local/state`) on Linux, `~/Library/Application Support` on
/// macOS, and `%LOCALAPPDATA%` on Windows. The crate name is the package that defines the struct
pub struct Builder<S> {
    dir: Option<PathBuf>,
    name: Option<String>,
    path: Option<PathBuf>,
    backend: Option<OpenBackend>,
    _state: PhantomData<fn() -> S>,
}

impl<S: State> Builder<S> {
    fn new() -> Self {
        Self {
            dir: None,
            name: None,
            path: None,
            backend: None,
            _state: PhantomData,
        }
    }

    /// Stores the file in `dir` instead of the default directory
    pub fn dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.dir = Some(dir.into());
        self
    }

    /// Names the file `<name>.redb` instead of using the struct name
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Uses exactly this file path, ignoring `dir` and `name`
    pub fn path(mut self, path: impl Into<PathBuf>) -> Self {
        self.path = Some(path.into());
        self
    }

    /// Stores the state through a custom redb storage backend, such as an encrypting one
    pub fn backend(mut self, backend: impl StorageBackend) -> Self {
        self.backend = Some(Box::new(move |builder| {
            builder.create_with_backend(backend)
        }));
        self
    }

    /// Keeps the state in memory only
    pub fn in_memory(self) -> Self {
        self.backend(redb::backends::InMemoryBackend::new())
    }

    /// Returns the file path this builder would open, or `None` for a custom backend
    pub fn resolved_path(&self) -> Result<Option<PathBuf>> {
        if self.backend.is_some() {
            return Ok(None);
        }

        if let Some(path) = &self.path {
            return Ok(Some(path.clone()));
        }

        let dir = match &self.dir {
            Some(dir) => dir.clone(),
            None => default_dir()?.join(S::APP),
        };

        let name = self.name.as_deref().unwrap_or(S::NAME);
        Ok(Some(dir.join(format!("{name}.redb"))))
    }

    /// Opens the state, creating the file and its directory if needed
    pub fn open(self) -> Result<Durable<S>> {
        let path = self.resolved_path()?;
        let builder = Database::builder();

        let db = match (self.backend, &path) {
            (Some(open), _) => open(&builder)?,
            (None, Some(path)) => {
                if let Some(parent) = path.parent() {
                    create_private_dir(parent)?;
                }

                builder.create(path)?
            }
            (None, None) => unreachable!("a builder without a backend always resolves a path"),
        };

        Durable::from_db(db, path)
    }
}

fn default_dir() -> Result<PathBuf> {
    dirs::state_dir()
        .or_else(dirs::data_local_dir)
        .ok_or(Error::NoDefaultDir)
}

#[cfg(unix)]
fn create_private_dir(dir: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;

    // app state can hold anything, so new directories are readable by the owner only
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)
}

#[cfg(not(unix))]
fn create_private_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)
}
