use std::convert::Infallible;
use std::fmt;
use std::marker::PhantomData;
use std::ops::Deref;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, mpsc};

use arc_swap::ArcSwap;
use redb::{Database, StorageBackend};
use tokio::sync::{OwnedMutexGuard, oneshot, watch};

use crate::error::{Error, Result, UpdateError};
use crate::field::{self, Changes, Loader, State};

/// A handle to durable state of type `S`
///
/// Cloning is cheap and every clone shares the same state. Reads return committed snapshots.
/// Writes go through [`update`](Self::update) and friends, which take a fair writer lock, so
/// updates commit in the order they reach it
pub struct Durable<S: State> {
    inner: Arc<Inner<S>>,
}

struct Inner<S: State> {
    // only touched while the writer lock is held; `None` once storage could not be reopened
    db: Mutex<Option<Database>>,
    // opens the storage again after an I/O error, when the source allows it
    reopen: Option<Reopen>,
    path: Option<PathBuf>,
    current: ArcSwap<S>,
    writer: Arc<tokio::sync::Mutex<()>>,
    poisoned: AtomicBool,
    commits: watch::Sender<Commit<S>>,
    hooks: ArcSwap<Vec<Arc<Hook<S>>>>,
    // runs async commits; started on the first one
    jobs: OnceLock<mpsc::Sender<Job<S>>>,
}

type Hook<S> = dyn Fn(&Commit<S>) + Send + Sync;
type Reopen = Box<dyn Fn() -> Result<Database> + Send + Sync>;

// an async commit handed to the writer thread together with the writer lock, so dropping the
// caller's future can never separate writing the changes from publishing them
struct Job<S: State> {
    inner: Arc<Inner<S>>,
    writer: OwnedMutexGuard<()>,
    next: S,
    changes: Changes,
    reply: oneshot::Sender<Result<()>>,
    // the caller's runtime, entered while hooks run so they can spawn tasks
    runtime: Option<tokio::runtime::Handle>,
}

impl<S: State> Job<S> {
    fn run(self) {
        let Self {
            inner,
            writer,
            next,
            changes,
            reply,
            runtime,
        } = self;

        let result = {
            let _runtime = runtime.as_ref().map(tokio::runtime::Handle::enter);
            let outcome = catch_unwind(AssertUnwindSafe(|| inner.commit(next, changes)));
            outcome.unwrap_or_else(|_| {
                inner.poisoned.store(true, Ordering::Release);
                Err(Error::Poisoned)
            })
        };

        // release the handle and the lock before replying, so a caller that destroys or
        // reopens the state right after the update finishes does not find it still in use
        drop(writer);
        drop(inner);

        // the caller may have stopped waiting; the commit and publish happened regardless
        let _ = reply.send(result);
    }
}

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

/// A notification that the state changed
pub struct Commit<S> {
    /// The state as of this commit
    pub snapshot: Snapshot<S>,
    /// Names of the persisted fields that changed
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
    /// Returns true if the named field changed
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

    /// Returns true if a failed commit left the handle unable to reload its state
    ///
    /// A poisoned handle refuses updates and its reads may be stale. Drop every handle and open
    /// the state again
    pub fn is_poisoned(&self) -> bool {
        self.inner.poisoned.load(Ordering::Acquire)
    }

    /// Applies `f` to a copy of the state and commits the changes it made
    ///
    /// Resolves once the changes are durable, and nobody else sees them before that. Once `f`
    /// has run, dropping the future does not cancel the commit
    ///
    /// On [`Error::CommitFailed`] the changes may or may not have been saved; the handle reloads
    /// from storage, so [`read`](Self::read) shows which
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
        let writer = Arc::clone(&self.inner.writer).lock_owned().await;
        let (output, next, changes) = self.inner.prepare(f)?;

        if changes.is_empty() {
            self.inner.publish(next, Vec::new());
            return Ok(output);
        }

        let (reply, done) = oneshot::channel();
        self.inner.submit(Job {
            inner: Arc::clone(&self.inner),
            writer,
            next,
            changes,
            reply,
            runtime: tokio::runtime::Handle::try_current().ok(),
        });

        match done.await {
            Ok(result) => result.map_err(UpdateError::Storage)?,
            Err(_) => return Err(UpdateError::Storage(Error::Poisoned)),
        }

        Ok(output)
    }

    /// Blocking form of [`update`](Self::update) for code outside an async runtime
    ///
    /// # Panics
    ///
    /// Panics if called from within an async runtime; use [`update`](Self::update) there, or
    /// call this from `spawn_blocking`
    pub fn update_blocking<T>(&self, f: impl FnOnce(&mut S) -> T) -> Result<T> {
        self.try_update_blocking(|state| Ok::<_, Infallible>(f(state)))
            .map_err(storage_error)
    }

    /// Blocking form of [`try_update`](Self::try_update) for code outside an async runtime
    ///
    /// # Panics
    ///
    /// Panics if called from within an async runtime; use [`try_update`](Self::try_update)
    /// there, or call this from `spawn_blocking`
    pub fn try_update_blocking<T, E>(
        &self,
        f: impl FnOnce(&mut S) -> Result<T, E>,
    ) -> Result<T, UpdateError<E>> {
        let _writer = self.inner.writer.blocking_lock();
        let (output, next, changes) = self.inner.prepare(f)?;
        self.inner.commit(next, changes)?;
        Ok(output)
    }

    /// Subscribes to changes; the receiver always holds the most recent one
    ///
    /// Use this to refresh a UI or wake a task when state changes
    pub fn subscribe(&self) -> watch::Receiver<Commit<S>> {
        self.inner.commits.subscribe()
    }

    /// Runs `hook` synchronously after every change, in commit order
    ///
    /// Hooks run while the writer lock is held, so keep them short and never call `update` from
    /// one. After an async update they run on endure's writer thread with the caller's tokio
    /// runtime entered, so `tokio::spawn` works. A hook that panics is skipped for that commit;
    /// the commit itself still succeeds.
    /// A hook that captures a clone of this handle keeps the state open until the process exits,
    /// so use [`subscribe`](Self::subscribe) when the reaction needs the handle
    pub fn on_commit(&self, hook: impl Fn(&Commit<S>) + Send + Sync + 'static) {
        let hook: Arc<Hook<S>> = Arc::new(hook);
        self.inner.hooks.rcu(|hooks| {
            let mut hooks = Vec::clone(hooks);
            hooks.push(Arc::clone(&hook));
            hooks
        });
    }

    /// Closes the state and deletes its file
    ///
    /// Fails with [`Error::InUse`] if other clones of this handle are alive, or while a commit
    /// from a dropped `update` future is still finishing
    pub fn destroy(self) -> Result<()> {
        let inner = Arc::try_unwrap(self.inner).map_err(|_| Error::InUse)?;
        let Some(path) = inner.path.clone() else {
            return Ok(());
        };

        // unlink while redb still holds the file lock, so nothing can open the path between
        // closing it and deleting it
        #[cfg(unix)]
        {
            let removed = std::fs::remove_file(&path);
            drop(inner);
            removed?;
        }

        // an open file can't be deleted here, so close first; another process could open the
        // path in between
        #[cfg(not(unix))]
        {
            drop(inner);
            std::fs::remove_file(&path)?;
        }

        Ok(())
    }

    fn from_db(db: Database, path: Option<PathBuf>, reopen: Option<Reopen>) -> Result<Self> {
        let state = load::<S>(&db, None)?;
        let snapshot = Snapshot(Arc::new(state));
        let (commits, _) = watch::channel(Commit {
            snapshot: snapshot.clone(),
            changed: Arc::from([]),
        });

        Ok(Self {
            inner: Arc::new(Inner {
                db: Mutex::new(Some(db)),
                reopen,
                path,
                current: ArcSwap::new(snapshot.0),
                writer: Arc::new(tokio::sync::Mutex::new(())),
                poisoned: AtomicBool::new(false),
                commits,
                hooks: ArcSwap::from_pointee(Vec::new()),
                jobs: OnceLock::new(),
            }),
        })
    }
}

impl<S: State> Inner<S> {
    fn prepare<T, E>(
        &self,
        f: impl FnOnce(&mut S) -> Result<T, E>,
    ) -> Result<(T, S, Changes), UpdateError<E>> {
        if self.poisoned.load(Ordering::Acquire) {
            return Err(UpdateError::Storage(Error::Poisoned));
        }

        // the maps and lists inside are structurally shared, so this copy is cheap
        let current = self.current.load_full();
        let mut next = S::clone(&current);
        let output = f(&mut next).map_err(UpdateError::Rejected)?;

        let mut changes = Changes::default();
        next.take_changes(&current, &mut changes)?;
        Ok((output, next, changes))
    }

    // writes the changes and publishes the new state; the caller holds the writer lock
    fn commit(&self, next: S, changes: Changes) -> Result<()> {
        if changes.is_empty() {
            self.publish(next, Vec::new());
            return Ok(());
        }

        // a panicking storage backend is treated like a failed commit, since the write may or
        // may not have landed
        let written = catch_unwind(AssertUnwindSafe(|| self.write(&changes)))
            .unwrap_or_else(|panic| Err(Error::BackendPanicked(panic_message(&panic))));

        match written {
            Ok(()) => {
                self.publish(next, changes.fields);
                Ok(())
            }
            Err(error) => {
                self.recover();
                Err(Error::CommitFailed(Box::new(error)))
            }
        }
    }

    fn write(&self, changes: &Changes) -> Result<()> {
        let db = self.db.lock().unwrap_or_else(|poison| poison.into_inner());
        let db = db.as_ref().ok_or(Error::Poisoned)?;

        let txn = db.begin_write()?;
        for (name, ops) in &changes.tables {
            let mut table = txn.open_table(field::table(name))?;
            field::apply(&mut table, ops)?;
        }

        txn.commit()?;
        Ok(())
    }

    // a failed commit may or may not have reached storage, so reload instead of guessing
    fn recover(&self) {
        let mut slot = self.db.lock().unwrap_or_else(|poison| poison.into_inner());
        let previous = self.current.load_full();

        if let Some(db) = slot.as_ref()
            && let Ok(state) = load::<S>(db, Some(&previous))
        {
            self.publish_reload(state);
            return;
        }

        // redb refuses all further work after an I/O error, so open the storage again; the old
        // handle is dropped first to release its file lock
        if let Some(reopen) = &self.reopen {
            *slot = None;

            let reopened = catch_unwind(AssertUnwindSafe(reopen))
                .unwrap_or_else(|panic| Err(Error::BackendPanicked(panic_message(&panic))))
                .and_then(|db| load::<S>(&db, Some(&previous)).map(|state| (db, state)));

            if let Ok((db, state)) = reopened {
                *slot = Some(db);
                self.publish_reload(state);
                return;
            }
        }

        self.poisoned.store(true, Ordering::Release);
    }

    fn publish_reload(&self, state: S) {
        let changed = S::fields().iter().map(|field| field.name).collect();
        self.publish(state, changed);
    }

    fn publish(&self, state: S, changed: Vec<&'static str>) {
        let snapshot = Snapshot(Arc::new(state));
        self.current.store(Arc::clone(&snapshot.0));

        if changed.is_empty() {
            return;
        }

        let commit = Commit {
            snapshot,
            changed: Arc::from(changed),
        };

        // subscribers first, so a misbehaving hook can never hide a commit from them
        self.commits.send_replace(commit.clone());

        for hook in self.hooks.load().iter() {
            let _ = catch_unwind(AssertUnwindSafe(|| hook(&commit)));
        }
    }

    fn submit(&self, job: Job<S>) {
        let sender = self.jobs.get_or_init(|| {
            let (sender, receiver) = mpsc::channel::<Job<S>>();
            std::thread::Builder::new()
                .name(format!("endure-writer-{}", S::NAME))
                .spawn(move || {
                    for job in receiver {
                        job.run();
                    }
                })
                .expect("spawn endure writer thread");

            sender
        });

        if let Err(mpsc::SendError(job)) = sender.send(job) {
            // the writer thread is gone, so commit here rather than lose the update
            job.run();
        }
    }
}

fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    panic
        .downcast_ref::<&str>()
        .map(|message| (*message).to_owned())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "storage backend panicked".to_owned())
}

fn storage_error(error: UpdateError<Infallible>) -> Error {
    match error {
        UpdateError::Storage(error) => error,
        UpdateError::Rejected(never) => match never {},
    }
}

fn load<S: State>(db: &Database, previous: Option<&S>) -> Result<S> {
    let txn = db.begin_write()?;
    let state = {
        let mut loader = Loader::new(&txn)?;
        S::load(&mut loader, previous)?
    };

    txn.commit()?;
    Ok(state)
}

type OpenOnce = Box<dyn FnOnce(&redb::Builder) -> Result<Database, redb::DatabaseError> + Send>;

enum Source {
    Default,
    Path(PathBuf),
    Backend(OpenOnce),
    BackendWith(Arc<dyn Fn() -> Result<Database> + Send + Sync>),
}

/// Configures where durable state is stored
///
/// By default the file is `<state dir>/<crate name>/<struct name>.redb`, where the state dir is
/// `$XDG_STATE_HOME` (usually `~/.local/state`) on Linux, `~/Library/Application Support` on
/// macOS, and `%LOCALAPPDATA%` on Windows. The crate name is the package that defines the struct
pub struct Builder<S> {
    dir: Option<PathBuf>,
    name: Option<String>,
    source: Source,
    _state: PhantomData<fn() -> S>,
}

impl<S: State> Builder<S> {
    fn new() -> Self {
        Self {
            dir: None,
            name: None,
            source: Source::Default,
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
        self.source = Source::Path(path.into());
        self
    }

    /// Stores the state through a custom redb storage backend, such as an encrypting one
    ///
    /// The backend is used once, so after an I/O error the handle cannot reopen it and becomes
    /// poisoned. Use [`backend_with`](Self::backend_with) to allow recovery
    pub fn backend(mut self, backend: impl StorageBackend) -> Self {
        self.source = Source::Backend(Box::new(move |builder| {
            builder.create_with_backend(backend)
        }));
        self
    }

    /// Stores the state through custom backends made by `make`, which is called again to reopen
    /// the storage after an I/O error
    ///
    /// An error from `make` fails the open, or poisons the handle if it happens while reopening
    pub fn backend_with<B: StorageBackend>(
        mut self,
        make: impl Fn() -> std::io::Result<B> + Send + Sync + 'static,
    ) -> Self {
        self.source = Source::BackendWith(Arc::new(move || {
            Ok(Database::builder().create_with_backend(make()?)?)
        }));
        self
    }

    /// Keeps the state in memory only
    pub fn in_memory(self) -> Self {
        self.backend(redb::backends::InMemoryBackend::new())
    }

    /// Returns the file path this builder would open, or `None` for a custom backend
    pub fn resolved_path(&self) -> Result<Option<PathBuf>> {
        match &self.source {
            Source::Path(path) => Ok(Some(path.clone())),
            Source::Backend(_) | Source::BackendWith(_) => Ok(None),
            Source::Default => {
                let dir = match &self.dir {
                    Some(dir) => dir.clone(),
                    None => default_dir()?.join(S::APP),
                };

                let name = self.name.as_deref().unwrap_or(S::NAME);
                Ok(Some(dir.join(format!("{name}.redb"))))
            }
        }
    }

    /// Opens the state, creating the file and its directory if needed
    pub fn open(self) -> Result<Durable<S>> {
        let path = self.resolved_path()?;

        match self.source {
            Source::Backend(open) => Durable::from_db(open(&Database::builder())?, None, None),
            Source::BackendWith(open) => {
                let db = open()?;
                Durable::from_db(db, None, Some(Box::new(move || open())))
            }
            Source::Default | Source::Path(_) => {
                // an absolute path keeps `destroy` and recovery pointed at this file even if the
                // process changes its working directory later
                let path = std::path::absolute(path.expect("file sources always resolve a path"))?;
                if let Some(parent) = path
                    .parent()
                    .filter(|parent| !parent.as_os_str().is_empty())
                {
                    create_private_dir(parent)?;
                }

                create_private_file(&path)?;
                let db = Database::create(&path)?;

                let reopen_path = path.clone();
                // reopening must find the same file; creating an empty one would lose the state
                let reopen: Reopen = Box::new(move || Ok(Database::open(&reopen_path)?));
                Durable::from_db(db, Some(path), Some(reopen))
            }
        }
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

#[cfg(unix)]
fn create_private_file(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::OpenOptionsExt;

    // create the file owner-only before redb opens it; an existing file keeps its mode
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
    {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(error) => Err(error),
    }
}

#[cfg(not(unix))]
fn create_private_file(_path: &Path) -> std::io::Result<()> {
    Ok(())
}
