# endure

Durable app state for Rust that feels like in-memory data.

Put persistent collections in a struct and derive `Clone` and `Durable`. Reads are plain field access. Writes are ordinary mutation inside a closure. When the closure returns, endure writes only the entries that changed to a [redb](https://github.com/cberner/redb) file in one transaction, and only after that commit does anyone else see the change. Restart the process and the state is still there.

```rust
use endure::{Durable, DurableCell, DurableMap, DurableVec};

#[derive(Clone, Durable)]
struct App {
    approvals: DurableMap<ApprovalId, Approval>,
    settings: DurableCell<Settings>,
    history: DurableVec<AuditEntry>,
}

let app = App::open()?;

// reads come from a committed snapshot, with no I/O
let pending = app.read().approvals.values().filter(|a| a.is_pending()).count();

// writes look like changing memory; the future resolves once they are on disk
app.update(|s| {
    s.settings.max_parallel = 4;
    s.history.push(AuditEntry::opened(now));
})
.await?;
```

## Similar ideas elsewhere

If you've used one of these, endure will feel familiar:

| Tool | What's alike | What's different |
|---|---|---|
| SwiftUI `@AppStorage` and the web's `localStorage` | Assign a value and it's still there next launch | Those hold single small values. endure holds typed maps and lists, and commits several changes atomically |
| SwiftData `@Model` | A macro on ordinary types makes them persistent, and changes are tracked for you | SwiftData saves in the background. endure saves when `update` returns, so you know the data is on disk |
| Jetpack DataStore (`updateData { }`, `data: Flow`) | Updates are atomic read-modify-write closures that run one at a time and finish once on disk. Readers watch a stream of the latest state | DataStore rewrites the whole object on every change. endure writes only the entries that changed |
| Clojure's `duratom` | An atom backed by storage: `swap!` with a function, and the result persists | endure tracks changes per entry instead of writing the whole value, and rolls back if the function fails |
| Python's `shelve` | A dict-like object backed by a file | endure is typed, transactional, and safe to share across threads |
| Cloudflare Durable Objects | One owner per state, transactional storage, and no reply until the write is durable | endure runs in your process against a local file |

## Collections

| Type | Behaves like | Stored as |
|---|---|---|
| `DurableMap<K, V>` | an ordered map, with `get`, `get_mut`, `insert`, `remove`, `range`, `retain` | one row per entry |
| `DurableCell<T>` | a single value you read through `Deref` and assign through `DerefMut` | one row |
| `DurableVec<T>` | a `Vec`, with `push`, `pop`, `insert`, `remove`, `get_mut`, `retain`, `sort` | one row per element |

Pushing, inserting or removing one `DurableVec` element writes one row; nothing shifts on disk. Cloning any collection is O(1), because the data lives in structurally shared trees from [imbl](https://github.com/jneem/imbl).

Keys and values need `Clone` and serde's `Serialize` and `Deserialize`. Map keys also need `Ord`. Before anything is written, endure checks that each changed value can be decoded again. A type that can't be read back, such as one using `skip_serializing_if` without `default`, is refused with `Error::Encode` instead of leaving a file that won't open.

Fields marked `#[durable(skip)]` live in memory only and start as `Default::default()`.

## Reading and writing

```rust
// read a snapshot; holding it never blocks writers
let state = app.read();
let approval = state.approvals.get(&id);

// write; returns whatever the closure returns
let count = app.update(|s| {
    s.approvals.insert(id, approval);
    s.approvals.len()
})
.await?;

// write with a chance to refuse; on Err nothing is saved, even changes made before the error
app.try_update(|s| {
    s.approvals.get_mut(&id).ok_or(NotFound)?.grant(until)?;
    Ok::<_, ApprovalError>(())
})
.await?;

// outside async code
app.update_blocking(|s| s.settings.max_parallel = 2)?;
```

Updates run one at a time, in the order they reach the writer lock, which is fair. Inside the closure you work on a private copy, so a returned error or a panic leaves the state exactly as it was. Once the closure has run, dropping the `update` future (a timeout, `select!`, a disconnected client) doesn't cancel the commit: it still finishes and becomes visible.

## Where the data lives

`App::open()` needs no configuration. The file is `<state dir>/<crate name>/<struct name>.redb`:

| OS | Default |
|---|---|
| Linux | `$XDG_STATE_HOME/<crate>/app.redb`, usually `~/.local/state/<crate>/app.redb` |
| macOS | `~/Library/Application Support/<crate>/app.redb` |
| Windows | `%LOCALAPPDATA%\<crate>\app.redb` |

`<crate>` is the package that defines the struct. The directory is created on first open; on Unix it is readable by the owner only. Use the builder to change any part:

```rust
let app = App::builder()
    .dir("/var/lib/myservice") // just the directory
    .name("approvals")         // just the file name: approvals.redb
    .open()?;

let app = App::builder().path("/tmp/state.redb").open()?; // an exact path
let app = App::memory();                                  // no file, for tests
```

`#[durable(name = "approvals")]` on the struct changes the default file name.

## Actors and services

The handle is cheap to clone and safe to share, so there is no store actor to write and no message enum per operation. An actor keeps the handle in its state and calls `update` from its handler:

```rust
// abbreviated ractor actor: Msg, Arguments and pre_start omitted
impl Actor for ApprovalActor {
    type State = Durable<App>;

    async fn handle(&self, _me: ActorRef<Msg>, msg: Msg, app: &mut Durable<App>) -> Result<(), ActorProcessingErr> {
        match msg {
            Msg::Grant { id, until, reply } => {
                let result = app.try_update(|s| grant(s, id, until)).await;
                reply.send(result.map_err(|e| e.to_string()))?; // sent only after the commit
            }
        }
        Ok(())
    }
}
```

The same calls work from an HTTP handler, a CLI, or a background task.

## Watching for changes

```rust
// a watch channel that always holds the latest commit
let mut commits = app.subscribe();
while commits.changed().await.is_ok() {
    let commit = commits.borrow_and_update().clone();
    if commit.changed("settings") {
        refresh_ui(&commit.snapshot.settings);
    }
}

// or a synchronous hook that runs after every commit, in order
app.on_commit(|commit| push_to_ui(commit.changed.to_vec()));
```

## Changing your types

Values are stored as MessagePack with named fields. Adding a field marked `#[serde(default)]` or removing a field needs nothing extra.

For changes serde can't absorb, list the earlier value types on the field with their version numbers, oldest first, and implement `From` between consecutive versions. The current type gets the next number. Stored values are upgraded and rewritten in the same transaction that opens the state:

```rust
#[derive(Clone, Durable)]
struct App {
    // ApprovalV1 -> ApprovalV2 -> Approval, which is version 3
    #[durable(versions(ApprovalV1 = 1, ApprovalV2 = 2))]
    approvals: DurableMap<ApprovalId, Approval>,
}
```

When no stored data needs a version any more, drop its entry. The numbers are explicit, so data older than the first listed version fails to open with `Error::OlderSchema` instead of being decoded as the wrong type. To drop every old entry, keep the current number with `#[durable(version = 3)]`.

To rename a field without losing its data, keep the old table name: `#[durable(table = "old_name")]`.

A build that finds data written by a newer version of a field refuses to open with `Error::NewerSchema`, instead of guessing.

## Custom storage

Any redb `StorageBackend` works, for example one that encrypts pages at rest. Pass a function that builds it, so endure can reopen the storage after an I/O error:

```rust
let app = App::builder()
    .backend_with(move || MyEncryptedBackend::open(&path, &key))
    .open()?;
```

`.backend(..)` takes a single backend instance instead. It can't be reopened, so an I/O error poisons the handle.

## Guarantees

- The redb file is the single source of truth. Memory only ever holds state read from it or committed to it.
- Each update is one atomic transaction. `update` resolves after the data is durable, and readers never see a change before it commits.
- Updates commit in the order they acquire the fair writer lock.
- A failed commit returns `Error::CommitFailed`. Its changes may or may not have reached storage, so endure reopens the storage and reloads, and `read()` then shows what was actually saved. Don't retry blindly; check the state first.
- If storage can't be reopened, the handle is poisoned: `is_poisoned()` returns true, updates return `Error::Poisoned`, and reads may be stale until you open the state again.
- Only one process can open a file at a time.

## Limits

- The whole state is loaded into memory when it opens. It suits app state, settings, queues and indexes, not large datasets.
- A `DurableCell` holds its value behind an `Arc`, so the first change in an update copies the whole value and the commit rewrites it. Keep large collections in a `DurableMap` or `DurableVec`.
- Map keys are not versioned. Changing a key type needs a manual migration. Keys that compare equal with `Ord` must also serialize identically.
- Values must not use interior mutability such as `RefCell` or `Mutex`. Changes made through them bypass change tracking and are never saved.
- Serde attributes that lose data, such as `#[serde(skip)]`, apply on disk too. After a restart a skipped field comes back as its default. `Option<Option<T>>` can't tell `Some(None)` from `None` once stored.
- If an `update` future is dropped while its commit is still finishing, `destroy` returns `Error::InUse` until the commit completes.
- Removing a field leaves its table in the file. Adding a field with the same name later reads the old data.
- `get_mut` and `DerefMut` mark an entry changed even if you don't modify it, so it is rewritten.
- The derive adds `open`, `builder` and `memory` functions to the struct, so it can't have its own functions with those names.
- Commit hooks run while the writer lock is held, so they must not call `update`. After an async update they run on endure's writer thread with your tokio runtime entered. A hook that captures a clone of the handle keeps the file open for the life of the process; use `subscribe` when the reaction needs the handle.

## Not yet

- An outbox for effects that must survive a crash, delivered at least once after commit
- Idempotent updates keyed by a request ID
- Lazily loaded maps for data too large to keep in memory

## License

Apache-2.0
