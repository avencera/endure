# endure

Durable app state for Rust that feels like in-memory data.

Put persistent collections in a struct and derive `Durable`. Reads are plain field access. Writes are ordinary mutation inside a closure. When the closure returns, endure writes only the entries that changed to a [redb](https://github.com/cberner/redb) file in one transaction, and only after that commit does anyone else see the change. Restart the process and the state is still there.

```rust
use endure::{Durable, DurableCell, DurableMap, DurableVec};

#[derive(Durable)]
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

## Collections

| Type | Behaves like | Stored as |
|---|---|---|
| `DurableMap<K, V>` | an ordered map, with `get`, `get_mut`, `insert`, `remove`, `range`, `retain` | one row per entry |
| `DurableCell<T>` | a single value you read through `Deref` and assign through `DerefMut` | one row |
| `DurableVec<T>` | a `Vec`, with `push`, `pop`, `insert`, `remove`, `get_mut`, `retain`, `sort` | one row per element |

Pushing, inserting or removing one `DurableVec` element writes one row; nothing shifts on disk. Cloning any collection is O(1), because the data lives in structurally shared trees from [imbl](https://github.com/jneem/imbl).

Keys and values need `Clone` and serde's `Serialize` and `Deserialize`. Map keys also need `Ord`.

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

Updates run one at a time, in the order they were called. Inside the closure you work on a private copy, so a returned error, a panic or a failed commit leaves the state exactly as it was.

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

For changes serde can't absorb, list the earlier value types on the field, oldest first, and implement `From` between consecutive versions. Stored values are upgraded and rewritten in the same transaction that opens the state:

```rust
#[derive(Durable)]
struct App {
    #[durable(versions(ApprovalV1, ApprovalV2))]
    approvals: DurableMap<ApprovalId, Approval>, // ApprovalV1 -> ApprovalV2 -> Approval
}
```

To rename a field without losing its data, keep the old table name: `#[durable(table = "old_name")]`.

A build that finds data written by a newer version of a field refuses to open with `Error::NewerSchema`, instead of guessing.

## Custom storage

Any redb `StorageBackend` works, for example one that encrypts pages at rest:

```rust
let app = App::builder().backend(MyEncryptedBackend::new(key)).open()?;
```

## Guarantees

- The redb file is the single source of truth. Memory only ever holds committed state.
- Each update is one atomic transaction. `update` resolves after the data is durable.
- Updates apply in call order. Readers never see a change before it commits.
- If a commit fails, endure reloads the state from disk, so memory and disk never disagree. If that reload also fails, further updates return `Error::Poisoned` until the state is reopened.
- Only one process can open a file at a time.

## Limits

- The whole state is loaded into memory when it opens. It suits app state, settings, queues and indexes, not large datasets.
- Map keys are not versioned. Changing a key type needs a manual migration.
- Values must not use interior mutability such as `RefCell` or `Mutex`. Changes made through them bypass change tracking and are never saved.
- The derive implements `Clone` for the struct, so don't derive `Clone` as well.
- Commit hooks run while the writer lock is held, so they must not call `update`.

## Not yet

- An outbox for effects that must survive a crash, delivered at least once after commit
- Idempotent updates keyed by a request ID
- Lazily loaded maps for data too large to keep in memory

## License

Apache-2.0
