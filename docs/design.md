# Design

endure gives an application one handle to state that reads like memory and survives restarts. This document explains how it works and why it works that way.

## Goals

- Reading state should be plain field access, with no I/O and no async.
- Changing state should be ordinary mutation, with no save calls, dirty flags, row mapping or message enums.
- The same handle should work in actors, async services, and blocking code.
- A process restart must lose nothing that a caller was told was saved.

## One source of truth

The redb file is the source of truth. The in-memory state is a copy of the last committed state, and only the writer that just committed replaces it. Nothing else can change memory, so memory and disk can't drift apart.

Each `update` runs these steps while holding a FIFO writer lock:

1. Clone the current snapshot. The collections are persistent trees (`imbl::OrdMap` and `imbl::Vector`), and cells hold an `Arc`, so this clone is O(number of fields), not O(data).
2. Run the closure on the clone. Collections record which keys it touched: `get_mut`, `insert`, `remove` and the other mutators mark keys as they are called, not when a guard drops, so a change can't go unrecorded.
3. Encode only the touched entries, check that each reads back to the same bytes, and write them in one redb write transaction.
4. Publish the clone as the new snapshot with `arc-swap`, notify subscribers, and release the lock.

If the closure returns an error or panics, or the commit fails, the clone is dropped and nobody ever saw it. Rollback costs nothing.

Async updates run step 2 in the caller's task. They then hand the owned writer lock, the new state and the encoded changes to a dedicated writer thread, which does steps 3 and 4. The caller waits for the result on a oneshot channel. Dropping the caller's future therefore can't split writing from publishing, and commits never occupy tokio's blocking pool or an async worker thread. Blocking updates do every step on the calling thread.

A failed commit is the one case where storage might be ahead of memory, because a commit can fail after its data reached the file. The caller gets `Error::CommitFailed`, and endure reloads instead of guessing. redb refuses all further work after an I/O error, so endure drops the database, opens the storage again (the same path, or the factory given to `backend_with`), and loads the state from it. Memory-only fields are carried over from the previous snapshot. If the storage can't be reopened, the handle is poisoned: updates return `Error::Poisoned`, `is_poisoned()` reports it, and reads keep serving the last known state until the application reopens it.

## Storage layout

- Each persisted field is one redb table, named after the field or `#[durable(table = "...")]`. Keys and values are byte slices.
- Map entries are stored as `key -> value`. Cells use a single row with an empty key.
- List elements are stored under hidden `u64` order keys, written big-endian so redb returns them in list order. Keys start `2^32` apart. A push, insert or remove touches one row. When an insert finds no gap between two neighbours, the list is renumbered and rewritten once.
- Keys and values are encoded as MessagePack with named fields (`rmp-serde`). The format is self-describing, so serde attributes such as `#[serde(default)]` handle additive changes without a migration. Positional formats like postcard or bincode would not.
- Map keys don't need an order-preserving encoding. All reads and range queries run in memory on the ordered tree, so the on-disk order of map keys never matters.
- `__endure_meta` records the storage format version and each table's schema version.

## Schema versions

Each field lists its earlier value types with explicit, consecutive version numbers, for example `#[durable(versions(V1 = 1, V2 = 2))]`. The current type is one more than the last listed number, or 1 when nothing is listed. Explicit numbers mean dropping an old entry can't silently relabel stored data. When the state opens:

- A table with rows but no recorded version is treated as version 1. An empty table is recorded at the current version.
- Data older than the first listed version fails with `Error::OlderSchema`.
- If the stored version is older, every value is decoded as the matching old type, converted through the `From` chain, and rewritten at the current version. This happens in the same transaction that loads the state, so an upgrade either completes or never happened.
- If the stored version is newer than the build knows, the open fails with `Error::NewerSchema`. An older build never rewrites data it can't fully understand.

## Concurrency

- Readers never take a lock. `read()` is an atomic pointer load.
- Writers take a fair mutex (`tokio::sync::Mutex`), so updates commit in the order they acquire it. Async futures are lazy, so an update joins the queue when it is first polled, not when it is created. `update_blocking` takes the same mutex from blocking code.
- redb holds a file lock, so a second process, or a second open in the same process, fails instead of corrupting the file.
- There is no atomicity across separate state structs. Fields that must change together belong in the same struct.

## Notifications

Each commit produces a `Commit` with the new snapshot and the names of the persisted fields it changed. A reload after a failed commit produces one listing every field. Subscribers get it through a `tokio::sync::watch` channel, which always holds the latest commit. Subscribers are updated first, then `on_commit` hooks run synchronously, in commit order, while the writer lock is still held. A hook that panics is caught, so it can't fail a commit that already succeeded or hide it from subscribers. Hooks live in an `ArcSwap` list, so a hook can register another hook without deadlocking.

Collections record whether each touched key existed when the update started. A key that was added and removed within one update produces no write. An update whose changes all cancel out commits nothing and notifies no one.

## Alternatives considered

- **native_db** offers typed models on redb with indexes and watches. Its last release (0.8.2, July 2025) pins redb 2 while redb is at 4.x. Its indexes and query layer are unnecessary when all state is in memory.
- **turso** (SQLite rewritten in Rust) has an async-only Rust API, while endure's transactions are synchronous closures. endure also stores opaque values, so SQL buys little here.
- **Event sourcing**, as in ave-actors and atomr, adds an event type and an apply function for every change. Writing the changed entries directly gives the same durability with far less code.
- **SQLite with relational tables** keeps CHECK constraints and ad hoc queries, at the cost of the row mapping and migration code this library exists to remove. Invariants move into Rust types with checked transition methods.

## Not yet built

- An outbox: a list of messages committed together with a state change and delivered at least once by a background task. It's for effects that must survive a crash between commit and send.
- Idempotent updates keyed by a request ID, with the result stored in the same transaction.
- Lazily loaded maps for data too large to hold in memory. These need an order-preserving key encoding and a fallible read API.
- Key versioning.
