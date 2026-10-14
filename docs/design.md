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
3. Encode only the touched entries and write them in one redb write transaction. With the `tokio` feature, the commit and its fsync run on the blocking pool.
4. Publish the clone as the new snapshot with `arc-swap`, notify subscribers, and release the lock.

If the closure returns an error or panics, or the commit fails, the clone is dropped and nobody ever saw it. Rollback costs nothing.

A failed commit is the one case where disk might be ahead of memory, because a commit can fail after its data reached the file. endure then reloads the state from disk instead of guessing. If redb refuses further work after an I/O error, the reload fails too, and the handle returns `Error::Poisoned` for every later update until the application reopens it.

## Storage layout

- Each persisted field is one redb table, named after the field or `#[durable(table = "...")]`. Keys and values are byte slices.
- Map entries are stored as `key -> value`. Cells use a single row with an empty key.
- List elements are stored under hidden `u64` order keys, written big-endian so redb returns them in list order. Keys start `2^32` apart. A push, insert or remove touches one row. When an insert finds no gap between two neighbours, the list is renumbered and rewritten once.
- Keys and values are encoded as MessagePack with named fields (`rmp-serde`). The format is self-describing, so serde attributes such as `#[serde(default)]` handle additive changes without a migration. Positional formats like postcard or bincode would not.
- Map keys don't need an order-preserving encoding. All reads and range queries run in memory on the ordered tree, so the on-disk order of map keys never matters.
- `__endure_meta` records the storage format version and each table's schema version.

## Schema versions

Each field has a schema version: one plus the number of types listed in `#[durable(versions(...))]`. When the state opens:

- Data with no recorded version is treated as version 1.
- If the stored version is older, every value is decoded as the matching old type, converted through the `From` chain, and rewritten at the current version. This happens in the same transaction that loads the state, so an upgrade either completes or never happened.
- If the stored version is newer than the build knows, the open fails with `Error::NewerSchema`. An older build never rewrites data it can't fully understand.

## Concurrency

- Readers never take a lock. `read()` is an atomic pointer load.
- Writers take a fair async mutex (`tokio::sync::Mutex`), so updates commit in the order they were called. `update_blocking` takes the same mutex from blocking code.
- redb holds a file lock, so a second process, or a second open in the same process, fails instead of corrupting the file.
- There is no atomicity across separate state structs. Fields that must change together belong in the same struct.

## Notifications

Each commit produces a `Commit` with the new snapshot and the names of the persisted fields it changed. Subscribers get it through a `tokio::sync::watch` channel, which always holds the latest commit. `on_commit` hooks run synchronously after each commit, in commit order, while the writer lock is still held.

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
