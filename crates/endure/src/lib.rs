//! Durable app state that feels like in-memory data
//!
//! Put persistent collections in a struct, derive [`Durable`](macro@Durable), and open it. Reads are plain field
//! access on a committed snapshot. Writes are ordinary mutation inside an update closure; when the
//! closure returns, the changed entries are written to a [redb](https://docs.rs/redb) file in one
//! transaction, and only then does anyone else see them
//!
//! ```no_run
//! use endure::{Durable, DurableCell, DurableMap, DurableVec};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Clone, Default, Serialize, Deserialize)]
//! struct Settings {
//!     max_parallel: u32,
//! }
//!
//! #[derive(Clone, Durable)]
//! struct App {
//!     tasks: DurableMap<u64, String>,
//!     settings: DurableCell<Settings>,
//!     history: DurableVec<String>,
//! }
//!
//! # async fn run() -> Result<(), endure::Error> {
//! let app = App::open()?;
//!
//! app.update(|s| {
//!     s.tasks.insert(1, "build".into());
//!     s.settings.max_parallel = 4;
//!     s.history.push("added build".into());
//! })
//! .await?;
//!
//! let state = app.read();
//! assert_eq!(state.tasks.get(&1).map(String::as_str), Some("build"));
//! # Ok(())
//! # }
//! ```
//!
//! # Guarantees
//!
//! - The file is the single source of truth. Memory holds only state read from it or committed
//!   to it
//! - Updates run one at a time, in the order they acquire a fair writer lock. Each is one atomic
//!   transaction, and resolves only after its changes are durable
//! - If the closure returns an error or panics, nothing changes. Once the closure has run,
//!   dropping the update future does not cancel the commit
//! - These guarantees cover changes made through the collections. Values must not use interior
//!   mutability such as `Mutex`: snapshots share values, so a write through one is visible
//!   everywhere at once and is neither rolled back nor tracked
//! - A failed commit returns [`Error::CommitFailed`] and the state is reloaded from storage, so
//!   [`Durable::read`] shows what was actually saved
//! - Only one process can open a file at a time
//!
//! # Schema changes
//!
//! Values are stored as MessagePack with named fields, so adding a field marked
//! `#[serde(default)]` or removing one needs no migration. For a change serde cannot absorb, list
//! the old value types on the field with their version numbers and implement `From` between
//! consecutive versions:
//!
//! ```ignore
//! #[derive(Clone, Durable)]
//! struct App {
//!     // values stored as ApprovalV1 (version 1) are upgraded to Approval (version 2) on open
//!     #[durable(versions(ApprovalV1 = 1))]
//!     approvals: DurableMap<ApprovalId, Approval>,
//! }
//! ```

mod cell;
mod codec;
mod error;
mod map;
mod store;
mod vec;

#[doc(hidden)]
pub mod field;

pub use cell::DurableCell;
pub use endure_derive::Durable;
pub use error::{Error, Result, UpdateError};
pub use map::DurableMap;
pub use store::{Builder, Commit, Durable, Snapshot};
pub use vec::DurableVec;

/// Re-exported so applications can implement or name a custom storage backend
pub use redb;

#[doc(hidden)]
pub mod __private {
    pub use crate::codec::decode;
    pub use crate::field::{Changes, Field, FieldSpec, Loader, State, Versions};
}
