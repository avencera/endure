use std::fmt;

/// Errors from opening, reading, or committing durable state
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The storage engine reported an error
    #[error("storage error: {0}")]
    Storage(#[from] redb::Error),

    /// A filesystem operation failed while preparing the database location
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// A stored value could not be decoded into its Rust type
    #[error("could not decode a value in table `{table}`: {message}")]
    Decode { table: String, message: String },

    /// A value could not be encoded for storage
    #[error("could not encode a value for table `{table}`: {message}")]
    Encode { table: String, message: String },

    /// The file was written by a newer schema version than this build knows
    #[error(
        "table `{table}` is at schema version {found}, but this build supports up to {supported}"
    )]
    NewerSchema {
        table: String,
        found: u32,
        supported: u32,
    },

    /// The file was written by a newer, incompatible version of endure
    #[error("database format version {found} is newer than the supported version {supported}")]
    UnsupportedFormat { found: u32, supported: u32 },

    /// No platform state directory exists, so the location must be set explicitly
    #[error("no default state directory on this platform; set one with `.dir(..)` or `.path(..)`")]
    NoDefaultDir,

    /// `destroy` was called while other handles to the same state were alive
    #[error("other handles to this state are still alive")]
    InUse,

    /// A commit failed and reloading from disk also failed, so memory may no longer match disk
    #[error("state is poisoned after a failed commit and reload; reopen it")]
    Poisoned,
}

macro_rules! from_redb {
    ($($ty:ident),* $(,)?) => {
        $(
            impl From<redb::$ty> for Error {
                fn from(error: redb::$ty) -> Self {
                    Self::Storage(error.into())
                }
            }
        )*
    };
}

from_redb!(
    DatabaseError,
    TransactionError,
    TableError,
    StorageError,
    CommitError
);

/// The error from [`Durable::try_update`](crate::Durable::try_update)
///
/// `Rejected` carries the closure's own error; nothing was written and memory is unchanged
/// `Storage` means the closure succeeded but the commit failed; memory then matches what is on disk
#[derive(Debug)]
pub enum UpdateError<E> {
    /// The update closure returned an error
    Rejected(E),
    /// The closure succeeded but persisting its changes failed
    Storage(Error),
}

impl<E> UpdateError<E> {
    /// Returns the closure's error, if the closure rejected the update
    pub fn rejected(self) -> Option<E> {
        match self {
            Self::Rejected(error) => Some(error),
            Self::Storage(_) => None,
        }
    }
}

impl<E: fmt::Display> fmt::Display for UpdateError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rejected(error) => write!(f, "update rejected: {error}"),
            Self::Storage(error) => write!(f, "update failed to persist: {error}"),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for UpdateError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Rejected(error) => Some(error),
            Self::Storage(error) => Some(error),
        }
    }
}

impl<E> From<Error> for UpdateError<E> {
    fn from(error: Error) -> Self {
        Self::Storage(error)
    }
}

/// Shorthand for results whose error is [`Error`]
pub type Result<T, E = Error> = std::result::Result<T, E>;
