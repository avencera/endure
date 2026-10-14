//! The contract between `#[derive(Clone, Durable)]` and the collection types
//!
//! Everything here is public only so generated code can reach it. It is not a stable API

use redb::{ReadableTable, TableDefinition, WriteTransaction};

use crate::error::{Error, Result};

/// One pending write to a table: a put when `value` is `Some`, a delete otherwise
#[derive(Debug)]
pub struct Op {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

/// Whether a field's stored rows are exactly the rows it would write now
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Layout {
    /// Every row is stored under the key the field would write
    Current,
    /// Some row is stored under bytes the field no longer produces, such as a map key written
    /// before a field was added to the key type, so the table must be rewritten
    Stale,
}

/// A persistent collection that can be a field of a durable state struct
pub trait Field: Clone + Send + Sync + 'static {
    /// The stored element type, which schema versions upgrade into
    type Item;

    /// Builds the field from its stored rows, given in key order
    fn load(
        table: &str,
        rows: Vec<(Vec<u8>, Vec<u8>)>,
        decode: &dyn Fn(&[u8]) -> Result<Self::Item>,
    ) -> Result<(Self, Layout)>;

    /// Writes every element, for rewriting a whole table
    fn all_ops(&self, table: &str, ops: &mut Vec<Op>) -> Result<()>;

    /// Moves the changes made since `previous`, the committed value this update started from,
    /// into `ops`, then gives the field a new epoch
    fn take_ops(&mut self, previous: &Self, table: &str, ops: &mut Vec<Op>) -> Result<()>;
}

/// Describes one persisted field of a state struct
#[derive(Debug, Clone, Copy)]
pub struct FieldSpec {
    /// The Rust field name
    pub name: &'static str,
    /// The table that stores it
    pub table: &'static str,
    /// The current schema version of its values
    pub version: u32,
}

/// A struct whose fields persist, implemented by `#[derive(Clone, Durable)]`
pub trait State: Clone + Send + Sync + Sized + 'static {
    /// Default file stem, the struct name in snake case
    const NAME: &'static str;
    /// Default directory name, the defining crate's package name
    const APP: &'static str;

    /// The persisted fields
    fn fields() -> &'static [FieldSpec];

    /// Builds the state from storage
    ///
    /// Memory-only fields are copied from `previous` when reloading, or start as their default
    fn load(loader: &mut Loader<'_>, previous: Option<&Self>) -> Result<Self>;

    /// Moves every field's changes since `previous` into `changes`
    fn take_changes(&mut self, previous: &Self, changes: &mut Changes) -> Result<()>;
}

/// The writes produced by one update
#[derive(Debug, Default)]
pub struct Changes {
    pub(crate) tables: Vec<(&'static str, Vec<Op>)>,
    pub(crate) fields: Vec<&'static str>,
}

impl Changes {
    /// Collects the pending changes of one field
    pub fn field<F: Field>(
        &mut self,
        name: &'static str,
        table: &'static str,
        field: &mut F,
        previous: &F,
    ) -> Result<()> {
        let mut ops = Vec::new();
        field.take_ops(previous, table, &mut ops)?;

        if !ops.is_empty() {
            self.fields.push(name);
            self.tables.push((table, ops));
        }

        Ok(())
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }
}

/// The schema versions one field can read
#[derive(Debug, Clone, Copy)]
pub struct Versions {
    /// The oldest stored version that can still be upgraded
    pub oldest: u32,
    /// The version new values are written at
    pub current: u32,
}

// schema version of each field's table, keyed by table name
const META: TableDefinition<&str, u32> = TableDefinition::new("__endure_meta");
// kept apart from META so no table name can collide with it
const FORMAT: TableDefinition<&str, u32> = TableDefinition::new("__endure_format");
const FORMAT_KEY: &str = "version";
const FORMAT_VERSION: u32 = 1;
// number of the last commit, so recovery can tell whether a failed commit reached storage
const COMMITS: TableDefinition<&str, u64> = TableDefinition::new("__endure_commits");
const COMMITS_KEY: &str = "last";

pub(crate) fn table(name: &str) -> TableDefinition<'_, &'static [u8], &'static [u8]> {
    TableDefinition::new(name)
}

/// Returns the number of the last commit stored in this transaction's view, or 0
pub(crate) fn last_commit(txn: &WriteTransaction) -> Result<u64> {
    let commits = txn.open_table(COMMITS)?;
    Ok(commits.get(COMMITS_KEY)?.map_or(0, |guard| guard.value()))
}

/// Records `number` as the last commit, inside the transaction that commits it
pub(crate) fn record_commit(txn: &WriteTransaction, number: u64) -> Result<()> {
    txn.open_table(COMMITS)?.insert(COMMITS_KEY, number)?;
    Ok(())
}

/// Reads every field of a state struct inside one write transaction, upgrading old schema
/// versions in place
pub struct Loader<'txn> {
    txn: &'txn WriteTransaction,
    // set once anything was written, so opening a current file can skip the commit
    wrote: bool,
}

impl<'txn> Loader<'txn> {
    pub(crate) fn new(txn: &'txn WriteTransaction) -> Result<Self> {
        let mut format_table = txn.open_table(FORMAT)?;
        let format = format_table.get(FORMAT_KEY)?.map(|guard| guard.value());

        let wrote = match format {
            Some(found) if found > FORMAT_VERSION => {
                return Err(Error::UnsupportedFormat {
                    found,
                    supported: FORMAT_VERSION,
                });
            }
            Some(_) => false,
            None => {
                format_table.insert(FORMAT_KEY, FORMAT_VERSION)?;
                true
            }
        };

        Ok(Self { txn, wrote })
    }

    /// Returns true if loading wrote anything that must be committed
    pub(crate) fn wrote(&self) -> bool {
        self.wrote
    }

    /// Loads one field, decoding each stored value with `decode(stored_version, bytes)`
    ///
    /// When the stored version is older than `versions.current`, every value is decoded through
    /// the upgrade path and rewritten at the current version in the same transaction
    pub fn field<F: Field>(
        &mut self,
        table_name: &'static str,
        versions: Versions,
        decode: impl Fn(u32, &[u8]) -> Result<F::Item>,
    ) -> Result<F> {
        let mut table = self.txn.open_table(table(table_name))?;
        let mut rows = Vec::new();
        for row in table.iter()? {
            let (key, value) = row?;
            rows.push((key.value().to_vec(), value.value().to_vec()));
        }

        let mut meta = self.txn.open_table(META)?;
        let recorded = meta.get(table_name)?.map(|guard| guard.value());
        let stored = match recorded {
            Some(stored) => stored,
            // rows without a recorded version predate versioning, so they are version 1
            None if !rows.is_empty() => 1,
            // an empty table has nothing to upgrade, so it starts at the current version
            None => versions.current,
        };

        if stored > versions.current {
            return Err(Error::NewerSchema {
                table: table_name.to_owned(),
                found: stored,
                supported: versions.current,
            });
        }

        if stored < versions.oldest && !rows.is_empty() {
            return Err(Error::OlderSchema {
                table: table_name.to_owned(),
                found: stored,
                oldest: versions.oldest,
            });
        }

        let (field, layout) = F::load(table_name, rows, &|bytes| decode(stored, bytes))?;

        // an upgrade or a stale key can change the bytes of any key, so clear the table first
        // instead of leaving rows behind that a later open would read again
        if stored < versions.current || layout == Layout::Stale {
            let mut ops = Vec::new();
            field.all_ops(table_name, &mut ops)?;
            table.retain(|_, _| false)?;
            apply(&mut table, &ops)?;
            self.wrote = true;
        }

        if recorded != Some(versions.current) {
            meta.insert(table_name, versions.current)?;
            self.wrote = true;
        }

        Ok(field)
    }
}

pub(crate) fn apply(
    table: &mut redb::Table<'_, &'static [u8], &'static [u8]>,
    ops: &[Op],
) -> Result<()> {
    for op in ops {
        match &op.value {
            Some(value) => {
                table.insert(op.key.as_slice(), value.as_slice())?;
            }
            None => {
                table.remove(op.key.as_slice())?;
            }
        }
    }

    Ok(())
}
