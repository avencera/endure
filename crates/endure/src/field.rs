//! The contract between `#[derive(Clone, Durable)]` and the collection types
//!
//! Everything here is public only so generated code can reach it. It is not a stable API

use std::sync::atomic::{AtomicU64, Ordering};

use redb::{ReadableTable, TableDefinition, WriteTransaction};

use crate::error::{Error, Result};

static EPOCH: AtomicU64 = AtomicU64::new(1);

/// Returns a new identity for a collection's contents
///
/// Every loaded, created, or committed collection gets a fresh epoch. An update compares each
/// field's epoch with the committed state it started from, so a field that was replaced
/// wholesale (`s.map = DurableMap::new()`, `mem::take`, or a clone of an older snapshot) is
/// rewritten in full instead of silently skipped
pub(crate) fn next_epoch() -> u64 {
    EPOCH.fetch_add(1, Ordering::Relaxed)
}

/// One pending write to a table: a put when `value` is `Some`, a delete otherwise
#[derive(Debug)]
pub struct Op {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
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
    ) -> Result<Self>;

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

pub(crate) fn table(name: &str) -> TableDefinition<'_, &'static [u8], &'static [u8]> {
    TableDefinition::new(name)
}

/// Reads every field of a state struct inside one write transaction, upgrading old schema
/// versions in place
pub struct Loader<'txn> {
    txn: &'txn WriteTransaction,
}

impl<'txn> Loader<'txn> {
    pub(crate) fn new(txn: &'txn WriteTransaction) -> Result<Self> {
        let mut format_table = txn.open_table(FORMAT)?;
        let format = format_table.get(FORMAT_KEY)?.map(|guard| guard.value());

        match format {
            Some(found) if found > FORMAT_VERSION => {
                return Err(Error::UnsupportedFormat {
                    found,
                    supported: FORMAT_VERSION,
                });
            }
            Some(_) => {}
            None => {
                format_table.insert(FORMAT_KEY, FORMAT_VERSION)?;
            }
        }

        Ok(Self { txn })
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
        let stored = match meta.get(table_name)?.map(|guard| guard.value()) {
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

        let field = F::load(table_name, rows, &|bytes| decode(stored, bytes))?;

        if stored < versions.current {
            let mut ops = Vec::new();
            field.all_ops(table_name, &mut ops)?;
            apply(&mut table, &ops)?;
        }

        meta.insert(table_name, versions.current)?;
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
