//! The contract between `#[derive(Durable)]` and the collection types
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

    /// Marks every element changed so the next commit rewrites all of them
    fn mark_all_changed(&mut self);

    /// Moves the pending changes into `ops`, encoding the current values
    fn take_ops(&mut self, table: &str, ops: &mut Vec<Op>) -> Result<()>;
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

/// A struct whose fields persist, implemented by `#[derive(Durable)]`
pub trait State: Clone + Send + Sync + Sized + 'static {
    /// Default file stem, the struct name in snake case
    const NAME: &'static str;
    /// Default directory name, the defining crate's package name
    const APP: &'static str;

    /// The persisted fields
    fn fields() -> &'static [FieldSpec];

    /// Builds the state from storage
    fn load(loader: &mut Loader<'_>) -> Result<Self>;

    /// Moves every field's pending changes into `changes`
    fn take_changes(&mut self, changes: &mut Changes) -> Result<()>;
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
    ) -> Result<()> {
        let mut ops = Vec::new();
        field.take_ops(table, &mut ops)?;

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

const META: TableDefinition<&str, u32> = TableDefinition::new("__endure_meta");
const FORMAT_KEY: &str = "__format";
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
        let mut meta = txn.open_table(META)?;
        let format = meta.get(FORMAT_KEY)?.map(|guard| guard.value());

        match format {
            Some(found) if found > FORMAT_VERSION => {
                return Err(Error::UnsupportedFormat {
                    found,
                    supported: FORMAT_VERSION,
                });
            }
            Some(_) => {}
            None => {
                meta.insert(FORMAT_KEY, FORMAT_VERSION)?;
            }
        }

        Ok(Self { txn })
    }

    /// Loads one field, decoding each stored value with `decode(stored_version, bytes)`
    ///
    /// When the stored version is older than `version`, every value is decoded through the
    /// upgrade path and rewritten at the current version in the same transaction
    pub fn field<F: Field>(
        &mut self,
        table_name: &'static str,
        version: u32,
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
            None => version,
        };

        if stored > version {
            return Err(Error::NewerSchema {
                table: table_name.to_owned(),
                found: stored,
                supported: version,
            });
        }

        let mut field = F::load(table_name, rows, &|bytes| decode(stored, bytes))?;

        if stored < version {
            field.mark_all_changed();
            let mut ops = Vec::new();
            field.take_ops(table_name, &mut ops)?;
            apply(&mut table, &ops)?;
        }

        meta.insert(table_name, version)?;
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
