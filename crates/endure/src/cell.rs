use std::fmt;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use serde::{Serialize, de::DeserializeOwned};

use crate::codec;
use crate::error::Result;
use crate::field::{Field, Op};

// a cell is stored as a single row under this key
const CELL_KEY: &[u8] = b"";

/// A single value that persists across restarts
///
/// Read it through `Deref`. Inside an update closure, assign through `DerefMut` or call
/// [`set`](Self::set); either marks the cell changed. A cell with nothing stored starts as
/// `T::default()`, so wrap the type in `Option` when absence matters
pub struct DurableCell<T> {
    // shared so cloning the state for an update stays O(1) however large the value is
    value: Arc<T>,
    dirty: bool,
}

impl<T: Clone> DurableCell<T> {
    /// Creates a cell holding `value`
    pub fn new(value: T) -> Self {
        Self {
            value: Arc::new(value),
            dirty: false,
        }
    }

    /// Replaces the value
    pub fn set(&mut self, value: T) {
        self.value = Arc::new(value);
        self.dirty = true;
    }

    /// Returns a reference to the value
    pub fn get(&self) -> &T {
        &self.value
    }
}

impl<T: Clone + Default> Default for DurableCell<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T> Clone for DurableCell<T> {
    fn clone(&self) -> Self {
        Self {
            value: Arc::clone(&self.value),
            dirty: self.dirty,
        }
    }
}

impl<T> Deref for DurableCell<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T: Clone> DerefMut for DurableCell<T> {
    fn deref_mut(&mut self) -> &mut T {
        self.dirty = true;
        Arc::make_mut(&mut self.value)
    }
}

impl<T: fmt::Debug> fmt::Debug for DurableCell<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.value.fmt(f)
    }
}

impl<T: PartialEq> PartialEq for DurableCell<T> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl<T: Eq> Eq for DurableCell<T> {}

impl<T> Field for DurableCell<T>
where
    T: Clone + Default + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    type Item = T;

    fn load(
        _table: &str,
        rows: Vec<(Vec<u8>, Vec<u8>)>,
        decode: &dyn Fn(&[u8]) -> Result<T>,
    ) -> Result<Self> {
        let value = match rows.into_iter().find(|(key, _)| key == CELL_KEY) {
            Some((_, bytes)) => decode(&bytes)?,
            None => T::default(),
        };

        Ok(Self::new(value))
    }

    fn mark_all_changed(&mut self) {
        self.dirty = true;
    }

    fn take_ops(&mut self, table: &str, ops: &mut Vec<Op>) -> Result<()> {
        if std::mem::take(&mut self.dirty) {
            ops.push(Op {
                key: CELL_KEY.to_vec(),
                value: Some(codec::encode_checked(table, &*self.value)?),
            });
        }

        Ok(())
    }
}
