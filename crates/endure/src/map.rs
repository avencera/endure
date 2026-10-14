use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::fmt;
use std::ops::{Index, RangeBounds};

use serde::{Serialize, de::DeserializeOwned};

use crate::codec;
use crate::error::Result;
use crate::field::{Field, Op};

/// An ordered map whose entries persist across restarts
///
/// Reads work like any map. Inside an update closure, `insert`, `remove`, and `get_mut` record
/// which keys changed, and only those keys are written when the update commits. Cloning is O(1)
/// because the entries live in a structurally shared tree
pub struct DurableMap<K, V> {
    entries: imbl::OrdMap<K, V>,
    // keys touched since the last commit, each with whether it was stored before the update;
    // the key's presence at commit decides put or delete, and a key that was neither stored
    // before nor present now needs no write at all
    dirty: BTreeMap<K, bool>,
}

impl<K: Ord + Clone, V: Clone> DurableMap<K, V> {
    /// Creates an empty map
    pub fn new() -> Self {
        Self {
            entries: imbl::OrdMap::new(),
            dirty: BTreeMap::new(),
        }
    }

    /// Returns the number of entries
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns true if the map has no entries
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns the value for `key`
    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.entries.get(key)
    }

    /// Returns true if the map has an entry for `key`
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.entries.contains_key(key)
    }

    /// Returns a mutable reference to the value for `key`, marking it changed
    pub fn get_mut<Q>(&mut self, key: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        // record the key before handing out the reference so a change can never go unseen
        let (stored_key, _) = self.entries.get_key_value(key)?;
        let stored_key = stored_key.clone();
        self.touch(&stored_key, true);
        self.entries.get_mut(key)
    }

    /// Returns a mutable reference to the value for `key`, inserting `default()` first if absent
    pub fn get_or_insert_with(&mut self, key: K, default: impl FnOnce() -> V) -> &mut V {
        let existed = self.entries.contains_key(&key);
        self.touch(&key, existed);
        if !existed {
            self.entries.insert(key.clone(), default());
        }

        self.entries.get_mut(&key).expect("entry was just ensured")
    }

    /// Inserts a value, returning the previous value for the key
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.touch(&key, self.entries.contains_key(&key));
        self.entries.insert(key, value)
    }

    /// Removes the entry for `key`, returning its value
    pub fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let (stored_key, value) = self.entries.remove_with_key(key)?;
        self.touch(&stored_key, true);
        Some(value)
    }

    /// Keeps only the entries for which `keep` returns true
    pub fn retain(&mut self, mut keep: impl FnMut(&K, &V) -> bool) {
        let removed: Vec<K> = self
            .entries
            .iter()
            .filter(|(key, value)| !keep(key, value))
            .map(|(key, _)| key.clone())
            .collect();

        for key in removed {
            self.entries.remove(&key);
            self.touch(&key, true);
        }
    }

    /// Removes every entry
    pub fn clear(&mut self) {
        let keys: Vec<K> = self.entries.keys().cloned().collect();
        for key in &keys {
            self.touch(key, true);
        }

        self.entries.clear();
    }

    /// Calls `f` on every entry with mutable access, marking every key changed
    pub fn for_each_mut(&mut self, mut f: impl FnMut(&K, &mut V)) {
        let keys: Vec<K> = self.entries.keys().cloned().collect();
        for key in keys {
            self.touch(&key, true);
            if let Some(value) = self.entries.get_mut(&key) {
                f(&key, value);
            }
        }
    }

    /// Iterates over entries in key order
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = (&K, &V)> + '_ {
        self.entries.iter()
    }

    /// Iterates over keys in order
    pub fn keys(&self) -> impl DoubleEndedIterator<Item = &K> + '_ {
        self.entries.keys()
    }

    /// Iterates over values in key order
    pub fn values(&self) -> impl DoubleEndedIterator<Item = &V> + '_ {
        self.entries.values()
    }

    /// Iterates over the entries whose keys fall in `range`
    pub fn range<R, Q>(&self, range: R) -> impl DoubleEndedIterator<Item = (&K, &V)> + '_
    where
        R: RangeBounds<Q>,
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.entries.range(range)
    }

    /// Returns the entry with the smallest key
    pub fn first(&self) -> Option<(&K, &V)> {
        self.entries.get_min().map(|(key, value)| (key, value))
    }

    /// Returns the entry with the largest key
    pub fn last(&self) -> Option<(&K, &V)> {
        self.entries.get_max().map(|(key, value)| (key, value))
    }

    // a key's first touch in an update records whether it was stored before; it is present now
    // exactly when it was stored before, because any earlier change would have touched it already
    fn touch(&mut self, key: &K, present: bool) {
        if !self.dirty.contains_key(key) {
            self.dirty.insert(key.clone(), present);
        }
    }
}

impl<K: Ord + Clone, V: Clone> Default for DurableMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Clone, V: Clone> Clone for DurableMap<K, V> {
    fn clone(&self) -> Self {
        Self {
            entries: self.entries.clone(),
            dirty: self.dirty.clone(),
        }
    }
}

impl<K: Ord + Clone + fmt::Debug, V: Clone + fmt::Debug> fmt::Debug for DurableMap<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.entries.iter()).finish()
    }
}

impl<K: Ord + Clone, V: Clone + PartialEq> PartialEq for DurableMap<K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.entries == other.entries
    }
}

impl<K: Ord + Clone, V: Clone + Eq> Eq for DurableMap<K, V> {}

impl<K, V, Q> Index<&Q> for DurableMap<K, V>
where
    K: Ord + Clone + Borrow<Q>,
    V: Clone,
    Q: Ord + ?Sized,
{
    type Output = V;

    fn index(&self, key: &Q) -> &V {
        self.get(key).expect("no entry for key in DurableMap")
    }
}

impl<'a, K: Ord + Clone, V: Clone> IntoIterator for &'a DurableMap<K, V> {
    type Item = (&'a K, &'a V);
    type IntoIter = Box<dyn DoubleEndedIterator<Item = (&'a K, &'a V)> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.iter())
    }
}

impl<K: Ord + Clone, V: Clone> Extend<(K, V)> for DurableMap<K, V> {
    fn extend<I: IntoIterator<Item = (K, V)>>(&mut self, iter: I) {
        for (key, value) in iter {
            self.insert(key, value);
        }
    }
}

impl<K, V> Field for DurableMap<K, V>
where
    K: Ord + Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
    V: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    type Item = V;

    fn load(
        table: &str,
        rows: Vec<(Vec<u8>, Vec<u8>)>,
        decode: &dyn Fn(&[u8]) -> Result<V>,
    ) -> Result<Self> {
        let mut entries = imbl::OrdMap::new();
        for (key, value) in rows {
            entries.insert(codec::decode::<K>(table, &key)?, decode(&value)?);
        }

        Ok(Self {
            entries,
            dirty: BTreeMap::new(),
        })
    }

    fn mark_all_changed(&mut self) {
        self.dirty = self.entries.keys().map(|key| (key.clone(), true)).collect();
    }

    fn take_ops(&mut self, table: &str, ops: &mut Vec<Op>) -> Result<()> {
        for (key, was_stored) in std::mem::take(&mut self.dirty) {
            let value = match self.entries.get(&key) {
                Some(value) => Some(codec::encode_checked(table, value)?),
                // added and removed within one update, so storage never saw it
                None if !was_stored => continue,
                None => None,
            };

            ops.push(Op {
                key: codec::encode_checked(table, &key)?,
                value,
            });
        }

        Ok(())
    }
}
