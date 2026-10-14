use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt;
use std::ops::Index;

use serde::{Serialize, de::DeserializeOwned};

use crate::codec;
use crate::error::{Error, Result};
use crate::field::{Field, Op};

// each element is stored under a hidden u64 order key; keys increase along the vector and leave
// gaps so a push or an insert writes one row instead of shifting every later element
const STEP: u64 = 1 << 32;
const BASE: u64 = 1 << 62;

/// An ordered list whose elements persist across restarts
///
/// Behaves like a `Vec`. Pushing, popping, inserting, or removing one element writes or deletes
/// one row. Operations that reorder everything, such as [`sort_by`](Self::sort_by), rewrite the
/// whole list. Cloning is O(1) because the elements live in a structurally shared tree
pub struct DurableVec<T> {
    items: imbl::Vector<T>,
    // order key of each element, strictly increasing, parallel to `items`
    keys: imbl::Vector<u64>,
    // order keys touched since the last commit, each with whether it was stored before the
    // update; present keys are written, absent ones deleted, and keys that were never stored
    // and are gone again need no write
    dirty: BTreeMap<u64, bool>,
}

impl<T: Clone> DurableVec<T> {
    /// Creates an empty list
    pub fn new() -> Self {
        Self {
            items: imbl::Vector::new(),
            keys: imbl::Vector::new(),
            dirty: BTreeMap::new(),
        }
    }

    /// Returns the number of elements
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Returns true if the list is empty
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Returns the element at `index`
    pub fn get(&self, index: usize) -> Option<&T> {
        self.items.get(index)
    }

    /// Returns the first element
    pub fn first(&self) -> Option<&T> {
        self.items.front()
    }

    /// Returns the last element
    pub fn last(&self) -> Option<&T> {
        self.items.back()
    }

    /// Returns a mutable reference to the element at `index`, marking it changed
    pub fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        let key = *self.keys.get(index)?;
        self.touch(key, true);
        self.items.get_mut(index)
    }

    /// Appends an element
    pub fn push(&mut self, value: T) {
        let key = match self.keys.back() {
            None => BASE,
            Some(&last) if last <= u64::MAX - STEP => last + STEP,
            Some(_) => {
                self.renumber();
                self.keys.back().map_or(BASE, |last| last + STEP)
            }
        };

        self.touch(key, false);
        self.keys.push_back(key);
        self.items.push_back(value);
    }

    /// Prepends an element
    pub fn push_front(&mut self, value: T) {
        self.insert(0, value);
    }

    /// Removes and returns the last element
    pub fn pop(&mut self) -> Option<T> {
        let key = self.keys.pop_back()?;
        self.touch(key, true);
        self.items.pop_back()
    }

    /// Removes and returns the first element
    pub fn pop_front(&mut self) -> Option<T> {
        let key = self.keys.pop_front()?;
        self.touch(key, true);
        self.items.pop_front()
    }

    /// Inserts an element at `index`, shifting later elements right
    ///
    /// # Panics
    ///
    /// Panics if `index > len`
    pub fn insert(&mut self, index: usize, value: T) {
        assert!(
            index <= self.len(),
            "insertion index {index} out of bounds for length {}",
            self.len()
        );

        if index == self.len() {
            self.push(value);
            return;
        }

        let key = match self.key_before(index) {
            Some(key) => key,
            None => {
                self.renumber();
                self.key_before(index)
                    .expect("renumbering leaves a gap before every element")
            }
        };

        self.touch(key, false);
        self.keys.insert(index, key);
        self.items.insert(index, value);
    }

    /// Removes and returns the element at `index`, shifting later elements left
    ///
    /// # Panics
    ///
    /// Panics if `index >= len`
    pub fn remove(&mut self, index: usize) -> T {
        let key = self.keys.remove(index);
        self.touch(key, true);
        self.items.remove(index)
    }

    /// Shortens the list to `len` elements
    pub fn truncate(&mut self, len: usize) {
        if len >= self.len() {
            return;
        }

        let removed: Vec<u64> = self.keys.iter().skip(len).copied().collect();
        for key in removed {
            self.touch(key, true);
        }

        self.keys.truncate(len);
        self.items.truncate(len);
    }

    /// Removes every element
    pub fn clear(&mut self) {
        self.truncate(0);
    }

    /// Keeps only the elements for which `keep` returns true
    pub fn retain(&mut self, mut keep: impl FnMut(&T) -> bool) {
        let mut index = 0;
        while index < self.items.len() {
            if keep(&self.items[index]) {
                index += 1;
            } else {
                self.remove(index);
            }
        }
    }

    /// Calls `f` on every element with mutable access, marking every element changed
    pub fn for_each_mut(&mut self, mut f: impl FnMut(&mut T)) {
        self.touch_all();
        for item in self.items.iter_mut() {
            f(item);
        }
    }

    /// Sorts the list with a comparator, rewriting every element
    pub fn sort_by(&mut self, compare: impl Fn(&T, &T) -> Ordering) {
        self.items.sort_by(compare);
        self.touch_all();
    }

    /// Sorts the list, rewriting every element
    pub fn sort(&mut self)
    where
        T: Ord,
    {
        self.sort_by(Ord::cmp);
    }

    /// Iterates over the elements in order
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &T> + ExactSizeIterator + '_ {
        self.items.iter()
    }

    // returns a free order key between the element before `index` and the one at `index`
    fn key_before(&self, index: usize) -> Option<u64> {
        let next = self.keys[index];
        let previous = match index {
            0 => return (next > 0).then(|| if next > STEP { next - STEP } else { next / 2 }),
            _ => self.keys[index - 1],
        };

        (next - previous >= 2).then(|| previous + (next - previous) / 2)
    }

    // spreads the keys evenly again once a gap has run out
    fn renumber(&mut self) {
        self.touch_all();
        self.keys = (0..self.items.len() as u64)
            .map(|position| BASE + position * STEP)
            .collect();

        // a new key equal to an old one was already touched as stored, so this keeps it
        let fresh: Vec<u64> = self.keys.iter().copied().collect();
        for key in fresh {
            self.touch(key, false);
        }
    }

    fn touch_all(&mut self) {
        let keys: Vec<u64> = self.keys.iter().copied().collect();
        for key in keys {
            self.touch(key, true);
        }
    }

    // a key's first touch in an update records whether it was stored before the update
    fn touch(&mut self, key: u64, stored: bool) {
        self.dirty.entry(key).or_insert(stored);
    }
}

impl<T: Clone> Default for DurableVec<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone> Clone for DurableVec<T> {
    fn clone(&self) -> Self {
        Self {
            items: self.items.clone(),
            keys: self.keys.clone(),
            dirty: self.dirty.clone(),
        }
    }
}

impl<T: Clone + fmt::Debug> fmt::Debug for DurableVec<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.items.iter()).finish()
    }
}

impl<T: Clone + PartialEq> PartialEq for DurableVec<T> {
    fn eq(&self, other: &Self) -> bool {
        self.items == other.items
    }
}

impl<T: Clone + Eq> Eq for DurableVec<T> {}

impl<T: Clone> Index<usize> for DurableVec<T> {
    type Output = T;

    fn index(&self, index: usize) -> &T {
        &self.items[index]
    }
}

impl<'a, T: Clone> IntoIterator for &'a DurableVec<T> {
    type Item = &'a T;
    type IntoIter = Box<dyn DoubleEndedIterator<Item = &'a T> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.items.iter())
    }
}

impl<T: Clone> Extend<T> for DurableVec<T> {
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        for item in iter {
            self.push(item);
        }
    }
}

impl<T> Field for DurableVec<T>
where
    T: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    type Item = T;

    fn load(
        table: &str,
        rows: Vec<(Vec<u8>, Vec<u8>)>,
        decode: &dyn Fn(&[u8]) -> Result<T>,
    ) -> Result<Self> {
        let mut vec = Self::new();
        // rows arrive sorted by key bytes, and big-endian keys sort numerically
        for (key, value) in rows {
            let key: [u8; 8] = key.as_slice().try_into().map_err(|_| Error::Decode {
                table: table.to_owned(),
                message: format!("order key has {} bytes, expected 8", key.len()),
            })?;

            vec.keys.push_back(u64::from_be_bytes(key));
            vec.items.push_back(decode(&value)?);
        }

        Ok(vec)
    }

    fn mark_all_changed(&mut self) {
        self.touch_all();
    }

    fn take_ops(&mut self, table: &str, ops: &mut Vec<Op>) -> Result<()> {
        for (key, was_stored) in std::mem::take(&mut self.dirty) {
            let value = match self.keys.binary_search(&key) {
                Ok(index) => Some(codec::encode_checked(table, &self.items[index])?),
                // pushed and removed within one update, so storage never saw it
                Err(_) if !was_stored => continue,
                Err(_) => None,
            };

            ops.push(Op {
                key: key.to_be_bytes().to_vec(),
                value,
            });
        }

        Ok(())
    }
}
