use std::cmp::Ordering;
use std::fmt;
use std::ops::Index;

use serde::{Serialize, de::DeserializeOwned};

use crate::codec;
use crate::error::{Error, Result};
use crate::field::{Field, Layout, Op};
use crate::tracker::{Pending, Tracker};

// each element is stored under a hidden u64 order key; keys increase along the vector and leave
// gaps so a push or an insert writes one row instead of shifting every later element
//
// structural edits run on a copy of `items` and change `keys` only after it is installed: imbl
// clones shared elements partway through an edit, and a panicking `Clone` caught inside the
// update must leave both vectors as they were, while key edits never panic
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
    tracker: Tracker<u64>,
}

impl<T: Clone> DurableVec<T> {
    /// Creates an empty list
    #[must_use]
    pub fn new() -> Self {
        Self {
            items: imbl::Vector::new(),
            keys: imbl::Vector::new(),
            tracker: Tracker::new(),
        }
    }

    /// Returns the number of elements
    #[must_use]
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Returns true if the list is empty
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Returns the element at `index`
    #[must_use]
    pub fn get(&self, index: usize) -> Option<&T> {
        self.items.get(index)
    }

    /// Returns the first element
    #[must_use]
    pub fn first(&self) -> Option<&T> {
        self.items.front()
    }

    /// Returns the last element
    #[must_use]
    pub fn last(&self) -> Option<&T> {
        self.items.back()
    }

    /// Returns a mutable reference to the element at `index`, marking it changed
    pub fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        let key = *self.keys.get(index)?;
        self.tracker.touch(&key, true);
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

        self.edit_items(|items| items.push_back(value));
        self.keys.push_back(key);
        self.tracker.touch(&key, false);
    }

    /// Prepends an element
    pub fn push_front(&mut self, value: T) {
        self.insert(0, value);
    }

    /// Removes and returns the last element
    pub fn pop(&mut self) -> Option<T> {
        let item = self.edit_items(imbl::Vector::pop_back)?;
        let key = self
            .keys
            .pop_back()
            .expect("keys and items have equal length");
        self.tracker.touch(&key, true);
        Some(item)
    }

    /// Removes and returns the first element
    pub fn pop_front(&mut self) -> Option<T> {
        let item = self.edit_items(imbl::Vector::pop_front)?;
        let key = self
            .keys
            .pop_front()
            .expect("keys and items have equal length");
        self.tracker.touch(&key, true);
        Some(item)
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

        self.edit_items(|items| items.insert(index, value));
        self.keys.insert(index, key);
        self.tracker.touch(&key, false);
    }

    /// Removes and returns the element at `index`, shifting later elements left
    ///
    /// # Panics
    ///
    /// Panics if `index >= len`
    pub fn remove(&mut self, index: usize) -> T {
        let item = self.edit_items(|items| items.remove(index));
        let key = self.keys.remove(index);
        self.tracker.touch(&key, true);
        item
    }

    /// Shortens the list to `len` elements
    pub fn truncate(&mut self, len: usize) {
        if len >= self.len() {
            return;
        }

        self.edit_items(|items| items.truncate(len));
        for key in self.keys.iter().skip(len) {
            self.tracker.touch(key, true);
        }

        self.keys.truncate(len);
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
        // a panicking comparator leaves only the copy half sorted
        self.edit_items(|items| items.sort_by(compare));
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
        for key in self.keys.iter() {
            self.tracker.touch(key, false);
        }
    }

    fn edit_items<R>(&mut self, edit: impl FnOnce(&mut imbl::Vector<T>) -> R) -> R {
        let mut items = self.items.clone();
        let result = edit(&mut items);
        self.items = items;
        result
    }

    fn touch_all(&mut self) {
        for key in self.keys.iter() {
            self.tracker.touch(key, true);
        }
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
            tracker: self.tracker.clone(),
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
    ) -> Result<(Self, Layout)> {
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

        // order keys are plain integers, so their bytes are always the ones a write produces
        Ok((vec, Layout::Current))
    }

    fn all_ops(&self, table: &str, ops: &mut Vec<Op>) -> Result<()> {
        for (key, item) in self.keys.iter().zip(self.items.iter()) {
            ops.push(Op {
                key: key.to_be_bytes().to_vec(),
                value: Some(codec::encode_checked(table, item)?),
            });
        }

        Ok(())
    }

    fn take_ops(&mut self, previous: &Self, table: &str, ops: &mut Vec<Op>) -> Result<()> {
        // a second guard for the ordering rule above: never write a list whose rows and keys
        // disagree
        if self.items.len() != self.keys.len() {
            return Err(Error::Encode {
                table: table.to_owned(),
                message: "list elements and their order keys are out of step".to_owned(),
            });
        }

        match self.tracker.take(&previous.tracker) {
            // the list was swapped for a different one, so its dirty set says nothing about storage
            Pending::Rewrite => {
                for key in previous.keys.iter() {
                    if self.keys.binary_search(key).is_err() {
                        ops.push(Op {
                            key: key.to_be_bytes().to_vec(),
                            value: None,
                        });
                    }
                }

                self.all_ops(table, ops)
            }
            Pending::Keys(dirty) => {
                for (key, was_stored) in dirty {
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
    }
}
