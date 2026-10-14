use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

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

/// Records which keys an update touched, and which committed contents a collection derives from
pub(crate) struct Tracker<K> {
    // keys touched since the last commit, each with whether it was stored before the update;
    // the key's presence at commit decides put or delete, and a key that was neither stored
    // before nor present now needs no write at all
    dirty: BTreeMap<K, bool>,
    // identity of the committed contents the collection derives from
    epoch: u64,
}

/// What an update must write for one collection
pub(crate) enum Pending<K> {
    /// The collection was replaced wholesale, so its dirty set says nothing about storage and
    /// the table must be rewritten
    Rewrite,
    /// Each touched key with whether it was stored before the update
    Keys(BTreeMap<K, bool>),
}

impl<K: Ord + Clone> Tracker<K> {
    /// Starts tracking with a fresh epoch and nothing touched
    pub(crate) fn new() -> Self {
        Self {
            dirty: BTreeMap::new(),
            epoch: next_epoch(),
        }
    }

    /// Records a touch of `key`, saying whether the key was stored before the update
    pub(crate) fn touch(&mut self, key: &K, stored: bool) {
        // only the first touch counts: any earlier change to the key would have touched it
        // already, so at that moment the key is present exactly when it was stored
        if !self.dirty.contains_key(key) {
            self.dirty.insert(key.clone(), stored);
        }
    }

    /// Takes the pending changes relative to `previous`, the committed tracker this update
    /// started from, then gives the collection a new epoch
    pub(crate) fn take(&mut self, previous: &Self) -> Pending<K> {
        let dirty = std::mem::take(&mut self.dirty);
        let replaced = self.epoch != previous.epoch;
        self.epoch = next_epoch();

        if replaced {
            Pending::Rewrite
        } else {
            Pending::Keys(dirty)
        }
    }
}

impl<K: Clone> Clone for Tracker<K> {
    fn clone(&self) -> Self {
        Self {
            dirty: self.dirty.clone(),
            epoch: self.epoch,
        }
    }
}
