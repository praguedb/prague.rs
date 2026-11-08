use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// What a write did. The numbering matches Prague (.NET).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum UpdateType {
    /// Rejected by a filter, nothing stored.
    Filtered = 0,
    /// The incoming value equals the stored one.
    Same = 1,
    Add = 2,
    Update = 3,
    Delete = 4,
}

impl fmt::Display for UpdateType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

/// A value that names its own identity. praguec-generated tables and views
/// implement it.
pub trait Keyed {
    type Key: Eq + Hash + Clone + Send + Sync + 'static;
    fn cache_key(&self) -> Self::Key;
}

pub(crate) struct Entry<V> {
    pub(crate) v: Arc<V>,
    /// Fingerprint of the encoded value; 0 = unknown.
    fp: u64,
    /// Unix ms of the write.
    ts: i64,
}

/// An index's state, kept inside the cache's lock so a write updates the
/// value and every index at once, and a read sees them agree.
pub(crate) trait IndexData<K, V>: Send + Sync {
    fn add(&mut self, k: &K, v: &V);
    fn remove(&mut self, k: &K, v: &V);
    fn as_any(&self) -> &dyn Any;
}

pub(crate) struct Inner<K, V> {
    pub(crate) items: HashMap<K, Entry<V>>,
    pub(crate) indexes: Vec<Box<dyn IndexData<K, V>>>,
}

impl<K, V> Inner<K, V> {
    /// The state of the index at `slot`, as the type its handle created.
    pub(crate) fn index<T: 'static>(&self, slot: usize) -> &T {
        self.indexes[slot]
            .as_any()
            .downcast_ref::<T>()
            .expect("an index handle names its own state")
    }
}

/// The latest value per key, plus its secondary indexes. Clone the `Arc` to
/// share it between the consumer feeding it and the code reading it.
pub struct Cache<K, V> {
    name: String,
    key_of: Box<dyn Fn(&V) -> K + Send + Sync>,
    pub(crate) inner: RwLock<Inner<K, V>>,
}

impl<K, V> Cache<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    /// A cache for a type that knows its key.
    pub fn new(name: impl Into<String>) -> Arc<Self>
    where
        V: Keyed<Key = K>,
    {
        Self::with_key(name, |v: &V| v.cache_key())
    }

    /// A cache whose key is extracted by `key_of`.
    pub fn with_key(
        name: impl Into<String>,
        key_of: impl Fn(&V) -> K + Send + Sync + 'static,
    ) -> Arc<Self> {
        Arc::new(Self {
            name: name.into(),
            key_of: Box::new(key_of),
            inner: RwLock::new(Inner {
                items: HashMap::new(),
                indexes: Vec::new(),
            }),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn read(&self) -> RwLockReadGuard<'_, Inner<K, V>> {
        // A panic in a reader's closure poisons nothing that matters: the data
        // is only ever changed whole, under the write lock.
        self.inner.read().unwrap_or_else(|e| e.into_inner())
    }

    fn write(&self) -> RwLockWriteGuard<'_, Inner<K, V>> {
        self.inner.write().unwrap_or_else(|e| e.into_inner())
    }

    pub fn len(&self) -> usize {
        self.read().items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The value stored under `k`.
    pub fn get(&self, k: &K) -> Option<Arc<V>> {
        self.read().items.get(k).map(|e| e.v.clone())
    }

    /// The values found for `keys`, in key order, skipping misses.
    pub fn get_many<'a>(&self, keys: impl IntoIterator<Item = &'a K>) -> Vec<Arc<V>> {
        let inner = self.read();
        keys.into_iter()
            .filter_map(|k| inner.items.get(k).map(|e| e.v.clone()))
            .collect()
    }

    /// The Unix-ms timestamp of `k`'s last write.
    pub fn last_updated(&self, k: &K) -> Option<i64> {
        self.read().items.get(k).map(|e| e.ts)
    }

    /// Calls `f` for every entry under the shared lock until it returns false.
    /// `f` must not write to this cache.
    pub fn each(&self, mut f: impl FnMut(&K, &Arc<V>) -> bool) {
        for (k, e) in &self.read().items {
            if !f(k, &e.v) {
                return;
            }
        }
    }

    /// Stores `v` under its own key. Without a fingerprint every write of an
    /// existing key counts as an Update.
    pub fn upsert(&self, v: V, ts: i64) -> UpdateType {
        let k = (self.key_of)(&v);
        self.apply(k, v, 0, ts).0
    }

    /// Stores `v` under `k`. `fp` is a fingerprint of the encoded value: when it
    /// is non-zero and equals the stored one, nothing changes and the result is
    /// Same. It is how a feeder suppresses re-published, unchanged records.
    /// Returns what the write did and the value it replaced.
    pub fn apply(&self, k: K, v: V, fp: u64, ts: i64) -> (UpdateType, Option<Arc<V>>) {
        let v = Arc::new(v);
        let mut inner = self.write();
        let Inner { items, indexes } = &mut *inner;
        if let Some(e) = items.get_mut(&k) {
            if fp != 0 && e.fp == fp {
                return (UpdateType::Same, Some(e.v.clone()));
            }
            for ix in indexes.iter_mut() {
                ix.remove(&k, &e.v);
                ix.add(&k, &v);
            }
            let old = std::mem::replace(&mut e.v, v);
            e.fp = fp;
            e.ts = ts;
            return (UpdateType::Update, Some(old));
        }
        for ix in indexes.iter_mut() {
            ix.add(&k, &v);
        }
        items.insert(k, Entry { v, fp, ts });
        (UpdateType::Add, None)
    }

    /// The fingerprint stored for `k`, so a feeder can skip decoding a value it
    /// already holds.
    pub fn fingerprint(&self, k: &K) -> Option<u64> {
        self.read().items.get(k).map(|e| e.fp)
    }

    /// Deletes `k` and returns what was stored.
    pub fn remove(&self, k: &K) -> Option<Arc<V>> {
        let mut inner = self.write();
        let e = inner.items.remove(k)?;
        for ix in inner.indexes.iter_mut() {
            ix.remove(k, &e.v);
        }
        Some(e.v)
    }

    /// Adds an index, backfilled from what is already stored, and returns its slot.
    pub(crate) fn register(&self, mut ix: Box<dyn IndexData<K, V>>) -> usize {
        let mut inner = self.write();
        for (k, e) in &inner.items {
            ix.add(k, &e.v);
        }
        inner.indexes.push(ix);
        inner.indexes.len() - 1
    }
}

impl<K, V> fmt::Debug for Cache<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Cache")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}
