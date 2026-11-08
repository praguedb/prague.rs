use std::any::Any;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::Hash;
use std::ops::Bound;
use std::sync::Arc;

use crate::cache::{Cache, IndexData, Inner};

type Of<V, I> = Arc<dyn Fn(&V) -> Option<I> + Send + Sync>;

/// Narrows a query's candidates. Lanes come from an index (`eq`, `any_of`,
/// `gte`, …) and AND together in a query; [`or`] unions them.
pub struct Lane<K, V> {
    /// The cache the lane reads, by address: a lane only plans a query on it.
    pub(crate) cache: usize,
    pub(crate) imp: Box<dyn LaneImpl<K, V>>,
}

/// The lane operations, all run under the owning cache's read lock.
pub(crate) trait LaneImpl<K, V>: Send + Sync {
    /// Counts candidates, stopping once the count exceeds `limit`.
    fn estimate(&self, inner: &Inner<K, V>, limit: usize) -> usize;
    /// Yields candidate keys until `f` returns false; reports whether it ran to the end.
    fn each(&self, inner: &Inner<K, V>, f: &mut dyn FnMut(&K) -> bool) -> bool;
    /// Whether `v` belongs to the lane.
    fn matches(&self, v: &V) -> bool;
}

fn cache_id<K, V>(c: &Arc<Cache<K, V>>) -> usize {
    Arc::as_ptr(c) as *const () as usize
}

/// The union of lanes over one cache.
pub fn or<K, V>(lanes: impl IntoIterator<Item = Lane<K, V>>) -> Lane<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    let lanes: Vec<Lane<K, V>> = lanes.into_iter().collect();
    let cache = lanes.first().map_or(0, |l| l.cache);
    assert!(
        lanes.iter().all(|l| l.cache == cache),
        "prague: or() of lanes over different caches"
    );
    Lane {
        cache,
        imp: Box::new(OrLane(lanes.into_iter().map(|l| l.imp).collect())),
    }
}

struct OrLane<K, V>(Vec<Box<dyn LaneImpl<K, V>>>);

impl<K, V> LaneImpl<K, V> for OrLane<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    fn estimate(&self, inner: &Inner<K, V>, limit: usize) -> usize {
        let mut n = 0;
        for l in &self.0 {
            n += l.estimate(inner, limit.saturating_sub(n));
            if n > limit {
                break;
            }
        }
        n
    }

    fn each(&self, inner: &Inner<K, V>, f: &mut dyn FnMut(&K) -> bool) -> bool {
        if let [one] = self.0.as_slice() {
            return one.each(inner, f);
        }
        let mut seen = HashSet::new();
        for l in &self.0 {
            let done = l.each(inner, &mut |k| {
                if seen.insert(k.clone()) { f(k) } else { true }
            });
            if !done {
                return false;
            }
        }
        true
    }

    fn matches(&self, v: &V) -> bool {
        self.0.iter().any(|l| l.matches(v))
    }
}

// ---- Unique ----------------------------------------------------------------

struct UniqueData<K, V, I> {
    of: Of<V, I>,
    m: HashMap<I, K>,
}

impl<K, V, I> IndexData<K, V> for UniqueData<K, V, I>
where
    K: Eq + Clone + Send + Sync + 'static,
    V: 'static,
    I: Eq + Hash + Send + Sync + 'static,
{
    fn add(&mut self, k: &K, v: &V) {
        if let Some(i) = (self.of)(v) {
            self.m.insert(i, k.clone());
        }
    }

    fn remove(&mut self, k: &K, v: &V) {
        if let Some(i) = (self.of)(v)
            && self.m.get(&i) == Some(k)
        {
            self.m.remove(&i);
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Maps one indexed value to one key. If two keys share a value, the latest
/// write owns it.
pub struct UniqueIndex<K, V, I> {
    c: Arc<Cache<K, V>>,
    slot: usize,
    of: Of<V, I>,
}

impl<K, V> Cache<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    /// Indexes `of(v)`, which must be unique across the cache.
    pub fn add_unique<I>(
        self: &Arc<Self>,
        of: impl Fn(&V) -> I + Send + Sync + 'static,
    ) -> UniqueIndex<K, V, I>
    where
        I: Eq + Hash + Clone + Send + Sync + 'static,
    {
        self.add_unique_opt(move |v| Some(of(v)))
    }

    /// Indexes `of(v)` when it is `Some`; values without one are not indexed.
    pub fn add_unique_opt<I>(
        self: &Arc<Self>,
        of: impl Fn(&V) -> Option<I> + Send + Sync + 'static,
    ) -> UniqueIndex<K, V, I>
    where
        I: Eq + Hash + Clone + Send + Sync + 'static,
    {
        let of: Of<V, I> = Arc::new(of);
        let slot = self.register(Box::new(UniqueData {
            of: of.clone(),
            m: HashMap::new(),
        }));
        UniqueIndex {
            c: self.clone(),
            slot,
            of,
        }
    }

    /// Indexes `of(v)`; many keys may share a value.
    pub fn add_many<I>(
        self: &Arc<Self>,
        of: impl Fn(&V) -> I + Send + Sync + 'static,
    ) -> ManyIndex<K, V, I>
    where
        I: Eq + Hash + Clone + Send + Sync + 'static,
    {
        self.add_many_opt(move |v| Some(of(v)))
    }

    /// Indexes `of(v)` when it is `Some`.
    pub fn add_many_opt<I>(
        self: &Arc<Self>,
        of: impl Fn(&V) -> Option<I> + Send + Sync + 'static,
    ) -> ManyIndex<K, V, I>
    where
        I: Eq + Hash + Clone + Send + Sync + 'static,
    {
        let of: Of<V, I> = Arc::new(of);
        let slot = self.register(Box::new(ManyData {
            of: of.clone(),
            m: HashMap::new(),
        }));
        ManyIndex {
            c: self.clone(),
            slot,
            of,
        }
    }

    /// Indexes `of(v)` in order, for range lanes and min/max.
    pub fn add_range<I>(
        self: &Arc<Self>,
        of: impl Fn(&V) -> I + Send + Sync + 'static,
    ) -> RangeIndex<K, V, I>
    where
        I: Ord + Clone + Send + Sync + 'static,
    {
        self.add_range_opt(move |v| Some(of(v)))
    }

    /// Indexes `of(v)` in order when it is `Some`.
    pub fn add_range_opt<I>(
        self: &Arc<Self>,
        of: impl Fn(&V) -> Option<I> + Send + Sync + 'static,
    ) -> RangeIndex<K, V, I>
    where
        I: Ord + Clone + Send + Sync + 'static,
    {
        let of: Of<V, I> = Arc::new(of);
        let slot = self.register(Box::new(RangeData {
            of: of.clone(),
            t: BTreeMap::new(),
        }));
        RangeIndex {
            c: self.clone(),
            slot,
            of,
        }
    }
}

impl<K, V, I> UniqueIndex<K, V, I>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
    I: Eq + Hash + Clone + Send + Sync + 'static,
{
    /// The value whose indexed field equals `i`.
    pub fn get(&self, i: &I) -> Option<Arc<V>> {
        let inner = self.c.read();
        let k = inner.index::<UniqueData<K, V, I>>(self.slot).m.get(i)?;
        inner.items.get(k).map(|e| e.v.clone())
    }

    /// The lane of the value whose indexed field equals `i`.
    pub fn eq(&self, i: I) -> Lane<K, V> {
        self.any_of([i])
    }

    /// The lane of values whose indexed field is any of `is`.
    pub fn any_of(&self, is: impl IntoIterator<Item = I>) -> Lane<K, V> {
        let set = is.into_iter().collect();
        Lane {
            cache: cache_id(&self.c),
            imp: Box::new(UniqueLane::<K, V, I> {
                slot: self.slot,
                of: self.of.clone(),
                set,
                k: Default::default(),
            }),
        }
    }
}

struct UniqueLane<K, V, I> {
    slot: usize,
    of: Of<V, I>,
    set: HashSet<I>,
    k: std::marker::PhantomData<fn() -> K>,
}

impl<K, V, I> LaneImpl<K, V> for UniqueLane<K, V, I>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
    I: Eq + Hash + Clone + Send + Sync + 'static,
{
    fn estimate(&self, _: &Inner<K, V>, _: usize) -> usize {
        self.set.len()
    }

    fn each(&self, inner: &Inner<K, V>, f: &mut dyn FnMut(&K) -> bool) -> bool {
        let m = &inner.index::<UniqueData<K, V, I>>(self.slot).m;
        for i in &self.set {
            if let Some(k) = m.get(i)
                && !f(k)
            {
                return false;
            }
        }
        true
    }

    fn matches(&self, v: &V) -> bool {
        (self.of)(v).is_some_and(|i| self.set.contains(&i))
    }
}

// ---- Many ------------------------------------------------------------------

struct ManyData<K, V, I> {
    of: Of<V, I>,
    m: HashMap<I, HashSet<K>>,
}

impl<K, V, I> IndexData<K, V> for ManyData<K, V, I>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: 'static,
    I: Eq + Hash + Send + Sync + 'static,
{
    fn add(&mut self, k: &K, v: &V) {
        if let Some(i) = (self.of)(v) {
            self.m.entry(i).or_default().insert(k.clone());
        }
    }

    fn remove(&mut self, k: &K, v: &V) {
        if let Some(i) = (self.of)(v)
            && let Some(b) = self.m.get_mut(&i)
        {
            b.remove(k);
            if b.is_empty() {
                self.m.remove(&i);
            }
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Maps an indexed value to every key holding it.
pub struct ManyIndex<K, V, I> {
    pub(crate) c: Arc<Cache<K, V>>,
    slot: usize,
    of: Of<V, I>,
}

impl<K, V, I> ManyIndex<K, V, I>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
    I: Eq + Hash + Clone + Send + Sync + 'static,
{
    /// Every value whose indexed field equals `i`, in no fixed order.
    pub fn lookup(&self, i: &I) -> Vec<Arc<V>> {
        self.lookup_locked(&self.c.read(), i)
    }

    pub(crate) fn lookup_locked(&self, inner: &Inner<K, V>, i: &I) -> Vec<Arc<V>> {
        match inner.index::<ManyData<K, V, I>>(self.slot).m.get(i) {
            Some(b) => b
                .iter()
                .filter_map(|k| inner.items.get(k).map(|e| e.v.clone()))
                .collect(),
            None => Vec::new(),
        }
    }

    /// How many values hold `i`.
    pub fn count(&self, i: &I) -> usize {
        let inner = self.c.read();
        inner
            .index::<ManyData<K, V, I>>(self.slot)
            .m
            .get(i)
            .map_or(0, HashSet::len)
    }

    /// The lane of values whose indexed field equals `i`.
    pub fn eq(&self, i: I) -> Lane<K, V> {
        self.any_of([i])
    }

    /// The lane of values whose indexed field is any of `is`.
    pub fn any_of(&self, is: impl IntoIterator<Item = I>) -> Lane<K, V> {
        let set = is.into_iter().collect();
        Lane {
            cache: cache_id(&self.c),
            imp: Box::new(ManyLane::<K, V, I> {
                slot: self.slot,
                of: self.of.clone(),
                set,
                k: Default::default(),
            }),
        }
    }
}

struct ManyLane<K, V, I> {
    slot: usize,
    of: Of<V, I>,
    set: HashSet<I>,
    k: std::marker::PhantomData<fn() -> K>,
}

impl<K, V, I> LaneImpl<K, V> for ManyLane<K, V, I>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
    I: Eq + Hash + Clone + Send + Sync + 'static,
{
    fn estimate(&self, inner: &Inner<K, V>, limit: usize) -> usize {
        let m = &inner.index::<ManyData<K, V, I>>(self.slot).m;
        let mut n = 0;
        for i in &self.set {
            n += m.get(i).map_or(0, HashSet::len);
            if n > limit {
                break;
            }
        }
        n
    }

    fn each(&self, inner: &Inner<K, V>, f: &mut dyn FnMut(&K) -> bool) -> bool {
        let m = &inner.index::<ManyData<K, V, I>>(self.slot).m;
        for i in &self.set {
            for k in m.get(i).into_iter().flatten() {
                if !f(k) {
                    return false;
                }
            }
        }
        true
    }

    fn matches(&self, v: &V) -> bool {
        (self.of)(v).is_some_and(|i| self.set.contains(&i))
    }
}

// ---- Range -----------------------------------------------------------------

struct RangeData<K, V, I> {
    of: Of<V, I>,
    t: BTreeMap<I, HashSet<K>>,
}

impl<K, V, I> IndexData<K, V> for RangeData<K, V, I>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: 'static,
    I: Ord + Send + Sync + 'static,
{
    fn add(&mut self, k: &K, v: &V) {
        if let Some(i) = (self.of)(v) {
            self.t.entry(i).or_default().insert(k.clone());
        }
    }

    fn remove(&mut self, k: &K, v: &V) {
        if let Some(i) = (self.of)(v)
            && let Some(b) = self.t.get_mut(&i)
        {
            b.remove(k);
            if b.is_empty() {
                self.t.remove(&i);
            }
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Orders values by an indexed field, for range lanes and min/max.
pub struct RangeIndex<K, V, I> {
    c: Arc<Cache<K, V>>,
    slot: usize,
    of: Of<V, I>,
}

impl<K, V, I> RangeIndex<K, V, I>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
    I: Ord + Clone + Send + Sync + 'static,
{
    /// The smallest indexed value.
    pub fn min(&self) -> Option<I> {
        self.c
            .read()
            .index::<RangeData<K, V, I>>(self.slot)
            .t
            .keys()
            .next()
            .cloned()
    }

    /// The largest indexed value.
    pub fn max(&self) -> Option<I> {
        self.c
            .read()
            .index::<RangeData<K, V, I>>(self.slot)
            .t
            .keys()
            .next_back()
            .cloned()
    }

    fn lane(&self) -> RangeLane<K, V, I> {
        RangeLane {
            cache: cache_id(&self.c),
            slot: self.slot,
            of: self.of.clone(),
            lo: Bound::Unbounded,
            hi: Bound::Unbounded,
            k: Default::default(),
        }
    }

    pub fn gt(&self, i: I) -> RangeLane<K, V, I> {
        self.lane().gt(i)
    }
    pub fn gte(&self, i: I) -> RangeLane<K, V, I> {
        self.lane().gte(i)
    }
    pub fn lt(&self, i: I) -> RangeLane<K, V, I> {
        self.lane().lt(i)
    }
    pub fn lte(&self, i: I) -> RangeLane<K, V, I> {
        self.lane().lte(i)
    }
}

/// A bounded interval over a [`RangeIndex`]; chain the other side:
/// `ix.gte(a).lt(b)` is `[a, b)`. A query takes it as a lane.
pub struct RangeLane<K, V, I> {
    cache: usize,
    slot: usize,
    of: Of<V, I>,
    lo: Bound<I>,
    hi: Bound<I>,
    k: std::marker::PhantomData<fn() -> K>,
}

impl<K, V, I: Ord> RangeLane<K, V, I> {
    pub fn gt(mut self, i: I) -> Self {
        self.lo = Bound::Excluded(i);
        self
    }
    pub fn gte(mut self, i: I) -> Self {
        self.lo = Bound::Included(i);
        self
    }
    pub fn lt(mut self, i: I) -> Self {
        self.hi = Bound::Excluded(i);
        self
    }
    pub fn lte(mut self, i: I) -> Self {
        self.hi = Bound::Included(i);
        self
    }

    fn contains(&self, i: &I) -> bool {
        let lo = match &self.lo {
            Bound::Included(b) => i >= b,
            Bound::Excluded(b) => i > b,
            Bound::Unbounded => true,
        };
        let hi = match &self.hi {
            Bound::Included(b) => i <= b,
            Bound::Excluded(b) => i < b,
            Bound::Unbounded => true,
        };
        lo && hi
    }

    /// The buckets inside the interval, ascending. An empty or inverted
    /// interval has none (BTreeMap::range would panic on it).
    fn buckets<'a>(
        &'a self,
        t: &'a BTreeMap<I, HashSet<K>>,
    ) -> Box<dyn Iterator<Item = &'a HashSet<K>> + 'a> {
        let empty = match (&self.lo, &self.hi) {
            (Bound::Included(a) | Bound::Excluded(a), Bound::Included(b) | Bound::Excluded(b)) => {
                a > b
                    || (a == b
                        && !(matches!(self.lo, Bound::Included(_))
                            && matches!(self.hi, Bound::Included(_))))
            }
            _ => false,
        };
        if empty {
            return Box::new(std::iter::empty());
        }
        Box::new(
            t.range((self.lo.as_ref(), self.hi.as_ref()))
                .map(|(_, b)| b),
        )
    }
}

impl<K, V, I> LaneImpl<K, V> for RangeLane<K, V, I>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
    I: Ord + Clone + Send + Sync + 'static,
{
    fn estimate(&self, inner: &Inner<K, V>, limit: usize) -> usize {
        let t = &inner.index::<RangeData<K, V, I>>(self.slot).t;
        let mut n = 0;
        for b in self.buckets(t) {
            n += b.len();
            if n > limit {
                break;
            }
        }
        n
    }

    fn each(&self, inner: &Inner<K, V>, f: &mut dyn FnMut(&K) -> bool) -> bool {
        let t = &inner.index::<RangeData<K, V, I>>(self.slot).t;
        for b in self.buckets(t) {
            for k in b {
                if !f(k) {
                    return false;
                }
            }
        }
        true
    }

    fn matches(&self, v: &V) -> bool {
        (self.of)(v).is_some_and(|i| self.contains(&i))
    }
}

impl<K, V, I> From<RangeLane<K, V, I>> for Lane<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
    I: Ord + Clone + Send + Sync + 'static,
{
    fn from(l: RangeLane<K, V, I>) -> Self {
        Lane {
            cache: l.cache,
            imp: Box::new(l),
        }
    }
}
