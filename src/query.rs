use std::cmp::Ordering;
use std::hash::Hash;
use std::sync::Arc;

use crate::cache::{Cache, Inner};
use crate::index::Lane;

type Filter<V> = Box<dyn Fn(&V) -> bool>;
type Order<V> = Box<dyn Fn(&V, &V) -> Ordering>;

/// A one-shot read: index lanes AND together, `filter` keeps what survives,
/// `sort` orders, `page` slices. Build it with [`Cache::query`].
pub struct Query<'c, K, V> {
    c: &'c Cache<K, V>,
    lanes: Vec<Lane<K, V>>,
    filters: Vec<Filter<V>>,
    order: Option<Order<V>>,
    skip: usize,
    take: usize,
}

/// A query's page plus the number of matches before paging.
#[derive(Debug, Clone)]
pub struct Results<V> {
    pub items: Vec<Arc<V>>,
    pub total_count: usize,
}

impl<K, V> Cache<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    pub fn query(&self) -> Query<'_, K, V> {
        Query {
            c: self,
            lanes: Vec::new(),
            filters: Vec::new(),
            order: None,
            skip: 0,
            take: usize::MAX,
        }
    }
}

impl<'c, K, V> Query<'c, K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    /// Adds a lane. A value must match every lane.
    ///
    /// # Panics
    ///
    /// When the lane comes from an index on another cache.
    pub fn with(mut self, lane: impl Into<Lane<K, V>>) -> Self {
        let lane = lane.into();
        assert_eq!(
            lane.cache, self.c as *const _ as *const () as usize,
            "prague: a lane from another cache's index"
        );
        self.lanes.push(lane);
        self
    }

    /// Adds a residual filter, run on candidates after the lanes.
    pub fn filter(mut self, f: impl Fn(&V) -> bool + 'static) -> Self {
        self.filters.push(Box::new(f));
        self
    }

    /// Orders the result, stably. Make `cmp` total if pages must be stable.
    pub fn sort(mut self, cmp: impl Fn(&V, &V) -> Ordering + 'static) -> Self {
        self.order = Some(Box::new(cmp));
        self
    }

    /// Keeps `take` results after skipping `skip`.
    pub fn page(mut self, skip: usize, take: usize) -> Self {
        self.skip = skip;
        self.take = take;
        self
    }

    /// Runs the query.
    pub fn execute(self) -> Results<V> {
        let inner = self.c.read();
        let mut all = Vec::new();
        let Some(order) = &self.order else {
            // Unsorted: collect only the page, count the rest.
            let mut n = 0usize;
            self.scan(&inner, &mut |v| {
                if n >= self.skip && n - self.skip < self.take {
                    all.push(v.clone());
                }
                n += 1;
                true
            });
            return Results {
                items: all,
                total_count: n,
            };
        };
        self.scan(&inner, &mut |v| {
            all.push(v.clone());
            true
        });
        all.sort_by(|a, b| order(a, b));
        let total = all.len();
        let lo = self.skip.min(total);
        let hi = lo + self.take.min(total - lo);
        all.truncate(hi);
        all.drain(..lo);
        Results {
            items: all,
            total_count: total,
        }
    }

    /// The number of matches, without materializing them.
    pub fn count(self) -> usize {
        let inner = self.c.read();
        let mut n = 0;
        self.scan(&inner, &mut |_| {
            n += 1;
            true
        });
        n
    }

    /// One match, or the first by sort order when set.
    pub fn first(self) -> Option<Arc<V>> {
        self.page(0, 1).execute().items.pop()
    }

    /// Yields every match. The plan: drive from the lane with the fewest
    /// candidates and check the others by value; no lanes means a full scan.
    /// An empty driving lane ends the query before anything is fetched.
    fn scan(&self, inner: &Inner<K, V>, f: &mut dyn FnMut(&Arc<V>) -> bool) {
        let keep = |v: &V| self.filters.iter().all(|p| p(v));
        if self.lanes.is_empty() {
            for e in inner.items.values() {
                if keep(&e.v) && !f(&e.v) {
                    return;
                }
            }
            return;
        }
        let (mut drive, mut best) = (0, usize::MAX);
        for (i, l) in self.lanes.iter().enumerate() {
            let n = l.imp.estimate(inner, best);
            if n < best {
                (drive, best) = (i, n);
            }
            if best == 0 {
                return;
            }
        }
        self.lanes[drive].imp.each(inner, &mut |k| {
            let Some(e) = inner.items.get(k) else {
                return true;
            };
            for (i, l) in self.lanes.iter().enumerate() {
                if i != drive && !l.imp.matches(&e.v) {
                    return true;
                }
            }
            if !keep(&e.v) {
                return true;
            }
            f(&e.v)
        });
    }
}
