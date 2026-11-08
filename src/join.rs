use std::hash::Hash;
use std::sync::Arc;

use crate::cache::Cache;
use crate::index::ManyIndex;

/// A left row with the right value its foreign key names, if any.
#[derive(Debug, Clone)]
pub struct One<L, R> {
    pub left: L,
    pub right: Option<Arc<R>>,
}

/// A left row with every right value that refers to it.
#[derive(Debug, Clone)]
pub struct Many<L, R> {
    pub left: L,
    pub right: Vec<Arc<R>>,
}

/// Pairs each row with `right`'s value under `fk(row)`: the many-to-one or
/// one-to-one direction of a `[references(T)]`. Rows whose `fk` is `None`, or
/// names nothing, get no right; pass `inner` to drop them instead.
///
/// The right cache is read under one lock for the whole batch, but not under
/// the lock that produced the rows: two caches fed by two topics are never a
/// transaction, as in Prague (.NET).
pub fn join_one<L, RK, R>(
    rows: impl IntoIterator<Item = L>,
    right: &Cache<RK, R>,
    fk: impl Fn(&L) -> Option<RK>,
    inner: bool,
) -> Vec<One<L, R>>
where
    RK: Eq + Hash + Clone + Send + Sync + 'static,
    R: Send + Sync + 'static,
{
    let r = right.read();
    rows.into_iter()
        .map(|left| {
            let right = fk(&left).and_then(|k| r.items.get(&k).map(|e| e.v.clone()));
            One { left, right }
        })
        .filter(|j| !inner || j.right.is_some())
        .collect()
}

/// Pairs each row with the right values whose indexed foreign key equals
/// `key(row)`: the one-to-many direction, driven by a Many index on the
/// referring cache. Pass `inner` to drop rows with no match.
pub fn join_many<L, RK, R, I>(
    rows: impl IntoIterator<Item = L>,
    by: &ManyIndex<RK, R, I>,
    key: impl Fn(&L) -> I,
    inner: bool,
) -> Vec<Many<L, R>>
where
    RK: Eq + Hash + Clone + Send + Sync + 'static,
    R: Send + Sync + 'static,
    I: Eq + Hash + Clone + Send + Sync + 'static,
{
    let r = by.c.read();
    rows.into_iter()
        .map(|left| {
            let right = by.lookup_locked(&r, &key(&left));
            Many { left, right }
        })
        .filter(|j| !inner || !j.right.is_empty())
        .collect()
}
