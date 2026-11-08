//! A read-side, in-memory event cache fed by compacted Kafka topics written by
//! Prague (.NET) producers: the Rust sibling of prague.go.
//!
//! A [`Cache`] holds the latest value per key. Secondary indexes are declared
//! in code ([`Cache::add_unique`], [`Cache::add_many`], [`Cache::add_range`])
//! because which lookups a consumer needs is a property of the consumer, not of
//! the data. Queries intersect index lanes, apply residual filters, sort and
//! page.
//!
//! Concurrency: one writer (the Kafka consumer), any number of readers. Writes
//! take an exclusive lock; a query holds a shared lock for its whole run, so a
//! result is a consistent snapshot. Values are handed out as `Arc<V>`.
//!
//! Contracts are not hand-written: praguec compiles the producer's `.prague`
//! schema to Rust (`praguec generate --lang rust`), and the generated types
//! implement [`wire::Decode`] and [`Keyed`].

mod cache;
mod index;
mod join;
mod query;
mod topic;
pub mod wire;

#[cfg(feature = "kafka")]
pub mod kafka;

pub use cache::{Cache, Keyed, UpdateType};
pub use index::{Lane, ManyIndex, RangeIndex, RangeLane, UniqueIndex, or};
pub use join::{Many, One, join_many, join_one};
pub use query::{Query, Results};
pub use topic::resolve_topic;
