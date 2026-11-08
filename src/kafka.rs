//! Feeds prague caches from compacted topics written by Prague (.NET)
//! producers, on Confluent's native Rust client.
//!
//! The wire contract is Prague's: the message key is the MessagePack encoding
//! of the cache key, the value is the MessagePack encoding of the item, and an
//! empty value is a tombstone. Every topic is read from the beginning, with no
//! consumer group and no committed offsets, so every process holds the whole
//! topic and a restart is a full reload.
//!
//! A topic is loaded once the consumer's position on every partition has
//! reached the end offset observed at startup. After-handlers fire only once a
//! topic is loaded, so they see live changes and not the replay.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::future::Future;
use std::hash::{BuildHasher, Hash, Hasher, RandomState};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::time::Duration;

use confluent_kafka::common::TopicPartition;
use confluent_kafka::common::header::Header;
use confluent_kafka::common::serialization::ByteArrayDeserializer;
use confluent_kafka::consumer::{
    Consumer as KafkaApi, ConsumerConfig, ConsumerRecord, KafkaConsumer,
};
use tokio::sync::watch;

use crate::wire::{self, Decode, HeaderValue, Meta};
use crate::{Cache, UpdateType, resolve_topic};

/// The records this feeder reads: raw key and value bytes.
pub type Record = ConsumerRecord<Vec<u8>, Vec<u8>>;

/// A feeder failure: a topic that cannot be bound or read, or the client's own.
#[derive(Debug)]
pub enum Error {
    Config(String),
    Kafka(confluent_kafka::common::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Config(s) => write!(f, "prague/kafka: {s}"),
            Error::Kafka(e) => write!(f, "prague/kafka: {e}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<confluent_kafka::common::Error> for Error {
    fn from(e: confluent_kafka::common::Error) -> Self {
        Error::Kafka(e)
    }
}

/// Configures a [`Consumer`].
#[derive(Clone, Default)]
pub struct Config {
    pub brokers: Vec<String>,
    /// Resolve `[v:name]` placeholders in topic templates.
    pub vars: HashMap<String, String>,
    /// Client properties applied after Prague's own: TLS, SASL, client.id, ….
    pub properties: HashMap<String, String>,
    /// Receives undecodable records and fetch errors. Default: stderr.
    pub on_error: Option<Arc<dyn Fn(String) + Send + Sync>>,
}

/// What a binding does with one record.
trait Binding: Send {
    fn apply(&mut self, r: &Record, live: bool);
}

/// Resolves when every registered topic is loaded. Clone it freely.
#[derive(Clone)]
pub struct Loaded(watch::Receiver<Vec<String>>);

impl Loaded {
    pub fn is_loaded(&self) -> bool {
        self.0.borrow().is_empty()
    }

    /// The topics not yet loaded: what a stalled start is waiting on.
    pub fn pending(&self) -> Vec<String> {
        self.0.borrow().clone()
    }

    /// Waits until every topic is loaded. Returns early, false, if the
    /// consumer stopped first.
    pub async fn wait(&mut self) -> bool {
        self.0.wait_for(Vec::is_empty).await.is_ok()
    }

    /// [`wait`](Self::wait), bounded: on timeout the error names the topics
    /// still loading, so a stalled start says what it is stuck on.
    pub async fn wait_timeout(&mut self, d: Duration) -> Result<(), Error> {
        match tokio::time::timeout(d, self.wait()).await {
            Ok(true) => Ok(()),
            Ok(false) => Err(Error::Config(
                "the consumer stopped before every topic loaded".into(),
            )),
            Err(_) => Err(Error::Config(format!(
                "initial load did not complete after {:.1}s; topics still loading: {:?}",
                d.as_secs_f64(),
                self.pending()
            ))),
        }
    }
}

/// Owns one Kafka client and feeds every registered cache.
pub struct Consumer {
    cfg: Config,
    on_error: Arc<dyn Fn(String) + Send + Sync>,
    bindings: HashMap<String, Box<dyn Binding>>,
    /// The topics not yet loaded.
    loaded: watch::Sender<Vec<String>>,
}

impl Consumer {
    pub fn new(cfg: Config) -> Self {
        let on_error = cfg
            .on_error
            .clone()
            .unwrap_or_else(|| Arc::new(|e| eprintln!("prague/kafka: {e}")));
        Self {
            cfg,
            on_error,
            bindings: HashMap::new(),
            loaded: watch::Sender::new(Vec::new()),
        }
    }

    /// Binds `cache` to the topic named by `template`.
    pub fn register<K, V>(
        &mut self,
        template: &str,
        cache: Arc<Cache<K, V>>,
        opts: Options<K, V>,
    ) -> Result<(), Error>
    where
        K: Decode + Eq + Hash + Clone + Send + Sync + 'static,
        V: Decode + Send + Sync + 'static,
    {
        let topic = resolve_topic(template, &self.cfg.vars).map_err(Error::Config)?;
        if self.bindings.contains_key(&topic) {
            return Err(Error::Config(format!("topic {topic:?} registered twice")));
        }
        let b = CacheBinding {
            cache,
            opts,
            seed: RandomState::new(),
            on_error: self.on_error.clone(),
        };
        self.loaded.send_modify(|p| p.push(topic.clone()));
        self.bindings.insert(topic, Box::new(b));
        Ok(())
    }

    /// A handle that resolves once every topic is loaded.
    pub fn loaded(&self) -> Loaded {
        Loaded(self.loaded.subscribe())
    }

    /// Consumes until `shutdown` resolves, with a client built from the config.
    /// It fails fast if a topic does not exist: Prague topics are provisioned,
    /// never auto-created.
    pub async fn run(self, shutdown: impl Future<Output = ()> + Send) -> Result<(), Error> {
        let mut props: HashMap<String, String> = HashMap::from([
            ("bootstrap.servers".into(), self.cfg.brokers.join(",")),
            // The only protocol this client implements; no group is joined.
            ("group.protocol".into(), "consumer".into()),
            ("enable.auto.commit".into(), "false".into()),
            ("auto.offset.reset".into(), "earliest".into()),
            ("allow.auto.create.topics".into(), "false".into()),
        ]);
        props.extend(self.cfg.properties.clone());
        let client = KafkaConsumer::new(
            ConsumerConfig::new(&props)?,
            ByteArrayDeserializer::new(),
            ByteArrayDeserializer::new(),
        )?;
        self.run_with(client, shutdown).await
    }

    /// Consumes with `client` until `shutdown` resolves: the seam for a
    /// [`MockConsumer`](confluent_kafka::consumer::MockConsumer) or a client
    /// built elsewhere. The client is assigned every partition of every
    /// registered topic and closed on return.
    pub async fn run_with<C: KafkaApi<Vec<u8>, Vec<u8>>>(
        mut self,
        mut client: C,
        shutdown: impl Future<Output = ()> + Send,
    ) -> Result<(), Error> {
        let result = self.consume(&mut client, shutdown).await;
        let closed = client.close().await;
        result?;
        closed.map_err(Error::from)
    }

    async fn consume<C: KafkaApi<Vec<u8>, Vec<u8>>>(
        &mut self,
        client: &mut C,
        shutdown: impl Future<Output = ()> + Send,
    ) -> Result<(), Error> {
        if self.bindings.is_empty() {
            return Err(Error::Config("no caches registered".into()));
        }
        let mut all = Vec::new();
        // Collected first: an iterator held across the awaits would make the
        // future need the bindings to be Sync.
        let topics: Vec<String> = self.bindings.keys().cloned().collect();
        for topic in &topics {
            let parts = client.partitions_for(topic).await?;
            if parts.is_empty() {
                return Err(Error::Config(format!("topic {topic:?} does not exist")));
            }
            all.extend(
                parts
                    .iter()
                    .map(|p| TopicPartition::new(p.topic().to_string(), p.partition())),
            );
        }
        client.assign(all.clone()).await?;
        client.seek_to_beginning(&all).await?;

        // What a topic must be read up to before it counts as loaded.
        let ends = client.end_offsets(&all).await?;
        let mut pending: HashMap<String, HashMap<TopicPartition, i64>> = HashMap::new();
        for tp in &all {
            let end = ends.get(tp).copied().unwrap_or(0);
            let parts = pending.entry(tp.topic().to_string()).or_default();
            if end > 0 {
                parts.insert(tp.clone(), end);
            }
        }
        let mut live: HashSet<String> = HashSet::new();
        Self::settle(&self.loaded, client, &mut pending, &mut live).await?;

        let mut shutdown = std::pin::pin!(shutdown);
        loop {
            let records = tokio::select! {
                biased;
                () = &mut shutdown => return Ok(()),
                r = client.poll(Duration::from_millis(100)) => r,
            };
            let records = match records {
                Ok(r) => r,
                Err(e) if e.is_wakeup_error() => continue,
                Err(e) => {
                    (self.on_error)(format!("poll: {e}"));
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            if records.is_empty() {
                // A client whose poll returns at once (a mock, a closed
                // fetch) would otherwise starve the shutdown branch and every
                // other task on this thread.
                tokio::task::yield_now().await;
            }
            for r in &records {
                if let Some(b) = self.bindings.get_mut(r.topic()) {
                    b.apply(r, live.contains(r.topic()));
                }
            }
            if live.len() < self.bindings.len() {
                Self::settle(&self.loaded, client, &mut pending, &mut live).await?;
            }
        }
    }

    /// Marks topics loaded whose every partition's position has reached its
    /// end offset. Position, not the last record seen: control records and
    /// deleted segments advance it without a record to see.
    ///
    /// It borrows the signal and not `self`: a `&self` held across the awaits
    /// would need the bindings to be `Sync`, and the consumer's future could
    /// not be spawned.
    async fn settle<C: KafkaApi<Vec<u8>, Vec<u8>>>(
        loaded: &watch::Sender<Vec<String>>,
        client: &mut C,
        pending: &mut HashMap<String, HashMap<TopicPartition, i64>>,
        live: &mut HashSet<String>,
    ) -> Result<(), Error> {
        for (topic, parts) in pending.iter_mut() {
            if live.contains(topic) {
                continue;
            }
            let mut reached = Vec::new();
            for (tp, &end) in parts.iter() {
                if client.position(tp).await? >= end {
                    reached.push(tp.clone());
                }
            }
            for tp in reached {
                parts.remove(&tp);
            }
            if parts.is_empty() {
                live.insert(topic.clone());
            }
        }
        loaded.send_if_modified(|p| {
            let before = p.len();
            p.retain(|t| !live.contains(t));
            p.len() != before
        });
        Ok(())
    }
}

/// Observes a live change: the key, the value now stored (on Delete, the value
/// removed) and the value it replaced. It runs on the consume task and must not
/// block.
type Predicate<T> = Box<dyn Fn(&T) -> bool + Send>;

pub type AfterHandler<K, V> = Box<dyn FnMut(UpdateType, &K, &Arc<V>, Option<&Arc<V>>) + Send>;

/// How one cache is fed from its topic.
pub struct Options<K, V> {
    key_filters: Vec<Predicate<K>>,
    /// Each with whether a rejection deletes the key.
    value_filters: Vec<(Predicate<V>, bool)>,
    header_filters: Vec<HeaderFilter>,
    after: Vec<AfterHandler<K, V>>,
}

impl<K, V> Default for Options<K, V> {
    fn default() -> Self {
        Self {
            key_filters: Vec::new(),
            value_filters: Vec::new(),
            header_filters: Vec::new(),
            after: Vec::new(),
        }
    }
}

impl<K, V> Options<K, V> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Drops records whose key fails `keep`. Tombstones are not filtered: a
    /// delete always applies.
    pub fn key_filter(mut self, keep: impl Fn(&K) -> bool + Send + 'static) -> Self {
        self.key_filters.push(Box::new(keep));
        self
    }

    /// Drops records whose decoded value fails `keep`. With `delete_on_reject`
    /// a rejected value also removes what the cache holds under its key, so an
    /// item that stops matching leaves the cache.
    pub fn value_filter(
        mut self,
        keep: impl Fn(&V) -> bool + Send + 'static,
        delete_on_reject: bool,
    ) -> Self {
        self.value_filters.push((Box::new(keep), delete_on_reject));
        self
    }

    /// Adds a header filter.
    pub fn header_filter(mut self, f: HeaderFilter) -> Self {
        self.header_filters.push(f);
        self
    }

    /// Registers `h` for changes once the topic is loaded. Same and Filtered
    /// are not reported.
    pub fn after(
        mut self,
        h: impl FnMut(UpdateType, &K, &Arc<V>, Option<&Arc<V>>) + Send + 'static,
    ) -> Self {
        self.after.push(Box::new(h));
        self
    }
}

/// Keeps or drops a record by one of its headers, before its key or value is
/// decoded. Filters on one binding all have to pass. A filter runs on every
/// occurrence of its header and passes when the header is absent, except
/// [`HeaderFilter::exists`], which is the way to require one.
///
/// A tombstone is filtered too: a rejected header drops the delete, so a
/// consumer filtering out another tenant does not act on that tenant's
/// removals. A tombstone missing a required header still applies, because
/// producers do not stamp headers on a delete.
pub struct HeaderFilter {
    key: String,
    required: bool,
    pass: Predicate<[u8]>,
}

impl HeaderFilter {
    /// Keeps records whose header `key` equals one of `values`. `T` is any
    /// type a header decodes to: String, i32, i64, Uuid, an enum, ….
    pub fn equals<T: HeaderValue + PartialEq + Send + 'static>(
        key: &str,
        values: impl IntoIterator<Item = T>,
    ) -> Self {
        let values: Vec<T> = values.into_iter().collect();
        Self {
            key: key.into(),
            required: false,
            pass: Box::new(move |b| T::from_header(b).is_ok_and(|v| values.contains(&v))),
        }
    }

    /// Keeps records whose header `key` equals none of `values`.
    pub fn not_equals<T: HeaderValue + PartialEq + Send + 'static>(
        key: &str,
        values: impl IntoIterator<Item = T>,
    ) -> Self {
        let values: Vec<T> = values.into_iter().collect();
        Self {
            key: key.into(),
            required: false,
            pass: Box::new(move |b| !T::from_header(b).is_ok_and(|v| values.contains(&v))),
        }
    }

    /// Keeps only records that carry the header `key`.
    pub fn exists(key: &str) -> Self {
        Self {
            key: key.into(),
            required: true,
            pass: Box::new(|_| true),
        }
    }

    /// Keeps records whose header decodes to a value `keep` accepts. A
    /// MessagePack nil passes when `pass_on_null`; a value that does not
    /// decode, or a `keep` that panics, rejects.
    pub fn predicate<T: HeaderValue>(
        key: &str,
        keep: impl Fn(T) -> bool + Send + 'static,
        pass_on_null: bool,
    ) -> Self {
        Self {
            key: key.into(),
            required: false,
            pass: Box::new(move |b| {
                if wire::is_nil(b) {
                    return pass_on_null;
                }
                let Ok(v) = T::from_header(b) else {
                    return false;
                };
                catch_unwind(AssertUnwindSafe(|| keep(v))).unwrap_or(false)
            }),
        }
    }
}

fn header_value(h: &impl Header) -> &[u8] {
    h.value().unwrap_or(&[])
}

/// Every occurrence of a filtered header must pass, and every required header
/// must be present unless the record is a tombstone.
fn headers_pass(filters: &[HeaderFilter], r: &Record) -> bool {
    filters.iter().all(|f| {
        let mut seen = false;
        for h in r.headers() {
            if h.key() == f.key {
                seen = true;
                if !(f.pass)(header_value(h)) {
                    return false;
                }
            }
        }
        seen || !f.required || is_tombstone(r)
    })
}

fn is_tombstone(r: &Record) -> bool {
    r.value().is_none_or(Vec::is_empty)
}

/// A record as [`Meta`], without copying its headers.
struct RecordMeta<'a>(&'a Record);

impl Meta for RecordMeta<'_> {
    fn each_header(&self, f: &mut dyn FnMut(&str, &[u8])) {
        for h in self.0.headers() {
            f(h.key(), header_value(h));
        }
    }

    fn timestamp(&self) -> i64 {
        self.0.timestamp()
    }
}

struct CacheBinding<K, V> {
    cache: Arc<Cache<K, V>>,
    opts: Options<K, V>,
    seed: RandomState,
    on_error: Arc<dyn Fn(String) + Send + Sync>,
}

impl<K, V> CacheBinding<K, V>
where
    K: Decode + Eq + Hash + Clone + Send + Sync + 'static,
    V: Decode + Send + Sync + 'static,
{
    /// Identifies what a record writes into the cache: its value, and for an
    /// enriched type the headers and timestamp `enrich` reads. A re-publish
    /// that changes only a stamped header is then a change, not Same.
    fn fingerprint(&self, r: &Record, value: &[u8]) -> u64 {
        let mut h = self.seed.build_hasher();
        h.write(value);
        let en = V::ENRICHMENT;
        for key in en.headers {
            for rh in r.headers() {
                if rh.key() == *key {
                    // Length-prefixed, so no two header sets hash the same bytes.
                    h.write_usize(key.len());
                    h.write(key.as_bytes());
                    let v = header_value(rh);
                    h.write_usize(v.len());
                    h.write(v);
                }
            }
        }
        if en.timestamp {
            h.write_i64(r.timestamp());
        }
        h.finish() | 1
    }

    fn notify(&mut self, live: bool, u: UpdateType, k: &K, cur: &Arc<V>, old: Option<&Arc<V>>) {
        if !live || u == UpdateType::Same {
            return;
        }
        // One handler's panic is reported and does not stop the others, nor
        // the consumer: the cache already holds the change.
        for h in &mut self.opts.after {
            if catch_unwind(AssertUnwindSafe(|| h(u, k, cur, old))).is_err() {
                (self.on_error)(format!(
                    "{}: an after handler panicked on {u:?}",
                    self.cache.name()
                ));
            }
        }
    }

    fn remove(&mut self, live: bool, k: &K) {
        if let Some(old) = self.cache.remove(k) {
            self.notify(live, UpdateType::Delete, k, &old, Some(&old));
        }
    }
}

impl<K, V> Binding for CacheBinding<K, V>
where
    K: Decode + Eq + Hash + Clone + Send + Sync + 'static,
    V: Decode + Send + Sync + 'static,
{
    fn apply(&mut self, r: &Record, live: bool) {
        let at = || format!("{}[{}]@{}", r.topic(), r.partition(), r.offset());
        if !headers_pass(&self.opts.header_filters, r) {
            return;
        }
        let k = match wire::decode::<K>(r.key().map_or(&[][..], Vec::as_slice)) {
            Ok(k) => k,
            Err(e) => return (self.on_error)(format!("{}: key: {e}", at())),
        };
        let value = match r.value() {
            Some(v) if !v.is_empty() => v.as_slice(),
            _ => return self.remove(live, &k),
        };
        // A filter that panics rejects, as one that returns false does.
        if !self
            .opts
            .key_filters
            .iter()
            .all(|keep| catch_unwind(AssertUnwindSafe(|| keep(&k))).unwrap_or(false))
        {
            return;
        }
        // Prague producers re-publish unchanged state; skip the decode outright.
        let fp = self.fingerprint(r, value);
        if self.cache.fingerprint(&k) == Some(fp) {
            return;
        }
        let mut v = match wire::decode::<V>(value) {
            Ok(v) => v,
            Err(e) => return (self.on_error)(format!("{}: value: {e}", at())),
        };
        for (keep, delete_on_reject) in &self.opts.value_filters {
            if !catch_unwind(AssertUnwindSafe(|| keep(&v))).unwrap_or(false) {
                if *delete_on_reject {
                    self.remove(live, &k);
                }
                return;
            }
        }
        // After the value filters, which is Prague's order: a value filter sees
        // what the value carried, never what the message stamped over it.
        v.enrich(&RecordMeta(r));
        let (u, old) = self.cache.apply(k.clone(), v, fp, r.timestamp());
        if live && u != UpdateType::Same && !self.opts.after.is_empty() {
            let cur = self.cache.get(&k).expect("a value just applied is stored");
            self.notify(live, u, &k, &cur, old.as_ref());
        }
    }
}
