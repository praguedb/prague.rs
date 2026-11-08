//! The consume path against Confluent's MockConsumer: load, live changes,
//! tombstones, filters, enrichment and the loaded signal.
#![cfg(feature = "kafka")]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use confluent_kafka::common::header::{RecordHeader, RecordHeaders};
use confluent_kafka::common::{PartitionInfo, TopicPartition};
use confluent_kafka::consumer::{ConsumerRecord, ConsumerRecordOptionsBuilder, MockConsumer};
use prague::kafka::{Config, Consumer, HeaderFilter, Options};
use prague::wire::{self, Decode, Enrichment, Meta, Reader};
use prague::{Cache, Keyed, UpdateType};

const TOPIC: &str = "Cache.7.Item";

/// `[id, name]`, with a region from the `x-region` header and the message
/// timestamp, as a praguec-emitted enriched table would be.
#[derive(Debug, Clone, Default, PartialEq)]
struct Item {
    id: i64,
    name: String,
    region: String,
    stamped: i64,
}

impl Keyed for Item {
    type Key = i64;
    fn cache_key(&self) -> i64 {
        self.id
    }
}

impl Decode for Item {
    const ENRICHMENT: Enrichment = Enrichment {
        headers: &["x-region"],
        timestamp: true,
    };

    fn decode(r: &mut Reader<'_>) -> Result<Self, wire::Error> {
        let mut x = Self::default();
        let n = r.read_array_len()?;
        for i in 0..n {
            match i {
                0 => x.id = Decode::decode(r)?,
                1 => x.name = Decode::decode(r)?,
                _ => r.skip()?,
            }
        }
        Ok(x)
    }

    fn enrich(&mut self, m: &dyn Meta) {
        wire::header(m, "x-region", &mut self.region);
        self.stamped = m.timestamp();
    }
}

fn value(id: u8, name: &str) -> Vec<u8> {
    assert!(id < 128 && name.len() < 32);
    let mut b = vec![0x92, id, 0xa0 | name.len() as u8];
    b.extend_from_slice(name.as_bytes());
    b
}

struct Msg {
    key: u8,
    value: Option<Vec<u8>>,
    headers: Vec<(&'static str, &'static [u8])>,
    ts: i64,
}

fn put(key: u8, name: &str) -> Msg {
    Msg {
        key,
        value: Some(value(key, name)),
        headers: vec![("x-region", b"eu")],
        ts: 1,
    }
}

fn tombstone(key: u8) -> Msg {
    Msg {
        key,
        value: None,
        headers: vec![],
        ts: 1,
    }
}

fn record(offset: i64, m: Msg) -> ConsumerRecord<Vec<u8>, Vec<u8>> {
    let headers = RecordHeaders::with_header_iter(
        m.headers
            .into_iter()
            .map(|(k, v)| RecordHeader::new(k.to_string(), Some(v.to_vec()))),
    );
    let opts = ConsumerRecordOptionsBuilder::new()
        .set_topic(TOPIC)
        .set_partition(0)
        .set_offset(offset)
        .set_timestamp(m.ts)
        .set_key(Some(vec![m.key]))
        .set_value(m.value)
        .set_headers(headers)
        .build()
        .unwrap();
    ConsumerRecord::with_options(opts)
}

fn tp() -> TopicPartition {
    TopicPartition::new(TOPIC, 0)
}

/// A mock with the topic's one partition, which ends at `end`. Each batch is
/// added on its own poll, in order, numbered from offset 0.
fn mock(end: i64, batches: Vec<Vec<Msg>>) -> MockConsumer<Vec<u8>, Vec<u8>> {
    let mut m = MockConsumer::new("earliest").unwrap();
    m.update_partitions(
        TOPIC,
        vec![PartitionInfo::new(TOPIC.into(), 0, None, vec![], vec![])],
    )
    .unwrap();
    m.update_beginning_offsets(HashMap::from([(tp(), 0)]));
    m.update_end_offsets(HashMap::from([(tp(), end)]));
    let mut offset = 0;
    for batch in batches {
        let recs: Vec<_> = batch
            .into_iter()
            .map(|msg| {
                offset += 1;
                record(offset - 1, msg)
            })
            .collect();
        m.schedule_poll_task(Box::new(move |c: &mut MockConsumer<Vec<u8>, Vec<u8>>| {
            for r in recs {
                c.add_record(r).unwrap();
            }
        }));
    }
    m
}

fn consumer(errors: &Arc<Mutex<Vec<String>>>) -> Consumer {
    let errors = errors.clone();
    Consumer::new(Config {
        vars: HashMap::from([("brandId".into(), "7".into())]),
        on_error: Some(Arc::new(move |e| errors.lock().unwrap().push(e))),
        ..Default::default()
    })
}

type Events = Arc<Mutex<Vec<(UpdateType, i64, Option<String>)>>>;

/// Runs until `done` holds, or fails after two seconds.
async fn run_until(c: Consumer, m: MockConsumer<Vec<u8>, Vec<u8>>, done: impl Fn() -> bool + Send) {
    let shutdown = async move {
        while !done() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(2), c.run_with(m, shutdown))
        .await
        .expect("the consumer did not reach the expected state")
        .unwrap();
}

#[tokio::test]
async fn loads_then_goes_live() {
    let errors = Arc::new(Mutex::new(Vec::new()));
    let events: Events = Arc::default();
    let cache: Arc<Cache<i64, Item>> = Cache::new("items");
    let mut c = consumer(&errors);
    let ev = events.clone();
    c.register(
        "Cache.[v:brandId].Item",
        cache.clone(),
        Options::new().after(move |u, k, _cur, old: Option<&Arc<Item>>| {
            ev.lock()
                .unwrap()
                .push((u, *k, old.map(|o| o.name.clone())))
        }),
    )
    .unwrap();
    let loaded = c.loaded();
    assert!(!loaded.is_loaded());
    assert_eq!(loaded.pending(), [TOPIC]);

    // The replay: offsets 0..4, ending at 4.
    let replay = vec![put(1, "a"), put(2, "b"), put(1, "a2"), tombstone(2)];
    let live = vec![
        put(1, "a3"), // Update
        put(1, "a3"), // Same bytes, same headers: skipped undecoded
        put(3, "c"),  // Add
        Msg {
            headers: vec![("x-region", b"us")],
            ..put(3, "c")
        }, // Same bytes, new header: Update
        tombstone(3), // Delete
        tombstone(9), // a delete of nothing: no event
    ];
    let m = mock(4, vec![replay, live]);

    let ev = events.clone();
    run_until(c, m, move || ev.lock().unwrap().len() >= 4).await;

    assert!(loaded.is_loaded());
    assert!(loaded.pending().is_empty());
    assert_eq!(
        *events.lock().unwrap(),
        [
            (UpdateType::Update, 1, Some("a2".into())),
            (UpdateType::Add, 3, None),
            (UpdateType::Update, 3, Some("c".into())),
            (UpdateType::Delete, 3, Some("c".into())),
        ],
        "the replay fires nothing; live changes fire once each"
    );
    let one = cache.get(&1).unwrap();
    assert_eq!(
        (one.name.as_str(), one.region.as_str(), one.stamped),
        ("a3", "eu", 1)
    );
    assert!(cache.get(&2).is_none() && cache.get(&3).is_none());
    assert_eq!(cache.len(), 1);
    assert!(
        errors.lock().unwrap().is_empty(),
        "{:?}",
        errors.lock().unwrap()
    );
}

#[tokio::test]
async fn filters_and_bad_messages() {
    let errors = Arc::new(Mutex::new(Vec::new()));
    let cache: Arc<Cache<i64, Item>> = Cache::new("items");
    let mut c = consumer(&errors);
    c.register(
        TOPIC,
        cache.clone(),
        Options::new()
            .header_filter(HeaderFilter::exists("x-region"))
            .header_filter(HeaderFilter::equals("x-region", ["eu".to_string()]))
            .key_filter(|k| *k != 13)
            .key_filter(|k| {
                if *k == 66 {
                    panic!("a filter bug")
                } else {
                    true
                }
            })
            .value_filter(|v: &Item| !v.name.starts_with("drop"), true),
    )
    .unwrap();

    let batch = vec![
        put(1, "a"),
        put(2, "b"),
        Msg {
            headers: vec![("x-region", b"us")],
            ..put(4, "us")
        }, // header rejects
        Msg {
            headers: vec![],
            ..put(5, "none")
        }, // a required header missing
        put(13, "k"),      // key filter rejects
        put(66, "p"),      // a panicking filter rejects
        put(2, "drop me"), // value filter rejects and deletes
        Msg {
            value: Some(vec![0xc1]),
            ..put(7, "x")
        }, // undecodable: reported, skipped
        tombstone(1),      // no header, still deletes
    ];
    let n = batch.len() as i64;
    let loaded = c.loaded();
    let probe = loaded.clone();
    run_until(c, mock(n, vec![batch]), move || probe.is_loaded()).await;

    assert!(loaded.is_loaded());
    assert_eq!(cache.len(), 0, "{:?}", cache.query().execute().items);
    let errors = errors.lock().unwrap();
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].contains("@7: value"), "{errors:?}");
}

#[tokio::test]
async fn a_panicking_handler_is_reported_and_the_rest_still_run() {
    let errors = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::new(Mutex::new(0));
    let cache: Arc<Cache<i64, Item>> = Cache::new("items");
    let mut c = consumer(&errors);
    let s = seen.clone();
    c.register(
        TOPIC,
        cache.clone(),
        Options::new()
            .after(|_, _, _, _| panic!("handler bug"))
            .after(move |_, _, _, _| *s.lock().unwrap() += 1),
    )
    .unwrap();
    let s = seen.clone();
    run_until(
        c,
        mock(0, vec![vec![put(1, "a"), put(2, "b")]]),
        move || *s.lock().unwrap() >= 2,
    )
    .await;
    assert_eq!(cache.len(), 2);
    assert_eq!(
        errors.lock().unwrap().len(),
        2,
        "{:?}",
        errors.lock().unwrap()
    );
}

#[tokio::test]
async fn a_stalled_load_names_what_it_waits_on() {
    let errors = Arc::new(Mutex::new(Vec::new()));
    let cache: Arc<Cache<i64, Item>> = Cache::new("items");
    let mut c = consumer(&errors);
    c.register(TOPIC, cache, Options::new()).unwrap();
    let mut loaded = c.loaded();
    // The partition ends at 5 and nothing arrives.
    let run = tokio::spawn(c.run_with(
        mock(5, vec![]),
        tokio::time::sleep(Duration::from_millis(200)),
    ));
    let err = loaded
        .wait_timeout(Duration::from_millis(50))
        .await
        .unwrap_err();
    assert!(err.to_string().contains(TOPIC), "{err}");
    run.await.unwrap().unwrap();
    assert!(!loaded.wait().await, "the consumer stopped before loading");
}

#[tokio::test]
async fn a_missing_topic_fails_fast() {
    let errors = Arc::new(Mutex::new(Vec::new()));
    let mut c = consumer(&errors);
    c.register(
        "Cache.Elsewhere",
        Cache::<i64, Item>::new("x"),
        Options::new(),
    )
    .unwrap();
    let err = c
        .run_with(mock(0, vec![]), std::future::pending())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("does not exist"), "{err}");
}
