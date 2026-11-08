# prague.rs

A read-only Rust consumer for [Prague](https://github.com/nanov/prague.net) caches, and the Rust
sibling of [prague.go](https://github.com/praguedb/prague.go). It loads the compacted Kafka topics a
Prague (.NET) producer writes into typed in-memory caches, keeps them live, and lets you query them
through secondary indexes. It has no producer.

Contracts are not hand-written. The same `.prague` schema that praguec compiles to C# for the
producer compiles to Rust for the consumer:

```
shop.prague ──praguec──▶ C# types  ──Prague.Kafka (CacheSerde)──▶ topic
            └─praguec --lang rust──▶ Rust types ◀──prague::kafka─────┘
```

Kafka is [`confluent-kafka`](https://crates.io/crates/confluent-kafka) 1.0, Confluent's native Rust
client. The MessagePack reader is this crate's own (`prague::wire`), so the decode path has no
dependency beyond `rust_decimal`.

## Generate the contracts

```bash
# from a praguec checkout
go run ./cmd/praguec generate --lang rust -o src/contracts shop.prague
```

Each `.prague` file becomes one module file (`shop.rs`), and a file reaches what it imports through
`super::<module>`, so declare them side by side:

```rust
mod contracts {
    #[rustfmt::skip]
    pub mod shop;
}
```

What a schema becomes:

- A table, view, record or struct becomes a struct with `pub` fields, and a `Decode` that reads the
  MessagePack array by index (tag − 1) and skips reserved or unknown slots. Presence is `Option`, a
  list is `Vec`. A type derives `Eq` and `Hash` unless it holds a `double` somewhere, so it can key
  a cache or an index.
- A table gets `Keyed`. A composite key becomes a hashable `<Table>Key` struct, which is also what
  the Kafka message key decodes into. Views key by their source table's key type.
- An enum is an open newtype over `i32` with one constant per arm (`Status::ACTIVE`), so a value a
  newer producer adds, or a flags combination, is kept rather than rejected.
- A `union X of Kind` is a Rust enum with a variant per arm, decoded from `[kind, payload]`. A kind
  the schema has no arm for is `X::Unknown(kind, wire::Raw)`, with the payload kept as written.
- `decimal(N)` is `wire::Decimal` (rust_decimal, which keeps the scale .NET wrote), `uuid` is
  `wire::Uuid`, `timestamp` is `wire::Timestamp` (UTC ticks, from `DateTime.ToBinary()`) and `date`
  is `wire::Date` (the `DateOnly` day number).
- `[rust: …]` annotations become attributes: `[rust: derive(serde::Serialize)]` on a table is
  `#[derive(serde::Serialize)]` on its struct.

## Use

```rust
use prague::kafka::{Config, Consumer, HeaderFilter, Options};
use prague::{Cache, join_many};

let products = Cache::<i64, shop::Product>::new("products");
let by_status = products.add_many(|p: &shop::Product| p.status);
let released = products.add_range(|p: &shop::Product| p.released);

let offers = Cache::<shop::OfferKey, shop::Offer>::new("offers");
let offers_by_product = offers.add_many(|o: &shop::Offer| o.product_id);

let mut c = Consumer::new(Config {
    brokers: vec!["kafka:9092".into()],
    vars: [("brandId".into(), "7".into())].into(),
    ..Default::default()
});
c.register(
    "Cache.[v:brandId].Product",
    products.clone(),
    Options::new()
        .header_filter(HeaderFilter::equals("x-region", ["eu".to_string(), "us".to_string()]))
        .value_filter(|p: &shop::Product| p.published, true)
        .after(|u, id, cur, old| { /* live changes only */ }),
)?;
c.register("Cache.[v:brandId].Offer", offers.clone(), Options::new())?;

let mut loaded = c.loaded();
tokio::spawn(c.run(shutdown));
loaded.wait_timeout(Duration::from_secs(60)).await?; // names the topics still loading

let p = products.get(&42); // Option<Arc<Product>>

let page = products
    .query()
    .with(by_status.eq(shop::Status::ACTIVE)) // lanes AND; prague::or unions
    .with(released.gte(from).lt(to))
    .filter(|p| p.stock.is_some()) // residual filter
    .sort(|a, b| b.released.cmp(&a.released))
    .page(0, 20)
    .execute(); // page.items, page.total_count

let with_offers = join_many(page.items, &offers_by_product, |p| p.id, false);
```

Indexes: `add_unique`, `add_many`, `add_range`, plus `…_opt` variants whose extractor returns an
`Option` for nullable fields. Indexes can be added before or after data arrives. A query drives
from the lane with the fewest candidates and checks the rest against each value. If any lane is
empty, the query returns before fetching anything. A lane from another cache's index is refused.

## Semantics, as in Prague (.NET)

- Message key = MessagePack of the cache key, value = MessagePack of the item, empty value =
  tombstone. Every partition is assigned and read from the start, with no consumer group and no
  committed offsets, so a restart is a full reload.
- A re-published, byte-identical value is `Same`. It is detected from a fingerprint of the value
  bytes, before decoding, and is neither stored nor reported.
- Header filters run before the key is decoded, and all of them must pass: `HeaderFilter::equals`
  (any of several values), `not_equals`, `exists` and `predicate` (with `pass_on_null` for a
  MessagePack nil). A filter checks every occurrence of its header and passes when the header is
  absent, except `exists`. A tombstone missing a required header still applies, so a delete cannot
  pin a key.
- Key filters, then value filters, run next. A value filter can delete the key on rejection. A
  filter that panics rejects; the consumer keeps going.
- Members annotated `[header]`, `[header("name")]` or `[from_timestamp]` in the schema are written
  from the message after value filters run, the way Prague's `HeadersSerDe` writes them: raw
  little-endian or MessagePack integers, UTF-8 strings, the `Guid.ToByteArray` layout, MessagePack
  for everything else. The headers and timestamp a type reads are part of its `Same` fingerprint,
  so a changed header is an `Update`.
- After-handlers fire only once the topic is loaded. A handler that panics is reported through
  `Config::on_error` and the others still run.
- A topic counts as loaded when the consumer's position on every partition has reached the end
  offset seen at startup. Position, not the last record seen: transaction markers and compacted-away
  tail offsets advance it without a record to see. `Loaded::pending()` names the topics still
  loading.
- A message whose key or value does not decode is reported through `Config::on_error` and skipped.
- Topic templates resolve `[v:name]` from `Config::vars` and `[e:NAME]` from the environment. A
  missing topic makes `run` fail rather than wait.

Differences: one writer and many readers behind an `RwLock` per cache, so a query sees a
consistent snapshot. That is stronger than Prague's lock-free reads and slower on hot read paths.
Values are `Arc`s. There are no prepared or frozen queries or global last-update index yet. `Same`
compares bytes, not Prague's structural equality, so a re-encoding that changes bytes without
changing data counts as `Update`.

## Proof of wire compatibility

The schemas and fixtures are prague.go's, so both consumers are held to the same bytes.
`schemas/shop.prague` is the reader. `schemas/shop-writer.prague` is a producer both ahead of it
(Product tags 16–17, a third member on `Price` and `Dimensions`) and behind it (data in tags 4–6,
which the reader has reserved). The fixtures in `testdata/` were written by Prague's
`CacheSerde<T>` from the C# types praguec emits. `tests/interop/contracts/` is praguec's Rust
emission of every schema, unedited.

- `tests/interop`: the generated Rust decodes those bytes. That covers nil presence, decimals with
  their scale, composite keys, a view read from table bytes, the writer's extra and reserved slots
  skipped at every level, every `SettingKind` of the tenant schema's union and an unknown kind kept
  raw, and the catalog and show-feed schemas with timestamps, dates and nested records.
- `tests/wire.rs`: the MessagePack reader against every marker family, written from the spec, and
  hostile input (lengths past the end, huge counts, deep nesting) erroring instead of panicking.
- `tests/kafka.rs`: the consume path against Confluent's `MockConsumer`: load then live, `Same`
  suppression, header-driven updates, tombstones, every filter, panicking handlers, a stalled load
  and a missing topic.
- `tests/cache.rs`: indexes, queries, joins, and readers running beside the writer.

```bash
cargo test                        # with Kafka
cargo test --no-default-features  # the cache and decoders alone
```
