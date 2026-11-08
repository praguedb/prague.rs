use std::sync::Arc;

use prague::{
    Cache, Keyed, ManyIndex, RangeIndex, UniqueIndex, UpdateType, join_many, join_one, or,
};

#[derive(Debug, Clone, Default)]
struct Item {
    id: i32,
    brand: &'static str,
    date: i64,
    code: Option<&'static str>,
}

impl Keyed for Item {
    type Key = i32;
    fn cache_key(&self) -> i32 {
        self.id
    }
}

struct Fixture {
    c: Arc<Cache<i32, Item>>,
    brand: ManyIndex<i32, Item, &'static str>,
    date: RangeIndex<i32, Item, i64>,
    code: UniqueIndex<i32, Item, &'static str>,
}

fn fixture() -> Fixture {
    let c = Cache::new("items");
    // Data before indexes: registration backfills.
    c.upsert(
        Item {
            id: 1,
            brand: "a",
            date: 10,
            code: Some("x1"),
        },
        0,
    );
    let brand = c.add_many(|i: &Item| i.brand);
    let date = c.add_range(|i: &Item| i.date);
    let code = c.add_unique_opt(|i: &Item| i.code);
    for it in [
        Item {
            id: 2,
            brand: "a",
            date: 20,
            code: None,
        },
        Item {
            id: 3,
            brand: "b",
            date: 30,
            code: Some("x3"),
        },
        Item {
            id: 4,
            brand: "b",
            date: 40,
            code: None,
        },
        Item {
            id: 5,
            brand: "c",
            date: 50,
            code: None,
        },
    ] {
        c.upsert(it, 0);
    }
    Fixture {
        c,
        brand,
        date,
        code,
    }
}

fn ids(items: &[Arc<Item>]) -> Vec<i32> {
    let mut out: Vec<i32> = items.iter().map(|i| i.id).collect();
    out.sort();
    out
}

#[test]
fn query_lanes() {
    let Fixture {
        c,
        brand,
        date,
        code,
    } = fixture();
    let cases: Vec<(&str, prague::Query<'_, i32, Item>, Vec<i32>)> = vec![
        ("all", c.query(), vec![1, 2, 3, 4, 5]),
        ("many eq", c.query().with(brand.eq("b")), vec![3, 4]),
        (
            "many any_of",
            c.query().with(brand.any_of(["a", "c"])),
            vec![1, 2, 5],
        ),
        (
            "range [20,40)",
            c.query().with(date.gte(20).lt(40)),
            vec![2, 3],
        ),
        (
            "range (20,40]",
            c.query().with(date.gt(20).lte(40)),
            vec![3, 4],
        ),
        ("range open high", c.query().with(date.gt(30)), vec![4, 5]),
        ("range inverted", c.query().with(date.gt(40).lt(20)), vec![]),
        (
            "range point open",
            c.query().with(date.gt(30).lt(30)),
            vec![],
        ),
        (
            "range point closed",
            c.query().with(date.gte(30).lte(30)),
            vec![3],
        ),
        (
            "intersect",
            c.query().with(brand.eq("b")).with(date.lt(35)),
            vec![3],
        ),
        (
            "empty lane",
            c.query().with(brand.eq("zzz")).with(date.gte(0)),
            vec![],
        ),
        ("unique", c.query().with(code.eq("x3")), vec![3]),
        (
            "or",
            c.query().with(or([brand.eq("c"), code.eq("x1")])),
            vec![1, 5],
        ),
        (
            "or overlapping",
            c.query().with(or([brand.eq("a"), date.lte(20).into()])),
            vec![1, 2],
        ),
        (
            "filter",
            c.query()
                .with(brand.any_of(["a", "b"]))
                .filter(|i| i.id % 2 == 0),
            vec![2, 4],
        ),
    ];
    for (name, q, want) in cases {
        let r = q.execute();
        assert_eq!(ids(&r.items), want, "{name}");
        assert_eq!(r.total_count, want.len(), "{name} total");
    }
}

#[test]
fn sort_page_count() {
    let Fixture { c, date, .. } = fixture();
    let r = c
        .query()
        .with(date.gte(0))
        .sort(|a, b| b.date.cmp(&a.date))
        .page(1, 2)
        .execute();
    assert_eq!(r.items.iter().map(|i| i.id).collect::<Vec<_>>(), [4, 3]);
    assert_eq!(r.total_count, 5);
    assert_eq!(c.query().with(date.lt(30)).count(), 2);
    assert_eq!(
        c.query()
            .sort(|a, b| b.date.cmp(&a.date))
            .first()
            .unwrap()
            .id,
        5
    );
    assert_eq!(date.min(), Some(10));
    assert_eq!(date.max(), Some(50));
    let unsorted = c.query().page(4, 10).execute();
    assert_eq!((unsorted.items.len(), unsorted.total_count), (1, 5));
    let past = c.query().sort(|a, b| a.id.cmp(&b.id)).page(9, 1).execute();
    assert_eq!((past.items.len(), past.total_count), (0, 5));
}

#[test]
fn updates_move_index_entries() {
    let Fixture {
        c,
        brand,
        date,
        code,
    } = fixture();
    assert_eq!(
        c.upsert(
            Item {
                id: 3,
                brand: "a",
                date: 5,
                code: None
            },
            0
        ),
        UpdateType::Update
    );
    assert_eq!(ids(&c.query().with(brand.eq("b")).execute().items), [4]);
    assert_eq!(ids(&c.query().with(date.lt(10)).execute().items), [3]);
    assert!(
        code.get(&"x3").is_none(),
        "unique value survived its removal"
    );
    assert!(c.remove(&4).is_some());
    assert_eq!(brand.count(&"b"), 0);
    assert_eq!(c.len(), 4);
    assert!(c.remove(&4).is_none());
}

#[test]
fn fingerprint_suppresses_same() {
    let c: Arc<Cache<i32, Item>> = Cache::new("x");
    assert_eq!(
        c.apply(
            1,
            Item {
                id: 1,
                ..Default::default()
            },
            77,
            1
        )
        .0,
        UpdateType::Add
    );
    assert_eq!(
        c.apply(
            1,
            Item {
                id: 1,
                ..Default::default()
            },
            77,
            2
        )
        .0,
        UpdateType::Same
    );
    assert_eq!(c.last_updated(&1), Some(1), "Same must not touch the entry");
    let (u, old) = c.apply(
        1,
        Item {
            id: 1,
            brand: "n",
            ..Default::default()
        },
        78,
        3,
    );
    assert_eq!(u, UpdateType::Update);
    assert_eq!(old.unwrap().brand, "");
    assert_eq!(c.fingerprint(&1), Some(78));
}

#[derive(Debug)]
struct Offer {
    id: i32,
    product_id: i32,
}

#[test]
fn joins() {
    let Fixture { c: products, .. } = fixture();
    let offers = Cache::with_key("offers", |o: &Offer| o.id);
    let by_product = offers.add_many(|o: &Offer| o.product_id);
    for (id, product_id) in [(10, 1), (11, 1), (12, 3), (13, 99)] {
        offers.upsert(Offer { id, product_id }, 0);
    }
    let all = offers.query().sort(|a, b| a.id.cmp(&b.id)).execute().items;
    let one = join_one(all.clone(), &products, |o| Some(o.product_id), false);
    assert_eq!(one.len(), 4);
    assert_eq!(one[0].right.as_ref().unwrap().id, 1);
    assert!(one[3].right.is_none());
    assert_eq!(
        join_one(all, &products, |o| Some(o.product_id), true).len(),
        3
    );

    let ps = products
        .query()
        .sort(|a, b| a.id.cmp(&b.id))
        .execute()
        .items;
    let many = join_many(ps, &by_product, |p| p.id, true);
    assert_eq!(many.len(), 2);
    assert_eq!(
        (many[0].left.id, many[0].right.len(), many[1].left.id),
        (1, 2, 3)
    );
}

#[test]
#[should_panic(expected = "another cache")]
fn a_lane_from_another_cache_is_refused() {
    let a = fixture();
    let b = fixture();
    let _ = a.c.query().with(b.brand.eq("a"));
}

#[test]
fn readers_run_beside_the_writer() {
    let Fixture { c, brand, .. } = fixture();
    let writer = {
        let c = c.clone();
        std::thread::spawn(move || {
            for n in 0..2000 {
                c.upsert(
                    Item {
                        id: 100 + n % 50,
                        brand: if n % 2 == 0 { "a" } else { "b" },
                        ..Default::default()
                    },
                    0,
                );
            }
        })
    };
    for _ in 0..200 {
        // A snapshot: every value a lane yields matches it.
        let r = c.query().with(brand.eq("a")).execute();
        assert!(r.items.iter().all(|i| i.brand == "a"));
    }
    writer.join().unwrap();
    assert_eq!(c.len(), 55);
}
