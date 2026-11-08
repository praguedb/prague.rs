//! The fixtures are written by Prague (.NET)'s CacheSerde from praguec's C#
//! emission of schemas/shop.prague; contracts::shop is praguec's Rust emission
//! of the same file.

use prague::Keyed;

// A decimal keeps the scale it was written with, as .NET's decimal does:
// MessagePack-CSharp writes 12.34m with scale 4 as "12.3400".

use crate::contracts::shop::{Offer, OfferKey, Product, SlimProduct, Status};

fn decode<T: prague::wire::Decode>(name: &str) -> T {
    crate::decode("interop", name)
}

#[test]
fn product_full() {
    assert_eq!(decode::<i64>("product-full.key"), 42);
    let p: Product = decode("product-full.value");
    assert_eq!(p.id, 42);
    assert_eq!(p.name, "Widget");
    assert_eq!(p.brand.as_deref(), Some("Acme"));
    assert_eq!(p.status, Status::ACTIVE);
    assert_eq!(p.r#override, Some(Status::RETIRED));
    assert_eq!(p.released, 1_767_225_600_000);
    assert_eq!(p.stock, Some(7));
    assert!(p.published);
    assert_eq!(
        p.discount.map(|d| d.to_string()).as_deref(),
        Some("12.3400")
    );
    let size = p.size.as_ref().expect("size");
    assert_eq!((size.width, size.height), (1.5, 2.25));
    assert_eq!(p.prices.len(), 2, "prices = {:?}", p.prices);
    assert_eq!(p.prices[0].currency, "EUR");
    assert_eq!(p.prices[0].amount.to_string(), "19.99");
    assert_eq!(p.prices[1].amount.to_string(), "-0.0001");
    assert_eq!(p.tags.len(), 2, "tags = {:?}", p.tags);
    assert_eq!(p.tags[1], "sale");
    assert_eq!(p.cache_key(), 42);
}

#[test]
fn product_sparse_has_no_presence() {
    let p: Product = decode("product-sparse.value");
    assert_eq!(p.id, 7);
    assert_eq!(p.name, "Bare");
    assert_eq!(p.brand, None);
    assert_eq!(p.r#override, None);
    assert_eq!(p.stock, None);
    assert_eq!(p.discount, None);
    assert_eq!(p.size, None);
}

#[test]
fn offer_composite_key() {
    let k: OfferKey = decode("offer.key");
    assert_eq!(
        k,
        OfferKey {
            product_id: 42,
            offer_id: 3
        }
    );
    let o: Offer = decode("offer.value");
    assert_eq!(o.cache_key(), k);
    assert_eq!(*o.offer_id(), 3);
    assert_eq!(o.price.amount.to_string(), "17.5");
    assert_eq!(o.updated_at, 99);
}

/// A view decodes a table's bytes: same tags, fewer members.
#[test]
fn view_reads_table_bytes() {
    let s: SlimProduct = decode("product-full.value");
    assert_eq!(s.id, 42);
    assert_eq!(s.name, "Widget");
    assert_eq!(s.status, Status::ACTIVE);
    assert_eq!(s.cache_key(), 42);
}

/// The writer (schemas/shop-writer.prague) is ahead of this reader -- Product
/// tags 16 and 17, Price and Dimensions tag 3 -- and behind it, filling tags
/// 4..6 that the reader reserves. Every one of those slots must be skipped, at
/// the top level and inside nested records and list elements, and nothing
/// after a skipped slot may shift.
#[test]
fn reader_skips_what_the_writer_knows_and_it_does_not() {
    let p: Product = decode("product-writer.value");
    // Before the reserved slots.
    assert_eq!(p.id, 43);
    assert_eq!(p.name, "Gadget");
    assert_eq!(p.brand.as_deref(), Some("Acme"));
    // Tag 7 comes straight after the three reserved slots the writer filled.
    assert_eq!(p.status, Status::ACTIVE);
    assert_eq!(p.released, 1_767_225_600_000);
    assert_eq!(p.stock, Some(3));
    assert_eq!(p.discount.map(|d| d.to_string()).as_deref(), Some("0.5"));
    // A struct with an unknown trailing member.
    let size = p.size.as_ref().expect("size");
    assert_eq!((size.width, size.height), (1.0, 2.0));
    // A list of records with an unknown member.
    assert_eq!(p.prices.len(), 1, "prices = {:?}", p.prices);
    assert_eq!(p.prices[0].currency, "EUR");
    assert_eq!(p.prices[0].amount.to_string(), "9.99");
    // Tag 15 is the last the reader knows; 16 and 17 follow it.
    assert_eq!(p.tags, ["writer"]);
    assert!(p.published);
}
