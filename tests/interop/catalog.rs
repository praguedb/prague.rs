//! The fixtures were written by the producer's own C# contract types through
//! Prague's CacheSerde; contracts::catalog is praguec's Rust emission of
//! schemas/catalog.prague, which describes the same wire.

use prague::Keyed;
use prague::wire::{Date, Timestamp, Uuid};

use crate::contracts::catalog::{
    ActInfo, ActMappingInfo, ActType, AttributeField, AttributeKind, CountryInfo, EditionInfo,
    EditionMappingInfo, Gender, GenreInfo, GenreMappingInfo, MappingKey, MappingKind,
    MappingMethod, MappingRequest, PerformerInfo, SaleWindow, SetInfo, SetKey, SetKind, ShowInfo,
    ShowMappingInfo, SlotRole, SourceInfo, SourceTypes, TicketSlot, TicketTypeInfo, TierFamilyInfo,
    TourInfo, VenueInfo,
};

fn decode<T: prague::wire::Decode>(name: &str) -> T {
    crate::decode("catalog", name)
}

fn s(v: &str) -> Option<String> {
    Some(v.to_owned())
}

fn tour() -> Uuid {
    Uuid::parse("5e1f0c2a-7b3d-4c8e-9f10-a1b2c3d4e5f6").unwrap()
}
fn edition() -> Uuid {
    Uuid::parse("9a8b7c6d-5e4f-4a3b-8c2d-1e0f9a8b7c6d").unwrap()
}

// The producer writes these as DateTime.ToBinary() with the Utc kind.
fn created() -> Timestamp {
    Timestamp::from_civil(2026, 1, 1, 0, 0, 0)
}
fn updated() -> Timestamp {
    Timestamp::from_civil(2026, 1, 2, 0, 0, 0)
}
fn starting() -> Timestamp {
    Timestamp::from_civil(2026, 7, 4, 20, 30, 0)
}

const ACT1: i64 = 7301234567890123;
const ACT2: i64 = 7301234567890456;
const PERFORMER: i64 = 7309876543210001;
const VENUE: i64 = 7305555555555501;
const SHOW: i64 = 7312345678901234;

#[test]
fn knowledge_base() {
    assert_eq!(decode::<i32>("genre.key"), 3, "genre.key");
    assert_eq!(
        decode::<GenreInfo>("genre.value"),
        GenreInfo {
            id: 3,
            slug: "indie-rock".into(),
            name: "Indie Rock".into(),
            version: 2,
            created_at: created(),
            updated_at: updated(),
            ..Default::default()
        },
        "genre"
    );
    assert_eq!(
        decode::<SourceInfo>("source.value"),
        SourceInfo {
            id: 1,
            name: "Primary Source".into(),
            version: 1,
            created_at: created(),
            updated_at: updated(),
            types: SourceTypes::PRICING | SourceTypes::STATE,
            ..Default::default()
        },
        "source"
    );
    assert_eq!(
        decode::<CountryInfo>("country.value"),
        CountryInfo {
            id: 44,
            name: "Portugal".into(),
            version: 1,
            created_at: created(),
            updated_at: updated(),
            country_code: s("PRT"),
            ..Default::default()
        },
        "country"
    );
    assert_eq!(
        decode::<CountryInfo>("country-grouping.value"),
        CountryInfo {
            id: 1,
            name: "International".into(),
            is_archived: true,
            version: 3,
            created_at: created(),
            updated_at: updated(),
            ..Default::default()
        },
        "country-grouping"
    );
    assert_eq!(decode::<Uuid>("tour.key"), tour(), "tour.key");
    assert_eq!(
        decode::<TourInfo>("tour.value"),
        TourInfo {
            id: tour(),
            genre_id: 3,
            country_id: 44,
            name: "Summer Arena Tour".into(),
            version: 1,
            created_at: created(),
            updated_at: updated(),
            ..Default::default()
        },
        "tour"
    );
    assert_eq!(
        decode::<ActInfo>("act-full.value"),
        ActInfo {
            id: ACT1,
            genre_id: 3,
            name: "The Nightingales".into(),
            short_name: s("Nightingales"),
            abbreviation: s("NGT"),
            country_id: Some(44),
            gender: Gender::MIXED,
            age_group: s("U21"),
            act_type: ActType::BAND,
            is_virtual: true,
            version: 4,
            created_at: created(),
            updated_at: updated(),
            ..Default::default()
        },
        "act-full"
    );
    assert_eq!(
        decode::<ActInfo>("act-sparse.value"),
        ActInfo {
            id: ACT2,
            genre_id: 3,
            name: "The Larks".into(),
            act_type: ActType::SOLO,
            version: 1,
            created_at: created(),
            updated_at: updated(),
            ..Default::default()
        },
        "act-sparse"
    );
    assert_eq!(
        decode::<PerformerInfo>("performer.value"),
        PerformerInfo {
            id: PERFORMER,
            genre_id: 3,
            name: "Rosa Almeida".into(),
            short_name: s("R. Almeida"),
            country_id: Some(44),
            gender: Gender::FEMALE,
            date_of_birth: Some(Date::new(1994, 5, 17)),
            role: s("vocals"),
            version: 1,
            created_at: created(),
            updated_at: updated(),
            ..Default::default()
        },
        "performer"
    );
    assert_eq!(
        decode::<VenueInfo>("venue.value"),
        VenueInfo {
            id: VENUE,
            name: "Riverside Hall".into(),
            city: s("Porto"),
            country_id: Some(44),
            capacity: Some(12000),
            version: 1,
            created_at: created(),
            updated_at: updated(),
            ..Default::default()
        },
        "venue"
    );
    assert_eq!(
        decode::<EditionInfo>("edition.value"),
        EditionInfo {
            id: edition(),
            tour_id: tour(),
            name: "Summer Arena Tour 2026".into(),
            year: s("2026"),
            start_date: Some(Date::new(2026, 6, 1)),
            version: 1,
            created_at: created(),
            updated_at: updated(),
            ..Default::default()
        },
        "edition"
    );
    let got: ShowInfo = decode("show.value");
    assert_eq!(
        got,
        ShowInfo {
            id: SHOW,
            genre_id: 3,
            country_id: 44,
            tour_id: tour(),
            edition_id: Some(edition()),
            venue_id: Some(VENUE),
            headliner_id: ACT1,
            opener_id: ACT2,
            scheduled_start: Some(starting()),
            version: 2,
            created_at: created(),
            updated_at: updated(),
            ..Default::default()
        },
        "show"
    );
    assert_eq!(got.cache_key(), SHOW, "show cache key");
}

#[test]
fn mappings() {
    let key: MappingKey = decode("act-mapping.key");
    assert_eq!(
        key,
        MappingKey {
            source_id: 1,
            provider_id: "src:act:4698".into()
        },
        "act-mapping.key"
    );
    let act: ActMappingInfo = decode("act-mapping.value");
    assert_eq!(
        act,
        ActMappingInfo {
            key: key.clone(),
            act_id: ACT1,
            version: 2,
            created_at: created(),
            updated_at: updated(),
            provider_name: s("Nightingales"),
            method: MappingMethod::ALIAS,
            score: Some(0.75),
        },
        "act-mapping"
    );
    assert_eq!(act.cache_key(), key, "act-mapping cache key");
    assert_eq!(
        decode::<GenreMappingInfo>("genre-mapping.value"),
        GenreMappingInfo {
            key: MappingKey {
                source_id: 1,
                provider_id: "src:genre:9".into()
            },
            genre_id: 3,
            version: 1,
            created_at: created(),
            updated_at: updated(),
            provider_name: s("Indie"),
            method: MappingMethod::NAME,
            score: Some(1.0),
        },
        "genre-mapping"
    );
    assert_eq!(
        decode::<EditionMappingInfo>("edition-mapping.value"),
        EditionMappingInfo {
            key: MappingKey {
                source_id: 1,
                provider_id: "src:edition:118".into()
            },
            edition_id: edition(),
            version: 1,
            created_at: created(),
            updated_at: updated(),
            method: MappingMethod::MANUAL,
            ..Default::default()
        },
        "edition-mapping"
    );
    assert_eq!(
        decode::<ShowMappingInfo>("show-mapping.value"),
        ShowMappingInfo {
            key: MappingKey {
                source_id: 1,
                provider_id: "src:show:73664876".into()
            },
            show_id: SHOW,
            version: 1,
            created_at: created(),
            updated_at: updated(),
            provider_name: s("Nightingales v Larks"),
            method: MappingMethod::SCHEDULE,
            score: Some(0.5),
        },
        "show-mapping"
    );
}

#[test]
fn ticket_catalogue() {
    assert_eq!(decode::<i32>("tier-family.key"), 2, "tier-family.key");
    assert_eq!(
        decode::<TierFamilyInfo>("tier-family.value"),
        TierFamilyInfo {
            id: 2,
            slug: "over-under".into(),
            name: "Over/Under".into(),
            description: "Two tiers either side of a line.".into(),
            version: 1,
            created_at: created(),
            updated_at: updated(),
            ..Default::default()
        },
        "tier-family"
    );
    assert_eq!(
        decode::<TicketTypeInfo>("ticket-type.value"),
        TicketTypeInfo {
            id: 7320000000000002,
            genre_id: 3,
            slug: "total".into(),
            revision: 2,
            name: "Total".into(),
            family_id: 2,
            offered: SaleWindow::BOTH,
            slots: vec![
                TicketSlot {
                    role: SlotRole::OVER,
                    name: None
                },
                TicketSlot {
                    role: SlotRole::UNDER,
                    name: None
                },
            ],
            line_attributes: vec![AttributeField {
                name: "total".into(),
                kind: AttributeKind::LINE
            }],
            published_at: updated(),
            version: 3,
            created_at: created(),
            updated_at: updated(),
            ticket_attributes: vec![AttributeField {
                name: "set".into(),
                kind: AttributeKind::SET
            }],
            ..Default::default()
        },
        "ticket-type"
    );
    let picks: TicketTypeInfo = decode("ticket-type-picks.value");
    assert_eq!(
        picks.slots,
        [
            TicketSlot {
                role: SlotRole::PICK,
                name: s("1X")
            },
            TicketSlot {
                role: SlotRole::PICK,
                name: s("X2")
            },
            TicketSlot {
                role: SlotRole::HEADLINER,
                name: None
            },
        ],
        "ticket-type-picks slots"
    );
    assert_eq!(picks.set_id, Some(4), "ticket-type-picks set");
    assert_eq!(picks.line_attributes.len(), 0, "ticket-type-picks lines");

    let key: SetKey = decode("set.key");
    assert_eq!(key, SetKey { genre_id: 3, id: 5 }, "set.key");
    assert_eq!(
        decode::<SetInfo>("set.value"),
        SetInfo {
            key,
            slug: "encore".into(),
            name: "Encore".into(),
            short_name: "ENC".into(),
            kind: SetKind::PHASE,
            is_on_stage: true,
            position: 5,
            version: 1,
            created_at: created(),
            updated_at: updated(),
            ..Default::default()
        },
        "set"
    );
}

#[test]
fn mapping_request() {
    assert_eq!(
        decode::<String>("mapping-request.key"),
        "1:event:src:show:73664876",
        "mapping-request.key"
    );
    assert_eq!(
        decode::<MappingRequest>("mapping-request.value"),
        MappingRequest {
            source_id: 1,
            kind: MappingKind::SHOW,
            provider_id: "src:show:73664876".into(),
            name: s("Nightingales v Larks"),
            genre_provider_id: s("src:genre:9"),
            country_provider_id: s("src:country:44"),
            tour_provider_id: s("src:tour:17"),
            country_code: s("PRT"),
            city: s("Porto"),
            headliner_provider_id: s("src:act:4698"),
            opener_provider_id: s("src:act:4702"),
            edition_provider_id: s("src:edition:118"),
            venue_provider_id: s("src:venue:31"),
            start_time_unix_ms: 1_783_197_000_000,
            ..Default::default()
        },
        "mapping-request"
    );
}
