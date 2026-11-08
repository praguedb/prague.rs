//! The fixtures were written by the producer's own C# contract types through
//! Prague's CacheSerde. Those types carry two members schemas/showfeed.prague
//! reserves -- the per-producer stamps map and a tier line's attributes -- so
//! every row here also proves the reader skips a map and a list of mixed
//! MessagePack types it has no member for.

use prague::Keyed;
use prague::wire::Uuid;

use crate::contracts::showfeed::{
    LineSettlement, Quote, QuoteStatus, SaleStage, SaleStatus, SetScore, SettlementCertainty,
    ShowKey, ShowStatus, SourceLiveShowInfo, SourceShowInfo, SourceState, SourceTicketTypeInfo,
    StageHealth, TicketTypeKey, TierLine,
};

fn decode<T: prague::wire::Decode>(name: &str) -> T {
    crate::decode("showfeed", name)
}

const SHOW: i64 = 7312345678901234;
const ACT1: i64 = 7301234567890123;
const ACT2: i64 = 7301234567890456;

fn tour() -> Uuid {
    Uuid::parse("5e1f0c2a-7b3d-4c8e-9f10-a1b2c3d4e5f6").unwrap()
}

#[test]
fn show() {
    let key: ShowKey = decode("show.key");
    assert_eq!(
        key,
        ShowKey {
            source: 1,
            show_id: SHOW
        },
        "show.key"
    );
    let row: SourceShowInfo = decode("show.value");
    assert_eq!(
        row,
        SourceShowInfo {
            key: key.clone(),
            show_id: SHOW,
            genre_id: 3,
            country_id: 44,
            tour_id: tour(),
            start_time_unix_ms: 1_783_197_000_000,
            status: ShowStatus::LIVE,
            headliner_id: ACT1,
            opener_id: ACT2,
            provider_show_id: "src:show:73664876".into(),
        },
        "show"
    );
    assert_eq!(row.cache_key(), key, "show cache key");
}

#[test]
fn live_show() {
    assert_eq!(
        decode::<SourceLiveShowInfo>("live-show.value"),
        SourceLiveShowInfo {
            key: ShowKey {
                source: 1,
                show_id: SHOW
            },
            show_id: SHOW,
            status: ShowStatus::LIVE,
            status_code: 6,
            headliner_score: 2,
            opener_score: 1,
            sets: vec![
                SetScore {
                    number: 1,
                    headliner: 1,
                    opener: 0
                },
                SetScore {
                    number: 2,
                    headliner: 1,
                    opener: 1
                },
            ],
            clock_seconds: 2710,
            health: None,
        },
        "live-show"
    );

    // A stage-health row: keyed [source, -1 - stage], the rest defaults.
    let key: ShowKey = decode("live-health.key");
    assert_eq!(
        key,
        ShowKey {
            source: 1,
            show_id: -2
        },
        "live-health.key"
    );
    assert_eq!(
        decode::<SourceLiveShowInfo>("live-health.value"),
        SourceLiveShowInfo {
            key,
            show_id: -2,
            status: ShowStatus::UNKNOWN,
            status_code: 0,
            headliner_score: -1,
            opener_score: -1,
            sets: vec![],
            clock_seconds: -1,
            health: Some(StageHealth {
                stage: SaleStage::LIVE,
                state: SourceState::DOWN,
                since_unix_ms: 1_783_190_000_456,
            }),
        },
        "live-health"
    );
}

#[test]
fn ticket_type() {
    let key: TicketTypeKey = decode("ticket-type.key");
    assert_eq!(
        key,
        TicketTypeKey {
            source: 1,
            show_id: SHOW,
            ticket_type_id: 2
        },
        "ticket-type.key"
    );
    let row: SourceTicketTypeInfo = decode("ticket-type.value");
    assert_eq!(row.cache_key(), key, "ticket-type cache key");
    assert_eq!(row.show_id, SHOW, "ticket-type: {row:?}");
    assert_eq!(row.stage, SaleStage::LIVE, "ticket-type: {row:?}");
    assert_eq!(row.lines.len(), 4, "ticket-type: {row:?}");
    let q = |price: f32, status: QuoteStatus| Quote {
        price: f64::from(price),
        status,
    };
    assert_eq!(
        row.lines[0],
        TierLine {
            slot: 0,
            status: SaleStatus::OPEN,
            favourite: true,
            quotes: vec![q(1.85, QuoteStatus::ACTIVE), q(1.95, QuoteStatus::ACTIVE)],
            ..Default::default()
        },
        "line 0"
    );
    assert_eq!(
        row.lines[1],
        TierLine {
            slot: 0,
            status: SaleStatus::SUSPENDED,
            quotes: vec![q(2.6, QuoteStatus::INACTIVE), q(1.45, QuoteStatus::ACTIVE)],
            ..Default::default()
        },
        "line 1"
    );
    assert_eq!(
        row.lines[2],
        TierLine {
            slot: 1,
            status: SaleStatus::SETTLED,
            quotes: vec![
                q(3.25, QuoteStatus::WON),
                q(2.2, QuoteStatus::LOST),
                q(5.0, QuoteStatus::VOID),
                q(0.0, QuoteStatus::HALF_LOST),
            ],
            settlement: LineSettlement::SETTLED,
            certainty: SettlementCertainty::CONFIRMED,
            ..Default::default()
        },
        "line 2"
    );
    assert_eq!(
        row.lines[3],
        TierLine {
            slot: 2,
            status: SaleStatus::OPEN,
            quotes: vec![],
            ..Default::default()
        },
        "line 3"
    );
}
