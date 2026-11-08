//! The fixtures were written by C# types through Prague's CacheSerde;
//! contracts::tenant is praguec's Rust emission of schemas/tenant.prague, the
//! same types.

use std::collections::HashMap;

use prague::Keyed;
use prague::wire::{self, Bytes, Reader, Uuid};

use crate::contracts::tenant::{
    BrandInfo, BrandStatus, Currency, CurrencyInfo, ExchangeRate, OperatorInfo, OperatorStatus,
    PasswordRuleMode, PlatformSetting, PlatformSettingKey, PlatformSettingType, RateProviderStatus,
    Setting, SettingCode, SettingEntityType, SettingGroup, SettingKey, SettingKind, SettingLock,
    SettingOwnerType, SettingValue, SiteInfo,
};

fn decode<T: wire::Decode>(name: &str) -> T {
    crate::decode("tenant", name)
}

fn uuid(s: &str) -> Uuid {
    Uuid::parse(s).unwrap()
}

fn acme() -> Uuid {
    uuid("0b6f2a1e-5c3d-4e7f-8a9b-0c1d2e3f4a5b")
}
fn zeta() -> Uuid {
    uuid("7f8e9d0c-1b2a-4c3d-9e8f-a0b1c2d3e4f5")
}
fn alpha() -> Uuid {
    uuid("a1b2c3d4-e5f6-4789-abcd-ef0123456789")
}
fn site() -> Uuid {
    uuid("c3d4e5f6-a7b8-4901-8def-012345678901")
}
fn license() -> Uuid {
    uuid("d4e5f6a7-b8c9-4012-9ef0-123456789012")
}
/// SettingOwner.Platform.
fn platform() -> Uuid {
    uuid("00000000-0000-0000-706c-6174666f726d")
}

const CREATED: i64 = 1_767_225_600_000;
const UPDATED: i64 = 1_767_312_000_000;

/// A reader at the value under string key `key` of the MessagePack map `b`.
fn map_value<'a>(b: &'a [u8], key: &str) -> Reader<'a> {
    let mut r = Reader::new(b);
    let n = r.read_map_len().unwrap_or_else(|e| panic!("{b:02x?}: {e}"));
    for _ in 0..n {
        if r.read_str().unwrap() == key {
            return r;
        }
        r.skip().unwrap();
    }
    panic!("{b:02x?} has no key {key:?}");
}

#[test]
fn tenancy() {
    assert_eq!(decode::<Uuid>("operator-acme.key"), acme(), "operator key");
    let o: OperatorInfo = decode("operator-acme.value");
    assert_eq!(o.id, acme(), "operator: {o:?}");
    assert_eq!(o.slug, "acme");
    assert_eq!(o.legal_entity.as_deref(), Some("Acme Europe AG"));
    assert_eq!(o.status, OperatorStatus::ACTIVE);
    assert_eq!(o.base_currency, Currency::EUR);
    assert_eq!(o.base_currency_code, "EUR");
    assert_eq!(o.created_at_unix_ms, CREATED);
    assert_eq!(o.updated_at_unix_ms, UPDATED);
    assert_eq!(o.cache_key(), acme());

    let z: OperatorInfo = decode("operator-zeta.value");
    assert_eq!(z.id, zeta(), "operator zeta: {z:?}");
    assert_eq!(z.legal_entity, None);
    assert_eq!(z.base_currency, Currency::USD);
    assert_eq!(z.status, OperatorStatus::PENDING);

    let b: BrandInfo = decode("brand-alpha.value");
    assert_eq!(b.id, alpha(), "brand: {b:?}");
    assert_eq!(b.operator_id, acme());
    assert_eq!(b.name, "Alpha Stores");
    assert_eq!(b.status, BrandStatus::ACTIVE);

    let s: SiteInfo = decode("site-alpha-com.value");
    assert_eq!(s.id, site(), "site: {s:?}");
    assert_eq!(s.brand_id, alpha());
    assert_eq!(s.operator_id, acme());
    assert_eq!(s.domain, "alpha.example");
    assert_eq!(s.license_id, license());

    // Header-only members have no slot: the value leaves them zero.
    assert_eq!(
        o.schema_version, 0,
        "a header-only member decoded from the value"
    );
    assert_eq!(
        b.schema_version, 0,
        "a header-only member decoded from the value"
    );
}

#[test]
fn platform_setting_keys_by_a_hashable_struct() {
    let k: PlatformSettingKey = decode("platform-setting-platform.key");
    assert_eq!(
        k,
        PlatformSettingKey {
            r#type: PlatformSettingType::PASSWORD_POLICY,
            tenant_id: None
        }
    );
    let p: PlatformSetting = decode("platform-setting-platform.value");
    assert_eq!(p.cache_key(), k, "platform setting: {p:?}");
    assert_eq!(p.version, 3);
    let pp = p.password_policy.as_ref().expect("password policy");
    assert_eq!(pp.min_length, 10, "password policy: {pp:?}");
    assert!(pp.require_uppercase);
    assert!(!pp.require_lowercase);
    assert!(pp.require_number);
    assert!(pp.passkeys_allowed_for_everyone);
    assert_eq!(pp.expiry.mode, PasswordRuleMode::REQUIRED_FOR_EVERYONE);
    assert_eq!(pp.expiry.value, Some(90));
    assert_eq!(pp.expiry.locked, Some(true));
    assert_eq!(pp.lockout.mode, PasswordRuleMode::OPERATOR_DECIDES);
    assert_eq!(pp.lockout.value, None);
    assert_eq!(pp.lockout.locked, None);

    let tk: PlatformSettingKey = decode("platform-setting-acme.key");
    assert_eq!(
        tk,
        PlatformSettingKey {
            r#type: PlatformSettingType::PASSWORD_POLICY,
            tenant_id: Some(acme())
        }
    );
    let a: PlatformSetting = decode("platform-setting-acme.value");
    assert_eq!(a.cache_key(), tk, "tenant setting: {a:?}");
    assert_eq!(a.password_policy, None);

    // Equal keys from different decodes are one map key.
    let m = HashMap::from([(k, 1), (tk, 2)]);
    assert_eq!(
        m.get(&decode::<PlatformSettingKey>("platform-setting-acme.key")),
        Some(&2),
        "a decoded key is not the same map key"
    );
}

fn setting(name: &str) -> Setting {
    let s: Setting = decode(&format!("setting-{name}.value"));
    let k: SettingKey = decode(&format!("setting-{name}.key"));
    assert_eq!(k, s.cache_key(), "{name}: message key vs value key");
    assert_eq!(s.lock_state, SettingLock::BOUNDED, "{name}: {s:?}");
    assert_eq!(s.version, 2, "{name}: {s:?}");
    assert_eq!(s.created_at_unix_ms, CREATED, "{name}: {s:?}");
    s
}

/// SettingValue is the pseudo-union: [kind, payload], the payload's type chosen
/// by the kind. One fixture per SettingKind.
#[test]
fn setting_value_every_kind() {
    let s = setting("nil");
    assert_eq!(s.value, SettingValue::Nil, "nil: {s:?}");
    assert_eq!(s.key.owner, platform());
    assert_eq!(s.owner_type, SettingOwnerType::PLATFORM);

    let s = setting("bool");
    assert_eq!(s.value, SettingValue::Bool(true), "bool");
    assert_eq!(s.key.code, SettingCode::PASSWORD);

    let s = setting("int");
    assert_eq!(s.value, SettingValue::Int(12), "int");
    assert_eq!(
        s.key,
        SettingKey {
            owner: acme(),
            group: SettingGroup::PASSWORD_POLICY,
            code: SettingCode::PASSWORD_MIN_LENGTH,
            subject_id: None,
        }
    );

    let s = setting("real");
    assert_eq!(s.value, SettingValue::Real(2.5), "real");
    assert_eq!(s.owner_type, SettingOwnerType::BRAND);

    let s = setting("guid");
    let SettingValue::Guid(g) = &s.value else {
        panic!("guid: {:?}", s.value)
    };
    assert_eq!(Uuid::from_dotnet(&g.0), Ok(license()), "guid");
    assert_eq!(s.key.subject_id, Some(site()));
    assert_eq!(s.entity_type, SettingEntityType::SITE);

    assert_eq!(
        setting("string").value,
        SettingValue::String("days".into()),
        "string"
    );
    assert_eq!(
        setting("bytes").value,
        SettingValue::Bytes(Bytes(vec![1, 2, 3])),
        "bytes"
    );
    assert_eq!(
        setting("json").value,
        SettingValue::Json(r#"{"a":1}"#.into()),
        "json"
    );

    let s = setting("object");
    let SettingValue::Object(obj) = &s.value else {
        panic!("object: {:?}", s.value)
    };
    assert_eq!(
        map_value(&obj.0, "kind").read_str(),
        Ok("temporary"),
        "object: {obj:?}"
    );
}

/// A kind this schema does not know keeps its payload as written, and an
/// element after the payload is skipped, both as the C# formatter does.
#[test]
fn setting_value_keeps_an_unknown_kind() {
    // [99, {"x": 1}, "later"]
    let b = [
        0x93, 0x63, 0x81, 0xa1, b'x', 0x01, 0xa5, b'l', b'a', b't', b'e', b'r',
    ];
    let v: SettingValue = wire::decode(&b).unwrap();
    let SettingValue::Unknown(kind, raw) = &v else {
        panic!("{v:?}")
    };
    assert_eq!(*kind, SettingKind(99));
    assert_eq!(map_value(&raw.0, "x").read_i64(), Ok(1), "raw {raw:?}");
}

#[test]
fn finance() {
    assert_eq!(decode::<i32>("currency-EUR.key"), 978, "currency key");
    let c: CurrencyInfo = decode("currency-EUR.value");
    assert_eq!(c.number, 978, "currency: {c:?}");
    assert_eq!(c.code, "EUR");
    assert_eq!(c.minor_unit, 2);
    assert_eq!(c.storage_exponent, 4);
    assert!(c.is_enabled);
    assert!(c.is_pivot);
    assert_eq!(c.provider, "ecb");

    let r: ExchangeRate = decode("exchange-rate.value");
    assert_eq!(r.pair, 978 << 16 | 840, "rate: {r:?}");
    assert_eq!(r.base, 978);
    assert_eq!(r.quote, 840);
    assert_eq!(r.rate_mantissa, 10_850);
    assert_eq!(r.rate_scale, 4);
    assert_eq!(r.rate_id, 70);

    let p: RateProviderStatus = decode("rate-provider.value");
    assert_eq!(p.cache_key(), "ecb", "provider: {p:?}");
    assert_eq!(p.next_update_unix_ms, UPDATED + 3_600_000);
}
