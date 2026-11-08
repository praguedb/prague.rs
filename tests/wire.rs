//! The MessagePack reader against every marker family, written by hand from
//! the spec, so it holds for producers other than the fixtures' one.

use prague::wire::{self, Bytes, Decimal, Kind, Raw, Reader, Uuid};

fn int_forms() -> Vec<(Vec<u8>, i64)> {
    vec![
        (vec![0x00], 0),
        (vec![0x7f], 127),
        (vec![0xff], -1),
        (vec![0xe0], -32),
        (vec![0xcc, 0xff], 255),
        (vec![0xcd, 0xff, 0xff], 65_535),
        (vec![0xce, 0xff, 0xff, 0xff, 0xff], 4_294_967_295),
        (
            vec![0xcf, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
            i64::MAX,
        ),
        (vec![0xd0, 0x80], -128),
        (vec![0xd1, 0x80, 0x00], -32_768),
        (vec![0xd2, 0x80, 0x00, 0x00, 0x00], i32::MIN as i64),
        (vec![0xd3, 0x80, 0, 0, 0, 0, 0, 0, 0], i64::MIN),
    ]
}

#[test]
fn integers_in_every_width() {
    for (b, want) in int_forms() {
        assert_eq!(wire::decode::<i64>(&b).unwrap(), want, "{b:02x?}");
        assert_eq!(Reader::new(&b).peek_kind().unwrap(), Kind::Int);
        let mut r = Reader::new(&b);
        r.skip().unwrap();
        assert!(r.is_empty(), "skip {b:02x?}");
    }
    assert!(
        wire::decode::<i64>(&[0xcf, 0x80, 0, 0, 0, 0, 0, 0, 0]).is_err(),
        "u64 above i64::MAX"
    );
    assert!(
        wire::decode::<i32>(&[0xce, 0xff, 0xff, 0xff, 0xff]).is_err(),
        "u32 above i32::MAX"
    );
    assert_eq!(wire::decode::<i32>(&[0xd2, 0, 0, 0, 0x2c]).unwrap(), 44);
}

#[test]
fn floats_and_ints_read_as_double() {
    let f32b = [0xca, 0x3f, 0xec, 0xcc, 0xcd]; // 1.85f32
    assert_eq!(wire::decode::<f64>(&f32b).unwrap(), 1.85f32 as f64);
    let mut f64b = vec![0xcb];
    f64b.extend_from_slice(&2.5f64.to_be_bytes());
    assert_eq!(wire::decode::<f64>(&f64b).unwrap(), 2.5);
    assert_eq!(wire::decode::<f64>(&[0x05]).unwrap(), 5.0);
}

fn sized(prefix8: u8, prefix16: u8, prefix32: u8, payload: &[u8]) -> [Vec<u8>; 3] {
    let n = payload.len();
    let mut a = vec![prefix8, n as u8];
    a.extend_from_slice(payload);
    let mut b = vec![prefix16];
    b.extend_from_slice(&(n as u16).to_be_bytes());
    b.extend_from_slice(payload);
    let mut c = vec![prefix32];
    c.extend_from_slice(&(n as u32).to_be_bytes());
    c.extend_from_slice(payload);
    [a, b, c]
}

#[test]
fn strings_and_binaries_in_every_width() {
    let mut fix = vec![0xa5];
    fix.extend_from_slice(b"hello");
    assert_eq!(wire::decode::<String>(&fix).unwrap(), "hello");
    for b in sized(0xd9, 0xda, 0xdb, b"hello") {
        assert_eq!(wire::decode::<String>(&b).unwrap(), "hello");
        let mut r = Reader::new(&b);
        r.skip().unwrap();
        assert!(r.is_empty());
    }
    for b in sized(0xc4, 0xc5, 0xc6, &[1, 2, 3]) {
        assert_eq!(wire::decode::<Bytes>(&b).unwrap(), Bytes(vec![1, 2, 3]));
        let mut r = Reader::new(&b);
        r.skip().unwrap();
        assert!(r.is_empty());
    }
    assert!(
        wire::decode::<String>(&[0xa2, 0xff, 0xfe]).is_err(),
        "not UTF-8"
    );
    assert!(wire::decode::<String>(&[0xa5, b'h']).is_err(), "truncated");
}

#[test]
fn arrays_and_maps_in_every_width_skip_whole() {
    // [1, {"a": [nil, true]}, "x"] in each array and map width.
    let inner_map = |m16: bool| -> Vec<u8> {
        let mut v = if m16 {
            vec![0xde, 0, 1]
        } else {
            vec![0xdf, 0, 0, 0, 1]
        };
        v.extend_from_slice(&[0xa1, b'a', 0x92, 0xc0, 0xc3]);
        v
    };
    for (head, m16) in [
        (vec![0x93], true),
        (vec![0xdc, 0, 3], false),
        (vec![0xdd, 0, 0, 0, 3], true),
    ] {
        let mut b = head;
        b.push(0x01);
        b.extend(inner_map(m16));
        b.extend_from_slice(&[0xa1, b'x']);
        b.push(0x2a); // a trailing value the skip must not touch
        let mut r = Reader::new(&b);
        let raw = r.read_raw().unwrap().to_vec();
        assert_eq!(r.remaining(), [0x2a]);
        assert_eq!(raw.len(), b.len() - 1);
        let mut r = Reader::new(&b);
        assert_eq!(r.read_array_len().unwrap(), 3);
    }
    let mut fixmap = Reader::new(&[0x81, 0x01, 0x02]);
    assert_eq!(fixmap.read_map_len().unwrap(), 1);
}

#[test]
fn every_extension_size_skips() {
    let forms: Vec<Vec<u8>> = vec![
        vec![0xd4, 1, 0],
        vec![0xd5, 1, 0, 0],
        vec![0xd6, 1, 0, 0, 0, 0],
        [vec![0xd7, 1], vec![0; 8]].concat(),
        [vec![0xd8, 1], vec![0; 16]].concat(),
        vec![0xc7, 3, 1, 0, 0, 0],
        vec![0xc8, 0, 3, 1, 0, 0, 0],
        vec![0xc9, 0, 0, 0, 3, 1, 0, 0, 0],
    ];
    for b in forms {
        let mut r = Reader::new(&b);
        assert_eq!(r.peek_kind().unwrap(), Kind::Ext);
        r.skip().unwrap();
        assert!(r.is_empty(), "{b:02x?}");
        let (ty, _) = Reader::new(&b).read_ext().unwrap();
        assert_eq!(ty, 1);
    }
}

#[test]
fn hostile_input_errors_instead_of_panicking() {
    // A length far past the input, an array claiming 2^32-1 elements, a
    // reserved marker, and deep nesting.
    assert!(wire::decode::<String>(&[0xdb, 0xff, 0xff, 0xff, 0xff]).is_err());
    assert!(wire::decode::<Vec<i32>>(&[0xdd, 0xff, 0xff, 0xff, 0xff]).is_err());
    assert!(Reader::new(&[0xc1]).skip().is_err());
    let deep = vec![0x91; 1_000_000];
    assert!(Reader::new(&deep).skip().is_err(), "truncated deep nesting");
}

#[test]
fn nil_is_none_or_default() {
    assert_eq!(wire::decode::<Option<i32>>(&[0xc0]).unwrap(), None);
    assert_eq!(wire::decode::<i32>(&[0xc0]).unwrap(), 0);
    assert_eq!(wire::decode::<String>(&[0xc0]).unwrap(), "");
    assert_eq!(
        wire::decode::<Vec<i32>>(&[0xc0]).unwrap(),
        Vec::<i32>::new()
    );
    assert_eq!(
        wire::decode::<Option<Vec<i32>>>(&[0x92, 1, 2]).unwrap(),
        Some(vec![1, 2])
    );
}

#[test]
fn uuid_decimal_raw() {
    let s = "0b6f2a1e-5c3d-4e7f-8a9b-0c1d2e3f4a5b";
    let mut b = vec![0xd9, 36];
    b.extend_from_slice(s.as_bytes());
    let u = wire::decode::<Uuid>(&b).unwrap();
    assert_eq!(u.to_string(), s);
    assert_eq!(Uuid::from_dotnet(&u.to_dotnet()).unwrap(), u);
    assert_eq!(u.to_dotnet()[..4], [0x1e, 0x2a, 0x6f, 0x0b]);
    assert!(Uuid::parse("0b6f2a1e5c3d4e7f8a9b0c1d2e3f4a5b").is_err());
    assert_eq!(s.to_uppercase().parse::<Uuid>().unwrap(), u);

    let mut d = vec![0xa7];
    d.extend_from_slice(b"12.3400");
    assert_eq!(wire::decode::<Decimal>(&d).unwrap().to_string(), "12.3400");
    assert_eq!(wire::decode::<Decimal>(&[0x07]).unwrap(), Decimal::from(7));

    assert_eq!(
        wire::decode::<Raw>(&[0x92, 0x01, 0xa1, b'x', 0x00]).unwrap(),
        Raw(vec![0x92, 0x01, 0xa1, b'x'])
    );
}

#[test]
fn header_values() {
    use wire::HeaderValue;
    assert_eq!(i32::from_header(&7i32.to_le_bytes()).unwrap(), 7);
    assert_eq!(i32::from_header(&[0x07]).unwrap(), 7);
    assert!(i32::from_header(&[0x07, 0x00]).is_err(), "trailing bytes");
    assert_eq!(i64::from_header(&(-3i64).to_le_bytes()).unwrap(), -3);
    assert_eq!(String::from_header(b"eu").unwrap(), "eu");
    let u = Uuid::parse("0b6f2a1e-5c3d-4e7f-8a9b-0c1d2e3f4a5b").unwrap();
    assert_eq!(Uuid::from_header(&u.to_dotnet()).unwrap(), u);
    assert!(wire::is_nil(&[0xc0]));
}
