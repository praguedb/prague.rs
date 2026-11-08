//! What praguec's Rust backend emits code against: the [`Decode`] trait, the
//! MessagePack [`Reader`] it reads with, and the types with no direct Rust
//! equivalent on Prague's wire ([`Uuid`], [`Decimal`], [`Timestamp`],
//! [`Date`], [`Bytes`], [`Raw`]). Application code rarely needs more than
//! [`decode`].

mod reader;
mod time;
mod types;

pub use reader::{Kind, Reader};
pub use rust_decimal::Decimal;
pub use time::{Date, Timestamp};
pub use types::{Bytes, Raw, Uuid};

use std::fmt;

/// A value that does not decode, with where it failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    msg: String,
}

impl Error {
    pub fn new(msg: impl Into<String>) -> Self {
        Self { msg: msg.into() }
    }

    pub(crate) fn unexpected(want: &str, marker: u8) -> Self {
        Self::new(format!("expected {want}, found marker {marker:#04x}"))
    }

    /// The same error, located at slot `i` of `ty`: `Show[4]: …`.
    pub fn at(self, ty: &str, i: usize) -> Self {
        Self::new(format!("{ty}[{i}]: {}", self.msg))
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.msg)
    }
}

impl std::error::Error for Error {}

/// What a type's [`Decode::enrich`] reads from the Kafka message. A consumer
/// folds it into change detection: the same value under a new header is a
/// change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Enrichment {
    pub headers: &'static [&'static str],
    pub timestamp: bool,
}

impl Enrichment {
    pub const NONE: Enrichment = Enrichment {
        headers: &[],
        timestamp: false,
    };

    pub fn is_none(&self) -> bool {
        self.headers.is_empty() && !self.timestamp
    }
}

/// What a Kafka message carries besides its key and value.
pub trait Meta {
    /// Calls `f` for every header in order, repeats included.
    fn each_header(&self, f: &mut dyn FnMut(&str, &[u8]));
    /// The message timestamp in Unix milliseconds.
    fn timestamp(&self) -> i64;
}

/// A value read from MessagePack. praguec emits it for every declaration; the
/// scalars, `Option` and `Vec` have it here.
///
/// Nil decodes as the default value for every type but `Option`, where it is
/// `None`: a C# producer writes nil for a null reference whatever the member's
/// declared presence.
pub trait Decode: Sized {
    fn decode(r: &mut Reader<'_>) -> Result<Self, Error>;

    /// What [`enrich`](Decode::enrich) reads from the message; none unless the
    /// type has `[header]` or `[from_timestamp]` members.
    const ENRICHMENT: Enrichment = Enrichment::NONE;

    /// Writes the members Prague writes from the Kafka message. A consumer
    /// calls it after the value decodes and its value filters pass, which is
    /// Prague's order.
    fn enrich(&mut self, _m: &dyn Meta) {}
}

/// Decodes one value from the start of `b`. Bytes after it are ignored, as
/// MessagePack-CSharp ignores them.
pub fn decode<T: Decode>(b: &[u8]) -> Result<T, Error> {
    T::decode(&mut Reader::new(b))
}

macro_rules! nil_default {
    ($r:ident) => {
        if $r.try_nil()? {
            return Ok(Default::default());
        }
    };
}

impl Decode for bool {
    fn decode(r: &mut Reader<'_>) -> Result<Self, Error> {
        nil_default!(r);
        r.read_bool()
    }
}

impl Decode for i32 {
    fn decode(r: &mut Reader<'_>) -> Result<Self, Error> {
        nil_default!(r);
        r.read_i32()
    }
}

impl Decode for i64 {
    fn decode(r: &mut Reader<'_>) -> Result<Self, Error> {
        nil_default!(r);
        r.read_i64()
    }
}

impl Decode for f64 {
    fn decode(r: &mut Reader<'_>) -> Result<Self, Error> {
        nil_default!(r);
        r.read_f64()
    }
}

impl Decode for String {
    fn decode(r: &mut Reader<'_>) -> Result<Self, Error> {
        nil_default!(r);
        r.read_str().map(str::to_owned)
    }
}

impl<T: Decode> Decode for Option<T> {
    fn decode(r: &mut Reader<'_>) -> Result<Self, Error> {
        if r.try_nil()? {
            return Ok(None);
        }
        T::decode(r).map(Some)
    }
}

impl<T: Decode> Decode for Vec<T> {
    fn decode(r: &mut Reader<'_>) -> Result<Self, Error> {
        nil_default!(r);
        let n = r.read_array_len()?;
        // A hostile length cannot reserve more than the input could hold.
        let mut out = Vec::with_capacity(n.min(r.remaining().len()));
        for i in 0..n {
            out.push(T::decode(r).map_err(|e| e.at("list", i))?);
        }
        Ok(out)
    }
}

/// MessagePack-CSharp writes System.Decimal as its invariant-culture string
/// ("12.3400"), so that is the form read; numeric forms are taken too.
impl Decode for Decimal {
    fn decode(r: &mut Reader<'_>) -> Result<Self, Error> {
        nil_default!(r);
        match r.peek_kind()? {
            Kind::Str => {
                let s = r.read_str()?;
                s.parse()
                    .map_err(|e| Error::new(format!("decimal {s:?}: {e}")))
            }
            Kind::Float => {
                let f = r.read_f64()?;
                Decimal::try_from(f).map_err(|e| Error::new(format!("decimal {f}: {e}")))
            }
            _ => r.read_i64().map(Decimal::from),
        }
    }
}

/// A Kafka header value, as Prague's `HeadersSerDe` writes it.
pub trait HeaderValue: Sized {
    fn from_header(b: &[u8]) -> Result<Self, Error>;
}

/// A string header is raw UTF-8.
impl HeaderValue for String {
    fn from_header(b: &[u8]) -> Result<Self, Error> {
        String::from_utf8(b.to_vec()).map_err(|e| Error::new(format!("header is not UTF-8: {e}")))
    }
}

/// An int32 header is 4 raw little-endian bytes (which no MessagePack integer
/// is) or exactly one MessagePack integer.
impl HeaderValue for i32 {
    fn from_header(b: &[u8]) -> Result<Self, Error> {
        if let Ok(le) = <[u8; 4]>::try_from(b) {
            return Ok(i32::from_le_bytes(le));
        }
        exact(b)
    }
}

/// An int64 header is 8 raw little-endian bytes or exactly one MessagePack
/// integer.
impl HeaderValue for i64 {
    fn from_header(b: &[u8]) -> Result<Self, Error> {
        if let Ok(le) = <[u8; 8]>::try_from(b) {
            return Ok(i64::from_le_bytes(le));
        }
        exact(b)
    }
}

/// A Guid header is Guid.ToByteArray()'s 16 bytes.
impl HeaderValue for Uuid {
    fn from_header(b: &[u8]) -> Result<Self, Error> {
        Uuid::from_dotnet(b)
    }
}

macro_rules! header_by_msgpack {
    ($($t:ty),*) => {$(
        impl HeaderValue for $t {
            fn from_header(b: &[u8]) -> Result<Self, Error> {
                exact(b)
            }
        }
    )*};
}
header_by_msgpack!(bool, f64, Decimal, Timestamp, Date);

/// Decodes one MessagePack value that must be the whole of `b`.
pub fn exact<T: Decode>(b: &[u8]) -> Result<T, Error> {
    let mut r = Reader::new(b);
    let v = T::decode(&mut r)?;
    if !r.is_empty() {
        return Err(Error::new("trailing bytes after header value"));
    }
    Ok(v)
}

/// Whether a header value is MessagePack nil, which Prague's predicate filters
/// answer with their `pass_on_null`.
pub fn is_nil(b: &[u8]) -> bool {
    b == [0xc0]
}

/// Writes every occurrence of `key` that decodes into `dst`, in order, so the
/// last decodable one wins and an undecodable one is skipped: Prague's
/// enricher. A message without the header leaves `dst` as the value had it.
pub fn header<T: HeaderValue>(m: &dyn Meta, key: &str, dst: &mut T) {
    m.each_header(&mut |k, b| {
        if k == key
            && let Ok(v) = T::from_header(b)
        {
            *dst = v;
        }
    });
}

/// [`header`] for an optional member.
pub fn header_opt<T: HeaderValue>(m: &dyn Meta, key: &str, dst: &mut Option<T>) {
    m.each_header(&mut |k, b| {
        if k == key
            && let Ok(v) = T::from_header(b)
        {
            *dst = Some(v);
        }
    });
}

/// The arm of a union whose wire is `[arm, payload]`: reads the array header
/// and the arm. The caller decodes the payload and then calls [`union_end`].
pub fn union_begin(r: &mut Reader<'_>, ty: &str) -> Result<(i32, usize), Error> {
    let n = r.read_array_len()?;
    if n < 2 {
        return Err(Error::new(format!(
            "{ty} is [arm, payload]; this array has {n} elements"
        )));
    }
    Ok((r.read_i32()?, n))
}

/// Skips what a union's array holds after its payload, for a later shape.
pub fn union_end(r: &mut Reader<'_>, n: usize) -> Result<(), Error> {
    for _ in 2..n {
        r.skip()?;
    }
    Ok(())
}
