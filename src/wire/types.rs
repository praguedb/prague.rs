use std::fmt;
use std::str::FromStr;

use super::{Decode, Error, Kind, Reader};

/// A protop `uuid`, a .NET Guid, in RFC 4122 byte order. MessagePack-CSharp
/// writes a Guid member as its 36-character string; a Guid header is
/// Guid.ToByteArray()'s 16 bytes ([`Uuid::from_dotnet`]).
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct Uuid(pub [u8; 16]);

impl Uuid {
    pub const NIL: Uuid = Uuid([0; 16]);

    /// Parses the 8-4-4-4-12 form, in either case.
    pub fn parse(s: &str) -> Result<Self, Error> {
        let bad = || Error::new(format!("{s:?} is not a uuid"));
        let b = s.as_bytes();
        if b.len() != 36 || b[8] != b'-' || b[13] != b'-' || b[18] != b'-' || b[23] != b'-' {
            return Err(bad());
        }
        let mut out = [0u8; 16];
        let mut nibbles = b.iter().filter(|&&c| c != b'-');
        for byte in &mut out {
            let hi = hex(*nibbles.next().ok_or_else(bad)?).ok_or_else(bad)?;
            let lo = hex(*nibbles.next().ok_or_else(bad)?).ok_or_else(bad)?;
            *byte = hi << 4 | lo;
        }
        Ok(Uuid(out))
    }

    /// Reads Guid.ToByteArray()'s layout: the first three groups little-endian.
    pub fn from_dotnet(b: &[u8]) -> Result<Self, Error> {
        let b: &[u8; 16] = b
            .try_into()
            .map_err(|_| Error::new("a .NET Guid is 16 bytes"))?;
        let mut u = *b;
        u[..4].reverse();
        u[4..6].reverse();
        u[6..8].reverse();
        Ok(Uuid(u))
    }

    /// The uuid as Guid.ToByteArray() lays it out.
    pub fn to_dotnet(&self) -> [u8; 16] {
        let mut b = self.0;
        b[..4].reverse();
        b[4..6].reverse();
        b[6..8].reverse();
        b
    }
}

fn hex(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

impl FromStr for Uuid {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Error> {
        Uuid::parse(s)
    }
}

impl fmt::Display for Uuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, b) in self.0.iter().enumerate() {
            if matches!(i, 4 | 6 | 8 | 10) {
                f.write_str("-")?;
            }
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for Uuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl Decode for Uuid {
    fn decode(r: &mut Reader<'_>) -> Result<Self, Error> {
        if r.try_nil()? {
            return Ok(Uuid::NIL);
        }
        Uuid::parse(r.read_str()?)
    }
}

/// A protop `bytes`: MessagePack bin.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct Bytes(pub Vec<u8>);

impl fmt::Debug for Bytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Bytes(")?;
        for b in &self.0 {
            write!(f, "{b:02x}")?;
        }
        write!(f, ")")
    }
}

impl Decode for Bytes {
    fn decode(r: &mut Reader<'_>) -> Result<Self, Error> {
        if r.try_nil()? {
            return Ok(Bytes::default());
        }
        // A producer may write a byte payload as a string; take its bytes.
        let b = if r.peek_kind()? == Kind::Str {
            r.read_str()?.as_bytes()
        } else {
            r.read_bin()?
        };
        Ok(Bytes(b.to_vec()))
    }
}

/// A protop `raw`: one MessagePack value kept as written, for a union arm the
/// schema gives no type. [`crate::wire::decode`] reads it further.
#[derive(Clone, PartialEq, Eq, Hash, Default)]
pub struct Raw(pub Vec<u8>);

impl fmt::Debug for Raw {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Raw(")?;
        for b in &self.0 {
            write!(f, "{b:02x}")?;
        }
        write!(f, ")")
    }
}

impl Decode for Raw {
    fn decode(r: &mut Reader<'_>) -> Result<Self, Error> {
        r.read_raw().map(|b| Raw(b.to_vec()))
    }
}
