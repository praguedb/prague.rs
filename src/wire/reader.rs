//! A MessagePack reader over a byte slice: just what decoding Prague's wire
//! needs, borrowing strings and binaries from the input.

use super::Error;

/// Reads MessagePack values one after another from a slice.
#[derive(Debug, Clone)]
pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

/// What kind of value comes next, as far as a decoder that dispatches on it
/// cares.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Nil,
    Bool,
    Int,
    Float,
    Str,
    Bin,
    Array,
    Map,
    Ext,
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    /// The bytes not read yet.
    pub fn remaining(&self) -> &'a [u8] {
        &self.buf[self.pos..]
    }

    pub fn is_empty(&self) -> bool {
        self.pos >= self.buf.len()
    }

    fn eof() -> Error {
        Error::new("unexpected end of input")
    }

    pub fn peek(&self) -> Result<u8, Error> {
        self.buf.get(self.pos).copied().ok_or_else(Self::eof)
    }

    /// The kind of the next value.
    pub fn peek_kind(&self) -> Result<Kind, Error> {
        Ok(match self.peek()? {
            0xc0 => Kind::Nil,
            0xc2 | 0xc3 => Kind::Bool,
            0x00..=0x7f | 0xe0..=0xff | 0xcc..=0xd3 => Kind::Int,
            0xca | 0xcb => Kind::Float,
            0xa0..=0xbf | 0xd9..=0xdb => Kind::Str,
            0xc4..=0xc6 => Kind::Bin,
            0x90..=0x9f | 0xdc | 0xdd => Kind::Array,
            0x80..=0x8f | 0xde | 0xdf => Kind::Map,
            0xc7..=0xc9 | 0xd4..=0xd8 => Kind::Ext,
            c => return Err(Error::new(format!("invalid MessagePack marker {c:#04x}"))),
        })
    }

    fn byte(&mut self) -> Result<u8, Error> {
        let b = self.peek()?;
        self.pos += 1;
        Ok(b)
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], Error> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|&e| e <= self.buf.len())
            .ok_or_else(Self::eof)?;
        let s = &self.buf[self.pos..end];
        self.pos = end;
        Ok(s)
    }

    fn be<const N: usize>(&mut self) -> Result<[u8; N], Error> {
        Ok(self.take(N)?.try_into().expect("take returns N bytes"))
    }

    fn len8(&mut self) -> Result<usize, Error> {
        Ok(self.byte()? as usize)
    }
    fn len16(&mut self) -> Result<usize, Error> {
        Ok(u16::from_be_bytes(self.be()?) as usize)
    }
    fn len32(&mut self) -> Result<usize, Error> {
        Ok(u32::from_be_bytes(self.be()?) as usize)
    }

    /// Consumes a nil and reports true, or leaves anything else unread.
    pub fn try_nil(&mut self) -> Result<bool, Error> {
        if self.peek()? == 0xc0 {
            self.pos += 1;
            return Ok(true);
        }
        Ok(false)
    }

    pub fn read_bool(&mut self) -> Result<bool, Error> {
        match self.byte()? {
            0xc2 => Ok(false),
            0xc3 => Ok(true),
            c => Err(Error::unexpected("a bool", c)),
        }
    }

    /// Any MessagePack integer that fits an i64.
    pub fn read_i64(&mut self) -> Result<i64, Error> {
        let c = self.byte()?;
        Ok(match c {
            0x00..=0x7f => c as i64,
            0xe0..=0xff => c as i8 as i64,
            0xcc => self.byte()? as i64,
            0xcd => u16::from_be_bytes(self.be()?) as i64,
            0xce => u32::from_be_bytes(self.be()?) as i64,
            0xcf => {
                let v = u64::from_be_bytes(self.be()?);
                i64::try_from(v).map_err(|_| Error::new(format!("integer {v} overflows i64")))?
            }
            0xd0 => self.byte()? as i8 as i64,
            0xd1 => i16::from_be_bytes(self.be()?) as i64,
            0xd2 => i32::from_be_bytes(self.be()?) as i64,
            0xd3 => i64::from_be_bytes(self.be()?),
            c => return Err(Error::unexpected("an integer", c)),
        })
    }

    /// Any MessagePack integer that fits an i32.
    pub fn read_i32(&mut self) -> Result<i32, Error> {
        let v = self.read_i64()?;
        i32::try_from(v).map_err(|_| Error::new(format!("integer {v} overflows i32")))
    }

    /// A float32, a float64, or an integer.
    pub fn read_f64(&mut self) -> Result<f64, Error> {
        match self.peek()? {
            0xca => {
                self.pos += 1;
                Ok(f32::from_be_bytes(self.be()?) as f64)
            }
            0xcb => {
                self.pos += 1;
                Ok(f64::from_be_bytes(self.be()?))
            }
            _ => self.read_i64().map(|v| v as f64),
        }
    }

    pub fn read_str(&mut self) -> Result<&'a str, Error> {
        let c = self.byte()?;
        let n = match c {
            0xa0..=0xbf => (c & 0x1f) as usize,
            0xd9 => self.len8()?,
            0xda => self.len16()?,
            0xdb => self.len32()?,
            c => return Err(Error::unexpected("a string", c)),
        };
        std::str::from_utf8(self.take(n)?)
            .map_err(|e| Error::new(format!("string is not UTF-8: {e}")))
    }

    pub fn read_bin(&mut self) -> Result<&'a [u8], Error> {
        let c = self.byte()?;
        let n = match c {
            0xc4 => self.len8()?,
            0xc5 => self.len16()?,
            0xc6 => self.len32()?,
            c => return Err(Error::unexpected("binary", c)),
        };
        self.take(n)
    }

    /// An extension's type and payload.
    pub fn read_ext(&mut self) -> Result<(i8, &'a [u8]), Error> {
        let c = self.byte()?;
        let n = match c {
            0xd4 => 1,
            0xd5 => 2,
            0xd6 => 4,
            0xd7 => 8,
            0xd8 => 16,
            0xc7 => self.len8()?,
            0xc8 => self.len16()?,
            0xc9 => self.len32()?,
            c => return Err(Error::unexpected("an extension", c)),
        };
        let ty = self.byte()? as i8;
        Ok((ty, self.take(n)?))
    }

    pub fn read_array_len(&mut self) -> Result<usize, Error> {
        let c = self.byte()?;
        match c {
            0x90..=0x9f => Ok((c & 0x0f) as usize),
            0xdc => self.len16(),
            0xdd => self.len32(),
            c => Err(Error::unexpected("an array", c)),
        }
    }

    pub fn read_map_len(&mut self) -> Result<usize, Error> {
        let c = self.byte()?;
        match c {
            0x80..=0x8f => Ok((c & 0x0f) as usize),
            0xde => self.len16(),
            0xdf => self.len32(),
            c => Err(Error::unexpected("a map", c)),
        }
    }

    /// Skips one value, whatever it is, nested values included.
    pub fn skip(&mut self) -> Result<(), Error> {
        // Values still to skip; iterative, so hostile nesting cannot overflow
        // the stack.
        let mut pending: usize = 1;
        while pending > 0 {
            pending -= 1;
            let c = self.byte()?;
            let skip = match c {
                0x00..=0x7f | 0xe0..=0xff | 0xc0 | 0xc2 | 0xc3 => 0,
                0xcc | 0xd0 => 1,
                0xcd | 0xd1 => 2,
                0xce | 0xd2 | 0xca => 4,
                0xcf | 0xd3 | 0xcb => 8,
                0xa0..=0xbf => (c & 0x1f) as usize,
                0xd9 | 0xc4 => self.len8()?,
                0xda | 0xc5 => self.len16()?,
                0xdb | 0xc6 => self.len32()?,
                0xd4 => 2,
                0xd5 => 3,
                0xd6 => 5,
                0xd7 => 9,
                0xd8 => 17,
                0xc7 => self.len8()? + 1,
                0xc8 => self.len16()? + 1,
                0xc9 => self.len32()? + 1,
                0x90..=0x9f => {
                    pending += (c & 0x0f) as usize;
                    0
                }
                0xdc => {
                    pending += self.len16()?;
                    0
                }
                0xdd => {
                    pending += self.len32()?;
                    0
                }
                0x80..=0x8f => {
                    pending += 2 * (c & 0x0f) as usize;
                    0
                }
                0xde => {
                    pending += 2 * self.len16()?;
                    0
                }
                0xdf => {
                    pending += 2 * self.len32()?;
                    0
                }
                c => return Err(Error::new(format!("invalid MessagePack marker {c:#04x}"))),
            };
            self.take(skip)?;
        }
        Ok(())
    }

    /// The next value's bytes, undecoded.
    pub fn read_raw(&mut self) -> Result<&'a [u8], Error> {
        let start = self.pos;
        self.skip()?;
        Ok(&self.buf[start..self.pos])
    }
}
