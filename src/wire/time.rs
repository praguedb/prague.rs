use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::{Decode, Error, Kind, Reader};

const TICKS_PER_SECOND: i64 = 10_000_000;
/// 1970-01-01T00:00:00Z in .NET ticks.
const UNIX_EPOCH_TICKS: i64 = 621_355_968_000_000_000;
const TICKS_MASK: i64 = 0x3FFF_FFFF_FFFF_FFFF;
/// The top bit: DateTimeKind.Local.
const LOCAL_MASK: i64 = i64::MIN;
/// Where .NET wraps a Local value whose UTC ticks went negative.
const TICKS_CEILING: i64 = 0x4000_0000_0000_0000;
/// DateTime.MaxValue.
const MAX_TICKS: i64 = 3_155_378_975_999_999_999;
const SECONDS_PER_DAY: i64 = 86_400;

/// A protop `timestamp`, a .NET DateTime, as an instant: UTC ticks, 100 ns
/// each since 0001-01-01. It is an integer so it compares, sorts, hashes and
/// keys a range index as the instant it is, over DateTime's whole range.
///
/// Prague writes a DateTime as `DateTime.ToBinary()`: the ticks in the low 62
/// bits and the DateTimeKind in the top two, with no offset on the wire. A Utc
/// or Local value is an instant (ToBinary stores a Local value's ticks already
/// shifted to UTC); an Unspecified one is a wall-clock time with no zone, read
/// as UTC. MessagePack's own timestamp extension is read too.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct Timestamp(pub i64);

impl Timestamp {
    /// Reads `DateTime.ToBinary()`'s value as an instant.
    pub fn from_binary(v: i64) -> Self {
        let mut ticks = v & TICKS_MASK;
        if v & LOCAL_MASK != 0 && ticks > MAX_TICKS {
            ticks -= TICKS_CEILING;
        }
        Timestamp(ticks)
    }

    /// The instant at `secs` and `nanos` past the Unix epoch, truncated to a tick.
    pub fn from_unix(secs: i64, nanos: u32) -> Self {
        Timestamp(UNIX_EPOCH_TICKS + secs * TICKS_PER_SECOND + i64::from(nanos) / 100)
    }

    /// The instant in Unix milliseconds.
    pub fn from_unix_millis(ms: i64) -> Self {
        Timestamp(UNIX_EPOCH_TICKS + ms * 10_000)
    }

    /// Seconds and nanoseconds past the Unix epoch; nanoseconds are never negative.
    pub fn to_unix(self) -> (i64, u32) {
        let t = self.0 - UNIX_EPOCH_TICKS;
        (
            t.div_euclid(TICKS_PER_SECOND),
            (t.rem_euclid(TICKS_PER_SECOND) * 100) as u32,
        )
    }

    pub fn unix_millis(self) -> i64 {
        (self.0 - UNIX_EPOCH_TICKS).div_euclid(10_000)
    }

    pub fn to_system_time(self) -> SystemTime {
        let (secs, nanos) = self.to_unix();
        if secs >= 0 {
            UNIX_EPOCH + Duration::new(secs as u64, nanos)
        } else {
            UNIX_EPOCH - Duration::new(secs.unsigned_abs(), 0) + Duration::new(0, nanos)
        }
    }

    pub fn from_system_time(t: SystemTime) -> Self {
        match t.duration_since(UNIX_EPOCH) {
            Ok(d) => Self::from_unix(d.as_secs() as i64, d.subsec_nanos()),
            Err(e) => {
                let d = e.duration();
                Timestamp(
                    UNIX_EPOCH_TICKS
                        - d.as_secs() as i64 * TICKS_PER_SECOND
                        - i64::from(d.subsec_nanos()) / 100,
                )
            }
        }
    }

    /// The UTC civil date and time: (year, month, day, hour, minute, second, nanosecond).
    pub fn to_civil(self) -> (i64, u32, u32, u32, u32, u32, u32) {
        let (secs, nanos) = self.to_unix();
        let (days, sod) = (
            secs.div_euclid(SECONDS_PER_DAY),
            secs.rem_euclid(SECONDS_PER_DAY),
        );
        let (y, m, d) = civil_from_days(days);
        (
            y,
            m,
            d,
            (sod / 3600) as u32,
            (sod / 60 % 60) as u32,
            (sod % 60) as u32,
            nanos,
        )
    }

    /// The instant at a UTC civil date and time.
    pub fn from_civil(y: i64, m: u32, d: u32, hh: u32, mm: u32, ss: u32) -> Self {
        let secs = days_from_civil(y, m, d) * SECONDS_PER_DAY + i64::from(hh * 3600 + mm * 60 + ss);
        Self::from_unix(secs, 0)
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (y, mo, d, h, mi, s, ns) = self.to_civil();
        write!(f, "{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}")?;
        if ns != 0 {
            write!(f, ".{:07}", ns / 100)?;
        }
        f.write_str("Z")
    }
}

impl fmt::Debug for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl Decode for Timestamp {
    fn decode(r: &mut Reader<'_>) -> Result<Self, Error> {
        match r.peek_kind()? {
            Kind::Nil => {
                r.try_nil()?;
                Ok(Timestamp::default())
            }
            Kind::Ext => {
                let (ty, b) = r.read_ext()?;
                if ty != -1 {
                    return Err(Error::new(format!(
                        "timestamp: extension type {ty} is not -1"
                    )));
                }
                // The three layouts of MessagePack's timestamp extension.
                let (secs, nanos) = match b.len() {
                    4 => (i64::from(u32::from_be_bytes(b.try_into().unwrap())), 0),
                    8 => {
                        let v = u64::from_be_bytes(b.try_into().unwrap());
                        ((v & 0x3_FFFF_FFFF) as i64, (v >> 34) as u32)
                    }
                    12 => (
                        i64::from_be_bytes(b[4..].try_into().unwrap()),
                        u32::from_be_bytes(b[..4].try_into().unwrap()),
                    ),
                    n => return Err(Error::new(format!("timestamp: extension of {n} bytes"))),
                };
                Ok(Timestamp::from_unix(secs, nanos))
            }
            _ => r
                .read_i64()
                .map(Timestamp::from_binary)
                .map_err(|e| Error::new(format!("timestamp: {e}"))),
        }
    }
}

/// A protop `date`, a .NET DateOnly: its day number, days since 0001-01-01,
/// which is what MessagePack-CSharp writes. An integer, so it sorts, hashes and
/// keys a range index as the date it is.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct Date(pub i32);

/// Days from 0001-01-01 to 1970-01-01.
const UNIX_EPOCH_DAYS: i64 = UNIX_EPOCH_TICKS / TICKS_PER_SECOND / SECONDS_PER_DAY;

impl Date {
    /// The civil date y-m-d.
    pub fn new(y: i64, m: u32, d: u32) -> Self {
        Date((days_from_civil(y, m, d) + UNIX_EPOCH_DAYS) as i32)
    }

    /// (year, month, day).
    pub fn to_civil(self) -> (i64, u32, u32) {
        civil_from_days(i64::from(self.0) - UNIX_EPOCH_DAYS)
    }

    /// The date's midnight, UTC.
    pub fn to_timestamp(self) -> Timestamp {
        Timestamp(i64::from(self.0) * SECONDS_PER_DAY * TICKS_PER_SECOND)
    }
}

impl fmt::Display for Date {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (y, m, d) = self.to_civil();
        write!(f, "{y:04}-{m:02}-{d:02}")
    }
}

impl fmt::Debug for Date {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl Decode for Date {
    fn decode(r: &mut Reader<'_>) -> Result<Self, Error> {
        if r.try_nil()? {
            return Ok(Date::default());
        }
        r.read_i32()
            .map(Date)
            .map_err(|e| Error::new(format!("date: {e}")))
    }
}

// Howard Hinnant's civil-date algorithms, days relative to 1970-01-01.
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let m = i64::from(m);
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + i64::from(d) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binary_kinds_read_as_one_instant() {
        let want = Timestamp::from_civil(2026, 1, 1, 0, 0, 0);
        assert_eq!(
            Timestamp::from_binary(5_250_714_242_427_387_904),
            want,
            "utc"
        );
        assert_eq!(
            Timestamp::from_binary(639_028_224_000_000_000),
            want,
            "unspecified"
        );
        assert_eq!(
            Timestamp::from_binary(639_028_224_000_000_000 | LOCAL_MASK),
            want,
            "local"
        );
        let wrapped =
            Timestamp::from_binary((TICKS_CEILING - 2 * 3600 * TICKS_PER_SECOND) | LOCAL_MASK);
        assert_eq!(wrapped.0, -2 * 3600 * TICKS_PER_SECOND);
    }

    #[test]
    fn extension_layouts() {
        let at = Timestamp::from_unix(1_783_197_000, 123_456_700);
        // timestamp 64: nanos << 34 | secs
        let v: u64 = (123_456_700u64 << 34) | 1_783_197_000;
        let mut b = vec![0xd7, 0xff];
        b.extend_from_slice(&v.to_be_bytes());
        assert_eq!(super::super::decode::<Timestamp>(&b).unwrap(), at);
        assert_eq!(at.to_string(), "2026-07-04T20:30:00.1234567Z");
    }

    #[test]
    fn dates() {
        let d = Date::new(1994, 5, 17);
        assert_eq!(d.0, 728_064);
        assert_eq!(d.to_string(), "1994-05-17");
        assert_eq!(Date::new(1, 1, 1).0, 0);
        assert_eq!(d.to_timestamp().to_civil(), (1994, 5, 17, 0, 0, 0, 0));
    }
}
