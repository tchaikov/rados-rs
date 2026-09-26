//! Renderings that match `ceph-dencoder`'s `dump_json` where `serde`'s
//! defaults do not.

#[cfg(any(feature = "user", feature = "rgw", feature = "lock"))]
use rados::UTime;
#[cfg(feature = "rgw")]
use serde::Serialize;
#[cfg(feature = "rgw")]
use serde::ser::SerializeSeq;

/// `encode_json` of a `utime_t` streams `utime_t::gmtime`: a count of
/// seconds below ten years prints as `<sec>.<usec>`, anything later as
/// ISO 8601 with six microsecond digits and a `Z`.
#[cfg(any(feature = "user", feature = "rgw"))]
pub(crate) fn utime<S: serde::Serializer>(
    t: &UTime,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    serializer.serialize_str(&gmtime(t))
}

/// `utime_t::gmtime` with `legacy_form` false.
#[cfg(any(feature = "user", feature = "rgw"))]
pub(crate) fn gmtime(t: &UTime) -> String {
    let usec = t.nsec / 1000;
    if t.sec < 315_360_000 {
        return format!("{}.{usec:06}", t.sec);
    }
    let days = i64::from(t.sec / 86_400);
    let secs = t.sec % 86_400;
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{usec:06}Z",
        secs / 3600,
        (secs % 3600) / 60,
        secs % 60
    )
}

/// Days since 1970-01-01 to a proleptic Gregorian `(year, month, day)`;
/// Howard Hinnant's `civil_from_days`.
#[cfg(any(feature = "user", feature = "rgw", feature = "lock"))]
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

/// `dump_int((int)b)`: a bool printed as `0` or `1`.
#[cfg(any(feature = "refcount", feature = "rgw"))]
pub(crate) fn bool_as_int<S: serde::Serializer>(b: &bool, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_u8(u8::from(*b))
}

/// `utime_t::gmtime_nsec`: as [`gmtime`], with nine fraction digits in
/// the calendar form (the below-ten-years form keeps six, as `gmtime`
/// prints it). `rgw_bi_log_entry::timestamp` is the one field in this
/// header that streams a `utime_t` this way rather than through
/// [`utime`].
#[cfg(feature = "rgw")]
pub(crate) fn gmtime_nsec(t: &UTime) -> String {
    if t.sec < 315_360_000 {
        return format!("{}.{:06}", t.sec, t.nsec / 1000);
    }
    let days = i64::from(t.sec / 86_400);
    let secs = t.sec % 86_400;
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:09}Z",
        secs / 3600,
        (secs % 3600) / 60,
        secs % 60,
        t.nsec
    )
}

#[cfg(feature = "rgw")]
pub(crate) fn utime_nsec<S: serde::Serializer>(
    t: &UTime,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    serializer.serialize_str(&gmtime_nsec(t))
}

/// `encode_json` of a `std::map`/`std::multimap`: an array of
/// `{"key": k, "val": v}` objects, in iteration order.
#[cfg(feature = "rgw")]
pub(crate) fn map_entries<'a, K, V, I, S>(entries: I, s: S) -> std::result::Result<S::Ok, S::Error>
where
    K: Serialize + 'a,
    V: Serialize + 'a,
    I: IntoIterator<Item = (&'a K, &'a V)>,
    S: serde::Serializer,
{
    #[derive(Serialize)]
    struct Entry<'a, K, V> {
        key: &'a K,
        val: &'a V,
    }

    let mut seq = s.serialize_seq(None)?;
    for (key, val) in entries {
        seq.serialize_element(&Entry { key, val })?;
    }
    seq.end()
}

/// A `real_time` that `dump` streams with `operator<<`: the calendar form
/// with microseconds and the zone offset, which is `+0000` where
/// `ceph-dencoder` runs.
#[cfg(feature = "rgw")]
pub(crate) fn real_time<S: serde::Serializer>(t: &UTime, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&iso_utc(t))
}

#[cfg(any(feature = "rgw", feature = "lock"))]
pub(crate) fn iso_utc(t: &UTime) -> String {
    let usec = t.nsec / 1000;
    let (year, month, day) = civil_from_days(i64::from(t.sec / 86_400));
    let secs = t.sec % 86_400;
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{usec:06}+0000",
        secs / 3600,
        (secs % 3600) / 60,
        secs % 60
    )
}

/// A `utime_t` streamed with `operator<<`: `utime_t::localtime`, which
/// prints a count of seconds below ten years as `<sec>.<usec>` and
/// anything later in the calendar form with the zone's `%z`, `+0000`
/// where `ceph-dencoder` runs.
#[cfg(feature = "lock")]
pub(crate) fn localtime(t: &UTime) -> String {
    if t.sec < 315_360_000 {
        return format!("{}.{:06}", t.sec, t.nsec / 1000);
    }
    iso_utc(t)
}

#[cfg(feature = "lock")]
pub(crate) fn utime_localtime<S: serde::Serializer>(
    t: &UTime,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    serializer.serialize_str(&localtime(t))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(feature = "user")]
    fn short_times_print_as_seconds_and_microseconds() {
        // utime_t::gmtime: below ten years of seconds it prints the raw count.
        assert_eq!(gmtime(&UTime { sec: 1, nsec: 0 }), "1.000000");
        assert_eq!(
            gmtime(&UTime {
                sec: 12345,
                nsec: 0
            }),
            "12345.000000"
        );
        assert_eq!(
            gmtime(&UTime {
                sec: 0,
                nsec: 999_999_999
            }),
            "0.999999"
        );
    }

    #[test]
    #[cfg(feature = "user")]
    fn absolute_times_print_as_iso_8601_with_microseconds() {
        // A cls_user_bucket_entry corpus sample: 0x66f91c3f seconds,
        // 753627000 nanoseconds, which ceph-dencoder dumps as below.
        assert_eq!(
            gmtime(&UTime {
                sec: 0x66f9_1c3f,
                nsec: 753_627_000
            }),
            "2024-09-29T09:22:07.753627Z"
        );
        assert_eq!(
            gmtime(&UTime {
                sec: 315_360_000,
                nsec: 0
            }),
            "1979-12-30T00:00:00.000000Z"
        );
        assert_eq!(
            gmtime(&UTime {
                sec: u32::MAX,
                nsec: 0
            }),
            "2106-02-07T06:28:15.000000Z"
        );
    }

    #[test]
    #[cfg(feature = "user")]
    fn civil_dates_match_known_days() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(-1), (1969, 12, 31));
        assert_eq!(civil_from_days(11_016), (2000, 2, 29));
        assert_eq!(civil_from_days(19_995), (2024, 9, 29));
    }

    #[test]
    #[cfg(feature = "user")]
    fn serializes_as_a_json_string() {
        #[derive(serde::Serialize)]
        struct T {
            #[serde(serialize_with = "utime")]
            t: UTime,
        }
        assert_eq!(
            serde_json::to_value(T {
                t: UTime {
                    sec: 12345,
                    nsec: 0
                }
            })
            .expect("json"),
            serde_json::json!({"t": "12345.000000"})
        );
    }

    #[test]
    #[cfg(feature = "refcount")]
    fn bool_as_int_prints_zero_or_one() {
        #[derive(serde::Serialize)]
        struct T(#[serde(serialize_with = "bool_as_int")] bool);
        assert_eq!(serde_json::to_string(&T(true)).expect("json"), "1");
        assert_eq!(serde_json::to_string(&T(false)).expect("json"), "0");
    }

    #[test]
    #[cfg(feature = "rgw")]
    fn streamed_real_time_is_iso_with_a_numeric_offset() {
        // operator<<(ostream&, real_time): calendar form even at the epoch,
        // microseconds, and the zone's %z, which is +0000 where dencoder runs.
        assert_eq!(
            iso_utc(&UTime { sec: 0, nsec: 0 }),
            "1970-01-01T00:00:00.000000+0000"
        );
        assert_eq!(
            iso_utc(&UTime { sec: 21, nsec: 32 }),
            "1970-01-01T00:00:21.000000+0000"
        );
        assert_eq!(
            iso_utc(&UTime {
                sec: 1_727_611_205,
                nsec: 747_275_000
            }),
            "2024-09-29T12:00:05.747275+0000"
        );
    }

    #[test]
    #[cfg(feature = "rgw")]
    fn gmtime_nsec_keeps_six_digits_below_ten_years_and_nine_past_it() {
        // rgw_bi_log_entry's oracle instance: {2 s, 3 ns}.
        assert_eq!(gmtime_nsec(&UTime { sec: 2, nsec: 3 }), "2.000000");
        assert_eq!(
            gmtime_nsec(&UTime {
                sec: 1_727_611_205,
                nsec: 747_275_123
            }),
            "2024-09-29T12:00:05.747275123Z"
        );
    }

    #[test]
    #[cfg(feature = "rgw")]
    fn map_entries_dumps_key_val_objects() {
        #[derive(Serialize)]
        struct Wrapper<'a>(#[serde(serialize_with = "map_entries_field")] &'a [(String, u32)]);

        fn map_entries_field<S: serde::Serializer>(
            m: &[(String, u32)],
            s: S,
        ) -> std::result::Result<S::Ok, S::Error> {
            map_entries(m.iter().map(|(k, v)| (k, v)), s)
        }

        let w = Wrapper(&[("a".to_owned(), 1), ("b".to_owned(), 2)]);
        assert_eq!(
            serde_json::to_string(&w).expect("json"),
            r#"[{"key":"a","val":1},{"key":"b","val":2}]"#
        );
    }

    #[test]
    #[cfg(feature = "lock")]
    fn localtime_switches_to_the_calendar_at_ten_years() {
        assert_eq!(localtime(&UTime::new(5, 0)), "5.000000");
        assert_eq!(localtime(&UTime::new(0, 0)), "0.000000");
        assert_eq!(
            localtime(&UTime::new(315_359_999, 999_999_999)),
            "315359999.999999"
        );
        assert_eq!(
            localtime(&UTime::new(315_360_000, 0)),
            "1979-12-30T00:00:00.000000+0000"
        );
        assert_eq!(
            localtime(&UTime::new(1_727_604_086, 460_555_954)),
            "2024-09-29T10:01:26.460555+0000"
        );
    }
}
