//! Renderings that match `ceph-dencoder`'s `dump_json` where `serde`'s
//! defaults do not.

use rados::UTime;

/// `encode_json` of a `utime_t` streams `utime_t::gmtime`: a count of
/// seconds below ten years prints as `<sec>.<usec>`, anything later as
/// ISO 8601 with six microsecond digits and a `Z`.
pub(crate) fn utime<S: serde::Serializer>(
    t: &UTime,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    serializer.serialize_str(&gmtime(t))
}

/// `utime_t::gmtime` with `legacy_form` false.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
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
    fn civil_dates_match_known_days() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(-1), (1969, 12, 31));
        assert_eq!(civil_from_days(11_016), (2000, 2, 29));
        assert_eq!(civil_from_days(19_995), (2024, 9, 29));
    }

    #[test]
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
}
