use crate::copy_engine::error::{CopyEngineError, Result};
use chrono::{DateTime, Utc};
use std::fmt::Display;

#[derive(Debug, Clone)]
pub struct Partition {
    pub from: Option<String>,
    pub till: Option<String>,
    pub column: String,
    pub method: String,
}

impl Display for Partition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.method == "hash" {
            let i = self.from.as_deref().unwrap_or("?");
            let n = self.till.as_deref().unwrap_or("?");
            return write!(f, "{} [hash {}/{}]", self.column, i, n);
        }
        let from = self.from.as_deref().unwrap_or("-∞");
        let till = self.till.as_deref().unwrap_or("+∞");
        write!(f, "{} [{} - {})", self.column, from, till)
    }
}

pub struct Splitter;

impl Splitter {
    /// Produces partitions for `column` given a `from` (inclusive) lower bound, an optional
    /// `till` (exclusive) upper bound, and an optional `method`.
    ///
    /// When `method` is "hash", it uses hash-based partitioning.
    /// For "date"/"day" and "time" methods the upper endpoint is only needed to place the
    /// interior split boundaries: when `till` is absent the current time stands in as the
    /// endpoint so the range can still be divided for parallelism, and the final partition is
    /// left open-ended (`column >= x`) so rows at or beyond it — including ones inserted after
    /// planning — are still copied. "date"/"day" splits one partition per calendar day (UTC);
    /// "time" splits into up to `num_partitions` sub-ranges.
    /// When no `from` is available a single partition is returned.
    ///
    /// # Errors
    ///
    /// Returns an error if `num_partitions` is 0 or if a bound cannot be parsed as a
    /// timestamp.
    pub fn split(
        column: &str,
        from: Option<&str>,
        till: Option<&str>,
        method: Option<&str>,
        num_partitions: usize,
    ) -> Result<Vec<Partition>> {
        match method {
            Some("hash") => return Ok(Self::split_hash(column, num_partitions)),
            Some("date" | "day") => {
                if let Some(f) = from {
                    let (upper, open_last) = resolve_upper(till);
                    return Self::split_by_date(column, f, &upper, open_last);
                }
            }
            _ => {}
        }

        let Some(f) = from else {
            return Ok(vec![Partition {
                column: column.to_string(),
                from: None,
                till: till.map(str::to_string),
                method: method.unwrap_or("time").to_string(),
            }]);
        };
        let (upper, open_last) = resolve_upper(till);
        Self::split_time_range(column, f, &upper, num_partitions, open_last)
    }

    /// Creates time-based partitions by dividing the interval [`from_ts`, `till_ts`) into
    /// `num_partitions`.
    ///
    /// When `open_last` is set, `till_ts` is treated as a splitting endpoint only: the final
    /// partition is left open-ended (`column >= x` with no upper bound) so rows at or beyond
    /// it are still captured.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `num_partitions` is 0.
    /// - Timestamps cannot be parsed.
    pub fn split_time_range(
        column: &str,
        from_ts: &str,
        till_ts: &str,
        num_partitions: usize,
        open_last: bool,
    ) -> Result<Vec<Partition>> {
        if num_partitions == 0 {
            return Err(CopyEngineError::Splitter(
                "Number of partitions must be greater than 0".to_string(),
            ));
        }

        let num_p_i64 = i64::try_from(num_partitions).map_err(|_| {
            CopyEngineError::Splitter("Number of partitions is too large".to_string())
        })?;

        let from = parse_ts(from_ts)?;
        let till = parse_ts(till_ts)?;

        // Closed upper bound for the final partition, or `None` when it stays open.
        let last_till = if open_last {
            None
        } else {
            Some(till.to_rfc3339())
        };

        if from >= till {
            // The upper endpoint is at or before the start (e.g. a synthesized "now" bound
            // with future-dated rows). With an open tail, still emit one partition so the
            // data is captured; otherwise the range is genuinely empty.
            if open_last {
                return Ok(vec![Partition {
                    column: column.to_string(),
                    from: Some(from.to_rfc3339()),
                    till: None,
                    method: "time".to_string(),
                }]);
            }
            return Ok(vec![]);
        }

        let duration = till.signed_duration_since(from);
        let total_ms = duration.num_milliseconds();
        let step_ms = total_ms / num_p_i64;

        if step_ms <= 0 {
            // Range is too small to split further; return a single partition covering the whole range
            return Ok(vec![Partition {
                column: column.to_string(),
                from: Some(from.to_rfc3339()),
                till: last_till,
                method: "time".to_string(),
            }]);
        }

        let mut partitions = Vec::with_capacity(num_partitions);
        for i in 0..num_partitions {
            let i_i64 = i64::try_from(i).map_err(|_| {
                CopyEngineError::Splitter("Partition index is too large".to_string())
            })?;

            let p_from = from + chrono::Duration::milliseconds(step_ms * i_i64);
            let p_till = if i == num_partitions - 1 {
                last_till.clone()
            } else {
                Some((from + chrono::Duration::milliseconds(step_ms * (i_i64 + 1))).to_rfc3339())
            };

            partitions.push(Partition {
                column: column.to_string(),
                from: Some(p_from.to_rfc3339()),
                till: p_till,
                method: "time".to_string(),
            });
        }

        Ok(partitions)
    }

    /// Creates one partition per calendar day (UTC) covering [`from_ts`, `till_ts`).
    ///
    /// Boundaries are aligned to UTC midnight, except the first partition starts at
    /// `from_ts` and the last ends at `till_ts`, so no edge rows are missed. The number
    /// of partitions therefore follows the span of the range, not `num_partitions`;
    /// concurrency is still bounded by the orchestrator's worker pool.
    ///
    /// When `open_last` is set, `till_ts` is treated as a splitting endpoint only: the final
    /// partition is left open-ended (`column >= x`) so rows at or beyond it are still captured.
    ///
    /// # Errors
    ///
    /// Returns an error if either timestamp cannot be parsed.
    pub fn split_by_date(
        column: &str,
        from_ts: &str,
        till_ts: &str,
        open_last: bool,
    ) -> Result<Vec<Partition>> {
        let from = parse_ts(from_ts)?;
        let till = parse_ts(till_ts)?;

        if from >= till {
            if open_last {
                return Ok(vec![Partition {
                    column: column.to_string(),
                    from: Some(from.to_rfc3339()),
                    till: None,
                    method: "date".to_string(),
                }]);
            }
            return Ok(vec![]);
        }

        let mut partitions = Vec::new();
        let mut cursor = from;
        while cursor < till {
            let p_till = next_utc_midnight(cursor)?.min(till);
            let is_last = p_till >= till;
            partitions.push(Partition {
                column: column.to_string(),
                from: Some(cursor.to_rfc3339()),
                till: if is_last && open_last {
                    None
                } else {
                    Some(p_till.to_rfc3339())
                },
                method: "date".to_string(),
            });
            cursor = p_till;
        }

        Ok(partitions)
    }

    #[must_use]
    pub fn split_hash(column: &str, num_partitions: usize) -> Vec<Partition> {
        (0..num_partitions)
            .map(|i| Partition {
                column: column.to_string(),
                from: Some(i.to_string()),
                till: Some(num_partitions.to_string()),
                method: "hash".to_string(),
            })
            .collect()
    }
}

/// Resolves the upper splitting endpoint for time/date methods.
///
/// A configured `till` is used as a closed upper bound (`open_last` = false). When it is
/// absent the current time stands in as the endpoint so the range can still be divided for
/// parallelism, and the caller leaves the final partition open-ended (`open_last` = true) so
/// rows at or beyond it — including ones inserted after planning — are still copied.
fn resolve_upper(till: Option<&str>) -> (String, bool) {
    till.map_or_else(
        || (Utc::now().to_rfc3339(), true),
        |t| (t.to_string(), false),
    )
}

/// Returns the first UTC midnight strictly after `dt`.
///
/// # Errors
///
/// Returns an error if advancing past `dt` overflows the representable date range.
fn next_utc_midnight(dt: DateTime<Utc>) -> Result<DateTime<Utc>> {
    let naive = dt
        .date_naive()
        .checked_add_days(chrono::Days::new(1))
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .ok_or_else(|| {
            CopyEngineError::Splitter(format!(
                "Timestamp '{dt}' is too large to advance to the next day"
            ))
        })?;
    Ok(DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc))
}

fn parse_ts(ts: &str) -> Result<DateTime<Utc>> {
    // Try RFC3339 (e.g., 2023-01-01T00:00:00Z)
    if let Ok(dt) = DateTime::parse_from_rfc3339(ts) {
        return Ok(dt.with_timezone(&Utc));
    }

    // Try Postgres timestamptz text output, e.g. "2026-05-25 00:12:12.265033+03"
    // (space separator, optional fractional seconds, hour-only offset). The `%#z`
    // modifier accepts loose offsets like "+03", "+0300", and "+03:00".
    if let Ok(dt) = DateTime::parse_from_str(ts, "%Y-%m-%d %H:%M:%S%.f%#z") {
        return Ok(dt.with_timezone(&Utc));
    }

    // Try YYYY-MM-DD HH:MM:SS[.f] (no timezone; assume UTC)
    if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(ts, "%Y-%m-%d %H:%M:%S%.f") {
        return Ok(DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc));
    }

    // Try YYYY-MM-DD
    if let Some(dt) = chrono::NaiveDate::parse_from_str(ts, "%Y-%m-%d")
        .ok()
        .and_then(|d| d.and_hms_opt(0, 0, 0))
    {
        return Ok(DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc));
    }

    Err(CopyEngineError::Splitter(format!(
        "Invalid timestamp format: '{ts}'. Supported: RFC3339, 'YYYY-MM-DD HH:MM:SS[.f]', 'YYYY-MM-DD'"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_display_bounded() {
        let p = Partition {
            column: "created_at".to_string(),
            from: Some("2023-01-01".to_string()),
            till: Some("2023-01-02".to_string()),
            method: "time".to_string(),
        };
        assert_eq!(format!("{p}"), "created_at [2023-01-01 - 2023-01-02)");
    }

    #[test]
    fn test_partition_display_unbounded() {
        let p = Partition {
            column: "created_at".to_string(),
            from: None,
            till: None,
            method: "time".to_string(),
        };
        assert_eq!(format!("{p}"), "created_at [-∞ - +∞)");
    }

    #[test]
    fn test_partition_display_half_open() {
        let from_only = Partition {
            column: "ts".to_string(),
            from: Some("2023-01-01".to_string()),
            till: None,
            method: "time".to_string(),
        };
        assert_eq!(format!("{from_only}"), "ts [2023-01-01 - +∞)");

        let till_only = Partition {
            column: "ts".to_string(),
            from: None,
            till: Some("2024-01-01".to_string()),
            method: "time".to_string(),
        };
        assert_eq!(format!("{till_only}"), "ts [-∞ - 2024-01-01)");
    }

    #[test]
    fn test_split_both_bounds() -> Result<()> {
        let partitions = Splitter::split("col", Some("2023-01-01"), Some("2023-01-10"), None, 4)?;
        assert_eq!(partitions.len(), 4);
        assert!(
            partitions[0]
                .from
                .as_deref()
                .unwrap_or("")
                .contains("2023-01-01")
        );
        assert!(
            partitions[3]
                .till
                .as_deref()
                .unwrap_or("")
                .contains("2023-01-10")
        );
        Ok(())
    }

    #[test]
    fn test_split_from_only() -> Result<()> {
        // With only a lower bound, the upper endpoint is synthesized from the current time so
        // the range is still split for parallelism, and the final partition is left
        // open-ended (column >= x) to capture the tail, including rows added after planning.
        let partitions = Splitter::split("col", Some("2023-01-01"), None, None, 4)?;
        assert_eq!(partitions.len(), 4);
        assert!(
            partitions[0]
                .from
                .as_deref()
                .unwrap_or("")
                .contains("2023-01-01")
        );
        assert!(partitions[3].till.is_none());
        Ok(())
    }

    #[test]
    fn test_split_till_only() -> Result<()> {
        let partitions = Splitter::split("col", None, Some("2024-01-01"), None, 4)?;
        assert_eq!(partitions.len(), 1);
        assert!(partitions[0].from.is_none());
        assert_eq!(partitions[0].till.as_deref(), Some("2024-01-01"));
        Ok(())
    }

    #[test]
    fn test_split_no_bounds() -> Result<()> {
        let partitions = Splitter::split("col", None, None, None, 4)?;
        assert_eq!(partitions.len(), 1);
        assert!(partitions[0].from.is_none());
        assert!(partitions[0].till.is_none());
        Ok(())
    }

    #[test]
    fn test_split_time_range() -> Result<()> {
        let partitions = Splitter::split_time_range("col", "2023-01-01", "2023-01-10", 4, false)?;
        assert_eq!(partitions.len(), 4);
        assert!(
            partitions[0]
                .from
                .as_deref()
                .unwrap_or("")
                .contains("2023-01-01")
        );
        assert!(
            partitions[3]
                .till
                .as_deref()
                .unwrap_or("")
                .contains("2023-01-10")
        );
        Ok(())
    }

    #[test]
    fn test_split_time_range_open_last() -> Result<()> {
        // open_last treats the upper timestamp as a splitting endpoint only: interior
        // partitions stay bounded, but the final one is left open (column >= x).
        let partitions = Splitter::split_time_range("col", "2023-01-01", "2023-01-10", 4, true)?;
        assert_eq!(partitions.len(), 4);
        assert!(partitions[2].till.is_some());
        assert!(partitions[3].till.is_none());
        assert!(
            partitions[3]
                .from
                .as_deref()
                .unwrap_or("")
                .contains("2023-01-")
        );
        Ok(())
    }

    #[test]
    fn test_split_time_range_open_last_reversed() -> Result<()> {
        // A synthesized endpoint at or before the start still yields a single open partition
        // so the data is captured rather than silently dropped.
        let partitions = Splitter::split_time_range("col", "2023-01-10", "2023-01-01", 4, true)?;
        assert_eq!(partitions.len(), 1);
        assert!(partitions[0].till.is_none());
        Ok(())
    }

    #[test]
    fn test_split_by_date_open_last() -> Result<()> {
        let partitions = Splitter::split_by_date("ts", "2023-01-01", "2023-01-04", true)?;
        assert_eq!(partitions.len(), 3);
        assert!(partitions[1].till.is_some());
        assert!(partitions[2].till.is_none());
        Ok(())
    }

    #[test]
    fn test_split_time_range_invalid_format() {
        let result = Splitter::split_time_range("col", "invalid", "2023-01-10", 4, false);
        assert!(result.is_err());
    }

    #[test]
    fn test_split_time_range_empty() -> Result<()> {
        let partitions = Splitter::split_time_range("col", "2023-01-10", "2023-01-01", 4, false)?;
        assert_eq!(partitions.len(), 0);
        Ok(())
    }

    #[test]
    fn test_parse_ts_fractional_seconds() -> Result<()> {
        use chrono::Datelike;
        use chrono::Timelike;
        let ts = "2026-05-06 23:57:24.48";
        let dt = parse_ts(ts)?;
        assert_eq!(dt.year(), 2026);
        assert_eq!(dt.month(), 5);
        assert_eq!(dt.day(), 6);
        assert_eq!(dt.hour(), 23);
        assert_eq!(dt.minute(), 57);
        assert_eq!(dt.second(), 24);
        assert_eq!(dt.nanosecond(), 480_000_000);

        let ts2 = "2026-05-06 23:57:24";
        let dt2 = parse_ts(ts2)?;
        assert_eq!(dt2.second(), 24);
        assert_eq!(dt2.nanosecond(), 0);
        Ok(())
    }

    #[test]
    fn test_parse_ts_pg_timestamptz_offset() -> Result<()> {
        use chrono::Timelike;
        // Postgres timestamptz text output: space separator, hour-only offset.
        let dt = parse_ts("2026-05-25 00:12:12.265033+03")?;
        // 00:12 at +03 is 21:12 UTC the previous day.
        assert_eq!(dt.hour(), 21);
        assert_eq!(dt.minute(), 12);
        assert_eq!(dt.second(), 12);
        assert_eq!(dt.nanosecond(), 265_033_000);

        // Loose offset variants should all resolve to the same instant.
        let compact = parse_ts("2026-05-25 00:12:12+0300")?;
        let colon = parse_ts("2026-05-25 00:12:12+03:00")?;
        let hour_only = parse_ts("2026-05-25 00:12:12+03")?;
        assert_eq!(compact, colon);
        assert_eq!(colon, hour_only);
        assert_eq!(compact.hour(), 21);
        Ok(())
    }

    #[test]
    fn test_split_by_date_whole_days() -> Result<()> {
        let partitions = Splitter::split(
            "ts",
            Some("2023-01-01"),
            Some("2023-01-04"),
            Some("date"),
            4,
        )?;
        // 3 full days: [01, 02), [02, 03), [03, 04)
        assert_eq!(partitions.len(), 3);
        assert!(partitions.iter().all(|p| p.method == "date"));
        assert!(
            partitions[0]
                .from
                .as_deref()
                .unwrap_or("")
                .starts_with("2023-01-01T00:00:00")
        );
        assert!(
            partitions[0]
                .till
                .as_deref()
                .unwrap_or("")
                .starts_with("2023-01-02T00:00:00")
        );
        assert!(
            partitions[2]
                .till
                .as_deref()
                .unwrap_or("")
                .starts_with("2023-01-04T00:00:00")
        );
        Ok(())
    }

    #[test]
    fn test_split_by_date_partial_edges() -> Result<()> {
        // Range that does not start or end on a midnight boundary.
        let partitions =
            Splitter::split_by_date("ts", "2023-01-01 06:00:00", "2023-01-03 09:00:00", false)?;
        // [06:00, day2 00:00), [day2 00:00, day3 00:00), [day3 00:00, 09:00)
        assert_eq!(partitions.len(), 3);
        assert!(
            partitions[0]
                .from
                .as_deref()
                .unwrap_or("")
                .starts_with("2023-01-01T06:00:00")
        );
        assert!(
            partitions[0]
                .till
                .as_deref()
                .unwrap_or("")
                .starts_with("2023-01-02T00:00:00")
        );
        assert!(
            partitions[2]
                .from
                .as_deref()
                .unwrap_or("")
                .starts_with("2023-01-03T00:00:00")
        );
        assert!(
            partitions[2]
                .till
                .as_deref()
                .unwrap_or("")
                .starts_with("2023-01-03T09:00:00")
        );
        // Partitions are contiguous and non-overlapping.
        for w in partitions.windows(2) {
            assert_eq!(w[0].till, w[1].from);
        }
        Ok(())
    }

    #[test]
    fn test_split_by_date_day_alias() -> Result<()> {
        let partitions =
            Splitter::split("ts", Some("2023-01-01"), Some("2023-01-03"), Some("day"), 4)?;
        assert_eq!(partitions.len(), 2);
        assert_eq!(partitions[0].method, "date");
        Ok(())
    }

    #[test]
    fn test_split_by_date_empty_range() -> Result<()> {
        let partitions = Splitter::split_by_date("ts", "2023-01-05", "2023-01-01", false)?;
        assert!(partitions.is_empty());
        Ok(())
    }

    #[test]
    fn test_split_hash() -> Result<()> {
        let partitions = Splitter::split("col", None, None, Some("hash"), 3)?;
        assert_eq!(partitions.len(), 3);
        assert_eq!(partitions[0].from.as_deref(), Some("0"));
        assert_eq!(partitions[0].till.as_deref(), Some("3"));
        assert_eq!(partitions[0].method, "hash");
        assert_eq!(partitions[1].from.as_deref(), Some("1"));
        assert_eq!(partitions[1].till.as_deref(), Some("3"));
        assert_eq!(partitions[2].from.as_deref(), Some("2"));
        assert_eq!(partitions[2].till.as_deref(), Some("3"));
        Ok(())
    }
}
