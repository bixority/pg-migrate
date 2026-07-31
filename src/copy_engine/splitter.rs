use crate::copy_engine::error::{CopyEngineError, Result};
use chrono::{DateTime, Utc};
use std::fmt::Display;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Partition {
    pub from: Option<String>,
    pub till: Option<String>,
    pub column: Arc<str>,
    pub method: Arc<str>,
}

impl Display for Partition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if &*self.method == "hash" {
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
        let column: Arc<str> = column.into();
        let method_arc: Arc<str> = method.unwrap_or("time").into();
        match method {
            Some("hash") => return Ok(Self::split_hash(&column, num_partitions)),
            Some("date" | "day") => {
                if let Some(f) = from {
                    let (upper, open_last) = resolve_upper(till);
                    return Self::split_by_date(column, method_arc, f, &upper, open_last);
                }
            }
            _ => {}
        }

        let Some(f) = from else {
            return Ok(vec![Partition {
                column,
                from: None,
                till: till.map(str::to_string),
                method: method_arc,
            }]);
        };
        let (upper, open_last) = resolve_upper(till);
        Self::split_time_range(column, method_arc, f, &upper, num_partitions, open_last)
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
        column: Arc<str>,
        method: Arc<str>,
        from_ts: &str,
        till_ts: &str,
        num_partitions: usize,
        open_last: bool,
    ) -> Result<Vec<Partition>> {
        if num_partitions == 0 {
            return Err(CopyEngineError::Splitter(
                "Number of partitions must be greater than 0".into(),
            ));
        }

        let num_p_i64 = i64::try_from(num_partitions)
            .map_err(|_| CopyEngineError::Splitter("Number of partitions is too large".into()))?;

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
                    column,
                    from: Some(from.to_rfc3339()),
                    till: None,
                    method,
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
                column,
                from: Some(from.to_rfc3339()),
                till: last_till,
                method,
            }]);
        }

        let mut partitions = Vec::with_capacity(num_partitions);
        for i in 0..num_partitions {
            let i_i64 = i64::try_from(i)
                .map_err(|_| CopyEngineError::Splitter("Partition index is too large".into()))?;

            let p_from = from + chrono::Duration::milliseconds(step_ms * i_i64);
            let p_till = if i == num_partitions - 1 {
                last_till.clone()
            } else {
                Some((from + chrono::Duration::milliseconds(step_ms * (i_i64 + 1))).to_rfc3339())
            };

            partitions.push(Partition {
                column: column.clone(),
                from: Some(p_from.to_rfc3339()),
                till: p_till,
                method: method.clone(),
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
        column: Arc<str>,
        method: Arc<str>,
        from_ts: &str,
        till_ts: &str,
        open_last: bool,
    ) -> Result<Vec<Partition>> {
        let from = parse_ts(from_ts)?;
        let till = parse_ts(till_ts)?;

        if from >= till {
            if open_last {
                return Ok(vec![Partition {
                    column,
                    from: Some(from.to_rfc3339()),
                    till: None,
                    method,
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
                column: column.clone(),
                from: Some(cursor.to_rfc3339()),
                till: if is_last && open_last {
                    None
                } else {
                    Some(p_till.to_rfc3339())
                },
                method: method.clone(),
            });
            cursor = p_till;
        }

        Ok(partitions)
    }

    #[must_use]
    pub fn split_hash(column: &Arc<str>, num_partitions: usize) -> Vec<Partition> {
        let method: Arc<str> = "hash".into();
        (0..num_partitions)
            .map(|i| Partition {
                column: column.clone(),
                from: Some(i.to_string()),
                till: Some(num_partitions.to_string()),
                method: method.clone(),
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
            CopyEngineError::Splitter(
                format!("Timestamp '{dt}' is too large to advance to the next day").into(),
            )
        })?;
    Ok(DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc))
}

pub fn parse_ts(ts: &str) -> Result<DateTime<Utc>> {
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
    ).into()))
}

