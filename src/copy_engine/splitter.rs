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
    /// Produces partitions for `column` given optional `from` (inclusive) and `till` (exclusive)
    /// bounds and an optional `method`.
    ///
    /// When `method` is "hash", it uses hash-based partitioning.
    /// Otherwise, when both `from` and `till` are provided, the range is split into up to
    /// `num_partitions` time-based sub-ranges.
    /// In all other cases a single partition is returned.
    ///
    /// # Errors
    ///
    /// Returns an error if `num_partitions` is 0 or if either bound cannot be parsed as a
    /// timestamp.
    pub fn split(
        column: &str,
        from: Option<&str>,
        till: Option<&str>,
        method: Option<&str>,
        num_partitions: usize,
    ) -> Result<Vec<Partition>> {
        if method == Some("hash") {
            return Ok(Self::split_hash(column, num_partitions));
        }

        match (from, till) {
            (Some(f), Some(t)) => Self::split_time_range(column, f, t, num_partitions),
            _ => Ok(vec![Partition {
                column: column.to_string(),
                from: from.map(str::to_string),
                till: till.map(str::to_string),
                method: method.unwrap_or("time").to_string(),
            }]),
        }
    }

    /// Creates time-based partitions by dividing the interval [`from_ts`, `till_ts`) into
    /// `num_partitions`.
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

        if from >= till {
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
                till: Some(till.to_rfc3339()),
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
                till
            } else {
                from + chrono::Duration::milliseconds(step_ms * (i_i64 + 1))
            };

            partitions.push(Partition {
                column: column.to_string(),
                from: Some(p_from.to_rfc3339()),
                till: Some(p_till.to_rfc3339()),
                method: "time".to_string(),
            });
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

fn parse_ts(ts: &str) -> Result<DateTime<Utc>> {
    // Try RFC3339 (e.g., 2023-01-01T00:00:00Z)
    if let Ok(dt) = DateTime::parse_from_rfc3339(ts) {
        return Ok(dt.with_timezone(&Utc));
    }

    // Try YYYY-MM-DD HH:MM:SS[.f]
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
        let partitions = Splitter::split("col", Some("2023-01-01"), None, None, 4)?;
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].from.as_deref(), Some("2023-01-01"));
        assert!(partitions[0].till.is_none());
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
        let partitions = Splitter::split_time_range("col", "2023-01-01", "2023-01-10", 4)?;
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
    fn test_split_time_range_invalid_format() {
        let result = Splitter::split_time_range("col", "invalid", "2023-01-10", 4);
        assert!(result.is_err());
    }

    #[test]
    fn test_split_time_range_empty() -> Result<()> {
        let partitions = Splitter::split_time_range("col", "2023-01-10", "2023-01-01", 4)?;
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
