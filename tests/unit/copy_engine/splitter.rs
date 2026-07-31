use pg_migrate::copy_engine::splitter::{Partition, Splitter, parse_ts};
use pg_migrate::copy_engine::Result;

#[test]
fn test_partition_display_bounded() {
    let p = Partition {
        column: "created_at".into(),
        from: Some("2023-01-01".to_string()),
        till: Some("2023-01-02".to_string()),
        method: "time".into(),
    };
    assert_eq!(format!("{p}"), "created_at [2023-01-01 - 2023-01-02)");
}

#[test]
fn test_partition_display_unbounded() {
    let p = Partition {
        column: "created_at".into(),
        from: None,
        till: None,
        method: "time".into(),
    };
    assert_eq!(format!("{p}"), "created_at [-∞ - +∞)");
}

#[test]
fn test_partition_display_half_open() {
    let from_only = Partition {
        column: "ts".into(),
        from: Some("2023-01-01".to_string()),
        till: None,
        method: "time".into(),
    };
    assert_eq!(format!("{from_only}"), "ts [2023-01-01 - +∞)");

    let till_only = Partition {
        column: "ts".into(),
        from: None,
        till: Some("2024-01-01".to_string()),
        method: "time".into(),
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
    let partitions = Splitter::split_time_range(
        "col".into(),
        "time".into(),
        "2023-01-01",
        "2023-01-10",
        4,
        false,
    )?;
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
    let partitions = Splitter::split_time_range(
        "col".into(),
        "time".into(),
        "2023-01-01",
        "2023-01-10",
        4,
        true,
    )?;
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
    let partitions = Splitter::split_time_range(
        "col".into(),
        "time".into(),
        "2023-01-10",
        "2023-01-01",
        4,
        true,
    )?;
    assert_eq!(partitions.len(), 1);
    assert!(partitions[0].till.is_none());
    Ok(())
}

#[test]
fn test_split_by_date_open_last() -> Result<()> {
    let partitions =
        Splitter::split_by_date("ts".into(), "date".into(), "2023-01-01", "2023-01-04", true)?;
    assert_eq!(partitions.len(), 3);
    assert!(partitions[1].till.is_some());
    assert!(partitions[2].till.is_none());
    Ok(())
}

#[test]
fn test_split_time_range_invalid_format() {
    let result = Splitter::split_time_range(
        "col".into(),
        "time".into(),
        "invalid",
        "2023-01-10",
        4,
        false,
    );
    assert!(result.is_err());
}

#[test]
fn test_split_time_range_empty() -> Result<()> {
    let partitions = Splitter::split_time_range(
        "col".into(),
        "time".into(),
        "2023-01-10",
        "2023-01-01",
        4,
        false,
    )?;
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
    let dt = parse_ts("2026-05-25 00:12:12.265033+03")?;
    assert_eq!(dt.hour(), 21);
    assert_eq!(dt.minute(), 12);
    assert_eq!(dt.second(), 12);
    assert_eq!(dt.nanosecond(), 265_033_000);

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
    assert_eq!(partitions.len(), 3);
    assert!(partitions.iter().all(|p| &*p.method == "date"));
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
    let partitions = Splitter::split_by_date(
        "ts".into(),
        "date".into(),
        "2023-01-01 06:00:00",
        "2023-01-03 09:00:00",
        false,
    )?;
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
    assert_eq!(&*partitions[0].method, "day");
    Ok(())
}

#[test]
fn test_split_by_date_empty_range() -> Result<()> {
    let partitions = Splitter::split_by_date(
        "ts".into(),
        "date".into(),
        "2023-01-05",
        "2023-01-01",
        false,
    )?;
    assert!(partitions.is_empty());
    Ok(())
}

#[test]
fn test_split_hash() -> Result<()> {
    let partitions = Splitter::split("col", None, None, Some("hash"), 3)?;
    assert_eq!(partitions.len(), 3);
    assert_eq!(partitions[0].from.as_deref(), Some("0"));
    assert_eq!(partitions[0].till.as_deref(), Some("3"));
    assert_eq!(&*partitions[0].method, "hash");
    assert_eq!(partitions[1].from.as_deref(), Some("1"));
    assert_eq!(partitions[1].till.as_deref(), Some("3"));
    assert_eq!(partitions[2].from.as_deref(), Some("2"));
    assert_eq!(partitions[2].till.as_deref(), Some("3"));
    Ok(())
}
