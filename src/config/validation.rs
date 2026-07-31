use super::types::CopyRule;
use crate::error::{Error, Result};

pub fn parse_fully_qualified(entry: &str) -> Option<(&str, &str, &str)> {
    let mut parts = entry.split('.');
    let (db, schema, table) = (parts.next()?, parts.next()?, parts.next()?);
    if parts.next().is_some() || db.is_empty() || schema.is_empty() || table.is_empty() {
        return None;
    }
    Some((db, schema, table))
}

pub fn is_valid_pattern(s: &str) -> bool {
    let parts: Vec<&str> = s.split('.').collect();
    if parts.is_empty() || parts.len() > 3 {
        return false;
    }
    parts.iter().all(|p| !p.is_empty())
}

pub fn validate_copy_rules(rules: &[CopyRule]) -> Result<()> {
    for rule in rules {
        if parse_fully_qualified(&rule.table).is_none() {
            return Err(Error::InvalidCopyRule {
                table: rule.table.clone().into(),
                reason: "expected 'DATABASE.SCHEMA.TABLE' format with all parts non-empty".into(),
            });
        }
    }
    Ok(())
}

pub fn validate_delay_table_data(patterns: &[String]) -> Result<()> {
    for pattern in patterns {
        if !is_valid_pattern(pattern) {
            return Err(Error::InvalidCopyRule {
                table: pattern.clone().into(),
                reason: "delay_table_data entry must be 'DB', 'DB.SCHEMA', or \
                         'DB.SCHEMA.TABLE' with all parts non-empty"
                    .into(),
            });
        }
    }
    Ok(())
}

pub fn validate_exclude_patterns(patterns: &[String]) -> Result<()> {
    for pattern in patterns {
        if !is_valid_pattern(pattern) {
            return Err(Error::InvalidCopyRule {
                table: pattern.clone().into(),
                reason: "exclude entry must be 'DB', 'DB.SCHEMA', or 'DB.SCHEMA.TABLE' with \
                         all parts non-empty"
                    .into(),
            });
        }
    }
    Ok(())
}
