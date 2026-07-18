use crate::db::MigrationState;
use crate::error::{Error, MigrationPhase, Result};
use crate::plan::MigrationPlan;
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::BTreeMap;
use std::fmt::Write;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

/// Returns the style used for migration progress bars.
pub fn migration_style() -> Result<ProgressStyle> {
    ProgressStyle::with_template("{msg}")
        .map_err(|e| Error::Config(format!("Invalid progress style template: {e}").into()))
}

#[derive(Clone, Debug)]
pub struct MigrationStates {
    states: BTreeMap<String, MigrationState>,
    order: Vec<String>,
}

impl MigrationStates {
    /// Builds the state table from the migration plan.
    ///
    /// A `"<db> (delayed)"` row is created whenever the plan schedules delayed
    /// work for that database — i.e. it has delayed tables *or* copy-engine
    /// rules. This mirrors the spawn predicate in `phase_migrate_all` exactly,
    /// so every delayed/copy-engine pipeline has a visible row to write into.
    #[must_use]
    pub fn new(plan: &MigrationPlan) -> Self {
        let mut order = Vec::new();
        let mut states = BTreeMap::new();

        for db_plan in &plan.databases {
            let db = &db_plan.name;
            order.push(db.clone());
            states.insert(db.clone(), MigrationState::new(db.clone(), db_plan.size));

            if !db_plan.delayed_tables.is_empty() || !db_plan.copy_rules.is_empty() {
                let delayed_name = format!("{db} (delayed)");
                order.push(delayed_name.clone());
                let mut delayed_state = MigrationState::new(delayed_name.clone(), db_plan.size);
                delayed_state.total_steps = 6;
                states.insert(delayed_name, delayed_state);
            }
        }

        order.sort();
        Self { states, order }
    }

    pub fn update(
        &mut self,
        db: &str,
        phase: MigrationPhase,
        step: u8,
        display: impl Into<String>,
    ) {
        if let Some(state) = self.states.get_mut(db) {
            state.set_phase(phase, step, display);
        }
    }

    pub fn fail(&mut self, db: &str, error: impl Into<String>) {
        if let Some(state) = self.states.get_mut(db) {
            state.fail(error);
        }
    }

    pub fn start_timing(&mut self, db: &str) {
        if let Some(state) = self.states.get_mut(db)
            && state.started_at.is_none()
        {
            state.started_at = Some(Instant::now());
        }
    }

    pub fn mark_regular_done(&mut self, db: &str) {
        if let Some(state) = self.states.get_mut(db) {
            state.mark_regular_done();
        }
    }

    #[must_use]
    pub fn get_state(&self, db: &str) -> Option<(MigrationPhase, u8)> {
        self.states.get(db).map(|s| (s.phase.clone(), s.step))
    }

    #[must_use]
    pub fn latest_regular_completion(&self) -> Option<Instant> {
        self.states
            .values()
            .filter_map(|s| s.regular_completed_at)
            .max()
    }

    #[must_use]
    pub fn render_table(&self) -> String {
        let mut output = String::new();

        let _ = writeln!(
            output,
            "{:<32} | {:>11} | {:>12} | {:>21} | {:>4} | Status",
            "Database", "Size", "Timing", "Phase", "%"
        );
        let _ = writeln!(
            output,
            "{:-<32}-|-{:-<11}-|-{:-<12}-|-{:-<21}-|-{:-<4}-|-{:-<40}",
            "", "", "", "", "", ""
        );

        for name in &self.order {
            let Some(state) = self.states.get(name) else {
                continue;
            };
            let size_str = if name.ends_with(" (delayed)") {
                String::new()
            } else {
                indicatif::HumanBytes(state.size).to_string()
            };
            let phase = colored_phase(&state.phase);
            let percent = state.percent();

            let timing = match (state.started_at, state.finished_at) {
                (Some(start), Some(finish)) => format_table_duration(finish.duration_since(start)),
                (Some(start), None) => format_table_duration(start.elapsed()),
                _ => "00:00:00".to_string(),
            };

            let _ = writeln!(
                output,
                "{:<32} | {:>11} | {:>12} | {:>30} | {:>3}% | {}",
                state.db, size_str, timing, phase, percent, state.display
            );
        }

        output
    }
}

fn colored_phase(phase: &MigrationPhase) -> String {
    match phase {
        MigrationPhase::Complete => format!("\x1b[32m{}\x1b[0m", phase.as_str()),
        MigrationPhase::Failed => format!("\x1b[31m{}\x1b[0m", phase.as_str()),
        _ => format!("\x1b[36m{}\x1b[0m", phase.as_str()),
    }
}

fn format_table_duration(duration: Duration) -> String {
    let secs = duration.as_secs();
    let hours = secs / 3600;
    let minutes = (secs % 3600) / 60;
    let seconds = secs % 60;

    format!("{hours:02}:{minutes:02}:{seconds:02}")
}

pub type SharedMigrationStates = Arc<Mutex<MigrationStates>>;

#[must_use]
pub fn shared_migration_states(plan: &MigrationPlan) -> SharedMigrationStates {
    Arc::new(Mutex::new(MigrationStates::new(plan)))
}

pub async fn redraw_loop(
    states: SharedMigrationStates,
    pb: ProgressBar,
    cancel: CancellationToken,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(1));

    loop {
        tokio::select! {
            _ = interval.tick() => {
                if let Ok(lock) = states.lock() {
                    let rendered = lock.render_table();
                    pb.set_message(rendered);
                }
            }
            () = cancel.cancelled() => {
                if let Ok(lock) = states.lock() {
                    let rendered = lock.render_table();
                    pb.set_message(rendered);
                }
                break;
            }
        }
    }
}

pub fn render_verification_report(
    db_name: &str,
    src_map: &BTreeMap<String, String>,
    dst_map: &BTreeMap<String, String>,
) -> (String, bool) {
    let mut tables: Vec<&String> = src_map.keys().collect();

    for k in dst_map.keys() {
        if !src_map.contains_key(k) {
            tables.push(k);
        }
    }
    tables.sort_unstable();

    let mut mismatch = false;
    let mut output = format!("Verification for {db_name}:\n");
    let _ = writeln!(
        output,
        "{:<40} | {:<15} | {:<15} | Status",
        "Table Name", "Source Rows", "Dest Rows"
    );
    let _ = writeln!(output, "{:-<40}-|-{:-<15}-|-{:-<15}-|--------", "", "", "");

    for t in &tables {
        let src_row = src_map.get(*t).map_or("MISSING", String::as_str);
        let dst_row = dst_map.get(*t).map_or("MISSING", String::as_str);

        let src_disp = if src_row == "MISSING" {
            format!("\x1b[31m{src_row}\x1b[0m")
        } else {
            (*src_row).to_string()
        };

        let dst_disp = if dst_row == "MISSING" {
            format!("\x1b[31m{dst_row}\x1b[0m")
        } else {
            (*dst_row).to_string()
        };

        let status_colored = if src_row == dst_row {
            "\x1b[32mOK\x1b[0m".to_string()
        } else {
            mismatch = true;
            "\x1b[31mMISMATCH\x1b[0m".to_string()
        };

        let src_len = if src_row == "MISSING" {
            7
        } else {
            src_row.len()
        };
        let dst_len = if dst_row == "MISSING" {
            7
        } else {
            dst_row.len()
        };
        let src_padding = " ".repeat(15_usize.saturating_sub(src_len));
        let dst_padding = " ".repeat(15_usize.saturating_sub(dst_len));

        let _ = writeln!(
            output,
            "{t:<40} | {src_disp}{src_padding} | {dst_disp}{dst_padding} | {status_colored}"
        );
    }

    (output, mismatch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::{CopyRulePlan, DatabasePlan, MigrationPlan};

    fn db_plan(name: &str, delayed: Vec<String>, copy: Vec<CopyRulePlan>) -> DatabasePlan {
        DatabasePlan {
            name: name.to_string(),
            size: 4096,
            regular_data_excludes: Vec::new(),
            full_excludes: Vec::new(),
            delayed_tables: delayed,
            copy_rules: copy,
            regular_table_names: Vec::new(),
            delayed_table_names: Vec::new(),
            copy_table_names: Vec::new(),
        }
    }

    fn copy_rule_plan(table: &str) -> CopyRulePlan {
        CopyRulePlan {
            table: table.to_string(),
            column: "created_at".to_string(),
            method: "time".to_string(),
            from: None,
            till: None,
            partitions: 1,
            rule_hash: 0,
        }
    }

    #[test]
    fn delayed_row_created_for_delayed_tables() {
        let plan = MigrationPlan {
            databases: vec![Arc::new(db_plan(
                "pdb1",
                vec!["public.bigtable".to_string()],
                vec![],
            ))],
        };
        let table = MigrationStates::new(&plan).render_table();
        assert!(table.contains("pdb1 (delayed)"), "table was:\n{table}");
    }

    #[test]
    fn delayed_row_created_for_copy_rules_only() {
        let plan = MigrationPlan {
            databases: vec![Arc::new(db_plan(
                "pdb2",
                vec![],
                vec![copy_rule_plan("public.events")],
            ))],
        };
        let table = MigrationStates::new(&plan).render_table();
        assert!(table.contains("pdb2 (delayed)"), "table was:\n{table}");
    }

    #[test]
    fn no_delayed_row_without_delayed_work() {
        let plan = MigrationPlan {
            databases: vec![Arc::new(db_plan("plain", vec![], vec![]))],
        };
        let table = MigrationStates::new(&plan).render_table();
        assert!(table.contains("plain"));
        assert!(!table.contains("(delayed)"), "table was:\n{table}");
    }

    #[test]
    fn test_render_verification_report() {
        let mut src = BTreeMap::new();
        src.insert("public.users".to_string(), "100".to_string());
        src.insert("public.posts".to_string(), "50".to_string());

        let mut dst = BTreeMap::new();
        dst.insert("public.users".to_string(), "100".to_string());
        dst.insert("public.posts".to_string(), "40".to_string());
        dst.insert("public.comments".to_string(), "10".to_string());

        let (report, mismatch) = render_verification_report("mydb", &src, &dst);
        assert!(mismatch);
        assert!(report.contains("mydb"));
        assert!(report.contains("public.users"));
        assert!(report.contains("100"));
        assert!(report.contains("OK"));
        assert!(report.contains("public.posts"));
        assert!(report.contains("MISMATCH"));
        assert!(report.contains("public.comments"));
        assert!(report.contains("MISSING"));
    }
}
