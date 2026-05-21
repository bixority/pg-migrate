use crate::Config;
use crate::db::{MigrationPhase, MigrationState};
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::BTreeMap;
use std::fmt::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

/// Returns the style used for migration progress bars.
/// Returns the style used for the state table.
///
/// # Errors
///
/// Returns an error if the template is invalid.
pub fn migration_style() -> Result<ProgressStyle, indicatif::style::TemplateError> {
    ProgressStyle::with_template("{msg}")
}

#[derive(Clone, Debug)]
pub struct MigrationStates {
    states: BTreeMap<String, MigrationState>,
}

impl MigrationStates {
    #[must_use]
    pub fn new(dbs_with_sizes: &[(String, u64)], config: &Config) -> Self {
        let states = dbs_with_sizes
            .iter()
            .map(|(db, size)| {
                let mut state = MigrationState::new(db.clone(), *size);
                let db_prefix = format!("{db}.");
                if config
                    .delay_table_data
                    .iter()
                    .any(|d| d.starts_with(&db_prefix))
                {
                    state.total_steps = 11;
                }
                (db.clone(), state)
            })
            .collect();

        Self { states }
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

    pub fn mark_regular_done(&mut self, db: &str) {
        if let Some(state) = self.states.get_mut(db) {
            state.mark_regular_done();
        }
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
            "{:<32} | {:>7} | {:<16} | {:>4} | Status",
            "Database", "Size", "Phase", "%"
        );
        let _ = writeln!(
            output,
            "{:-<32}-|-{:-<7}-|-{:-<16}-|-{:-<4}-|-{:-<40}",
            "", "", "", "", ""
        );

        for state in self.states.values() {
            let size = indicatif::HumanBytes(state.size);
            let phase = colored_phase(&state.phase);
            let percent = state.percent();

            let _ = writeln!(
                output,
                "{:<32} | {:>7} | {:<16} | {:>3}% | {}",
                state.db, size, phase, percent, state.display
            );
        }

        output
    }
}

fn colored_phase(phase: &MigrationPhase) -> String {
    match phase {
        MigrationPhase::Complete => format!("\x1b[32m{}\x1b[0m", phase.as_str()),
        MigrationPhase::Failed => format!("\x1b[31m{}\x1b[0m", phase.as_str()),
        MigrationPhase::Pending => phase.as_str().to_string(),
        _ => format!("\x1b[36m{}\x1b[0m", phase.as_str()),
    }
}

pub type SharedMigrationStates = Arc<RwLock<MigrationStates>>;

#[must_use]
pub fn shared_migration_states(
    dbs_with_sizes: &[(String, u64)],
    config: &Config,
) -> SharedMigrationStates {
    Arc::new(RwLock::new(MigrationStates::new(dbs_with_sizes, config)))
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
                let rendered = states.read().await.render_table();
                pb.set_message(rendered);
            }
            () = cancel.cancelled() => {
                let rendered = states.read().await.render_table();
                pb.set_message(rendered);
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

        let _ = writeln!(
            output,
            "{t:<40} | {src_disp:<15} | {dst_disp:<15} | {status_colored}"
        );
    }

    (output, mismatch)
}
