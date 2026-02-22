use indicatif::ProgressStyle;
use std::collections::BTreeMap;
use std::fmt::Write;

/// Returns the style used for migration progress bars.
///
/// # Errors
///
/// Returns an error if the template is invalid.
pub fn migration_style() -> Result<ProgressStyle, indicatif::style::TemplateError> {
    Ok(
        ProgressStyle::with_template("[{elapsed_precise}] {bar:40.cyan/blue} {percent:>3}% {msg}")?
            .progress_chars("#>-"),
    )
}

/// Renders a verification report for a database.
///
/// Returns a tuple containing the formatted report string and a boolean indicating if there was a mismatch.
#[must_use]
pub fn render_verification_report(
    db: &str,
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
    let mut output = format!("Verification for {db}:\n");
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
