use pg_migrate::strip_ansi;

#[test]
fn test_strip_ansi() {
    let s = "\x1b[32mgreen\x1b[0m and \x1b[1mbold\x1b[0m";
    assert_eq!(strip_ansi(s), "green and bold");
}

#[test]
fn test_strip_ansi_no_ansi() {
    let s = "plain text";
    assert_eq!(strip_ansi(s), "plain text");
}

#[test]
fn test_strip_ansi_complex() {
    let s = "\x1b[1;31mred bold\x1b[0m";
    assert_eq!(strip_ansi(s), "red bold");
}
