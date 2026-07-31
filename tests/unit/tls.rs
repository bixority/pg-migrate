use pg_migrate::tls::{make_tls, parse_ssl_mode, ssl_mode_str};
use tokio_postgres::config::SslMode;

#[test]
fn parses_known_modes() {
    assert_eq!(parse_ssl_mode("disable"), SslMode::Disable);
    assert_eq!(parse_ssl_mode("Require"), SslMode::Require);
    assert_eq!(parse_ssl_mode("verify-full"), SslMode::Require);
    assert_eq!(parse_ssl_mode("prefer"), SslMode::Prefer);
    assert_eq!(parse_ssl_mode(""), SslMode::Prefer);
    assert_eq!(parse_ssl_mode("bogus"), SslMode::Prefer);
}

#[test]
fn maps_modes_to_strings() {
    assert_eq!(ssl_mode_str(parse_ssl_mode("verify-full")), "require");
    assert_eq!(ssl_mode_str(parse_ssl_mode("disable")), "disable");
    assert_eq!(ssl_mode_str(parse_ssl_mode("anything")), "prefer");
}

#[test]
fn builds_connector() {
    // Smoke test: constructing the connector must not panic.
    let _connector = make_tls();
}
