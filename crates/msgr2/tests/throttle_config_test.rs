//! Integration tests for throttle configuration from ceph.conf

use msgr2::ConnectionConfig;
use std::fs;
use tempfile::TempDir;

#[test]
fn test_from_ceph_conf_with_throttle() {
    let temp_dir = TempDir::new().unwrap();
    let conf_path = temp_dir.path().join("ceph.conf");

    // Create a test ceph.conf with throttle settings
    let conf_content = r#"
[global]
ms_dispatch_throttle_bytes = 50M

[client]
# Client-specific settings
"#;

    fs::write(&conf_path, conf_content).unwrap();

    // Load config from ceph.conf
    let config = ConnectionConfig::from_ceph_conf(conf_path.to_str().unwrap())
        .expect("Failed to load config");

    // Verify throttle was set
    assert!(config.throttle_config.is_some());
    let throttle = config.throttle_config.unwrap();
    assert_eq!(throttle.max_bytes_per_sec, 50 * 1024 * 1024);
}

#[test]
fn test_from_ceph_conf_without_throttle() {
    let temp_dir = TempDir::new().unwrap();
    let conf_path = temp_dir.path().join("ceph.conf");

    // Create a test ceph.conf without throttle settings
    let conf_content = r#"
[global]
mon_host = 192.168.1.1:6789

[client]
# No throttle settings
"#;

    fs::write(&conf_path, conf_content).unwrap();

    // Load config from ceph.conf
    let config = ConnectionConfig::from_ceph_conf(conf_path.to_str().unwrap())
        .expect("Failed to load config");

    // Verify no throttle was set (default behavior)
    assert!(config.throttle_config.is_none());
}

#[test]
fn test_from_ceph_conf_with_various_units() {
    let test_cases = vec![
        ("100M", 100 * 1024 * 1024),
        ("1G", 1024 * 1024 * 1024),
        ("512K", 512 * 1024),
        ("100_M", 100 * 1024 * 1024),
        ("1024", 1024),
    ];

    for (input, expected) in test_cases {
        let temp_dir = TempDir::new().unwrap();
        let conf_path = temp_dir.path().join("ceph.conf");

        let conf_content = format!(
            r#"
[global]
ms_dispatch_throttle_bytes = {}
"#,
            input
        );

        fs::write(&conf_path, &conf_content).unwrap();

        let config = ConnectionConfig::from_ceph_conf(conf_path.to_str().unwrap())
            .expect("Failed to load config");

        assert!(config.throttle_config.is_some());
        let throttle = config.throttle_config.unwrap();
        assert_eq!(
            throttle.max_bytes_per_sec, expected,
            "Failed for input: {}",
            input
        );
    }
}

#[test]
fn test_with_ceph_default_throttle() {
    let config = ConnectionConfig::default().with_ceph_default_throttle();

    assert!(config.throttle_config.is_some());
    let throttle = config.throttle_config.unwrap();
    assert_eq!(throttle.max_bytes_per_sec, 100 * 1024 * 1024);
}

#[test]
fn test_with_custom_throttle() {
    use msgr2::ThrottleConfig;

    let throttle = ThrottleConfig::with_limits(100, 10 * 1024 * 1024, 50);
    let config = ConnectionConfig::default().with_throttle(throttle);

    assert!(config.throttle_config.is_some());
    let throttle = config.throttle_config.unwrap();
    assert_eq!(throttle.max_messages_per_sec, 100);
    assert_eq!(throttle.max_bytes_per_sec, 10 * 1024 * 1024);
    assert_eq!(throttle.max_dispatch_queue_depth, 50);
}
