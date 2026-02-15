//! DNS SRV record resolution for monitor discovery
//!
//! Implements DNS SRV-based monitor discovery as described in Ceph's
//! `MonMap::build_initial()`. When no monitor addresses are configured,
//! the client can discover monitors via DNS SRV records.
//!
//! The default SRV record name is `_ceph-mon._tcp`, which can be
//! customized via the `mon_dns_srv_name` configuration option.
//!
//! The service name may include a domain suffix separated by `_`,
//! e.g., `ceph-mon_example.com` resolves `_ceph-mon._tcp.example.com`.

use crate::error::{MonClientError, Result};
use crate::monmap::MonMap;
use hickory_resolver::TokioAsyncResolver;
use std::net::SocketAddr;
use tracing::{debug, warn};

/// Default DNS SRV service name for Ceph monitors
pub const DEFAULT_MON_DNS_SRV_NAME: &str = "ceph-mon";

/// Ceph monitor default msgr2 port (IANA assigned)
const CEPH_MON_PORT_IANA: u16 = 3300;

/// Ceph monitor default legacy port
const CEPH_MON_PORT_LEGACY: u16 = 6789;

/// Resolve monitor addresses via DNS SRV records.
///
/// Queries DNS for SRV records matching the given service name,
/// then resolves the returned hostnames to IP addresses.
///
/// The `srv_name` parameter follows Ceph conventions:
/// - `"ceph-mon"` → queries `_ceph-mon._tcp`
/// - `"ceph-mon_example.com"` → queries `_ceph-mon._tcp.example.com`
///
/// Returns a MonMap populated with the discovered monitors, or an error
/// if no monitors could be found.
pub async fn resolve_mon_addrs_via_dns_srv(srv_name: &str) -> Result<MonMap> {
    let (service, domain) = parse_srv_name(srv_name);

    let query_name = if domain.is_empty() {
        format!("_{service}._tcp")
    } else {
        format!("_{service}._tcp.{domain}")
    };

    debug!("Resolving monitors via DNS SRV: {}", query_name);

    let resolver = TokioAsyncResolver::tokio_from_system_conf()
        .map_err(|e| MonClientError::Other(format!("Failed to create DNS resolver: {}", e)))?;

    let srv_records = resolver.srv_lookup(&query_name).await.map_err(|e| {
        debug!("DNS SRV lookup failed for {}: {}", query_name, e);
        MonClientError::InvalidMonMap(format!("DNS SRV lookup failed for {}: {}", query_name, e))
    })?;

    let mut mon_addrs = Vec::new();

    for record in srv_records.iter() {
        let target = record.target().to_string();
        let port = record.port();
        // Strip trailing dot from DNS name
        let target_name = target.trim_end_matches('.');

        debug!(
            "SRV record: {} port={} priority={} weight={}",
            target_name,
            port,
            record.priority(),
            record.weight()
        );

        // Resolve the hostname to IP addresses
        match resolver.lookup_ip(target_name).await {
            Ok(ips) => {
                for ip in ips.iter() {
                    let socket_addr = SocketAddr::new(ip, port);
                    // Determine address type based on port number, matching Ceph behavior
                    let addr_str = if port == CEPH_MON_PORT_LEGACY {
                        format!("v1:{}", socket_addr)
                    } else if port == CEPH_MON_PORT_IANA {
                        format!("v2:{}", socket_addr)
                    } else {
                        // For non-standard ports, add both v2 and v1 entries
                        // to match Ceph's _add_ambiguous_addr behavior
                        mon_addrs.push(format!("v2:{}", socket_addr));
                        format!("v1:{}", socket_addr)
                    };
                    mon_addrs.push(addr_str);
                }
            }
            Err(e) => {
                warn!("Failed to resolve hostname {}: {}", target_name, e);
            }
        }
    }

    if mon_addrs.is_empty() {
        return Err(MonClientError::InvalidMonMap(
            "No monitor addresses found via DNS SRV".to_string(),
        ));
    }

    debug!("Resolved {} monitor addresses via DNS SRV", mon_addrs.len());
    MonMap::build_initial(&mon_addrs)
}

/// Parse DNS SRV service name into (service, domain) components.
///
/// Follows Ceph's convention where `_` separates service name from domain:
/// - `"ceph-mon"` → `("ceph-mon", "")`
/// - `"ceph-mon_example.com"` → `("ceph-mon", "example.com")`
fn parse_srv_name(srv_name: &str) -> (&str, &str) {
    match srv_name.find('_') {
        Some(idx) => (&srv_name[..idx], &srv_name[idx + 1..]),
        None => (srv_name, ""),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_srv_name_simple() {
        let (service, domain) = parse_srv_name("ceph-mon");
        assert_eq!(service, "ceph-mon");
        assert_eq!(domain, "");
    }

    #[test]
    fn test_parse_srv_name_with_domain() {
        let (service, domain) = parse_srv_name("ceph-mon_example.com");
        assert_eq!(service, "ceph-mon");
        assert_eq!(domain, "example.com");
    }

    #[test]
    fn test_parse_srv_name_with_subdomain() {
        let (service, domain) = parse_srv_name("ceph-mon_ceph.example.com");
        assert_eq!(service, "ceph-mon");
        assert_eq!(domain, "ceph.example.com");
    }

    #[test]
    fn test_default_srv_name() {
        assert_eq!(DEFAULT_MON_DNS_SRV_NAME, "ceph-mon");
    }

    #[tokio::test]
    async fn test_resolve_nonexistent_srv() {
        // Querying a non-existent SRV record should return an error
        let result =
            resolve_mon_addrs_via_dns_srv("ceph-mon_nonexistent.invalid.example.test").await;
        assert!(result.is_err());
    }
}
