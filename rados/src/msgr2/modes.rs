//! The connection modes a client offers, read from `ms_mon_client_mode` and
//! `ms_client_mode` as C++ `AuthRegistry` reads them.

use crate::EntityType;
use crate::cephconfig::CephConfig;
use crate::msgr2::ConnectionMode;

/// The default of `ms_mon_client_mode`
/// (v19.2.6:src/common/options/global.yaml.in:973-989).
pub const MS_MON_CLIENT_MODE_DEFAULT: &str = "secure crc";

/// The default of `ms_client_mode`
/// (v19.2.6:src/common/options/global.yaml.in:1015-1027).
pub const MS_CLIENT_MODE_DEFAULT: &str = "crc secure";

/// Parse a connection mode list as C++ `AuthRegistry::_parse_mode_list`
/// does (v19.2.6:src/auth/AuthRegistry.cc:92-115): split it on any of
/// `;,= \t`, as `get_str_list` does (src/common/str_list.cc:30-33), and keep
/// each `crc` and `secure` in order. A name is matched case-sensitively,
/// and any other is skipped with a warning, as is a list left empty.
pub fn parse_mode_list(list: &str) -> Vec<ConnectionMode> {
    let mut modes = Vec::new();
    for name in list
        .split([';', ',', '=', ' ', '\t'])
        .filter(|name| !name.is_empty())
    {
        match name {
            "crc" => modes.push(ConnectionMode::Crc),
            "secure" => modes.push(ConnectionMode::Secure),
            _ => tracing::warn!("unknown connection mode {name:?} in {list:?}"),
        }
    }
    if modes.is_empty() {
        tracing::warn!("no connection modes defined in {list:?}");
    }
    modes
}

/// The connection modes a client offers, in order of preference, by the
/// type of the peer.
///
/// An empty list is kept, as in C++: a monitor connection then fails before
/// it sends AUTH_REQUEST, and any other peer is offered no mode, which a
/// C++ server rejects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientModes {
    /// Modes offered to monitors and managers: `ms_mon_client_mode`.
    pub mon_client_modes: Vec<ConnectionMode>,
    /// Modes offered to every other peer: `ms_client_mode`.
    pub client_modes: Vec<ConnectionMode>,
}

impl Default for ClientModes {
    /// C++'s defaults: SECURE first for monitors and managers, CRC first
    /// for the rest.
    fn default() -> Self {
        Self {
            mon_client_modes: parse_mode_list(MS_MON_CLIENT_MODE_DEFAULT),
            client_modes: parse_mode_list(MS_CLIENT_MODE_DEFAULT),
        }
    }
}

impl ClientModes {
    /// Read `ms_mon_client_mode` and `ms_client_mode` as `entity` reads them:
    /// from its own section, then its type's, then `[global]`. An option
    /// that is unset takes its C++ default.
    ///
    /// The mon config database is not read for these options, though C++
    /// applies it before it creates the messenger
    /// (v19.2.6:src/librados/RadosClient.cc:232, then 247), so require
    /// SECURE through `ceph.conf` or [`crate::ClientBuilder`]'s setters.
    ///
    /// `ms_mode` is not read: C++ v19.2.6 has no such option, only a
    /// kernel client mount option and an `rbd map` option of that name.
    pub fn from_ceph_config(config: &CephConfig, entity: &str) -> Self {
        let read =
            |option, default| parse_mode_list(config.get_for(entity, option).unwrap_or(default));
        Self {
            mon_client_modes: read("ms_mon_client_mode", MS_MON_CLIENT_MODE_DEFAULT),
            client_modes: read("ms_client_mode", MS_CLIENT_MODE_DEFAULT),
        }
    }

    /// The modes a client offers a peer of `peer_type`: `mon_client_modes`
    /// for a monitor or a manager, `client_modes` for any other. C++
    /// `AuthRegistry::get_supported_methods` for a client
    /// (v19.2.6:src/auth/AuthRegistry.cc:199-213).
    pub fn for_peer(&self, peer_type: EntityType) -> &[ConnectionMode] {
        if peer_type == EntityType::MON || peer_type == EntityType::MGR {
            &self.mon_client_modes
        } else {
            &self.client_modes
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ConnectionMode::{Crc, Secure};

    #[test]
    fn parse_keeps_crc_and_secure_in_order() {
        assert_eq!(parse_mode_list("secure crc"), vec![Secure, Crc]);
        assert_eq!(parse_mode_list("crc secure"), vec![Crc, Secure]);
        assert_eq!(parse_mode_list("secure"), vec![Secure]);
        assert_eq!(parse_mode_list("crc"), vec![Crc]);
        assert_eq!(parse_mode_list("crc crc"), vec![Crc, Crc]);
    }

    #[test]
    fn parse_splits_on_get_str_list_separators() {
        for list in [
            "secure,crc",
            "secure;crc",
            "secure=crc",
            "secure\tcrc",
            " ,secure ;; crc= ",
        ] {
            assert_eq!(parse_mode_list(list), vec![Secure, Crc], "{list:?}");
        }
    }

    #[test]
    fn parse_skips_unknown_names() {
        assert_eq!(parse_mode_list("legacy secure"), vec![Secure]);
        assert_eq!(parse_mode_list("prefer-crc crc"), vec![Crc]);
        // Matched case-sensitively, as in C++.
        assert_eq!(parse_mode_list("SECURE Crc crc"), vec![Crc]);
        assert_eq!(parse_mode_list("bogus"), vec![]);
        assert_eq!(parse_mode_list(""), vec![]);
        assert_eq!(parse_mode_list(" , "), vec![]);
    }

    #[test]
    fn default_prefers_secure_for_mons_and_crc_for_the_rest() {
        let modes = ClientModes::default();
        assert_eq!(modes.mon_client_modes, vec![Secure, Crc]);
        assert_eq!(modes.client_modes, vec![Crc, Secure]);
    }

    #[test]
    fn for_peer_gives_mons_and_mgrs_the_mon_modes() {
        let modes = ClientModes {
            mon_client_modes: vec![Secure],
            client_modes: vec![Crc],
        };
        assert_eq!(modes.for_peer(EntityType::MON), [Secure]);
        assert_eq!(modes.for_peer(EntityType::MGR), [Secure]);
        assert_eq!(modes.for_peer(EntityType::OSD), [Crc]);
        assert_eq!(modes.for_peer(EntityType::MDS), [Crc]);
        assert_eq!(modes.for_peer(EntityType::CLIENT), [Crc]);
    }

    #[test]
    fn from_ceph_config_reads_as_the_entity_does() {
        let conf = CephConfig::parse(
            "[global]\n\
             ms_client_mode = secure\n\
             [client]\n\
             ms mon client mode = crc\n\
             [client.rgw.x]\n\
             ms_client_mode = crc, secure\n\
             ms_mon_client_mode = secure\n",
        )
        .unwrap();
        let rgw = ClientModes::from_ceph_config(&conf, "client.rgw.x");
        assert_eq!(rgw.mon_client_modes, vec![Secure]);
        assert_eq!(rgw.client_modes, vec![Crc, Secure]);
        let admin = ClientModes::from_ceph_config(&conf, "client.admin");
        assert_eq!(admin.mon_client_modes, vec![Crc]);
        assert_eq!(admin.client_modes, vec![Secure]);
    }

    #[test]
    fn from_ceph_config_defaults_what_is_unset() {
        let conf = CephConfig::parse("[global]\nfsid = x\n").unwrap();
        assert_eq!(
            ClientModes::from_ceph_config(&conf, "client.admin"),
            ClientModes::default()
        );
    }

    #[test]
    fn from_ceph_config_keeps_an_empty_list() {
        let conf = CephConfig::parse("[global]\nms_client_mode = legacy\n").unwrap();
        let modes = ClientModes::from_ceph_config(&conf, "client.admin");
        assert_eq!(modes.client_modes, vec![]);
        assert_eq!(modes.mon_client_modes, vec![Secure, Crc]);
    }
}
