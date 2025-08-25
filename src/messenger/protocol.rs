use crate::error::{Error, Result};
use crate::types::{EntityAddr, EntityName, FeatureSet};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ProtocolVersion(u32);

impl ProtocolVersion {
    pub const V1: Self = Self(1);
    pub const V2: Self = Self(2);
    pub const CURRENT: Self = Self::V2;

    pub fn new(version: u32) -> Self {
        Self(version)
    }

    pub fn value(&self) -> u32 {
        self.0
    }

    pub fn is_supported(&self) -> bool {
        matches!(*self, Self::V1 | Self::V2)
    }
}

impl std::fmt::Display for ProtocolVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "v{}", self.0)
    }
}

#[derive(Debug, Clone)]
pub struct ProtocolFeatures {
    pub required: FeatureSet,
    pub supported: FeatureSet,
}

impl ProtocolFeatures {
    pub fn new(required: FeatureSet, supported: FeatureSet) -> Self {
        Self { required, supported }
    }

    pub fn default_v2() -> Self {
        Self {
            required: FeatureSet::MSGR2,
            supported: FeatureSet::MSGR2
                .union(FeatureSet::SERVER_QUINCY)
                .union(FeatureSet::CRUSH_TUNABLES5),
        }
    }

    pub fn can_connect(&self, peer_features: FeatureSet) -> bool {
        // Check that all our required features are supported by peer
        let our_required_supported = self.required.intersection(peer_features);
        our_required_supported.value() == self.required.value()
    }

    pub fn negotiate(&self, peer_features: FeatureSet) -> FeatureSet {
        self.supported.intersection(peer_features)
    }
}

impl Default for ProtocolFeatures {
    fn default() -> Self {
        Self::default_v2()
    }
}

#[derive(Debug, Clone)]
pub struct AuthInfo {
    pub protocol: u32,
    pub authorizer: Vec<u8>,
}

impl AuthInfo {
    pub fn none() -> Self {
        Self {
            protocol: 0, // CEPH_AUTH_NONE
            authorizer: Vec::new(),
        }
    }

    pub fn cephx(authorizer: Vec<u8>) -> Self {
        Self {
            protocol: 1, // CEPH_AUTH_CEPHX
            authorizer,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SessionInfo {
    pub entity_name: EntityName,
    pub entity_addr: EntityAddr,
    pub features: FeatureSet,
    pub global_seq: u64,
    pub connect_seq: u64,
}

impl SessionInfo {
    pub fn new(entity_name: EntityName, entity_addr: EntityAddr) -> Self {
        Self {
            entity_name,
            entity_addr,
            features: FeatureSet::EMPTY,
            global_seq: 0,
            connect_seq: 0,
        }
    }

    pub fn with_features(mut self, features: FeatureSet) -> Self {
        self.features = features;
        self
    }
}

#[derive(Debug, Clone)]
pub struct MessageRoute {
    pub source: EntityName,
    pub dest: EntityName,
    pub features: FeatureSet,
}

impl MessageRoute {
    pub fn new(source: EntityName, dest: EntityName, features: FeatureSet) -> Self {
        Self {
            source,
            dest,
            features,
        }
    }
}

#[derive(Debug)]
pub struct ProtocolState {
    version: ProtocolVersion,
    features: ProtocolFeatures,
    sessions: HashMap<EntityName, SessionInfo>,
    routes: HashMap<EntityName, MessageRoute>,
}

impl ProtocolState {
    pub fn new() -> Self {
        Self {
            version: ProtocolVersion::CURRENT,
            features: ProtocolFeatures::default(),
            sessions: HashMap::new(),
            routes: HashMap::new(),
        }
    }

    pub fn with_version(mut self, version: ProtocolVersion) -> Self {
        self.version = version;
        self
    }

    pub fn with_features(mut self, features: ProtocolFeatures) -> Self {
        self.features = features;
        self
    }

    pub fn version(&self) -> ProtocolVersion {
        self.version
    }

    pub fn features(&self) -> &ProtocolFeatures {
        &self.features
    }

    pub fn add_session(&mut self, session: SessionInfo) {
        self.sessions.insert(session.entity_name, session);
    }

    pub fn remove_session(&mut self, entity_name: &EntityName) -> Option<SessionInfo> {
        self.sessions.remove(entity_name)
    }

    pub fn get_session(&self, entity_name: &EntityName) -> Option<&SessionInfo> {
        self.sessions.get(entity_name)
    }

    pub fn add_route(&mut self, route: MessageRoute) {
        self.routes.insert(route.dest, route);
    }

    pub fn remove_route(&mut self, dest: &EntityName) -> Option<MessageRoute> {
        self.routes.remove(dest)
    }

    pub fn get_route(&self, dest: &EntityName) -> Option<&MessageRoute> {
        self.routes.get(dest)
    }

    pub fn validate_connection(&self, peer_features: FeatureSet) -> Result<FeatureSet> {
        if !self.features.can_connect(peer_features) {
            return Err(Error::Protocol(format!(
                "Incompatible features: required={:x}, peer_supported={:x}",
                self.features.required.value(),
                peer_features.value()
            )));
        }

        Ok(self.features.negotiate(peer_features))
    }

    pub fn sessions(&self) -> &HashMap<EntityName, SessionInfo> {
        &self.sessions
    }

    pub fn routes(&self) -> &HashMap<EntityName, MessageRoute> {
        &self.routes
    }
}

impl Default for ProtocolState {
    fn default() -> Self {
        Self::new()
    }
}