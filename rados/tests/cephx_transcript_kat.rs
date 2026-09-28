//! Cephx known answers from a real mon and OSD, checked without a cluster.
//!
//! `cephx_capture` recorded an aes256k entity's auth exchanges with a Ceph
//! mon, in CRC and SECURE mode, and with `osd.0`, and took the session keys
//! from the mon's own log. So every expected key is the mon's, the AUTH_DONE
//! payloads and the OSD's challenge and reply are C++ output, and the MonMap
//! expectation is `ceph-dencoder`'s. Authorizers 1 and 2 are rados-rs output
//! that the OSD accepted.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use rados::Denc;
use rados::auth::protocol::{
    AuthMode, CEPH_CON_MODE_CRC, CEPH_CON_MODE_SECURE, CEPHX_KEY_USAGE_AUTH_CONNECTION_SECRET,
    CEPHX_KEY_USAGE_AUTHORIZE, CEPHX_KEY_USAGE_AUTHORIZE_CHALLENGE,
    CEPHX_KEY_USAGE_AUTHORIZE_REPLY, CEPHX_KEY_USAGE_TICKET_SESSION_KEY, CephXAuthorizeA,
    CephXAuthorizeB, CephXAuthorizeReply, CephXEncryptedEnvelope, CephXResponseHeader,
    ServiceTicketReply,
};
use rados::auth::{CEPH_CRYPTO_AES256KRB5, CryptoKey};
use rados::denc::MonMapAuth;
use rados::{CephXClientHandler, EntityType};
use serde::Deserialize;

const FIXTURES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/cephx/");
const RELEASES: &[&str] = &["19.2.6"];

/// `ceph-dencoder type MonMap import - decode dump_json` of
/// `monmap-v<release>.bin`, the fields `jq` kept, verbatim.
fn dencoder_dump(release: &str) -> serde_json::Value {
    let json = match release {
        "19.2.6" => {
            r#"{
  "epoch": 2,
  "auth_epoch": 0,
  "auth_service_cipher": {
    "name": "aes256k",
    "value": 2
  },
  "auth_allowed_ciphers": [
    {
      "name": "aes",
      "value": 1
    },
    {
      "name": "aes256k",
      "value": 2
    }
  ],
  "auth_preferred_cipher": {
    "name": "aes256k",
    "value": 2
  }
}"#
        }
        other => panic!("no ceph-dencoder dump recorded for {other}"),
    };
    serde_json::from_str(json).expect("dencoder JSON")
}

#[derive(Deserialize)]
struct MonFixture {
    release: String,
    entity: String,
    client_key: String,
    con_mode: u32,
    global_id: u64,
    auth_done_hex: String,
    connection_secret_len: usize,
    mon_logged_session_keys: BTreeMap<String, String>,
}

#[derive(Deserialize)]
struct OsdFixture {
    release: String,
    entity: String,
    con_mode: u32,
    global_id: u64,
    osd_session_key: String,
    authorizer1_hex: String,
    challenge_hex: String,
    authorizer2_hex: String,
    reply_hex: String,
}

fn fixture<T: serde::de::DeserializeOwned>(file: &str) -> T {
    let path = format!("{FIXTURES}{file}");
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}"));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("{path}: {e}"))
}

fn mon_fixture(release: &str, mode: &str) -> MonFixture {
    let fx: MonFixture = fixture(&format!("v{release}-mon-{mode}.json"));
    assert_eq!(fx.release, release);
    fx
}

fn osd_fixture(release: &str) -> OsdFixture {
    let fx: OsdFixture = fixture(&format!("v{release}-osd.json"));
    assert_eq!(fx.release, release);
    fx
}

fn unhex(hex_str: &str) -> Bytes {
    Bytes::from(hex::decode(hex_str).expect("hex"))
}

fn key(base64: &str) -> CryptoKey {
    CryptoKey::from_base64(base64).expect("fixture key")
}

/// The C++ entity type name `build_service_ticket` logs.
fn service_name(service: &EntityType) -> &'static str {
    match *service {
        EntityType::AUTH => "auth",
        EntityType::MON => "mon",
        EntityType::OSD => "osd",
        EntityType::MGR => "mgr",
        EntityType::MDS => "mds",
        other => panic!("no service name for {other:?}"),
    }
}

/// The cluster's `auth_service_cipher`, as `ceph-dencoder` read it.
fn service_cipher(release: &str) -> u16 {
    let value = dencoder_dump(release)["auth_service_cipher"]["value"]
        .as_u64()
        .expect("auth_service_cipher");
    u16::try_from(value).expect("cipher")
}

/// A handler that has taken `fx`'s AUTH_DONE, and the connection secret it
/// returned.
fn mon_handler(fx: &MonFixture) -> (CephXClientHandler, Option<Bytes>) {
    let mut handler = CephXClientHandler::new(&fx.entity, AuthMode::Mon).expect("handler");
    handler
        .set_secret_key_from_base64(&fx.client_key)
        .expect("client key");
    let (_, secret) = handler
        .handle_auth_done(unhex(&fx.auth_done_hex), fx.global_id, fx.con_mode)
        .expect("handle_auth_done");
    (handler, secret)
}

/// The connection-secret envelope's ciphertext in an AUTH_DONE payload, and
/// its offset: after the `CephXResponseHeader` and the AUTH ticket's
/// `ServiceTicketReply`, an outer and an inner length.
fn connection_secret_ciphertext(auth_done: &Bytes) -> (usize, Bytes) {
    let mut buf = auth_done.clone();
    CephXResponseHeader::decode(&mut buf, 0).expect("CephXResponseHeader");
    ServiceTicketReply::decode(&mut buf, 0).expect("ServiceTicketReply");
    let mut envelope = Bytes::decode(&mut buf, 0).expect("connection secret bufferlist");
    let ciphertext = Bytes::decode(&mut envelope, 0).expect("connection secret ciphertext");
    assert!(envelope.is_empty(), "bytes after the ciphertext");
    let offset = auth_done.len() - buf.len() - ciphertext.len();
    (offset, ciphertext)
}

fn check_mon(release: &str, mode: &str) {
    let fx = mon_fixture(release, mode);
    let client_type = key(&fx.client_key).crypto_type;
    assert_eq!(client_type, CEPH_CRYPTO_AES256KRB5, "the capture's key");
    let logged: BTreeMap<&str, CryptoKey> = fx
        .mon_logged_session_keys
        .iter()
        .map(|(service, k)| (service.as_str(), key(k)))
        .collect();

    let (handler, secret) = mon_handler(&fx);
    let tickets = &handler.get_session().expect("session").ticket_handlers;
    let stored: BTreeSet<&str> = tickets.keys().map(service_name).collect();
    assert_eq!(
        stored,
        logged.keys().copied().collect::<BTreeSet<_>>(),
        "stored tickets against the services the mon logged"
    );
    for (service, ticket) in tickets {
        let name = service_name(service);
        let want = &logged[name];
        assert_eq!(ticket.session_key.crypto_type, want.crypto_type, "{name}");
        assert_eq!(ticket.session_key.secret, want.secret, "{name}");
        let want_type = if *service == EntityType::AUTH {
            client_type
        } else {
            client_type.min(service_cipher(release))
        };
        assert_eq!(ticket.session_key.crypto_type, want_type, "{name} type");
    }

    let auth_key = &logged["auth"];
    let auth_done = unhex(&fx.auth_done_hex);
    let (offset, ciphertext) = connection_secret_ciphertext(&auth_done);
    let mut plain = auth_key
        .decrypt(CEPHX_KEY_USAGE_AUTH_CONNECTION_SECRET, &ciphertext)
        .expect("the connection secret under the AUTH key, usage 0x03");
    let envelope = CephXEncryptedEnvelope::<Bytes>::decode(&mut plain, 0).expect("magic");
    assert!(plain.is_empty(), "bytes after the connection secret");
    assert_eq!(envelope.payload.len(), fx.connection_secret_len);
    assert!(
        auth_key
            .decrypt(CEPHX_KEY_USAGE_TICKET_SESSION_KEY, &ciphertext)
            .is_err(),
        "the connection secret decrypts under usage 0x04"
    );

    match fx.con_mode {
        CEPH_CON_MODE_SECURE => {
            assert_eq!(secret, Some(envelope.payload), "handle_auth_done's secret");
            let mut flipped = auth_done.to_vec();
            flipped[offset] ^= 0x01;
            let mut handler = CephXClientHandler::new(&fx.entity, AuthMode::Mon).expect("handler");
            handler
                .set_secret_key_from_base64(&fx.client_key)
                .expect("client key");
            assert!(
                handler
                    .handle_auth_done(Bytes::from(flipped), fx.global_id, fx.con_mode)
                    .is_err(),
                "a SECURE AUTH_DONE with a corrupt connection secret is accepted"
            );
        }
        CEPH_CON_MODE_CRC => assert_eq!(secret, None, "CRC mode keeps no secret"),
        other => panic!("con_mode {other}"),
    }
}

#[test]
fn mon_auth_done_crc() {
    for release in RELEASES {
        check_mon(release, "crc");
    }
}

#[test]
fn mon_auth_done_secure() {
    for release in RELEASES {
        check_mon(release, "secure");
    }
}

/// An authorizer's `CephXAuthorizeA` and its `CephXAuthorizeB`, opened under
/// `key` with usage 0x10.
fn open_authorizer(key: &CryptoKey, hex_str: &str) -> (CephXAuthorizeA, CephXAuthorizeB) {
    let mut buf = unhex(hex_str);
    let a = CephXAuthorizeA::decode(&mut buf, 0).expect("CephXAuthorizeA");
    let ciphertext = Bytes::decode(&mut buf, 0).expect("CephXAuthorizeB ciphertext");
    assert!(buf.is_empty(), "bytes after the authorizer");
    let mut plain = key
        .decrypt(CEPHX_KEY_USAGE_AUTHORIZE, &ciphertext)
        .expect("the authorizer under the OSD key, usage 0x10");
    let b = CephXEncryptedEnvelope::<CephXAuthorizeB>::decode(&mut plain, 0)
        .expect("CephXAuthorizeB")
        .payload;
    (a, b)
}

#[test]
fn osd_challenge_and_reply() {
    for release in RELEASES {
        let fx = osd_fixture(release);
        let osd_key = key(&fx.osd_session_key);

        let challenge_bytes = unhex(&fx.challenge_hex);
        let mut buf = challenge_bytes.clone();
        let ciphertext = Bytes::decode(&mut buf, 0).expect("challenge ciphertext");
        assert!(buf.is_empty(), "bytes after the challenge");
        let mut plain = osd_key
            .decrypt(CEPHX_KEY_USAGE_AUTHORIZE_CHALLENGE, &ciphertext)
            .expect("the challenge under the OSD key, usage 0x11");
        let challenge = CephXEncryptedEnvelope::<CephXAuthorizeReply>::decode(&mut plain, 0)
            .expect("challenge envelope")
            .payload
            .nonce_plus_one;
        assert!(plain.is_empty(), "bytes after the challenge");
        assert!(
            osd_key
                .decrypt(CEPHX_KEY_USAGE_AUTHORIZE_REPLY, &ciphertext)
                .is_err(),
            "the challenge decrypts under usage 0x12"
        );

        let secure = mon_fixture(release, "secure");
        assert_eq!(secure.entity, fx.entity);
        assert_eq!(secure.global_id, fx.global_id);
        let (handler, _) = mon_handler(&secure);
        assert_eq!(
            handler
                .decrypt_authorize_challenge(EntityType::OSD, challenge_bytes)
                .expect("decrypt_authorize_challenge"),
            challenge
        );
        let osd_blob = handler.get_session().expect("session").ticket_handlers[&EntityType::OSD]
            .ticket_blob
            .clone()
            .expect("OSD ticket blob");

        for (hex_str, have_challenge) in [(&fx.authorizer1_hex, false), (&fx.authorizer2_hex, true)]
        {
            let (a, b) = open_authorizer(&osd_key, hex_str);
            assert_eq!(a.global_id, fx.global_id);
            assert_eq!(a.service_id, EntityType::OSD.bits());
            assert_eq!(a.ticket_blob.secret_id, osd_blob.secret_id);
            assert_eq!(a.ticket_blob.blob, osd_blob.blob);
            assert_eq!(b.have_challenge, have_challenge);
        }
        let (_, b2) = open_authorizer(&osd_key, &fx.authorizer2_hex);
        assert_eq!(b2.server_challenge_plus_one, challenge.wrapping_add(1));

        let mut buf = unhex(&fx.reply_hex);
        let ciphertext = Bytes::decode(&mut buf, 0).expect("reply ciphertext");
        assert!(buf.is_empty(), "bytes after the reply");
        let mut plain = osd_key
            .decrypt(CEPHX_KEY_USAGE_AUTHORIZE_REPLY, &ciphertext)
            .expect("the reply under the OSD key, usage 0x12");
        let reply = CephXEncryptedEnvelope::<CephXAuthorizeReply>::decode(&mut plain, 0)
            .expect("reply envelope")
            .payload;
        assert!(plain.is_empty(), "bytes after the reply");
        assert_eq!(reply.nonce_plus_one, b2.nonce.wrapping_add(1));
        match fx.con_mode {
            CEPH_CON_MODE_SECURE => assert_eq!(
                reply.connection_secret.map(|s| s.len()),
                Some(64),
                "SECURE reply's connection secret"
            ),
            CEPH_CON_MODE_CRC => assert_eq!(reply.connection_secret, None),
            other => panic!("con_mode {other}"),
        }
        assert!(
            osd_key
                .decrypt(CEPHX_KEY_USAGE_AUTHORIZE_CHALLENGE, &ciphertext)
                .is_err(),
            "the reply decrypts under usage 0x11"
        );
    }
}

#[test]
fn monmap_v10_auth_fields() {
    for release in RELEASES {
        let path = format!("{FIXTURES}monmap-v{release}.bin");
        let bytes = std::fs::read(&path).unwrap_or_else(|e| panic!("{path}: {e}"));
        let map = <rados::MonMap as Denc>::decode(&mut Bytes::from(bytes), u64::MAX)
            .expect("MonMap decode");

        let dump = dencoder_dump(release);
        let int =
            |v: &serde_json::Value| i32::try_from(v.as_i64().expect("integer")).expect("fits i32");
        let want = MonMapAuth {
            epoch: u32::try_from(dump["auth_epoch"].as_u64().expect("auth_epoch"))
                .expect("auth_epoch"),
            service_cipher: int(&dump["auth_service_cipher"]["value"]),
            allowed_ciphers: dump["auth_allowed_ciphers"]
                .as_array()
                .expect("auth_allowed_ciphers")
                .iter()
                .map(|c| int(&c["value"]))
                .collect(),
            preferred_cipher: int(&dump["auth_preferred_cipher"]["value"]),
        };
        assert_eq!(map.auth, Some(want));
        assert_eq!(
            u64::from(map.epoch.0),
            dump["epoch"].as_u64().expect("epoch")
        );
    }
}
