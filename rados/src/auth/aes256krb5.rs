//! The cephx `aes256k` cipher (`CEPH_CRYPTO_AES256KRB5`): RFC 8009
//! AES256-CTS-HMAC-SHA384-192 as Ceph v19.2.6 implements it in
//! `src/auth/Crypto.cc`.
//!
//! A message is `C || H`, where `C` is AES-256-CBC with ciphertext stealing
//! (CS3, zero IV) over a 16-byte random confounder followed by the plaintext,
//! under `Ke`, and `H` is HMAC-SHA384 under `Ki` over a zero IV and `C`,
//! truncated to 24 bytes. `Ke` and `Ki` are derived from the whole secret for
//! each key usage.

use crate::auth::error::{CephXError, Result};
use aes::Aes256;
use aes::cipher::generic_array::GenericArray;
use aes::cipher::{BlockDecrypt, BlockEncrypt, KeyInit};
use hmac::{Hmac, Mac};
use rand::RngCore;
use sha2::Sha384;
use subtle::ConstantTimeEq;

pub(crate) const KEY_LEN: usize = 32;
pub(crate) const BLOCK_LEN: usize = 16;
pub(crate) const CONFOUNDER_LEN: usize = 16;
pub(crate) const MAC_LEN: usize = 24;
pub(crate) const MIN_CIPHERTEXT_LEN: usize = CONFOUNDER_LEN + MAC_LEN;

/// Derivation kinds of RFC 3961: checksum (`Kc`), encryption (`Ke`) and
/// integrity (`Ki`).
#[cfg(test)]
pub(crate) const KIND_CHECKSUM: u8 = 0x99;
pub(crate) const KIND_ENCRYPTION: u8 = 0xAA;
pub(crate) const KIND_INTEGRITY: u8 = 0x55;

type HmacSha384 = Hmac<Sha384>;

pub(crate) fn validate_secret(secret: &[u8]) -> Result<()> {
    if secret.len() < KEY_LEN {
        return Err(CephXError::CryptographicError(format!(
            "secret shorter than {KEY_LEN} bytes ({} bytes)",
            secret.len()
        )));
    }
    Ok(())
}

fn hmac_sha384(key: &[u8], parts: &[&[u8]]) -> Result<HmacSha384> {
    let mut mac = <HmacSha384 as Mac>::new_from_slice(key)
        .map_err(|e| CephXError::CryptographicError(format!("invalid HMAC key: {e}")))?;
    for part in parts {
        mac.update(part);
    }
    Ok(mac)
}

/// RFC 8009 KDF-HMAC-SHA2 with a single iteration: the first `len` bytes of
/// `HMAC-SHA384(secret, 00000001 | BE32(usage) | kind | 00 | BE32(len * 8))`.
pub(crate) fn derive(secret: &[u8], usage: u32, kind: u8, len: usize) -> Result<Vec<u8>> {
    let block = kdf_block(usage, kind, len);
    let out = hmac_sha384(secret, &[&block])?.finalize().into_bytes();
    Ok(out[..len].to_vec())
}

fn kdf_block(usage: u32, kind: u8, len: usize) -> [u8; 14] {
    let mut block = [0u8; 14];
    block[0..4].copy_from_slice(&1u32.to_be_bytes());
    block[4..8].copy_from_slice(&usage.to_be_bytes());
    block[8] = kind;
    block[9] = 0;
    block[10..14].copy_from_slice(&((len * 8) as u32).to_be_bytes());
    block
}

fn integrity_mac(ki: &[u8], ciphertext: &[u8]) -> Result<[u8; MAC_LEN]> {
    let full = hmac_sha384(ki, &[&[0u8; BLOCK_LEN], ciphertext])?
        .finalize()
        .into_bytes();
    let mut mac = [0u8; MAC_LEN];
    mac.copy_from_slice(&full[..MAC_LEN]);
    Ok(mac)
}

fn xor_block(a: &mut [u8; BLOCK_LEN], b: &[u8]) {
    for (x, y) in a.iter_mut().zip(b) {
        *x ^= y;
    }
}

pub(crate) fn encrypt(secret: &[u8], usage: u32, plaintext: &[u8]) -> Result<Vec<u8>> {
    let mut confounder = [0u8; CONFOUNDER_LEN];
    rand::thread_rng().fill_bytes(&mut confounder);
    encrypt_with_confounder(secret, usage, &confounder, plaintext)
}

/// [`encrypt`] with a caller-chosen confounder. Ceph accepts an injected
/// confounder only from its unit tests; outside tests the confounder must be
/// random.
pub(crate) fn encrypt_with_confounder(
    secret: &[u8],
    usage: u32,
    confounder: &[u8; CONFOUNDER_LEN],
    plaintext: &[u8],
) -> Result<Vec<u8>> {
    validate_secret(secret)?;
    let ke = derive(secret, usage, KIND_ENCRYPTION, KEY_LEN)?;
    let ki = derive(secret, usage, KIND_INTEGRITY, MAC_LEN)?;
    let cipher = Aes256::new(GenericArray::from_slice(&ke));

    let n = CONFOUNDER_LEN + plaintext.len();
    let m = n.div_ceil(BLOCK_LEN);
    let r = n - BLOCK_LEN * (m - 1);

    let mut input = Vec::with_capacity(BLOCK_LEN * m);
    input.extend_from_slice(confounder);
    input.extend_from_slice(plaintext);
    input.resize(BLOCK_LEN * m, 0);

    let mut blocks: Vec<[u8; BLOCK_LEN]> = Vec::with_capacity(m);
    let mut prev = [0u8; BLOCK_LEN];
    for chunk in input.as_chunks::<BLOCK_LEN>().0 {
        let mut block = prev;
        xor_block(&mut block, chunk);
        cipher.encrypt_block(GenericArray::from_mut_slice(&mut block));
        blocks.push(block);
        prev = block;
    }

    let mut out = Vec::with_capacity(n + MAC_LEN);
    if m == 1 {
        out.extend_from_slice(&blocks[0]);
    } else {
        for block in &blocks[..m - 2] {
            out.extend_from_slice(block);
        }
        out.extend_from_slice(&blocks[m - 1]);
        out.extend_from_slice(&blocks[m - 2][..r]);
    }
    let mac = integrity_mac(&ki, &out)?;
    out.extend_from_slice(&mac);
    Ok(out)
}

pub(crate) fn decrypt(secret: &[u8], usage: u32, ciphertext: &[u8]) -> Result<Vec<u8>> {
    validate_secret(secret)?;
    if ciphertext.len() < MIN_CIPHERTEXT_LEN {
        return Err(CephXError::CryptographicError(format!(
            "ciphertext shorter than {MIN_CIPHERTEXT_LEN} bytes ({} bytes)",
            ciphertext.len()
        )));
    }
    let (c, h) = ciphertext.split_at(ciphertext.len() - MAC_LEN);

    let ki = derive(secret, usage, KIND_INTEGRITY, MAC_LEN)?;
    let expected = integrity_mac(&ki, c)?;
    if !bool::from(expected.ct_eq(h)) {
        return Err(CephXError::CryptographicError(
            "integrity check failed".into(),
        ));
    }

    let ke = derive(secret, usage, KIND_ENCRYPTION, KEY_LEN)?;
    let cipher = Aes256::new(GenericArray::from_slice(&ke));

    let n = c.len();
    let m = n.div_ceil(BLOCK_LEN);
    let r = n - BLOCK_LEN * (m - 1);

    let decrypt_block = |block: &[u8]| -> [u8; BLOCK_LEN] {
        let mut out = [0u8; BLOCK_LEN];
        out.copy_from_slice(block);
        cipher.decrypt_block(GenericArray::from_mut_slice(&mut out));
        out
    };

    let mut plain = Vec::with_capacity(n);
    if m == 1 {
        plain.extend_from_slice(&decrypt_block(c));
    } else {
        let d = decrypt_block(&c[BLOCK_LEN * (m - 2)..BLOCK_LEN * (m - 1)]);
        let tail = &c[BLOCK_LEN * (m - 1)..];
        let last: Vec<u8> = d[..r].iter().zip(tail).map(|(a, b)| a ^ b).collect();
        let mut penultimate = [0u8; BLOCK_LEN];
        penultimate[..r].copy_from_slice(tail);
        penultimate[r..].copy_from_slice(&d[r..]);

        let mut prev = [0u8; BLOCK_LEN];
        let head = c[..BLOCK_LEN * (m - 2)].as_chunks::<BLOCK_LEN>().0;
        for block in head.iter().chain(std::iter::once(&penultimate)) {
            let mut p = decrypt_block(block);
            xor_block(&mut p, &prev);
            plain.extend_from_slice(&p);
            prev = *block;
        }
        plain.extend_from_slice(&last);
    }
    plain.drain(..CONFOUNDER_LEN);
    Ok(plain)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;

    const RFC_KEY: &str = "6D404D37FAF79F9DF0D33568D320669800EB4836472EA8A026D16B7182460C52";

    fn hx(s: &str) -> Vec<u8> {
        hex::decode(s).unwrap()
    }

    fn conf(s: &str) -> [u8; CONFOUNDER_LEN] {
        hx(s).try_into().unwrap()
    }

    // src/test/crypto.cc:390-447@v19.2.6 (RFC 8009 appendix A), usage 2.
    const VECTORS: [(&str, usize, &str); 4] = [
        (
            "F764E9FA15C276478B2C7D0C4E5F58E4",
            0,
            "41F53FA5BFE7026D91FAF9BE959195A058707273A96A40F0A01960621AC612748B9BBFBE7EB4CE3C",
        ),
        (
            "B80D3251C1F6471494256FFE712D0B9A",
            6,
            "4ED7B37C2BCAC8F74F23C1CF07E62BC7B75FB3F637B9F559C7F664F69EAB7B6092237526EA0D1F61CB20D69D10F2",
        ),
        (
            "53BF8A0D105265D4E276428624CE5E63",
            16,
            "BC47FFEC7998EB91E8115CF8D19DAC4BBBE2E163E87DD37F49BECA92027764F68CF51F14D798C2273F35DF574D1F932E40C4FF255B36A266",
        ),
        (
            "763E65367E864F02F55153C7E3B58AF1",
            21,
            "40013E2DF58E8751957D2878BCD2D6FE101CCFD556CB1EAE79DB3C3EE86429F2B2A602AC86FEF6ECB647D6295FAE077A1FEB517508D2C16B4192E01F62",
        ),
    ];

    fn counting(len: usize) -> Vec<u8> {
        (0..len).map(|i| i as u8).collect()
    }

    #[test]
    fn rfc8009_vectors_encrypt_and_decrypt() {
        let key = hx(RFC_KEY);
        for (confounder, len, expected) in VECTORS {
            let plaintext = counting(len);
            let ct = encrypt_with_confounder(&key, 2, &conf(confounder), &plaintext).unwrap();
            assert_eq!(ct, hx(expected), "encrypt, plaintext length {len}");
            assert_eq!(decrypt(&key, 2, &hx(expected)).unwrap(), plaintext);
        }
    }

    // Computed independently in Python (hmac/hashlib and the `cryptography`
    // package's AES-CBC) from the definition that reproduces the four vectors
    // above. The plaintext is the bytes 00 01 02 .. of the given length.
    #[test]
    fn five_block_cs3_vectors() {
        let key = hx(RFC_KEY);
        let confounder = conf("a0a1a2a3a4a5a6a7a8a9aaabacadaeaf");
        let cases = [
            (
                53,
                "ca0522838c12dfd084d33a407bf109a3e80556051f828e1fe307f9392d7cba733b6510a90ea5abbdea280d562a78bac74eb2626fe95091e7be6f0f759a8bf17b0bdff321a656ca4738330db2aa0d3a2efd5902db3852fecbc2be298fd6",
            ),
            (
                64,
                "ca0522838c12dfd084d33a407bf109a3e80556051f828e1fe307f9392d7cba733b6510a90ea5abbdea280d562a78bac7890dadd0da9838a25c00514c275511000bdff321a626f7044cae44ffc3ad8de1646067624725c1e101f6743674defcbf7c96332c08179340",
            ),
        ];
        for (len, expected) in cases {
            let plaintext = counting(len);
            let ct = encrypt_with_confounder(&key, 4, &confounder, &plaintext).unwrap();
            assert_eq!(hex::encode(&ct), expected, "plaintext length {len}");
            assert_eq!(decrypt(&key, 4, &ct).unwrap(), plaintext);
        }
    }

    #[test]
    fn derived_keys_for_usage_2() {
        let key = hx(RFC_KEY);
        assert_eq!(
            kdf_block(2, KIND_ENCRYPTION, KEY_LEN).to_vec(),
            hx("0000000100000002aa0000000100")
        );
        assert_eq!(
            derive(&key, 2, KIND_CHECKSUM, 24).unwrap(),
            hx("EF5718BE86CC84963D8BBB5031E9F5C4BA41F28FAF69E73D")
        );
        assert_eq!(
            derive(&key, 2, KIND_ENCRYPTION, KEY_LEN).unwrap(),
            hx("56AB22BEE63D82D7BC5227F6773F8EA7A5EB1C825160C38312980C442E5C7E49")
        );
        assert_eq!(
            derive(&key, 2, KIND_INTEGRITY, MAC_LEN).unwrap(),
            hx("69B16514E3CD8E56B82010D5C73012B622C4D00FFC23ED1F")
        );
    }

    fn assert_err_contains(result: Result<Vec<u8>>, needle: &str) {
        let err = result.unwrap_err().to_string();
        assert!(err.contains(needle), "{err:?} lacks {needle:?}");
    }

    #[test]
    fn tampering_fails_the_integrity_check() {
        let key = hx(RFC_KEY);
        let good = hx(VECTORS[1].2);

        let mut bad_mac = good.clone();
        *bad_mac.last_mut().unwrap() ^= 1;
        assert_err_contains(decrypt(&key, 2, &bad_mac), "integrity check failed");

        let mut bad_ct = good.clone();
        bad_ct[3] ^= 1;
        assert_err_contains(decrypt(&key, 2, &bad_ct), "integrity check failed");

        assert_err_contains(decrypt(&key, 3, &good), "integrity check failed");
    }

    #[test]
    fn short_ciphertext_is_rejected() {
        let key = hx(RFC_KEY);
        assert_err_contains(
            decrypt(&key, 2, &[0u8; 39]),
            "ciphertext shorter than 40 bytes",
        );
    }

    #[test]
    fn usage_changes_the_ciphertext() {
        let key = hx(RFC_KEY);
        let (confounder, len, expected) = VECTORS[1];
        let ct = encrypt_with_confounder(&key, 3, &conf(confounder), &counting(len)).unwrap();
        assert_eq!(ct.len(), hx(expected).len());
        assert_ne!(ct, hx(expected));
    }

    #[test]
    fn secret_length() {
        for len in 0..KEY_LEN {
            let err = validate_secret(&vec![0u8; len]).unwrap_err().to_string();
            assert!(err.contains("secret shorter than 32 bytes"), "{err}");
        }
        for len in KEY_LEN..50 {
            validate_secret(&vec![0u8; len]).unwrap();
        }

        // HMAC zero-pads its key, so a trailing zero byte would not change it.
        let mut long = hx(RFC_KEY);
        long.push(1);
        let confounder = conf(VECTORS[1].0);
        let from_long = encrypt_with_confounder(&long, 2, &confounder, b"abc").unwrap();
        let from_prefix =
            encrypt_with_confounder(&long[..KEY_LEN], 2, &confounder, b"abc").unwrap();
        assert_ne!(from_long, from_prefix);
    }

    #[test]
    fn random_round_trips() {
        let key = hx(RFC_KEY);
        let mut rng = rand::thread_rng();
        for len in 0..=64 {
            let mut plaintext = vec![0u8; len];
            rng.fill_bytes(&mut plaintext);
            let usage: u32 = rng.r#gen();
            let ct = encrypt(&key, usage, &plaintext).unwrap();
            assert_eq!(ct.len(), CONFOUNDER_LEN + len + MAC_LEN);
            assert_eq!(decrypt(&key, usage, &ct).unwrap(), plaintext);
        }
    }
}
