use hmac::{Hmac, Mac};
use protocol::StreamTokenClaims;
use sha2::Sha256;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use subtle::ConstantTimeEq;
use thiserror::Error;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AuthError {
    #[error("Invalid secret key length or format")]
    InvalidKey,
    #[error("Invalid hex encoding in signature")]
    InvalidHex,
    #[error("Signature verification failed (mismatch)")]
    SignatureMismatch,
    #[error("Token expired at timestamp {expired_at}, current time {current_time}")]
    TokenExpired { expired_at: i64, current_time: i64 },
    #[error("Timestamp drift exceeded allowable window ({drift_secs}s > {max_skew_secs}s)")]
    TimestampDriftExceeded { drift_secs: i64, max_skew_secs: i64 },
    #[error("Replay attack detected: nonce '{nonce}' already used by edge '{edge_id}'")]
    NonceReplayed { edge_id: String, nonce: String },
    #[error("Missing or malformed X-Nonce header")]
    MissingNonce,
}

/// In-memory thread-safe tracker for anti-replay verification of nonces.
#[derive(Clone, Default, Debug)]
pub struct NonceTracker {
    seen_nonces: Arc<RwLock<HashMap<String, i64>>>,
}

impl NonceTracker {
    pub fn new() -> Self {
        Self {
            seen_nonces: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Verifies that the nonce has not been seen. If fresh, records it with an expiration timestamp.
    /// If already seen within the active validity window, returns AuthError::NonceReplayed.
    pub fn check_and_record(
        &self,
        edge_id: &str,
        nonce: &str,
        expires_at: i64,
        current_time: i64,
    ) -> Result<(), AuthError> {
        let key = format!("{}:{}", edge_id, nonce);
        let mut guard = self.seen_nonces.write().unwrap();

        // Periodic pruning if threshold is reached
        if guard.len() > 500 {
            guard.retain(|_, &mut exp| exp > current_time);
        }

        if let Some(&existing_exp) = guard.get(&key) {
            if existing_exp > current_time {
                return Err(AuthError::NonceReplayed {
                    edge_id: edge_id.to_string(),
                    nonce: nonce.to_string(),
                });
            }
        }

        guard.insert(key, expires_at);
        Ok(())
    }

    /// Explicitly sweeps expired nonces from memory.
    pub fn sweep_expired(&self, current_time: i64) -> usize {
        let mut guard = self.seen_nonces.write().unwrap();
        let before = guard.len();
        guard.retain(|_, &mut exp| exp > current_time);
        before - guard.len()
    }

    /// Returns the number of nonces currently tracked in memory.
    pub fn len(&self) -> usize {
        self.seen_nonces.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.seen_nonces.read().unwrap().is_empty()
    }
}

/// Computes HMAC-SHA256 and returns a lowercase hex string.
pub fn sign_hmac_sha256(secret: &[u8], data: &[u8]) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret).expect("HMAC-SHA256 accepts keys of any length");
    mac.update(data);
    hex::encode(mac.finalize().into_bytes())
}

/// Verifies an HMAC-SHA256 signature using constant-time comparison.
pub fn verify_hmac_sha256(
    secret: &[u8],
    data: &[u8],
    signature_hex: &str,
) -> Result<(), AuthError> {
    let expected_bytes = hex::decode(signature_hex).map_err(|_| AuthError::InvalidHex)?;
    let mut mac = HmacSha256::new_from_slice(secret).map_err(|_| AuthError::InvalidKey)?;
    mac.update(data);
    let calculated = mac.finalize().into_bytes();

    if calculated.as_slice().ct_eq(&expected_bytes).into() {
        Ok(())
    } else {
        Err(AuthError::SignatureMismatch)
    }
}

/// Builds the canonical representation of an Origin request and signs it.
/// Format: `METHOD\nPATH_AND_QUERY\nTIMESTAMP\nEDGE_ID\nNONCE`
pub fn sign_origin_request(
    secret: &[u8],
    method: &str,
    path_and_query: &str,
    timestamp: i64,
    edge_id: &str,
    nonce: &str,
) -> String {
    let payload = format!(
        "{}\n{}\n{}\n{}\n{}",
        method.to_uppercase(),
        path_and_query,
        timestamp,
        edge_id,
        nonce
    );
    sign_hmac_sha256(secret, payload.as_bytes())
}

#[derive(Debug, Clone)]
pub struct OriginRequestParams<'a> {
    pub method: &'a str,
    pub path_and_query: &'a str,
    pub timestamp: i64,
    pub edge_id: &'a str,
    pub nonce: &'a str,
    pub signature_hex: &'a str,
}

/// Verifies an incoming Origin request signature and timestamp window with dual-key rotation support.
pub fn verify_origin_request_with_rotation(
    primary_secret: &[u8],
    fallback_secret: Option<&[u8]>,
    params: &OriginRequestParams<'_>,
    max_skew_secs: i64,
    current_time: i64,
) -> Result<(), AuthError> {
    let drift = (current_time - params.timestamp).abs();
    if drift > max_skew_secs {
        return Err(AuthError::TimestampDriftExceeded {
            drift_secs: drift,
            max_skew_secs,
        });
    }

    let payload = format!(
        "{}\n{}\n{}\n{}\n{}",
        params.method.to_uppercase(),
        params.path_and_query,
        params.timestamp,
        params.edge_id,
        params.nonce
    );

    // 1. Attempt verification with primary active key
    if verify_hmac_sha256(primary_secret, payload.as_bytes(), params.signature_hex).is_ok() {
        return Ok(());
    }

    // 2. If fallback key is provided during key rotation, attempt fallback verification
    if let Some(fallback) = fallback_secret {
        if verify_hmac_sha256(fallback, payload.as_bytes(), params.signature_hex).is_ok() {
            return Ok(());
        }
    }

    Err(AuthError::SignatureMismatch)
}

/// Verifies an incoming Origin request signature and timestamp window.
pub fn verify_origin_request(
    secret: &[u8],
    params: &OriginRequestParams<'_>,
    max_skew_secs: i64,
    current_time: i64,
) -> Result<(), AuthError> {
    verify_origin_request_with_rotation(secret, None, params, max_skew_secs, current_time)
}

/// Hardened verification: checks dual-key rotation, timestamp drift window, AND records/checks nonce against replay attacks.
pub fn verify_origin_request_hardened(
    primary_secret: &[u8],
    fallback_secret: Option<&[u8]>,
    params: &OriginRequestParams<'_>,
    nonce_tracker: &NonceTracker,
    max_skew_secs: i64,
    current_time: i64,
) -> Result<(), AuthError> {
    if params.nonce.trim().is_empty() {
        return Err(AuthError::MissingNonce);
    }

    // 1. Verify cryptographic signature & timestamp drift
    verify_origin_request_with_rotation(
        primary_secret,
        fallback_secret,
        params,
        max_skew_secs,
        current_time,
    )?;

    // 2. Anti-replay verification
    let expires_at = params.timestamp + max_skew_secs;
    nonce_tracker.check_and_record(params.edge_id, params.nonce, expires_at, current_time)?;

    Ok(())
}

/// Signs a client stream token.
pub fn sign_client_stream_token(secret: &[u8], claims: &StreamTokenClaims) -> String {
    sign_hmac_sha256(secret, claims.canonical_message().as_bytes())
}

/// Verifies a client stream token, checking both expiration and constant-time signature.
pub fn verify_client_stream_token(
    secret: &[u8],
    claims: &StreamTokenClaims,
    signature_hex: &str,
    current_time: i64,
) -> Result<(), AuthError> {
    if claims.expires_at < current_time {
        return Err(AuthError::TokenExpired {
            expired_at: claims.expires_at,
            current_time,
        });
    }

    verify_hmac_sha256(secret, claims.canonical_message().as_bytes(), signature_hex)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hmac_sign_and_verify() {
        let secret = b"super-secret-key-123";
        let data = b"video-stream-content-456";
        let sig = sign_hmac_sha256(secret, data);

        assert!(verify_hmac_sha256(secret, data, &sig).is_ok());
        assert!(verify_hmac_sha256(b"wrong-key", data, &sig).is_err());
        assert!(verify_hmac_sha256(secret, b"tampered-data", &sig).is_err());
    }

    #[test]
    fn test_client_token_expiration() {
        let secret = b"edge-secret";
        let claims = StreamTokenClaims::new("stream-1", "edge-de-01", None, 1000);
        let sig = sign_client_stream_token(secret, &claims);

        // Before expiry: OK
        assert!(verify_client_stream_token(secret, &claims, &sig, 999).is_ok());
        // At expiry: OK
        assert!(verify_client_stream_token(secret, &claims, &sig, 1000).is_ok());
        // After expiry: Err
        assert!(verify_client_stream_token(secret, &claims, &sig, 1001).is_err());
    }

    #[test]
    fn test_origin_request_signature_and_rotation() {
        let primary_secret = b"primary-secret-v2";
        let fallback_secret = b"old-secret-v1";
        let edge_id = "edge-01";
        let nonce = "random-nonce-9876";
        let ts = 1700000000;

        // 1. Signed with primary secret
        let sig_primary = sign_origin_request(
            primary_secret,
            "GET",
            "/hls/stream1/seg-01.m4s",
            ts,
            edge_id,
            nonce,
        );

        let params_primary = OriginRequestParams {
            method: "GET",
            path_and_query: "/hls/stream1/seg-01.m4s",
            timestamp: ts,
            edge_id,
            nonce,
            signature_hex: &sig_primary,
        };

        // Verifies against primary
        assert!(verify_origin_request_with_rotation(
            primary_secret,
            Some(fallback_secret),
            &params_primary,
            30,
            ts + 10
        )
        .is_ok());

        // 2. Signed with old fallback secret (during key rollover)
        let sig_fallback = sign_origin_request(
            fallback_secret,
            "GET",
            "/hls/stream1/seg-01.m4s",
            ts,
            edge_id,
            nonce,
        );

        let params_fallback = OriginRequestParams {
            method: "GET",
            path_and_query: "/hls/stream1/seg-01.m4s",
            timestamp: ts,
            edge_id,
            nonce,
            signature_hex: &sig_fallback,
        };

        // Verifies successfully against fallback
        assert!(verify_origin_request_with_rotation(
            primary_secret,
            Some(fallback_secret),
            &params_fallback,
            30,
            ts + 10
        )
        .is_ok());

        // Fails if fallback is None
        assert!(verify_origin_request_with_rotation(
            primary_secret,
            None,
            &params_fallback,
            30,
            ts + 10
        )
        .is_err());

        // Fails with completely unknown secret
        let sig_unknown = sign_origin_request(
            b"unknown-hacker-key",
            "GET",
            "/hls/stream1/seg-01.m4s",
            ts,
            edge_id,
            nonce,
        );
        let params_unknown = OriginRequestParams {
            method: "GET",
            path_and_query: "/hls/stream1/seg-01.m4s",
            timestamp: ts,
            edge_id,
            nonce,
            signature_hex: &sig_unknown,
        };
        assert!(verify_origin_request_with_rotation(
            primary_secret,
            Some(fallback_secret),
            &params_unknown,
            30,
            ts + 10
        )
        .is_err());
    }

    #[test]
    fn test_nonce_tracker_anti_replay() {
        let tracker = NonceTracker::new();
        let edge_id = "edge-de-01";
        let nonce = "unique-nonce-123";
        let now = 1700000000;
        let expires_at = now + 30;

        // First check succeeds
        assert!(tracker
            .check_and_record(edge_id, nonce, expires_at, now)
            .is_ok());
        assert_eq!(tracker.len(), 1);

        // Immediate replay fails
        let replay_res = tracker.check_and_record(edge_id, nonce, expires_at, now + 5);
        assert_eq!(
            replay_res,
            Err(AuthError::NonceReplayed {
                edge_id: edge_id.to_string(),
                nonce: nonce.to_string(),
            })
        );

        // Another edge node with the same nonce is accepted (isolated per edge)
        assert!(tracker
            .check_and_record("edge-it-02", nonce, expires_at, now + 5)
            .is_ok());
        assert_eq!(tracker.len(), 2);

        // Sweep expired nonces after expiration time
        let purged = tracker.sweep_expired(now + 31);
        assert_eq!(purged, 2);
        assert!(tracker.is_empty());
    }
}
