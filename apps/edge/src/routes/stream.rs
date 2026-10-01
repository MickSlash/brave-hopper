use axum::{
    extract::{ConnectInfo, Path, Request, State},
    http::{
        header::{self, HeaderName},
        HeaderMap, StatusCode,
    },
    response::{IntoResponse, Response},
    Json,
};
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::{engine, state::EdgeState, upstream::UpstreamClient};

#[derive(Clone)]
pub struct StreamRouteState {
    pub edge_state: EdgeState,
    pub upstream: Arc<UpstreamClient>,
}

#[derive(Default, Debug)]
struct TokenParams {
    expires: Option<i64>,
    sig: Option<String>,
    ip: Option<String>,
    edge_id: Option<String>,
}

fn parse_token_params(query: Option<&str>) -> TokenParams {
    let mut params = TokenParams::default();
    let query = match query {
        Some(q) => q,
        None => return params,
    };

    for pair in query.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            match k {
                "expires" => {
                    params.expires = v.parse::<i64>().ok();
                }
                "sig" | "signature" => {
                    params.sig = Some(v.to_string());
                }
                "ip" | "client_ip" => {
                    params.ip = Some(v.to_string());
                }
                "edge_id" => {
                    params.edge_id = Some(v.to_string());
                }
                _ => {}
            }
        }
    }

    params
}

fn extract_client_ip(headers: &HeaderMap, peer_addr: Option<SocketAddr>) -> Option<String> {
    if let Some(forwarded) = headers.get("x-forwarded-for") {
        if let Ok(s) = forwarded.to_str() {
            if let Some(first) = s.split(',').next() {
                let trimmed = first.trim();
                if !trimmed.is_empty() {
                    return Some(trimmed.to_string());
                }
            }
        }
    }

    if let Some(real_ip) = headers.get("x-real-ip") {
        if let Ok(s) = real_ip.to_str() {
            let trimmed = s.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }

    if let Some(addr) = peer_addr {
        return Some(addr.ip().to_string());
    }

    None
}

async fn validate_client_token(
    state: &EdgeState,
    stream_id: &str,
    query: Option<&str>,
    headers: &HeaderMap,
    peer_addr: Option<SocketAddr>,
) -> Result<(), Response> {
    if !state.auth_enabled.load(Ordering::Relaxed) {
        return Ok(());
    }

    let params = parse_token_params(query);

    let (expires, sig) = match (params.expires, params.sig) {
        (Some(e), Some(s)) => (e, s),
        _ => {
            return Err((
                StatusCode::FORBIDDEN,
                Json(serde_json::json!({
                    "error": "Missing signature or expiration in request query"
                })),
            )
                .into_response());
        }
    };

    let now = chrono::Utc::now().timestamp();
    if now > expires {
        return Err((
            StatusCode::GONE,
            Json(serde_json::json!({
                "error": "Stream token has expired",
                "expired_at": expires,
                "current_time": now
            })),
        )
            .into_response());
    }

    let secret = state.client_signing_secret.read().await;
    if secret.is_empty() {
        tracing::error!(
            "Stream token auth is enabled, but client_signing_secret is not configured"
        );
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": "Edge client signing key not configured"
            })),
        )
            .into_response());
    }

    let token_edge_id = params.edge_id.as_deref().unwrap_or("*");
    if token_edge_id != "*" && token_edge_id != state.config.edge.name {
        tracing::warn!(
            expected = %state.config.edge.name,
            got = %token_edge_id,
            "Edge node identifier mismatch in client token"
        );
        return Err((
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({
                "error": "Stream token not authorized for this edge node"
            })),
        )
            .into_response());
    }

    // IP validation
    let enforce_ip = state.enforce_ip.load(Ordering::Relaxed);
    if enforce_ip && params.ip.is_none() {
        return Err((
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({
                "error": "Client IP enforcement active but missing ip parameter"
            })),
        )
            .into_response());
    }

    let client_ip = extract_client_ip(headers, peer_addr);
    if let Some(ref claimed_ip) = params.ip {
        if let Some(ref actual_ip) = client_ip {
            if actual_ip != claimed_ip {
                tracing::warn!(
                    claimed = %claimed_ip,
                    actual = %actual_ip,
                    "Client IP address mismatch"
                );
                return Err((
                    StatusCode::FORBIDDEN,
                    Json(serde_json::json!({
                        "error": "Client IP mismatch"
                    })),
                )
                    .into_response());
            }
        }
    }

    let claims = protocol::StreamTokenClaims::new(stream_id, token_edge_id, params.ip, expires);

    match auth::verify_client_stream_token(secret.as_bytes(), &claims, &sig, now) {
        Ok(()) => Ok(()),
        Err(auth::AuthError::TokenExpired { .. }) => Err((
            StatusCode::GONE,
            Json(serde_json::json!({
                "error": "Stream token has expired"
            })),
        )
            .into_response()),
        Err(err) => {
            tracing::warn!(error = %err, "Cryptographic signature verification failed");
            Err((
                StatusCode::FORBIDDEN,
                Json(serde_json::json!({
                    "error": "Invalid token signature"
                })),
            )
                .into_response())
        }
    }
}

pub async fn handle_stream_request(
    State(route_state): State<StreamRouteState>,
    Path((stream_id, path)): Path<(String, String)>,
    req: Request,
) -> Response {
    if route_state.edge_state.get_status() == protocol::NodeStatus::Draining
        && path.ends_with(".m3u8")
    {
        tracing::warn!(
            stream_id = %stream_id,
            path = %path,
            "Edge node is DRAINING: rejecting new playback session manifest"
        );
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [(axum::http::header::RETRY_AFTER, "5")],
            Json(serde_json::json!({
                "error": "Edge node is draining for maintenance; please retry on an alternative edge",
                "code": "EDGE_DRAINING"
            })),
        )
            .into_response();
    }
    let peer_addr = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ci| ci.0);

    // Rate Limiting check per client IP
    let client_ip =
        extract_client_ip(req.headers(), peer_addr).unwrap_or_else(|| "127.0.0.1".to_string());
    if let crate::limiter::RateLimitResult::Denied {
        retry_after_secs,
        limit,
    } = route_state.edge_state.rate_limiter.check(&client_ip)
    {
        tracing::warn!(
            client_ip = %client_ip,
            retry_after = retry_after_secs,
            "Client IP exceeded rate limit"
        );
        return (
            StatusCode::TOO_MANY_REQUESTS,
            [
                (header::RETRY_AFTER, retry_after_secs.to_string()),
                (
                    HeaderName::from_static("x-ratelimit-limit"),
                    limit.round().to_string(),
                ),
                (
                    HeaderName::from_static("x-ratelimit-remaining"),
                    "0".to_string(),
                ),
            ],
            Json(serde_json::json!({
                "error": "Too Many Requests: Rate limit exceeded",
                "code": "RATE_LIMIT_EXCEEDED",
                "retry_after_secs": retry_after_secs
            })),
        )
            .into_response();
    }
    let query_str = req
        .uri()
        .query()
        .map(|q| format!("?{}", q))
        .unwrap_or_default();

    let query_ref = req.uri().query().map(|q| q.to_string());

    if let Err(resp) = validate_client_token(
        &route_state.edge_state,
        &stream_id,
        query_ref.as_deref(),
        req.headers(),
        peer_addr,
    )
    .await
    {
        return resp;
    }

    let method = req.method().clone();
    let prefix = route_state
        .edge_state
        .config
        .origin
        .path_prefix
        .trim_matches('/');
    let origin_path = if prefix.is_empty() {
        format!("/{}/{}{}", stream_id, path, query_str)
    } else {
        format!("/{}/{}/{}{}", prefix, stream_id, path, query_str)
    };

    engine::proxy_origin_stream(
        route_state.edge_state,
        route_state.upstream,
        method,
        &stream_id,
        &origin_path,
        req.headers(),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::EdgeConfig;
    use axum::http::HeaderMap;

    fn setup_test_state(secret: &str, enforce_ip: bool) -> EdgeState {
        let mut config = EdgeConfig::default();
        config.edge.name = "edge-test-01".to_string();
        config.auth.enabled = true;
        config.auth.client_signing_secret = secret.to_string();
        config.auth.enforce_ip = enforce_ip;
        EdgeState::new(config)
    }

    #[tokio::test]
    async fn test_validate_token_missing_params() {
        let state = setup_test_state("my-secret", false);
        let headers = HeaderMap::new();

        let res = validate_client_token(&state, "stream1", None, &headers, None).await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().status(), StatusCode::FORBIDDEN);

        let res2 =
            validate_client_token(&state, "stream1", Some("sig=abcd1234"), &headers, None).await;
        assert!(res2.is_err());
        assert_eq!(res2.unwrap_err().status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn test_validate_token_expired() {
        let state = setup_test_state("my-secret", false);
        let headers = HeaderMap::new();

        let expired_ts = chrono::Utc::now().timestamp() - 100;
        let query = format!("expires={}&sig=abcd", expired_ts);

        let res = validate_client_token(&state, "stream1", Some(&query), &headers, None).await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().status(), StatusCode::GONE);
    }

    #[tokio::test]
    async fn test_validate_token_valid_and_tampered() {
        let secret = "my-test-secret-key-123";
        let state = setup_test_state(secret, false);
        let headers = HeaderMap::new();

        let expires = chrono::Utc::now().timestamp() + 3600;
        let claims = protocol::StreamTokenClaims::new("stream1", "*", None, expires);
        let sig = auth::sign_client_stream_token(secret.as_bytes(), &claims);

        // Valid signature -> OK
        let valid_query = format!("expires={}&sig={}", expires, sig);
        let ok_res =
            validate_client_token(&state, "stream1", Some(&valid_query), &headers, None).await;
        assert!(ok_res.is_ok());

        // Tampered signature -> 403 Forbidden
        let tampered_query = format!("expires={}&sig={}ff", expires, &sig[..sig.len() - 2]);
        let err_res =
            validate_client_token(&state, "stream1", Some(&tampered_query), &headers, None).await;
        assert!(err_res.is_err());
        assert_eq!(err_res.unwrap_err().status(), StatusCode::FORBIDDEN);

        // Wrong stream ID -> 403 Forbidden
        let wrong_stream_res =
            validate_client_token(&state, "other-stream", Some(&valid_query), &headers, None).await;
        assert!(wrong_stream_res.is_err());
        assert_eq!(
            wrong_stream_res.unwrap_err().status(),
            StatusCode::FORBIDDEN
        );
    }

    #[tokio::test]
    async fn test_validate_token_edge_id_affinity() {
        let secret = "affinity-secret";
        let state = setup_test_state(secret, false);
        let headers = HeaderMap::new();

        let expires = chrono::Utc::now().timestamp() + 3600;

        // Token meant for this edge node "edge-test-01"
        let claims_correct =
            protocol::StreamTokenClaims::new("stream1", "edge-test-01", None, expires);
        let sig_correct = auth::sign_client_stream_token(secret.as_bytes(), &claims_correct);
        let query_correct = format!(
            "expires={}&sig={}&edge_id=edge-test-01",
            expires, sig_correct
        );

        let res =
            validate_client_token(&state, "stream1", Some(&query_correct), &headers, None).await;
        assert!(res.is_ok());

        // Token meant for another edge node "edge-eu-02"
        let claims_other = protocol::StreamTokenClaims::new("stream1", "edge-eu-02", None, expires);
        let sig_other = auth::sign_client_stream_token(secret.as_bytes(), &claims_other);
        let query_other = format!("expires={}&sig={}&edge_id=edge-eu-02", expires, sig_other);

        let res2 =
            validate_client_token(&state, "stream1", Some(&query_other), &headers, None).await;
        assert!(res2.is_err());
        assert_eq!(res2.unwrap_err().status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn test_validate_token_ip_binding() {
        let secret = "ip-binding-secret";
        let state = setup_test_state(secret, true); // enforce_ip = true
        let mut headers = HeaderMap::new();
        headers.insert("x-real-ip", "203.0.113.195".parse().unwrap());

        let expires = chrono::Utc::now().timestamp() + 3600;

        // Missing IP when enforce_ip is true -> Forbidden
        let claims_no_ip = protocol::StreamTokenClaims::new("stream1", "*", None, expires);
        let sig_no_ip = auth::sign_client_stream_token(secret.as_bytes(), &claims_no_ip);
        let query_no_ip = format!("expires={}&sig={}", expires, sig_no_ip);
        let res_no_ip =
            validate_client_token(&state, "stream1", Some(&query_no_ip), &headers, None).await;
        assert!(res_no_ip.is_err());
        assert_eq!(res_no_ip.unwrap_err().status(), StatusCode::FORBIDDEN);

        // Matching IP -> OK
        let claims_ip = protocol::StreamTokenClaims::new(
            "stream1",
            "*",
            Some("203.0.113.195".to_string()),
            expires,
        );
        let sig_ip = auth::sign_client_stream_token(secret.as_bytes(), &claims_ip);
        let query_ip = format!("expires={}&sig={}&ip=203.0.113.195", expires, sig_ip);
        let res_matching_ip =
            validate_client_token(&state, "stream1", Some(&query_ip), &headers, None).await;
        assert!(res_matching_ip.is_ok());

        // Mismatched IP -> Forbidden
        let query_mismatch = format!("expires={}&sig={}&ip=198.51.100.42", expires, sig_ip);
        let res_mismatch =
            validate_client_token(&state, "stream1", Some(&query_mismatch), &headers, None).await;
        assert!(res_mismatch.is_err());
        assert_eq!(res_mismatch.unwrap_err().status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn test_draining_edge_rejects_manifests() {
        let state = setup_test_state("my-secret", false);
        state.set_status(protocol::NodeStatus::Draining);

        let upstream = Arc::new(UpstreamClient::new());
        let route_state = StreamRouteState {
            edge_state: state,
            upstream,
        };

        let req = Request::builder()
            .uri("/streams/stream1/master.m3u8")
            .body(axum::body::Body::empty())
            .unwrap();

        let resp = handle_stream_request(
            State(route_state),
            Path(("stream1".to_string(), "master.m3u8".to_string())),
            req,
        )
        .await;

        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            resp.headers().get("retry-after").unwrap().to_str().unwrap(),
            "5"
        );
    }

    #[tokio::test]
    async fn test_rate_limiter_blocks_abusive_requests() {
        let mut config = EdgeConfig::default();
        config.edge.name = "edge-rate-test".to_string();
        config.auth.enabled = false;
        config.rate_limit.enabled = true;
        config.rate_limit.burst_capacity = 2.0;
        config.rate_limit.requests_per_second = 0.01;

        let state = EdgeState::new(config);
        let upstream = Arc::new(UpstreamClient::new());
        let route_state = StreamRouteState {
            edge_state: state,
            upstream,
        };

        // Helper to send request
        let send_req = |route_st: StreamRouteState| async move {
            let req = Request::builder()
                .uri("/stream/stream1/seg-01.m4s")
                .header("x-real-ip", "198.51.100.99")
                .body(axum::body::Body::empty())
                .unwrap();

            handle_stream_request(
                State(route_st),
                Path(("stream1".to_string(), "seg-01.m4s".to_string())),
                req,
            )
            .await
        };

        // Request 1: Allowed through rate limiter
        let resp1 = send_req(route_state.clone()).await;
        assert_ne!(resp1.status(), StatusCode::TOO_MANY_REQUESTS);

        // Request 2: Allowed through rate limiter
        let resp2 = send_req(route_state.clone()).await;
        assert_ne!(resp2.status(), StatusCode::TOO_MANY_REQUESTS);

        // Request 3: Blocked by rate limiter -> 429 Too Many Requests
        let resp3 = send_req(route_state.clone()).await;
        assert_eq!(resp3.status(), StatusCode::TOO_MANY_REQUESTS);
        let retry_after: u64 = resp3
            .headers()
            .get("retry-after")
            .unwrap()
            .to_str()
            .unwrap()
            .parse()
            .unwrap();
        assert!(retry_after >= 1);
        assert_eq!(
            resp3
                .headers()
                .get("x-ratelimit-limit")
                .unwrap()
                .to_str()
                .unwrap(),
            "2"
        );
        assert_eq!(
            resp3
                .headers()
                .get("x-ratelimit-remaining")
                .unwrap()
                .to_str()
                .unwrap(),
            "0"
        );
    }
}
