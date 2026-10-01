use axum::{
    body::Body,
    http::{header, HeaderMap, HeaderName, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use bytes::Bytes;
use common::{
    format_content_range, format_unsatisfiable_content_range, rewrite_hls_playlist, HttpRange,
    RangeError,
};
use futures_util::Stream;
use serde_json::json;
use std::{
    io::SeekFrom,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
};
use tokio::io::{AsyncRead, AsyncSeekExt, ReadBuf};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::{
    cache::{CacheHit, CacheKey},
    singleflight::{FlightMetadata, FollowerHandle, LeaderGuard, LeaderHandle},
    state::EdgeState,
    upstream::UpstreamClient,
};

/// Message sent to background disk writer task during cache miss streaming.
enum CacheDiskMessage {
    Chunk(Bytes),
    Complete { content_type: Option<String> },
}

/// RAII Guard that manages active stream lifecycle and increments byte counters.
/// When the downstream client disconnects or finishes, active_streams is decremented
/// and request summary metrics are emitted.
struct StreamGuard {
    request_id: Uuid,
    path: String,
    start_time: Instant,
    state: EdgeState,
    bytes_transferred: u64,
    is_origin: bool,
}

impl StreamGuard {
    fn new_origin_stream(request_id: Uuid, path: String, state: EdgeState) -> Self {
        state.metrics.inc_active_streams();
        state.metrics.inc_requests();
        state.metrics.inc_origin_requests();

        Self {
            request_id,
            path,
            start_time: Instant::now(),
            state,
            bytes_transferred: 0,
            is_origin: true,
        }
    }

    fn new_cache_hit(request_id: Uuid, path: String, state: EdgeState) -> Self {
        state.metrics.inc_active_streams();
        state.metrics.inc_requests();
        state.metrics.inc_cache_hit();

        Self {
            request_id,
            path,
            start_time: Instant::now(),
            state,
            bytes_transferred: 0,
            is_origin: false,
        }
    }

    fn new_coalesced_follower(request_id: Uuid, path: String, state: EdgeState) -> Self {
        state.metrics.inc_active_streams();
        state.metrics.inc_requests();

        Self {
            request_id,
            path,
            start_time: Instant::now(),
            state,
            bytes_transferred: 0,
            is_origin: false,
        }
    }

    fn record_chunk(&mut self, size: usize) {
        self.bytes_transferred += size as u64;
        self.state.metrics.add_bytes_out(size as u64);
        if self.is_origin {
            self.state.metrics.add_bytes_in(size as u64);
        }
    }
}

impl Drop for StreamGuard {
    fn drop(&mut self) {
        self.state.metrics.dec_active_streams();
        let elapsed = self.start_time.elapsed().as_millis();
        info!(
            request_id = %self.request_id,
            path = %self.path,
            is_origin = self.is_origin,
            duration_ms = elapsed,
            bytes = self.bytes_transferred,
            "Stream closed (completed or client disconnected)"
        );
    }
}

/// Pinned stream adapter that reads from local filesystem cache with bounded memory buffers.
struct FileReaderStream {
    file: tokio::fs::File,
    remaining: u64,
    guard: StreamGuard,
}

impl Stream for FileReaderStream {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.remaining == 0 {
            return Poll::Ready(None);
        }

        let max_to_read = std::cmp::min(self.remaining, 64 * 1024) as usize;
        let mut buf = vec![0u8; max_to_read];
        let mut read_buf = ReadBuf::new(&mut buf);

        match Pin::new(&mut self.file).poll_read(cx, &mut read_buf) {
            Poll::Ready(Ok(())) => {
                let bytes_read = read_buf.filled().len();
                if bytes_read == 0 {
                    self.remaining = 0;
                    Poll::Ready(None)
                } else {
                    self.remaining = self.remaining.saturating_sub(bytes_read as u64);
                    buf.truncate(bytes_read);
                    let bytes = Bytes::from(buf);
                    self.guard.record_chunk(bytes.len());
                    Poll::Ready(Some(Ok(bytes)))
                }
            }
            Poll::Ready(Err(e)) => {
                error!(error = %e, "Error reading chunk from local cache file");
                Poll::Ready(Some(Err(e)))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Stream adapter for SingleFlight coalesced followers.
struct MeteredFollowerStream<S> {
    inner: S,
    guard: StreamGuard,
}

impl<S> Stream for MeteredFollowerStream<S>
where
    S: Stream<Item = Result<Bytes, std::io::Error>> + Unpin,
{
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(bytes))) => {
                self.guard.record_chunk(bytes.len());
                Poll::Ready(Some(Ok(bytes)))
            }
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// A pinned stream adapter that wraps an upstream chunk stream with RAII telemetry tracking,
/// background disk cache tee, and live broadcasting to SingleFlight followers.
struct MeteredStream<S> {
    inner: S,
    guard: StreamGuard,
    disk_tx: Option<tokio::sync::mpsc::Sender<CacheDiskMessage>>,
    content_type: Option<String>,
    leader_handle: Option<LeaderHandle>,
    _leader_guard: Option<LeaderGuard>,
}

impl<S> Stream for MeteredStream<S>
where
    S: Stream<Item = Result<Bytes, reqwest::Error>> + Unpin,
{
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(bytes))) => {
                self.guard.record_chunk(bytes.len());

                // Broadcast chunk to waiting singleflight followers
                if let Some(lh) = &self.leader_handle {
                    lh.publish_chunk(bytes.clone());
                }

                // Tee chunk to background cache disk writer if active
                if let Some(tx) = &self.disk_tx {
                    if let Err(tokio::sync::mpsc::error::TrySendError::Full(_)) =
                        tx.try_send(CacheDiskMessage::Chunk(bytes.clone()))
                    {
                        warn!(
                            "Disk cache writer channel full; disabling cache tee for this request"
                        );
                        self.disk_tx = None;
                    } else if let Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) =
                        tx.try_send(CacheDiskMessage::Chunk(bytes.clone()))
                    {
                        self.disk_tx = None;
                    }
                }

                Poll::Ready(Some(Ok(bytes)))
            }
            Poll::Ready(Some(Err(err))) => {
                error!(error = %err, "Error reading upstream chunk from origin");
                self.guard.state.metrics.inc_origin_errors();
                self.disk_tx = None; // Abort cache write on upstream error
                if let Some(lh) = self.leader_handle.take() {
                    lh.publish_error(err.to_string());
                }
                Poll::Ready(Some(Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    err.to_string(),
                ))))
            }
            Poll::Ready(None) => {
                // Upstream transfer completed successfully
                if let Some(tx) = self.disk_tx.take() {
                    let _ = tx.try_send(CacheDiskMessage::Complete {
                        content_type: self.content_type.clone(),
                    });
                }
                if let Some(lh) = self.leader_handle.take() {
                    lh.finish();
                }
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Core streaming pass-through:
/// 1. Checks local filesystem cache for media segments (Cache HIT).
/// 2. Checks SingleFlight deduplication map to coalesce concurrent cache misses.
/// 3. If Leader: requests upstream origin with connection pooling, broadcasts to followers, and tees to disk.
/// 4. Handles dynamic HLS playlist rewriting for edge affinity.
pub async fn proxy_origin_stream(
    state: EdgeState,
    upstream: Arc<UpstreamClient>,
    client_method: Method,
    stream_id: &str,
    path_and_query: &str,
    client_headers: &HeaderMap,
) -> Response {
    let request_id = Uuid::new_v4();
    let edge_id = state
        .node_id
        .read()
        .await
        .map(|id| id.to_string())
        .unwrap_or_else(|| state.config.edge.name.clone());

    let is_hls_playlist = path_and_query.contains(".m3u8");
    let is_cacheable_segment = !is_hls_playlist
        && (client_method == Method::GET || client_method == Method::HEAD)
        && (path_and_query.contains(".m4s")
            || path_and_query.contains(".mp4")
            || path_and_query.contains(".ts")
            || path_and_query.contains(".bin")
            || path_and_query.contains(".aac")
            || path_and_query.contains(".vtt"));

    let cache_key = if is_cacheable_segment {
        Some(CacheKey::new(stream_id, path_and_query))
    } else {
        None
    };

    // ==========================================
    // 1. LOCAL CACHE LOOKUP (Cache HIT)
    // ==========================================
    if let Some(ref key) = cache_key {
        if let Some(hit) = state.cache.lookup(key).await {
            debug!(
                request_id = %request_id,
                path = %path_and_query,
                file = ?hit.full_path,
                "Cache HIT: Serving media chunk directly from disk"
            );

            return serve_cache_hit(
                state,
                request_id,
                edge_id,
                path_and_query,
                hit,
                client_headers,
                client_method,
            )
            .await;
        }
    }

    // ==========================================
    // 2. SINGLE-FLIGHT REQUEST COALESCING CHECK
    // ==========================================
    let mut leader_registration = None;

    if is_cacheable_segment && client_method == Method::GET {
        if let Some(ref key) = cache_key {
            // Attempt to attach to an active in-flight Leader
            if let Some(follower) = state.single_flight.join(&key.hash).await {
                debug!(
                    request_id = %request_id,
                    path = %path_and_query,
                    hash = %key.hash,
                    "SingleFlight: Coalescing request onto active in-flight leader"
                );
                return serve_coalesced_follower(
                    state,
                    request_id,
                    edge_id,
                    path_and_query,
                    follower,
                )
                .await;
            }

            // No active leader exists; this request becomes the Leader
            leader_registration = Some(state.single_flight.register(&key.hash).await);
        }
    }

    // ==========================================
    // 3. UPSTREAM FETCH (Cache MISS or Pass-Through)
    // ==========================================
    if is_cacheable_segment {
        state.metrics.inc_cache_miss();
        debug!(
            request_id = %request_id,
            path = %path_and_query,
            "Cache MISS: Leader fetching from origin and teeing to cache/followers"
        );
    }

    let (origin_url, origin_secret) = {
        let url = state.origin_base_url.read().await.clone();
        let secret = state.origin_auth_secret.read().await.clone();
        (url, secret)
    };

    let upstream_resp = match upstream
        .fetch_stream(
            &origin_url,
            &origin_secret,
            &edge_id,
            client_method.clone(),
            path_and_query,
            client_headers,
        )
        .await
    {
        Ok(resp) => resp,
        Err(err) => {
            state.metrics.inc_origin_errors();
            error!(request_id = %request_id, error = %err, "Origin connection failed");

            // Notify singleflight followers of origin failure
            if let Some((ref lh, _)) = leader_registration {
                lh.publish_error(err.to_string());
            }

            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({ "error": "Origin unavailable", "details": err.to_string() })),
            )
                .into_response();
        }
    };

    let status = StatusCode::from_u16(upstream_resp.status().as_u16())
        .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);

    let mut response_headers = HeaderMap::new();

    // Forward essential media and caching headers
    let allowed_headers = [
        header::CONTENT_TYPE,
        header::CONTENT_LENGTH,
        header::ACCEPT_RANGES,
        header::CONTENT_RANGE,
        header::ETAG,
        header::LAST_MODIFIED,
        header::CACHE_CONTROL,
    ];

    for name in &allowed_headers {
        if let Some(val) = upstream_resp.headers().get(name) {
            if let Ok(val_str) = val.to_str() {
                if let Ok(header_val) = HeaderValue::from_str(val_str) {
                    response_headers.insert(name.clone(), header_val);
                }
            }
        }
    }

    // Attach custom cluster telemetry and cache status headers
    if let Ok(hv) = HeaderValue::from_str(&format!("edge={},req={}", edge_id, request_id)) {
        response_headers.insert(HeaderName::from_static("x-stream-edge"), hv);
    }

    if is_cacheable_segment {
        response_headers.insert(
            HeaderName::from_static("x-cache-status"),
            HeaderValue::from_static("MISS"),
        );
    }

    // Broadcast headers and status to all waiting SingleFlight followers
    if let Some((ref lh, _)) = leader_registration {
        lh.publish_metadata(FlightMetadata {
            status,
            headers: response_headers.clone(),
        });
    }

    // ==========================================
    // 4. DYNAMIC HLS MANIFEST REWRITING
    // ==========================================
    if is_hls_playlist && status == StatusCode::OK {
        let query_params = path_and_query
            .find('?')
            .map(|i| &path_and_query[i + 1..])
            .unwrap_or_default();

        let body_bytes = match upstream_resp.bytes().await {
            Ok(b) => b,
            Err(e) => {
                state.metrics.inc_origin_errors();
                error!(error = %e, "Failed reading playlist body from origin");
                return (
                    StatusCode::BAD_GATEWAY,
                    "Failed reading playlist from origin",
                )
                    .into_response();
            }
        };

        state.metrics.add_bytes_in(body_bytes.len() as u64);
        let playlist_str = String::from_utf8_lossy(&body_bytes);
        let rewritten = rewrite_hls_playlist(&playlist_str, query_params);
        let rewritten_len = rewritten.len();
        state.metrics.add_bytes_out(rewritten_len as u64);
        state.metrics.inc_requests();

        response_headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/vnd.apple.mpegurl"),
        );
        response_headers.insert(
            header::CONTENT_LENGTH,
            HeaderValue::from_str(&rewritten_len.to_string()).unwrap(),
        );

        return (status, response_headers, rewritten).into_response();
    }

    // ==========================================
    // 5. CACHE MISS STREAMING, BROADCAST & DISK TEE
    // ==========================================
    let mut disk_tx = None;
    let content_type = response_headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    // Only cache full 200 OK responses for cacheable segments
    if is_cacheable_segment && status == StatusCode::OK && client_method == Method::GET {
        if let Some(ref key) = cache_key {
            let estimated_size = response_headers
                .get(header::CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok());

            match state.cache.start_write(key, estimated_size).await {
                Ok(writer) => {
                    let (tx, mut rx) = tokio::sync::mpsc::channel::<CacheDiskMessage>(16);
                    let cache_mgr = state.cache.clone();

                    let task_content_type = content_type.clone();
                    // Background disk write task
                    tokio::spawn(async move {
                        let mut writer = writer;
                        let mut committed = false;

                        while let Some(msg) = rx.recv().await {
                            match msg {
                                CacheDiskMessage::Chunk(chunk) => {
                                    if let Err(e) = writer.write_chunk(&chunk).await {
                                        warn!(
                                            error = %e,
                                            "Cache disk write failed, aborting cache file"
                                        );
                                        writer.abort().await;
                                        return;
                                    }
                                    if let Some(expected) = estimated_size {
                                        if writer.bytes_written >= expected && expected > 0 {
                                            if let Err(e) = cache_mgr
                                                .commit_write(
                                                    &mut writer,
                                                    task_content_type.clone(),
                                                )
                                                .await
                                            {
                                                warn!(
                                                    error = %e,
                                                    "Failed to commit cache file to index"
                                                );
                                            } else {
                                                committed = true;
                                            }
                                            break;
                                        }
                                    }
                                }
                                CacheDiskMessage::Complete { content_type } => {
                                    if !committed {
                                        if let Err(e) =
                                            cache_mgr.commit_write(&mut writer, content_type).await
                                        {
                                            warn!(
                                                error = %e,
                                                "Failed to commit cache file to index"
                                            );
                                        } else {
                                            committed = true;
                                        }
                                    }
                                    break;
                                }
                            }
                        }

                        if !committed {
                            if let Some(expected) = estimated_size {
                                if writer.bytes_written >= expected && expected > 0 {
                                    let _ = cache_mgr
                                        .commit_write(&mut writer, task_content_type)
                                        .await;
                                    committed = true;
                                }
                            }
                        }

                        if !committed {
                            writer.abort().await;
                        }
                    });

                    disk_tx = Some(tx);
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        "Could not initialize cache file, degrading to direct streaming"
                    );
                }
            }
        }
    }

    let (leader_handle, leader_guard) = match leader_registration {
        Some((lh, lg)) => (Some(lh), Some(lg)),
        None => (None, None),
    };

    let guard = StreamGuard::new_origin_stream(request_id, path_and_query.to_string(), state);
    let chunk_stream = upstream_resp.bytes_stream();
    let metered_stream = MeteredStream {
        inner: chunk_stream,
        guard,
        disk_tx,
        content_type,
        leader_handle,
        _leader_guard: leader_guard,
    };

    (status, response_headers, Body::from_stream(metered_stream)).into_response()
}

/// Serves a coalesced follower attached to an active in-flight Leader.
async fn serve_coalesced_follower(
    state: EdgeState,
    request_id: Uuid,
    edge_id: String,
    path_and_query: &str,
    mut follower: FollowerHandle,
) -> Response {
    let metadata = match follower.wait_for_metadata().await {
        Ok(m) => m,
        Err(err) => {
            error!(request_id = %request_id, error = %err, "SingleFlight leader failed");
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({ "error": "Coalesced request failed", "details": err })),
            )
                .into_response();
        }
    };

    let mut response_headers = metadata.headers;

    // Attach custom cluster telemetry and coalesced cache status header
    if let Ok(hv) = HeaderValue::from_str(&format!("edge={},req={}", edge_id, request_id)) {
        response_headers.insert(HeaderName::from_static("x-stream-edge"), hv);
    }
    response_headers.insert(
        HeaderName::from_static("x-cache-status"),
        HeaderValue::from_static("COALESCED"),
    );

    let guard = StreamGuard::new_coalesced_follower(request_id, path_and_query.to_string(), state);
    let follower_stream = follower.into_stream();
    let metered = MeteredFollowerStream {
        inner: follower_stream,
        guard,
    };

    (
        metadata.status,
        response_headers,
        Body::from_stream(metered),
    )
        .into_response()
}

/// Serves a cache hit directly from disk, supporting HTTP range requests (RFC 7233).
async fn serve_cache_hit(
    state: EdgeState,
    request_id: Uuid,
    edge_id: String,
    path_and_query: &str,
    hit: CacheHit,
    client_headers: &HeaderMap,
    client_method: Method,
) -> Response {
    let mut file = match tokio::fs::File::open(&hit.full_path).await {
        Ok(f) => f,
        Err(e) => {
            warn!(error = %e, path = ?hit.full_path, "Cache file open error, miss fallback");
            return (StatusCode::INTERNAL_SERVER_ERROR, "Cache file unreadable").into_response();
        }
    };

    let total_size = hit.size_bytes;
    let mut response_headers = HeaderMap::new();

    // Set content type
    let content_type = hit
        .content_type
        .unwrap_or_else(|| "application/octet-stream".to_string());
    if let Ok(hv) = HeaderValue::from_str(&content_type) {
        response_headers.insert(header::CONTENT_TYPE, hv);
    }

    // Set cluster & cache headers
    if let Ok(hv) = HeaderValue::from_str(&format!("edge={},req={}", edge_id, request_id)) {
        response_headers.insert(HeaderName::from_static("x-stream-edge"), hv);
    }
    response_headers.insert(
        HeaderName::from_static("x-cache-status"),
        HeaderValue::from_static("HIT"),
    );
    response_headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));

    // Check Range header
    let range_header = client_headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok());

    let (status, start_offset, stream_len) = if let Some(range_raw) = range_header {
        match HttpRange::parse(range_raw) {
            Ok(spec) => match spec.resolve(total_size) {
                Ok((start, end)) => {
                    let content_range = format_content_range(start, end, total_size);
                    if let Ok(hv) = HeaderValue::from_str(&content_range) {
                        response_headers.insert(header::CONTENT_RANGE, hv);
                    }
                    let length = end - start + 1;
                    (StatusCode::PARTIAL_CONTENT, start, length)
                }
                Err(RangeError::Unsatisfiable { total_len }) => {
                    let content_range = format_unsatisfiable_content_range(total_len);
                    if let Ok(hv) = HeaderValue::from_str(&content_range) {
                        response_headers.insert(header::CONTENT_RANGE, hv);
                    }
                    state.metrics.inc_cache_hit();
                    state.metrics.inc_requests();
                    return (StatusCode::RANGE_NOT_SATISFIABLE, response_headers).into_response();
                }
                Err(_) => (StatusCode::OK, 0, total_size),
            },
            Err(_) => (StatusCode::OK, 0, total_size),
        }
    } else {
        (StatusCode::OK, 0, total_size)
    };

    response_headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&stream_len.to_string()).unwrap(),
    );

    if client_method == Method::HEAD {
        state.metrics.inc_cache_hit();
        state.metrics.inc_requests();
        return (status, response_headers).into_response();
    }

    if start_offset > 0 {
        if let Err(e) = file.seek(SeekFrom::Start(start_offset)).await {
            error!(error = %e, "Failed to seek cache file for byte-range request");
            return (StatusCode::INTERNAL_SERVER_ERROR, "File seek error").into_response();
        }
    }

    let guard = StreamGuard::new_cache_hit(request_id, path_and_query.to_string(), state);
    let file_stream = FileReaderStream {
        file,
        remaining: stream_len,
        guard,
    };

    (status, response_headers, Body::from_stream(file_stream)).into_response()
}
