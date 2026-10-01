# Protocol & Interface Specifications - Distributed Rust Streaming CDN

This document defines the wire formats, HTTP contracts, and cryptographic standards governing communication across the cluster:
1. **Edge <-> Control Plane** (Registration & Heartbeats)
2. **Client <-> Control Plane** (Discovery & Token Issuance)
3. **Client <-> Edge** (Tokenized Media Delivery & HLS Rewriting)
4. **Edge <-> Origin** (HMAC-Signed Upstream Requests)
5. **Control Plane <-> Edge** (Cache Invalidation)

---

## 1. Edge <-> Control Plane Protocol

### 1.1 Edge Registration
On boot, an edge node registers with the Control Plane using its assigned static provisioning token.

- **Method & Path**: `POST /internal/edges/register`
- **Headers**:
  - `Content-Type: application/json`
  - `Authorization: Bearer <PROVISIONING_TOKEN>`
- **Request Body**:
```json
{
  "node_name": "edge-de-01",
  "hostname": "edge01.example.com",
  "public_port": 8443,
  "internal_port": 8444,
  "version": "0.1.0",
  "cpu_count": 2,
  "ram_total_mb": 2048,
  "cache_capacity_gb": 100,
  "max_connections": 5000,
  "max_streams": 500,
  "max_bandwidth_mbps": 1000,
  "weight": 1.0,
  "monthly_bandwidth_limit_gb": 20000
}
```
- **Response (`200 OK`)**:
```json
{
  "node_id": "018f3a5b-9d41-71e2-b91c-2df549c81120",
  "heartbeat_interval_secs": 15,
  "auth_secret": "c61b2a9e8f41...",
  "origin_base_url": "https://origin.internal.example.com",
  "origin_auth_secret": "e93f821a7...",
  "assigned_weight": 1.0,
  "status": "ONLINE"
}
```

---

### 1.2 Edge Heartbeat
Every 10–30 seconds, the edge transmits an aggregated telemetry snapshot to the Control Plane.

- **Method & Path**: `POST /internal/edges/{node_id}/heartbeat`
- **Headers**:
  - `Content-Type: application/json`
  - `Authorization: Bearer <AUTH_SECRET>`
- **Request Body**:
```json
{
  "timestamp": 1759258900,
  "uptime_secs": 84200,
  "status": "ONLINE",
  "cpu_percent": 14.5,
  "memory_used_mb": 112,
  "memory_total_mb": 2048,
  "active_connections": 182,
  "active_streams": 64,
  "bandwidth_in_bps": 42000000,
  "bandwidth_out_bps": 284000000,
  "cache_used_mb": 45200,
  "cache_capacity_mb": 102400,
  "cache_hit_ratio": 0.825,
  "origin_latency_ms": 18.2,
  "origin_requests_count": 1420,
  "origin_errors_count": 1,
  "monthly_bandwidth_used_bytes": 1425892048128
}
```
- **Response (`200 OK`)**:
```json
{
  "acknowledged": true,
  "next_heartbeat_secs": 15,
  "command": "NONE"
}
```
*Note*: `command` can be `"NONE"`, `"DRAIN"`, `"RELOAD_CONFIG"`, or `"TERMINATE"`.

---

## 2. Client <-> Control Plane (Stream Discovery)

### 2.1 Requesting Stream Access
Clients obtain an authorized edge redirection from the Control Plane.

- **Method & Path**: `GET /api/stream/{stream_id}`
- **Query Parameters**:
  - `client_ip` (optional, inferred from socket or proxy header)
- **Response (`307 Temporary Redirect` or `200 OK JSON`)**:
  - Redirect Location:
    `https://edge02.example.com:8443/hls/{stream_id}/master.m3u8?token=abc...&expires=1759262500`

---

## 3. Client <-> Edge Protocol

### 3.1 Signed URL Security Specification
Every media URL requested at an edge must contain valid HMAC signature parameters.

- **Token Construction**:
  ```
  Message = stream_id + ":" + client_ip + ":" + expires_timestamp + ":" + edge_id
  Signature = HMAC_SHA256(ClientSecret, Message)
  Token = HexEncode(Signature)
  ```
- **Verification Criteria**:
  1. `expires_timestamp >= current_unix_timestamp`
  2. Constant-time equality: `Verify(Token, HMAC_SHA256(Secret, Message)) == true`
  3. Optional client IP matching (or subnet mask `/24`)

### 3.2 HLS Rewriting & Edge Affinity
When an edge delivers a playlist (`.m3u8`), all relative or absolute URLs pointing to variant playlists (`720p.m3u8`) and media segments (`seg-1.m4s`) are rewritten:
- The edge preserves its own hostname / relative path.
- The signed security query parameters (`token=...&expires=...`) are propagated to every segment entry.
- This guarantees edge affinity and prevents client ping-pong between edges.

### 3.3 HTTP Byte-Range Delivery
Media segments and video files support standard RFC 7233 byte-range queries:
- Request: `Range: bytes=1048576-2097151`
- Response: `206 Partial Content`
- Headers:
  - `Accept-Ranges: bytes`
  - `Content-Range: bytes 1048576-2097151/15728640`
  - `Content-Length: 1048576`
  - `Content-Type: video/mp4` (or `video/iso.segment`)

---

## 4. Edge <-> Origin Protocol (Origin Shielding)

Edge nodes query the protected Origin using cryptographically signed requests.

- **Required Request Headers**:
  - `X-Edge-ID`: UUID of the requesting edge node.
  - `X-Timestamp`: Current UTC Unix timestamp.
  - `X-Signature`: Hex-encoded HMAC-SHA256 signature.
- **Canonical Signature String**:
  ```
  VERB + "\n" + PATH_AND_QUERY + "\n" + TIMESTAMP + "\n" + EDGE_ID
  ```
- **Origin Verification Window**:
  `abs(OriginCurrentTime - X-Timestamp) <= 30 seconds`

---

## 5. Cache Invalidation Protocol

- **Method & Path**: `POST /internal/cache/invalidate`
- **Request Body**:
```json
{
  "scope": "STREAM",
  "pattern": "stream-101/*",
  "reason": "Content updated by creator",
  "issued_at": 1759259100
}
```
Supported scopes:
- `EXACT`: Single segment/playlist file.
- `STREAM`: All variants and chunks belonging to a `stream_id`.
- `PREFIX`: Pattern matching on directory paths.

---

## 6. Health & Readiness Standard Endpoints

Both `stream-control` and `stream-edge` binaries implement:

### Liveness: `GET /health`
Returns `200 OK` if the process is responsive.
```json
{
  "status": "ok",
  "service": "stream-edge",
  "version": "0.1.0",
  "uptime_secs": 128
}
```

### Readiness: `GET /ready`
Returns `200 OK` if the node is initialized and ready to accept traffic; returns `503 Service Unavailable` if starting up, degraded, or draining.
```json
{
  "status": "ready",
  "node_id": "018f3a5b-9d41-71e2-b91c-2df549c81120",
  "state": "ONLINE"
}
```
