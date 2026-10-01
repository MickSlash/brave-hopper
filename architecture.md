# Architecture Overview - Distributed High-Performance Rust Streaming CDN

## 1. Architectural Philosophy

The streaming platform is designed from the ground up for **extreme resource efficiency**, **horizontal scalability**, **bounded memory footprint**, and **resilience against origin overload and network degradation**.

### Core Tenets
1. **Intelligent Control Plane, Ultra-Lean Edge Nodes**: The Control Plane manages topology, scheduling, telemetry, authentication tokens, and administrative operations. The Edge nodes are lightweight, stateless proxies with local disk caching—designed to be disposable, crash-resilient, and independent of the Control Plane's real-time availability.
2. **Control Plane Out of the Critical Data Path**: Clients receive signed redirect tokens from the Control Plane (or edge discovery service) once. All subsequent media streams, playlists, and chunk transfers flow strictly between `Client <-> Edge <-> Local Disk Cache / Origin`. If the Control Plane goes down, active and existing streams continue uninterrupted.
3. **Bounded Buffering & Strict Backpressure**: Video segments are never buffered entirely in RAM (`Origin -> RAM -> Client` is prohibited). Data is streamed through small, bounded chunk buffers (configurable: 16 KB – 128 KB). If a client is slow or stalls, reading from the upstream Origin pauses automatically via Tokio backpressure. If the client disconnects, the upstream request is cancelled immediately.
4. **Resilient Local Caching**: File-based caching using a two-level directory hash hierarchy (`cache/ab/cd/{hash}.m4s`). Disk writes are atomic (`.tmp` write followed by atomic rename). Cache reads hit the filesystem directly. If disk space is exhausted or write errors occur, the edge gracefully degrades to direct upstream streaming without breaking the playback experience.
5. **Single-Flight Request Coalescing**: If 100 clients concurrently request a video chunk that is not yet cached (cache miss), the edge node issues exactly **1 upstream request to the Origin** and streams the incoming data simultaneously to the disk writer and all waiting client listeners via a streaming tee.

---

## 2. High-Level Component Architecture

```
                                  ┌──────────────────────────────┐
                                  │      ADMINISTRATOR / WEB     │
                                  │     (Browser / API Client)   │
                                  └──────────────┬───────────────┘
                                                 │
                                                 │ HTTPS / SSE
                                                 ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                                     CONTROL PLANE                                      │
│                                (Binary: stream-control)                                │
│                                                                                        │
│  - REST API & Token Issuance (/api/stream/{id})                                        │
│  - Edge Discovery & Health Evaluation (Online / Degraded / Offline / Draining)         │
│  - Node Registration & Heartbeat Ingestion                                             │
│  - Lightweight Admin Dashboard (Askama + HTMX + Server-Sent Events)                   │
│  - PostgreSQL (SQLx) for persistent topology, tokens, hourly/daily aggregates          │
└────────────────────────────────────────┬───────────────────────────────────────────────┘
                                         │
                     ┌───────────────────┼───────────────────┐
                     │ Registration      │ Heartbeats        │ Discovery
                     │ & Config Sync     │ & Realtime Stats  │ & Token Verification
                     ▼                   ▼                   ▼
          ┌─────────────────────┐┌─────────────────────┐┌─────────────────────┐
          │      EDGE #01       ││      EDGE #02       ││      EDGE #03       │
          │ (Binary: stream-edge││ (Binary: stream-edge││ (Binary: stream-edge│
          │  Target: 512MB RAM  ││  Target: 1-2GB RAM  ││  Target: Atom 230   │
          │  Local FS Cache     ││  Local FS Cache     ││  Local FS Cache     │
          │  Single-Flight Engine││ Single-Flight Engine││ Single-Flight Engine│
          └──────────┬──────────┘└──────────┬──────────┘└──────────┬──────────┘
                     │                      │                      │
                     │ HMAC-Signed Request  │ HMAC-Signed Request  │ HMAC-Signed Request
                     │ (X-Edge-ID, Sig)     │ (X-Edge-ID, Sig)     │ (X-Edge-ID, Sig)
                     └──────────────────────┼──────────────────────┘
                                            │
                                            ▼
                           ┌─────────────────────────────────┐
                           │          ORIGIN SERVER          │
                           │   (Protected Storage Backend)   │
                           │    Master / Variant Playlists   │
                           │       fMP4 (.m4s) / TS Chunks   │
                           └─────────────────────────────────┘
```

---

## 3. Component Details & Responsibilities

### 3.1 Control Plane (`stream-control`)
- **API & Scheduler**:
  - Validates client stream requests and computes candidate edge nodes based on real-time load metrics (active connections, active streams, CPU, memory, bandwidth saturation, origin latency, error rates, and node weights).
  - Emits cryptographically signed, short-lived streaming URLs (HMAC-SHA256) binding client to edge and resource.
- **Node Management**:
  - Handles initial edge registration with hardware specification handshake (`cpu_count`, `ram_total_mb`, `cache_capacity_gb`, `max_connections`).
  - Processes heartbeats (every 10–30s) tracking health and transition nodes between `ONLINE`, `DEGRADED`, and `OFFLINE`.
  - Supports administrative graceful drain commands for zero-downtime maintenance.
- **Admin UI & Telemetry**:
  - Rendered via Askama templates, dynamic live updates powered by HTMX and Server-Sent Events (SSE). No heavy Single-Page Application (SPA) bundle.
  - Ingests aggregated atomic metrics into PostgreSQL tables with hourly and daily rollups.

### 3.2 Edge Node (`stream-edge`)
- **Ultra-Lean Streaming Engine**:
  - Implemented in Rust on Tokio, Axum, and Hyper.
  - Zero heavy local databases: no PostgreSQL, Redis, or embedded complex DBs.
  - In-memory index with fast filesystem traversal and reconstruction.
- **HLS Processing & Edge Affinity**:
  - Rewrites incoming HLS playlists (`.m3u8`) so variant streams and child segments explicitly preserve edge affinity (avoiding cache misses caused by ping-ponging segments across edges).
  - Preserves signed security tokens and query parameters across playlist traversals.
- **HTTP Range & Seek Support**:
  - Transparent support for byte-range requests (`Range`, `Accept-Ranges: bytes`, `Content-Range`, `206 Partial Content`, `416 Range Not Satisfiable`).
- **Disk Cache & Single-Flight Coalescing**:
  - Cache directory structure: `cache/{xx}/{yy}/{hash}.m4s`.
  - Atomic writes: `hash.tmp.{pid}.{nonce}` -> `sync_data()` -> `rename()` -> `hash.m4s`.
  - In-flight request deduplication: When a cache miss occurs, subsequent concurrent requests for the identical key attach to a broadcast stream rather than bombarding the Origin.
- **Resource Footprint Targets**:
  - **Minimal (Atom 230 / Low Resource Mode)**: 1 vCPU, 512 MB RAM, Tokio worker threads = 1.
  - **Standard VPS**: 1–2 vCPU, 1–2 GB RAM, 1 Gbps uplink.

### 3.3 Origin Server Shielding
- Origins are completely isolated from direct public internet access.
- Authentication between Edge and Origin uses HMAC-SHA256 request signatures:
  - Header `X-Edge-ID`: Identifier assigned by Control Plane.
  - Header `X-Timestamp`: Unix timestamp (preventing replay attacks within a 30-second window).
  - Header `X-Signature`: `HMAC_SHA256(OriginSecret, METHOD + " " + PATH + "\n" + TIMESTAMP + "\n" + EDGE_ID)`.
- Optional IP allowlisting and mTLS support for enterprise deployments.

---

## 4. Hardware Profiles & Resource Budgets

| Profile | Target Hardware | Tokio Workers | RAM Idle Target | RAM Active (100 streams) |
|---|---|---|---|---|
| **Atom 230 / Embedded** | 1 Core, 512 MB RAM, 100 Mbps | 1 | < 15 MB | < 45 MB |
| **Standard VPS** | 1-2 vCPU, 1-2 GB RAM, 1 Gbps | Auto (num CPUs) | < 25 MB | < 90 MB |
| **Dedicated High-Bandwidth** | 4-8 vCPU, 8-16 GB RAM, 10 Gbps | Auto (num CPUs) | < 40 MB | < 250 MB |

---

## 5. Failure Modes & Degradation Strategies

1. **Control Plane Offline**:
   - Edge nodes continue serving existing clients and verifying new client tokens using cached symmetric secrets / public keys.
   - Heartbeat sender retries with exponential backoff and jitter without interrupting client streaming tasks.
2. **Local Disk Cache Full**:
   - Background LRU eviction purges oldest accessed objects once high-water mark (e.g. 90% disk quota) is crossed.
   - If disk write fails (`ENOSPC` or filesystem read-only), the edge switches to streaming pass-through directly from Origin to Client without failing the client stream.
3. **Client Disconnect / Slow Client**:
   - Bounded channel backpressure halts reading from Origin when client socket buffer is saturated.
   - Socket disconnect immediately drops the tokio task and aborts the upstream origin client channel.
4. **Origin Latency Spike / Timeout**:
   - Configurable connect and read timeouts (default: 3s connect, 10s chunk read). Circuit breaker marks Origin as degraded and reports to Control Plane.
