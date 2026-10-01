use axum::http::{HeaderMap, StatusCode};
use bytes::Bytes;
use futures_util::Stream;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock as StdRwLock},
};
use tokio::sync::{broadcast, watch, RwLock as TokioRwLock};
use tracing::{debug, warn};

/// Maximum live broadcast backlog before slow followers are lagged
const BROADCAST_CAPACITY: usize = 256;

#[derive(Debug, Clone)]
pub struct FlightMetadata {
    pub status: StatusCode,
    pub headers: HeaderMap,
}

#[derive(Clone)]
pub enum FlightMessage {
    Chunk(Bytes),
    Complete,
    Error(String),
}

/// An active in-flight request being executed by a Leader.
struct InFlightLeader {
    /// Broadcast channel transmitting chunks to all connected followers
    tx: broadcast::Sender<FlightMessage>,
    /// Watch channel notifying followers when response status and headers are ready
    meta_rx: watch::Receiver<Option<Result<FlightMetadata, String>>>,
    /// In-memory buffer storing chunks received so far for late-joining followers
    buffered_chunks: Arc<StdRwLock<Vec<Bytes>>>,
    /// Flag indicating whether the upstream transfer has finished
    is_finished: Arc<StdRwLock<bool>>,
}

/// Single-Flight Request Coalescing Engine.
/// Deduplicates concurrent cache misses so only 1 upstream request is issued to Origin.
#[derive(Clone)]
pub struct SingleFlight {
    leaders: Arc<TokioRwLock<HashMap<String, Arc<InFlightLeader>>>>,
}

impl Default for SingleFlight {
    fn default() -> Self {
        Self::new()
    }
}

impl SingleFlight {
    pub fn new() -> Self {
        Self {
            leaders: Arc::new(TokioRwLock::new(HashMap::new())),
        }
    }

    /// Attempts to join an existing in-flight request as a Follower.
    /// If no leader is in flight, returns `None`, signaling the caller to become the Leader.
    pub async fn join(&self, key: &str) -> Option<FollowerHandle> {
        let guard = self.leaders.read().await;
        let leader = guard.get(key)?.clone();
        drop(guard);

        Some(FollowerHandle {
            rx: leader.tx.subscribe(),
            meta_rx: leader.meta_rx.clone(),
            buffered_chunks: leader.buffered_chunks.clone(),
            is_finished: leader.is_finished.clone(),
        })
    }

    /// Registers a new Leader in the in-flight map.
    /// Returns a `LeaderHandle` used to broadcast metadata and chunks,
    /// and an RAII cleanup guard to ensure the map entry is removed on completion.
    pub async fn register(&self, key: &str) -> (LeaderHandle, LeaderGuard) {
        let (tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        let (meta_tx, meta_rx) = watch::channel(None);
        let buffered_chunks = Arc::new(StdRwLock::new(Vec::new()));
        let is_finished = Arc::new(StdRwLock::new(false));

        let leader = Arc::new(InFlightLeader {
            tx: tx.clone(),
            meta_rx,
            buffered_chunks: buffered_chunks.clone(),
            is_finished: is_finished.clone(),
        });

        self.leaders.write().await.insert(key.to_string(), leader);

        let handle = LeaderHandle {
            tx,
            meta_tx,
            buffered_chunks,
            is_finished,
        };

        let guard = LeaderGuard {
            key: key.to_string(),
            leaders: self.leaders.clone(),
        };

        (handle, guard)
    }

    /// Returns the number of currently active in-flight leaders.
    #[allow(dead_code)]
    pub async fn in_flight_count(&self) -> usize {
        self.leaders.read().await.len()
    }
}

/// RAII Guard that removes the key from the SingleFlight map when dropped.
pub struct LeaderGuard {
    key: String,
    leaders: Arc<TokioRwLock<HashMap<String, Arc<InFlightLeader>>>>,
}

impl Drop for LeaderGuard {
    fn drop(&mut self) {
        let key = self.key.clone();
        let leaders = self.leaders.clone();
        tokio::spawn(async move {
            leaders.write().await.remove(&key);
            debug!(key = %key, "SingleFlight leader deregistered from in-flight map");
        });
    }
}

/// Handle held by the Leader to broadcast metadata and chunks to all Followers.
pub struct LeaderHandle {
    tx: broadcast::Sender<FlightMessage>,
    meta_tx: watch::Sender<Option<Result<FlightMetadata, String>>>,
    buffered_chunks: Arc<StdRwLock<Vec<Bytes>>>,
    is_finished: Arc<StdRwLock<bool>>,
}

impl LeaderHandle {
    /// Broadcasts upstream headers and status code to waiting followers.
    pub fn publish_metadata(&self, metadata: FlightMetadata) {
        let _ = self.meta_tx.send(Some(Ok(metadata)));
    }

    /// Broadcasts an upstream error, causing followers to abort or fail cleanly.
    pub fn publish_error(&self, error_msg: String) {
        let _ = self.meta_tx.send(Some(Err(error_msg.clone())));
        let _ = self.tx.send(FlightMessage::Error(error_msg));
    }

    /// Synchronously broadcasts a newly arrived media chunk to all followers and stores it in the replay buffer.
    pub fn publish_chunk(&self, chunk: Bytes) {
        self.buffered_chunks.write().unwrap().push(chunk.clone());
        let _ = self.tx.send(FlightMessage::Chunk(chunk));
    }

    /// Synchronously broadcasts end-of-stream to all followers.
    pub fn finish(&self) {
        *self.is_finished.write().unwrap() = true;
        let _ = self.tx.send(FlightMessage::Complete);
    }
}

/// Handle held by a Follower to receive headers and chunks from the Leader.
pub struct FollowerHandle {
    pub rx: broadcast::Receiver<FlightMessage>,
    pub meta_rx: watch::Receiver<Option<Result<FlightMetadata, String>>>,
    pub buffered_chunks: Arc<StdRwLock<Vec<Bytes>>>,
    pub is_finished: Arc<StdRwLock<bool>>,
}

impl FollowerHandle {
    /// Awaits response metadata (status and headers) from the Leader.
    pub async fn wait_for_metadata(&mut self) -> Result<FlightMetadata, String> {
        loop {
            if let Some(ref res) = *self.meta_rx.borrow() {
                return res.clone();
            }
            if self.meta_rx.changed().await.is_err() {
                return Err("Leader dropped before sending metadata".to_string());
            }
        }
    }

    /// Creates a stream that first replays all chunks already buffered by the leader,
    /// then seamlessly transitions to live broadcast chunks until completion.
    pub fn into_stream(
        self,
    ) -> std::pin::Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>> {
        let initial_chunks = self.buffered_chunks.read().unwrap().clone();
        let already_finished = *self.is_finished.read().unwrap();

        create_follower_stream(initial_chunks, self.rx, already_finished)
    }
}

/// Creates a Stream adapter that drains replayed chunks then listens to live broadcast chunks.
pub fn create_follower_stream(
    initial_chunks: Vec<Bytes>,
    rx: broadcast::Receiver<FlightMessage>,
    already_finished: bool,
) -> std::pin::Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>> {
    Box::pin(futures_util::stream::unfold(
        (initial_chunks, 0usize, rx, already_finished),
        |(chunks, mut idx, mut rx, finished)| async move {
            // Replay phase
            if idx < chunks.len() {
                let chunk = chunks[idx].clone();
                idx += 1;
                return Some((Ok(chunk), (chunks, idx, rx, finished)));
            }

            if finished {
                return None;
            }

            // Live broadcast phase
            match rx.recv().await {
                Ok(FlightMessage::Chunk(bytes)) => Some((Ok(bytes), (chunks, idx, rx, finished))),
                Ok(FlightMessage::Complete) => None,
                Ok(FlightMessage::Error(err)) => Some((
                    Err(std::io::Error::new(
                        std::io::ErrorKind::ConnectionReset,
                        err,
                    )),
                    (chunks, idx, rx, true),
                )),
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    warn!(skipped, "Follower lagged behind leader broadcast stream");
                    Some((
                        Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "Stream buffer lagged",
                        )),
                        (chunks, idx, rx, true),
                    ))
                }
                Err(broadcast::error::RecvError::Closed) => None,
            }
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;

    #[tokio::test]
    async fn test_singleflight_leader_and_follower() {
        let sf = SingleFlight::new();
        let key = "stream1:seg-01.m4s";

        // Step 1: First request becomes Leader
        assert!(sf.join(key).await.is_none());
        let (leader, _guard) = sf.register(key).await;

        // Step 2: Second request becomes Follower
        let mut follower = sf
            .join(key)
            .await
            .expect("Follower should join active leader");

        // Step 3: Leader publishes headers
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "video/mp4".parse().unwrap());
        leader.publish_metadata(FlightMetadata {
            status: StatusCode::OK,
            headers: headers.clone(),
        });

        // Step 4: Follower receives metadata
        let meta = follower
            .wait_for_metadata()
            .await
            .expect("Metadata received");
        assert_eq!(meta.status, StatusCode::OK);
        assert_eq!(meta.headers.get("content-type").unwrap(), "video/mp4");

        // Step 5: Leader publishes chunks
        let chunk1 = Bytes::from_static(b"hello ");
        let chunk2 = Bytes::from_static(b"world!");
        leader.publish_chunk(chunk1.clone());
        leader.publish_chunk(chunk2.clone());
        leader.finish();

        // Step 6: Follower consumes stream
        let mut stream = follower.into_stream();
        let mut collected = Vec::new();
        while let Some(item) = stream.next().await {
            collected.extend_from_slice(&item.unwrap());
        }

        assert_eq!(collected, b"hello world!");
    }

    #[tokio::test]
    async fn test_singleflight_late_follower_replay() {
        let sf = SingleFlight::new();
        let key = "stream1:seg-02.m4s";

        let (leader, _guard) = sf.register(key).await;

        // Leader publishes metadata and 2 chunks BEFORE follower joins
        leader.publish_metadata(FlightMetadata {
            status: StatusCode::OK,
            headers: HeaderMap::new(),
        });
        leader.publish_chunk(Bytes::from_static(b"chunk1_"));
        leader.publish_chunk(Bytes::from_static(b"chunk2_"));

        // Now follower joins late!
        let mut follower = sf.join(key).await.expect("Follower joins");
        let _ = follower.wait_for_metadata().await.unwrap();

        // Leader publishes chunk 3 and finishes
        leader.publish_chunk(Bytes::from_static(b"chunk3"));
        leader.finish();

        // Follower must receive all 3 chunks in order!
        let mut stream = follower.into_stream();
        let mut collected = Vec::new();
        while let Some(item) = stream.next().await {
            collected.extend_from_slice(&item.unwrap());
        }

        assert_eq!(collected, b"chunk1_chunk2_chunk3");
    }

    #[tokio::test]
    async fn test_singleflight_error_propagation() {
        let sf = SingleFlight::new();
        let key = "stream1:seg-error.m4s";

        let (leader, _guard) = sf.register(key).await;
        let mut follower = sf.join(key).await.expect("Follower joins");

        leader.publish_error("Origin timed out".to_string());

        let res = follower.wait_for_metadata().await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err(), "Origin timed out");
    }
}
