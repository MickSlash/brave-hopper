use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheKey {
    pub stream_id: String,
    pub clean_path: String,
    pub extension: String,
    pub hash: String,
}

impl CacheKey {
    /// Generates a deterministic CacheKey from stream_id and path_and_query.
    /// Filters out dynamic authentication and session tokens (token, expires, sig, etc.)
    /// while preserving parameters that alter content (e.g. size_mb, quality).
    #[allow(dead_code)]
    pub fn new(stream_id: &str, path_and_query: &str) -> Self {
        Self::new_with_range(stream_id, path_and_query, None)
    }

    /// Generates a deterministic CacheKey from stream_id, path_and_query, and optional byte range.
    /// Incorporates the HTTP Range header (e.g. bytes=100-200) so that 206 Partial Content
    /// byte ranges are safely cached as discrete chunks without collision.
    pub fn new_with_range(stream_id: &str, path_and_query: &str, range: Option<&str>) -> Self {
        let (raw_path, raw_query) = match path_and_query.split_once('?') {
            Some((p, q)) => (p, q),
            None => (path_and_query, ""),
        };

        let clean_path = raw_path.trim_start_matches('/').to_string();

        let extension = Path::new(&clean_path)
            .extension()
            .and_then(|ext| ext.to_str())
            .unwrap_or("bin")
            .to_string();

        let clean_query = filter_cache_query(raw_query);

        let mut hasher = Sha256::new();
        hasher.update(stream_id.as_bytes());
        hasher.update(b":");
        hasher.update(clean_path.as_bytes());
        if !clean_query.is_empty() {
            hasher.update(b"?");
            hasher.update(clean_query.as_bytes());
        }
        if let Some(r) = range {
            hasher.update(b"#range=");
            hasher.update(r.trim().as_bytes());
        }

        let hash = hex::encode(hasher.finalize());

        Self {
            stream_id: stream_id.to_string(),
            clean_path,
            extension,
            hash,
        }
    }

    /// Computes the two-tier hierarchical relative storage path.
    /// Example: `ab/cd/abcdef0123456789....m4s`
    pub fn relative_path(&self) -> PathBuf {
        let dir_1 = &self.hash[0..2];
        let dir_2 = &self.hash[2..4];
        let filename = format!("{}.{}", self.hash, self.extension);
        PathBuf::from(dir_1).join(dir_2).join(filename)
    }

    /// Generates an atomic temporary filename for streaming writes before rename.
    /// Example: `abcdef0123...tmp.3921.1492`
    pub fn tmp_filename(&self) -> String {
        format!(
            "{}.tmp.{}.{}",
            self.hash,
            std::process::id(),
            Uuid::new_v4().simple()
        )
    }
}

/// Normalizes query string by excluding transient security tokens and sorting remaining keys.
fn filter_cache_query(query: &str) -> String {
    if query.is_empty() {
        return String::new();
    }

    let mut filtered_params: Vec<(&str, &str)> = query
        .split('&')
        .filter_map(|pair| {
            let mut parts = pair.splitn(2, '=');
            let key = parts.next()?;
            let val = parts.next().unwrap_or("");

            match key {
                "token" | "expires" | "sig" | "signature" | "ip" | "client_ip" | "edge_id"
                | "_" | "t" => None,
                _ => Some((key, val)),
            }
        })
        .collect();

    filtered_params.sort_by_key(|&(k, _)| k);

    filtered_params
        .into_iter()
        .map(|(k, v)| {
            if v.is_empty() {
                k.to_string()
            } else {
                format!("{}={}", k, v)
            }
        })
        .collect::<Vec<_>>()
        .join("&")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_key_generation() {
        let key1 = CacheKey::new(
            "stream-1",
            "/media/stream-1/seg-01.m4s?token=abc&expires=99999",
        );
        let key2 = CacheKey::new(
            "stream-1",
            "/media/stream-1/seg-01.m4s?token=xyz&expires=88888",
        );

        // Security query parameters must not alter the content cache hash
        assert_eq!(key1.hash, key2.hash);
        assert_eq!(key1.extension, "m4s");
    }

    #[test]
    fn test_content_altering_params() {
        let key1 = CacheKey::new("stream-1", "/media/stream-1/large.bin?size_mb=10&token=abc");
        let key2 = CacheKey::new("stream-1", "/media/stream-1/large.bin?size_mb=20&token=abc");

        // Different content-altering parameters must yield different cache hashes
        assert_ne!(key1.hash, key2.hash);
    }

    #[test]
    fn test_relative_path_structure() {
        let key = CacheKey::new("s1", "chunk.m4s");
        let path = key.relative_path();
        let path_str = path.to_string_lossy().replace('\\', "/");

        assert_eq!(path_str.len(), 2 + 1 + 2 + 1 + 64 + 4); // ab/cd/{hash}.m4s
        assert!(path_str.ends_with(".m4s"));
    }
}
