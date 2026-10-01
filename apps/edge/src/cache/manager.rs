use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{fs, io::AsyncWriteExt, sync::RwLock};
use tracing::{debug, info, warn};

use super::{
    index::{CacheEntry, CacheIndex},
    key::CacheKey,
};

#[derive(Debug, Clone)]
pub struct CacheHit {
    pub full_path: PathBuf,
    #[allow(dead_code)]
    pub relative_path: PathBuf,
    pub size_bytes: u64,
    pub content_type: Option<String>,
    pub content_range: Option<String>,
}

pub struct CacheWriter {
    pub temp_path: PathBuf,
    pub target_path: PathBuf,
    pub relative_path: PathBuf,
    pub hash: String,
    pub file: Option<fs::File>,
    pub bytes_written: u64,
    pub is_committed: bool,
    pub is_aborted: bool,
}

impl CacheWriter {
    /// Writes a chunk of bytes to the temporary cache file.
    pub async fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), std::io::Error> {
        if self.is_aborted {
            return Ok(());
        }

        if let Some(file) = &mut self.file {
            file.write_all(chunk).await?;
            self.bytes_written += chunk.len() as u64;
            Ok(())
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "Cache file is not open",
            ))
        }
    }

    /// Flushes, syncs to disk, and prepares for atomic rename.
    pub async fn finish_write(&mut self) -> Result<(), std::io::Error> {
        if let Some(mut file) = self.file.take() {
            file.flush().await?;
            file.sync_data().await?;
        }
        Ok(())
    }

    /// Aborts writing and cleans up the temporary file immediately.
    pub async fn abort(&mut self) {
        if self.is_aborted || self.is_committed {
            return;
        }
        self.is_aborted = true;
        self.file.take(); // Close file handle before removing
        if let Err(e) = fs::remove_file(&self.temp_path).await {
            debug!(error = %e, path = ?self.temp_path, "Could not delete aborted temp file (may not exist yet)");
        }
    }
}

impl Drop for CacheWriter {
    fn drop(&mut self) {
        // If not committed and not explicitly aborted, asynchronously remove orphan temp file
        if !self.is_committed && !self.is_aborted {
            let temp_path = self.temp_path.clone();
            tokio::spawn(async move {
                let _ = fs::remove_file(temp_path).await;
            });
        }
    }
}

pub struct CacheManager {
    root_dir: PathBuf,
    index: RwLock<CacheIndex>,
    total_bytes: AtomicU64,
}

impl CacheManager {
    pub fn new(root_dir: PathBuf, max_size_bytes: u64, ttl: Duration) -> Arc<Self> {
        Arc::new(Self {
            root_dir,
            index: RwLock::new(CacheIndex::new(max_size_bytes, ttl)),
            total_bytes: AtomicU64::new(0),
        })
    }

    /// Initializes the cache directory structure and scans existing files to rebuild the LRU index.
    pub async fn init(&self) -> Result<(), std::io::Error> {
        fs::create_dir_all(&self.root_dir).await?;

        info!(cache_dir = ?self.root_dir, "Scanning cache directory to rebuild in-memory index...");

        let mut scanned_entries = 0usize;
        let mut scanned_bytes = 0u64;
        let mut stale_temps = 0usize;

        let mut dir_queue = vec![self.root_dir.clone()];

        while let Some(current_dir) = dir_queue.pop() {
            let mut read_dir = match fs::read_dir(&current_dir).await {
                Ok(rd) => rd,
                Err(e) => {
                    warn!(dir = ?current_dir, error = %e, "Failed to read cache subdir during scan");
                    continue;
                }
            };

            while let Ok(Some(entry)) = read_dir.next_entry().await {
                let path = entry.path();
                let file_type = match entry.file_type().await {
                    Ok(ft) => ft,
                    Err(_) => continue,
                };

                if file_type.is_dir() {
                    dir_queue.push(path);
                    continue;
                }

                if file_type.is_file() {
                    let filename = path.file_name().and_then(|n| n.to_str()).unwrap_or("");

                    // Clean up leftover temporary files from previous process runs
                    if filename.contains(".tmp.") {
                        let _ = fs::remove_file(&path).await;
                        stale_temps += 1;
                        continue;
                    }

                    // Process cache media chunks
                    if let Ok(metadata) = entry.metadata().await {
                        let size = metadata.len();
                        if let Ok(rel_path) = path.strip_prefix(&self.root_dir) {
                            let hash = Path::new(filename)
                                .file_stem()
                                .and_then(|s| s.to_str())
                                .unwrap_or(filename)
                                .to_string();

                            let content_type = detect_content_type_from_path(&path);

                            let cache_entry =
                                CacheEntry::new(hash, rel_path.to_path_buf(), size, content_type);

                            let evicted = self.index.write().await.insert(cache_entry);

                            // Delete any immediately evicted files if cache was already over-capacity
                            for ev in evicted {
                                let ev_path = self.root_dir.join(&ev.relative_path);
                                let _ = fs::remove_file(ev_path).await;
                            }

                            scanned_entries += 1;
                            scanned_bytes += size;
                        }
                    }
                }
            }
        }

        self.total_bytes
            .store(self.index.read().await.used_bytes(), Ordering::Relaxed);

        info!(
            entries = scanned_entries,
            size_mb = scanned_bytes / (1024 * 1024),
            stale_temp_files_cleaned = stale_temps,
            "Cache directory scan and index reconstruction complete"
        );

        Ok(())
    }

    /// Looks up a cache key in the index.
    pub async fn lookup(&self, key: &CacheKey) -> Option<CacheHit> {
        let entry = self.index.write().await.get(&key.hash)?;

        let full_path = self.root_dir.join(&entry.relative_path);

        Some(CacheHit {
            full_path,
            relative_path: entry.relative_path,
            size_bytes: entry.size_bytes,
            content_type: entry.content_type,
            content_range: entry.content_range,
        })
    }

    /// Prepares an atomic writer for a cache entry.
    pub async fn start_write(
        &self,
        key: &CacheKey,
        estimated_size: Option<u64>,
    ) -> Result<CacheWriter, std::io::Error> {
        // Evict if needed before writing
        if let Some(needed) = estimated_size {
            let evicted = self.index.write().await.evict_to_fit(needed);
            self.total_bytes
                .store(self.index.read().await.used_bytes(), Ordering::Relaxed);
            self.delete_files(evicted).await;
        }

        let rel_path = key.relative_path();
        let target_path = self.root_dir.join(&rel_path);

        // Ensure subdirectories (e.g. cache/ab/cd) exist
        if let Some(parent) = target_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let temp_filename = key.tmp_filename();
        let temp_path = target_path
            .parent()
            .unwrap_or(&self.root_dir)
            .join(temp_filename);

        let file = fs::File::create(&temp_path).await?;

        Ok(CacheWriter {
            temp_path,
            target_path,
            relative_path: rel_path,
            hash: key.hash.clone(),
            file: Some(file),
            bytes_written: 0,
            is_committed: false,
            is_aborted: false,
        })
    }

    /// Commits a completed writer atomically by renaming the temporary file and adding to the index.
    #[allow(dead_code)]
    pub async fn commit_write(
        &self,
        writer: &mut CacheWriter,
        content_type: Option<String>,
    ) -> Result<PathBuf, std::io::Error> {
        self.commit_write_with_range(writer, content_type, None).await
    }

    /// Commits a completed writer atomically with both content_type and optional HTTP content_range.
    pub async fn commit_write_with_range(
        &self,
        writer: &mut CacheWriter,
        content_type: Option<String>,
        content_range: Option<String>,
    ) -> Result<PathBuf, std::io::Error> {
        writer.finish_write().await?;

        // Atomic filesystem rename
        fs::rename(&writer.temp_path, &writer.target_path).await?;
        writer.is_committed = true;
        let target_path = writer.target_path.clone();

        let entry = CacheEntry::new_with_range(
            writer.hash.clone(),
            writer.relative_path.clone(),
            writer.bytes_written,
            content_type,
            content_range,
        );

        let evicted = self.index.write().await.insert(entry);
        self.total_bytes
            .store(self.index.read().await.used_bytes(), Ordering::Relaxed);

        // Asynchronously delete any evicted files
        self.delete_files(evicted).await;

        debug!(
            hash = %writer.hash,
            bytes = writer.bytes_written,
            path = ?target_path,
            "Successfully committed cache file"
        );

        Ok(target_path)
    }

    /// Asynchronously deletes evicted files from disk.
    async fn delete_files(&self, evicted: Vec<CacheEntry>) {
        if evicted.is_empty() {
            return;
        }

        for entry in evicted {
            let file_path = self.root_dir.join(&entry.relative_path);
            if let Err(e) = fs::remove_file(&file_path).await {
                warn!(path = ?file_path, error = %e, "Failed to remove evicted cache file");
            } else {
                debug!(path = ?file_path, "Removed evicted cache file");
            }
        }
    }

    #[inline]
    pub fn used_bytes(&self) -> u64 {
        self.total_bytes.load(Ordering::Relaxed)
    }

    #[inline]
    #[allow(dead_code)]
    pub async fn max_size_bytes(&self) -> u64 {
        self.index.read().await.max_size_bytes()
    }

    #[inline]
    #[allow(dead_code)]
    pub async fn entry_count(&self) -> usize {
        self.index.read().await.entry_count()
    }
}

fn detect_content_type_from_path(path: &Path) -> Option<String> {
    let ext = path.extension()?.to_str()?;
    match ext.to_lowercase().as_str() {
        "m4s" => Some("video/iso.segment".to_string()),
        "mp4" => Some("video/mp4".to_string()),
        "ts" => Some("video/MP2T".to_string()),
        "m3u8" => Some("application/vnd.apple.mpegurl".to_string()),
        "bin" => Some("application/octet-stream".to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cache_manager_write_and_lookup() {
        let temp_dir =
            std::env::temp_dir().join(format!("edge_test_cache_{}", uuid::Uuid::new_v4()));
        let manager = CacheManager::new(
            temp_dir.clone(),
            10 * 1024 * 1024,
            Duration::from_secs(3600),
        );

        manager.init().await.expect("init should succeed");

        let key = CacheKey::new("test-stream", "video/seg-01.m4s");
        assert!(manager.lookup(&key).await.is_none());

        let mut writer = manager
            .start_write(&key, Some(1024))
            .await
            .expect("start write should succeed");

        let chunk1 = vec![1u8; 512];
        let chunk2 = vec![2u8; 512];
        writer.write_chunk(&chunk1).await.unwrap();
        writer.write_chunk(&chunk2).await.unwrap();

        let target_path = manager
            .commit_write(&mut writer, Some("video/iso.segment".to_string()))
            .await
            .expect("commit write should succeed");

        assert!(target_path.exists());
        assert_eq!(manager.used_bytes(), 1024);

        let hit = manager.lookup(&key).await.expect("lookup should hit");
        assert_eq!(hit.size_bytes, 1024);
        assert_eq!(hit.content_type.as_deref(), Some("video/iso.segment"));

        // Clean up
        let _ = fs::remove_dir_all(temp_dir).await;
    }

    #[tokio::test]
    async fn test_cache_manager_abort_write() {
        let temp_dir =
            std::env::temp_dir().join(format!("edge_test_cache_abort_{}", uuid::Uuid::new_v4()));
        let manager = CacheManager::new(
            temp_dir.clone(),
            10 * 1024 * 1024,
            Duration::from_secs(3600),
        );

        manager.init().await.unwrap();

        let key = CacheKey::new("test-stream", "video/abort-seg.m4s");
        let mut writer = manager.start_write(&key, None).await.unwrap();
        let temp_path = writer.temp_path.clone();

        writer.write_chunk(b"partial data").await.unwrap();
        assert!(temp_path.exists());

        writer.abort().await;
        assert!(!temp_path.exists());
        assert!(manager.lookup(&key).await.is_none());

        let _ = fs::remove_dir_all(temp_dir).await;
    }
}
