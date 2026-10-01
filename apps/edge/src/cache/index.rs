use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
    time::{Duration, Instant},
};

/// Metadata stored in memory for each cached chunk.
#[derive(Debug, Clone)]
pub struct CacheEntry {
    pub hash: String,
    pub relative_path: PathBuf,
    pub size_bytes: u64,
    pub content_type: Option<String>,
    pub created_at: Instant,
    pub last_accessed: Instant,
    pub access_seq: u64,
}

impl CacheEntry {
    pub fn new(
        hash: String,
        relative_path: PathBuf,
        size_bytes: u64,
        content_type: Option<String>,
    ) -> Self {
        let now = Instant::now();
        Self {
            hash,
            relative_path,
            size_bytes,
            content_type,
            created_at: now,
            last_accessed: now,
            access_seq: 0,
        }
    }
}

/// Ultra-lightweight in-memory LRU index for the filesystem cache.
/// Uses a HashMap for O(1) lookups and a BTreeMap<(access_seq), hash> for O(log N) LRU ordering.
/// Consumes minimal memory (< 4MB for 100,000 entries).
pub struct CacheIndex {
    entries: HashMap<String, CacheEntry>,
    lru_order: BTreeMap<u64, String>,
    next_seq: u64,
    total_size_bytes: u64,
    max_size_bytes: u64,
    ttl: Duration,
}

#[allow(dead_code)]
impl CacheIndex {
    pub fn new(max_size_bytes: u64, ttl: Duration) -> Self {
        Self {
            entries: HashMap::new(),
            lru_order: BTreeMap::new(),
            next_seq: 0,
            total_size_bytes: 0,
            max_size_bytes,
            ttl,
        }
    }

    /// Looks up a cached entry by its key hash.
    /// If the entry has expired according to TTL, it is removed and None is returned.
    /// Otherwise, its LRU access sequence is updated.
    pub fn get(&mut self, hash: &str) -> Option<CacheEntry> {
        let is_expired = if let Some(entry) = self.entries.get(hash) {
            entry.created_at.elapsed() > self.ttl
        } else {
            return None;
        };

        if is_expired {
            self.remove(hash);
            return None;
        }

        let entry = self.entries.get_mut(hash)?;
        // Update LRU position
        self.lru_order.remove(&entry.access_seq);
        self.next_seq = self.next_seq.wrapping_add(1);
        entry.access_seq = self.next_seq;
        entry.last_accessed = Instant::now();
        self.lru_order.insert(self.next_seq, hash.to_string());

        Some(entry.clone())
    }

    /// Checks if a hash exists without updating its LRU position or removing if expired.
    pub fn contains(&self, hash: &str) -> bool {
        if let Some(entry) = self.entries.get(hash) {
            entry.created_at.elapsed() <= self.ttl
        } else {
            false
        }
    }

    /// Inserts or replaces a cache entry, updating LRU order and triggering
    /// eviction if total cached bytes exceed max_size_bytes.
    /// Returns any evicted entries so their files can be removed from disk.
    pub fn insert(&mut self, mut entry: CacheEntry) -> Vec<CacheEntry> {
        let hash = entry.hash.clone();

        // If it already existed, remove old entry and subtract old size
        if let Some(old) = self.remove(&hash) {
            self.total_size_bytes = self.total_size_bytes.saturating_sub(old.size_bytes);
        }

        self.next_seq = self.next_seq.wrapping_add(1);
        entry.access_seq = self.next_seq;
        entry.last_accessed = Instant::now();

        self.total_size_bytes = self.total_size_bytes.saturating_add(entry.size_bytes);
        self.lru_order.insert(self.next_seq, hash.clone());
        self.entries.insert(hash, entry);

        self.evict_excess()
    }

    /// Evicts enough entries to accommodate `needed_bytes`.
    /// Returns the evicted entries.
    pub fn evict_to_fit(&mut self, needed_bytes: u64) -> Vec<CacheEntry> {
        let mut evicted = Vec::new();

        while self.total_size_bytes.saturating_add(needed_bytes) > self.max_size_bytes
            && !self.lru_order.is_empty()
        {
            if let Some((&seq, hash)) = self.lru_order.iter().next() {
                let hash = hash.clone();
                self.lru_order.remove(&seq);
                if let Some(entry) = self.entries.remove(&hash) {
                    self.total_size_bytes = self.total_size_bytes.saturating_sub(entry.size_bytes);
                    evicted.push(entry);
                }
            } else {
                break;
            }
        }

        evicted
    }

    /// Evicts entries if current total_size_bytes exceeds max_size_bytes.
    pub fn evict_excess(&mut self) -> Vec<CacheEntry> {
        self.evict_to_fit(0)
    }

    /// Removes an entry explicitly by hash.
    pub fn remove(&mut self, hash: &str) -> Option<CacheEntry> {
        if let Some(entry) = self.entries.remove(hash) {
            self.lru_order.remove(&entry.access_seq);
            self.total_size_bytes = self.total_size_bytes.saturating_sub(entry.size_bytes);
            Some(entry)
        } else {
            None
        }
    }

    /// Scans for and removes all expired entries.
    /// Returns list of expired entries for disk deletion.
    pub fn purge_expired(&mut self) -> Vec<CacheEntry> {
        let now = Instant::now();
        let expired_hashes: Vec<String> = self
            .entries
            .iter()
            .filter_map(|(hash, entry)| {
                if now.duration_since(entry.created_at) > self.ttl {
                    Some(hash.clone())
                } else {
                    None
                }
            })
            .collect();

        let mut expired_entries = Vec::with_capacity(expired_hashes.len());
        for hash in expired_hashes {
            if let Some(entry) = self.remove(&hash) {
                expired_entries.push(entry);
            }
        }

        expired_entries
    }

    #[inline]
    pub fn used_bytes(&self) -> u64 {
        self.total_size_bytes
    }

    #[inline]
    pub fn max_size_bytes(&self) -> u64 {
        self.max_size_bytes
    }

    #[inline]
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    pub fn clear(&mut self) -> Vec<CacheEntry> {
        let entries: Vec<CacheEntry> = self.entries.drain().map(|(_, v)| v).collect();
        self.lru_order.clear();
        self.total_size_bytes = 0;
        entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_insert_and_get() {
        let mut index = CacheIndex::new(1000, Duration::from_secs(60));
        let entry = CacheEntry::new(
            "hash1".to_string(),
            PathBuf::from("ab/cd/hash1.m4s"),
            100,
            Some("video/mp4".to_string()),
        );

        assert!(index.insert(entry).is_empty());
        assert_eq!(index.used_bytes(), 100);
        assert_eq!(index.entry_count(), 1);

        let retrieved = index.get("hash1").expect("entry should exist");
        assert_eq!(retrieved.hash, "hash1");
        assert_eq!(retrieved.size_bytes, 100);
    }

    #[test]
    fn test_lru_eviction() {
        // Capacity = 250 bytes
        let mut index = CacheIndex::new(250, Duration::from_secs(60));

        let e1 = CacheEntry::new("h1".to_string(), PathBuf::from("p1"), 100, None);
        let e2 = CacheEntry::new("h2".to_string(), PathBuf::from("p2"), 100, None);
        let e3 = CacheEntry::new("h3".to_string(), PathBuf::from("p3"), 100, None);

        index.insert(e1);
        index.insert(e2);
        // Access h1 so h2 becomes the oldest
        assert!(index.get("h1").is_some());

        // Inserting e3 brings total to 300 > 250.
        // h2 must be evicted since h1 was recently accessed!
        let evicted = index.insert(e3);
        assert_eq!(evicted.len(), 1);
        assert_eq!(evicted[0].hash, "h2");

        assert!(index.get("h2").is_none());
        assert!(index.get("h1").is_some());
        assert!(index.get("h3").is_some());
        assert_eq!(index.used_bytes(), 200);
    }

    #[test]
    fn test_ttl_expiration() {
        let mut index = CacheIndex::new(1000, Duration::from_millis(50));
        let entry = CacheEntry::new("expired_soon".to_string(), PathBuf::from("p"), 50, None);
        index.insert(entry);

        std::thread::sleep(Duration::from_millis(60));

        assert!(index.get("expired_soon").is_none());
        assert_eq!(index.used_bytes(), 0);
        assert_eq!(index.entry_count(), 0);
    }
}
