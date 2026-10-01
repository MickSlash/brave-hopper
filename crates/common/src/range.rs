use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RangeError {
    #[error("Invalid range header format: {0}")]
    InvalidFormat(String),
    #[error("Requested range is not satisfiable for total length {total_len}")]
    Unsatisfiable { total_len: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpRange {
    /// `bytes=start-end` (inclusive range)
    FromTo(u64, u64),
    /// `bytes=start-` (from start to end of resource)
    From(u64),
    /// `bytes=-suffix` (last N bytes of resource)
    Suffix(u64),
}

impl HttpRange {
    /// Parses an RFC 7233 byte-range specification (e.g. `bytes=0-1023`, `bytes=500-`, `bytes=-200`).
    pub fn parse(header_value: &str) -> Result<Self, RangeError> {
        let val = header_value.trim();
        let bytes_spec = val
            .strip_prefix("bytes=")
            .ok_or_else(|| RangeError::InvalidFormat("Expected 'bytes=' prefix".to_string()))?;

        // Multiple ranges (multipart) are uncommon in video streaming and discouraged; take the primary range
        let first_range = bytes_spec.split(',').next().unwrap_or(bytes_spec).trim();

        if let Some(suffix_str) = first_range.strip_prefix('-') {
            let suffix_len: u64 = suffix_str
                .parse()
                .map_err(|_| RangeError::InvalidFormat("Invalid suffix length".to_string()))?;
            if suffix_len == 0 {
                return Err(RangeError::InvalidFormat(
                    "Suffix length cannot be zero".to_string(),
                ));
            }
            Ok(HttpRange::Suffix(suffix_len))
        } else if let Some(dash_idx) = first_range.find('-') {
            let start_str = &first_range[..dash_idx];
            let end_str = &first_range[dash_idx + 1..];

            let start: u64 = start_str
                .parse()
                .map_err(|_| RangeError::InvalidFormat("Invalid range start".to_string()))?;

            if end_str.is_empty() {
                Ok(HttpRange::From(start))
            } else {
                let end: u64 = end_str
                    .parse()
                    .map_err(|_| RangeError::InvalidFormat("Invalid range end".to_string()))?;
                if start > end {
                    return Err(RangeError::InvalidFormat(
                        "Range start cannot exceed end".to_string(),
                    ));
                }
                Ok(HttpRange::FromTo(start, end))
            }
        } else {
            Err(RangeError::InvalidFormat(
                "Missing dash separator in range".to_string(),
            ))
        }
    }

    /// Resolves the abstract range against a known resource length, returning inclusive `(start, end)`.
    pub fn resolve(&self, total_len: u64) -> Result<(u64, u64), RangeError> {
        if total_len == 0 {
            return Err(RangeError::Unsatisfiable { total_len });
        }

        match *self {
            HttpRange::FromTo(start, end) => {
                if start >= total_len {
                    Err(RangeError::Unsatisfiable { total_len })
                } else {
                    let clamped_end = end.min(total_len - 1);
                    Ok((start, clamped_end))
                }
            }
            HttpRange::From(start) => {
                if start >= total_len {
                    Err(RangeError::Unsatisfiable { total_len })
                } else {
                    Ok((start, total_len - 1))
                }
            }
            HttpRange::Suffix(suffix_len) => {
                if suffix_len == 0 {
                    Err(RangeError::Unsatisfiable { total_len })
                } else if suffix_len >= total_len {
                    Ok((0, total_len - 1))
                } else {
                    Ok((total_len - suffix_len, total_len - 1))
                }
            }
        }
    }
}

/// Formats the RFC 7233 Content-Range header value for successful partial responses.
/// Example: `bytes 0-1023/10485760`
pub fn format_content_range(start: u64, end: u64, total: u64) -> String {
    format!("bytes {}-{}/{}", start, end, total)
}

/// Formats the Content-Range header for 416 Range Not Satisfiable responses.
/// Example: `bytes */10485760`
pub fn format_unsatisfiable_content_range(total: u64) -> String {
    format!("bytes */{}", total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ranges() {
        assert_eq!(
            HttpRange::parse("bytes=0-499").unwrap(),
            HttpRange::FromTo(0, 499)
        );
        assert_eq!(
            HttpRange::parse("bytes=500-").unwrap(),
            HttpRange::From(500)
        );
        assert_eq!(
            HttpRange::parse("bytes=-500").unwrap(),
            HttpRange::Suffix(500)
        );
        assert!(HttpRange::parse("bytes=-0").is_err());
        assert!(HttpRange::parse("bytes=500-200").is_err());
        assert!(HttpRange::parse("items=0-10").is_err());
    }

    #[test]
    fn test_resolve_ranges() {
        let total = 10_000;

        // bytes=0-499 -> 0..=499
        let (s, e) = HttpRange::FromTo(0, 499).resolve(total).unwrap();
        assert_eq!((s, e), (0, 499));

        // bytes=500- -> 500..=9999
        let (s, e) = HttpRange::From(500).resolve(total).unwrap();
        assert_eq!((s, e), (500, 9999));

        // bytes=-500 -> 9500..=9999
        let (s, e) = HttpRange::Suffix(500).resolve(total).unwrap();
        assert_eq!((s, e), (9500, 9999));

        // Suffix larger than total -> full file 0..=9999
        let (s, e) = HttpRange::Suffix(20000).resolve(total).unwrap();
        assert_eq!((s, e), (0, 9999));

        // Out of bounds start -> 416
        assert_eq!(
            HttpRange::From(10_000).resolve(total).unwrap_err(),
            RangeError::Unsatisfiable { total_len: 10_000 }
        );
        assert_eq!(
            HttpRange::FromTo(10_000, 12_000)
                .resolve(total)
                .unwrap_err(),
            RangeError::Unsatisfiable { total_len: 10_000 }
        );
    }
}
