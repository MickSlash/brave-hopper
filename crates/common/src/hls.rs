/// Rewrites an HLS playlist (.m3u8), ensuring Edge Affinity by attaching
/// authentication/session query parameters to all child playlists, media segments,
/// and initialization segments (`EXT-X-MAP`).
pub fn rewrite_hls_playlist(playlist: &str, query_params: &str) -> String {
    let clean_query = query_params.trim_start_matches('?');
    let mut rewritten = String::with_capacity(playlist.len() + 1024);

    for line in playlist.lines() {
        let trimmed = line.trim();

        if trimmed.is_empty() {
            rewritten.push('\n');
            continue;
        }

        if trimmed.starts_with("#EXT") {
            // Check if tag contains URI="..." attribute (e.g. EXT-X-MAP, EXT-X-MEDIA, EXT-X-KEY)
            if let Some(uri_attr_idx) = trimmed.find("URI=\"") {
                let start_quote = uri_attr_idx + 5;
                if let Some(end_quote) = trimmed[start_quote..].find('"') {
                    let end_quote_idx = start_quote + end_quote;
                    let uri_val = &trimmed[start_quote..end_quote_idx];
                    let rewritten_uri = append_query_and_sanitize(uri_val, clean_query);

                    rewritten.push_str(&trimmed[..start_quote]);
                    rewritten.push_str(&rewritten_uri);
                    rewritten.push_str(&trimmed[end_quote_idx..]);
                    rewritten.push('\n');
                    continue;
                }
            }

            // Normal HLS tag without URI attribute
            rewritten.push_str(trimmed);
            rewritten.push('\n');
        } else if trimmed.starts_with('#') {
            // Comment or non-EXT tag
            rewritten.push_str(trimmed);
            rewritten.push('\n');
        } else {
            // Media segment URL or variant playlist URL
            let rewritten_uri = append_query_and_sanitize(trimmed, clean_query);
            rewritten.push_str(&rewritten_uri);
            rewritten.push('\n');
        }
    }

    rewritten
}

/// Appends query parameters to a URI string, handling both relative paths
/// and full absolute URLs (stripping origin domains if present to preserve edge affinity).
fn append_query_and_sanitize(raw_uri: &str, clean_query: &str) -> String {
    let path = if let Some(stripped) = raw_uri
        .strip_prefix("http://")
        .or_else(|| raw_uri.strip_prefix("https://"))
    {
        // Strip host and retain path starting from the first '/'
        if let Some(slash_idx) = stripped.find('/') {
            &stripped[slash_idx..]
        } else {
            raw_uri
        }
    } else {
        raw_uri
    };

    if clean_query.is_empty() {
        path.to_string()
    } else if path.contains('?') {
        format!("{}&{}", path, clean_query)
    } else {
        format!("{}?{}", path, clean_query)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rewrite_master_playlist() {
        let master = r#"#EXTM3U
#EXT-X-VERSION:6
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio",NAME="English",DEFAULT=YES,URI="audio/en.m3u8"
#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID="subs",NAME="English",URI="subs/en.m3u8"
#EXT-X-STREAM-INF:BANDWIDTH=800000,RESOLUTION=640x360
360p.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=2000000,RESOLUTION=1280x720
720p.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=5000000,RESOLUTION=1920x1080
https://origin.internal.com/streams/1080p.m3u8
"#;

        let query = "token=secret123&expires=99999";
        let output = rewrite_hls_playlist(master, query);

        assert!(output.contains(r#"URI="audio/en.m3u8?token=secret123&expires=99999""#));
        assert!(output.contains(r#"URI="subs/en.m3u8?token=secret123&expires=99999""#));
        assert!(output.contains("360p.m3u8?token=secret123&expires=99999"));
        assert!(output.contains("720p.m3u8?token=secret123&expires=99999"));
        // Absolute URL stripped to path and retaining query
        assert!(output.contains("/streams/1080p.m3u8?token=secret123&expires=99999"));
    }

    #[test]
    fn test_rewrite_media_playlist_with_ext_x_map() {
        let variant = r#"#EXTM3U
#EXT-X-VERSION:6
#EXT-X-TARGETDURATION:6
#EXT-X-MEDIA-SEQUENCE:1
#EXT-X-MAP:URI="init.mp4"
#EXTINF:6.000,
seg-01.m4s
#EXTINF:6.000,
seg-02.m4s
#EXT-X-ENDLIST
"#;

        let query = "token=auth456";
        let output = rewrite_hls_playlist(variant, query);

        assert!(output.contains(r#"#EXT-X-MAP:URI="init.mp4?token=auth456""#));
        assert!(output.contains("seg-01.m4s?token=auth456"));
        assert!(output.contains("seg-02.m4s?token=auth456"));
        assert!(output.contains("#EXT-X-ENDLIST"));
    }

    #[test]
    fn test_empty_query_params() {
        let variant = "#EXTM3U\nseg-01.ts\n";
        let output = rewrite_hls_playlist(variant, "");
        assert_eq!(output, "#EXTM3U\nseg-01.ts\n");
    }

    #[test]
    fn test_rewrite_fmp4_single_file_byterange_manifest() {
        let manifest = r#"#EXTM3U
#EXT-X-VERSION:7
#EXT-X-TARGETDURATION:12
#EXT-X-MEDIA-SEQUENCE:0
#EXT-X-MAP:URI="Mirai Nikki S01E01.m4s",BYTERANGE="1390@0"
#EXTINF:7.966292,
#EXT-X-BYTERANGE:3047269@1390
Mirai Nikki S01E01.m4s
#EXTINF:2.419083,
#EXT-X-BYTERANGE:602196@3048659
Mirai Nikki S01E01.m4s
#EXT-X-ENDLIST
"#;

        let query = "expires=1790809129&sig=abcdef0123";
        let output = rewrite_hls_playlist(manifest, query);

        // Verify EXT-X-MAP rewritten with query while retaining BYTERANGE attribute
        assert!(output.contains(r#"#EXT-X-MAP:URI="Mirai Nikki S01E01.m4s?expires=1790809129&sig=abcdef0123",BYTERANGE="1390@0""#));
        // Verify BYTERANGE tags are preserved untouched
        assert!(output.contains("#EXT-X-BYTERANGE:3047269@1390"));
        assert!(output.contains("#EXT-X-BYTERANGE:602196@3048659"));
        // Verify segment lines have token appended
        assert!(output.contains("Mirai Nikki S01E01.m4s?expires=1790809129&sig=abcdef0123"));
        assert!(output.contains("#EXT-X-ENDLIST"));
    }

    #[test]
    fn test_rewrite_happy_sugar_life_manifest() {
        let manifest = r#"#EXTM3U
#EXT-X-VERSION:7
#EXT-X-TARGETDURATION:13
#EXT-X-MEDIA-SEQUENCE:0
#EXT-X-MAP:URI="Happy Sugar Life S01E01.m4s",BYTERANGE="1386@0"
#EXTINF:6.631625,
#EXT-X-BYTERANGE:2277107@1386
Happy Sugar Life S01E01.m4s
#EXTINF:3.962292,
#EXT-X-BYTERANGE:1976493@2278493
Happy Sugar Life S01E01.m4s
#EXT-X-ENDLIST
"#;

        let query = "expires=1790858888&sig=cafebabe&ip=127.0.0.1&edge_id=edge-de-01";
        let output = rewrite_hls_playlist(manifest, query);

        assert!(output.contains(r#"#EXT-X-MAP:URI="Happy Sugar Life S01E01.m4s?expires=1790858888&sig=cafebabe&ip=127.0.0.1&edge_id=edge-de-01",BYTERANGE="1386@0""#));
        assert!(output.contains("#EXT-X-BYTERANGE:2277107@1386"));
        assert!(output.contains("#EXT-X-BYTERANGE:1976493@2278493"));
        assert!(output.contains("Happy Sugar Life S01E01.m4s?expires=1790858888&sig=cafebabe&ip=127.0.0.1&edge_id=edge-de-01"));
        assert!(output.contains("#EXT-X-ENDLIST"));
    }
}
