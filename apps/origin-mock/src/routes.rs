use axum::{
    body::Body,
    extract::{Path, Query},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use bytes::Bytes;
use common::{format_content_range, format_unsatisfiable_content_range, HttpRange, RangeError};
use futures_util::stream;
use serde::Deserialize;
use serde_json::json;
use tracing::info;

#[derive(Debug, Deserialize)]
pub struct MediaQueryParams {
    pub size_mb: Option<usize>,
}

pub async fn health_check() -> impl IntoResponse {
    let payload = json!({
        "status": "ok",
        "service": "stream-origin",
        "version": env!("CARGO_PKG_VERSION")
    });
    (StatusCode::OK, Json(payload))
}

pub async fn media_handler(
    Path(path): Path<String>,
    Query(params): Query<MediaQueryParams>,
    headers: HeaderMap,
) -> Response {
    info!(path = %path, "Origin serving media asset");

    match path.as_str() {
        "test/sample.txt" => {
            let full_data = b"Hello from protected origin! Chunk data payload verified.\n";
            let total_len = full_data.len() as u64;

            serve_range_data(
                full_data,
                total_len,
                "text/plain",
                headers.get(header::RANGE),
            )
        }
        "test/large.bin" => {
            let size_mb = params.size_mb.unwrap_or(10).clamp(1, 500);
            let total_bytes = (size_mb * 1024 * 1024) as u64;

            serve_synthetic_stream(
                total_bytes,
                "application/octet-stream",
                headers.get(header::RANGE),
            )
        }
        "hls-demo/master.m3u8" => {
            let playlist = "#EXTM3U\n#EXT-X-VERSION:6\n#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"audio-aac\",NAME=\"English\",DEFAULT=YES,AUTOSELECT=YES,URI=\"audio/en.m3u8\"\n#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID=\"subs\",NAME=\"English\",DEFAULT=NO,AUTOSELECT=YES,URI=\"subs/en.vtt\"\n#EXT-X-STREAM-INF:BANDWIDTH=1500000,RESOLUTION=1280x720,AUDIO=\"audio-aac\",SUBTITLES=\"subs\"\n720p.m3u8\n#EXT-X-STREAM-INF:BANDWIDTH=4000000,RESOLUTION=1920x1080,AUDIO=\"audio-aac\",SUBTITLES=\"subs\"\n1080p.m3u8\n";
            serve_range_data(
                playlist.as_bytes(),
                playlist.len() as u64,
                "application/vnd.apple.mpegurl",
                headers.get(header::RANGE),
            )
        }
        "hls-demo/720p.m3u8" => {
            let playlist = "#EXTM3U\n#EXT-X-VERSION:6\n#EXT-X-TARGETDURATION:6\n#EXT-X-MEDIA-SEQUENCE:1\n#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:6.000,\nseg-01.m4s\n#EXTINF:6.000,\nseg-02.m4s\n#EXT-X-ENDLIST\n";
            serve_range_data(
                playlist.as_bytes(),
                playlist.len() as u64,
                "application/vnd.apple.mpegurl",
                headers.get(header::RANGE),
            )
        }
        "hls-demo/1080p.m3u8" => {
            let playlist = "#EXTM3U\n#EXT-X-VERSION:6\n#EXT-X-TARGETDURATION:6\n#EXT-X-MEDIA-SEQUENCE:1\n#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:6.000,\nseg-01.m4s\n#EXTINF:6.000,\nseg-02.m4s\n#EXT-X-ENDLIST\n";
            serve_range_data(
                playlist.as_bytes(),
                playlist.len() as u64,
                "application/vnd.apple.mpegurl",
                headers.get(header::RANGE),
            )
        }
        "hls-demo/audio/en.m3u8" => {
            let playlist = "#EXTM3U\n#EXT-X-VERSION:6\n#EXT-X-TARGETDURATION:6\n#EXT-X-MEDIA-SEQUENCE:1\n#EXTINF:6.000,\naudio-01.m4s\n#EXT-X-ENDLIST\n";
            serve_range_data(
                playlist.as_bytes(),
                playlist.len() as u64,
                "application/vnd.apple.mpegurl",
                headers.get(header::RANGE),
            )
        }
        "hls-demo/subs/en.vtt" => {
            let vtt = "WEBVTT\n\n00:00:00.000 --> 00:00:06.000\nHello from HLS subtitle track!\n";
            serve_range_data(
                vtt.as_bytes(),
                vtt.len() as u64,
                "text/vtt",
                headers.get(header::RANGE),
            )
        }
        "hls-demo/init.mp4" => {
            // Synthetic 1 KB initialization header
            serve_synthetic_stream(1024, "video/mp4", headers.get(header::RANGE))
        }
        "hls-demo/seg-01.m4s" | "hls-demo/seg-02.m4s" => {
            // Synthetic 500 KB video segments
            serve_synthetic_stream(500 * 1024, "video/iso.segment", headers.get(header::RANGE))
        }
        "hls-demo/audio/audio-01.m4s" => {
            // Synthetic 100 KB audio segment
            serve_synthetic_stream(100 * 1024, "audio/mp4", headers.get(header::RANGE))
        }
        _ => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "Media asset not found on origin" })),
        )
            .into_response(),
    }
}

/// Serves static byte slice with Range (206) and 416 support.
fn serve_range_data(
    data: &'static [u8],
    total_len: u64,
    content_type: &'static str,
    range_header: Option<&HeaderValue>,
) -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));

    if let Some(range_val) = range_header.and_then(|v| v.to_str().ok()) {
        match HttpRange::parse(range_val) {
            Ok(http_range) => match http_range.resolve(total_len) {
                Ok((start, end)) => {
                    let length = end - start + 1;
                    headers.insert(
                        header::CONTENT_RANGE,
                        HeaderValue::from_str(&format_content_range(start, end, total_len))
                            .unwrap(),
                    );
                    headers.insert(
                        header::CONTENT_LENGTH,
                        HeaderValue::from_str(&length.to_string()).unwrap(),
                    );

                    let slice = &data[start as usize..=end as usize];
                    (
                        StatusCode::PARTIAL_CONTENT,
                        headers,
                        Bytes::from_static(slice),
                    )
                        .into_response()
                }
                Err(RangeError::Unsatisfiable { .. }) => {
                    headers.insert(
                        header::CONTENT_RANGE,
                        HeaderValue::from_str(&format_unsatisfiable_content_range(total_len))
                            .unwrap(),
                    );
                    (StatusCode::RANGE_NOT_SATISFIABLE, headers, Body::empty()).into_response()
                }
                Err(_) => (StatusCode::BAD_REQUEST, "Invalid Range Header").into_response(),
            },
            Err(_) => {
                // If Range header is malformed, RFC allows ignoring it and returning 200 OK
                headers.insert(
                    header::CONTENT_LENGTH,
                    HeaderValue::from_str(&total_len.to_string()).unwrap(),
                );
                (StatusCode::OK, headers, Bytes::from_static(data)).into_response()
            }
        }
    } else {
        headers.insert(
            header::CONTENT_LENGTH,
            HeaderValue::from_str(&total_len.to_string()).unwrap(),
        );
        (StatusCode::OK, headers, Bytes::from_static(data)).into_response()
    }
}

/// Serves synthetic media stream with Range (206) and 416 support without loading into RAM.
fn serve_synthetic_stream(
    total_bytes: u64,
    content_type: &'static str,
    range_header: Option<&HeaderValue>,
) -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));

    let chunk_size: u64 = 64 * 1024; // 64 KB

    if let Some(range_val) = range_header.and_then(|v| v.to_str().ok()) {
        match HttpRange::parse(range_val) {
            Ok(http_range) => match http_range.resolve(total_bytes) {
                Ok((start, end)) => {
                    let length = end - start + 1;
                    headers.insert(
                        header::CONTENT_RANGE,
                        HeaderValue::from_str(&format_content_range(start, end, total_bytes))
                            .unwrap(),
                    );
                    headers.insert(
                        header::CONTENT_LENGTH,
                        HeaderValue::from_str(&length.to_string()).unwrap(),
                    );

                    let stream = stream::unfold(start, move |current| async move {
                        if current > end {
                            None
                        } else {
                            let remaining = end - current + 1;
                            let to_send = remaining.min(chunk_size) as usize;
                            let buffer = vec![0xAA; to_send];
                            Some((
                                Ok::<Bytes, std::io::Error>(Bytes::from(buffer)),
                                current + to_send as u64,
                            ))
                        }
                    });

                    (
                        StatusCode::PARTIAL_CONTENT,
                        headers,
                        Body::from_stream(stream),
                    )
                        .into_response()
                }
                Err(RangeError::Unsatisfiable { .. }) => {
                    headers.insert(
                        header::CONTENT_RANGE,
                        HeaderValue::from_str(&format_unsatisfiable_content_range(total_bytes))
                            .unwrap(),
                    );
                    (StatusCode::RANGE_NOT_SATISFIABLE, headers, Body::empty()).into_response()
                }
                Err(_) => (StatusCode::BAD_REQUEST, "Invalid Range Header").into_response(),
            },
            Err(_) => {
                headers.insert(
                    header::CONTENT_LENGTH,
                    HeaderValue::from_str(&total_bytes.to_string()).unwrap(),
                );
                let stream = stream::unfold(0u64, move |current| async move {
                    if current >= total_bytes {
                        None
                    } else {
                        let remaining = total_bytes - current;
                        let to_send = remaining.min(chunk_size) as usize;
                        let buffer = vec![0xAA; to_send];
                        Some((
                            Ok::<Bytes, std::io::Error>(Bytes::from(buffer)),
                            current + to_send as u64,
                        ))
                    }
                });
                (StatusCode::OK, headers, Body::from_stream(stream)).into_response()
            }
        }
    } else {
        headers.insert(
            header::CONTENT_LENGTH,
            HeaderValue::from_str(&total_bytes.to_string()).unwrap(),
        );
        let stream = stream::unfold(0u64, move |current| async move {
            if current >= total_bytes {
                None
            } else {
                let remaining = total_bytes - current;
                let to_send = remaining.min(chunk_size) as usize;
                let buffer = vec![0xAA; to_send];
                Some((
                    Ok::<Bytes, std::io::Error>(Bytes::from(buffer)),
                    current + to_send as u64,
                ))
            }
        });
        (StatusCode::OK, headers, Body::from_stream(stream)).into_response()
    }
}
