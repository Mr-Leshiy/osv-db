use std::{
    fs::{File, OpenOptions},
    io::{Seek, Write},
    path::Path,
};

/// Performs an HTTP range requests to download a large content to a file.
///
/// <https://developer.mozilla.org/en-US/docs/Web/HTTP/Guides/Range_requests>
/// 1. Sends a HEAD request and inspects two headers:
///    - 'Accept-Ranges': bytes — server supports Range requests
///    - 'Content-Length': total byte size needed to plan chunks
/// 2. If both are present -> download chunked; otherwise -> plain GET
pub async fn chuncked_download_to(
    client: &reqwest::Client,
    url: &str,
    chunk_size: u64,
    path: impl AsRef<Path>,
) -> anyhow::Result<File> {
    let head = client.head(url).send().await?;

    let accepts_ranges = head
        .headers()
        .get(reqwest::header::ACCEPT_RANGES)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v == "bytes");

    let content_length = head
        .headers()
        .get(reqwest::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());

    match (accepts_ranges, content_length) {
        (true, Some(total)) => download_chunked(client, url, total, chunk_size, path).await,
        _ => simple_download_to(client, url, path).await,
    }
}

async fn download_chunked(
    client: &reqwest::Client,
    url: &str,
    total: u64,
    chunk_size: u64,
    path: impl AsRef<Path>,
) -> anyhow::Result<File> {
    let (tx, mut rx) = tokio::sync::mpsc::channel(4);

    let spawner_client = client.clone();
    let spawner_url = url.to_owned();

    // Separate spawner task: kicks off all chunk downloads without blocking the receiver.
    // Each download task sends (chunk, offset) as soon as it finishes, so the receiver
    // can start writing to disk before all tasks have even been spawned.
    let _spawner = tokio::spawn(async move {
        let mut start = 0_u64;
        while start < total {
            let end = start.saturating_add(chunk_size).min(total);
            let offset = start;
            let chunk_client = spawner_client.clone();
            let chunk_url = spawner_url.clone();
            let chunk_tx = tx.clone();

            let _handle = tokio::spawn(async move {
                let result = async {
                    let chunk = chunk_client
                        .get(&chunk_url)
                        .header(
                            reqwest::header::RANGE,
                            format!("bytes={offset}-{}", end.saturating_sub(1)),
                        )
                        .send()
                        .await?
                        .bytes()
                        .await?;
                    anyhow::Ok((chunk, offset))
                }
                .await;
                // Receiver gone means the writer hit an error; nothing to do
                drop(chunk_tx.send(result).await);
            });

            start = end;
        }
        // tx dropped here; channel closes once all chunk tasks also drop their clones
    });

    // Pre-allocate the file so every offset is a valid write position
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .read(true)
        .open(path)?;
    file.set_len(total)?;

    // Receive and write each chunk in the order it arrives
    while let Some(result) = rx.recv().await {
        let (chunk, offset) = result?;
        file.seek(std::io::SeekFrom::Start(offset))?;
        file.write_all(&chunk)?;
    }

    Ok(file)
}

/// Performs a simple HTTP  download via GET request of content to a file.
pub async fn simple_download_to(
    client: &reqwest::Client,
    url: &str,
    path: impl AsRef<Path>,
) -> anyhow::Result<File> {
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .read(true)
        .open(path)?;
    let data = client.get(url).send().await?.bytes().await?;
    file.write_all(&data)?;
    Ok(file)
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Seek, SeekFrom};

    use tempfile::TempDir;

    use super::*;

    /// `httpbin.org/range/<N>` returns exactly N bytes of `a-z` cycling ASCII,
    /// with `Accept-Ranges: bytes` and `Content-Length` set — ideal for chunked path.
    #[tokio::test]
    async fn chuncked_download_to_test() {
        /// <https://httpbin.org/range> limit
        const N: u64 = 1024;
        let expected: Vec<u8> = (0..N).map(|i| b'a' + (i % 26) as u8).collect();

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("output");
        let client = reqwest::Client::new();
        let mut file = chuncked_download_to(
            &client,
            &format!("https://httpbin.org/range/{N}"),
            N / 2,
            path,
        )
        .await
        .unwrap();

        file.seek(SeekFrom::Start(0)).unwrap();
        let mut content = Vec::new();
        file.read_to_end(&mut content).unwrap();
        assert_eq!(content, expected);
    }

    /// `httpbin.org/base64/<b64>` decodes and serves the payload as-is,
    /// without range support — exercises the plain GET fallback.
    /// `SGVsbG8gV29ybGQ=` decodes to `Hello World`.
    #[tokio::test]
    async fn simple_download_to_test() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("output");
        let client = reqwest::Client::new();
        let mut file =
            simple_download_to(&client, "https://httpbin.org/base64/SGVsbG8gV29ybGQ=", path)
                .await
                .unwrap();

        file.seek(SeekFrom::Start(0)).unwrap();
        let mut content = String::new();
        file.read_to_string(&mut content).unwrap();
        assert_eq!(content, "Hello World");
    }
}
