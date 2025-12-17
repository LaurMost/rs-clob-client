use std::sync::Mutex;
use std::time::{Duration, Instant};

use httpmock::prelude::*;
use polymarket_client_sdk::clob::{Client, ConfigBuilder};

#[tokio::test]
async fn retries_retryable_status_and_succeeds() -> anyhow::Result<()> {
    let server = MockServer::start();
    let attempts = Mutex::new(0);

    let mock = server.mock(|when, then| {
        when.method(httpmock::Method::GET).path("/");
        then.respond_with(move |_req: &HttpMockRequest| {
            let mut attempts = attempts.lock().unwrap();
            *attempts += 1;

            if *attempts < 3 {
                return HttpMockResponse::builder()
                    .status(500)
                    .body("temporary failure")
                    .build();
            }

            HttpMockResponse::builder()
                .status(200)
                .header("content-type", "application/json")
                .body("\"ok\"")
                .build()
        });
    });

    let config = ConfigBuilder::default()
        .max_attempts(3)
        .base_backoff(Duration::from_millis(1))
        .max_backoff(Duration::from_millis(5))
        .build()?;
    let client = Client::new(&server.base_url(), config)?;

    let response = client.ok().await?;

    mock.assert_calls(3);
    assert_eq!(response, "ok");

    Ok(())
}

#[tokio::test]
async fn respects_retry_after_header() -> anyhow::Result<()> {
    let server = MockServer::start();
    let attempts = Mutex::new(0);

    let mock = server.mock(|when, then| {
        when.method(httpmock::Method::GET).path("/");
        then.respond_with(move |_req: &HttpMockRequest| {
            let mut attempts = attempts.lock().unwrap();
            *attempts += 1;

            if *attempts == 1 {
                return HttpMockResponse::builder()
                    .status(429)
                    .header("retry-after", "1")
                    .body("slow down")
                    .build();
            }

            HttpMockResponse::builder()
                .status(200)
                .header("content-type", "application/json")
                .body("\"ok\"")
                .build()
        });
    });

    let config = ConfigBuilder::default()
        .max_attempts(2)
        .base_backoff(Duration::from_millis(1))
        .max_backoff(Duration::from_millis(5))
        .build()?;
    let client = Client::new(&server.base_url(), config)?;

    let start = Instant::now();
    let response = client.ok().await?;
    let elapsed = start.elapsed();

    mock.assert_calls(2);
    assert!(elapsed >= Duration::from_secs(1));
    assert_eq!(response, "ok");

    Ok(())
}
