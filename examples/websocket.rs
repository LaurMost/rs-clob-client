#![allow(clippy::print_stdout, reason = "Examples are okay to print to stdout")]

use std::str::FromStr as _;

use alloy::signers::Signer as _;
use alloy::signers::local::LocalSigner;
use futures::StreamExt as _;
use polymarket_client_sdk::clob::{Client, Config};
use polymarket_client_sdk::ws::model::WsMessage;
use polymarket_client_sdk::{POLYGON, PRIVATE_KEY_VAR, StreamConfig, WsClient};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = StreamConfig::default();

    // Market channel subscriptions do not require authentication.
    let mut market_stream = WsClient::market(
        "wss://ws.polymarket.com/ws",
        [
            // Replace these with the asset ids you want to monitor.
            "2f9b8c00-f8aa-4d7b-9ad5-3f6c955828b9",
            "5f52c8d1-5d2d-45bb-a09d-62f4fbcd48cc",
        ],
    )?
    .subscribe_stream(config.clone())
    .await?;

    tokio::spawn(async move {
        while let Some(message) = market_stream.next().await {
            println!("market message: {message:?}");
        }
    });

    // User channel subscriptions require credentials. Reuse the authenticated HTTP client's
    // credentials instead of deriving a new API key.
    let private_key = std::env::var(PRIVATE_KEY_VAR).expect("Need a private key");
    let signer = LocalSigner::from_str(&private_key)?.with_chain_id(Some(POLYGON));

    let http_client = Client::new("https://clob.polymarket.com", Config::default())?
        .authentication_builder(&signer)
        .authenticate()
        .await?;

    let credentials = http_client.credentials().clone();

    let mut user_stream = WsClient::user(
        "wss://ws.polymarket.com/ws",
        [
            // Replace with the markets you want user-level notifications for.
            "2438c3cb-4372-4ede-a3e6-cc19a610aa1c",
        ],
        credentials,
    )?
    .subscribe_stream(config)
    .await?;

    while let Some(message) = user_stream.next().await {
        match message {
            WsMessage::OrderEvent(event) => println!("order event: {event:?}"),
            WsMessage::TradeEvent(event) => println!("trade event: {event:?}"),
            other => println!("user message: {other:?}"),
        }
    }

    Ok(())
}
