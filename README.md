![Polymarket](assets/logo.png)

# Polymarket Rust Client

[![Crates.io](https://img.shields.io/crates/v/polymarket-client-sdk.svg)](https://crates.io/crates/polymarket-client-sdk)
[![CI](https://github.com/Polymarket/rs-clob-client/actions/workflows/ci.yml/badge.svg)](https://github.com/Polymarket/rs-clob-client/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/Polymarket/rs-clob-client/graph/badge.svg?token=FW1BYWWFJ2)](https://codecov.io/gh/Polymarket/rs-clob-client)

An ergonomic Rust client for interacting with Polymarket services, primarily the Central Limit Order Book (CLOB).
This crate provides strongly typed request builders, authenticated endpoints, `alloy` support and more.

## Table of Contents

- [Overview](#overview)
- [Getting Started](#getting-started)
- [Examples](#examples)
- [Setting Token Allowances](#token-allowances)
- [Minimum Supported Rust Version (MSRV)](#minimum-supported-rust-version-msrv)
- [Contributing](#contributing)
- [About Polymarket](#about-polymarket)

## Overview

- **Typed CLOB requests** (orders, trades, markets, balances, and more)
- **Dual authentication flows**
    - Normal authenticated flow
    - [Builder](https://docs.polymarket.com/developers/builders/builder-intro) authentication flow
- **Type-level state machine**
    - Prevents using authenticated endpoints before authenticating
    - Compile-time enforcement of correct transitions
- **Signer support** via `alloy::signers::Signer`
    - Including remote signers, e.g. AWS KMS
- **Zero-cost abstractions** — no dynamic dispatch in hot paths
- **Order builders** for easy construction & signing
- **Full `serde` support**
- **Async-first design** with `reqwest`
- **WebSocket subscriptions** for market and user events


## Getting started

Add the crate to your `Cargo.toml`:

```toml
[dependencies]
polymarket-client-sdk = "0.1"
```

or

```bash
cargo add polymarket-client-sdk
```

Then run any of the examples
```bash
cargo run --example unauthenticated
```

## Examples

Some hand-picked examples. Please see `examples/` for more.

- [authenticated](./examples/authenticated.rs)
- [aws_authenticated](./examples/aws_authenticated.rs)
- [builder_authenticated](./examples/builder_authenticated.rs)
- [streaming](./examples/streaming.rs)
- [websocket](./examples/websocket.rs)
- [unauthenticated](./examples/unauthenticated.rs)

### Unauthenticated client (read-only)
```rust,no_run
use polymarket_client_sdk::clob::Client;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = Client::default();

    let ok = client.ok().await?;
    println!("Ok: {ok}");

    Ok(())
}
```

### Authenticated client

Set `POLYMARKET_PRIVATE_KEY` as an environment variable with your private key.

#### [EOA](https://www.binance.com/en/academy/glossary/externally-owned-account-eoa) wallets
If using MetaMask or hardware wallet, you must first set token allowances. See [Token Allowances](#token-allowances) section below.

```rust,no_run
use std::str::FromStr as _;

use alloy::signers::Signer as _;
use alloy::signers::local::LocalSigner;
use polymarket_client_sdk::{POLYGON, PRIVATE_KEY_VAR};
use polymarket_client_sdk::clob::{Client, Config};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let private_key = std::env::var(PRIVATE_KEY_VAR).expect("Need a private key");
    let signer = LocalSigner::from_str(&private_key)?.with_chain_id(Some(POLYGON));
    let client = Client::new("https://clob.polymarket.com", Config::default())?
        .authentication_builder(&signer)
        .authenticate()
        .await?;

    let ok = client.ok().await?;
    println!("Ok: {ok}");

    let api_keys = client.api_keys().await?;
    println!("API keys: {api_keys:?}");

    Ok(())
}
```

### WebSocket subscriptions

Use the WebSocket client for streaming market or user events. Market subscriptions do not require
authentication. User subscriptions reuse the API credentials you obtained while authenticating the
HTTP client, so you can share them directly without deriving a new key.

```rust,no_run
use std::str::FromStr as _;

use alloy::signers::Signer as _;
use alloy::signers::local::LocalSigner;
use futures::StreamExt as _;
use polymarket_client_sdk::clob::{Client, Config};
use polymarket_client_sdk::ws::model::WsMessage;
use polymarket_client_sdk::{POLYGON, PRIVATE_KEY_VAR, StreamConfig, WsClient};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Market channel
    let mut market_stream = WsClient::market(
        "wss://ws.polymarket.com/ws",
        ["2f9b8c00-f8aa-4d7b-9ad5-3f6c955828b9"],
    )?
    .subscribe_stream(StreamConfig::default())
    .await?;

    tokio::spawn(async move {
        while let Some(msg) = market_stream.next().await {
            println!("market message: {msg:?}");
        }
    });

    // User channel (requires credentials from an authenticated HTTP client)
    let private_key = std::env::var(PRIVATE_KEY_VAR)?;
    let signer = LocalSigner::from_str(&private_key)?.with_chain_id(Some(POLYGON));
    let http_client = Client::new("https://clob.polymarket.com", Config::default())?
        .authentication_builder(&signer)
        .authenticate()
        .await?;

    let credentials = http_client.credentials().clone();

    let mut user_stream = WsClient::user(
        "wss://ws.polymarket.com/ws",
        ["2438c3cb-4372-4ede-a3e6-cc19a610aa1c"],
        credentials,
    )?
    .subscribe_stream(StreamConfig::default())
    .await?;

    while let Some(msg) = user_stream.next().await {
        match msg {
            WsMessage::OrderEvent(event) => println!("order event: {event:?}"),
            WsMessage::TradeEvent(event) => println!("trade event: {event:?}"),
            _ => (),
        }
    }

    Ok(())
}
```

### Configuration

`clob::Config` controls how the underlying HTTP client behaves:

* `request_timeout`: Optional request timeout (`None` preserves reqwest's default).
* `connect_timeout`: Optional connect timeout (`None` preserves reqwest's default).
* `user_agent`: Optional user agent string (defaults to `rs_clob_client`).
* `client_builder`: Optional closure that returns a preconfigured `reqwest::ClientBuilder` used as the base for the client.
* `use_server_time`: Whether to fetch server time before signing requests.

```rust
# fn main() -> Result<(), Box<dyn std::error::Error>> {
use std::sync::Arc;
use std::time::Duration;

use polymarket_client_sdk::clob::{Client, Config, ConfigBuilder};

let config = ConfigBuilder::default()
    .request_timeout(Some(Duration::from_secs(10)))
    .connect_timeout(Some(Duration::from_secs(5)))
    .user_agent(Some("custom-agent/1.0".to_string()))
    .client_builder(Some(Arc::new(|| reqwest::Client::builder().no_proxy())))
    .build()?;

let client = Client::new("https://clob.polymarket.com", config)?;

# Ok(())
# }
```

#### Proxy/Safe wallets
For proxy/Safe wallets, create your client as such:

```rust,ignore
let client = Client::new("https://clob.polymarket.com", Config::default())?
    .authentication_builder(&signer)
    .funder(address!("<your-address>"))
    .signature_type(SignatureType::Proxy)
    .authenticate()
    .await?;
```

#### Funder Address
The **funder address** is the actual address that holds your funds on Polymarket. When using proxy wallets (email wallets
like Magic or browser extension wallets), the signing key differs from the address holding the funds. The funder address
ensures orders are properly attributed to your funded account.

#### Signature Types
The **signature_type** parameter tells the system how to verify your signatures:
- `signature_type=0` (default): Standard EOA (Externally Owned Account) signatures - includes MetaMask, hardware wallets,
   and any wallet where you control the private key directly
- `signature_type=1`: Email/Magic wallet signatures (delegated signing)
- `signature_type=2`: Browser wallet proxy signatures (when using a proxy contract, not direct wallet connections)

See [SignatureType](src/types.rs#L115) for more information.

**Place a market order**

```rust,no_run
use std::str::FromStr as _;

use alloy::signers::Signer as _;
use alloy::signers::local::LocalSigner;
use polymarket_client_sdk::{POLYGON, PRIVATE_KEY_VAR};
use polymarket_client_sdk::clob::{Client, Config};
use polymarket_client_sdk::types::{Amount, OrderType, Side};
use rust_decimal::Decimal;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let private_key = std::env::var(PRIVATE_KEY_VAR).expect("Need a private key");
    let signer = LocalSigner::from_str(&private_key)?.with_chain_id(Some(POLYGON));
    let client = Client::new("https://clob.polymarket.com", Config::default())?
        .authentication_builder(&signer)
        .authenticate()
        .await?;

    let order = client
        .market_order()
        .token_id("token")
        .amount(Amount::usdc(Decimal::ONE_HUNDRED)?)
        .side(Side::Buy)
        .order_type(OrderType::FOK)
        .build()
        .await?;
    let signed_order = client.sign(&signer, order).await?;
    let response = client.post_order(signed_order).await?;

    Ok(())
}
```

**Place a limit order**

```rust,no_run
use std::str::FromStr as _;

use alloy::signers::Signer as _;
use alloy::signers::local::LocalSigner;
use polymarket_client_sdk::{POLYGON, PRIVATE_KEY_VAR};
use polymarket_client_sdk::clob::{Client, Config};
use polymarket_client_sdk::types::{Amount, OrderType, Side};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let private_key = std::env::var(PRIVATE_KEY_VAR).expect("Need a private key");
    let signer = LocalSigner::from_str(&private_key)?.with_chain_id(Some(POLYGON));
    let client = Client::new("https://clob.polymarket.com", Config::default())?
        .authentication_builder(&signer)
        .authenticate()
        .await?;

    let order = client
        .limit_order()
        .token_id("1")
        .size(Decimal::ONE_HUNDRED)
        .price(dec!(0.1))
        .side(Side::Buy)
        .build()
        .await?;
    let signed_order = client.sign(&signer, order).await?;
    let response = client.post_order(signed_order).await?;

    Ok(())
}
```

### Builder-authenticated client

Remote signing
```rust,no_run
use std::str::FromStr as _;

use alloy::primitives::{Address, address};
use alloy::signers::Signer as _;
use alloy::signers::local::LocalSigner;
use polymarket_client_sdk::auth::builder::Config as BuilderConfig;
use polymarket_client_sdk::{POLYGON, PRIVATE_KEY_VAR};
use polymarket_client_sdk::clob::{Client, Config};
use polymarket_client_sdk::types::{SignatureType, TradesRequest};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let private_key = std::env::var(PRIVATE_KEY_VAR).expect("Need a private key");
    let signer = LocalSigner::from_str(&private_key)?.with_chain_id(Some(POLYGON));
    let builder_config = BuilderConfig::remote("http://localhost:3000/sign", None)?; // Or your signing server
    let funder = address!("0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266"); // Use your funder address

    let client = Client::new("https://clob.polymarket.com", Config::default())?
        .authentication_builder(&signer)
        .funder(funder)
        .signature_type(SignatureType::Proxy)
        .authenticate()
        .await?;

    let client = client.promote_to_builder(builder_config)?;

    let ok = client.ok().await?;
    println!("Ok: {ok}");

    let api_keys = client.api_keys().await?;
    println!("API keys: {api_keys:?}");

    let builder_trades = client.builder_trades(&TradesRequest::default(), None).await?;
    println!("Builder trades: {builder_trades:?}");

    Ok(())
}
```

## Token Allowances

### Do I need to set allowances?
MetaMask and EOA users must set token allowances.
If you are using a proxy or [Safe](https://help.safe.global/en/articles/40869-what-is-safe)-type wallet, then you do not.

### What are allowances?
Think of allowances as permissions. Before Polymarket can move your funds to execute trades, you need to give the
exchange contracts permission to access your USDC and conditional tokens.

### Quick Setup
You need to approve two types of tokens:
1. **USDC** (for deposits and trading)
2. **Conditional Tokens** (the outcome tokens you trade)

Each needs approval for the exchange contracts to work properly.

### Setting Allowances
Use [examples/approvals.rs](examples/approvals.rs) to approve the right contracts. Run once to approve USDC. Then change
the `TOKEN_TO_APPROVE` and run for each conditional token.

**Pro tip**: You only need to set these once per wallet. After that, you can trade freely.

## Minimum Supported Rust Version (MSRV)

**MSRV: Rust [1.88](https://releases.rs/docs/1.88.0/)**

Older versions *may* compile, but are not supported.

This project aims to maintain compatibility with a Rust version that is at least six months old.

Version updates may occur more frequently than the policy guideline states if external forces require it. For example,
a CVE in a downstream dependency requiring an MSRV bump would be considered an acceptable reason to violate the six-month
guideline.


## Contributing
We encourage contributions from the community. Check out our [contributing guidelines](.github/CONTRIBUTING.md) for
instructions on how to contribute to this SDK.


## About Polymarket
[Polymarket](https://docs.polymarket.com/polymarket-learn/get-started/what-is-polymarket) is the world’s largest prediction market, allowing you to stay informed and profit from your knowledge by
betting on future events across various topics.
Studies show prediction markets are often more accurate than pundits because they combine news, polls, and expert
opinions into a single value that represents the market’s view of an event’s odds. Our markets reflect accurate, unbiased,
and real-time probabilities for the events that matter most to you. Markets seek truth.
