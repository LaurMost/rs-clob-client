use std::cmp::min;
use std::time::Duration;

pub mod model;

use derive_builder::Builder;
use futures::{SinkExt as _, StreamExt as _};
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{interval, sleep};
use tokio_stream::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use url::Url;

use crate::Result;
use crate::auth::Credentials;
use crate::error::{Error, Kind};

type Socket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

/// [`WsClient`] channel choice
#[derive(Clone, Debug)]
pub enum Channel {
    /// Market-level updates filtered to specific asset ids.
    Market { asset_ids: Vec<String> },
    /// User-level updates filtered to specific markets.
    User { markets: Vec<String> },
}

/// Configuration for websocket streaming behavior.
///
/// Bounded channels enforce backpressure on both outbound websocket frames and inbound message
/// delivery. When the consumer cannot keep up with incoming messages, sends will await additional
/// capacity instead of growing unbounded. Reconnection attempts use exponential backoff capped by
/// `max_backoff` and will stop once `max_attempts` is exceeded.
#[derive(Clone, Builder)]
#[builder(pattern = "owned", build_fn(error = "Error"))]
#[builder(default)]
pub struct StreamConfig {
    /// Maximum number of connection attempts including the initial connection.
    max_attempts: usize,
    /// Base duration for exponential backoff between retry attempts.
    base_backoff: Duration,
    /// Maximum duration to wait between retry attempts.
    max_backoff: Duration,
    /// Duration between keepalive pings sent to the server.
    keepalive_interval: Duration,
    /// Bounded capacity for websocket writes. Backpressure is applied if the producer outpaces the
    /// writer task.
    outbound_buffer: usize,
    /// Bounded capacity for inbound [`WsMessage`] delivery to consumers.
    inbound_buffer: usize,
    /// Bounded capacity for error notifications delivered alongside the message stream.
    error_buffer: usize,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            base_backoff: Duration::from_millis(500),
            max_backoff: Duration::from_secs(8),
            keepalive_interval: Duration::from_secs(10),
            outbound_buffer: 64,
            inbound_buffer: 256,
            error_buffer: 16,
        }
    }
}

/// Builder for [`WsClient`].
#[derive(Clone, Debug)]
pub struct WsClientBuilder {
    base_url: Url,
    credentials: Option<Credentials>,
    channel: Option<Channel>,
}

impl WsClientBuilder {
    /// Create a new builder from the WebSocket base url.
    pub fn new(base_url: impl AsRef<str>) -> Result<Self> {
        let base_url =
            Url::parse(base_url.as_ref()).map_err(|e| Error::with_source(Kind::Validation, e))?;

        Ok(Self {
            base_url,
            credentials: None,
            channel: None,
        })
    }

    /// Provide API credentials for user channels.
    #[must_use]
    pub fn credentials(mut self, credentials: Credentials) -> Self {
        self.credentials = Some(credentials);
        self
    }

    /// Select which channel to subscribe to when connecting.
    #[must_use]
    pub fn channel(mut self, channel: Channel) -> Self {
        self.channel = Some(channel);
        self
    }

    /// Finalize the builder into a [`WsClient`].
    pub fn build(self) -> Result<WsClient> {
        let channel = self
            .channel
            .ok_or_else(|| Error::validation("channel selection is required"))?;

        if matches!(channel, Channel::User { .. }) && self.credentials.is_none() {
            return Err(Error::validation(
                "user channel subscriptions require credentials",
            ));
        }

        Ok(WsClient {
            base_url: self.base_url,
            credentials: self.credentials,
            channel,
        })
    }
}

/// WebSocket client with helpers for subscribing to market or user channels.
#[derive(Clone, Debug)]
pub struct WsClient {
    base_url: Url,
    credentials: Option<Credentials>,
    channel: Channel,
}

impl WsClient {
    /// Begin building a new client targeting `base_url`.
    pub fn builder(base_url: impl AsRef<str>) -> Result<WsClientBuilder> {
        WsClientBuilder::new(base_url)
    }

    /// Convenience helper to create a market subscription client.
    pub fn market(
        base_url: impl AsRef<str>,
        asset_ids: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self> {
        Self::builder(base_url)?
            .channel(Channel::Market {
                asset_ids: asset_ids.into_iter().map(Into::into).collect(),
            })
            .build()
    }

    /// Convenience helper to create a user subscription client.
    pub fn user(
        base_url: impl AsRef<str>,
        markets: impl IntoIterator<Item = impl Into<String>>,
        credentials: Credentials,
    ) -> Result<Self> {
        Self::builder(base_url)?
            .credentials(credentials)
            .channel(Channel::User {
                markets: markets.into_iter().map(Into::into).collect(),
            })
            .build()
    }

    /// Connect and subscribe, returning a [`WsConnection`] that yields incoming messages.
    pub async fn connect(&self) -> Result<WsConnection> {
        self.connect_with_config(&StreamConfig::default()).await
    }

    /// Connect using the provided [`StreamConfig`].
    pub async fn connect_with_config(&self, config: &StreamConfig) -> Result<WsConnection> {
        let (socket, _response) = connect_async(self.base_url.clone()).await?;
        let (write, read) = socket.split();
        let (tx, rx) = mpsc::channel(config.outbound_buffer);

        let forward = spawn_writer(write, rx, config.keepalive_interval);

        let mut connection = WsConnection {
            incoming: read,
            outgoing: tx,
            forward,
        };

        connection
            .subscribe(&self.channel, self.credentials.as_ref())
            .await?;

        Ok(connection)
    }

    /// Subscribe to market-level updates, yielding a stream of [`model::WsMessage`] instances.
    /// Errors encountered while deserializing messages or reconnecting will be sent on the
    /// companion error channel returned by [`WsStream::errors`].
    pub async fn subscribe_market_stream(
        base_url: impl AsRef<str>,
        asset_ids: impl IntoIterator<Item = impl Into<String>>,
        config: StreamConfig,
    ) -> Result<WsStream> {
        Self::market(base_url, asset_ids)?
            .subscribe_stream(config)
            .await
    }

    /// Subscribe to user-level updates, yielding a stream of [`model::WsMessage`] instances.
    /// Errors encountered while deserializing messages or reconnecting will be sent on the
    /// companion error channel returned by [`WsStream::errors`].
    pub async fn subscribe_user_stream(
        base_url: impl AsRef<str>,
        markets: impl IntoIterator<Item = impl Into<String>>,
        credentials: Credentials,
        config: StreamConfig,
    ) -> Result<WsStream> {
        Self::user(base_url, markets, credentials)?
            .subscribe_stream(config)
            .await
    }

    /// Subscribe using this client's channel configuration with the provided streaming settings.
    pub async fn subscribe_stream(&self, config: StreamConfig) -> Result<WsStream> {
        let (message_tx, message_rx) = mpsc::channel(config.inbound_buffer);
        let (error_tx, error_rx) = mpsc::channel(config.error_buffer);
        let client = self.clone();
        let config_for_task = config.clone();

        let task = tokio::spawn(async move {
            let mut attempts = 0usize;

            while attempts < config_for_task.max_attempts {
                if message_tx.is_closed() {
                    break;
                }

                match client.connect_with_config(&config_for_task).await {
                    Ok(mut connection) => {
                        match pump_messages(&mut connection, &message_tx, &error_tx).await {
                            PumpOutcome::End => break,
                            PumpOutcome::Retry(err) => {
                                let _ = error_tx.send(err).await;
                            }
                        }
                    }
                    Err(err) => {
                        let _ = error_tx.send(err).await;
                    }
                }

                attempts += 1;
                if attempts >= config_for_task.max_attempts || message_tx.is_closed() {
                    break;
                }

                sleep(backoff_duration(&config_for_task, attempts - 1)).await;
            }
        });

        Ok(WsStream {
            messages: ReceiverStream::new(message_rx),
            errors: error_rx,
            _task: task,
        })
    }
}

/// Active WebSocket connection. Messages can be received by polling [`WsConnection::next`]
/// and sent via [`WsConnection::send_text`] or [`WsConnection::send_json`].
pub struct WsConnection {
    incoming: futures::stream::SplitStream<Socket>,
    outgoing: mpsc::Sender<Message>,
    forward: JoinHandle<Result<()>>,
}

/// Handle for streaming websocket updates with graceful backpressure and error reporting.
pub struct WsStream {
    messages: ReceiverStream<model::WsMessage>,
    errors: mpsc::Receiver<Error>,
    _task: JoinHandle<()>,
}

impl WsStream {
    /// Access the error channel carrying connection and deserialization failures.
    pub fn errors(&mut self) -> &mut mpsc::Receiver<Error> {
        &mut self.errors
    }
}

impl Stream for WsStream {
    type Item = model::WsMessage;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.messages.poll_next_unpin(cx)
    }
}

impl WsConnection {
    /// Send a pre-serialized message.
    pub async fn send_text(&self, message: impl Into<String>) -> Result<()> {
        self.outgoing
            .send(Message::Text(message.into()))
            .await
            .map_err(|e| Error::with_source(Kind::Internal, e))
    }

    async fn send_message(&self, message: Message) -> Result<()> {
        self.outgoing
            .send(message)
            .await
            .map_err(|e| Error::with_source(Kind::Internal, e))
    }

    /// Send JSON-serialized data over the socket.
    pub async fn send_json<T: Serialize>(&self, value: &T) -> Result<()> {
        let payload = serde_json::to_string(value)?;
        self.send_text(payload).await
    }

    /// Receive the next message from the socket.
    pub async fn next(&mut self) -> Option<Result<Message>> {
        self.incoming
            .next()
            .await
            .map(|msg| msg.map_err(Into::into))
    }

    /// Close the socket gracefully.
    pub async fn close(self) -> Result<()> {
        // Sending a close frame and dropping the sender will stop the writer task.
        let _ = self.outgoing.send(Message::Close(None)).await;
        drop(self.outgoing);
        self.forward.await??;
        Ok(())
    }

    async fn subscribe(
        &mut self,
        channel: &Channel,
        credentials: Option<&Credentials>,
    ) -> Result<()> {
        match channel {
            Channel::Market { asset_ids } => {
                let payload = MarketSubscribe {
                    kind: "subscribe",
                    channel: MarketSubscribe::CHANNEL,
                    asset_ids,
                };
                self.send_json(&payload).await
            }
            Channel::User { markets } => {
                let Some(credentials) = credentials else {
                    return Err(Error::validation(
                        "credentials are required for user channel subscriptions",
                    ));
                };

                let payload = UserSubscribe {
                    kind: "subscribe",
                    channel: UserSubscribe::CHANNEL,
                    markets,
                    api_key: credentials.key,
                    api_secret: credentials.secret.reveal(),
                    api_passphrase: credentials.passphrase.reveal(),
                };
                self.send_json(&payload).await
            }
        }
    }
}

enum PumpOutcome {
    End,
    Retry(Error),
}

async fn pump_messages(
    connection: &mut WsConnection,
    outbound: &mpsc::Sender<model::WsMessage>,
    errors: &mpsc::Sender<Error>,
) -> PumpOutcome {
    loop {
        if outbound.is_closed() {
            return PumpOutcome::End;
        }

        match connection.next().await {
            Some(Ok(Message::Text(text))) => {
                match serde_json::from_str::<model::WsMessage>(&text) {
                    Ok(message) => {
                        if outbound.send(message).await.is_err() {
                            return PumpOutcome::End;
                        }
                    }
                    Err(err) => {
                        let _ = errors.send(Error::with_source(Kind::Internal, err)).await;
                    }
                }
            }
            Some(Ok(Message::Ping(payload))) => {
                let _ = connection.send_message(Message::Pong(payload)).await;
            }
            Some(Ok(Message::Close(_))) => {
                return PumpOutcome::Retry(Error::validation("websocket closed by peer"));
            }
            Some(Ok(_)) => {}
            Some(Err(err)) => return PumpOutcome::Retry(err.into()),
            None => {
                return PumpOutcome::Retry(Error::validation(
                    "websocket stream ended unexpectedly",
                ));
            }
        }
    }
}

fn backoff_duration(config: &StreamConfig, attempt: usize) -> Duration {
    let multiplier = 1u32.saturating_shl(attempt as u32);
    let backoff = config.base_backoff.saturating_mul(multiplier);
    min(backoff, config.max_backoff)
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
struct MarketSubscribe<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    channel: &'static str,
    asset_ids: &'a [String],
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
struct UserSubscribe<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    channel: &'static str,
    markets: &'a [String],
    api_key: crate::types::ApiKey,
    api_secret: &'a str,
    api_passphrase: &'a str,
}

fn spawn_writer(
    mut write: futures::stream::SplitSink<Socket, Message>,
    mut rx: mpsc::Receiver<Message>,
    keepalive_interval: Duration,
) -> JoinHandle<Result<()>> {
    tokio::spawn(async move {
        let mut ticker = interval(keepalive_interval);

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    write.send(Message::Text(String::from("PING"))).await?;
                }
                Some(msg) = rx.recv() => {
                    if let Message::Close(_) = msg {
                        write.send(msg).await?;
                        write.close().await?;
                        return Ok(());
                    }

                    write.send(msg).await?;
                }
                else => {
                    write.close().await?;
                    return Ok(());
                }
            }
        }
    })
}

impl MarketSubscribe<'_> {
    const CHANNEL: &'static str = "market";
}

impl UserSubscribe<'_> {
    const CHANNEL: &'static str = "user";
}
