use std::time::Duration;

use futures::{SinkExt as _, StreamExt as _};
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::interval;
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
        let (socket, _response) = connect_async(self.base_url.clone()).await?;
        let (write, read) = socket.split();
        let (tx, rx) = mpsc::unbounded_channel();

        let forward = spawn_writer(write, rx);

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
}

/// Active WebSocket connection. Messages can be received by polling [`WsConnection::next`]
/// and sent via [`WsConnection::send_text`] or [`WsConnection::send_json`].
pub struct WsConnection {
    incoming: futures::stream::SplitStream<Socket>,
    outgoing: mpsc::UnboundedSender<Message>,
    forward: JoinHandle<Result<()>>,
}

impl WsConnection {
    /// Send a pre-serialized message.
    pub async fn send_text(&self, message: impl Into<String>) -> Result<()> {
        self.outgoing
            .send(Message::Text(message.into()))
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
        let _ = self.outgoing.send(Message::Close(None));
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
    mut rx: mpsc::UnboundedReceiver<Message>,
) -> JoinHandle<Result<()>> {
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(10));

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
