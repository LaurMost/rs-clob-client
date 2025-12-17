use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Order book snapshot updates.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Book {
    pub market: String,
    pub asset_id: String,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub timestamp: String,
}

/// Best bid/ask changes for a market.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct PriceChange {
    pub market: String,
    pub asset_id: String,
    pub best_bid: String,
    pub best_offer: String,
}

/// Minimum tick size updates.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct TickSizeChange {
    pub market: String,
    pub minimum_tick_size: String,
}

/// Latest trade price updates.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct LastTradePrice {
    pub market: String,
    pub asset_id: String,
    pub price: String,
    pub side: String,
    pub timestamp: String,
}

/// Order lifecycle updates on the user channel.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct OrderEvent {
    pub order_id: String,
    pub market: String,
    pub asset_id: String,
    pub price: String,
    pub size: String,
    pub remaining: String,
    pub side: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub client_order_id: Option<String>,
}

/// Trade fills on the user channel.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct TradeEvent {
    pub trade_id: String,
    pub order_id: String,
    pub market: String,
    pub asset_id: String,
    pub price: String,
    pub size: String,
    pub side: String,
    pub taker: String,
    pub maker: String,
    pub timestamp: String,
}

/// Bid/ask levels represented as two stringly-typed numbers: price and size.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PriceLevel(pub String, pub String);

/// Top-level WebSocket message wrapper.
#[derive(Clone, Debug, PartialEq)]
pub enum WsMessage {
    Book(Book),
    PriceChange(PriceChange),
    TickSizeChange(TickSizeChange),
    LastTradePrice(LastTradePrice),
    OrderEvent(OrderEvent),
    TradeEvent(TradeEvent),
}

impl Serialize for WsMessage {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let (tag_field, tag_value, payload) = match self {
            WsMessage::Book(book) => ("event_type", "book", Value::from_serialize(book)),
            WsMessage::PriceChange(change) => {
                ("event_type", "price_change", Value::from_serialize(change))
            }
            WsMessage::TickSizeChange(change) => (
                "event_type",
                "tick_size_change",
                Value::from_serialize(change),
            ),
            WsMessage::LastTradePrice(price) => {
                ("type", "last_trade_price", Value::from_serialize(price))
            }
            WsMessage::OrderEvent(event) => {
                ("event_type", "order_event", Value::from_serialize(event))
            }
            WsMessage::TradeEvent(event) => {
                ("event_type", "trade_event", Value::from_serialize(event))
            }
        };

        let mut map = match payload {
            Value::Object(map) => map,
            other => {
                return Err(serde::ser::Error::custom(format!(
                    "expected payload object, found {other:?}"
                )));
            }
        };

        map.insert(tag_field.to_string(), Value::String(tag_value.to_string()));
        Value::Object(map).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for WsMessage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let mut value = Value::deserialize(deserializer)?;
        let tag = match value
            .get("event_type")
            .or_else(|| value.get("type"))
            .and_then(Value::as_str)
        {
            Some(tag) => tag.to_string(),
            None => {
                return Err(serde::de::Error::custom(
                    "websocket message missing event_type/type tag",
                ));
            }
        };

        if let Value::Object(map) = &mut value {
            map.remove("event_type");
            map.remove("type");
        }

        match tag.as_str() {
            "book" => Book::deserialize(&value).map(WsMessage::Book),
            "price_change" => PriceChange::deserialize(&value).map(WsMessage::PriceChange),
            "tick_size_change" => {
                TickSizeChange::deserialize(&value).map(WsMessage::TickSizeChange)
            }
            "last_trade_price" => {
                LastTradePrice::deserialize(&value).map(WsMessage::LastTradePrice)
            }
            "order_event" => OrderEvent::deserialize(&value).map(WsMessage::OrderEvent),
            "trade_event" => TradeEvent::deserialize(&value).map(WsMessage::TradeEvent),
            other => Err(serde::de::Error::custom(format!(
                "unknown websocket message type: {other}"
            ))),
        }
    }
}

trait ValueExt {
    fn from_serialize<T: Serialize>(value: &T) -> Value;
}

impl ValueExt for Value {
    fn from_serialize<T: Serialize>(value: &T) -> Value {
        serde_json::to_value(value).expect("serialization cannot fail for Value")
    }
}
