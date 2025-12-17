use polymarket_client_sdk::ws::model::{
    Book, LastTradePrice, OrderEvent, PriceChange, PriceLevel, TickSizeChange, TradeEvent,
    WsMessage,
};
use serde_json::Value;

fn assert_roundtrip(json: &str, expected: WsMessage) {
    let parsed: WsMessage = serde_json::from_str(json).expect("failed to parse message");
    assert_eq!(parsed, expected);

    let value: Value = serde_json::from_str(json).expect("expected valid json");
    let serialized = serde_json::to_value(parsed).expect("re-serialization failed");
    assert_eq!(serialized, value);
}

#[test]
fn book_round_trip() {
    let json = r#"{
        "event_type": "book",
        "market": "0xcafe-market",
        "asset_id": "token-1",
        "bids": [["0.45", "120.5"], ["0.44", "30.0"]],
        "asks": [["0.55", "200.0"]],
        "timestamp": "1718119912"
    }"#;

    let expected = WsMessage::Book(Book {
        market: "0xcafe-market".into(),
        asset_id: "token-1".into(),
        bids: vec![
            PriceLevel("0.45".into(), "120.5".into()),
            PriceLevel("0.44".into(), "30.0".into()),
        ],
        asks: vec![PriceLevel("0.55".into(), "200.0".into())],
        timestamp: "1718119912".into(),
    });

    assert_roundtrip(json, expected);
}

#[test]
fn price_change_round_trip() {
    let json = r#"{
        "event_type": "price_change",
        "market": "0xcafe-market",
        "asset_id": "token-1",
        "best_bid": "0.45",
        "best_offer": "0.55"
    }"#;

    let expected = WsMessage::PriceChange(PriceChange {
        market: "0xcafe-market".into(),
        asset_id: "token-1".into(),
        best_bid: "0.45".into(),
        best_offer: "0.55".into(),
    });

    assert_roundtrip(json, expected);
}

#[test]
fn tick_size_change_round_trip() {
    let json = r#"{
        "event_type": "tick_size_change",
        "market": "0xcafe-market",
        "minimum_tick_size": "0.01"
    }"#;

    let expected = WsMessage::TickSizeChange(TickSizeChange {
        market: "0xcafe-market".into(),
        minimum_tick_size: "0.01".into(),
    });

    assert_roundtrip(json, expected);
}

#[test]
fn last_trade_price_round_trip() {
    let json = r#"{
        "type": "last_trade_price",
        "market": "0xcafe-market",
        "asset_id": "token-1",
        "price": "0.50",
        "side": "buy",
        "timestamp": "1718119912"
    }"#;

    let expected = WsMessage::LastTradePrice(LastTradePrice {
        market: "0xcafe-market".into(),
        asset_id: "token-1".into(),
        price: "0.50".into(),
        side: "buy".into(),
        timestamp: "1718119912".into(),
    });

    assert_roundtrip(json, expected);
}

#[test]
fn order_event_round_trip() {
    let json = r#"{
        "event_type": "order_event",
        "order_id": "order-123",
        "market": "0xcafe-market",
        "asset_id": "token-1",
        "price": "0.45",
        "size": "150",
        "remaining": "50",
        "side": "buy",
        "status": "OPEN",
        "client_order_id": "client-789"
    }"#;

    let expected = WsMessage::OrderEvent(OrderEvent {
        order_id: "order-123".into(),
        market: "0xcafe-market".into(),
        asset_id: "token-1".into(),
        price: "0.45".into(),
        size: "150".into(),
        remaining: "50".into(),
        side: "buy".into(),
        status: "OPEN".into(),
        client_order_id: Some("client-789".into()),
    });

    assert_roundtrip(json, expected);
}

#[test]
fn trade_event_round_trip() {
    let json = r#"{
        "event_type": "trade_event",
        "trade_id": "trade-456",
        "order_id": "order-123",
        "market": "0xcafe-market",
        "asset_id": "token-1",
        "price": "0.47",
        "size": "25",
        "side": "sell",
        "taker": "0xfeed",
        "maker": "0xdead",
        "timestamp": "1718119913"
    }"#;

    let expected = WsMessage::TradeEvent(TradeEvent {
        trade_id: "trade-456".into(),
        order_id: "order-123".into(),
        market: "0xcafe-market".into(),
        asset_id: "token-1".into(),
        price: "0.47".into(),
        size: "25".into(),
        side: "sell".into(),
        taker: "0xfeed".into(),
        maker: "0xdead".into(),
        timestamp: "1718119913".into(),
    });

    assert_roundtrip(json, expected);
}
