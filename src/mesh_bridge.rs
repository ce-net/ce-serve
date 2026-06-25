//! mesh_bridge — a dumb, allowlisted browser<->mesh transport over a WebSocket.
//!
//! This is the answer to "how does a frontend talk to the mesh?" without an app-tier HTTP backend.
//! A browser opens a WebSocket to `/mesh-bridge` and speaks the same HTTP-shaped contract the
//! `window.__ceNode` bridge exposes: each frame is one `{id, method, path, body?}` request, answered
//! by one `{id, status, body}` reply (or, for the inbound stream, a series of `{id, chunk}` frames
//! terminated by `{id, end:true}`). ce-serve forwards every call to the co-located `ce` node's
//! loopback API through the authenticated `ce_rs::CeClient`, so the browser reaches the REAL mesh
//! (gossipsub, the DHT, content-addressed blobs) and never holds the node's api token.
//!
//! Two properties make this safe to expose to any browser:
//!   * DUMB — it keeps no KV, no rooms, no app state. It is pure transport.
//!   * ALLOWLISTED — only the mesh surface in [`forward`] is reachable (publish/subscribe, the inbound
//!     stream, blobs, discovery, request/reply, health/status). Money, job, and admin routes on the
//!     node are unreachable through this socket.
//!
//! It is the BridgeTransport half of the transport seam; a real js-libp2p peer is the other half, and
//! both install the same `window.__ceNode` so app code never knows which is underneath.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::IntoResponse;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio::task::AbortHandle;

use crate::Shared;

/// One forwarded reply: either a JSON body or raw bytes (for blob GET).
struct Reply {
    status: u16,
    json: Option<Value>,
    bytes: Option<Vec<u8>>,
}

impl Reply {
    fn ok_empty() -> Self {
        Reply { status: 200, json: Some(json!({})), bytes: None }
    }
    fn json(status: u16, v: Value) -> Self {
        Reply { status, json: Some(v), bytes: None }
    }
    fn err(status: u16, msg: impl std::fmt::Display) -> Self {
        Reply { status, json: Some(json!({ "error": msg.to_string() })), bytes: None }
    }
}

/// Map a fallible `ce_rs` unit call to a 200/502 reply.
fn unit<E: std::fmt::Display>(r: Result<(), E>) -> Reply {
    match r {
        Ok(()) => Reply::ok_empty(),
        Err(e) => Reply::err(502, e),
    }
}

/// A node client authenticated with the co-located node's token (discovered from its data dir).
fn node() -> ce_rs::CeClient {
    ce_rs::CeClient::with_token(crate::node_url(), ce_rs::discover_api_token())
}

pub async fn mesh_bridge_ws(ws: WebSocketUpgrade, State(st): State<Shared>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle(socket, st))
}

async fn handle(socket: WebSocket, _st: Shared) {
    let (mut sink, mut ws_in) = socket.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();

    // Pump replies/chunks to the socket. Stops when the socket dies.
    let pump = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if sink.send(Message::Text(msg)).await.is_err() {
                break;
            }
        }
    });

    // One authenticated node client for the whole connection. The browser never sees the token.
    let ce = Arc::new(node());

    // Active inbound streams, keyed by request id, so a `{id, cancel:true}` can abort one.
    let streams: Arc<Mutex<HashMap<i64, AbortHandle>>> = Arc::new(Mutex::new(HashMap::new()));

    while let Some(Ok(msg)) = ws_in.next().await {
        let text = match msg {
            Message::Text(t) => t,
            Message::Close(_) => break,
            _ => continue,
        };
        let Ok(v) = serde_json::from_str::<Value>(&text) else { continue };
        let id = v.get("id").and_then(Value::as_i64).unwrap_or(0);

        if v.get("cancel").and_then(Value::as_bool).unwrap_or(false) {
            if let Some(h) = streams.lock().unwrap().remove(&id) {
                h.abort();
            }
            continue;
        }

        let method = v
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or("GET")
            .to_ascii_uppercase();
        let path = v.get("path").and_then(Value::as_str).unwrap_or("").to_string();
        let body = v.get("body").cloned();
        let body_hex = v.get("body_hex").and_then(Value::as_str).map(str::to_string);

        // The one long-lived endpoint: the inbound mesh stream. Forward node SSE frames as
        // `{id, chunk}` until the client cancels, the socket dies, or the node stream ends.
        if method == "GET" && path == "/mesh/messages/stream" {
            let txc = tx.clone();
            let task = tokio::spawn(async move {
                // Own a client inside the task so the borrowed stream lives only here.
                let ce = node();
                match ce.messages_stream().await {
                    Ok(s) => {
                        // The SSE stream is !Unpin; pin it on the stack so `.next()` works.
                        let mut s = std::pin::pin!(s);
                        while let Some(item) = s.next().await {
                            let Ok(m) = item else { continue };
                            // Re-emit exactly the shape the node's SSE carries and the browser parses.
                            let chunk = json!({
                                "from": m.from,
                                "topic": m.topic,
                                "payload_hex": m.payload_hex,
                                "received_at": m.received_at,
                                "reply_token": m.reply_token,
                            })
                            .to_string();
                            if txc.send(json!({ "id": id, "chunk": chunk }).to_string()).is_err() {
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        let _ = txc.send(
                            json!({ "id": id, "status": 502, "body": { "error": e.to_string() } })
                                .to_string(),
                        );
                    }
                }
                let _ = txc.send(json!({ "id": id, "end": true }).to_string());
            });
            streams.lock().unwrap().insert(id, task.abort_handle());
            continue;
        }

        // One-shot request: forward to the mesh allowlist, answer with a single frame.
        let ce = ce.clone();
        let txc = tx.clone();
        tokio::spawn(async move {
            let reply = forward(&ce, &method, &path, body.as_ref(), body_hex.as_deref()).await;
            let mut out = serde_json::Map::new();
            out.insert("id".into(), json!(id));
            out.insert("status".into(), json!(reply.status));
            if let Some(bytes) = reply.bytes {
                out.insert("body_hex".into(), json!(hex::encode(bytes)));
            } else {
                out.insert("body".into(), reply.json.unwrap_or(Value::Null));
            }
            let _ = txc.send(Value::Object(out).to_string());
        });
    }

    // Connection gone: abort any live streams and the outbound pump.
    for (_id, h) in streams.lock().unwrap().drain() {
        h.abort();
    }
    pump.abort();
}

/// Forward one HTTP-shaped mesh request to the node via `ce_rs`. Anything not matched here is a 404 —
/// this match IS the security boundary between the open browser socket and the node's full API.
async fn forward(
    ce: &ce_rs::CeClient,
    method: &str,
    path: &str,
    body: Option<&Value>,
    body_hex: Option<&str>,
) -> Reply {
    let s = |k: &str| body.and_then(|o| o.get(k)).and_then(Value::as_str).unwrap_or("");
    let payload = |k: &str| {
        body.and_then(|o| o.get(k))
            .and_then(Value::as_str)
            .and_then(|h| hex::decode(h).ok())
            .unwrap_or_default()
    };

    match (method, path) {
        ("POST", "/mesh/subscribe") => unit(ce.subscribe(s("topic")).await),
        ("POST", "/mesh/publish") => unit(ce.publish(s("topic"), &payload("payload_hex")).await),
        ("POST", "/mesh/send") => {
            unit(ce.send_message(s("to"), s("topic"), &payload("payload_hex")).await)
        }
        ("POST", "/mesh/request") => {
            let timeout = body
                .and_then(|o| o.get("timeout_ms"))
                .and_then(Value::as_u64)
                .unwrap_or(10_000);
            match ce
                .request(s("to"), s("topic"), &payload("payload_hex"), timeout)
                .await
            {
                Ok(b) => Reply::json(200, json!({ "payload_hex": hex::encode(b) })),
                Err(e) => Reply::err(502, e),
            }
        }
        ("POST", "/mesh/reply") => {
            let token = body
                .and_then(|o| o.get("token"))
                .and_then(Value::as_u64)
                .unwrap_or(0);
            unit(ce.reply(token, &payload("payload_hex")).await)
        }
        // Blobs go to the node's content-addressed store (DHT-replicated).
        ("POST" | "PUT", "/blobs") => {
            let bytes = body_hex.and_then(|h| hex::decode(h).ok()).unwrap_or_default();
            match ce.put_blob(bytes).await {
                Ok(hash) => Reply::json(200, json!({ "hash": hash })),
                Err(e) => Reply::err(502, e),
            }
        }
        ("GET", p) if p.starts_with("/blobs/") => {
            let hash = &p["/blobs/".len()..];
            match ce.get_blob(hash).await {
                Ok(b) => Reply { status: 200, json: None, bytes: Some(b) },
                Err(_) => Reply::err(404, "blob not found"),
            }
        }
        ("POST", "/discovery/advertise") => unit(ce.advertise_service(s("service")).await),
        ("GET", p) if p.starts_with("/discovery/find/") => {
            let svc = &p["/discovery/find/".len()..];
            match ce.find_service(svc).await {
                Ok(providers) => Reply::json(200, json!({ "providers": providers })),
                Err(e) => Reply::err(502, e),
            }
        }
        ("GET", "/health") => Reply::json(200, json!({ "ok": ce.health().await.unwrap_or(false) })),
        ("GET", "/status") => match ce.status().await {
            Ok(st) => Reply::json(200, json!({ "node_id": st.node_id, "height": st.height })),
            Err(e) => Reply::err(502, e),
        },
        _ => Reply::err(404, "not on the mesh allowlist"),
    }
}
