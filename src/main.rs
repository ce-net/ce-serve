//! ce-serve — the HTTP edge of ce-net. The ONLY public HTTP tier.
//!
//! Its single job is to serve content over HTTP: the Svelte ce-net.com frontend and each app's
//! bundle, plus the `/mesh-bridge` WebSocket that turns a served page into a real mesh node. Every
//! served HTML page gets the bridge installer injected automatically, so a frontend reaches the mesh
//! (gossipsub, DHT, content-addressed blobs) with no app-tier HTTP backend.
//!
//! Everything else is a mesh backend — ce-hub (the registry/tracker), app logic — that ce-serve and
//! the page reach over the mesh, never app-to-app HTTP. ce-serve itself reaches the mesh through the
//! co-located `ce` node (CE_NODE_URL, default http://127.0.0.1:8844); the browser never holds the
//! node token, ce-serve forwards mesh calls on its behalf over `/mesh-bridge`.
//!
//! v0 serves a static bundle from disk (the SvelteKit build output at CE_SERVE_ROOT). NEXT: resolve
//! Host -> appId -> bundle-manifest CID via ce-hub over the mesh, and serve every file from the
//! content-addressed blob store — so ce-net.com is just another app row and adding an app needs no
//! redeploy. The serving path below is structured so that swap touches only `resolve` + `load_file`.

use std::net::SocketAddr;
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;

use axum::http::{header, HeaderMap, StatusCode, Uri};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;

mod mesh_bridge;

#[derive(Clone)]
pub(crate) struct AppState {
    /// Local static root for the default site bundle (the SvelteKit build output).
    site_root: PathBuf,
    /// Single-page-app fallback: unknown routes serve `/index.html` so the client router handles them.
    spa: bool,
}
pub(crate) type Shared = Arc<AppState>;

/// The browser-side transport installer, injected into every served HTML page. Keeping the canonical
/// copy here (the edge owns injection) means a page needs no `<script src>` of its own to get a node.
const BRIDGE_JS: &str = include_str!("../assets/mesh-bridge.js");

/// The ce node loopback base URL — how the edge reaches the mesh.
pub(crate) fn node_url() -> String {
    std::env::var("CE_NODE_URL")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "http://127.0.0.1:8844".to_string())
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "ce_serve=info".into()),
        )
        .init();

    let site_root = PathBuf::from(
        std::env::var("CE_SERVE_ROOT").unwrap_or_else(|_| "site".to_string()),
    );
    let spa = std::env::var("CE_SERVE_SPA").map(|v| v != "0").unwrap_or(true);
    let port: u16 = std::env::var("CE_SERVE_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8790);

    let state: Shared = Arc::new(AppState { site_root, spa });

    let app = Router::new()
        // The transport installer + the bridge socket: the edge's mesh surface for browsers.
        .route("/__ce/mesh-bridge.js", get(serve_bridge_js))
        .route("/mesh-bridge", get(mesh_bridge::mesh_bridge_ws))
        .route("/healthz", get(|| async { "ok" }))
        // Everything else: serve content (Host -> app -> bundle), SPA fallback, bridge injected.
        .fallback(serve_content)
        .layer(tower_http::cors::CorsLayer::permissive())
        .with_state(state.clone());

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr).await.expect("bind");
    tracing::info!(%addr, root = %state.site_root.display(), "ce-serve: HTTP edge listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .expect("serve");
}

async fn serve_bridge_js() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
        BRIDGE_JS,
    )
}

/// Resolve a (host, request-path) to a storage key under the site root, then serve the bytes with the
/// bridge script injected into HTML and SPA fallback for unknown routes.
async fn serve_content(
    axum::extract::State(st): axum::extract::State<Shared>,
    uri: Uri,
    _headers: HeaderMap,
) -> impl IntoResponse {
    // NEXT: map `host` -> appId -> bundle CID via ce-hub over the mesh, and pick the app's root.
    // v0 serves the single configured site bundle.
    let Some(key) = norm_path(uri.path()) else {
        return (StatusCode::BAD_REQUEST, "bad path").into_response();
    };

    match load_file(&st.site_root, &key).await {
        Some((bytes, ct)) => respond_file(bytes, ct),
        None => {
            // SPA fallback: serve index.html so the client router handles unknown routes.
            if st.spa {
                if let Some((bytes, _)) = load_file(&st.site_root, "index.html").await {
                    return respond_file(bytes, "text/html; charset=utf-8".into());
                }
            }
            (StatusCode::NOT_FOUND, "not found").into_response()
        }
    }
}

fn respond_file(bytes: Vec<u8>, ct: String) -> axum::response::Response {
    if ct.starts_with("text/html") {
        return ([(header::CONTENT_TYPE, ct)], inject_bridge(&bytes)).into_response();
    }
    ([(header::CONTENT_TYPE, ct)], bytes).into_response()
}

/// Load a file's bytes + content-type for a normalized key, mapping a directory/empty key to
/// `index.html`. Returns None if the file is absent.
async fn load_file(root: &FsPath, key: &str) -> Option<(Vec<u8>, String)> {
    let mut path = root.join(key);
    if key.is_empty() || path.is_dir() {
        path = path.join("index.html");
    }
    match tokio::fs::read(&path).await {
        Ok(bytes) => {
            let ct = mime_for(path.to_str().unwrap_or("")).to_string();
            Some((bytes, ct))
        }
        Err(_) => None,
    }
}

/// Normalize a request path into a safe storage key: strip the leading slash, reject `..`/absolute
/// segments. Empty (`/`) maps to the app root (later resolved to index.html).
fn norm_path(raw: &str) -> Option<String> {
    let p = raw.trim_start_matches('/');
    if p.is_empty() {
        return Some(String::new());
    }
    let mut parts = Vec::new();
    for seg in p.split('/') {
        if seg.is_empty() || seg == "." || seg == ".." {
            return None;
        }
        parts.push(seg);
    }
    Some(parts.join("/"))
}

/// Insert the bridge installer into an HTML document so the served page becomes a mesh node with no
/// `<script>` of its own. Prefer just before `</head>`, else `</body>`, else append.
fn inject_bridge(html: &[u8]) -> Vec<u8> {
    let tag = "<script src=\"/__ce/mesh-bridge.js\"></script>";
    let s = String::from_utf8_lossy(html);
    if s.contains(tag) {
        return html.to_vec();
    }
    let anchor = s
        .find("</head>")
        .or_else(|| s.find("</body>"));
    match anchor {
        Some(idx) => {
            let mut out = String::with_capacity(s.len() + tag.len());
            out.push_str(&s[..idx]);
            out.push_str(tag);
            out.push_str(&s[idx..]);
            out.into_bytes()
        }
        None => {
            let mut out = s.into_owned();
            out.push_str(tag);
            out.into_bytes()
        }
    }
}

fn mime_for(path: &str) -> &'static str {
    let ext = path.rsplit('.').next().unwrap_or("").to_ascii_lowercase();
    match ext.as_str() {
        "html" | "htm" => "text/html; charset=utf-8",
        "js" | "mjs" => "text/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "json" => "application/json; charset=utf-8",
        "wasm" => "application/wasm",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "webp" => "image/webp",
        "gif" => "image/gif",
        "ico" => "image/x-icon",
        "woff2" => "font/woff2",
        "woff" => "font/woff",
        "txt" | "wgsl" => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}
