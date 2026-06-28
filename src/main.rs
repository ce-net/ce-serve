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
mod resolve;

#[derive(Clone)]
pub(crate) struct AppState {
    /// Resolves Host -> content-addressed bundle over the mesh (the production serving path).
    resolver: Arc<resolve::Resolver>,
    /// Local static root for the default site bundle — dev fallback when no bundle is registered.
    site_root: PathBuf,
    /// Single-page-app fallback: unknown routes serve the fallback shell so the client router runs.
    spa: bool,
    /// Default host to resolve when a request omits Host (e.g. health probes); usually "ce-net.com".
    default_host: String,
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

    let default_host = std::env::var("CE_SERVE_DEFAULT_HOST").unwrap_or_default();
    let state: Shared = Arc::new(AppState {
        resolver: Arc::new(resolve::Resolver::new(node_url())),
        site_root,
        spa,
        default_host,
    });

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
    headers: HeaderMap,
) -> impl IntoResponse {
    let Some(key) = norm_path(uri.path()) else {
        return (StatusCode::BAD_REQUEST, "bad path").into_response();
    };

    // Conditional GET: the browser sends back the content-id ETag we gave it; if it still matches, 304.
    let inm = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim().trim_matches('"').to_string());

    // Production path: resolve Host -> content-addressed bundle over the mesh and serve from blobs.
    let host = req_host(&headers, &st.default_host);
    if !host.is_empty() {
        if let Some(bref) = st.resolver.resolve_host(&host).await {
            return serve_from_bundle(&st, &bref, &key, inm.as_deref())
                .await
                .unwrap_or_else(|| (StatusCode::NOT_FOUND, "not found").into_response());
        }
    }

    // Dev fallback: serve the local bundle dir (CE_SERVE_ROOT) when no bundle is registered.
    serve_from_dir(&st, &key).await
}

/// The request Host, lowercased and without port; falls back to the configured default host.
fn req_host(headers: &HeaderMap, default: &str) -> String {
    let h = headers
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .map(|h| h.split(':').next().unwrap_or(h).trim().to_ascii_lowercase())
        .unwrap_or_default();
    if h.is_empty() {
        default.to_ascii_lowercase()
    } else {
        h
    }
}

/// Serve one path from a resolved content-addressed bundle: look up the file cid in the manifest
/// (SPA fallback to 200.html/index.html), fetch the blob, inject the bridge into HTML.
async fn serve_from_bundle(st: &Shared, bref: &resolve::BundleRef, key: &str, inm: Option<&str>) -> Option<axum::response::Response> {
    let manifest = st.resolver.manifest(&bref.cid).await?;
    let want = if key.is_empty() { "index.html" } else { key };
    let (file_cid, ct) = match manifest.files.get(want) {
        Some(cid) => (cid.clone(), mime_for(want).to_string()),
        None => {
            // Prerendered route mapping: /apps -> apps.html, /foo -> foo/index.html.
            let html_key = format!("{want}.html");
            let idx_key = format!("{want}/index.html");
            if let Some(cid) = manifest.files.get(&html_key).or_else(|| manifest.files.get(&idx_key)) {
                (cid.clone(), "text/html; charset=utf-8".to_string())
            } else if bref.spa || manifest.spa {
                // SPA fallback shell (client router handles the route).
                let cid = manifest
                    .files
                    .get("200.html")
                    .or_else(|| manifest.files.get("index.html"))?
                    .clone();
                (cid, "text/html; charset=utf-8".to_string())
            } else {
                return None;
            }
        }
    };
    // The file's content id IS its strong validator. If the browser already holds this exact content,
    // answer 304 — no re-download — even though the filename (e.g. pkg/app_bg.wasm) is stable across deploys.
    if inm == Some(file_cid.as_str()) && !ct.starts_with("text/html") {
        return Some((StatusCode::NOT_MODIFIED, [(header::ETAG, format!("\"{file_cid}\""))]).into_response());
    }
    let bytes = st.resolver.blob(&file_cid).await?;
    Some(respond_file(bytes, ct, Some(&file_cid), is_immutable_asset(want)))
}

/// Dev fallback: serve from the local bundle dir with SPA fallback.
async fn serve_from_dir(st: &Shared, key: &str) -> axum::response::Response {
    match load_file(&st.site_root, key).await {
        Some((bytes, ct)) => respond_file(bytes, ct, None, is_immutable_asset(key)),
        None => {
            if st.spa {
                for name in ["200.html", "index.html"] {
                    if let Some((bytes, _)) = load_file(&st.site_root, name).await {
                        return respond_file(bytes, "text/html; charset=utf-8".into(), None, false);
                    }
                }
            }
            (StatusCode::NOT_FOUND, "not found").into_response()
        }
    }
}

/// Whether a path is a CONTENT-HASHED bundle asset (the hash is in the filename, e.g. vite's
/// `index-D_NtxU66.js`, `app-DCKp3R-I.wasm`). The URL changes whenever the bytes do, so it can be
/// cached forever (`immutable`) — no revalidation round-trip. Stable-named bundles (spacegame's
/// `boot.js`, `pkg/app_bg.wasm`) do NOT match and keep revalidating via ETag, which is correct for them.
fn is_immutable_asset(path: &str) -> bool {
    let name = path.rsplit('/').next().unwrap_or(path);
    let Some(dot) = name.rfind('.') else { return false };
    let (stem, ext) = (&name[..dot], &name[dot + 1..]);
    if !matches!(
        ext,
        "js" | "css" | "wasm" | "woff" | "woff2" | "ttf" | "otf" | "png" | "jpg" | "jpeg" | "gif"
            | "svg" | "webp" | "avif" | "ico" | "map"
    ) {
        return false;
    }
    // A trailing `-<hash>` segment of >=8 url-safe chars marks a content hash.
    match stem.rfind('-') {
        Some(dash) => {
            let hash = &stem[dash + 1..];
            hash.len() >= 8 && hash.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        }
        None => false,
    }
}

fn respond_file(bytes: Vec<u8>, ct: String, etag: Option<&str>, immutable: bool) -> axum::response::Response {
    // Bundle files have STABLE names (index.html, pkg/app_bg.wasm, …) but content that changes every
    // deploy, so a long browser/edge cache serves stale bytes — and a wasm vs JS-glue VERSION MISMATCH
    // ("import requires a callable"). HTML is never cached (it points at the live bundle). Other files
    // carry their content id as a strong ETag with `no-cache`, so the browser and the CDN revalidate and
    // get a cheap 304 when unchanged or the new bytes the moment the content (hence the id) changes.
    // EXCEPTION: content-hashed assets (the hash is in the URL) are immutable -> cache for a year, no
    // revalidation at all. That is the fast path for vite-style bundles (cast).
    if ct.starts_with("text/html") {
        return (
            [
                (header::CONTENT_TYPE, ct),
                (header::CACHE_CONTROL, "no-cache, must-revalidate".to_string()),
            ],
            inject_bridge(&bytes),
        )
            .into_response();
    }
    let cc = if immutable { "public, max-age=31536000, immutable" } else { "no-cache" };
    match etag {
        Some(e) => (
            [
                (header::CONTENT_TYPE, ct),
                (header::CACHE_CONTROL, cc.to_string()),
                (header::ETAG, format!("\"{e}\"")),
            ],
            bytes,
        )
            .into_response(),
        None => (
            [(header::CONTENT_TYPE, ct), (header::CACHE_CONTROL, cc.to_string())],
            bytes,
        )
            .into_response(),
    }
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
