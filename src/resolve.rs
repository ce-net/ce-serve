//! resolve — turn a Host into content-addressed files, over the mesh.
//!
//! The edge resolves `Host -> {app_id, cid, spa}` by asking ce-hub OVER THE MESH (request/reply on
//! `ce-hub/resolve/1`, ce-hub found via `find_service("ce-hub")`) — never app-to-app HTTP. The `cid`
//! is a bundle manifest blob `{ spa, files: { "<path>": "<file-cid>" } }`; each file is itself a blob.
//! ce-serve fetches both from the node's content-addressed store (`ce_rs` get_blob). Resolutions are
//! cached briefly (a bundle can be repointed); manifests and file blobs are immutable so cached hard.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde::Deserialize;
use serde_json::json;

const RESOLVE_TOPIC: &str = "ce-hub/resolve/1";
const HUB_SERVICE: &str = "ce-hub";
const RESOLVE_TTL: Duration = Duration::from_secs(30);
const REQUEST_TIMEOUT_MS: u64 = 8_000;
/// Crude bound on the immutable blob cache: clear it when it grows past this many bytes.
const BLOB_CACHE_MAX_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Deserialize)]
pub struct BundleRef {
    pub app_id: String,
    pub cid: String,
    #[serde(default)]
    pub spa: bool,
}

#[derive(Clone, Deserialize)]
pub struct Manifest {
    #[serde(default)]
    pub spa: bool,
    #[serde(default)]
    pub files: HashMap<String, String>,
}

pub struct Resolver {
    node_url: String,
    hub_node: Mutex<Option<String>>,
    host_cache: Mutex<HashMap<String, (Instant, Option<BundleRef>)>>,
    manifest_cache: Mutex<HashMap<String, Manifest>>,
    blob_cache: Mutex<(HashMap<String, Vec<u8>>, usize)>,
}

impl Resolver {
    pub fn new(node_url: String) -> Self {
        Resolver {
            node_url,
            hub_node: Mutex::new(None),
            host_cache: Mutex::new(HashMap::new()),
            manifest_cache: Mutex::new(HashMap::new()),
            blob_cache: Mutex::new((HashMap::new(), 0)),
        }
    }

    fn ce(&self) -> ce_rs::CeClient {
        ce_rs::CeClient::with_token(self.node_url.clone(), ce_rs::discover_api_token())
    }

    /// The ce-hub node id, found via the DHT and cached. Re-resolves if the cached one is empty.
    async fn hub_node(&self, ce: &ce_rs::CeClient) -> Option<String> {
        if let Some(n) = self.hub_node.lock().unwrap().clone() {
            return Some(n);
        }
        let providers = ce.find_service(HUB_SERVICE).await.ok()?;
        let node = providers.into_iter().next()?;
        *self.hub_node.lock().unwrap() = Some(node.clone());
        Some(node)
    }

    /// Resolve a host to its bundle pointer, cached for `RESOLVE_TTL` (negative results too).
    pub async fn resolve_host(&self, host: &str) -> Option<BundleRef> {
        if let Some((at, cached)) = self.host_cache.lock().unwrap().get(host).cloned() {
            if at.elapsed() < RESOLVE_TTL {
                return cached;
            }
        }
        let resolved = self.resolve_host_uncached(host).await;
        self.host_cache
            .lock()
            .unwrap()
            .insert(host.to_string(), (Instant::now(), resolved.clone()));
        resolved
    }

    async fn resolve_host_uncached(&self, host: &str) -> Option<BundleRef> {
        let ce = self.ce();
        let hub = self.hub_node(&ce).await?;
        let payload = json!({ "host": host }).to_string().into_bytes();
        let reply = match ce.request(&hub, RESOLVE_TOPIC, &payload, REQUEST_TIMEOUT_MS).await {
            Ok(b) => b,
            Err(_) => {
                // The cached hub node may be stale; drop it so the next call re-finds.
                *self.hub_node.lock().unwrap() = None;
                return None;
            }
        };
        let bref: BundleRef = serde_json::from_slice(&reply).ok()?;
        if bref.app_id.is_empty() || bref.cid.is_empty() {
            return None;
        }
        Some(bref)
    }

    /// The bundle manifest for a cid (immutable -> cached forever).
    pub async fn manifest(&self, cid: &str) -> Option<Manifest> {
        if let Some(m) = self.manifest_cache.lock().unwrap().get(cid).cloned() {
            return Some(m);
        }
        let bytes = self.blob(cid).await?;
        let m: Manifest = serde_json::from_slice(&bytes).ok()?;
        self.manifest_cache.lock().unwrap().insert(cid.to_string(), m.clone());
        Some(m)
    }

    /// A file blob by cid (immutable -> cached, with a crude byte bound).
    pub async fn blob(&self, cid: &str) -> Option<Vec<u8>> {
        if let Some(b) = self.blob_cache.lock().unwrap().0.get(cid).cloned() {
            return Some(b);
        }
        let bytes = self.ce().get_blob(cid).await.ok()?;
        {
            let mut guard = self.blob_cache.lock().unwrap();
            if guard.1 + bytes.len() > BLOB_CACHE_MAX_BYTES {
                guard.0.clear();
                guard.1 = 0;
            }
            guard.1 += bytes.len();
            guard.0.insert(cid.to_string(), bytes.clone());
        }
        Some(bytes)
    }
}
