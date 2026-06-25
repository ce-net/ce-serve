//! ce-serve-publish — publish a built static bundle as a content-addressed bundle, then register it.
//!
//! Walks DIR, uploads every file to the node's content-addressed blob store (`ce_rs` put_blob),
//! builds a manifest `{ v, spa, files: { "<path>": "<cid>" } }`, uploads the manifest to get the
//! bundle cid, then registers `HOST -> { app_id, cid, spa }` in ce-hub. After that, ce-serve serves
//! HOST from those blobs over the mesh — so ce-net.com (and any app) is just a registry row; updating
//! it is another publish, no edge redeploy.
//!
//! Usage:  ce-serve-publish <dir> <host> <app_id> [--no-spa]
//!   env:  CE_NODE_URL (default http://127.0.0.1:8844), CE_HUB_URL (default http://127.0.0.1:8970)

use std::collections::BTreeMap;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let pos: Vec<&String> = args.iter().filter(|a| !a.starts_with("--")).collect();
    if pos.len() < 3 {
        eprintln!("usage: ce-serve-publish <dir> <host> <app_id> [--no-spa]");
        std::process::exit(2);
    }
    let dir = PathBuf::from(pos[0]);
    let host = pos[1].to_ascii_lowercase();
    let app_id = pos[2].clone();
    let spa = !args.iter().any(|a| a == "--no-spa");

    let node_url = std::env::var("CE_NODE_URL").unwrap_or_else(|_| "http://127.0.0.1:8844".into());
    let hub_url = std::env::var("CE_HUB_URL").unwrap_or_else(|_| "http://127.0.0.1:8970".into());
    let ce = ce_rs::CeClient::with_token(node_url, ce_rs::discover_api_token());

    // Upload every file in the bundle; record path -> content cid.
    let mut files: BTreeMap<String, String> = BTreeMap::new();
    let mut stack = vec![dir.clone()];
    while let Some(d) = stack.pop() {
        for entry in std::fs::read_dir(&d)? {
            let p = entry?.path();
            if p.is_dir() {
                stack.push(p);
                continue;
            }
            let rel = p
                .strip_prefix(&dir)?
                .to_string_lossy()
                .replace('\\', "/");
            let bytes = std::fs::read(&p)?;
            let n = bytes.len();
            let cid = ce
                .put_blob(bytes)
                .await
                .map_err(|e| anyhow::anyhow!("put_blob {rel}: {e}"))?;
            println!("  {rel} -> {cid} ({n} bytes)");
            files.insert(rel, cid);
        }
    }
    if files.is_empty() {
        anyhow::bail!("no files found under {}", dir.display());
    }

    let manifest = serde_json::json!({ "v": 1, "spa": spa, "files": files });
    let cid = ce
        .put_blob(serde_json::to_vec(&manifest)?)
        .await
        .map_err(|e| anyhow::anyhow!("put manifest: {e}"))?;
    println!("manifest -> {cid} ({} files, spa={spa})", files.len());

    // Register HOST -> bundle in ce-hub (admin op over localhost HTTP).
    let http = reqwest::Client::new();
    let resp = http
        .put(format!("{hub_url}/bundles/{host}"))
        .json(&serde_json::json!({ "app_id": app_id, "cid": cid, "spa": spa }))
        .send()
        .await?;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        anyhow::bail!("ce-hub register failed {status}: {body}");
    }
    println!("registered {host} -> app {app_id}, bundle {cid}");
    println!("ce-serve now serves https://{host}/ from this content-addressed bundle.");
    Ok(())
}
