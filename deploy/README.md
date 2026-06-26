# Deploying the ce-net web fleet

Everything here is "just an app the mesh runs." There is no bespoke web infrastructure — ce-serve is a
container ce-gke places on a mesh host, and the site + each app are content-addressed bundles tracked
by ce-hub and fetched from the mesh blob store.

## Pieces

| App | What it is | Deployed by |
|---|---|---|
| `ce-serve` | the one HTTP edge: serves bundles + `/mesh-bridge` | `ce-gke apply -f ce-serve/deploy/ce-serve.gke.yaml` |
| `spacegame` | the flagship scale-test backend (healed mesh service) | `ce-gke apply -f spacegame/deploy/*.gke.yaml` |
| `ce-net-site` (Svelte) | the ce-net.com frontend bundle | `ce-serve-publish` (below) |
| `ce-hub` | the registry/tracker (hosts nothing) | `web/deploy/ce-build.sh hub` |

## 1. Build the container images

```
# from the workspace root (build context needs the sibling ce-rs):
docker build -f ce-serve/Dockerfile -t ce-net/ce-serve:latest .
# spacegame is an analogous ce-rs binary
```

## 2. Give the apps a node token (so a container can reach the mesh)

```
ce-gke secret set ce-node-token "$(cat ~/.local/share/ce/api.token)"
```

## 3. Place them on the mesh

```
ce-gke apply -f ce-serve/deploy/ce-serve.gke.yaml
ce-gke apply -f spacegame/deploy/spacegame.gke.yaml   # flagship scale test
ce-gke get        # READY counts; ce-gke heals replicas on host churn
```

## 4. Publish the frontend bundle (and register ce-net.com)

```
cd ce-net-site && npm install && npm run build      # -> build/
# upload every file to the node blob store, build a manifest, register host->bundle in ce-hub:
ce-serve-publish ce-net-site/build ce-net.com ce-net-site
```

After this, ce-serve resolves `Host: ce-net.com` -> bundle CID via ce-hub **over the mesh**, fetches
the files from the content-addressed blob store, injects the `/mesh-bridge` installer into the HTML,
and serves it. Updating the site is another `ce-serve-publish` — no edge redeploy, no nginx edit.

## Public exposure

Cloudflare/nginx terminate TLS and forward to a healthy ce-serve replica (the only thing that needs a
public port). Everything else is mesh-internal.

## Relay shortcut (no docker)

On the relay, ce-serve can also run as a plain native binary beside the node (built by
`web/deploy/ce-build.sh`, pointed at `CE_NODE_URL=http://127.0.0.1:8844`) while the containerized
ce-gke path is the portable, self-healing form.
