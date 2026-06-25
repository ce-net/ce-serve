// mesh-bridge.js — install a real mesh node into the page over one WebSocket. (classic script)
//
// ce-serve injects this into every served HTML page, so a frontend becomes a real mesh node with no
// <script> of its own. It backs the shared `window.__ceNode` contract
//   request(method, path, init) -> {status, headers, body}
//   stream(path, signal)        -> async-iterable of decoded payload strings
// by tunnelling each call over a single same-origin WebSocket to `/mesh-bridge`, which ce-serve
// forwards to a real `ce` node. The page therefore reaches the actual mesh — gossipsub, the DHT,
// content-addressed blobs — with no app-tier HTTP backend and without ever holding the node token.
//
// This is the BridgeTransport half of the browser<->mesh transport seam. The other half is a real
// js-libp2p peer that installs the SAME `window.__ceNode` in-process; if that is already present this
// script stands down, so app code never knows which transport is live. The ESM module variant for
// bundlers lives at web/ce-app/client/mesh-bridge.js — keep the two in sync.
(function () {
  "use strict";
  var HEX = "0123456789abcdef";
  function toHex(bytes) {
    var s = "";
    for (var i = 0; i < bytes.length; i++) s += HEX[bytes[i] >> 4] + HEX[bytes[i] & 0xf];
    return s;
  }
  function fromHex(hex) {
    var clean = hex.length % 2 ? "0" + hex : hex;
    var out = new Uint8Array(clean.length / 2);
    for (var i = 0; i < out.length; i++) out[i] = parseInt(clean.substr(i * 2, 2), 16);
    return out;
  }
  function sleep(ms) {
    return new Promise(function (r) {
      setTimeout(r, ms);
    });
  }
  function bridgeUrl() {
    var proto = location.protocol === "https:" ? "wss:" : "ws:";
    return proto + "//" + location.host + "/mesh-bridge";
  }

  function makeConnection() {
    var sock = null;
    var opening = null;
    var nextId = 1;
    var pending = new Map(); // id -> { resolve, reject }
    var streams = new Map(); // id -> { push, end }

    function connect() {
      if (sock && sock.readyState === WebSocket.OPEN) return Promise.resolve(sock);
      if (opening) return opening;
      opening = new Promise(function (resolve, reject) {
        var s;
        try {
          s = new WebSocket(bridgeUrl());
        } catch (e) {
          opening = null;
          reject(e);
          return;
        }
        s.onopen = function () {
          sock = s;
          opening = null;
          resolve(s);
        };
        s.onerror = function () {
          if (sock !== s) {
            opening = null;
            reject(new Error("mesh-bridge connect failed"));
          }
        };
        s.onclose = function () {
          if (sock === s) sock = null;
          pending.forEach(function (p) {
            p.reject(new Error("mesh-bridge closed"));
          });
          pending.clear();
          streams.forEach(function (q) {
            q.end();
          });
        };
        s.onmessage = function (ev) {
          var m;
          try {
            m = JSON.parse(ev.data);
          } catch (_) {
            return;
          }
          var id = m.id;
          if (m.chunk !== undefined) {
            var q = streams.get(id);
            if (q) q.push(m.chunk);
            return;
          }
          if (m.end) {
            var qe = streams.get(id);
            if (qe) qe.end();
            return;
          }
          var p = pending.get(id);
          if (p) {
            pending.delete(id);
            p.resolve(m);
          }
        };
      });
      return opening;
    }

    async function request(method, path, init) {
      var s = await connect();
      var id = nextId++;
      var frame = { id: id, method: method, path: path };
      var body = init && init.body;
      if (body instanceof Uint8Array) frame.body_hex = toHex(body);
      else if (typeof body === "string" && body.length) {
        try {
          frame.body = JSON.parse(body);
        } catch (_) {
          frame.body = body;
        }
      } else if (body && typeof body === "object") {
        frame.body = body;
      }
      var reply = await new Promise(function (resolve, reject) {
        pending.set(id, { resolve: resolve, reject: reject });
        try {
          s.send(JSON.stringify(frame));
        } catch (e) {
          pending.delete(id);
          reject(e);
        }
      });
      var out = { status: reply.status || 200, headers: {} };
      if (reply.body_hex !== undefined) out.body = fromHex(reply.body_hex);
      else out.body = reply.body;
      return out;
    }

    async function* stream(path, signal) {
      while (!(signal && signal.aborted)) {
        var s;
        try {
          s = await connect();
        } catch (_) {
          await sleep(800);
          continue;
        }
        var id = nextId++;
        var buf = [];
        var wake = null;
        var ended = false;
        var q = {
          push: function (chunk) {
            buf.push(chunk);
            if (wake) {
              var w = wake;
              wake = null;
              w();
            }
          },
          end: function () {
            ended = true;
            if (wake) {
              var w2 = wake;
              wake = null;
              w2();
            }
          },
        };
        streams.set(id, q);
        var onAbort = function () {
          try {
            s.send(JSON.stringify({ id: id, cancel: true }));
          } catch (_) {}
          q.end();
        };
        if (signal) signal.addEventListener("abort", onAbort, { once: true });
        try {
          s.send(JSON.stringify({ id: id, method: "GET", path: path }));
        } catch (_) {
          q.end();
        }
        try {
          for (;;) {
            if (buf.length) {
              yield buf.shift();
              continue;
            }
            if (ended || (signal && signal.aborted)) break;
            await new Promise(function (r) {
              wake = r;
            });
          }
        } finally {
          streams.delete(id);
          if (signal) signal.removeEventListener("abort", onAbort);
        }
        if (signal && signal.aborted) return;
        await sleep(500);
      }
    }

    return { request: request, stream: stream };
  }

  function installMeshBridge() {
    try {
      if (globalThis.__ceNode && typeof globalThis.__ceNode.request === "function") {
        return globalThis.__ceNode; // a real node (libp2p peer) already claimed the seam
      }
    } catch (_) {}
    var conn = makeConnection();
    var bridge = { transport: "bridge", request: conn.request, stream: conn.stream };
    try {
      globalThis.__ceNode = bridge;
    } catch (_) {}
    return bridge;
  }

  try {
    globalThis.installMeshBridge = installMeshBridge;
    if (typeof window !== "undefined") installMeshBridge();
  } catch (_) {}
})();
