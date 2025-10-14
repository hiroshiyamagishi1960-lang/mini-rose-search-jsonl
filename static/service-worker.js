// 🌹 Mini Rose PWA - Service Worker（自動更新 + manifest UTF-8）
// 2025-10-06

// ★キャッシュ名を更新（これを変えるだけでも旧キャッシュは確実に破棄されます）
const CACHE_VERSION = "rose-20251006";
const STATIC_CACHE = `static-${CACHE_VERSION}`;

// 事前キャッシュ（HTMLは含めない！自動更新のため）
const PRECACHE = [
  "/static/icon-192.png",
  "/static/icon-512.png"
  // ※ manifest.json は毎回ネットから取得（プリキャッシュしない）
];

self.addEventListener("install", (event) => {
  self.skipWaiting();
  event.waitUntil(
    caches.open(STATIC_CACHE).then((cache) => cache.addAll(PRECACHE)).catch(()=>{})
  );
});

self.addEventListener("activate", (event) => {
  event.waitUntil((async () => {
    const keys = await caches.keys();
    await Promise.all(keys.filter(k => k !== STATIC_CACHE).map(k => caches.delete(k)));
    await self.clients.claim();
  })());
});

// ---- fetch 戦略 ----
self.addEventListener("fetch", (event) => {
  const req = event.request;
  const url = new URL(req.url);

  // 1) manifest.json は毎回ネットから取得し、UTF-8 JSON に矯正（既存仕様を維持）
  if (url.pathname === "/static/manifest.json") {
    event.respondWith(handleManifestUTF8(req));
    return;
  }

  // 2) HTML（ナビゲーション）は Network-First（常に最新を取りにいく）
  const acceptsHTML = (req.headers.get("accept") || "").includes("text/html");
  const isNavigate = req.mode === "navigate";
  if (isNavigate || acceptsHTML) {
    event.respondWith(networkFirstHTML(req));
    return;
  }

  // 3) 同一オリジンの静的資産は Cache-First + 背景更新（stale-while-revalidate）
  const isStatic =
    url.origin === self.location.origin &&
    (url.pathname.startsWith("/static/") ||
      /\.(?:js|css|png|jpe?g|svg|ico|webmanifest|json|woff2?)$/i.test(url.pathname));
  if (isStatic) {
    event.respondWith(cacheFirstRevalidate(req));
    return;
  }

  // 4) それ以外（API等）は素通し
});

// ---- helpers ----
async function handleManifestUTF8(request) {
  try {
    const resp = await fetch(request, { cache: "no-store" });
    const buf = await resp.arrayBuffer();
    const headers = new Headers(resp.headers);
    headers.set("Content-Type", "application/json; charset=utf-8");
    return new Response(buf, { status: resp.status, statusText: resp.statusText, headers });
  } catch (e) {
    const cached = await caches.match(request);
    if (cached) return cached;
    throw e;
  }
}

async function networkFirstHTML(request) {
  try {
    // HTMLは常に最新を取りにいく
    const fresh = await fetch(request, { cache: "no-store" });
    return fresh;
  } catch (e) {
    // オフライン時のみキャッシュにフォールバック
    const cache = await caches.open(STATIC_CACHE);
    const cached = await cache.match(request);
    // 適切なフォールバックが無ければ簡易レスポンス
    return cached || new Response("オフラインです。再接続後にもう一度お試しください。", { status: 503 });
  }
}

async function cacheFirstRevalidate(request) {
  const cache = await caches.open(STATIC_CACHE);
  const cached = await cache.match(request);
  const fetching = fetch(request).then(res => {
    if (res && res.status === 200 && request.method === "GET") {
      cache.put(request, res.clone());
    }
    return res;
  }).catch(() => null);
  return cached || (await fetching) || new Response("", { status: 504 });
}
