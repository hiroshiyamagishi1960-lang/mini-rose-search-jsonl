/* PWA Service Worker — cache versioned
   更新手順：CACHE_VERSION と ASSET_VERSION（クエリ）を同じ値に上げてデプロイするだけ */
const CACHE_VERSION = "v-2025-10-24-01";
const ASSET_VERSION = "2025-10-24-01";
const STATIC_CACHE = `app-static-${CACHE_VERSION}`;

// できるだけ少数の“入口”だけを事前キャッシュ（大物は都度キャッシュ）
const PRECACHE_URLS = [
  `/static/manifest.json?v=${ASSET_VERSION}`,
  `/static/icons/icon-192.png?v=${ASSET_VERSION}`,
  `/static/icons/icon-512.png?v=${ASSET_VERSION}`,
  `/static/icons/maskable-512.png?v=${ASSET_VERSION}`
];

// HTMLは network-first（常に最新を取りにいく）
// 静的アセットは cache-first（高速表示）
self.addEventListener("install", (event) => {
  self.skipWaiting();
  event.waitUntil(
    caches.open(STATIC_CACHE).then((cache) => cache.addAll(PRECACHE_URLS))
  );
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    (async () => {
      const keys = await caches.keys();
      await Promise.all(
        keys
          .filter((k) => k !== STATIC_CACHE)
          .map((k) => caches.delete(k))
      );
      await self.clients.claim();
    })()
  );
});

self.addEventListener("fetch", (event) => {
  const req = event.request;
  const url = new URL(req.url);

  // 1) HTML/ドキュメントは network-first（オフライン時のみキャッシュにフォールバック）
  const isDocument = req.mode === "navigate" || req.destination === "document" || (req.headers.get("accept") || "").includes("text/html");
  if (isDocument) {
    event.respondWith(
      (async () => {
        try {
          // HTMLは基本キャッシュしない（最新優先）
          const fresh = await fetch(req, { cache: "no-store" });
          return fresh;
        } catch (e) {
          // オフライン時のフォールバック（あれば）
          const cache = await caches.open(STATIC_CACHE);
          const cached = await cache.match("/ui");
          return cached || new Response("オフラインです。再接続してください。", { status: 503, headers: { "Content-Type": "text/plain; charset=utf-8" } });
        }
      })()
    );
    return;
  }

  // 2) 静的アセット（アイコン/manifest/js/cssなど）は cache-first
  if (req.method === "GET" && (url.pathname.startsWith("/static/") || url.pathname.startsWith("/favicon"))) {
    event.respondWith(
      (async () => {
        const cache = await caches.open(STATIC_CACHE);
        const hit = await cache.match(req);
        if (hit) return hit;
        try {
          const resp = await fetch(req);
          // 成功時だけ cache.put
          if (resp && resp.status === 200) {
            cache.put(req, resp.clone());
          }
          return resp;
        } catch (e) {
          return new Response("", { status: 504 });
        }
      })()
    );
  }
});
