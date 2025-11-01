/* PWA Service Worker — version-consistent / safe auto-update
   使い方：ui.html 側の VER と同じ値をこの ?v= に付けて登録してください。
   例: navigator.serviceWorker.register("/static/service-worker.js?v=2025-11-01")
*/

const VER = new URL(self.location).searchParams.get("v") || "2025-11-01";
const CACHE_VERSION = `v-${VER}`;
const ASSET_VERSION = VER;  // UI側の ?v= と必ず同じ値
const STATIC_CACHE  = `app-static-${CACHE_VERSION}`;

// 最小限の静的資産だけ事前キャッシュ（manifest / icons）
const PRECACHE_URLS = [
  `/static/manifest.json?v=${ASSET_VERSION}`,
  `/static/icons/icon-192.png?v=${ASSET_VERSION}`,
  `/static/icons/icon-512.png?v=${ASSET_VERSION}`,
  `/static/icons/maskable-512.png?v=${ASSET_VERSION}`
];

self.addEventListener("install", (event) => {
  // 旧版が動作中でも即切替
  self.skipWaiting();
  event.waitUntil(
    caches.open(STATIC_CACHE).then((cache) => cache.addAll(PRECACHE_URLS))
  );
});

self.addEventListener("activate", (event) => {
  event.waitUntil((async () => {
    // 自分の STATIC_CACHE 以外は完全削除（混在を防止）
    const keys = await caches.keys();
    await Promise.all(keys.filter(k => k !== STATIC_CACHE).map(k => caches.delete(k)));
    await self.clients.claim();
  })());
});

self.addEventListener("fetch", (event) => {
  const req = event.request;
  const url = new URL(req.url);

  // 1) HTML/ドキュメントは常に最新（network-first, no-store）
  const isDocument =
    req.mode === "navigate" ||
    req.destination === "document" ||
    (req.headers.get("accept") || "").includes("text/html");
  if (isDocument) {
    event.respondWith((async () => {
      try {
        return await fetch(req, { cache: "no-store" });
      } catch (e) {
        // オフライン時フォールバック（任意: /ui を落としていれば返す）
        const cache = await caches.open(STATIC_CACHE);
        const cached = await cache.match("/ui");
        return cached || new Response("オフラインです。再接続してください。", {
          status: 503,
          headers: { "Content-Type": "text/plain; charset=utf-8" }
        });
      }
    })());
    return;
  }

  // 2) /api/* は常にネット（キャッシュしない）
  if (url.pathname.startsWith("/api/")) {
    event.respondWith(fetch(req, { cache: "no-store" }).catch(() => fetch(req)));
    return;
  }

  // 3) 静的資産（/static/* や /favicon*）は cache-first だが、
  //    「現在の版（ASSET_VERSION）と異なる ?v= を持つリクエスト」はキャッシュを使わない
  const isStatic = req.method === "GET" &&
                   (url.pathname.startsWith("/static/") || url.pathname.startsWith("/favicon"));
  if (isStatic) {
    const reqVer = url.searchParams.get("v");
    const versionMatches = (reqVer === null) || (reqVer === ASSET_VERSION);

    event.respondWith((async () => {
      const cache = await caches.open(STATIC_CACHE);

      // 別版の静的資産は必ずネットから取り直す（混在防止）
      if (!versionMatches) {
        try {
          const fresh = await fetch(req, { cache: "no-store" });
          return fresh;
        } catch (e) {
          // 旧版をあえて返さずエラーにする方が、誤表示を防げる（必要なら 504）
          return new Response("", { status: 504 });
        }
      }

      // 同版なら cache-first（ヒットすれば高速、なければ取得して保存）
      const hit = await cache.match(req);
      if (hit) return hit;

      try {
        const resp = await fetch(req, { cache: "no-store" });
        if (resp && resp.ok) cache.put(req, resp.clone());
        return resp;
      } catch (e) {
        return new Response("", { status: 504 });
      }
    })());
    return;
  }

  // 4) それ以外は素直にネットへ（保守的挙動）
  event.respondWith(fetch(req, { cache: "no-store" }).catch(() => fetch(req)));
});
