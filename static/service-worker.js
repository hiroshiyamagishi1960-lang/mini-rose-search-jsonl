/* Service Worker — UI無改変で
   1) つねに最新を取得（HTML/JS/CSS/manifest/icon/APIすべて no-store）
   2) /health と /version を「戻る」付きのHTMLで見せる（PWAでも迷子にならない）
   3) 既存キャッシュは全面削除して混在を根絶
*/

self.addEventListener("install", (event) => {
  // 旧版が動作中でも即このSWに切り替え
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil((async () => {
    // 既存キャッシュは名前不問で全削除（混在・取り違えを根絶）
    const keys = await caches.keys();
    await Promise.all(keys.map((k) => caches.delete(k)));
    // 以後このSWを即有効化
    await self.clients.claim();
  })());
});

// ---- ユーティリティ：簡易テンプレートHTML ----
function wrapSimplePage(title, bodyHtml) {
  return new Response(
`<!doctype html>
<html lang="ja"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>${title}</title>
<style>
  body{font-family: system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial; line-height:1.6; margin:0; background:#fafafa; color:#0f172a}
  .wrap{max-width:900px; margin:16px auto; padding:0 16px}
  h1{font-size:20px; margin:16px 0}
  .card{background:#fff; border:1px solid #e5e7eb; border-radius:12px; padding:12px}
  pre{white-space:pre-wrap; word-break:break-word; background:#f8fafc; border:1px solid #e5e7eb; border-radius:8px; padding:10px; margin:8px 0}
  a.btn{display:inline-block; padding:10px 14px; border:1px solid #e5e7eb; border-radius:10px; text-decoration:none; color:#2563eb; background:#fff}
</style>
</head>
<body>
  <div class="wrap">
    <h1>${title}</h1>
    <div class="card">${bodyHtml}</div>
  </div>
</body></html>`,
    { status: 200, headers: { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store" } }
  );
}

// ---- /health と /version をラップして「戻る」を提供 ----
async function handleHealthOrVersion(req, pathname) {
  try {
    // 常に最新取得（no-store）
    const fresh = await fetch(req, { cache: "no-store" });
    // そのままJSON/テキストを読み出し
    const text = await fresh.text();
    // JSONらしければ整形
    let pretty = text;
    try { pretty = JSON.stringify(JSON.parse(text), null, 2); } catch (_) {}
    const title = pathname === "/health" ? "Health" : "Version";
    const html = `
      <p>下はサーバーの最新応答です。</p>
      <pre>${escapeHtml(pretty)}</pre>
      <p><a class="btn" href="/ui">← 検索画面（/ui）に戻る</a></p>
    `;
    return wrapSimplePage(`Mini Rose — ${title}`, html);
  } catch (e) {
    const msg = (e && e.message) ? e.message : String(e);
    return wrapSimplePage("表示エラー", `
      <p>読み込みに失敗しました。</p>
      <pre>${escapeHtml(msg)}</pre>
      <p><a class="btn" href="/ui">← 検索画面（/ui）に戻る</a></p>
    `);
  }
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}

self.addEventListener("fetch", (event) => {
  const req = event.request;
  const url = new URL(req.url);

  // 1) /health /version を開いたら、PWAでも「戻る」付きのHTMLを返す（UI側の改修不要）
  const isDoc = req.mode === "navigate" || req.destination === "document" ||
                (req.headers.get("accept") || "").includes("text/html");
  if (isDoc && (url.pathname === "/health" || url.pathname === "/version")) {
    event.respondWith(handleHealthOrVersion(req, url.pathname));
    return;
  }

  // 2) /api/* は常に最新（no-store）— 検索順位の最新性を保証
  if (url.pathname.startsWith("/api/")) {
    event.respondWith(fetch(req, { cache: "no-store" }).catch(() => fetch(req)));
    return;
  }

  // 3) ドキュメント（/ui など）も常に最新（no-store）— 古いUIを残さない
  if (isDoc) {
    event.respondWith(fetch(req, { cache: "no-store" }).catch(() => fetch(req)));
    return;
  }

  // 4) 静的資産（/static/*, /favicon*）も常に最新（no-store）
  if (req.method === "GET" && (url.pathname.startsWith("/static/") || url.pathname.startsWith("/favicon"))) {
    event.respondWith(fetch(req, { cache: "no-store" }).catch(() => fetch(req)));
    return;
  }

  // 5) それ以外も no-store（安全側）
  event.respondWith(fetch(req, { cache: "no-store" }).catch(() => fetch(req)));
});
