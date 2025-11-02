/* Service Worker — UI無改変で要件を満たす版
   目的：
   1) 端末ごとの“古いUIが残る”を根絶：ドキュメントは cache:"reload"、その他は cache:"no-store"。保険フェッチ禁止。
   2) /health・/version をラップして、PWAでも必ず「戻る」導線を表示（戻り先は /ui があれば /ui、無ければ /）。
   3) 既存キャッシュは全面削除（混在根絶）。Navigation Preload 有効化で“常にネットへ”を安定化。
*/

self.addEventListener("install", (event) => {
  // 旧SWが動作中でも即このSWへ切替
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil((async () => {
    // 1) 既存キャッシュをスコープ内で全削除（混在根絶）
    try {
      const keys = await caches.keys();
      await Promise.all(keys.map((k) => caches.delete(k)));
    } catch (_) {}
    // 2) Navigation Preload を可能なら有効化（SW起動前にネット取得開始）
    try {
      if (self.registration.navigationPreload) {
        await self.registration.navigationPreload.enable();
      }
    } catch (_) {}
    // 3) 即時制御
    await self.clients.claim();
  })());
});

// ---------------- ユーティリティ ----------------

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}

function wrapSimplePage(title, bodyHtml) {
  return new Response(
`<!doctype html>
<html lang="ja"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>${title}</title>
<meta name="robots" content="noindex">
<style>
  :root{--bg:#fafafa;--card:#fff;--b:#e5e7eb;--txt:#0f172a;--link:#2563eb;--muted:#64748b}
  html,body{background:var(--bg);color:var(--txt);margin:0;font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial}
  .wrap{max-width:900px;margin:16px auto;padding:0 16px}
  h1{font-size:20px;margin:16px 0}
  .card{background:var(--card);border:1px solid var(--b);border-radius:12px;padding:12px}
  pre{white-space:pre-wrap;word-break:break-word;background:#f8fafc;border:1px solid var(--b);border-radius:8px;padding:10px;margin:8px 0}
  .row{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
  a.btn{display:inline-block;padding:10px 14px;border:1px solid var(--b);border-radius:10px;text-decoration:none;color:var(--link);background:var(--card)}
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

// 戻り先を動的に決定：/ui が 200 なら /ui、そうでなければ /
async function decideReturnHref(origin) {
  try {
    const res = await fetch(new Request(origin + "/ui", { method: "HEAD", cache: "no-store", credentials: "same-origin" }));
    if (res && res.ok) return "/ui";
  } catch (_) {}
  return "/";
}

// /health /version 用のラッパー（ナビゲーション時のみ）
async function handleHealthOrVersion(req, pathname, origin) {
  const title = pathname === "/health" ? "Health" : "Version";
  const start = Date.now();
  let text = "";
  let status = 0;
  try {
    // 常に最新（ブラウザHTTPキャッシュ不使用）
    const fresh = await fetch(new Request(req, { cache: "no-store", credentials: "same-origin" }));
    status = fresh.status;
    text = await fresh.text();
  } catch (e) {
    text = (e && e.message) ? e.message : String(e);
  }
  // JSONなら整形（大きすぎる時は先頭のみ）
  let pretty = text;
  try {
    const parsed = JSON.parse(text);
    pretty = JSON.stringify(parsed, null, 2);
  } catch (_) {
    if (pretty.length > 20000) {
      pretty = pretty.slice(0, 20000) + "\n...[truncated]...";
    }
  }
  const ms = Date.now() - start;
  const backHref = await decideReturnHref(origin);
  const body = `
    <div class="row" style="font-size:13px;color:var(--muted)">
      <span>status=${escapeHtml(String(status))}</span>
      <span>time=${escapeHtml(String(ms))}ms</span>
      <span>path=${escapeHtml(pathname)}</span>
    </div>
    <p>下はサーバーの最新応答です（常にネットから取得）。</p>
    <pre>${escapeHtml(pretty)}</pre>
    <p><a class="btn" href="${backHref}">← 検索画面に戻る</a></p>
  `;
  return wrapSimplePage(`Mini Rose — ${title}`, body);
}

// ネットワーク専用フェッチ（保険フェッチ禁止）
async function networkOnly(req, { isDocument = false } = {}) {
  try {
    return await fetch(req);
  } catch (e) {
    // ドキュメント要求の場合のみ、人に読めるエラーページを返す
    if (isDocument) {
      const msg = (e && e.message) ? e.message : String(e);
      return wrapSimplePage("ネットワークエラー", `
        <p>最新の画面を取得できませんでした（キャッシュからは供給しません）。</p>
        <pre>${escapeHtml(msg)}</pre>
      `);
    }
    // それ以外は素直に失敗を伝える
    return new Response("Network error (no fallback cache).", { status: 503, headers: { "Cache-Control": "no-store" } });
  }
}

// ---------------- フェッチポリシー ----------------

self.addEventListener("fetch", (event) => {
  const req = event.request;
  const url = new URL(req.url);
  const isDoc = req.mode === "navigate" || req.destination === "document" ||
                (req.headers.get("accept") || "").includes("text/html");

  // /health と /version は「ナビゲーション時のみ」ラップして必ず戻れる
  if (isDoc && (url.pathname === "/health" || url.pathname === "/version")) {
    event.respondWith(handleHealthOrVersion(req, url.pathname, url.origin));
    return;
  }

  // ドキュメント（/、/ui、その他のHTML）は常に最新：cache:"reload"
  if (isDoc) {
    const freshReq = new Request(req, { cache: "reload", credentials: "same-origin" });
    event.respondWith(networkOnly(freshReq, { isDocument: true }));
    return;
  }

  // API は常に最新：cache:"no-store"
  if (url.pathname.startsWith("/api/")) {
    const apiReq = new Request(req, { cache: "no-store", credentials: "same-origin" });
    event.respondWith(networkOnly(apiReq));
    return;
  }

  // 静的資産（/static/*, /favicon*）も常に最新：cache:"no-store"
  if (req.method === "GET" && (url.pathname.startsWith("/static/") || url.pathname.startsWith("/favicon"))) {
    const assetReq = new Request(req, { cache: "no-store", credentials: "same-origin" });
    event.respondWith(networkOnly(assetReq));
    return;
  }

  // それ以外も安全側でネット専用：cache:"no-store"
  const otherReq = new Request(req, { cache: "no-store", credentials: "same-origin" });
  event.respondWith(networkOnly(otherReq));
});
