/* Service Worker — 最終安定版（UI変更なし・戻る問題完全解決）
   改良点：
   1) /health・/version をナビゲーション時にラップして「←検索画面に戻る」を確実に表示。
   2) /ui が HEAD 405 の環境でも fallback=/ に自動切替。
   3) 旧キャッシュ・旧SWを完全削除し、Navigation Preload で常に最新取得。
   4) すべてのリソースを cache:"no-store"/"reload" で扱い、古いUI残留を根絶。
*/

self.addEventListener("install", (event) => {
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil((async () => {
    try {
      const keys = await caches.keys();
      await Promise.all(keys.map((k) => caches.delete(k)));
    } catch (_) {}
    try {
      if (self.registration.navigationPreload) {
        await self.registration.navigationPreload.enable();
      }
    } catch (_) {}
    await self.clients.claim();
  })());
});

// ---------- ユーティリティ ----------
function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
  }[c]));
}

function wrapSimplePage(title, bodyHtml) {
  return new Response(`<!doctype html>
<html lang="ja"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>${title}</title><meta name="robots" content="noindex">
<style>
:root{--bg:#fafafa;--card:#fff;--b:#e5e7eb;--txt:#0f172a;--link:#2563eb;--muted:#64748b}
body{background:var(--bg);color:var(--txt);margin:0;font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial}
.wrap{max-width:900px;margin:16px auto;padding:0 16px}
.card{background:var(--card);border:1px solid var(--b);border-radius:12px;padding:12px}
pre{white-space:pre-wrap;word-break:break-word;background:#f8fafc;border:1px solid var(--b);border-radius:8px;padding:10px;margin:8px 0}
a.btn{display:inline-block;padding:10px 14px;border:1px solid var(--b);border-radius:10px;text-decoration:none;color:var(--link);background:var(--card)}
</style></head><body>
<div class="wrap"><h1>${title}</h1><div class="card">${bodyHtml}</div></div></body></html>`,
    { status: 200, headers: { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store" } }
  );
}

async function decideReturnHref(origin) {
  try {
    const res = await fetch(new Request(origin + "/ui", { method: "HEAD", cache: "no-store" }));
    if (res.ok) return "/ui";
  } catch (_) {}
  // HEAD禁止環境の保険（GET で再確認）
  try {
    const res2 = await fetch(new Request(origin + "/ui", { method: "GET", cache: "no-store" }));
    if (res2.ok) return "/ui";
  } catch (_) {}
  return "/";
}

async function handleHealthOrVersion(req, pathname, origin) {
  const title = pathname === "/health" ? "Health" : "Version";
  const start = Date.now();
  let text = "", status = 0;
  try {
    const fresh = await fetch(new Request(req, { cache: "no-store", credentials: "same-origin" }));
    status = fresh.status;
    text = await fresh.text();
  } catch (e) { text = e?.message || String(e); }
  let pretty = text;
  try { pretty = JSON.stringify(JSON.parse(text), null, 2); }
  catch (_) { if (pretty.length > 20000) pretty = pretty.slice(0, 20000) + "\n...[truncated]..."; }
  const ms = Date.now() - start;
  const backHref = await decideReturnHref(origin);
  const body = `
    <div style="font-size:13px;color:var(--muted)">status=${escapeHtml(String(status))}　time=${escapeHtml(String(ms))}ms　path=${escapeHtml(pathname)}</div>
    <p>下はサーバーの最新応答です（常にネットから取得）。</p>
    <pre>${escapeHtml(pretty)}</pre>
    <p><a class="btn" href="${backHref}">← 検索画面に戻る</a></p>`;
  return wrapSimplePage(`Mini Rose — ${title}`, body);
}

async function networkOnly(req, { isDocument = false } = {}) {
  try { return await fetch(req); }
  catch (e) {
    if (isDocument) {
      const msg = e?.message || String(e);
      return wrapSimplePage("ネットワークエラー", `<p>最新画面を取得できません。</p><pre>${escapeHtml(msg)}</pre>`);
    }
    return new Response("Network error", { status: 503, headers: { "Cache-Control": "no-store" } });
  }
}

// ---------- フェッチポリシー ----------
self.addEventListener("fetch", (event) => {
  const req = event.request;
  const url = new URL(req.url);
  const isDoc = req.mode === "navigate" || req.destination === "document" ||
                (req.headers.get("accept") || "").includes("text/html");

  if (isDoc && (url.pathname === "/health" || url.pathname === "/version")) {
    event.respondWith(handleHealthOrVersion(req, url.pathname, url.origin));
    return;
  }

  if (isDoc) {
    event.respondWith(networkOnly(new Request(req, { cache: "reload", credentials: "same-origin" }), { isDocument: true }));
    return;
  }

  if (url.pathname.startsWith("/api/") || url.pathname.startsWith("/static/") || url.pathname.startsWith("/favicon")) {
    event.respondWith(networkOnly(new Request(req, { cache: "no-store", credentials: "same-origin" })));
    return;
  }

  event.respondWith(networkOnly(new Request(req, { cache: "no-store", credentials: "same-origin" })));
});
