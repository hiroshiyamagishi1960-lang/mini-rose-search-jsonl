/* Service Worker — 自己解除・安定版（冗長ログ付き）
   目的：
   - 端末ごとに残っている古いSW/キャッシュを“自動で完全掃除”
   - その場で最新UIにリロード
   - 以後は SW を使わず、ブラウザが直で取得（= 不整合の温床を断つ）
   特徴：
   - 詳細ログ（console）で状況を把握しやすい
   - 例外を個別に握りつぶして“確実に次へ進む”
   - fetch ハンドラなし＝読み替わった瞬間から一切干渉しない
*/

(function(){
  const TAG = "[SW-OFF]";
  const log = (...a) => { try{ console.log(TAG, ...a); }catch(_){} };
  const err = (...a) => { try{ console.error(TAG, ...a); }catch(_){} };

  self.addEventListener("install", (event) => {
    log("install: skipWaiting()");
    self.skipWaiting();
  });

  self.addEventListener("activate", (event) => {
    event.waitUntil((async () => {
      log("activate: start");

      // 1) すべてのキャッシュ削除
      try {
        const keys = await caches.keys();
        log("activate: cache keys =", keys);
        await Promise.all(keys.map(k => caches.delete(k)));
        log("activate: cache cleared");
      } catch (e) {
        err("activate: cache clear failed", e);
      }

      // 2) 自分自身を登録解除（以後SWなし）
      try {
        const ok = await self.registration.unregister();
        log("activate: unregister =", ok);
      } catch (e) {
        err("activate: unregister failed", e);
      }

      // 3) クライアントを乗っ取り & その場で最新に更新
      try {
        await self.clients.claim();
        const cs = await self.clients.matchAll({ type: "window", includeUncontrolled: true });
        log("activate: clients =", cs?.length || 0);
        await Promise.all(cs.map(c => {
          try { return c && c.navigate ? c.navigate(c.url) : null; }
          catch(e){ err("activate: navigate failed", e); return null; }
        }));
        log("activate: clients reloaded");
      } catch (e) {
        err("activate: claim/navigate failed", e);
      }

      log("activate: done");
    })());
  });

  // UI からの“保険用”メッセージ（任意）
  self.addEventListener("message", (event) => {
    const data = event?.data || {};
    if (data && data.type === "FORCE_UNREGISTER") {
      (async () => {
        try {
          await caches.keys().then(keys => Promise.all(keys.map(k => caches.delete(k))));
        } catch(_) {}
        try { await self.registration.unregister(); } catch(_) {}
        try {
          await self.clients.claim();
          const cs = await self.clients.matchAll({ type: "window", includeUncontrolled: true });
          await Promise.all(cs.map(c => c?.navigate ? c.navigate(c.url) : null));
        } catch(_) {}
      })();
    }
  });

  // fetchハンドラを持たない = 以後いっさい干渉しない
})();
