(function () {
  const $ = (sel, root=document) => root.querySelector(sel);
  const $$ = (sel, root=document) => Array.from(root.querySelectorAll(sel));
  const byId = id => document.getElementById(id);
  const fmtTime = d => new Date(d).toLocaleString('ja-JP', {hour12:false});

  const status = byId('status');
  const tpl = byId('tpl-msg');

  // --- Tab switch（タブ切替） ---
  const tabChat = byId('tab-chat');
  const tabSearch = byId('tab-search');
  const panelChat = byId('panel-chat');
  const panelSearch = byId('panel-search');
  function activate(tab, panel) {
    $$('.tab').forEach(b => b.classList.remove('active'));
    $$('.panel').forEach(p => p.classList.remove('active'));
    tab.classList.add('active');
    panel.classList.add('active');
  }
  tabChat.addEventListener('click', () => activate(tabChat, panelChat));
  tabSearch.addEventListener('click', () => activate(tabSearch, panelSearch));

  // --- helpers（補助関数） ---
  function card(kind, text) {
    const node = tpl.content.cloneNode(true);
    node.querySelector('.badge').textContent = kind;
    node.querySelector('time').textContent = fmtTime(Date.now());
    node.querySelector('.content').textContent = text;
    return node;
  }
  function setStatus(t){ status.textContent = t; }

  // --- Ask ROSE (/api/chat エンドポイント＝入り口の道) ---
  const chatForm = byId('chat-form');
  const chatInput = byId('chat-input');
  const chatLog = byId('chat-log');

  chatForm.addEventListener('submit', async (e) => {
    e.preventDefault();
    const q = (chatInput.value || '').trim();
    if (!q) return;
    chatLog.prepend(card('You', q));
    chatInput.value = '';
    setStatus('ROSEに問い合わせ中…');

    try {
      const res = await fetch('/api/chat', {
        method:'POST',                                  // POST（データを送る通信）
        headers: {'content-type':'application/json'},   // JSON（機械が読むデータの形）
        body: JSON.stringify({ query: q })
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      const text = typeof data === 'string' ? data : (data.answer ?? JSON.stringify(data, null, 2));
      chatLog.prepend(card('ROSE', text));
      setStatus('OK');
    } catch (err) {
      chatLog.prepend(card('Error', String(err)));
      setStatus('エラー');
    }
  });

  // --- Search (/api/search エンドポイント＝入り口の道) ---
  const searchForm = byId('search-form');
  const searchInput = byId('search-input');
  const searchResults = byId('search-results');

  searchForm.addEventListener('submit', async (e) => {
    e.preventDefault();
    const q = (searchInput.value || '').trim();
    if (!q) return;
    searchResults.prepend(card('Query', q));
    searchInput.value = '';
    setStatus('検索中…');

    try {
      const res = await fetch('/api/search', {
        method:'POST',
        headers: {'content-type':'application/json'},
        body: JSON.stringify({ query: q, limit: 10 })
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      const pretty = Array.isArray(data) ? data.map(x=>JSON.stringify(x)).join('\n') : JSON.stringify(data, null, 2);
      searchResults.prepend(card('Result', pretty));
      setStatus('OK');
    } catch (err) {
      searchResults.prepend(card('Error', String(err)));
      setStatus('エラー');
    }
  });

  // 初期フォーカス（最初にカーソルを入れる）
  chatInput.focus();
})();
