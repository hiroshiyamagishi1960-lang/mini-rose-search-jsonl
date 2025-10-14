# 🌹 Mini Rose Search — Auto Refresh System (2025-10-09 Stable)

## 📘 概要
ミニバラ盆栽デジタル資料館の知識ベース（KB）を  
Notion → GitHub → Render へ自動同期する完全自動化構成。

- 更新周期：毎日 03:00 JST（GitHub Actions スケジュール）
- 成功時：通知なし（静かに完了）
- 失敗時：GitHub／メールで通知（`notifications@github.com`）

---

## ⚙️ 構成ファイル
| ファイル | 説明 |
|-----------|------|
| `newrose/refresh_kb.py` | KB自動取得・保存スクリプト（ETag対応） |
| `requirements.txt` | 依存ライブラリ（`requests` 含む） |
| `.github/workflows/refresh-kb.yml` | 自動更新ワークフロー（定期実行） |
| `snapshots/mykb.current.jsonl` | 最新KB（自動更新対象） |
| `README.md` | 運用・バックアップ用メモ |

---

## 🔁 自動実行の流れ
1. GitHub Actions が毎日 18:00 UTC（=03:00 JST）に起動  
2. `newrose/refresh_kb.py` が `KB_URL`（Secret）から最新 KB を取得  
3. 更新があれば `snapshots/mykb.current.jsonl` を上書き  
4. 自動コミット： `github-actions[bot] auto: refresh KB (YYYY-MM-DD)`

---

## 🔐 GitHub Secrets
| 名前 | 内容 |
|------|------|
| `KB_URL` | iCloud / GitHub Raw / Dropbox の `kb.jsonl` 直リンク |

---

## 🔔 通知設定（失敗時のみ）
- On GitHub：ON / Email：ON / Only notify for failed workflows：ON  
- 送信元：`notifications@github.com`

---

## 🧰 復元手順（バックアップから）
1. ZIP を展開しリポジトリとしてアップロード  
2. Render で連携（再デプロイ不要）  
3. GitHub Secrets に `KB_URL` を設定  
4. Actions → 「Run workflow」でテスト実行  
5. 成功後 `/health` と `/api/search?q=更新テスト` で確認

---

## 🪙 バージョンタグ
`v2025.10.09-auto-refresh-stable`

---

## 🧾 作成者
- Author: Hiroshi Yamagishi (山岸浩志)
- Project: Mini Rose Search / デジタル資料館  
- Date: 2025-10-09  
- Maintainer: Halo (AI Assistant)
