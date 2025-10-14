# NewRose 24/7 Cloud Deploy（最小）

## 使い方（Render 版：クリック最小）
1) このフォルダ内のファイルを **プロジェクト直下**（app.py がある階層）に置く
   - Dockerfile
   - requirements.txt（あなたの既存 requirements に追記してOK）
   - render.yaml
2) GitHub に push
3) Render で「New +」→「Blueprint」→ リポジトリを選択 → `render.yaml` を指定
4) 環境変数に **KB_URL** を設定（iCloud共有 or GitHub Raw / Dropbox ?dl=1）
5) デプロイ後、`/health` が 200 なら稼働OK

## 使い方（Fly.io 版：CLI）
1) `flyctl` をインストール
2) `flyctl launch`（既存 app 名は `newrose-fastapi` 例）
3) 環境変数 KB_URL を設定：`flyctl secrets set KB_URL="https://..."`
4) `flyctl deploy`

## 注意
- コンテナの起動コマンドは：`uvicorn app:app --host 0.0.0.0 --port $PORT`
- 既存の requirements.txt がある場合、本ファイルの内容を **追記** してください（上書きしない）。
- すでに「手順B（自動KB取得）」を組み込んでいることが前提です。
