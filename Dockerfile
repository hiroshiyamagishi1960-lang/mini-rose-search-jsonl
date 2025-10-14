# ベース（やさしい日本語：土台のPython）
FROM python:3.11-slim

# 環境（ログをすぐ出す/pyc作らない）
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# 作業フォルダ
WORKDIR /app

# まず依存を入れる
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# アプリ本体をコピー
COPY . .

# ポート開放
EXPOSE 8000

# 起動コマンド（やさしい日本語：サーバを立てる）
CMD python verify_ui.py && uvicorn app:app --host 0.0.0.0 --port 10000
