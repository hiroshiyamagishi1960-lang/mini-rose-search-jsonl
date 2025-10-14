# NewRose FastAPI - Dockerfile (Render/Fly 対応)
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1     PYTHONUNBUFFERED=1     PIP_NO_CACHE_DIR=1

WORKDIR /app

# システム依存を最小に
RUN apt-get update -y && apt-get install -y --no-install-recommends     build-essential curl ca-certificates  && rm -rf /var/lib/apt/lists/*

# 依存インストール
COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt

# アプリ本体（ユーザーのプロジェクトに合わせて後で上書きされる想定）
# ここではプレースホルダを置くだけ。ユーザー側で本物の app.py が存在する前提。
COPY . /app

EXPOSE 8080
# Render は $PORT を渡す。Fly は 8080 を使う前提に。
CMD ["/bin/sh", "-c", "uvicorn app:app --host 0.0.0.0 --port ${PORT:-8080}"]
