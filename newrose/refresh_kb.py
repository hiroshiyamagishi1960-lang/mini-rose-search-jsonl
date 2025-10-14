from __future__ import annotations
import os, sys, json, hashlib, time
from pathlib import Path

try:
    import requests  # type: ignore
except Exception as e:
    raise RuntimeError("requests が未インストールです。`pip install requests` を実行してください") from e

SNAP_DIR = (Path(__file__).resolve().parents[1] / "snapshots")
SNAP_DIR.mkdir(parents=True, exist_ok=True)
DEST = SNAP_DIR / "mykb.current.jsonl"
ETAG = DEST.with_suffix(DEST.suffix + ".etag")

def _read_text(p: Path) -> str | None:
    try:
        return p.read_text(encoding="utf-8")
    except Exception:
        return None

def _write_bytes(p: Path, data: bytes) -> None:
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_bytes(data)
    tmp.replace(p)

def _save_etag(value: str | None) -> None:
    if value:
        ETAG.write_text(value, encoding="utf-8")
    elif ETAG.exists():
        ETAG.unlink(missing_ok=True)

def _get_headers() -> dict:
    headers = {"User-Agent": "newrose/refresh-kb"}
    if ETAG.exists():
        headers["If-None-Match"] = _read_text(ETAG) or ""
    return headers

def _download(url: str) -> tuple[str, bytes] | None:
    """ETag対応のGET。304ならNoneを返す。"""
    resp = requests.get(url, headers=_get_headers(), timeout=(10, 30), stream=True)
    if resp.status_code == 304:
        return None
    resp.raise_for_status()
    etag = resp.headers.get("ETag", "")
    data = resp.content
    return etag, data

def ensure_kb() -> dict:
    """
    KB_URL（環境変数）から snapshots/mykb.current.jsonl を同期。
    - 取得成功: リモート採用
    - 304: 変更なし
    - 失敗/未設定: ローカルを維持 or 最小ダミー作成
    戻り値: 状態サマリ
    """
    url = (os.getenv("KB_URL") or "").strip()
    SNAP_DIR.mkdir(parents=True, exist_ok=True)

    # ローカルが無ければ最小ダミーを作成
    if not DEST.exists():
        _write_bytes(DEST, b'{"id":"init","topic":"mini-rose-bonsai","note":"placeholder"}\\n')

    if not url:
        return {"status": "local-only", "path": str(DEST), "bytes": DEST.stat().st_size, "msg": "KB_URL未設定"}

    try:
        got = _download(url)
        if got is None:
            # 変更なし
            return {"status": "not-modified", "path": str(DEST), "bytes": DEST.stat().st_size, "etag": _read_text(ETAG)}

        etag, data = got
        _write_bytes(DEST, data)
        _save_etag(etag)
        return {"status": "fetched", "path": str(DEST), "bytes": len(data), "etag": etag}
    except Exception as e:
        # フォールバック：ローカル継続
        return {
            "status": "error-fallback",
            "error": f"{type(e).__name__}: {e}",
            "path": str(DEST),
            "bytes": DEST.stat().st_size,
        }

if __name__ == "__main__":
    info = ensure_kb()
    print(json.dumps(info, ensure_ascii=False, indent=2))
