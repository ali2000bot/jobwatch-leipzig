import sqlite3
import json
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, Set

DB_PATH = Path("jobwatch_state.db")


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS kv_store (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)
        conn.commit()


def _load_json(key: str, default: Any):
    with get_conn() as conn:
        row = conn.execute(
            "SELECT value FROM kv_store WHERE key = ?",
            (key,)
        ).fetchone()

    if not row:
        return default

    try:
        return json.loads(row["value"])
    except Exception:
        return default


def _save_json(key: str, value: Any):
    payload = json.dumps(value, ensure_ascii=False, indent=2)
    now = datetime.now().isoformat(timespec="seconds")

    with get_conn() as conn:
        conn.execute("""
            INSERT INTO kv_store (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = excluded.updated_at
        """, (key, payload, now))
        conn.commit()


# --- Snapshot ---
def load_snapshot():
    return _load_json("snapshot", {"timestamp": None, "items": []})


def save_snapshot(items):
    _save_json("snapshot", {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "items": items
    })


# --- Favorites ---
def load_favorites():
    return _load_json("favorites", {})


def save_favorites(favs: Dict[str, Any]):
    _save_json("favorites", favs)


# --- Hidden Jobs ---
def load_hidden_jobs():
    return _load_json("hidden_jobs", {"hidden": [], "updated_at": None})


def save_hidden_jobs(hidden_keys: Set[str]):
    _save_json("hidden_jobs", {
        "hidden": sorted(list(hidden_keys)),
        "updated_at": datetime.now().isoformat(timespec="seconds")
    })


# --- Hidden Companies ---
def load_hidden_companies():
    data = _load_json("hidden_companies", [])
    if isinstance(data, list):
        return set(x.lower() for x in data)
    return set()


def save_hidden_companies(companies: Set[str]):
    _save_json("hidden_companies", sorted(list(companies)))


# --- Company State ---
def load_company_state():
    return _load_json("company_state", {})


def save_company_state(state: Dict[str, Any]):
    _save_json("company_state", state)
