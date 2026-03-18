import streamlit as st
from datetime import datetime
from typing import Any, Dict, Set
from supabase import create_client, Client


def get_client() -> Client:
    return create_client(
        st.secrets["SUPABASE_URL"],
        st.secrets["SUPABASE_KEY"]
    )


def init_db():
    pass


def _load_json(key: str, default: Any):
    supabase = get_client()
    res = (
        supabase.table("jobwatch_state")
        .select("value")
        .eq("id", key)
        .limit(1)
        .execute()
    )

    data = getattr(res, "data", None) or []
    return data[0]["value"] if data else default


def _save_json(key: str, value: Any):
    supabase = get_client()
    supabase.table("jobwatch_state").upsert({
        "id": key,
        "value": value,
        "updated_at": datetime.utcnow().isoformat()
    }).execute()


def load_snapshot():
    return _load_json("snapshot", {"timestamp": None, "items": []})


def save_snapshot(items):
    _save_json("snapshot", {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "items": items
    })


def load_favorites():
    return _load_json("favorites", {})


def save_favorites(favs: Dict[str, Any]):
    _save_json("favorites", favs)


def load_hidden_jobs():
    return _load_json("hidden_jobs", {"hidden": [], "updated_at": None})


def save_hidden_jobs(hidden_keys: Set[str]):
    _save_json("hidden_jobs", {
        "hidden": sorted(list(hidden_keys)),
        "updated_at": datetime.now().isoformat(timespec="seconds")
    })


def load_hidden_companies():
    data = _load_json("hidden_companies", [])
    return set(x.lower() for x in data) if isinstance(data, list) else set()


def save_hidden_companies(companies: Set[str]):
    _save_json("hidden_companies", sorted(list(companies)))


def load_company_state():
    return _load_json("company_state", {})


def save_company_state(state: Dict[str, Any]):
    _save_json("company_state", state)
