import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

import requests
import streamlit as st

BASE = "https://rest.arbeitsagentur.de/jobboerse/jobsuche-service"
SEARCH_URL = f"{BASE}/pc/v4/app/jobs"
API_KEY_DEFAULT = "jobboerse-jobsuche"

STATE_DIR = ".jobwatch_state"
SNAPSHOT_FILE = os.path.join(STATE_DIR, "snapshot.json")


def ensure_state_dir() -> None:
    os.makedirs(STATE_DIR, exist_ok=True)


def load_snapshot() -> Dict[str, Any]:
    if not os.path.exists(SNAPSHOT_FILE):
        return {"timestamp": None, "items": []}
    with open(SNAPSHOT_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_snapshot(items: List[Dict[str, Any]]) -> None:
    ensure_state_dir()
    payload = {"timestamp": datetime.now().isoformat(timespec="seconds"), "items": items}
    with open(SNAPSHOT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def headers(api_key: str) -> Dict[str, str]:
    # API-Key + ein unkritischer User-Agent
    return {
        "X-API-Key": api_key,
        "Accept": "application/json",
        "User-Agent": "JobWatch-Leipzig/1.0",
    }


def extract_items(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    # v4/app/jobs liefert häufig "stellenangebote"
    if isinstance(data.get("stellenangebote"), list):
        return data["stellenangebote"]
    # Fallback:
    emb = data.get("_embedded") or {}
    if isinstance(emb.get("jobs"), list):
        return emb["jobs"]
    return []


def item_id(it: Dict[str, Any]) -> str:
    # Möglichst stabil: refnr wenn vorhanden, sonst hashId
    return it.get("refnr") or it.get("refNr") or it.get("hashId") or it.get("hashID") or ""


def item_title(it: Dict[str, Any]) -> str:
    return it.get("titel") or it.get("beruf") or it.get("title") or "Ohne Titel"


def item_company(it: Dict[str, Any]) -> str:
    return it.get("arbeitgeber") or it.get("arbeitgeberName") or it.get("unternehmen") or ""


def item_location(it: Dict[str, Any]) -> str:
    return it.get("arbeitsort") or it.get("ort") or it.get("wo") or ""


def details_url(it: Dict[str, Any]) -> Optional[str]:
    links = it.get("_links") or {}
    for k in ["details", "jobdetails"]:
        v = links.get(k)
        if isinstance(v, dict) and isinstance(v.get("href"), str):
            href = v["href"]
            return href if href.startswith("http") else (BASE + href)
    return None


@st.cache_data(ttl=300, show_spinner=False)
def fetch_search(
    api_key: str,
    wo: str,
    umkreis_km: int,
    was: str,
    aktualitaet_tage: int,
    size: int,
    page: int = 1,
    arbeitszeit: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    params = {
        "angebotsart": "1",
        "page": str(page),
        "pav": "false",
        "size": str(size),
        "umkreis": str(umkreis_km),
        "aktualitaet": str(aktualitaet_tage),
        "wo": wo,
        "was": was,
    }
    if arbeitszeit:
        params["arbeitszeit"] = arbeitszeit  # z.B. "ho"

    r = requests.get(SEARCH_URL, headers=headers(api_key), params=params, timeout=25)
    if r.status_code != 200:
        return [], f"Suche HTTP {r.status_code}: {r.text[:400]}"
    data = r.json()
    return extract_items(data), None


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_details(api_key: str, url: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    r = requests.get(url, headers=headers(api_key), timeout=25)
    if r.status_code != 200:
        return None, f"Details HTTP {r.status_code}: {r.text[:400]}"
    return r.json(), None


def build_default_query() -> str:
    fach = [
        "Thermophysik", "thermophysikalisch",
        "Thermoanalyse", "Thermal Analysis",
        "Analytik", "Materialcharakterisierung",
        "Wärmeleitfähigkeit", "Kalorimetrie", "DSC", "TGA", "LFA",
    ]
    rollen = ["Teamleiter", "Laborleiter", "Abteilungsleiter", "Leiter", "Head", "Lead", "Manager"]
    bereiche = [
        "Forschung", "Entwicklung", "R&D", "Research", "Development",
        "Projektmanagement", "Project Manager", "Program Manager",
        "Vertrieb", "Sales",
    ]
    return " ".join(bereiche + fach + rollen)


st.set_page_config(page_title="JobWatch Leipzig", layout="wide")
st.title("JobWatch Leipzig – neue Angebote finden & vergleichen")

with st.sidebar:
    st.header("Sucheinstellungen")
    wo = st.text_input("Ort", value="Leipzig")
    umkreis = st.selectbox("Umkreis vor Ort (km)", [25, 50], index=0)

    include_ho = st.checkbox("Homeoffice/Telearbeit extra berücksichtigen", value=True)
    ho_umkreis = st.slider("Umkreis Homeoffice (km)", 50, 800, 200, 50)

    was = st.text_area("Suchtext", value=build_default_query(), height=120)
    aktualitaet = st.slider("Nur Jobs der letzten X Tage", 0, 100, 30, 5)
    size = st.selectbox("Treffer pro Seite", [25, 50, 100], index=1)

    st.divider()
    api_key = st.text_input("X-API-Key", value=API_KEY_DEFAULT)

col1, col2 = st.columns([2, 1], gap="large")

with col2:
    snap = load_snapshot()
    st.subheader("Snapshot")
    st.write(snap.get("timestamp") or "— noch keiner gespeichert")

    if st.button("Snapshot speichern (aktueller Stand)"):
        # wird nach dem Abruf gesetzt
        st.session_state["save_snapshot_requested"] = True

    if st.button("Snapshot löschen"):
        ensure_state_dir()
        if os.path.exists(SNAPSHOT_FILE):
            os.remove(SNAPSHOT_FILE)
        st.success("Snapshot gelöscht. Seite neu laden.")

with col1:
    with st.spinner("Suche läuft…"):
        items_local, err1 = fetch_search(api_key, wo, int(umkreis), was, int(aktualitaet), int(size), arbeitszeit=None)
        items_ho: List[Dict[str, Any]] = []
        err2 = None
        if include_ho:
            items_ho, err2 = fetch_search(api_key, wo, int(ho_umkreis), was, int(aktualitaet), int(size), arbeitszeit="ho")

    if err1 or err2:
        st.error("Fehler beim Abruf")
        if err1:
            st.code(err1)
        if err2:
            st.code(err2)

    for it in items_local:
        it["_bucket"] = f"Vor Ort ({umkreis} km)"
    for it in items_ho:
        it["_bucket"] = f"Homeoffice ({ho_umkreis} km)"

    items_now = items_local + items_ho

    prev_items = snap.get("items", [])
    prev_ids: Set[str] = {item_id(x) for x in prev_items if item_id(x)}
    now_ids: Set[str] = {item_id(x) for x in items_now if item_id(x)}
    new_ids = now_ids - prev_ids

    st.subheader(f"Treffer: {len(items_now)}")
    st.caption(f"Neu seit Snapshot: {len(new_ids)}")

    if st.session_state.get("save_snapshot_requested"):
        save_snapshot(items_now)
        st.session_state["save_snapshot_requested"] = False
        st.success("Snapshot gespeichert.")

    def sort_key(it: Dict[str, Any]):
        jid = item_id(it)
        is_new = 0 if jid in new_ids else 1
        return (is_new, item_title(it).lower())

    st.divider()
    st.write("### Ergebnisse (klick = Details aufklappen)")

    for it in sorted(items_now, key=sort_key):
        jid = item_id(it)
        is_new = jid in new_ids
        label = f"{'🟢 NEU  ' if is_new else ''}{item_title(it)}"
        meta = " | ".join([x for x in [it.get("_bucket", ""), item_company(it), item_location(it)] if x])

        with st.expander(label):
            st.caption(meta)

            url = details_url(it)
            if not url:
                st.warning("Für dieses Angebot ist keine Detail-URL vorhanden.")
                continue

            details, derr = fetch_details(api_key, url)
            if derr:
                st.error(derr)
                continue

            st.write("**Kurzinfo**")
            c1, c2, c3 = st.columns(3)
            c1.write(f"**Arbeitgeber:** {details.get('arbeitgeber') or details.get('arbeitgeberName') or '—'}")
            c2.write(f"**Ort:** {details.get('arbeitsort') or '—'}")
            c3.write(f"**ID:** {jid or '—'}")

            st.write("**Beschreibung**")
            desc = details.get("stellenbeschreibung") or details.get("beschreibung") or details.get("jobbeschreibung")
            if isinstance(desc, str) and desc.strip():
                st.write(desc)
            else:
                st.info("Keine ausführliche Beschreibung im Detail-Response gefunden.")
