import json
import math
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
import pydeck as pdk
import requests
import streamlit as st
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE = "https://rest.arbeitsagentur.de/jobboerse/jobsuche-service"
SEARCH_URL = f"{BASE}/pc/v4/app/jobs"
API_KEY_DEFAULT = "jobboerse-jobsuche"

STATE_DIR = ".jobwatch_state"
SNAPSHOT_FILE = os.path.join(STATE_DIR, "snapshot.json")

# Default Wohnort: 06242 Braunsbedra (WGS84, ca. Stadtzentrum)
DEFAULT_HOME_LABEL = "06242 Braunsbedra"
DEFAULT_HOME_LAT = 51.2861
DEFAULT_HOME_LON = 11.8900

# ---- Standard-Keywords ----
DEFAULT_FOCUS_KEYWORDS = [
    "thermoanalyse", "thermophysik", "thermal analysis", "thermophysical",
    "dsc", "tga", "lfa", "dilatometrie", "dilatometer", "sta", "dma", "tma",
    "wärmeleitfähigkeit", "thermal conductivity", "diffusivität", "diffusivity",
    "kalorimetrie", "calorimetry", "cp", "wärmekapazität", "heat capacity",
    "materialcharakterisierung", "material characterization",
    "analytik", "instrumentierung", "messgerät", "labor",
    "werkstoff", "werkstoffe", "polymer", "keramik", "metall",
    "f&e", "verfahrenstechnik", "physik", "physics",
]
DEFAULT_LEADERSHIP_KEYWORDS = [
    "laborleiter", "teamleiter", "gruppenleiter", "abteilungsleiter",
    "leiter", "head", "lead", "director", "manager", "principal",
]
DEFAULT_NEGATIVE_KEYWORDS = [
    "insurance", "versicherung",
    "assistant", "assistenz", "sekretariat",
    "office", "backoffice", "reception", "empfang",
    "vorstandsassistenz", "management assistant",
]


# ---------------- State ----------------
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


# ---------------- BA API helpers ----------------
def headers(api_key: str) -> Dict[str, str]:
    return {
        "User-Agent": "Jobsuche/2.9.2 (de.arbeitsagentur.jobboerse; build:1077) Streamlit",
        "Host": "rest.arbeitsagentur.de",
        "X-API-Key": api_key,
        "Accept": "application/json",
        "Connection": "keep-alive",
    }


def extract_items(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if isinstance(data.get("stellenangebote"), list):
        return data["stellenangebote"]
    emb = data.get("_embedded") or {}
    if isinstance(emb, dict):
        if isinstance(emb.get("stellenangebote"), list):
            return emb["stellenangebote"]
        if isinstance(emb.get("jobs"), list):
            return emb["jobs"]
    if isinstance(data.get("jobs"), list):
        return data["jobs"]
    return []


def item_id(it: Dict[str, Any]) -> str:
    return it.get("refnr") or it.get("refNr") or it.get("hashId") or it.get("hashID") or ""


def item_title(it: Dict[str, Any]) -> str:
    return it.get("titel") or it.get("beruf") or it.get("title") or "Ohne Titel"


def item_company(it: Dict[str, Any]) -> str:
    return it.get("arbeitgeber") or it.get("arbeitgeberName") or it.get("unternehmen") or ""


def pretty_location(it: Dict[str, Any]) -> str:
    loc = it.get("arbeitsort") or it.get("ort") or it.get("wo")
    if isinstance(loc, str):
        return loc
    if isinstance(loc, dict):
        ort = (loc.get("ort") or "").strip()
        region = (loc.get("region") or "").strip()
        land = (loc.get("land") or "").strip()
        parts = [p for p in [ort, region, land] if p]
        return ", ".join(parts) if parts else "—"
    if loc is None:
        return "—"
    try:
        return json.dumps(loc, ensure_ascii=False)
    except Exception:
        return str(loc)


def extract_latlon_from_item(it: Dict[str, Any]) -> Optional[Tuple[float, float]]:
    loc = it.get("arbeitsort") or {}
    if isinstance(loc, dict):
        coords = loc.get("koordinaten") or {}
        if isinstance(coords, dict):
            lat = coords.get("lat")
            lon = coords.get("lon")
            try:
                if lat is not None and lon is not None:
                    return float(lat), float(lon)
            except Exception:
                pass

    coords2 = it.get("koordinaten")
    if isinstance(coords2, dict):
        lat = coords2.get("lat")
        lon = coords2.get("lon")
        try:
            if lat is not None and lon is not None:
                return float(lat), float(lon)
        except Exception:
            pass

    return None


def details_url_api(it: Dict[str, Any]) -> Optional[str]:
    links = it.get("_links") or {}
    for k in ["details", "jobdetails"]:
        v = links.get(k)
        if isinstance(v, dict) and isinstance(v.get("href"), str):
            href = v["href"]
            return href if href.startswith("http") else (BASE + href)
    return None


def jobsuche_web_url(it: Dict[str, Any]) -> Optional[str]:
    ref = item_id(it)
    if not ref:
        return None
    return f"https://www.arbeitsagentur.de/jobsuche/jobdetail/{ref}"


def short_field(it: Dict[str, Any], *keys: str) -> str:
    for k in keys:
        v = it.get(k)
        if v is None:
            continue
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


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
    params: Dict[str, Any] = {
        "page": str(page),
        "size": str(size),
        "umkreis": str(umkreis_km),
        "aktualitaet": str(aktualitaet_tage),
        "wo": wo,
    }
    if was and was.strip():
        params["was"] = was
    if arbeitszeit:
        params["arbeitszeit"] = arbeitszeit

    try:
        r = requests.get(
            SEARCH_URL,
            headers=headers(api_key),
            params=params,
            timeout=25,
            verify=False,
        )
    except Exception as e:
        return [], f"Request-Fehler: {type(e).__name__}: {e}"

    if r.status_code != 200:
        return [], f"Suche HTTP {r.status_code}: {r.text[:600]}"

    try:
        data = r.json()
    except Exception:
        return [], "Suche: Antwort war kein gültiges JSON."

    return extract_items(data), None


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_details(api_key: str, url: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    try:
        r = requests.get(url, headers=headers(api_key), timeout=25, verify=False)
    except Exception as e:
        return None, f"Details-Request-Fehler: {type(e).__name__}: {e}"

    if r.status_code != 200:
        return None, f"Details HTTP {r.status_code}: {r.text[:600]}"

    try:
        return r.json(), None
    except Exception:
        return None, "Details: Antwort war kein gültiges JSON."


# ---------------- Distance + travel time ----------------
def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0088
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c


def distance_from_home_km(it: Dict[str, Any], home_lat: float, home_lon: float) -> Optional[float]:
    ll = extract_latlon_from_item(it)
    if not ll:
        return None
    lat, lon = ll
    return haversine_km(home_lat, home_lon, lat, lon)


def travel_time_minutes(distance_km: Optional[float], speed_kmh: float) -> Optional[int]:
    if distance_km is None or speed_kmh <= 0:
        return None
    minutes = int(round((distance_km / speed_kmh) * 60))
    return max(0, minutes)


def distance_badge_html(dist_km: Optional[float], t_min: Optional[int], near_km: int, mid_km: int) -> str:
    if dist_km is None:
        return '<span style="background:#999;color:white;padding:2px 8px;border-radius:999px;font-size:12px;">Entf.: —</span>'

    if dist_km <= near_km:
        bg = "#2e7d32"  # grün
    elif dist_km <= mid_km:
        bg = "#f9a825"  # gelb
    else:
        bg = "#c62828"  # rot

    t_part = f" · ~{t_min} min" if t_min is not None else ""
    txt = f"{dist_km:.1f} km{t_part}"
    return f'<span style="background:{bg};color:white;padding:2px 8px;border-radius:999px;font-size:12px;">{txt}</span>'


# ---------------- Keywords editor ----------------
def parse_keywords(text: str) -> List[str]:
    raw: List[str] = []
    for line in text.splitlines():
        raw.extend([p.strip() for p in line.split(",")])
    return [x for x in raw if x]


def keywords_to_text(words: List[str]) -> str:
    return "\n".join(words)


# ---------------- Queries ----------------
def build_queries() -> Dict[str, str]:
    # Breite Suchprofile; Feinsteuerung passiert über Scoring/Filter.
    q_rd = "Forschung Entwicklung R&D Thermoanalyse Thermophysik Analytik"
    q_pm = "Projektmanagement Project Manager Program Manager"
    q_sales = "Vertrieb Sales Business Development Key Account Manager"
    return {"R&D": q_rd, "Projektmanagement": q_pm, "Vertrieb": q_sales}


# ---------------- UI ----------------
st.set_page_config(page_title="JobWatch Leipzig", layout="wide")
st.title("JobWatch Leipzig – neue Angebote finden & vergleichen")

# Session defaults für Keyword-Editor
if "kw_focus" not in st.session_state:
    st.session_state["kw_focus"] = keywords_to_text(DEFAULT_FOCUS_KEYWORDS)
if "kw_lead" not in st.session_state:
    st.session_state["kw_lead"] = keywords_to_text(DEFAULT_LEADERSHIP_KEYWORDS)
if "kw_neg" not in st.session_state:
    st.session_state["kw_neg"] = keywords_to_text(DEFAULT_NEGATIVE_KEYWORDS)

with st.sidebar:
    st.header("Wohnort & Entfernung")
    home_label = st.text_input("Wohnort (Anzeige)", value=DEFAULT_HOME_LABEL)
    c_home1, c_home2 = st.columns(2)
    with c_home1:
        home_lat = st.number_input("Breitengrad", value=float(DEFAULT_HOME_LAT), format="%.6f")
    with c_home2:
        home_lon = st.number_input("Längengrad", value=float(DEFAULT_HOME_LON), format="%.6f")

    st.subheader("Farbmarkierung Entfernung")
    near_km = st.slider("Grün bis (km)", 5, 80, 25, 5)
    mid_km = st.slider("Gelb bis (km)", 10, 150, 60, 5)

    st.subheader("Fahrzeit-Schätzung")
    speed_kmh = st.slider("Ø Geschwindigkeit (km/h)", 30, 140, 75, 5)

    st.divider()
    st.header("Sucheinstellungen")
    wo = st.text_input("Ort (BA-Suche)", value="Leipzig")
    umkreis = st.selectbox("Umkreis vor Ort (km)", [25, 50], index=1)

    include_ho = st.checkbox("Homeoffice/Telearbeit extra berücksichtigen", value=True)
    ho_umkreis = st.slider("Umkreis Homeoffice (km)", 50, 800, 200, 50)

    queries = build_queries()
    selected = st.multiselect("Profile", list(queries.keys()), default=list(queries.keys()))

    aktualitaet = st.slider("Nur Jobs der letzten X Tage", 0, 365, 60, 5)
    size = st.selectbox("Treffer pro Seite", [25, 50, 100], index=1)

    st.divider()
    api_key = st.text_input("X-API-Key", value=API_KEY_DEFAULT)

    st.subheader("Profil-Filter")
    only_focus = st.checkbox("Nur profilrelevante Treffer anzeigen", value=True)
    hide_irrelevant = st.checkbox("Assistenzen/Office/Insurance ausblenden", value=True)
    min_score = st.slider("Mindest-Score", 0, 50, 8, 1)

    st.divider()
    st.subheader("Keywords (sichtbar & editierbar)")
    with st.expander("Fokus-Keywords", expanded=False):
        st.session_state["kw_focus"] = st.text_area(
            "Ein Begriff pro Zeile (oder Komma-getrennt)",
            value=st.session_state["kw_focus"],
            height=160,
        )
    with st.expander("Leitung/Führung-Keywords", expanded=False):
        st.session_state["kw_lead"] = st.text_area(
            "Ein Begriff pro Zeile (oder Komma-getrennt)",
            value=st.session_state["kw_lead"],
            height=120,
        )
    with st.expander("Negative Keywords", expanded=False):
        st.session_state["kw_neg"] = st.text_area(
            "Ein Begriff pro Zeile (oder Komma-getrennt)",
            value=st.session_state["kw_neg"],
            height=120,
        )

    c_reset, c_dbg = st.columns(2)
    with c_reset:
        if st.button("↩︎ Keywords zurücksetzen"):
            st.session_state["kw_focus"] = keywords_to_text(DEFAULT_FOCUS_KEYWORDS)
            st.session_state["kw_lead"] = keywords_to_text(DEFAULT_LEADERSHIP_KEYWORDS)
            st.session_state["kw_neg"] = keywords_to_text(DEFAULT_NEGATIVE_KEYWORDS)
            st.rerun()
    with c_dbg:
        debug = st.checkbox("Debug anzeigen", value=False)

# Live geparste Keywords
FOCUS_KEYWORDS = parse_keywords(st.session_state["kw_focus"])
LEADERSHIP_KEYWORDS = parse_keywords(st.session_state["kw_lead"])
NEGATIVE_KEYWORDS = parse_keywords(st.session_state["kw_neg"])

col1, col2 = st.columns([2, 1], gap="large")

with col2:
    snap = load_snapshot()
    st.subheader("Snapshot")
    st.write(snap.get("timestamp") or "— noch keiner gespeichert")

    if st.button("Snapshot speichern (aktueller Stand)"):
        st.session_state["save_snapshot_requested"] = True

    if st.button("Snapshot löschen"):
        ensure_state_dir()
        if os.path.exists(SNAPSHOT_FILE):
            os.remove(SNAPSHOT_FILE)
        st.success("Snapshot gelöscht. Seite neu laden.")

with col1:
    def looks_leadership(it: Dict[str, Any]) -> bool:
        text = " ".join([str(item_title(it)), str(it.get("kurzbeschreibung", ""))]).lower()
        return any(k in text for k in LEADERSHIP_KEYWORDS)

    def relevance_score(it: Dict[str, Any]) -> int:
        text = " ".join(
            [
                str(item_title(it)),
                str(it.get("kurzbeschreibung", "")),
                str(it.get("arbeitgeber", "")),
                str(it.get("arbeitgeberName", "")),
            ]
        ).lower()

        score = 0
        for k in FOCUS_KEYWORDS:
            if k in text:
                score += 10
        for k in LEADERSHIP_KEYWORDS:
            if k in text:
                score += 6
        for k in ["forschung", "entwicklung", "r&d", "research", "development"]:
            if k in text:
                score += 4
        for k in NEGATIVE_KEYWORDS:
            if k in text:
                score -= 12
        return score

    def is_probably_irrelevant(it: Dict[str, Any]) -> bool:
        text = f"{item_title(it)} {it.get('kurzbeschreibung','')}".lower()
        hard = ["vorstandsassistenz", "management assistant", "assistant", "assistenz", "sekretariat", "insurance", "versicherung"]
        return any(h in text for h in hard)

    # --- Search ---
    with st.spinner("Suche läuft…"):
        all_items: List[Dict[str, Any]] = []
        errs: List[str] = []

        if not selected:
            errs.append("Bitte mindestens ein Profil auswählen.")
        else:
            for name in selected:
                q = queries[name]

                items_local, e1 = fetch_search(api_key, wo, int(umkreis), q, int(aktualitaet), int(size), arbeitszeit=None)
                if e1:
                    errs.append(f"{name} (vor Ort): {e1}")
                for it in items_local:
                    it["_profile"] = name
                    it["_bucket"] = f"Vor Ort ({umkreis} km)"
                all_items.extend(items_local)

                if include_ho:
                    items_ho, e2 = fetch_search(api_key, wo, int(ho_umkreis), q, int(aktualitaet), int(size), arbeitszeit="ho")
                    if e2:
                        errs.append(f"{name} (homeoffice): {e2}")
                    for it in items_ho:
                        it["_profile"] = name
                        it["_bucket"] = f"Homeoffice ({ho_umkreis} km)"
                    all_items.extend(items_ho)

    if errs:
        st.error("Fehler / Hinweise")
        for e in errs:
            st.code(e)

    # Dedup nach ID
    items_now: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    for it in all_items:
        jid = item_id(it)
        if jid and jid not in seen:
            seen.add(jid)
            items_now.append(it)

    # Profil-Filter
    if hide_irrelevant:
        items_now = [it for it in items_now if not is_probably_irrelevant(it)]
    if only_focus:
        items_now = [it for it in items_now if relevance_score(it) >= min_score]

    # Snapshot-Vergleich
    prev_items = snap.get("items", [])
    prev_ids: Set[str] = {item_id(x) for x in prev_items if item_id(x)}
    now_ids: Set[str] = {item_id(x) for x in items_now if item_id(x)}
    new_ids = now_ids - prev_ids

    # ---------- SORTIERUNG: Entfernung aufsteigend ----------
    def sort_key(it: Dict[str, Any]):
        dist = distance_from_home_km(it, float(home_lat), float(home_lon))
        dist_rank = dist if dist is not None else 999999.0
        is_new = 0 if item_id(it) in new_ids else 1
        score = relevance_score(it)
        return (dist_rank, is_new, -score, item_title(it).lower())

    items_now_sorted = sorted(items_now, key=sort_key)

    st.subheader(f"Treffer: {len(items_now_sorted)}")
    st.caption(f"Neu seit Snapshot: {len(new_ids)}")

    if st.session_state.get("save_snapshot_requested"):
        save_snapshot(items_now_sorted)
        st.session_state["save_snapshot_requested"] = False
        st.success("Snapshot gespeichert.")

    if debug:
        st.info("Debug ist aktiv.")
        test_items, test_err = fetch_search(api_key, wo, int(umkreis), "", 365, 25, page=1, arbeitszeit=None)
        st.write(f"Debug-Test ohne Suchtext (365 Tage, {umkreis} km): **{len(test_items)} Treffer**")
        if test_err:
            st.code(test_err)
        if test_items:
            st.write("Erste Treffer (Debug):")
            for t in test_items[:3]:
                st.write("-", item_title(t))

    # ---------- GROSSE KARTE (pydeck) ----------
    map_rows = []
    for it in items_now_sorted[:500]:
        ll = extract_latlon_from_item(it)
        if not ll:
            continue
        lat, lon = ll
        dist = distance_from_home_km(it, float(home_lat), float(home_lon))
        map_rows.append(
            {
                "lat": float(lat),
                "lon": float(lon),
                "title": item_title(it),
                "company": item_company(it),
                "dist_km": float(dist) if dist is not None else 999999.0,
            }
        )

    st.caption(f"📍 Treffer mit Koordinaten: {len(map_rows)} von {len(items_now_sorted)}")

    home_df = pd.DataFrame(
        [{"lat": float(home_lat), "lon": float(home_lon), "title": "Wohnort", "company": "", "dist_km": 0.0}]
    )

    if len(map_rows) > 0:
        st.write("### Karte (Wohnort + nächste Treffer)")
        df_map = pd.DataFrame(map_rows).sort_values("dist_km").head(50)
        df_all = pd.concat([home_df, df_map], ignore_index=True)

        layer = pdk.Layer(
            "ScatterplotLayer",
            data=df_all,
            get_position="[lon, lat]",
            get_radius=350,
            pickable=True,
            auto_highlight=True,
        )

        view_state = pdk.ViewState(
            latitude=float(home_lat),
            longitude=float(home_lon),
            zoom=8.6,
            pitch=0,
        )

        tooltip = {
            "html": "<b>{title}</b><br/>{company}<br/>Dist: {dist_km} km",
            "style": {"backgroundColor": "white", "color": "black"},
        }

        st.pydeck_chart(
            pdk.Deck(layers=[layer], initial_view_state=view_state, tooltip=tooltip),
            use_container_width=True,
        )
        st.caption("Hinweis: Karte zeigt bis zu 50 nächstgelegene Treffer mit Koordinaten (plus Wohnort).")

    st.divider()
    st.write("### Ergebnisse (klick = Details aufklappen)")

    for it in items_now_sorted:
        jid = item_id(it)
        is_new = jid in new_ids
        score = relevance_score(it)

        dist = distance_from_home_km(it, float(home_lat), float(home_lon))
        t_min = travel_time_minutes(dist, float(speed_kmh))
        badge = distance_badge_html(dist, t_min, int(near_km), int(mid_km))

        lead = "⭐ " if looks_leadership(it) else ""
        label = f"{'🟢 NEU  ' if is_new else ''}{lead}{item_title(it)}"

        meta_text = " | ".join(
            [
                f"Score: {score}",
                it.get("_profile", ""),
                it.get("_bucket", ""),
                item_company(it),
                pretty_location(it),
            ]
        )

        with st.expander(label):
            st.markdown(badge + f' <span style="color:#666;">{meta_text}</span>', unsafe_allow_html=True)

            web_url = jobsuche_web_url(it)
            if web_url:
                try:
                    st.link_button("🔗 In BA Jobsuche öffnen", web_url)
                except Exception:
                    st.markdown(f"[🔗 In BA Jobsuche öffnen]({web_url})")

            # Mini-Karte pro Treffer (Wohnort + Jobpunkt)
            ll = extract_latlon_from_item(it)
            if ll:
                lat, lon = ll
                df_pair = pd.DataFrame(
                    [
                        {"lat": float(home_lat), "lon": float(home_lon), "name": "Wohnort"},
                        {"lat": float(lat), "lon": float(lon), "name": "Job"},
                    ]
                )
                st.pydeck_chart(
                    pdk.Deck(
                        layers=[
                            pdk.Layer(
                                "ScatterplotLayer",
                                data=df_pair,
                                get_position="[lon, lat]",
                                get_radius=450,
                                pickable=True,
                            )
                        ],
                        initial_view_state=pdk.ViewState(
                            latitude=float(home_lat),
                            longitude=float(home_lon),
                            zoom=9.0,
                            pitch=0,
                        ),
                        tooltip={"html": "<b>{name}</b>"},
                    ),
                    use_container_width=True,
                )

            api_url = details_url_api(it)
            if not api_url:
                st.info("Keine API-Detail-URL im Suchtreffer vorhanden – zeige Basisinfos aus der Ergebnisliste.")
                basis = {
                    "RefNr": item_id(it),
                    "Titel": item_title(it),
                    "Arbeitgeber": item_company(it),
                    "Ort": pretty_location(it),
                    "Entfernung": f"{dist:.1f} km" if dist is not None else "—",
                    "Fahrzeit (Schätzung)": f"~{t_min} min" if t_min is not None else "—",
                    "Profil": it.get("_profile", ""),
                    "Quelle": it.get("_bucket", ""),
                    "Kurzbeschreibung": short_field(it, "kurzbeschreibung", "beschreibungKurz", "kurztext"),
                    "Veröffentlicht": short_field(it, "veroeffentlichungsdatum", "veroeffentlichtAm", "date"),
                    "Aktualisiert": short_field(it, "aktualisiertAm", "aktualisiert", "updated"),
                }
                basis = {k: v for k, v in basis.items() if v and str(v).strip()}
                st.table([[k, v] for k, v in basis.items()])
                continue

            details, derr = fetch_details(api_key, api_url)
            if derr:
                st.error(derr)
                st.info("Falls Details über die API nicht abrufbar sind, nutze den Link oben zur BA-Webseite.")
                continue
            if not details:
                st.info("Keine Details erhalten.")
                continue

            st.write("**Kurzprofil**")
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Relevanz-Score", score)
            c2.write(f"**Entfernung:** {dist:.1f} km" if dist is not None else "**Entfernung:** —")
            c3.write(f"**Fahrzeit:** ~{t_min} min" if t_min is not None else "**Fahrzeit:** —")
            c4.write(f"**RefNr:** {jid or '—'}")

            st.write("**Rahmendaten**")
            d_arbeitgeber = details.get("arbeitgeber") or details.get("arbeitgeberName") or item_company(it) or "—"
            d_ort = details.get("arbeitsort") or pretty_location(it) or "—"
            st.write(f"- **Arbeitgeber:** {d_arbeitgeber}")
            st.write(f"- **Ort:** {d_ort}")

            desc = (
                details.get("stellenbeschreibung")
                or details.get("beschreibung")
                or details.get("jobbeschreibung")
                or details.get("aufgaben")
                or details.get("anforderungen")
            )

            st.write("**Beschreibung / Aufgaben / Anforderungen**")
            if isinstance(desc, str) and desc.strip():
                st.write(desc)
            else:
                st.info("Keine ausführliche Beschreibung im Detail-Response gefunden. Nutze ggf. den BA-Link oben.")
