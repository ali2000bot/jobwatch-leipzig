import json
import math
import os
import hashlib
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

import requests
import streamlit as st
import streamlit.components.v1 as components
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =========================
# BA Jobsuche (App Endpoint)
# =========================
BASE = "https://rest.arbeitsagentur.de/jobboerse/jobsuche-service"
SEARCH_URL = f"{BASE}/pc/v4/app/jobs"
API_KEY_DEFAULT = "jobboerse-jobsuche"

# =========================
# Snapshot state
# =========================
STATE_DIR = ".jobwatch_state"
SNAPSHOT_FILE = os.path.join(STATE_DIR, "snapshot.json")

# =========================
# Default Wohnort: 06242 Braunsbedra
# =========================
DEFAULT_HOME_LABEL = "06242 Braunsbedra"
DEFAULT_HOME_LAT = 51.2861
DEFAULT_HOME_LON = 11.8900

# =========================
# Keywords (Defaults)
# =========================
DEFAULT_FOCUS_KEYWORDS = [
    "thermoanalyse", "thermophysik", "thermal analysis", "thermophysical",
    "dsc", "tga", "lfa", "dilatometrie", "dilatometer", "sta", "dma", "tma",
    "wärmeleitfähigkeit", "thermal conductivity", "diffusivität", "diffusivity",
    "kalorimetrie", "calorimetry", "cp", "wärmekapazität", "heat capacity",
    "materialcharakterisierung", "material characterization",
    "analytik", "instrumentierung", "messgerät", "labor",
    "werkstoff", "werkstoffe", "polymer", "keramik", "metall",
    "f&e", "forschung", "entwicklung", "r&d", "research", "development",
    "verfahrenstechnik", "thermodynamik", "wärmeübertragung", "kältetechnik", "thermische simulation", "physik", "physics",
    "wärmetechnik",
]
# Für den Score (nicht für ⭐): darf ruhig etwas breiter sein
DEFAULT_LEADERSHIP_KEYWORDS = [
    "laborleiter", "teamleiter", "gruppenleiter", "abteilungsleiter", "bereichsleiter",
    "leiter", "head", "lead", "director", "manager", "principal",
]
DEFAULT_NEGATIVE_KEYWORDS = [
    "insurance", "versicherung",
    "assistant", "assistenz", "sekretariat",
    "office", "backoffice", "reception", "empfang",
    "vorstandsassistenz", "dachdecker", "management assistant",
]

# =========================
# Strategie A / Variante 2:
# Ziel-Organisationen (20) + Karriereseiten (nur Links, kein Scraping)
# =========================
TARGET_ORGS: List[Dict[str, Any]] = [
    # --- Industrie / Chemie / Energie (Region) ---
    {"name": "InfraLeuna", "match": ["infraleuna"], "url": "https://www.infraleuna.de/career"},  # :contentReference[oaicite:1]{index=1}
    {"name": "TotalEnergies / Raffinerie Leuna", "match": ["totalenergies", "raffinerie", "leuna"], "url": "https://jobs.totalenergies.com/de_DE/careers/Home"},
    {"name": "Dow (Schkopau/Böhlen)", "match": ["dow", "olefinverbund"], "url": "https://de.dow.com/de-de/karriere.html"},  # :contentReference[oaicite:2]{index=2}
    {"name": "Trinseo (Schkopau)", "match": ["trinseo"], "url": "https://www.trinseo.com/careers"},
    {"name": "DOMO / Caproleuna", "match": ["domo", "caproleuna"], "url": "https://www.domochemicals.com/de/stellenangebote"},
    {"name": "UPM Biochemicals (Leuna)", "match": ["upm"], "url": "https://www.upmbiochemicals.com/de/karriere/"},
    {"name": "ADDINOL (Leuna)", "match": ["addinol"], "url": "https://addinol.de/unternehmen/karriere/"},
    {"name": "Arkema", "match": ["arkema"], "url": "https://www.arkema.com/germany/de/careers/"},  # :contentReference[oaicite:3]{index=3}
    {"name": "Eastman", "match": ["eastman"], "url": "https://www.eastman.com/en/careers"},  # :contentReference[oaicite:4]{index=4}
    {"name": "Innospec", "match": ["innospec"], "url": "https://www.inno-spec.de/karriere/"},  # :contentReference[oaicite:5]{index=5}
    {"name": "Shell (Catalysts/Leuna)", "match": ["shell", "catalysts"], "url": "https://www.shell.de/ueber-uns/karriere.html"},  # :contentReference[oaicite:6]{index=6}
    {"name": "Linde", "match": ["linde"], "url": "https://de.lindecareers.com/"},
    {"name": "Air Liquide", "match": ["air liquide"], "url": "https://de.airliquide.com/karriere"},
    {"name": "BASF", "match": ["basf"], "url": "https://www.basf.com/global/de/careers"},  # :contentReference[oaicite:7]{index=7}
    {"name": "Wacker Chemie", "match": ["wacker"], "url": "https://www.wacker.com/cms/de-de/careers/overview.html"},  # :contentReference[oaicite:8]{index=8}
    {"name": "Verbio (Leipzig)", "match": ["verbio"], "url": "https://www.verbio.de/karriere/"},  # :contentReference[oaicite:9]{index=9}
    {"name": "VNG (Leipzig)", "match": ["vng"], "url": "https://karriere.vng.de/"},
    {"name": "Siemens Energy", "match": ["siemens energy"], "url": "https://jobs.siemens-energy.com/de_DE/jobs/Jobs"},  # :contentReference[oaicite:10]{index=10}
    {"name": "Siemens (allgemein)", "match": ["siemens"], "url": "https://www.siemens.com/de/de/unternehmen/jobs.html"},  # :contentReference[oaicite:11]{index=11}

    # --- Automotive / Logistik (regional, falls interessant) ---
    {"name": "BMW Werk Leipzig", "match": ["bmw"], "url": "https://www.bmwgroup.jobs/de/de/standorte/werke-in-deutschland/werk-leipzig.html"},
    {"name": "Porsche Leipzig", "match": ["porsche"], "url": "https://www.porsche-leipzig.com/jobs-karriere/"},
    {"name": "DHL Hub Leipzig", "match": ["dhl", "deutsche post", "hub leipzig"], "url": "https://www.dhl.com/de-de/microsites/express/hubs/hub-leipzig/jobs.html"},  # :contentReference[oaicite:12]{index=12}
    {"name": "Mitteldeutsche Flughafen AG (LEJ)", "match": ["mitteldeutsche flughafen", "flughafen leipzig", "leipzig/halle", "leipzig-halle"], "url": "https://www.mdf-ag.com/karriere/alle-jobs/flughafen-leipzig-halle"},  # :contentReference[oaicite:13]{index=13}

    # --- Regionale Infrastruktur/Versorger ---
    {"name": "Stadtwerke Leipzig / Leipziger Gruppe", "match": ["stadtwerke leipzig", "leipziger gruppe", "l-gruppe", "l.de"], "url": "https://www.l.de/karriere/stellenangebote/"},  # :contentReference[oaicite:14]{index=14}
    {"name": "enviaM-Gruppe", "match": ["enviam", "envia", "mitgas"], "url": "https://jobs.enviam-gruppe.de/"},  # :contentReference[oaicite:15]{index=15}

    # --- Analytik / Prüfen / Zertifizieren ---
    {"name": "GBA Group", "match": ["gba"], "url": "https://www.gba-group.com/karriere/jobs/"},  # :contentReference[oaicite:16]{index=16}
    {"name": "Eurofins", "match": ["eurofins"], "url": "https://careers.eurofins.com/de"},  # :contentReference[oaicite:17]{index=17}
    {"name": "SGS", "match": ["sgs"], "url": "https://www.sgs.com/de-de/unternehmen/karriere-bei-sgs/stellenangebote"},  # :contentReference[oaicite:18]{index=18}
    {"name": "DEKRA", "match": ["dekra"], "url": "https://www.dekra.de/de/karriere/ueberblick/"},  # :contentReference[oaicite:19]{index=19}

    # --- Forschung / Institute / Uni / HAW ---
    {"name": "UFZ Helmholtz (Leipzig)", "match": ["ufz", "helmholtz-zentrum", "umweltforschung"], "url": "https://www.ufz.de/index.php?de=34275"},  # :contentReference[oaicite:20]{index=20}
    {"name": "DBFZ Leipzig", "match": ["dbfz"], "url": "https://www.dbfz.de/karriere/stellenausschreibungen"},
    {"name": "Fraunhofer IZI (Leipzig)", "match": ["fraunhofer izi", "izi"], "url": "https://www.izi.fraunhofer.de/de/jobs-karriere.html"},  # :contentReference[oaicite:21]{index=21}
    {"name": "Fraunhofer (Jobportal)", "match": ["fraunhofer"], "url": "https://www.fraunhofer.de/de/jobs-und-karriere.html"},  # :contentReference[oaicite:22]{index=22}
    {"name": "Leibniz IOM (Leipzig)", "match": ["iom", "oberflächenmodifizierung", "leibniz"], "url": "https://www.leibniz-gemeinschaft.de/karriere/stellenportal/"},  # :contentReference[oaicite:23]{index=23}
    {"name": "Leibniz IPB (Halle)", "match": ["ipb", "pflanzenbiochemie", "leibniz"], "url": "https://www.leibniz-gemeinschaft.de/karriere/stellenportal/"},  # :contentReference[oaicite:24]{index=24}
    {"name": "Max-Planck-Gesellschaft (Stellenbörse)", "match": ["max-planck", "max planck", "mpg"], "url": "https://www.mpg.de/stellenboerse"},  # :contentReference[oaicite:25]{index=25}
    {"name": "Universität Leipzig", "match": ["universität leipzig", "uni leipzig"], "url": "https://www.uni-leipzig.de/universitaet/arbeiten-an-der-universitaet-leipzig/stellenausschreibungen"},
    {"name": "MLU Halle", "match": ["martin-luther-universität", "universität halle", "uni halle", "mlu"], "url": "https://personal.verwaltung.uni-halle.de/jobs/"},
    {"name": "Hochschule Merseburg", "match": ["hochschule merseburg", "hs merseburg"], "url": "https://www.hs-merseburg.de/hochschule/information/stellenausschreibungen/"},
    {"name": "HTWK Leipzig", "match": ["htwk"], "url": "https://www.htwk-leipzig.de/hochschule/stellenangebote"},
]

# -------------------- Snapshot helpers --------------------
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


# -------------------- BA API helpers --------------------
def headers(api_key: str) -> Dict[str, str]:
    return {
        "User-Agent": "Jobsuche/2.9.2 (de.arbeitsagentur.jobboerse; build:1077) Streamlit",
        "X-API-Key": api_key,
        "Accept": "application/json",
        "Connection": "keep-alive",
    }


def extract_items(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if isinstance(data.get("stellenangebote"), list):
        return data["stellenangebote"]
    emb = data.get("_embedded") or {}
    if isinstance(emb, dict) and isinstance(emb.get("jobs"), list):
        return emb["jobs"]
    if isinstance(data.get("jobs"), list):
        return data["jobs"]
    return []


def item_id_raw(it: Dict[str, Any]) -> str:
    v = it.get("refnr") or it.get("refNr") or it.get("hashId") or it.get("hashID") or ""
    return str(v).strip() if v is not None else ""


def item_title(it: Dict[str, Any]) -> str:
    v = it.get("titel") or it.get("beruf") or it.get("title") or "Ohne Titel"
    return str(v)


def item_company(it: Dict[str, Any]) -> str:
    v = it.get("arbeitgeber") or it.get("arbeitgeberName") or it.get("unternehmen") or ""
    return str(v)


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


def item_key(it: Dict[str, Any]) -> str:
    rid = item_id_raw(it)
    if rid:
        return f"ba:{rid}"
    base = (item_title(it) + "|" + item_company(it) + "|" + pretty_location(it)).strip().lower()
    h = hashlib.sha1(base.encode("utf-8", errors="ignore")).hexdigest()[:16]
    return f"hx:{h}"


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
    rid = item_id_raw(it)
    if not rid:
        return None
    return f"https://www.arbeitsagentur.de/jobsuche/jobdetail/{rid}"


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
        "angebotsart": "1",
        "page": str(page),
        "pav": "false",
        "size": str(size),
        "umkreis": str(umkreis_km),
        "aktualitaet": str(aktualitaet_tage),
        "wo": wo,
    }
    if was and was.strip():
        params["was"] = was.strip()
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
        return [], f"Suche HTTP {r.status_code}: {r.text[:400]}"

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
        return None, f"Details HTTP {r.status_code}: {r.text[:400]}"

    try:
        return r.json(), None
    except Exception:
        return None, "Details: Antwort war kein gültiges JSON."


# -------------------- Distance + travel time --------------------
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
    return haversine_km(home_lat, home_lon, ll[0], ll[1])


def travel_time_minutes(distance_km: Optional[float], speed_kmh: float) -> Optional[int]:
    if distance_km is None or speed_kmh <= 0:
        return None
    minutes = int(round((distance_km / speed_kmh) * 60))
    return max(0, minutes)


def distance_bucket(dist_km: Optional[float], near_km: int, mid_km: int) -> str:
    if dist_km is None:
        return "na"
    if dist_km <= near_km:
        return "green"
    if dist_km <= mid_km:
        return "yellow"
    return "red"


def distance_emoji(bucket: str) -> str:
    return {"green": "🟩", "yellow": "🟨", "red": "🟥", "na": "⬜"}.get(bucket, "⬜")


def distance_badge_html(dist_km: Optional[float], t_min: Optional[int], near_km: int, mid_km: int) -> str:
    if dist_km is None:
        return '<span style="background:#999;color:white;padding:2px 8px;border-radius:999px;font-size:12px;">Entf.: —</span>'
    if dist_km <= near_km:
        bg = "#2e7d32"
    elif dist_km <= mid_km:
        bg = "#f9a825"
    else:
        bg = "#c62828"
    t_part = f" · ~{t_min} min" if t_min is not None else ""
    txt = f"{dist_km:.1f} km{t_part}"
    return f'<span style="background:{bg};color:white;padding:2px 8px;border-radius:999px;font-size:12px;">{txt}</span>'


def google_directions_url(origin_lat: float, origin_lon: float, dest_lat: float, dest_lon: float) -> str:
    return (
        "https://www.google.com/maps/dir/?api=1"
        f"&origin={origin_lat}%2C{origin_lon}"
        f"&destination={dest_lat}%2C{dest_lon}"
        "&travelmode=driving"
    )


# -------------------- Keyword helpers --------------------
def parse_keywords(text: str) -> List[str]:
    raw: List[str] = []
    for line in text.splitlines():
        raw.extend([p.strip() for p in line.split(",")])
    return [x for x in raw if x]


def keywords_to_text(words: List[str]) -> str:
    return "\n".join(words)


# -------------------- Profile Queries --------------------
def build_queries() -> Dict[str, str]:
    q_rd = "Forschung Entwicklung R&D Thermoanalyse Thermophysik Analytik DSC TGA LFA"
    q_pm = "Projektmanagement Project Manager Program Manager Teamleiter Leiter"
    q_sales = "Vertrieb Sales Business Development Key Account Manager"
    return {"R&D": q_rd, "Projektmanagement": q_pm, "Vertrieb": q_sales}


# -------------------- Ziel-Org Matching --------------------
def match_target_org(company: str) -> Optional[Dict[str, Any]]:
    c = (company or "").lower()
    if not c.strip():
        return None
    for org in TARGET_ORGS:
        if any(m in c for m in org.get("match", [])):
            return org
    return None


# -------------------- Leaflet map: numbered pins --------------------
def leaflet_map_html(
    home_lat: float,
    home_lon: float,
    home_label: str,
    markers: List[Dict[str, Any]],
    height_px: int = 520,
) -> str:
    markers_json = json.dumps(markers, ensure_ascii=False)

    return f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
    integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY=" crossorigin=""/>
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
    integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo=" crossorigin=""></script>
  <style>
    html, body {{ margin:0; padding:0; }}
    #map {{ height: {height_px}px; width: 100%; }}

    .pinwrap {{
      position: relative;
      width: 28px;
      height: 42px;
      transform: translate(-14px, -40px);
    }}
    .pinsvg {{
      width: 28px;
      height: 42px;
      display: block;
    }}
    .pinnum {{
      position: absolute;
      top: 9px;
      left: 0;
      width: 28px;
      text-align: center;
      font-weight: 800;
      font-size: 12px;
      color: #111;
      text-shadow: 0 1px 0 rgba(255,255,255,0.85);
      pointer-events: none;
    }}
  </style>
</head>
<body>
<div id="map"></div>
<script>
  const markers = {markers_json};

  function pinSvg(color) {{
    return `
      <svg class="pinsvg" viewBox="0 0 26 38" xmlns="http://www.w3.org/2000/svg">
        <path d="M13 0C5.8 0 0 5.8 0 13c0 10.2 13 25 13 25s13-14.8 13-25C26 5.8 20.2 0 13 0z" fill="${{color}}"/>
        <circle cx="13" cy="13" r="5.2" fill="white" opacity="0.95"/>
      </svg>
    `;
  }}

  function numberedIcon(color, numText) {{
    const html = `<div class="pinwrap">${{pinSvg(color)}}<div class="pinnum">${{numText}}</div></div>`;
    return L.divIcon({{
      className: '',
      html: html,
      iconSize: [28, 42],
      iconAnchor: [14, 40],
      popupAnchor: [0, -40]
    }});
  }}

  const map = L.map('map');
  L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
    maxZoom: 19,
    attribution: '&copy; OpenStreetMap contributors'
  }}).addTo(map);

  const fg = L.featureGroup().addTo(map);

  const homeIcon = numberedIcon("#1565c0", "");
  const homeMarker = L.marker([{home_lat}, {home_lon}], {{icon: homeIcon}}).addTo(fg);
  homeMarker.bindPopup("<b>Wohnort</b><br/>{home_label}");

  markers.forEach(m => {{
    const lat = m.lat, lon = m.lon;
    const title = (m.title || '').replace(/</g,'&lt;');
    const company = (m.company || '').replace(/</g,'&lt;');
    const dist = (m.dist_km != null) ? (Math.round(m.dist_km*10)/10) : null;
    const idx = m.idx || '';

    let color = "#c62828";
    if (m.pin === "green") color = "#2e7d32";
    if (m.pin === "yellow") color = "#f9a825";

    const icon = numberedIcon(color, idx);

    const popup =
      '<b>' + idx + ') ' + title + '</b><br/>' + company
      + (dist!=null ? '<br/>Dist: ' + dist + ' km' : '');

    L.marker([lat, lon], {{icon: icon}}).addTo(fg).bindPopup(popup);
  }});

  if (fg.getLayers().length > 1) {{
    map.fitBounds(fg.getBounds().pad(0.18));
  }} else {{
    map.setView([{home_lat}, {home_lon}], 10);
  }}
</script>
</body>
</html>
"""


# =========================
# Streamlit App
# =========================
st.set_page_config(page_title="JobWatch Leipzig", layout="wide")
st.title("Raum Leipzig – Jobs finden & vergleichen")

# Keyword Session defaults
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
    wo = st.text_input("Ort (BA-Suche)", value="Merseburg")
    umkreis = st.selectbox("Umkreis vor Ort (km)", [25, 40, 50], index=1)

    include_ho = st.checkbox("Homeoffice/Telearbeit extra berücksichtigen", value=True)
    ho_umkreis = st.slider("Umkreis Homeoffice (km)", 50, 200, 200, 25)

    # Homeoffice-Bonus
    ho_bonus = st.slider("Homeoffice-Bonus (Score)", 0, 30, 8, 1)

    # Score-Details
    show_score_breakdown = st.checkbox("Score-Aufschlüsselung anzeigen", value=True)

    queries = build_queries()
    selected_profiles = st.multiselect("Profile", list(queries.keys()), default=list(queries.keys()))

    aktualitaet = st.slider("Nur Jobs der letzten X Tage", 0, 365, 60, 5)
    size = st.selectbox("Treffer pro Seite", [25, 50, 100], index=1)

    st.divider()
    api_key = st.text_input("X-API-Key", value=API_KEY_DEFAULT)

    st.subheader("Profil-Filter")
    only_focus = st.checkbox("Nur profilrelevante Treffer anzeigen", value=True)
    hide_irrelevant = st.checkbox("Assistenzen/Office/Insurance ausblenden", value=True)
    min_score = st.slider("Mindest-Score", 0, 80, 8, 1)

    # Ziel-Organisationen (Variante 2)
    st.divider()
    st.subheader("Ziel-Organisationen (20)")
    st.caption("Variante 2: keine zusätzlichen Jobbörsen abfragen – nur BA + direkte Karrierelinks.")
    with st.expander("Liste anzeigen / öffnen", expanded=False):
        for org in TARGET_ORGS:
            try:
                st.link_button(f"🏢 {org['name']}", org["url"])
            except Exception:
                st.markdown(f"[🏢 {org['name']}]({org['url']})")

    st.divider()
    st.subheader("Keywords (sichtbar & editierbar)")
    with st.expander("Fokus-Keywords", expanded=False):
        st.session_state["kw_focus"] = st.text_area(
            "Ein Begriff pro Zeile (oder Komma-getrennt)",
            value=st.session_state["kw_focus"],
            height=150,
        )
    with st.expander("Leitung/Führung-Keywords (für Score)", expanded=False):
        st.session_state["kw_lead"] = st.text_area(
            "Ein Begriff pro Zeile (oder Komma-getrennt)",
            value=st.session_state["kw_lead"],
            height=110,
        )
    with st.expander("Negative Keywords", expanded=False):
        st.session_state["kw_neg"] = st.text_area(
            "Ein Begriff pro Zeile (oder Komma-getrennt)",
            value=st.session_state["kw_neg"],
            height=110,
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

FOCUS_KEYWORDS = [k.lower() for k in parse_keywords(st.session_state["kw_focus"])]
LEADERSHIP_KEYWORDS = [k.lower() for k in parse_keywords(st.session_state["kw_lead"])]
NEGATIVE_KEYWORDS = [k.lower() for k in parse_keywords(st.session_state["kw_neg"])]

col1, col2 = st.columns([8, 1], gap="large")

with col2:
    snap = load_snapshot()
    st.subheader("Snapshot")
    st.write(snap.get("timestamp") or "— noch keiner gespeichert")

    if st.button("Stand speichern"):
        st.session_state["save_snapshot_requested"] = True

    if st.button("Stand löschen"):
        ensure_state_dir()
        if os.path.exists(SNAPSHOT_FILE):
            os.remove(SNAPSHOT_FILE)
        st.success("Snapshot gelöscht. Seite neu laden.")


# =========================
# ⭐ Strengere Stern-Logik (NICHT jeder "Manager")
# =========================
def looks_leadership_strict(it: Dict[str, Any]) -> bool:
    text = " ".join(
        [
            str(item_title(it)),
            str(it.get("kurzbeschreibung", "")),
        ]
    ).lower()

    # harte Leitungsbegriffe
    strict_terms = [
        "laborleiter",
        "teamleiter",
        "gruppenleiter",
        "abteilungsleiter",
        "bereichsleiter",
        " head of ",
        "director",
    ]

    # "leiter" nur als eigenes Wort
    if " leiter " in f" {text} ":
        return True

    return any(term in text for term in strict_terms)


def is_homeoffice_item(it: Dict[str, Any]) -> bool:
    b = str(it.get("_bucket", "")).lower()
    if "homeoffice" in b:
        return True
    az = str(it.get("arbeitszeit", "")).lower()
    return az == "ho"


def score_breakdown(it: Dict[str, Any], ho_bonus_val: int) -> Tuple[int, List[str]]:
    """
    Liefert (score, details) für Transparenz.
    """
    text = " ".join(
        [
            str(item_title(it)),
            str(it.get("kurzbeschreibung", "")),
            str(item_company(it)),
            str(pretty_location(it)),
        ]
    ).lower()

    score = 0
    parts: List[str] = []

    for k in FOCUS_KEYWORDS:
        if k and k in text:
            score += 10
            parts.append(f"+10 {k}")

    for k in LEADERSHIP_KEYWORDS:
        if k and k in text:
            score += 6
            parts.append(f"+6 {k}")

    for k in NEGATIVE_KEYWORDS:
        if k and k in text:
            score -= 12
            parts.append(f"−12 {k}")

    if is_homeoffice_item(it) and ho_bonus_val > 0:
        score += int(ho_bonus_val)
        parts.append(f"+{int(ho_bonus_val)} homeoffice")

    if not parts:
        parts = ["(keine Keyword-Treffer)"]

    return score, parts


def relevance_score(it: Dict[str, Any], ho_bonus_val: int) -> int:
    s, _ = score_breakdown(it, ho_bonus_val)
    return s


def is_probably_irrelevant(it: Dict[str, Any]) -> bool:
    text = f"{item_title(it)} {it.get('kurzbeschreibung','')}".lower()
    hard = [
        "vorstandsassistenz", "management assistant",
        "assistant", "assistenz", "sekretariat",
        "insurance", "versicherung",
    ]
    return any(h in text for h in hard)


def render_fact_grid(rows: List[Tuple[str, str]]) -> None:
    rows = [(k, v) for (k, v) in rows if v is not None and str(v).strip() != ""]
    if not rows:
        return
    n = len(rows)
    half = (n + 1) // 2
    left = rows[:half]
    right = rows[half:]

    c1, c2 = st.columns(2)
    with c1:
        for k, v in left:
            st.markdown(f"**{k}:** {v}")
    with c2:
        for k, v in right:
            st.markdown(f"**{k}:** {v}")


with col1:
    if not selected_profiles:
        st.warning("Bitte mindestens ein Profil auswählen.")
        st.stop()

    with st.spinner("Suche läuft…"):
        all_items: List[Dict[str, Any]] = []
        errs: List[str] = []

        queries = build_queries()

        for name in selected_profiles:
            q = queries[name]

            items_local, e1 = fetch_search(api_key, wo, int(umkreis), q, int(aktualitaet), int(size), page=1, arbeitszeit=None)
            if e1:
                errs.append(f"{name} (vor Ort): {e1}")
            for it in items_local:
                it["_profile"] = name
                it["_bucket"] = f"Vor Ort ({umkreis} km)"
            all_items.extend(items_local)

            if include_ho:
                items_ho, e2 = fetch_search(api_key, wo, int(ho_umkreis), q, int(aktualitaet), int(size), page=1, arbeitszeit="ho")
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

    # Dedup nach robuster ID
    items_now: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    for it in all_items:
        k = item_key(it)
        if k not in seen:
            seen.add(k)
            it["_key"] = k
            items_now.append(it)

    # Filter
    if hide_irrelevant:
        items_now = [it for it in items_now if not is_probably_irrelevant(it)]
    if only_focus:
        items_now = [it for it in items_now if relevance_score(it, int(ho_bonus)) >= int(min_score)]

    # Snapshot compare (nutze _key)
    prev_items = snap.get("items", [])
    prev_keys: Set[str] = {x.get("_key") or item_key(x) for x in prev_items}
    now_keys: Set[str] = {x.get("_key") or item_key(x) for x in items_now}
    new_keys = now_keys - prev_keys

    # Sort: Entfernung -> neu -> Score
    def sort_key(it: Dict[str, Any]):
        dist = distance_from_home_km(it, float(home_lat), float(home_lon))
        dist_rank = dist if dist is not None else 999999.0
        is_new_rank = 0 if (it.get("_key") in new_keys) else 1
        score = relevance_score(it, int(ho_bonus))
        return (dist_rank, is_new_rank, -score, item_title(it).lower())

    items_sorted = sorted(items_now, key=sort_key)

    # Nummerierung: IMMER (1..N)
    for i, it in enumerate(items_sorted, start=1):
        it["_idx"] = i

    st.subheader(f"Treffer: {len(items_sorted)}")
    st.caption(f"Neu seit Snapshot: {len(new_keys)}")

    if st.session_state.get("save_snapshot_requested"):
        save_snapshot(items_sorted)
        st.session_state["save_snapshot_requested"] = False
        st.success("Snapshot gespeichert.")

    # Marker bauen (nur Treffer mit Koordinaten)
    markers: List[Dict[str, Any]] = []
    for it in items_sorted:
        ll = extract_latlon_from_item(it)
        if not ll:
            continue
        dist = distance_from_home_km(it, float(home_lat), float(home_lon))
        d = float(dist) if dist is not None else None
        bucket = distance_bucket(d, int(near_km), int(mid_km))

        markers.append(
            {
                "idx": it.get("_idx", ""),
                "lat": float(ll[0]),
                "lon": float(ll[1]),
                "title": item_title(it),
                "company": item_company(it),
                "dist_km": d,
                "pin": bucket,
            }
        )

    if debug:
        st.info(
            f"Debug: items_sorted={len(items_sorted)} | marker={len(markers)} | ho_bonus={ho_bonus} | target_orgs={len(TARGET_ORGS)}"
        )

    if markers:
        st.write("### Karte")
        components.html(
            leaflet_map_html(float(home_lat), float(home_lon), home_label, markers[:80], height_px=520),
            height=560,
        )

    st.divider()
    st.write("### Ergebnisse")

    for it in items_sorted:
        idx = int(it.get("_idx", 0) or 0)
        k = it.get("_key") or item_key(it)
        is_new = (k in new_keys)

        score, parts = score_breakdown(it, int(ho_bonus))

        dist = distance_from_home_km(it, float(home_lat), float(home_lon))
        t_min = travel_time_minutes(dist, float(speed_kmh))
        bucket = distance_bucket(dist, int(near_km), int(mid_km))
        emo = distance_emoji(bucket)

        # ⭐ (streng)
        star = "⭐ " if looks_leadership_strict(it) else ""

        # Homeoffice icon
        ho_tag = " 🏠" if is_homeoffice_item(it) else ""

        # Zielorganisation marker
        org = match_target_org(item_company(it))
        target_tag = " 🎯" if org else ""

        num_txt = f"{idx:02d}" if idx > 0 else "??"
        dist_txt = f"{dist:.1f} km" if dist is not None else "— km"

        label = f"{'🟢 ' if is_new else ''}{emo} {num_txt} · {dist_txt} · {star}{item_title(it)}{ho_tag}{target_tag}"

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
            badge = distance_badge_html(dist, t_min, int(near_km), int(mid_km))
            st.markdown(badge + f' <span style="color:#666;">{meta_text}</span>', unsafe_allow_html=True)

            rid = item_id_raw(it) or "—"
            facts = [
                ("Nr.", num_txt),
                ("Distanz", dist_txt),
                ("Fahrzeit (Schätzung)", f"~{t_min} min" if t_min is not None else "—"),
                ("Homeoffice", "Ja (Bonus aktiv)" if is_homeoffice_item(it) else "—"),
                ("Ziel-Organisation", org["name"] if org else "—"),
                ("Arbeitgeber", item_company(it) or "—"),
                ("Ort", pretty_location(it)),
                ("Profil", it.get("_profile", "")),
                ("Quelle", it.get("_bucket", "")),
                ("Score", str(score)),
                ("RefNr/BA-ID", rid),
            ]
            render_fact_grid(facts)

            # Karriereseite (Variante 2)
            if org:
                st.write("**Karriereseite (Ziel-Organisation)**")
                try:
                    st.link_button("🏢 Karriereseite öffnen", org["url"])
                except Exception:
                    st.markdown(f"[🏢 Karriereseite öffnen]({org['url']})")

            # Score-Aufschlüsselung
            if show_score_breakdown:
                st.write("**Score-Aufschlüsselung**")
                st.code(" | ".join(parts))

            # BA Web Link + Route
            web_url = jobsuche_web_url(it)
            ll = extract_latlon_from_item(it)
            if web_url or ll:
                cL, cR = st.columns(2)
                with cL:
                    if web_url:
                        try:
                            st.link_button("🔗 In BA Jobsuche öffnen", web_url)
                        except Exception:
                            st.markdown(f"[🔗 In BA Jobsuche öffnen]({web_url})")
                with cR:
                    if ll:
                        gdir = google_directions_url(float(home_lat), float(home_lon), float(ll[0]), float(ll[1]))
                        try:
                            st.link_button("🚗 Route in Google Maps", gdir)
                        except Exception:
                            st.markdown(f"[🚗 Route in Google Maps]({gdir})")

            st.divider()

            # Details (wenn verfügbar)
            api_url = details_url_api(it)
            if not api_url:
                st.info("Keine API-Detail-URL im Suchtreffer vorhanden – Basisinfos aus Ergebnisliste.")
                kurz = short_field(it, "kurzbeschreibung", "beschreibungKurz", "kurztext")
                st.write("**Kurzbeschreibung**")
                st.write(kurz if kurz else "—")
                continue

            details, derr = fetch_details(api_key, api_url)
            if derr:
                st.error(derr)
                st.info("Wenn Details per API nicht gehen: nutze den BA-Link oben.")
                continue
            if not details:
                st.info("Keine Details erhalten.")
                continue

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
