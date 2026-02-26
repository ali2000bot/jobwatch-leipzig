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
# Snapshot + Firmencheck state
# =========================
STATE_DIR = ".jobwatch_state"
SNAPSHOT_FILE = os.path.join(STATE_DIR, "snapshot.json")
COMPANY_STATE_FILE = os.path.join(STATE_DIR, "company_monitor.json")


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


# -------------------- Firmencheck (manuell, pro Firma) helpers --------------------
def load_company_state() -> Dict[str, Any]:
    if not os.path.exists(COMPANY_STATE_FILE):
        return {}
    with open(COMPANY_STATE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_company_state(state: Dict[str, Any]) -> None:
    ensure_state_dir()
    with open(COMPANY_STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


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
    "verfahrenstechnik", "thermodynamik", "wärmeübertragung", "kältetechnik",
    "thermische simulation", "physik", "physics",
    "wärmetechnik", "brandprüfung", "laser flash",
]
DEFAULT_LEADERSHIP_KEYWORDS = [
    "laborleiter", "teamleiter", "gruppenleiter", "abteilungsleiter", "bereichsleiter",
    "leiter", "head", "lead", "director", "manager", "principal",
    "betriebsstättenleiter", "standortleiter",
    "sektionsleiter", "section manager",
]
DEFAULT_NEGATIVE_KEYWORDS = [
    "insurance", "versicherung",
    "assistant", "assistenz", "sekretariat",
    "office", "backoffice", "reception", "empfang",
    "vorstandsassistenz", "dachdecker", "management assistant",
]

# =========================
# Ziel-Organisationen (BEREINIGT, ohne Dubletten) + High Priority
# =========================
TARGET_ORGS: List[Dict[str, Any]] = [
    # --- Industrie / Chemie / Energie (Region) ---
    {"name": "InfraLeuna", "match": ["infraleuna"], "url": "https://www.infraleuna.de/career"},
    {"name": "TotalEnergies / Raffinerie Leuna", "match": ["totalenergies", "raffinerie", "leuna"], "url": "https://jobs.totalenergies.com/de_DE/careers/Home"},
    {"name": "Dow (Schkopau/Böhlen)", "match": ["dow", "olefinverbund"], "url": "https://de.dow.com/de-de/karriere.html"},
    {"name": "Trinseo (Schkopau)", "match": ["trinseo"], "url": "https://www.trinseo.com/careers"},
    {"name": "DOMO / Caproleuna", "match": ["domo", "caproleuna"], "url": "https://www.domochemicals.com/de/stellenangebote"},
    {"name": "UPM Biochemicals (Leuna)", "match": ["upm"], "url": "https://www.upmbiochemicals.com/de/karriere/"},
    {"name": "ADDINOL (Leuna)", "match": ["addinol"], "url": "https://addinol.de/unternehmen/karriere/"},
    {"name": "Arkema", "match": ["arkema"], "url": "https://www.arkema.com/germany/de/careers/"},
    {"name": "Eastman", "match": ["eastman"], "url": "https://www.eastman.com/en/careers"},
    {"name": "Innospec", "match": ["innospec"], "url": "https://www.inno-spec.de/karriere/"},
    {"name": "Shell (Catalysts/Leuna)", "match": ["shell", "catalysts"], "url": "https://www.shell.de/ueber-uns/karriere.html"},
    {"name": "Linde", "match": ["linde"], "url": "https://de.lindecareers.com/"},
    {"name": "Air Liquide", "match": ["air liquide"], "url": "https://de.airliquide.com/karriere"},
    {"name": "BASF", "match": ["basf"], "url": "https://www.basf.com/global/de/careers"},
    {"name": "Wacker Chemie", "match": ["wacker"], "url": "https://www.wacker.com/cms/de-de/careers/overview.html"},
    {"name": "Verbio (Leipzig)", "match": ["verbio"], "url": "https://www.verbio.de/karriere/"},
    {"name": "VNG (Leipzig)", "match": ["vng"], "url": "https://karriere.vng.de/"},
    {"name": "Siemens Energy", "match": ["siemens energy"], "url": "https://jobs.siemens-energy.com/de_DE/jobs/Jobs"},
    {"name": "Siemens (allgemein)", "match": ["siemens"], "url": "https://www.siemens.com/de/de/unternehmen/jobs.html"},

    # --- Automotive / Logistik (regional) ---
    {"name": "BMW Werk Leipzig", "match": ["bmw"], "url": "https://www.bmwgroup.jobs/de/de/standorte/werke-in-deutschland/werk-leipzig.html"},
    {"name": "Porsche Leipzig", "match": ["porsche"], "url": "https://www.porsche-leipzig.com/jobs-karriere/"},
    {"name": "DHL Hub Leipzig", "match": ["dhl", "deutsche post", "hub leipzig"], "url": "https://www.dhl.com/de-de/microsites/express/hubs/hub-leipzig/jobs.html"},
    {"name": "Mitteldeutsche Flughafen AG (LEJ)", "match": ["mitteldeutsche flughafen", "flughafen leipzig", "leipzig/halle", "leipzig-halle"], "url": "https://www.mdf-ag.com/karriere/alle-jobs/flughafen-leipzig-halle"},

    # --- Regionale Infrastruktur/Versorger ---
    {"name": "Stadtwerke Leipzig / Leipziger Gruppe", "match": ["stadtwerke leipzig", "leipziger gruppe", "l-gruppe", "l.de"], "url": "https://www.l.de/karriere/stellenangebote/"},
    {"name": "enviaM-Gruppe", "match": ["enviam", "envia", "mitgas"], "url": "https://jobs.enviam-gruppe.de/"},

    # --- Analytik / Prüfen / Zertifizieren ---
    {"name": "GBA Group", "match": ["gba"], "url": "https://www.gba-group.com/karriere/jobs/"},
    {"name": "Eurofins", "match": ["eurofins"], "url": "https://careers.eurofins.com/de"},
    {"name": "SGS", "match": ["sgs"], "url": "https://www.sgs.com/de-de/unternehmen/karriere-bei-sgs/stellenangebote"},
    {"name": "DEKRA", "match": ["dekra"], "url": "https://www.dekra.de/de/karriere/ueberblick/"},

    # --- Forschung / Institute / Uni / HAW ---
    {"name": "UFZ Helmholtz (Leipzig)", "match": ["ufz", "helmholtz-zentrum", "umweltforschung"], "url": "https://www.ufz.de/index.php?de=34275"},
    {"name": "DBFZ Leipzig", "match": ["dbfz", "deutsches biomasseforschungszentrum"], "url": "https://www.dbfz.de/karriere/stellenausschreibungen", "priority": "high"},
    {"name": "Fraunhofer IZI (Leipzig)", "match": ["fraunhofer izi", "izi"], "url": "https://www.izi.fraunhofer.de/de/jobs-karriere.html"},
    {"name": "Fraunhofer IMWS (Halle)", "match": ["fraunhofer imws", "imws", "mikrostruktur", "mikrostruktur von werkstoffen", "halle (saale)"], "url": "https://www.imws.fraunhofer.de/de/schnelleinstieg-fuer-bewerber/jobs-am-imws.html", "priority": "high"},
    {"name": "Fraunhofer (Jobportal)", "match": ["fraunhofer"], "url": "https://www.fraunhofer.de/de/jobs-und-karriere.html"},

    {"name": "Leibniz-Institut für Oberflächenmodifizierung (IOM)", "match": ["leibniz iom", "iom leipzig", "oberflächenmodifizierung", "iom"], "url": "https://www.iom-leipzig.de/de/karriere/", "priority": "high"},
    {"name": "Leibniz IPB (Halle)", "match": ["ipb", "pflanzenbiochemie", "leibniz"], "url": "https://www.ipb-halle.de/karriere/stellenangebote/"},

    {"name": "Max-Planck-Gesellschaft (Stellenbörse)", "match": ["max-planck", "max planck", "mpg"], "url": "https://www.mpg.de/stellenboerse"},
    {"name": "Universität Leipzig (Stellen)", "match": ["universität leipzig", "uni leipzig"], "url": "https://www.uni-leipzig.de/universitaet/arbeiten-an-der-universitaet-leipzig/stellenausschreibungen"},
    {"name": "MLU Halle (Stellen)", "match": ["martin-luther-universität", "universität halle", "uni halle", "mlu"], "url": "https://personal.verwaltung.uni-halle.de/jobs/"},
    {"name": "Hochschule Merseburg", "match": ["hochschule merseburg", "hs merseburg"], "url": "https://www.hs-merseburg.de/hochschule/information/stellenausschreibungen/"},
    {"name": "HTWK Leipzig (Stellen)", "match": ["htwk"], "url": "https://www.htwk-leipzig.de/hochschule/stellenangebote"},

    # --- Materialprüfung / Wärmeleitfähigkeit / Thermophysik ---
    {"name": "MFPA Leipzig GmbH", "match": ["mfpa leipzig", "mfpa", "prüfanstalt", "materialforschungs"], "url": "https://www.mfpa-leipzig.de/", "priority": "high"},
    {"name": "MFPA Leipzig – Wärmeleitfähigkeit (Service)", "match": ["mfpa leipzig", "mfpa"], "url": "https://www.mfpa-leipzig.de/service/pruefung-der-waermeleitfaehigkeit-von-daemmstoffen/"},
    {"name": "Universität Leipzig – Technische Chemie (Equipment)", "match": ["institut für technische chemie", "technical chemistry", "universität leipzig", "uni leipzig"], "url": "https://www.chemie.uni-leipzig.de/en/institute-of-chemical-technology/technical-equipment"},
    {"name": "MLU Halle – Thermal analysis (Geo/MinGeo)", "match": ["geo.uni-halle", "thermalanalysis", "mlu", "uni halle", "martin-luther"], "url": "https://geo.uni-halle.de/en/mingeochem/laboratories/thermalanalysis/"},
    {"name": "HTWK Leipzig – MNZ Werkstoffprüfung", "match": ["mnz", "htwk"], "url": "https://mnz.htwk-leipzig.de/forschung/analytisches-zentrum/analysemethoden/werkstoffpruefung/"},
    {"name": "TZO Leipzig – Labor Umwelterprobung & Werkstoffprüfung", "match": ["tzo leipzig", "luw"], "url": "https://tzoleipzig.de/labor-fuer-umwelterprobung/"},
]


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
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" crossorigin=""/>
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js" crossorigin=""></script>
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

    include_ho = st.checkbox("Homeoffice/Telearbeit extra berücksichtigen", value=False)
    ho_umkreis = st.slider("Umkreis Homeoffice (km)", 50, 200, 100, 25)

    ho_bonus = st.slider("Homeoffice-Bonus (Score)", 0, 30, 8, 1)
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

    st.divider()
    st.subheader(f"Ziel-Organisationen ({len(TARGET_ORGS)})")
    st.caption("Direkte Karrierelinks (manueller Check im Firmencheck-Tab).")
    with st.expander("Liste anzeigen / öffnen", expanded=False):
        for org in TARGET_ORGS:
            hp = "🔥 " if org.get("priority") == "high" else ""
            try:
                st.link_button(f"{hp}🏢 {org['name']}", org["url"])
            except Exception:
                st.markdown(f"[{hp}🏢 {org['name']}]({org['url']})")

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

# Layout: links Hauptfläche, rechts Snapshot-Spalte
col1, col2 = st.columns([6, 1], gap="large")

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
# ⭐ Strengere Stern-Logik
# =========================
def looks_leadership_strict(it: Dict[str, Any]) -> bool:
    text = " ".join([str(item_title(it)), str(it.get("kurzbeschreibung", ""))]).lower()
    strict_terms = [
        "laborleiter", "teamleiter", "gruppenleiter", "abteilungsleiter", "bereichsleiter",
        " head of ", "director",
    ]
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


# =========================
# Tabs: BA-Suche + Firmencheck (manuell)
# =========================
with col1:
    tab_ba, tab_company = st.tabs(["BA-Suche", "Firmencheck (manuell)"])

    # -------------------- TAB 1: BA-Suche --------------------
    with tab_ba:
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

        # Sort: HighPriority -> Entfernung -> neu -> Score
        def sort_key(it: Dict[str, Any]):
            org = match_target_org(item_company(it))
            priority_rank = 0
            if org and org.get("priority") == "high":
                priority_rank = -1

            dist = distance_from_home_km(it, float(home_lat), float(home_lon))
            dist_rank = dist if dist is not None else 999999.0

            is_new_rank = 0 if (it.get("_key") in new_keys) else 1
            score = relevance_score(it, int(ho_bonus))
            return (priority_rank, dist_rank, is_new_rank, -score, item_title(it).lower())

        items_sorted = sorted(items_now, key=sort_key)

        # Nummerierung: IMMER (1..N)
        for i, it in enumerate(items_sorted, start=1):
            it["_idx"] = i

        st.subheader(f"Treffer: {len(items_sorted)}")
        st.caption(f"Neu seit Snapshot: {len(new_keys)}")

        st.divider()
        st.write("## 🔥 High-Priority Treffer")
        hp_items = [
            it for it in items_sorted
            if (match_target_org(item_company(it)) and match_target_org(item_company(it)).get("priority") == "high")
        ]
        if hp_items:
            for it in hp_items:
                st.write(f"• {item_title(it)} – {item_company(it)}")
        else:
            st.info("Aktuell keine High-Priority Treffer.")

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
            if org:
                target_tag = " 🔥🎯" if org.get("priority") == "high" else " 🎯"
            else:
                target_tag = ""

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

                # Karriereseite (Ziel-Organisation)
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

    # -------------------- TAB 2: Firmencheck (manuell, pro Firma) --------------------
    with tab_company:
        st.subheader("Firmencheck (manuell, pro Firma)")
        st.caption("Öffne die Karriereseite, trage Anzahl + Notizen ein und speichere 'Heute geprüft'.")

        company_state = load_company_state()
        today = datetime.now().strftime("%Y-%m-%d")
        
        # --- Bewertungslogik: "überfällig" nach Tagen ---
        use_sliders = st.checkbox("Schwellen per Slider einstellen", value=True, key="fc_use_sliders")

        if use_sliders:
            warn_days = st.slider("Gelb ab X Tagen ohne Prüfung", 1, 60, 7, 1, key="fc_warn_days")
            crit_days = st.slider("Rot ab X Tagen ohne Prüfung", 2, 120, 14, 1, key="fc_crit_days")
        else:
            # feste Werte (fallback)
            warn_days = 7
            crit_days = 14

        # Sicherheit: crit muss >= warn sein
        if crit_days < warn_days:
            crit_days = warn_days

        def days_since(date_str: str) -> Optional[int]:
            """
            Erwartet YYYY-MM-DD. Gibt Anzahl Tage seit Datum zurück, oder None.
            """
            s = (date_str or "").strip()
            if not s:
                return None
            try:
                d = datetime.strptime(s, "%Y-%m-%d").date()
                return (datetime.now().date() - d).days
            except Exception:
                return None

        def freshness_badge(last_checked: str, warn: int, crit: int) -> Tuple[str, str]:
            """
            Return (emoji, label)
            """
            ds = days_since(last_checked)
            if ds is None:
                return "🔴", "nie geprüft"
            if ds >= crit:
                return "🔴", f"{ds} Tage"
            if ds >= warn:
                return "🟡", f"{ds} Tage"
            return "🟢", f"{ds} Tage"
        
        # --- Firmencheck: Filter / Suche / Export ---
        st.markdown("### Übersicht & Tools")

        only_high = st.checkbox("Nur High-Priority (🔥) anzeigen", value=False, key="fc_only_high")
        name_filter = st.text_input("Firma suchen (Teilstring)", value="", key="fc_name_filter").strip().lower()

        # Hilfs-Map: org_name -> org (für priority/url)
        org_by_name: Dict[str, Dict[str, Any]] = {o["name"]: o for o in TARGET_ORGS}

        # Übersicht berechnen
        checked_today = 0
        checked_any = 0
        overdue = 0
        total_positive = 0
      
        for org in TARGET_ORGS:
            org_name = org["name"]
            data = company_state.get(org_name, {})
            last_checked = str(data.get("last_checked") or "")
            if last_checked:
                checked_any += 1
            if last_checked == today:
                checked_today += 1

            # Überfällig = Gelb oder Rot nach Tagen-Logik
            emoji, _label = freshness_badge(last_checked, int(warn_days), int(crit_days))
            if emoji in ("🟡", "🔴"):
                overdue += 1

            prev_count = int(data.get("prev_count", 0) or 0)
            cur_count = int(data.get("count", 0) or 0)
            diff = cur_count - prev_count
            if diff > 0:
                total_positive += diff

        cA, cB, cC, cD = st.columns(4)
        cA.metric("Firmen gesamt", len(TARGET_ORGS))
        cB.metric("Heute geprüft", checked_today)
        cC.metric("Überfällig", overdue)
        cD.metric("Σ +neu (seit letzter Prüfung)", total_positive)

        st.divider()
               
        # Export vorbereiten
        export_payload = {
            "exported_at": datetime.now().isoformat(timespec="seconds"),
            "today": today,
            "items": []
        }

        # CSV (ohne pandas)
        csv_lines = ["name,url,priority,last_checked,count,prev_count,diff,notes"]
        
        def org_sort_key(o: Dict[str, Any]) -> Tuple[int, int, str]:
            # 1) High-Priority zuerst
            pr = 0 if o.get("priority") == "high" else 1

            # 2) Überfälligkeit / nie geprüft zuerst
            org_name = o.get("name", "")
            data = company_state.get(org_name, {})
            last_checked = str(data.get("last_checked") or "")
            ds = days_since(last_checked)
            # nie geprüft => ganz nach oben
            if ds is None:
                overdue_rank = 0
                ds_rank = 10**9
            else:
                # rot/gelb -> weiter oben
                overdue_rank = 1 if ds >= int(warn_days) else 2
                ds_rank = -ds  # je älter, desto weiter oben innerhalb der Gruppe

            return (pr, overdue_rank, ds_rank, org_name)

        filtered_orgs = []
        for org in sorted(TARGET_ORGS, key=org_sort_key):
            if only_high and org.get("priority") != "high":
                continue
            if name_filter and (name_filter not in org.get("name", "").lower()):
                continue
            filtered_orgs.append(org)

        st.caption(f"Angezeigte Firmen: {len(filtered_orgs)} von {len(TARGET_ORGS)}")
        st.divider()

        for org in filtered_orgs:
            org_name = org["name"]
            url = org["url"]

            if org_name not in company_state:
                company_state[org_name] = {
                    "last_checked": "",
                    "count": 0,
                    "prev_count": 0,
                    "notes": ""
                }

            data = company_state[org_name]
            prev_count = int(data.get("prev_count", 0))
            saved_count = int(data.get("count", 0))
            priority = org.get("priority", "")
            last_checked = str(data.get("last_checked") or "")
            notes_saved = str(data.get("notes") or "")

            diff_saved = saved_count - prev_count

            export_payload["items"].append({
                "name": org_name,
                "url": url,
                "priority": priority,
                "last_checked": last_checked,
                "count": saved_count,
                "prev_count": prev_count,
                "diff": diff_saved,
                "notes": notes_saved,
            })

            # CSV-Zeile (quotes minimal sicher machen)
            def _csv_safe(s: str) -> str:
                s = (s or "").replace('"', '""').replace("\n", " ").replace("\r", " ")
                return f'"{s}"'

            csv_lines.append(",".join([
                _csv_safe(org_name),
                _csv_safe(url),
                _csv_safe(priority),
                _csv_safe(last_checked),
                str(saved_count),
                str(prev_count),
                str(diff_saved),
                _csv_safe(notes_saved),
            ]))

            hp = "🔥 " if org.get("priority") == "high" else ""

            headL, headR, headX = st.columns([5, 1.3, 1.1])
            with headL:
                emoji, age_label = freshness_badge(str(data.get("last_checked") or ""), int(warn_days), int(crit_days))
                st.markdown(f"### {emoji} {hp}🏢 {org_name}  ·  {age_label}")
                st.caption(f"Zuletzt geprüft: {data.get('last_checked') or '—'}")
            with headR:
                try:
                    st.link_button("🏢 Öffnen", url)
                except Exception:
                    st.markdown(f"[🏢 Öffnen]({url})")
            with headX:
                if st.button("↩︎ Reset", key=f"reset_{org_name}"):
                    company_state[org_name] = {"last_checked": "", "count": 0, "prev_count": 0, "notes": ""}
                    save_company_state(company_state)
                    st.success("Zurückgesetzt.")
                    st.rerun()

            c1, c2, c3 = st.columns([1.6, 1.6, 3.8])
            with c1:
                new_count = st.number_input(
                    "Anzahl interessante Stellen",
                    min_value=0,
                    value=saved_count,
                    step=1,
                    key=f"count_{org_name}",
                )
            with c2:
                if st.button("✔ Heute geprüft", key=f"check_{org_name}"):
                    data["prev_count"] = int(data.get("count", 0))
                    data["count"] = int(new_count)
                    data["last_checked"] = today
                    company_state[org_name] = data
                    save_company_state(company_state)
                    st.success("Gespeichert.")
                    st.rerun()
            with c3:
                if data.get("last_checked"):
                    diff = int(new_count) - prev_count
                    if diff > 0:
                        st.markdown(f"🟢 **+{diff}** seit letzter Prüfung")
                    elif diff < 0:
                        st.markdown(f"🔴 **{diff}** seit letzter Prüfung")
                    else:
                        st.markdown("🟡 **keine Veränderung**")

            notes = st.text_area(
                "Notizen / interessante Titel (z.B. Stichpunkte oder konkrete Jobtitel)",
                value=str(data.get("notes", "")),
                height=90,
                key=f"notes_{org_name}",
            )
            if st.button("💾 Notizen speichern", key=f"save_notes_{org_name}"):
                data["notes"] = notes
                data["count"] = int(new_count)
                company_state[org_name] = data
                save_company_state(company_state)
                st.success("Notizen gespeichert.")

            st.divider()
        st.markdown("### Export")
        json_bytes = json.dumps(export_payload, ensure_ascii=False, indent=2).encode("utf-8")
        csv_bytes = ("\n".join(csv_lines)).encode("utf-8")

        cE1, cE2 = st.columns(2)
        with cE1:
            st.download_button(
                "⬇️ Firmencheck als JSON herunterladen",
                data=json_bytes,
                file_name=f"firmencheck_{today}.json",
                mime="application/json",
            )
        with cE2:
            st.download_button(
                "⬇️ Firmencheck als CSV herunterladen",
                data=csv_bytes,
                file_name=f"firmencheck_{today}.csv",
                mime="text/csv",
            )
