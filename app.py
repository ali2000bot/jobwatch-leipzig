import json
import math
import os
import hashlib
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple
from collections import defaultdict

import requests
import streamlit as st
import streamlit.components.v1 as components
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ============================================================
# Persistenter State-Ordner (immer neben app.py)
# ============================================================
APP_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_DIR = os.path.join(APP_DIR, ".jobwatch_state")
SNAPSHOT_FILE = os.path.join(STATE_DIR, "snapshot.json")
COMPANY_STATE_FILE = os.path.join(STATE_DIR, "company_monitor.json")
HIDDEN_JOBS_FILE = os.path.join(STATE_DIR, "hidden_jobs.json")
FAVORITES_FILE = os.path.join(STATE_DIR, "favorites.json")


def ensure_state_dir() -> None:
    os.makedirs(STATE_DIR, exist_ok=True)


# -------------------- Snapshot helpers --------------------
def load_snapshot() -> Dict[str, Any]:
    if not os.path.exists(SNAPSHOT_FILE):
        return {"timestamp": None, "items": []}
    with open(SNAPSHOT_FILE, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except Exception:
            return {"timestamp": None, "items": []}


def save_snapshot(items: List[Dict[str, Any]]) -> None:
    ensure_state_dir()
    payload = {"timestamp": datetime.now().isoformat(timespec="seconds"), "items": items}
    with open(SNAPSHOT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


# -------------------- Firmencheck state helpers --------------------
def load_company_state() -> Dict[str, Any]:
    if not os.path.exists(COMPANY_STATE_FILE):
        return {}
    with open(COMPANY_STATE_FILE, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

# ---------------- Favoriten ---------------------
def save_company_state(state: Dict[str, Any]) -> None:
    ensure_state_dir()
    with open(COMPANY_STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

def load_favorites() -> Dict[str, Any]:
    """
    Struktur:
    {
      "<job_key>": {
        "added_at": "YYYY-MM-DD HH:MM",
        "note": "... optional ..."
      },
      ...
    }
    """
    if not os.path.exists(FAVORITES_FILE):
        return {}
    try:
        with open(FAVORITES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_favorites(favs: Dict[str, Any]) -> None:
    ensure_state_dir()
    with open(FAVORITES_FILE, "w", encoding="utf-8") as f:
        json.dump(favs, f, ensure_ascii=False, indent=2)


def is_favorited(job_key: str, favs: Dict[str, Any]) -> bool:
    return bool(job_key) and job_key in favs

# -------------------- Hidden jobs helpers --------------------
def load_hidden_jobs() -> Dict[str, Any]:
    """
    { "hidden": ["ba:123...", "hx:abcd..."], "updated_at": "..." }
    """
    if not os.path.exists(HIDDEN_JOBS_FILE):
        return {"hidden": [], "updated_at": None}
    with open(HIDDEN_JOBS_FILE, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
        except Exception:
            return {"hidden": [], "updated_at": None}
    if not isinstance(data, dict):
        return {"hidden": [], "updated_at": None}
    if "hidden" not in data or not isinstance(data["hidden"], list):
        data["hidden"] = []
    if "updated_at" not in data:
        data["updated_at"] = None
    return data


def save_hidden_jobs(hidden_keys: Set[str]) -> None:
    ensure_state_dir()
    payload = {
        "hidden": sorted(list(hidden_keys)),
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    }
    with open(HIDDEN_JOBS_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


# ============================================================
# Geocoding (Wohnort + optional Job-Orte)
# ============================================================
@st.cache_data(ttl=7 * 24 * 3600, show_spinner=False)
def geocode_nominatim(query: str) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    """
    Geocoding via OSM Nominatim.
    Returns: (lat, lon, display_name_or_error)
    """
    q = (query or "").strip()
    if not q:
        return None, None, "Kein Ort angegeben."

    url = "https://nominatim.openstreetmap.org/search"
    params = {"q": q, "format": "json", "limit": 1}
    headers = {"User-Agent": "JobWatchLeipzig/1.0 (Streamlit App)"}

    try:
        r = requests.get(url, params=params, headers=headers, timeout=20)
    except Exception as e:
        return None, None, f"Geocode-Request fehlgeschlagen: {type(e).__name__}: {e}"

    if r.status_code == 429:
        return None, None, "Geocode HTTP 429 (zu viele Anfragen). Bitte kurz warten und erneut versuchen."
    if r.status_code != 200:
        return None, None, f"Geocode HTTP {r.status_code}: {r.text[:200]}"

    try:
        data = r.json()
    except Exception:
        return None, None, "Geocode: Antwort war kein gültiges JSON."

    if not isinstance(data, list) or len(data) == 0:
        return None, None, "Keine Koordinaten gefunden. Tipp: 'PLZ Ort' eingeben."

    hit = data[0]
    try:
        lat = float(hit.get("lat"))
        lon = float(hit.get("lon"))
        name = str(hit.get("display_name") or q)
        return lat, lon, name
    except Exception:
        return None, None, "Geocode: Treffer ohne gültige Koordinaten."


@st.cache_data(ttl=7 * 24 * 3600, show_spinner=False)
def geocode_job_location(query: str) -> Optional[Tuple[float, float]]:
    q = (query or "").strip()
    if not q:
        return None
    lat, lon, _msg = geocode_nominatim(q)
    if lat is None or lon is None:
        return None
    return float(lat), float(lon)

favorites = load_favorites()

# ============================================================
# BA Jobsuche (App Endpoint)
# ============================================================
BASE = "https://rest.arbeitsagentur.de/jobboerse/jobsuche-service"
SEARCH_URL = f"{BASE}/pc/v4/app/jobs"
API_KEY_DEFAULT = "jobboerse-jobsuche"


def ba_headers(api_key: str) -> Dict[str, str]:
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
    aktualitaet_tage: Optional[int],
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
        "wo": wo,
    }

    if aktualitaet_tage is not None:
        params["aktualitaet"] = str(int(aktualitaet_tage))
    if was and was.strip():
        params["was"] = was.strip()
    if arbeitszeit:
        params["arbeitszeit"] = arbeitszeit

    try:
        r = requests.get(
            SEARCH_URL,
            headers=ba_headers(api_key),
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
        r = requests.get(url, headers=ba_headers(api_key), timeout=25, verify=False)
    except Exception as e:
        return None, f"Details-Request-Fehler: {type(e).__name__}: {e}"

    if r.status_code != 200:
        return None, f"Details HTTP {r.status_code}: {r.text[:400]}"

    try:
        return r.json(), None
    except Exception:
        return None, "Details: Antwort war kein gültiges JSON."


# ============================================================
# Distance + travel time
# ============================================================
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


# ============================================================
# Keyword helpers + defaults
# ============================================================
DEFAULT_HOME_LABEL = "06242 Braunsbedra"
DEFAULT_HOME_LAT = 51.2861
DEFAULT_HOME_LON = 11.8900

DEFAULT_FOCUS_KEYWORDS = [
    "thermoanalyse", "thermophysik", "thermal analysis", "thermophysical",
    "dsc", "tga", "lfa", "hfm", "heat flow meter", "laser flash", "laser flash analysis",
    "wärmeleitfähigkeit", "thermal conductivity", "diffusivität", "diffusivity",
    "kalorimetrie", "calorimetry", "wärmekapazität", "heat capacity",
    "materialcharakterisierung", "material characterization",
    "analytik", "instrumentierung", "messgerät", "labor",
    "werkstoff", "werkstoffe", "polymer", "keramik", "metall",
    "f&e", "forschung", "entwicklung", "r&d", "research", "development",
    "verfahrenstechnik", "thermodynamik", "wärmeübertragung",
    "thermische simulation", "physik", "physics",
]

DEFAULT_LEADERSHIP_KEYWORDS = [
    "laborleiter", "teamleiter", "gruppenleiter", "abteilungsleiter", "bereichsleiter",
    "leiter", "head", "lead", "director", "manager", "principal",
    "sektionsleiter", "section manager",
]

DEFAULT_NEGATIVE_KEYWORDS = [
    # Pflege/Gesundheit
    "altenpfleger", "pflege", "pflegefachkraft", "krankenpfleger", "pflegedienst",
    "gesundheits", "medizinische", "arzthelfer", "mfa", "therapeut", "betreuungskraft", 
    "zahntechniker", "zahntechnikerin", 
    # Gastro/Service
    "kellner", "servicekraft", "küche", "koch", "spülkraft", "restaurant", "barista",
    # Reinigung/Facility
    "reinigung", "reinigungskraft", "hausmeister", "gebäudereinigung",
    # Lager/Logistik/Produktion (wenn zu viel Rauschen)
    "kommissionierer", "lager", "picker", "packen", "versand", "zusteller",
    "staplerfahrer", "gabelstaplerfahrer", "postbote", "produktionshelfer",
    "maschinenbediener", "produktionsmitarbeiter", "montagehelfer", "schlosser", "busfahrer", 
    "schweißer", "bauleiter", "polymerchemiker", "chemiker", "kraftfahrer", "schichtleiter",
    "metallhelfer", "metallbauer", "industriemechaniker", "chemielaborant", "vorarbeiter", 
    "lackierer",
    # Büro/sonstiges Rauschen
    "assistant", "assistenz", "sekretariat", "vorstandsassistenz",
    "insurance", "versicherung", "minijob", "steuerfachangestellte", "sachbearbeiter", 
    "personalreferent", "junior", "bürosachbearbeitung", "referent", "büroassistenz", "büroassistent", 
    "facharzt", "integrationshelfer", "empfangsleiter", 
]

def parse_keywords(text: str) -> List[str]:
    raw: List[str] = []
    for line in (text or "").splitlines():
        raw.extend([p.strip() for p in line.split(",")])
    return [x for x in raw if x]


def keywords_to_text(words: List[str]) -> str:
    return "\n".join(words)


# ============================================================
# Jobarten / Profile (breit)
# ============================================================
def build_queries() -> Dict[str, str]:
    """
    Breit suchen: BA 'was' eher kurz halten.
    Relevanz steuern wir danach über Score/Keywords.
    """
    return {
        "Breit (ohne Suchtext)": "",
        "Leitung (breit)": "Leiter Teamleiter Laborleiter Gruppenleiter Abteilungsleiter Bereichsleiter Head Director",
        "Projektmanagement": "Projektmanager Project Manager Program Manager Technical Project",
        "Technischer Vertrieb": "Sales Engineer Technischer Vertrieb Key Account Business Development",
        "R&D / Entwicklung": "Forschung Entwicklung R&D Engineer Scientist",
    }


# ============================================================
# Ziel-Organisationen (aus deinem letzten Stand)
# ============================================================
TARGET_ORGS: List[Dict[str, Any]] = [
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
    {"name": "BMW Werk Leipzig", "match": ["bmw"], "url": "https://www.bmwgroup.jobs/de/de/standorte/werke-in-deutschland/werk-leipzig.html"},
    {"name": "Porsche Leipzig", "match": ["porsche"], "url": "https://www.porsche-leipzig.com/jobs-karriere/"},
    {"name": "DHL Hub Leipzig", "match": ["dhl", "deutsche post", "hub leipzig"], "url": "https://www.dhl.com/de-de/microsites/express/hubs/hub-leipzig/jobs.html"},
    {"name": "Mitteldeutsche Flughafen AG (LEJ)", "match": ["mitteldeutsche flughafen", "flughafen leipzig", "leipzig/halle", "leipzig-halle"], "url": "https://www.mdf-ag.com/karriere/alle-jobs/flughafen-leipzig-halle"},
    {"name": "Stadtwerke Leipzig / Leipziger Gruppe", "match": ["stadtwerke leipzig", "leipziger gruppe", "l-gruppe", "l.de"], "url": "https://www.l.de/karriere/stellenangebote/"},
    {"name": "enviaM-Gruppe", "match": ["enviam", "envia", "mitgas"], "url": "https://jobs.enviam-gruppe.de/"},
    {"name": "GBA Group", "match": ["gba"], "url": "https://www.gba-group.com/karriere/jobs/"},
    {"name": "Eurofins", "match": ["eurofins"], "url": "https://careers.eurofins.com/de"},
    {"name": "SGS", "match": ["sgs"], "url": "https://www.sgs.com/de-de/unternehmen/karriere-bei-sgs/stellenangebote"},
    {"name": "DEKRA", "match": ["dekra"], "url": "https://www.dekra.de/de/karriere/ueberblick/"},
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
    {"name": "MFPA Leipzig GmbH", "match": ["mfpa leipzig", "mfpa", "prüfanstalt", "materialforschungs"], "url": "https://www.mfpa-leipzig.de/", "priority": "high"},
    {"name": "MFPA Leipzig – Wärmeleitfähigkeit (Service)", "match": ["mfpa leipzig", "mfpa"], "url": "https://www.mfpa-leipzig.de/service/pruefung-der-waermeleitfaehigkeit-von-daemmstoffen/"},
    {"name": "Universität Leipzig – Technische Chemie (Equipment)", "match": ["institut für technische chemie", "technical chemistry", "universität leipzig", "uni leipzig"], "url": "https://www.chemie.uni-leipzig.de/en/institute-of-chemical-technology/technical-equipment"},
    {"name": "MLU Halle – Thermal analysis (Geo/MinGeo)", "match": ["geo.uni-halle", "thermalanalysis", "mlu", "uni halle", "martin-luther"], "url": "https://geo.uni-halle.de/en/mingeochem/laboratories/thermalanalysis/"},
    {"name": "HTWK Leipzig – MNZ Werkstoffprüfung", "match": ["mnz", "htwk"], "url": "https://mnz.htwk-leipzig.de/forschung/analytisches-zentrum/analysemethoden/werkstoffpruefung/"},
    {"name": "TZO Leipzig – Labor Umwelterprobung & Werkstoffprüfung", "match": ["tzo leipzig", "luw"], "url": "https://tzoleipzig.de/labor-fuer-umwelterprobung/"},
]


def match_target_org(company: str) -> Optional[Dict[str, Any]]:
    c = (company or "").lower()
    if not c.strip():
        return None
    for org in TARGET_ORGS:
        if any(m in c for m in org.get("match", [])):
            return org
    return None


# ============================================================
# Leaflet map: numbered pins (grouped label supported like "3–5")
# ============================================================
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
      white-space: nowrap;
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
    const html = `<div class="pinwrap">${{pinSvg(color)}}<div class="pinnum">${{numText || ""}}</div></div>`;
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
  // Pendelradius-Kreis
  L.circle([home_lat, home_lon], {
    radius: MAX_RADIUS_METERS,
    color: "#1565c0",
    weight: 2,
    fillColor: "#1565c0",
    fillOpacity: 0.08
  }).addTo(map);

  markers.forEach(m => {{
    const lat = m.lat, lon = m.lon;
    const title = (m.title || '').replace(/</g,'&lt;');
    const company = (m.company || '').replace(/</g,'&lt;');
    const dist = (m.dist_km != null) ? (Math.round(m.dist_km*10)/10) : null;
    const idx = (m.idx || '').toString();

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


# ============================================================
# Score / Relevanz
# ============================================================
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


def score_breakdown(
    it: Dict[str, Any],
    focus_keywords: List[str],
    leadership_keywords: List[str],
    negative_keywords: List[str],
    ho_bonus_val: int,
) -> Tuple[int, List[str]]:
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

    for k in focus_keywords:
        if k and k in text:
            score += 10
            parts.append(f"+10 {k}")

    for k in leadership_keywords:
        if k and k in text:
            score += 6
            parts.append(f"+6 {k}")

    for k in negative_keywords:
        if k and k in text:
            score -= 12
            parts.append(f"−12 {k}")

    if is_homeoffice_item(it) and ho_bonus_val > 0:
        score += int(ho_bonus_val)
        parts.append(f"+{int(ho_bonus_val)} homeoffice")

    if not parts:
        parts = ["(keine Keyword-Treffer)"]

    return score, parts


def is_probably_irrelevant(it: Dict[str, Any], negative_keywords: List[str]) -> bool:
    text = f"{item_title(it)} {it.get('kurzbeschreibung','')}".lower()
    return any(h in text for h in negative_keywords if h)


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


# ============================================================
# Streamlit App
# ============================================================
st.set_page_config(page_title="JobWatch Leipzig", layout="wide")
st.title("Raum Leipzig – Jobs finden & vergleichen")

# Session defaults for keywords
if "kw_focus" not in st.session_state:
    st.session_state["kw_focus"] = keywords_to_text(DEFAULT_FOCUS_KEYWORDS)
if "kw_lead" not in st.session_state:
    st.session_state["kw_lead"] = keywords_to_text(DEFAULT_LEADERSHIP_KEYWORDS)
if "kw_neg" not in st.session_state:
    st.session_state["kw_neg"] = keywords_to_text(DEFAULT_NEGATIVE_KEYWORDS)

# Defaults (werden in Sidebar überschrieben)
size = 100
api_key = API_KEY_DEFAULT
debug = False
near_km = 25
mid_km = 60
speed_kmh = 75
ho_bonus = 8
max_pages = 100
max_results = 1000

# Load snapshot once (right column uses it too)
snap = load_snapshot()

# Sidebar
with st.sidebar:
    st.header("JobWatch – Einstellungen")

    # -------------------------
    # Wohnort (1 Feld, Auto-Geocode)
    # -------------------------
    st.subheader("Wohnort")

    if "home_query" not in st.session_state:
        st.session_state["home_query"] = DEFAULT_HOME_LABEL
    if "home_lat" not in st.session_state:
        st.session_state["home_lat"] = float(DEFAULT_HOME_LAT)
    if "home_lon" not in st.session_state:
        st.session_state["home_lon"] = float(DEFAULT_HOME_LON)
    if "home_display" not in st.session_state:
        st.session_state["home_display"] = DEFAULT_HOME_LABEL
    if "geocode_error" not in st.session_state:
        st.session_state["geocode_error"] = ""
    if "home_query_last" not in st.session_state:
        st.session_state["home_query_last"] = ""
    if "home_geocode_last_ts" not in st.session_state:
        st.session_state["home_geocode_last_ts"] = 0.0

    def _auto_geocode():
        q = (st.session_state.get("home_query") or "").strip()

        # 1) nur bei echter Änderung
        last_q = (st.session_state.get("home_query_last") or "").strip()
        if q == last_q:
            return
        st.session_state["home_query_last"] = q

        # 2) minimale Eingabelänge (verhindert Requests bei "L", "Le", ...)
        if len(q) < 4:
            st.session_state["geocode_error"] = "Bitte etwas genauer eingeben (z.B. '06242 Braunsbedra')."
            return

        # 3) Rate-Limit: max. 1 Request pro 60s
        last_ts = float(st.session_state.get("home_geocode_last_ts") or 0.0)
        if time.time() - last_ts < 60:
            st.session_state["geocode_error"] = "Geocoding ist limitiert (429-Schutz). Bitte kurz warten und dann erneut Enter."
            return
        st.session_state["home_geocode_last_ts"] = time.time()

        lat, lon, msg = geocode_nominatim(q)
        if lat is None or lon is None:
            st.session_state["geocode_error"] = msg or "Geocoding fehlgeschlagen."
            return

        st.session_state["home_lat"] = float(lat)
        st.session_state["home_lon"] = float(lon)
        st.session_state["home_display"] = msg or q
        st.session_state["geocode_error"] = ""

    st.text_input("PLZ/Ort oder Adresse", key="home_query", on_change=_auto_geocode)

    if st.session_state.get("geocode_error"):
        st.warning(st.session_state["geocode_error"])
    else:
        st.caption(f"📍 verwendet: {st.session_state.get('home_display')}")

    home_query = str(st.session_state.get("home_query") or DEFAULT_HOME_LABEL)
    home_lat = float(st.session_state.get("home_lat") or DEFAULT_HOME_LAT)
    home_lon = float(st.session_state.get("home_lon") or DEFAULT_HOME_LON)
    home_label = str(st.session_state.get("home_display") or home_query)

    st.divider()

    # -------------------------
    # BA-Suche (minimal)
    # -------------------------
    st.subheader("Suche")

    wo = home_query

    max_distance_filter = st.slider(
        "Maximale Entfernung (km)",
        10, 200, 80, 5
    )
    st.caption("Dieser Radius bestimmt sowohl die BA-Suche als auch die angezeigten Jobs.")
    # gleicher Wert wird für BA-Suche verwendet
    umkreis = int(max_distance_filter)

    include_ho = st.checkbox("Homeoffice berücksichtigen", value=False)
    
    ho_umkreis = st.slider("Homeoffice-Umkreis (km)", 50, 200, 100, 25) if include_ho else 0

    aktualitaet_option = st.selectbox(
        "Aktualität",
        ["7 Tage", "30 Tage", "60 Tage", "180 Tage", "Alle"],
        index=2,
    )
    if aktualitaet_option == "Alle":
        aktualitaet = None
    else:
        aktualitaet = int(aktualitaet_option.split()[0])

    queries = build_queries()
    selected_profiles = st.multiselect(
        "Jobarten",
        list(queries.keys()),
        default=["Breit (ohne Suchtext)"],
    )

    st.divider()

    # -------------------------
    # Filter + Hidden Jobs
    # -------------------------
    st.subheader("Filter")
    only_focus = st.checkbox("Nur passende Treffer anzeigen", value=True)
    min_score = st.slider("Mindest-Relevanz", 0, 80, 6, 1)
    hide_irrelevant = st.checkbox("Offensichtlich unpassende Treffer ausblenden", value=True)

    hide_marked = st.checkbox("Bereits ausgeblendete Jobs verbergen", value=True)
    show_hidden_manage = st.checkbox("Ausblend-Liste verwalten", value=False)

    st.divider()

    # -------------------------
    # Erweitert
    # -------------------------
    with st.expander("Erweitert", expanded=False):
        st.caption("Nur wenn du feintunen oder debuggen willst.")

        st.markdown("**Suche-Breite**")
        max_pages = st.slider("Max. Seiten pro Jobart", 1, 100, 100, 1)
        max_results = st.slider("Stopp bei max. Treffern", 100, 4000, 2000, 100)
        st.caption(f"Techn. Maximum: {int(max_pages)*size}")
        st.caption(f"App-Limit: {int(max_results)}")

        enable_job_geocode = st.checkbox("Fehlende Koordinaten für Karte nachschlagen (langsamer)", value=False)
        max_job_geocodes = st.slider("Max. Geocoding pro Lauf", 0, 50, 10, 5)

        st.markdown("**Entfernung / Fahrzeit**")
        near_km = st.slider("Grün bis (km)", 5, 80, 25, 5)
        mid_km = st.slider("Gelb bis (km)", 10, 150, 60, 5)
        speed_kmh = st.slider("Ø Geschwindigkeit (km/h)", 30, 140, 75, 5)

        umkreis = int(max_distance_filter)
        st.divider()
        st.markdown("**Score-Tuning**")
        ho_bonus = st.slider("Homeoffice-Bonus (Score)", 0, 30, 8, 1)

        st.divider()
        st.markdown("**Technik**")
    
        api_key = st.text_input("X-API-Key (nur bei Problemen)", value=API_KEY_DEFAULT)
        debug = st.checkbox("Debug anzeigen", value=False)

        st.divider()
        st.markdown("**Keywords (optional)**")
        with st.expander("Fokus-Keywords bearbeiten", expanded=False):
            st.session_state["kw_focus"] = st.text_area(
                "Ein Begriff pro Zeile (oder Komma-getrennt)",
                value=st.session_state["kw_focus"],
                height=150,
            )
        with st.expander("Leitung/Führung-Keywords bearbeiten", expanded=False):
            st.session_state["kw_lead"] = st.text_area(
                "Ein Begriff pro Zeile (oder Komma-getrennt)",
                value=st.session_state["kw_lead"],
                height=110,
            )
        with st.expander("Negative Keywords bearbeiten", expanded=False):
            st.session_state["kw_neg"] = st.text_area(
                "Ein Begriff pro Zeile (oder Komma-getrennt)",
                value=st.session_state["kw_neg"],
                height=110,
            )

        if st.button("↩︎ Keywords zurücksetzen"):
            st.session_state["kw_focus"] = keywords_to_text(DEFAULT_FOCUS_KEYWORDS)
            st.session_state["kw_lead"] = keywords_to_text(DEFAULT_LEADERSHIP_KEYWORDS)
            st.session_state["kw_neg"] = keywords_to_text(DEFAULT_NEGATIVE_KEYWORDS)
            st.rerun()

# Keywords lists
FOCUS_KEYWORDS = [k.lower() for k in parse_keywords(st.session_state["kw_focus"])]
LEADERSHIP_KEYWORDS = [k.lower() for k in parse_keywords(st.session_state["kw_lead"])]
NEGATIVE_KEYWORDS = [k.lower() for k in parse_keywords(st.session_state["kw_neg"])]

# Layout: links Hauptfläche, rechts Snapshot-Spalte
col1, col2 = st.columns([6, 1], gap="large")

with col2:
    st.subheader("Snapshot")
    st.write(snap.get("timestamp") or "— noch keiner gespeichert")

    if st.button("Stand speichern"):
        st.session_state["save_snapshot_requested"] = True

    if st.button("Stand löschen"):
        ensure_state_dir()
        if os.path.exists(SNAPSHOT_FILE):
            os.remove(SNAPSHOT_FILE)
        st.success("Snapshot gelöscht. Seite neu laden.")

    st.divider()
    st.subheader("Ziel-Organisationen")
    st.caption("Karriereseiten (manueller Check).")
    with st.expander("Liste anzeigen / öffnen", expanded=False):
        for org in TARGET_ORGS:
            try:
                st.link_button(f"🏢 {org['name']}", org["url"])
            except Exception:
                st.markdown(f"[🏢 {org['name']}]({org['url']})")


# ============================================================
# Tabs: BA-Suche + Firmencheck (manuell)
# ============================================================
with col1:
    tab_ba, tab_company = st.tabs(["BA-Suche", "Firmencheck (manuell)"])

    # -------------------- TAB 1: BA-Suche --------------------
    with tab_ba:
        if not selected_profiles:
            st.warning("Bitte mindestens eine Jobart auswählen.")
            st.stop()

        # Hidden jobs state
        _hidden_data = load_hidden_jobs()
        hidden_keys: Set[str] = set(_hidden_data.get("hidden", []))

        if show_hidden_manage:
            st.subheader("🙈 Ausblend-Liste")
            if len(all_items) >= int(max_results):
                st.warning(f"Suche wurde bei {int(max_results)} Treffern gestoppt (Erweitert → Stopp-Limit).")
            st.caption(f"{len(hidden_keys)} Jobs ausgeblendet (Stand: {_hidden_data.get('updated_at') or '—'})")
            cHM1, cHM2 = st.columns([1.2, 3.8])
            with cHM1:
                if st.button("🧹 Alle löschen"):
                    save_hidden_jobs(set())
                    st.success("Ausblend-Liste geleert.")
                    st.rerun()
            with cHM2:
                if hidden_keys:
                    st.code("\n".join(sorted(hidden_keys)))
            st.divider()

        wo = home_query

        # Live-UI
        live_status = st.empty()
        live_progress = st.progress(0)
        live_hint = st.empty()
        
        with st.spinner("Suche läuft…"):
            all_items: List[Dict[str, Any]] = []
            errs: List[str] = []
            qmap = build_queries()

            total_limit = int(max_results)
            pages_limit = int(max_pages)
            done_pages = 0
            expected_pages = max(1, len(selected_profiles) * pages_limit * (2 if include_ho else 1))

            for name in selected_profiles:
                q = qmap.get(name, "")

                # -------- Vor Ort --------
                for page in range(1, pages_limit + 1):
                    if len(all_items) >= total_limit:
                        break

                    done_pages += 1
                    pct = min(1.0, done_pages / expected_pages)

                    live_status.markdown(
                        f"**Live:** Profil **{name}** · Vor Ort · Seite **{page}/{pages_limit}** · Treffer **{len(all_items)}/{total_limit}**"
                    )
                    live_progress.progress(int(pct * 100))

                    items_local, e1 = fetch_search(
                        api_key, wo, int(umkreis), q, aktualitaet, int(size),
                        page=page, arbeitszeit=None
                    )

                    if e1:
                        errs.append(f"{name} (vor Ort) Seite {page}: {e1}")
                        break

                    if not items_local:
                        break

                    live_hint.caption(f"Letzte Seite: +{len(items_local)} Treffer")

                    for it in items_local:
                        it["_profile"] = name
                        it["_bucket"] = f"Vor Ort ({umkreis} km)"
                        all_items.append(it)

                    if len(items_local) < int(size):
                        break

                # -------- Homeoffice --------
                if include_ho:
                    for page in range(1, pages_limit + 1):
                        if len(all_items) >= total_limit:
                            break

                        done_pages += 1
                        pct = min(1.0, done_pages / expected_pages)

                        live_status.markdown(
                            f"**Live:** Profil **{name}** · Homeoffice · Seite **{page}/{pages_limit}** · Treffer **{len(all_items)}/{total_limit}**"
                        )
                        live_progress.progress(int(pct * 100))

                        items_ho, e2 = fetch_search(
                            api_key, wo, int(ho_umkreis), q, aktualitaet, int(size),
                            page=page, arbeitszeit="ho"
                        )

                        if e2:
                            errs.append(f"{name} (homeoffice) Seite {page}: {e2}")
                            break

                        if not items_ho:
                            break

                        live_hint.caption(f"Letzte Seite (HO): +{len(items_ho)} Treffer")

                        for it in items_ho:
                            it["_profile"] = name
                            it["_bucket"] = f"Homeoffice ({ho_umkreis} km)"
                            all_items.append(it)

                        if len(items_ho) < int(size):
                            break

        live_progress.progress(100)
        live_status.success(f"Fertig. Roh-Treffer: {len(all_items)}")
        
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

        # Hidden filter
        if hide_marked:
            items_now = [it for it in items_now if (it.get("_key") or item_key(it)) not in hidden_keys]

        # Filter
        if hide_irrelevant:
            items_now = [it for it in items_now if not is_probably_irrelevant(it, NEGATIVE_KEYWORDS)]

        if only_focus:
            items_now = [
                it for it in items_now
                if score_breakdown(it, FOCUS_KEYWORDS, LEADERSHIP_KEYWORDS, NEGATIVE_KEYWORDS, int(ho_bonus))[0] >= int(min_score)
            ]

        # Snapshot compare
        prev_items = snap.get("items", [])
        prev_keys: Set[str] = {x.get("_key") or item_key(x) for x in prev_items if isinstance(x, dict)}
        now_keys: Set[str] = {x.get("_key") or item_key(x) for x in items_now}
        new_keys = now_keys - prev_keys

        # Sort: HighPriority -> Entfernung -> neu -> Score
        def sort_key(it: Dict[str, Any]):
            org = match_target_org(item_company(it))
            priority_rank = -1 if (org and org.get("priority") == "high") else 0

            dist = distance_from_home_km(it, float(home_lat), float(home_lon))
            dist_rank = dist if dist is not None else 999999.0

            is_new_rank = 0 if (it.get("_key") in new_keys) else 1

            score = score_breakdown(it, FOCUS_KEYWORDS, LEADERSHIP_KEYWORDS, NEGATIVE_KEYWORDS, int(ho_bonus))[0]
            return (priority_rank, dist_rank, is_new_rank, -score, item_title(it).lower())

        # Entfernungslimit anwenden
        items_now_filtered = []
        for it in items_now:
            dist = distance_from_home_km(it, float(home_lat), float(home_lon))

            if dist is None:
                items_now_filtered.append(it)
                continue

            if dist <= float(max_distance_filter):
                items_now_filtered.append(it)
                continue

            if include_ho and is_homeoffice_item(it):
                items_now_filtered.append(it)

        items_now = items_now_filtered

        # alt
        #items_now_filtered = []
        #for it in items_now:
        #    dist = distance_from_home_km(it, float(home_lat), float(home_lon))
        #    if dist is None or dist <= float(max_distance_filter):
        #        items_now_filtered.append(it)

        #items_now = items_now_filtered
        
        items_sorted = sorted(items_now, key=sort_key)

        # Nummerierung
        for i, it in enumerate(items_sorted, start=1):
            it["_idx"] = i

        st.subheader(f"Treffer: {len(items_sorted)}")
        
        st.divider()
        with st.expander(f"📌 Merkliste ({len(favorites)})", expanded=False):
            if not favorites:
                st.info("Noch keine gemerkten Stellen.")
            else:
                # sortiert nach Zeitpunkt
                fav_items = []
                for it in items_sorted:
                    k = it.get("_key") or item_key(it)
                    if k in favorites:
                        fav_items.append((k, it))

                if not fav_items:
                    st.warning("Es sind Favoriten gespeichert, aber sie sind in der aktuellen Suche nicht enthalten.")
                    st.caption("Tipp: Favoriten bleiben gespeichert – erscheinen aber nur, wenn sie wieder gefunden werden.")
                else:
                    for k, it in fav_items:
                        note = (favorites.get(k, {}) or {}).get("note", "")
                        when = (favorites.get(k, {}) or {}).get("added_at", "")
                        st.markdown(
                            f"**{item_title(it)}**  \n"
                            f"{item_company(it)} · {pretty_location(it)}  \n"
                            f"{('📝 ' + note) if note else ''}  \n"
                            f"{('🕒 ' + when) if when else ''}"
                        )
                        web_url = jobsuche_web_url(it)
                        if web_url:
                            try:
                                st.link_button("🔗 BA öffnen", web_url, key=f"fav_link_{k}")
                            except Exception:   
                                st.markdown(f"[🔗 BA öffnen]({web_url})")
                        st.divider()
 
        # if len(all_items) >= int(max_results):
        #    st.warning("Suche wurde bei 2000 Treffern gestoppt.")
            
        st.caption(f"Neu seit Snapshot: {len(new_keys)}")

        # High-Priority Section
        st.divider()
        st.write("## 🔥 High-Priority Treffer")
        hp_items = [
            it for it in items_sorted
            if (match_target_org(item_company(it)) and match_target_org(item_company(it)).get("priority") == "high")
        ]
        if hp_items:
            for it in hp_items[:15]:
                st.write(f"• {item_title(it)} – {item_company(it)}")
        else:
            st.info("Aktuell keine High-Priority Treffer.")

        # Save snapshot
        if st.session_state.get("save_snapshot_requested"):
            save_snapshot(items_sorted)
            st.session_state["save_snapshot_requested"] = False
            st.success("Snapshot gespeichert.")

        # Build markers (with grouping same coords)
        raw_markers: List[Dict[str, Any]] = []
        missing_coords = 0
        geocode_used = 0

        # If variables not defined (when "Erweitert" never opened), set defaults:
        enable_job_geocode = bool(locals().get("enable_job_geocode", False))
        max_job_geocodes = int(locals().get("max_job_geocodes", 0))

        for it in items_sorted:
            ll = extract_latlon_from_item(it)

            if not ll:
                if enable_job_geocode and geocode_used < int(max_job_geocodes):
                    loc_text = pretty_location(it)
                    ll = geocode_job_location(loc_text)
                    geocode_used += 1

                if not ll:
                    missing_coords += 1
                    continue

            dist = distance_from_home_km(it, float(home_lat), float(home_lon))
            d = float(dist) if dist is not None else None
            bucket = distance_bucket(d, int(near_km), int(mid_km))

            raw_markers.append(
                {
                    "idx": int(it.get("_idx", 0)),
                    "lat": float(ll[0]),
                    "lon": float(ll[1]),
                    "title": item_title(it),
                    "company": item_company(it),
                    "dist_km": d,
                    "pin": bucket,
                }
            )

        groups = defaultdict(list)
        for m in raw_markers:
            key = (round(m["lat"], 6), round(m["lon"], 6))
            groups[key].append(m)

        markers: List[Dict[str, Any]] = []
        for (_lat, _lon), group in groups.items():
            if len(group) == 1:
                markers.append(group[0])
            else:
                idxs = sorted([g["idx"] for g in group if isinstance(g.get("idx"), int) and g["idx"] > 0])
                idx_label = f"{idxs[0]}–{idxs[-1]}" if idxs else "?"
                base = group[0].copy()
                base["idx"] = idx_label
                base["title"] = f"{len(group)} Treffer an diesem Ort"
                markers.append(base)

        if debug:
            st.info(f"Marker nach Gruppierung: {len(markers)} | ursprüngliche Marker: {len(raw_markers)} | ohne Koordinaten: {missing_coords}")

        st.caption(f"Treffer gesamt: {len(items_sorted)} | Marker auf Karte: {len(markers)}")

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
            is_hidden = (k in hidden_keys)
            fav = is_favorited(k, favorites)

            score, parts = score_breakdown(it, FOCUS_KEYWORDS, LEADERSHIP_KEYWORDS, NEGATIVE_KEYWORDS, int(ho_bonus))

            dist = distance_from_home_km(it, float(home_lat), float(home_lon))
            t_min = travel_time_minutes(dist, float(speed_kmh))
            bucket = distance_bucket(dist, int(near_km), int(mid_km))
            emo = distance_emoji(bucket)

            star = "⭐ " if looks_leadership_strict(it) else ""
            ho_tag = " 🏠" if is_homeoffice_item(it) else ""

            org = match_target_org(item_company(it))
            target_tag = ""
            if org:
                target_tag = " 🔥🎯" if org.get("priority") == "high" else " 🎯"

            num_txt = f"{idx:02d}" if idx > 0 else "??"
            dist_txt = f"{dist:.1f} km" if dist is not None else "— km"

            pin = "📌 " if fav else ""
            label = f"{pin}{'🟢 ' if is_new else ''}{emo} {num_txt} · {dist_txt} · {star}{item_title(it)}{ho_tag}{target_tag}"
            
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
                # --- Favorit togglen + Notiz ---
                cFav1, cFav2 = st.columns([1.2, 3.8])
                with cFav1:
                    if not fav:
                        if st.button("📌 Merken", key=f"fav_add_{k}"):
                            favorites[k] = {
                                "added_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
                                "note": favorites.get(k, {}).get("note", "")
                            }
                            save_favorites(favorites)
                            st.rerun()
                    else:
                        if st.button("🗑️ Entfernen", key=f"fav_del_{k}"):
                            favorites.pop(k, None)
                            save_favorites(favorites)
                            st.rerun()

                with cFav2:
                    if fav:
                        note_key = f"fav_note_{k}"
                        note_val = (favorites.get(k, {}) or {}).get("note", "")
                        new_note = st.text_input("Notiz (optional)", value=note_val, key=note_key)
                        if new_note != note_val:
                            favorites[k]["note"] = new_note
                            save_favorites(favorites)

                # ---- Hide/Unhide controls ----
                cH1, cH2 = st.columns([1.4, 6.6])
                with cH1:
                    if not is_hidden:
                        if st.button("🙈 Ausblenden", key=f"hide_{k}"):
                            hidden_keys.add(k)
                            save_hidden_jobs(hidden_keys)
                            st.rerun()
                    else:
                        if st.button("👁️ Einblenden", key=f"unhide_{k}"):
                            hidden_keys.discard(k)
                            save_hidden_jobs(hidden_keys)
                            st.rerun()
                with cH2:
                    st.caption("Ausgeblendete Jobs werden bei künftigen Suchen automatisch versteckt.")

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

                if org:
                    st.write("**Karriereseite (Ziel-Organisation)**")
                    try:
                        st.link_button("🏢 Karriereseite öffnen", org["url"])
                    except Exception:
                        st.markdown(f"[🏢 Karriereseite öffnen]({org['url']})")

                # Score-Aufschlüsselung bleibt hier als Info (ohne extra Checkbox)
                st.write("**Score-Aufschlüsselung**")
                st.code(" | ".join(parts))

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

        use_sliders = st.checkbox("Schwellen per Slider einstellen", value=True, key="fc_use_sliders")
        if use_sliders:
            warn_days = st.slider("Gelb ab X Tagen ohne Prüfung", 1, 60, 7, 1, key="fc_warn_days")
            crit_days = st.slider("Rot ab X Tagen ohne Prüfung", 2, 120, 14, 1, key="fc_crit_days")
        else:
            warn_days = 7
            crit_days = 14
        if crit_days < warn_days:
            crit_days = warn_days

        def days_since(date_str: str) -> Optional[int]:
            s = (date_str or "").strip()
            if not s:
                return None
            try:
                d = datetime.strptime(s, "%Y-%m-%d").date()
                return (datetime.now().date() - d).days
            except Exception:
                return None

        def freshness_badge(last_checked: str, warn: int, crit: int) -> Tuple[str, str]:
            ds = days_since(last_checked)
            if ds is None:
                return "🔴", "nie geprüft"
            if ds >= crit:
                return "🔴", f"{ds} Tage"
            if ds >= warn:
                return "🟡", f"{ds} Tage"
            return "🟢", f"{ds} Tage"

        # Übersicht
        st.markdown("### Übersicht & Tools")
        only_high = st.checkbox("Nur High-Priority (🔥) anzeigen", value=False, key="fc_only_high")
        name_filter = st.text_input("Firma suchen (Teilstring)", value="", key="fc_name_filter").strip().lower()
        only_interesting = st.checkbox("Nur Firmen mit ⭐ interessanten Stellen", value=False, key="fc_only_interesting")
        only_new = st.checkbox("Nur Firmen mit 🟢 +neu seit letzter Prüfung", value=False, key="fc_only_new")

        checked_today = 0
        overdue = 0
        total_positive = 0

        for org in TARGET_ORGS:
            org_name = org["name"]
            data = company_state.get(org_name, {})
            last_checked = str(data.get("last_checked") or "")
            if last_checked == today:
                checked_today += 1

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

        export_payload = {"exported_at": datetime.now().isoformat(timespec="seconds"), "today": today, "items": []}
        csv_lines = ["name,url,priority,last_checked,count,prev_count,diff,notes"]

        def org_sort_key(o: Dict[str, Any]) -> Tuple[int, int, int, str]:
            pr = 0 if o.get("priority") == "high" else 1
            org_name = o.get("name", "")
            data = company_state.get(org_name, {})
            last_checked = str(data.get("last_checked") or "")
            ds = days_since(last_checked)
            if ds is None:
                overdue_rank = 0
                ds_rank = 10**9
            else:
                overdue_rank = 1 if ds >= int(warn_days) else 2
                ds_rank = -ds
            return (pr, overdue_rank, ds_rank, org_name)

        filtered_orgs = []
        for org in sorted(TARGET_ORGS, key=org_sort_key):
            if only_high and org.get("priority") != "high":
                continue
            if name_filter and (name_filter not in org.get("name", "").lower()):
                continue

            org_name = org.get("name", "")
            data = company_state.get(org_name, {}) if isinstance(company_state, dict) else {}
            c = int(data.get("count", 0) or 0)
            prev = int(data.get("prev_count", 0) or 0)
            diff = c - prev

            if only_interesting and c <= 0:
                continue
            if only_new and diff <= 0:
                continue

            filtered_orgs.append(org)

        st.caption(f"Angezeigte Firmen: {len(filtered_orgs)} von {len(TARGET_ORGS)}")
        st.divider()

        for org in filtered_orgs:
            org_name = org["name"]
            url = org["url"]

            if org_name not in company_state:
                company_state[org_name] = {"last_checked": "", "count": 0, "prev_count": 0, "notes": ""}

            data = company_state[org_name]
            prev_count = int(data.get("prev_count", 0))
            saved_count = int(data.get("count", 0))
            priority = org.get("priority", "")
            last_checked = str(data.get("last_checked") or "")
            notes_saved = str(data.get("notes") or "")

            diff_saved = saved_count - prev_count

            export_payload["items"].append(
                {
                    "name": org_name,
                    "url": url,
                    "priority": priority,
                    "last_checked": last_checked,
                    "count": saved_count,
                    "prev_count": prev_count,
                    "diff": diff_saved,
                    "notes": notes_saved,
                }
            )

            def _csv_safe(s: str) -> str:
                s = (s or "").replace('"', '""').replace("\n", " ").replace("\r", " ")
                return f'"{s}"'

            csv_lines.append(
                ",".join(
                    [
                        _csv_safe(org_name),
                        _csv_safe(url),
                        _csv_safe(priority),
                        _csv_safe(last_checked),
                        str(saved_count),
                        str(prev_count),
                        str(diff_saved),
                        _csv_safe(notes_saved),
                    ]
                )
            )

            hp = "🔥 " if org.get("priority") == "high" else ""
            emoji, age_label = freshness_badge(str(data.get("last_checked") or ""), int(warn_days), int(crit_days))
            diff_tag = f" · 🟢 +{diff_saved}" if diff_saved > 0 else (f" · 🔴 {diff_saved}" if diff_saved < 0 else "")
            count_tag = f" · ⭐ {saved_count} interessant" if saved_count > 0 else ""

            headL, headR, headX = st.columns([5, 1.3, 1.1])
            with headL:
                st.markdown(f"### {emoji} {hp}🏢 {org_name}{count_tag}{diff_tag}  ·  {age_label}")
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
