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
# Persistenz / Dateien
# ============================================================
APP_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_DIR = os.path.join(APP_DIR, ".jobwatch_state")

SNAPSHOT_FILE = os.path.join(STATE_DIR, "snapshot.json")
COMPANY_STATE_FILE = os.path.join(STATE_DIR, "company_monitor.json")
HIDDEN_JOBS_FILE = os.path.join(STATE_DIR, "hidden_jobs.json")
FAVORITES_FILE = os.path.join(STATE_DIR, "favorites.json")
HIDDEN_COMPANIES_FILE = os.path.join(STATE_DIR, "hidden_companies.json")


def ensure_state_dir() -> None:
    os.makedirs(STATE_DIR, exist_ok=True)


# ============================================================
# Defaults / Konstanten
# ============================================================
DEFAULT_HOME_LABEL = "06242 Braunsbedra"
DEFAULT_HOME_LAT = 51.2861
DEFAULT_HOME_LON = 11.8900

BASE = "https://rest.arbeitsagentur.de/jobboerse/jobsuche-service"
SEARCH_URL = f"{BASE}/pc/v4/app/jobs"
API_KEY_DEFAULT = "jobboerse-jobsuche"

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
    "altenpfleger", "pflege", "pflegefachkraft", "krankenpfleger", "pflegedienst",
    "psychologe", "betreuungsassistent",
    "gesundheits", "medizinische", "arzthelfer", "mfa", "therapeut", "betreuungskraft",
    "zahntechniker", "zahntechnikerin", "erzieher", "erzieherin", "friseurmeister",
    "kellner", "servicekraft", "küche", "koch", "spülkraft", "restaurant", "barista",
    "reinigung", "reinigungskraft", "hausmeister", "gebäudereinigung", "saisonkraft",
    "verkäufer", "lehrer",
    "kommissionierer", "lager", "picker", "packen", "versand", "zusteller", "sicherheitskraft",
    "staplerfahrer", "gabelstaplerfahrer", "postbote", "produktionshelfer", "aushilfe",
    "maschinenbediener", "produktionsmitarbeiter", "montagehelfer", "schlosser", "busfahrer",
    "lkw-fahrer", "elektriker", "maurer", "monteurin", "mechatroniker", "elektroniker",
    "schweißer", "bauleiter", "polymerchemiker", "chemiker", "kraftfahrer", "schichtleiter",
    "metallhelfer", "metallbauer", "industriemechaniker", "chemielaborant", "vorarbeiter",
    "metallbearbeitung",
    "lackierer", "monteur", "lüftungsbauer", "fachkraft", "blechbearbeiter", "helfer",
    "maschinist", "rohrverrichter", "metallfacharbeiter", "metallbearbeiter", "tischler",
    "assistant", "assistenz", "sekretariat", "vorstandsassistenz", "marktleiter",
    "insurance", "versicherung", "minijob", "steuerfachangestellte", "sachbearbeiter",
    "personalreferent", "junior", "bürosachbearbeitung", "referent", "büroassistenz", "büroassistent",
    "facharzt", "integrationshelfer", "empfangsleiter", "schulbegleiter", "held", "filialleiter",
    "personalentwicklung", "informatiker", "wirtschaftsinformatiker", "programmleiter",
]

RECRUITING_COMPANY_KEYWORDS = [
    "gmbh & co. kg personal", "personalvermittlung", "personalberatung", "personaldienst",
    "personaldienstleistung", "personaldienstleister", "recruiting", "headhunter",
    "talent acquisition", "staffing", "job agency", "arbeitsvermittlung", "hr solutions",
    "hr services", "people solutions", "career services", "jobcenter", "arbeitsagentur",
    "randstad", "adecco", "manpower", "persona service", "ferchau", "hays", "dis ag",
    "amadeus fire", "experis", "jobactive", "arwa", "orizon", "akut", "job impulse",
    "bindan", "alpha consult", "timepartner", "permacon", "tempton", "piening", "dekra arbeit",
    "hofmann", "i. k. hofmann", "run zeitarbeit", "unique personalservice", "meteor personaldienste",
    "aerb personal",
]

RECRUITING_TEXT_PATTERNS = [
    "im auftrag unseres kunden", "im auftrag eines kunden", "für unseren kunden", "für einen unserer kunden",
    "für einen namhaften kunden", "unser kunde", "unser mandant", "im rahmen der personalvermittlung",
    "im rahmen der direktvermittlung", "direktvermittlung", "personalvermittlung", "vermittlungsgutschein",
    "zeitarbeit", "arbeitnehmerüberlassung", "aü", "aueg", "arbeitnehmerueberlassung", "temp to perm",
]


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
]
# ============================================================
# Allgemeine Helper
# ============================================================
def parse_keywords(text: str) -> List[str]:
    raw: List[str] = []
    for line in (text or "").splitlines():
        raw.extend([p.strip() for p in line.split(",")])
    return [x for x in raw if x]


def keywords_to_text(words: List[str]) -> str:
    return "\n".join(words)


def sanitize_md_text(s: str) -> str:
    return (
        str(s or "")
        .replace("*", "✱")
        .replace("_", "‗")
        .replace("`", "´")
    )


def normalize_text(s: str) -> str:
    return " ".join((s or "").lower().replace("\n", " ").replace("\r", " ").split())


def ba_headers(api_key: str) -> Dict[str, str]:
    return {
        "User-Agent": "Jobsuche/2.9.2 (de.arbeitsagentur.jobboerse; build:1077) Streamlit",
        "X-API-Key": api_key,
        "Accept": "application/json",
        "Connection": "keep-alive",
    }


# ============================================================
# Persistente Daten laden / speichern
# ============================================================
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


def load_company_state() -> Dict[str, Any]:
    if not os.path.exists(COMPANY_STATE_FILE):
        return {}
    with open(COMPANY_STATE_FILE, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}


def save_company_state(state: Dict[str, Any]) -> None:
    ensure_state_dir()
    with open(COMPANY_STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def load_favorites() -> Dict[str, Any]:
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


def load_hidden_jobs() -> Dict[str, Any]:
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


def load_hidden_companies() -> Set[str]:
    if not os.path.exists(HIDDEN_COMPANIES_FILE):
        return set()
    try:
        with open(HIDDEN_COMPANIES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                return set(x.lower() for x in data)
    except Exception:
        pass
    return set()


def save_hidden_companies(companies: Set[str]) -> None:
    ensure_state_dir()
    with open(HIDDEN_COMPANIES_FILE, "w", encoding="utf-8") as f:
        json.dump(sorted(list(companies)), f, ensure_ascii=False, indent=2)


# ============================================================
# API / Jobfelder
# ============================================================
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


def short_field(it: Dict[str, Any], *keys: str) -> str:
    for k in keys:
        v = it.get(k)
        if v is None:
            continue
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


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
# Geocoding / Distanz / Matching / Enrichment
# ============================================================
@st.cache_data(ttl=7 * 24 * 3600, show_spinner=False)
def geocode_nominatim(query: str) -> Tuple[Optional[float], Optional[float], Optional[str]]:
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


def build_queries() -> Dict[str, str]:
    return {
        "Breit (ohne Suchtext)": "",
        "Leitung (breit)": "Leiter Teamleiter Laborleiter Gruppenleiter Abteilungsleiter Bereichsleiter Head Director",
        "Projektmanagement": "Projektmanager Project Manager Program Manager Technical Project",
        "Technischer Vertrieb": "Sales Engineer Technischer Vertrieb Key Account Business Development",
        "R&D / Entwicklung": "Forschung Entwicklung R&D Engineer Scientist",
    }


def match_target_org(company: str) -> Optional[Dict[str, Any]]:
    c = (company or "").lower()
    if not c.strip():
        return None
    for org in TARGET_ORGS:
        if any(m in c for m in org.get("match", [])):
            return org
    return None


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


def is_recruiting_company(company: str) -> bool:
    c = normalize_text(company)
    if not c:
        return False
    return any(k in c for k in RECRUITING_COMPANY_KEYWORDS)


def is_recruiting_posting(it: Dict[str, Any]) -> bool:
    company = item_company(it)
    if is_recruiting_company(company):
        return True

    text = normalize_text(
        " ".join(
            [
                item_title(it),
                str(it.get("kurzbeschreibung", "")),
                str(it.get("beschreibung", "")),
                str(it.get("jobbeschreibung", "")),
                str(it.get("stellenbeschreibung", "")),
                company,
            ]
        )
    )
    return any(p in text for p in RECRUITING_TEXT_PATTERNS)


def enrich_item(
    it: Dict[str, Any],
    home_lat: float,
    home_lon: float,
    focus_keywords: List[str],
    leadership_keywords: List[str],
    negative_keywords: List[str],
    ho_bonus_val: int,
    speed_kmh: float,
) -> Dict[str, Any]:
    title = item_title(it)
    company = item_company(it)
    location = pretty_location(it)

    score, parts = score_breakdown(
        it,
        focus_keywords,
        leadership_keywords,
        negative_keywords,
        int(ho_bonus_val),
    )

    dist = distance_from_home_km(it, float(home_lat), float(home_lon))
    t_min = travel_time_minutes(dist, float(speed_kmh))
    org = match_target_org(company)

    it["_title"] = title
    it["_title_safe"] = sanitize_md_text(title)
    it["_company"] = company
    it["_company_safe"] = sanitize_md_text(company)
    it["_location"] = location
    it["_location_safe"] = sanitize_md_text(location)
    it["_score"] = score
    it["_score_parts"] = parts
    it["_distance_km"] = dist
    it["_travel_min"] = t_min
    it["_org"] = org
    it["_is_leadership"] = looks_leadership_strict(it)
    return it


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
# Karte
# ============================================================
def leaflet_map_html(
    home_lat: float,
    home_lon: float,
    home_label: str,
    markers: List[Dict[str, Any]],
    max_distance_km: float,
    height_px: int = 520,
) -> str:
    radius_m = int(max_distance_km * 1000)
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

  L.circle([{home_lat}, {home_lon}], {{
    radius: {radius_m},
    color: "#1565c0",
    weight: 2,
    fillColor: "#1565c0",
    fillOpacity: 0.08
  }}).addTo(map);

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
      + (dist != null ? '<br/>Dist: ' + dist + ' km' : '');

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
# App-Start
# ============================================================
st.set_page_config(page_title="JobWatch Leipzig", layout="wide")
st.title("Raum Leipzig – Jobs finden & vergleichen")

if "kw_focus" not in st.session_state:
    st.session_state["kw_focus"] = keywords_to_text(DEFAULT_FOCUS_KEYWORDS)
if "kw_lead" not in st.session_state:
    st.session_state["kw_lead"] = keywords_to_text(DEFAULT_LEADERSHIP_KEYWORDS)
if "kw_neg" not in st.session_state:
    st.session_state["kw_neg"] = keywords_to_text(DEFAULT_NEGATIVE_KEYWORDS)

favorites = load_favorites()
snap = load_snapshot()

size = 100
api_key = API_KEY_DEFAULT
debug = False
near_km = 25
mid_km = 60
speed_kmh = 75
ho_bonus = 0
max_pages = 100
max_results = 1000

with st.sidebar:
    st.header("JobWatch – Einstellungen")

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
        last_q = (st.session_state.get("home_query_last") or "").strip()
        if q == last_q:
            return
        st.session_state["home_query_last"] = q

        if len(q) < 4:
            st.session_state["geocode_error"] = "Bitte etwas genauer eingeben (z.B. '06242 Braunsbedra')."
            return

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

    st.subheader("Wohnort")
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
    st.subheader("Suche")

    max_distance_filter = st.slider("Maximale Entfernung (km)", 10, 200, 35, 5)
    st.caption("Dieser Radius bestimmt sowohl die BA-Suche als auch die angezeigten Jobs.")
    umkreis = int(max_distance_filter)

    aktualitaet_option = st.selectbox("Aktualität", ["7 Tage", "30 Tage", "60 Tage", "180 Tage", "Alle"], index=2)
    aktualitaet = None if aktualitaet_option == "Alle" else int(aktualitaet_option.split()[0])

    queries = build_queries()
    selected_profiles = st.multiselect("Jobarten", list(queries.keys()), default=["Breit (ohne Suchtext)"])

    st.divider()
    st.subheader("Filter")
    only_focus = st.checkbox("Nur passende Treffer anzeigen", value=True)
    min_score = st.slider("Mindest-Relevanz", 0, 80, 6, 1)
    hide_irrelevant = st.checkbox("Offensichtlich unpassende Treffer ausblenden", value=True)
    hide_marked = st.checkbox("Bereits ausgeblendete Jobs verbergen", value=True)
    show_hidden_manage = st.checkbox("Ausblend-Liste verwalten", value=False)

st.info(
    "Die bereinigte Version ist hier absichtlich kompakter gehalten. "
    "Wenn du möchtest, sende ich dir im nächsten Schritt den kompletten BA-Tab "
    "und danach den kompletten Firmencheck-Tab als direkt einsetzbare Fortsetzung."
)
