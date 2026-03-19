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
import re
from persistence import *
init_db()

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


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
    "laborleitung", "laboratory", "testing", "measurement systems", "characterization",
    "heat transfer", "materials science", "materials testing",
]

DEFAULT_LEADERSHIP_KEYWORDS = [
    "laborleiter", "teamleiter", "gruppenleiter", "abteilungsleiter", "bereichsleiter",
    "leiter", "head", "lead", "director", "manager", "principal",
    "sektionsleiter", "section manager", "supervisor",
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
    "schweißer", "bauleiter", "kraftfahrer", "schichtleiter",
    "metallhelfer", "metallbauer", "industriemechaniker", "chemielaborant", "vorarbeiter",
    "metallbearbeitung",
    "lackierer", "monteur", "lüftungsbauer", "blechbearbeiter", "helfer",
    "maschinist", "rohrverrichter", "metallfacharbeiter", "metallbearbeiter", "tischler",
    "assistenz", "sekretariat", "vorstandsassistenz", "marktleiter",
    "insurance", "versicherung", "minijob", "steuerfachangestellte", "sachbearbeiter",
    "personalreferent", "junior", "bürosachbearbeitung", "büroassistenz", "büroassistent",
    "facharzt", "integrationshelfer", "empfangsleiter", "schulbegleiter", "held", "filialleiter",
    "informatiker", "wirtschaftsinformatiker", "programmleiter",
    "frontend", "backend", "developer", "software developer", "mobile", "android",
    "ios", "retail", "ecommerce", "seo", "social media", "content manager",
    "data scientist", "data engineer", "machine learning", "ai engineer", "fullstack",
    "web developer", "cloud engineer", "devops", "react", "angular", "nodejs", "php developer",
    "javascript developer", "mobile developer", "ios developer", "android developer",
    "campaign manager", "automotive software", "security engineer",
    "data processing", "streaming", "procurement", "commodity manager", "digital",
    "campaign", "audience", "data strategy", 
    "tendering", "food systems", "libya", "middle east", 
    "crm campaign", "audience manager", "marketing manager", "social media manager",
    "advertising", "growth manager", "baustoff", "klinische chemie", "immunologie",
    "mtla", "mta", "mtl", "fahrzeug", "automotive", "application manager sap",
    "application manager",
    "software",
    "it ",
    "cloud",
    "saas",
    "plattform",
    "ki ",
    "ai ",
    "solution architect",  
]

GLOBAL_BAD_KEYWORDS = [
    "software",
    "it ",
    "cloud",
    "saas",
    "plattform",
    "ki ",
    "ai ",
    "machine learning",
    "solution architect",
    "enterprise",
    "sap",
]

# ist für alle Jobarten aktiv:
BAD_MESSTECHNIK_TITLES = [
    "techniker",
    "messtechniker",
    "servicetechniker",
    "schweißtechniker",
    "welding",
    "obermonteur",
    "monteur",
    "montage",
    "inbetriebnahme",
    "wartung",
    "installation",
    "field service",
    "außendienst",
    "production",
    "fertigung",
    "schicht",
    "schweißer",
    "schweiß",
    "elektriker",
    "mechatroniker",
    "glasfaser",
    "cad-konstrukteur",
    "konstrukteur",
    "einkäufer",
    "strategischer einkäufer",
    "cost manager",
    "cost engineer",
    "financial accountant",
    "vertriebsinnendienst",
    "instandhalter",
    "instandhaltung",
    "msr",
    "emsr",
    "schlaflabor",
    "nachtdienst",
    "sachbearbeiter",
    "kaufmännisch",
    "innendienst",
    "augenoptik",
    "produktionsmitarbeiter",
    "produktions",
    "mitarbeiter",
    "chemielaborant",
    "chemielaborantin",
    "laborant",
    "laborantin",
    "kristallzucht",
    "sharepoint",
    "chemisch-technischer",
    "cta",
    "elektrotechnik",
    "betriebskosten",
    "emv",
]

MESSTECHNIK_REQUIRED_HINTS = [
    "labor",
    "analytik",
    "analysis",
    "instrument",
    "instrumentation",
    "metrology",
    "messtechnik",
    "prüftechnik",
    "prüfung",
    "material",
    "materials",
    "werkstoff",
    "charakterisierung",
    "characterization",
    "physik",
    "physics",
    "wissenschaft",
    "scientific",
    "applikation",
    "application",
]

MESSTECHNIK_GOOD_TITLE_HINTS = [
    "ingenieur",
    "engineer",
    "scientist",
    "wissenschaftlicher mitarbeiter",
    "Application Engineer Scientific Instruments",
    "Field Application Scientist",
    "Application Scientist",
    "application scientist",
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
    "aerb personal", "jobkraft personalmanagement", "walter-fach-kraft industrie", "ifas personalmanagement",
]

RECRUITING_TEXT_PATTERNS = [
    "im auftrag unseres kunden", "im auftrag eines kunden", "für unseren kunden", "für einen unserer kunden",
    "für einen namhaften kunden", "unser kunde", "unser mandant", "im rahmen der personalvermittlung",
    "im rahmen der direktvermittlung", "direktvermittlung", "personalvermittlung", "vermittlungsgutschein",
    "zeitarbeit", "arbeitnehmerüberlassung", "aü", "aueg", "arbeitnehmerueberlassung", "temp to perm",
]

INDUSTRY_KEYWORDS = [
    "instrument", "instrumentation", "measurement", "measuring", "metrology", "testing", "test system",
    "laboratory", "labor", "materials", "material testing", "material analysis", "thermal", "therm",
    "physics", "scientific", "analytical", "instrumente", "messtechnik", "prüftechnik", "werkstoff",
    "materialprüfung",
]

COMPANY_BOOST_KEYWORDS = [
    "netzsch", "zeiss", "spectris", "mettler", "mettler toledo", "anton paar", "perkinelmer",
    "ta instruments", "waters", "bruker", "malvern", "malvern panalytical", "shimadzu",
    "horiba", "agilent", "eurofins", "sgs", "gba", "dekra", "wacker", "basf", "linde",
    "air liquide", "siemens energy", "fraunhofer", "ufz", "dbfz", "iom", "imws",
]

TITLE_BOOST_KEYWORDS = [
    "instrumentation", "instrument", "measurement", "measuring", "metrology", "materials",
    "material testing", "materials testing", "laboratory", "labor", "thermal", "scientific",
    "analytical", "messtechnik", "prüftechnik", "materialprüfung", "werkstoff",
]

LAB_LEADERSHIP_TITLE_KEYWORDS = [
    "laborleiter", "leiter prüflabor", "leiter materiallabor",
    "head of laboratory", "laboratory manager",
    "teamleiter labor", "teamleiter analytik",
    "gruppenleiter labor", "laborleiter qualitätskontrolle",
    "stellvertretender laborleiter", "teamleiter qualitätskontrolle",
    "leiter qualitätskontrolle",
]

INDUSTRY_TERMS_TO_TRACK = [
    "instrumentation", "instrument", "measurement", "measuring", "metrology", "materials",
    "material testing", "materials testing", "laboratory", "labor", "thermal", "scientific",
    "analytical", "messtechnik", "prüftechnik", "materialprüfung", "werkstoff",
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
    s = (s or "").lower()
    s = s.replace("-", " ")
    s = s.replace("/", " ")
    s = s.replace("\n", " ").replace("\r", " ")
    return " ".join(s.split())

def ba_headers(api_key: str) -> Dict[str, str]:
    return {
        "User-Agent": "Jobsuche/2.9.2 (de.arbeitsagentur.jobboerse; build:1077) Streamlit",
        "X-API-Key": api_key,
        "Accept": "application/json",
        "Connection": "keep-alive",
    }

def normalize_job_title(title: str) -> str:
    t = (title or "").strip()

    if ":" in t:
        t = t.split(":", 1)[1].strip()

    t = re.sub(r"\(.*?\)", "", t)
    t = t.replace("*", "")
    t = re.sub(r"\s+", " ", t).strip()

    return t

def industry_score_boost(it: Dict[str, Any], keywords: List[str]) -> int:
    text = " ".join(
        [
            str(item_title(it)),
            str(it.get("kurzbeschreibung", "")),
            str(it.get("beschreibung", "")),
            str(item_company(it)),
            str(pretty_location(it)),
        ]
    ).lower()

    score = 0

    for kw in keywords:
        if keyword_match(text, kw):
            score += 2

    return score

def company_score_boost(it: Dict[str, Any], keywords: List[str]) -> int:
    company = str(item_company(it)).lower()

    score = 0

    for kw in keywords:
        if kw and kw in company:
            score += 6

    return score

def title_score_boost(it: Dict[str, Any], keywords: List[str]) -> int:
    title = str(item_title(it)).lower()

    score = 0

    for kw in keywords:
        if kw and kw in title:
            score += 6

    return score

def lab_leadership_title_boost(it: Dict[str, Any], keywords: List[str]) -> int:
    title = str(item_title(it)).lower()
    score = 0

    for kw in keywords:
        if keyword_match(title, kw):
            score += 10

    return score

def keyword_match(text: str, kw: str) -> bool:
    if not kw:
        return False

    pattern = r"\b" + re.escape(kw.lower()) + r"\b"
    return re.search(pattern, text.lower()) is not None

def is_favorited(job_key: str, favs: Dict[str, Any]) -> bool:
    return bool(job_key) and job_key in favs

def passes_profile_specific_filter(it: Dict[str, Any]) -> bool:
    profile = str(it.get("_profile", "")).strip().lower()

    text = " ".join(
        [
            str(item_title(it)),
            str(it.get("kurzbeschreibung", "")),
            str(it.get("beschreibung", "")),
            str(item_company(it)),
            str(pretty_location(it)),
        ]
    ).lower()

    title = str(item_title(it)).lower()

    if profile == "messtechnik":
        has_content_hint = any(h in text for h in MESSTECHNIK_REQUIRED_HINTS)
        has_title_hint = any(h in title for h in MESSTECHNIK_GOOD_TITLE_HINTS)

        return has_content_hint and has_title_hint

    return True

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
    berufsfeld: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    params: Dict[str, Any] = {
        "angebotsart": "1",
        "page": str(page),
        "pav": "false",
        "size": str(size),
        "umkreis": str(umkreis_km),
        "wo": wo,
    }
    if berufsfeld:
        params["berufsfeld"] = berufsfeld

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

def build_queries():
    return {
        "Laborleiter Messtechnik": {
            "was": "Laborleiter Messtechnik",
            "berufsfeld": ""
        },
        "Laborleiter Materialprüfung": {
            "was": "Laborleiter Materialprüfung",
            "berufsfeld": ""
        },
        "Laborleiter Analytik": {
            "was": "Laborleiter Analytik",
            "berufsfeld": ""
        },
        "Laboratory Manager Testing": {
            "was": "Laboratory Manager Testing",
            "berufsfeld": ""
        },
        "Head of Laboratory Materials": {
            "was": "Head of Laboratory Materials",
            "berufsfeld": ""
        },
        "Teamleiter Labor": {
            "was": "Teamleiter Labor",
            "berufsfeld": ""
        },
        "Teamleiter Materialprüfung": {
            "was": "Teamleiter Materialprüfung",
            "berufsfeld": ""
        },
        "Gruppenleiter Messtechnik": {
            "was": "Gruppenleiter Messtechnik",
            "berufsfeld": ""
        },
        "Messtechnik": {
            "was": "Messtechnik",
            "berufsfeld": ""
        },
        "Materialprüfung": {
            "was": "Materialprüfung",
            "berufsfeld": ""
        },
        "Thermoanalyse": {
            "was": "Thermoanalyse",
            "berufsfeld": ""
        },
        "Thermophysik": {
            "was": "Thermophysik",
            "berufsfeld": ""
        },
        "Thermal Analysis": {
            "was": "Thermal Analysis",
            "berufsfeld": ""
        },
        "Thermophysical": {
            "was": "Thermophysical",
            "berufsfeld": ""
        },
        "Heat Transfer": {
            "was": "Heat Transfer",
            "berufsfeld": ""
        },
        "Research Engineer": {
            "was": "Research Engineer",
            "berufsfeld": ""
        },
        "Scientist Materials": {
            "was": "Scientist Materials",
            "berufsfeld": ""
        },
        "Wissenschaftlicher Mitarbeiter Thermodynamik": {
            "was": "Wissenschaftlicher Mitarbeiter Thermodynamik",
            "berufsfeld": ""
        },
        "Produktmanager Messtechnik": {
            "was": "Produktmanager Messtechnik",
            "berufsfeld": ""
        },
        "Technical Product Manager Instruments": {
            "was": "Technical Product Manager Instruments",
            "berufsfeld": ""
        },
        "Product Line Manager Instruments": {
            "was": "Product Line Manager Instruments",
            "berufsfeld": ""
        },
        "Application Scientist": {
            "was": "Application Scientist",
            "berufsfeld": ""
        },
        "Application Engineer": {
            "was": "Application Engineer",
            "berufsfeld": ""
        },
        "Field Application Engineer": {
            "was": "Field Application Engineer",
            "berufsfeld": ""
        },
        #"Application Manager": {
        #    "was": "Application Manager",
        #    "berufsfeld": ""
        #},
        "Technical Specialist": {
            "was": "Technical Specialist",
            "berufsfeld": ""
        },
        "Scientific Consultant": {
            "was": "Scientific Consultant",
            "berufsfeld": ""
        },
        "Product Specialist": {
            "was": "Product Specialist",
            "berufsfeld": ""
        },
        "Technical Sales Support": {
            "was": "Technical Sales Support",
            "berufsfeld": ""
        },
        "Breit": {
            "was": "",
            "berufsfeld": ""
        },
        "Leiter Prüflabor": {
            "was": "Leiter Prüflabor",
            "berufsfeld": ""
        },
        "Laborleiter Qualitätskontrolle": {
            "was": "Laborleiter Qualitätskontrolle",
            "berufsfeld": ""
        },
        "Teamleiter Analytik": {
            "was": "Teamleiter Analytik",
            "berufsfeld": ""
        },
        "Leiter Materiallabor": {
            "was": "Leiter Materiallabor",
            "berufsfeld": ""
        },
        "Prüflabor Messtechnik": {
            "was": "Prüflabor Messtechnik",
            "berufsfeld": ""
        },
        "Materialcharakterisierung": {
            "was": "Materialcharakterisierung",
            "berufsfeld": ""
        },
        "Werkstoffprüfung Labor": {
            "was": "Werkstoffprüfung Labor",
            "berufsfeld": ""
        },
        "Applikationslabor": {
            "was": "Applikationslabor",
            "berufsfeld": ""
        },
        "Application Engineer Scientific Instruments": {
            "was": "Application Engineer Scientific Instruments",
            "berufsfeld": ""
        },
        "Field Application Scientist": {
            "was": "Field Application Scientist",
            "berufsfeld": ""
        }
    }

def build_query_groups():
    return {
        "Laborleitung / Führung": [
            "Laborleiter Messtechnik",
            "Laborleiter Materialprüfung",
            "Laborleiter Analytik",
            "Leiter Prüflabor",
            "Laborleiter Qualitätskontrolle",
            "Teamleiter Labor",
            "Teamleiter Analytik",
            "Teamleiter Materialprüfung",
            "Leiter Materiallabor",
            "Head of Laboratory Materials",
            "Gruppenleiter Messtechnik",
        ],
        "Labor / Materialcharakterisierung / Messtechnik": [
            "Messtechnik",
            "Materialprüfung",
            "Materialcharakterisierung",
            "Werkstoffprüfung Labor",
            "Prüflabor Messtechnik",
            "Applikationslabor",
        ],
        "Thermal / Thermoanalyse / Thermophysik": [
            "Thermoanalyse",
            "Thermophysik",
            "Thermal Analysis",
            "Thermophysical",
            "Heat Transfer",
            "Wissenschaftlicher Mitarbeiter Thermodynamik",
        ],
        "Application / Scientific Support": [
            "Application Scientist",
            "Application Engineer",
            "Field Application Engineer",
            "Application Engineer Scientific Instruments",
            "Field Application Scientist",
            # "Application Manager",
            "Technical Specialist",
            "Scientific Consultant",
            "Product Specialist",
            "Technical Sales Support",
        ],
        "Produktmanagement / Strategie": [
            "Produktmanager Messtechnik",
            "Technical Product Manager Instruments",
            "Product Line Manager Instruments",
        ],
        "Forschung / Entwicklung": [
            "Research Engineer",
            "Scientist Materials",
        ],
        "Breite Suche": [
            "Breit",
        ],
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
        if keyword_match(text, k):
            score += 8
            parts.append(f"+8 {k}")

    for k in leadership_keywords:
        if keyword_match(text, k):
            score += 5
            parts.append(f"+5 {k}")

    for k in negative_keywords:
        if keyword_match(text, k):
            score -= 4
            parts.append(f"−4 {k}")

    if is_homeoffice_item(it) and ho_bonus_val > 0:
        score += int(ho_bonus_val)
        parts.append(f"+{int(ho_bonus_val)} homeoffice")

    if not parts:
        parts = ["(keine Keyword-Treffer)"]

    return score, parts


def is_probably_irrelevant(it: Dict[str, Any], negative_keywords: List[str]) -> bool:
    text = " ".join([
        item_title(it),
        item_company(it),
        it.get("kurzbeschreibung", ""),
    ]).lower()

    text = text.replace("-", " ")

    for kw in negative_keywords:
        if keyword_match(text, kw):
            return True

    return False


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

    boost = industry_score_boost(it, INDUSTRY_KEYWORDS)
    score += boost
    
    if boost > 0:
        parts.append(f"+{boost} industrie")
    
    company_boost = company_score_boost(it, COMPANY_BOOST_KEYWORDS)
    score += company_boost
    
    if company_boost > 0:
        parts.append(f"+{company_boost} firma")

    title_boost = title_score_boost(it, TITLE_BOOST_KEYWORDS)
    score += title_boost
    
    if title_boost > 0:
        parts.append(f"+{title_boost} titel")

    lab_title_boost = lab_leadership_title_boost(it, LAB_LEADERSHIP_TITLE_KEYWORDS)
    score += lab_title_boost
    
    if lab_title_boost > 0:
        parts.append(f"+{lab_title_boost} labor-leitung")
    
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
st.markdown(
    """
<div style="
font-size:1.6rem;
font-weight:700;
margin-bottom:6px;">
🧭 JobWatch Leipzig
</div>

<div style="
font-size:0.95rem;
opacity:0.8;
margin-bottom:14px;">
Jobs im Raum Leipzig entdecken, vergleichen und priorisieren
</div>
""",
    unsafe_allow_html=True,
)
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

    max_distance_filter = st.slider("Maximale Entfernung (km)", 10, 200, 50, 5)
    st.caption("Dieser Radius bestimmt sowohl die BA-Suche als auch die angezeigten Jobs.")
    umkreis = int(max_distance_filter)

    aktualitaet_option = st.selectbox(
        "Aktualität",
        ["7 Tage", "30 Tage", "60 Tage", "180 Tage", "Alle"],
        index=2,
    )
    aktualitaet = None if aktualitaet_option == "Alle" else int(aktualitaet_option.split()[0])

    #----manuelle Auswahl deaktiviert -------------------------------
    if False: 
        queries = build_queries()   
        selected_profiles = st.multiselect(
            "Jobarten",
            list(queries.keys()),
            default=[
                "Laborleiter Messtechnik",
                "Laborleiter Materialprüfung",
                "Laborleiter Analytik",
                "Leiter Prüflabor",
                "Laborleiter Qualitätskontrolle",
                "Teamleiter Labor",
                "Teamleiter Analytik",
                "Teamleiter Materialprüfung",
                "Leiter Materiallabor",
                "Head of Laboratory Materials",
                "Applikationslabor",
                "Prüflabor Messtechnik",
                "Materialcharakterisierung",
                "Werkstoffprüfung Labor",
                "Thermoanalyse",
                "Thermal Analysis",
                "Application Scientist",
                "Application Engineer",
                "Field Application Engineer",
                "Application Engineer Scientific Instruments",
                "Field Application Scientist",
                "Produktmanager Messtechnik",
                "Technical Product Manager Instruments",
                "Product Line Manager Instruments",
            ]
        )
    # Ende deaktiviert------------------------------------------------
    queries = build_queries()
    query_groups = build_query_groups()
    
    all_profiles = list(queries.keys())
    default_profiles = [k for k in all_profiles if k != "Breit"]
    
    
    # --- Initialisierung ---
    if "selected_profiles_ui" not in st.session_state:
        st.session_state["selected_profiles_ui"] = default_profiles.copy()
    
    
    # --- Hilfsfunktionen ---
    def set_profile_selection(selection):
        st.session_state["selected_profiles_ui"] = selection.copy()
        for group_name, group_items in query_groups.items():
            st.session_state[f"group_{group_name}"] = [
                x for x in group_items if x in selection
            ]
    
    
    def detect_mode(selection):
        selection_set = set(selection)
    
        if selection_set == set(all_profiles):
            return "Alles"
    
        if selection_set == set(default_profiles):
            return "Standard"
    
        if not selection_set:
            return "Reset"
    
        return "Eigene Auswahl"
    
    
    def group_state(selected, total):
        if selected == 0:
            return "⚪"
        if selected == total:
            return "✅"
        return "🟢"
    
    
    # --- Anzeige ---
    st.markdown("### Jobarten")
    
    mode = detect_mode(st.session_state["selected_profiles_ui"])
    total_selected = len(st.session_state["selected_profiles_ui"])
    
    chip_html = f"""
    <div style="
        display:inline-block;
        padding:4px 10px;
        border-radius:999px;
        background:#e0e0e0;
        color:#333;
        font-size:0.85rem;
        font-weight:600;
    ">
        Modus: {mode} · {total_selected}/{len(all_profiles)}
    </div>
    """
    
    st.markdown(chip_html, unsafe_allow_html=True)
        
    # --- Buttons ---
    # --- Modus bestimmen ---
    mode = detect_mode(st.session_state["selected_profiles_ui"])
    
    # Optionen (inkl. Eigene Auswahl)
    radio_options = ["Alles", "Standard", "Eigene Auswahl", "Reset"]
    
    # Fallback (sollte selten nötig sein)
    if mode not in radio_options:
        mode_for_ui = "Eigene Auswahl"
    else:
        mode_for_ui = mode
    
    # --- UI ---
    selected_mode = st.radio(
        "Schnellauswahl",
        radio_options,
        index=radio_options.index(mode_for_ui),
        horizontal=True,
        label_visibility="collapsed",
    )
    
    # --- Reaktion auf Auswahl ---
    if selected_mode == "Alles":
        if set(st.session_state["selected_profiles_ui"]) != set(all_profiles):
            set_profile_selection(all_profiles)
            st.rerun()
    
    elif selected_mode == "Standard":
        if set(st.session_state["selected_profiles_ui"]) != set(default_profiles):
            set_profile_selection(default_profiles)
            st.rerun()
    
    elif selected_mode == "Reset":
        if st.session_state["selected_profiles_ui"]:
            set_profile_selection([])
            st.rerun()
    
    # 👉 Wichtig:
    # Bei "Eigene Auswahl" passiert absichtlich nichts!
    
    
    # --- Gruppen ---
    for group_name, group_items in query_groups.items():
        key = f"group_{group_name}"
    
        if key not in st.session_state:
            st.session_state[key] = [
                x for x in group_items
                if x in st.session_state["selected_profiles_ui"]
            ]
    
        selected_count = len(st.session_state[key])
        total_count = len(group_items)
        icon = group_state(selected_count, total_count)
    
        with st.expander(f"{icon} {group_name} · {selected_count}/{total_count}", expanded=False):
    
            col_a, col_b = st.columns(2)
    
            with col_a:
                if st.button(f"Alle ({group_name})", key=f"all_{group_name}"):
                    st.session_state[key] = group_items.copy()
                    st.rerun()
    
            with col_b:
                if st.button(f"Keine ({group_name})", key=f"none_{group_name}"):
                    st.session_state[key] = []
                    st.rerun()
    
            st.multiselect(
                group_name,
                group_items,
                key=key,
                label_visibility="collapsed",
            )
    
    
    # --- Gesamtauswahl zusammenbauen ---
    selected_set = set()
    
    for group_name in query_groups:
        key = f"group_{group_name}"
        selected_set.update(st.session_state.get(key, []))
    
    selected_profiles = [x for x in all_profiles if x in selected_set]
    st.session_state["selected_profiles_ui"] = selected_profiles
    
    st.divider()
    st.subheader("Filter")
    only_focus = st.checkbox("Nur passende Treffer anzeigen", value=True)
    min_score = st.slider("Mindest-Relevanz", 0, 80, 6, 1)
    hide_irrelevant = st.checkbox("Offensichtlich unpassende Treffer ausblenden", value=True)
    hide_marked = st.checkbox("Bereits ausgeblendete Jobs verbergen", value=True)
    show_hidden_manage = st.checkbox("Ausblend-Liste verwalten", value=False)

    st.divider()

    with st.expander("Erweitert", expanded=False):
        st.caption("Nur wenn du feintunen oder debuggen willst.")

        st.markdown("**Suche-Breite**")
        max_pages = st.slider("Max. Seiten pro Jobart", 1, 100, 100, 1)
        max_results = st.slider("Stopp bei max. Treffern", 100, 10000, 2000, 100)
        st.caption(f"Techn. Maximum: {int(max_pages) * size}")
        st.caption(f"App-Limit: {int(max_results)}")

        enable_job_geocode = st.checkbox(
            "Fehlende Koordinaten für Karte nachschlagen (langsamer)",
            value=False
        )
        max_job_geocodes = st.slider("Max. Geocoding pro Lauf", 0, 50, 10, 5)

        st.markdown("**Entfernung / Fahrzeit**")
        near_km = st.slider("Grün bis (km)", 5, 80, 10, 5)
        mid_km = st.slider("Gelb bis (km)", 10, 150, 35, 5)
        speed_kmh = st.slider("Ø Geschwindigkeit (km/h)", 30, 140, 75, 5)

        umkreis = int(max_distance_filter)

        st.divider()
        st.markdown("**Score-Tuning**")
        ho_bonus = 0

        st.divider()
        st.markdown("**Technik**")
        api_key = st.text_input("X-API-Key (nur bei Problemen)", value=API_KEY_DEFAULT)
        
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
            
FOCUS_KEYWORDS = [k.lower() for k in parse_keywords(st.session_state["kw_focus"])]
LEADERSHIP_KEYWORDS = [k.lower() for k in parse_keywords(st.session_state["kw_lead"])]
NEGATIVE_KEYWORDS = [k.lower() for k in parse_keywords(st.session_state["kw_neg"])]

col1, col2 = st.columns([7.2, 1.2], gap="small")

with col2:
    st.markdown(
        """
        <style>
        .side-card {
            border: 1px solid rgba(128,128,128,0.25);
            border-radius: 14px;
            padding: 14px 14px 10px 14px;
            margin-bottom: 14px;
            background: rgba(255,255,255,0.02);
        }
        .side-title {
            font-size: 1.0rem;
            font-weight: 700;
            white-space: nowrap;
            margin-bottom: 4px;
        }
        .side-sub {
            font-size: 0.82rem;
            color: #666;
            margin-bottom: 10px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown('<div class="side-card">', unsafe_allow_html=True)
    st.markdown('<div class="side-title">💾 Snapshot</div>', unsafe_allow_html=True)

    snap_time = snap.get("timestamp") or "Noch kein Snapshot gespeichert"
    st.markdown(f'<div class="side-sub">{snap_time}</div>', unsafe_allow_html=True)

    if st.button("Stand speichern", use_container_width=True):
        st.session_state["save_snapshot_requested"] = True

    if st.button("Stand löschen", use_container_width=True):
        save_snapshot([])
        st.success("Snapshot gelöscht.")
        st.rerun()

    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown('<div class="side-card">', unsafe_allow_html=True)
    st.markdown('<div class="side-title">🎯 Organisationen</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="side-sub">Karriereseiten zum manuellen Prüfen</div>',
        unsafe_allow_html=True
    )

    with st.expander("Liste anzeigen", expanded=False):
        for org in TARGET_ORGS:
            try:
                st.link_button(
                    f"🏢 {org['name']}",
                    org["url"],
                    use_container_width=True
                )
            except Exception:
                st.markdown(f"[🏢 {org['name']}]({org['url']})")

    st.markdown("</div>", unsafe_allow_html=True)

with col1:
    tab_ba, tab_company = st.tabs(["BA-Suche", "Firmencheck (manuell)"])
    markers = []
    with tab_ba:
        if not selected_profiles:
            st.warning("Bitte mindestens eine Jobart auswählen.")
            st.stop()

        all_items: List[Dict[str, Any]] = []

        _hidden_data = load_hidden_jobs()
        hidden_keys: Set[str] = set(_hidden_data.get("hidden", []))
        hidden_companies = load_hidden_companies()

        if show_hidden_manage:
            st.subheader("🙈 Ausblend-Liste")

            st.markdown("**Ausgeblendete Jobs**")
            st.caption(f"{len(hidden_keys)} Jobs ausgeblendet (Stand: {_hidden_data.get('updated_at') or '—'})")

            cHM1, cHM2 = st.columns([1.2, 3.8])
            with cHM1:
                if st.button("🧹 Jobs leeren", key="clear_hidden_jobs"):
                    save_hidden_jobs(set())
                    st.success("Ausblend-Liste für Jobs geleert.")
                    st.rerun()
            with cHM2:
                if hidden_keys:
                    st.code("\n".join(sorted(hidden_keys)))
                else:
                    st.caption("Keine Jobs ausgeblendet.")

            st.divider()

            st.markdown("**Blockierte Firmen**")
            st.caption(f"{len(hidden_companies)} Firmen blockiert")

            cHC1, cHC2 = st.columns([1.2, 3.8])
            with cHC1:
                if st.button("🧹 Firmen leeren", key="clear_hidden_companies"):
                    save_hidden_companies(set())
                    st.success("Blockierte Firmen geleert.")
                    st.rerun()
            with cHC2:
                if hidden_companies:
                    for comp in sorted(hidden_companies):
                        c1, c2 = st.columns([4.5, 1])
                        with c1:
                            st.write(comp)
                        with c2:
                            if st.button("❌", key=f"unblock_company_{comp}"):
                                hidden_companies.discard(comp)
                                save_hidden_companies(hidden_companies)
                                st.rerun()
                else:
                    st.caption("Keine Firmen blockiert.")

            st.divider()

        live_status = st.empty()
        live_progress = st.progress(0)
        live_hint = st.empty()

        with st.spinner("Suche läuft…"):
            all_items = []
            errs: List[str] = []
            qmap = build_queries()
        
            profile_counter = {name: 0 for name in selected_profiles}
            title_counter = {}
            industry_term_counter = {}

            total_limit = int(max_results)
            pages_limit = int(max_pages)
            done_pages = 0
            expected_pages = max(1, len(selected_profiles) * pages_limit)

            for name in selected_profiles:

                profile = qmap.get(name, {"was": "", "berufsfeld": ""})
            
                q = profile.get("was", "")
                berufsfeld = profile.get("berufsfeld", "")
            
                for page in range(1, pages_limit + 1):
            
                    if len(all_items) >= total_limit:
                        break
                    done_pages += 1
                    pct = min(1.0, done_pages / expected_pages)

                    live_status.markdown(
                        f"**Live:** Profil **{name}** · Seite **{page}/{pages_limit}** · Treffer **{len(all_items)}/{total_limit}**"
                    )
                    live_progress.progress(int(pct * 100))

                    items_local, e1 = fetch_search(
                        api_key,
                        home_query,
                        int(umkreis),
                        q,
                        aktualitaet,
                        int(size),
                        page=page,
                        arbeitszeit=None,
                        berufsfeld=berufsfeld,
                    )

                    if e1:
                        errs.append(f"{name} Seite {page}: {e1}")
                        break

                    if not items_local:
                        break
                    
                    for it in items_local:
                        it["_profile"] = name
                        it["_bucket"] = f"Vor Ort ({umkreis} km)"
                        all_items.append(it)
                    
                        profile_counter[name] = profile_counter.get(name, 0) + 1
                    
                    if len(items_local) < int(size):
                        break

        live_progress.progress(100)
        status_top = st.empty()               
        if errs:
            st.error("Fehler / Hinweise")
            for e in errs:
                st.code(e)

        # 1) Deduplizierung per Key
        items_now: List[Dict[str, Any]] = []
        seen: Set[str] = set()
        for it in all_items:
            k = item_key(it)
            if k not in seen:
                seen.add(k)
                it["_key"] = k
                items_now.append(it)

        # 2) Deduplizierung per Titel/Firma/Ort
        dedup2: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        for it in items_now:
            key2 = (
                item_title(it).lower().strip(),
                item_company(it).lower().strip(),
                pretty_location(it).lower().strip(),
            )
            if key2 not in dedup2:
                dedup2[key2] = it

        items_now = list(dedup2.values())

        # 3) Hidden / Blocklisten
        if hide_marked:
            items_now = [it for it in items_now if (it.get("_key") or item_key(it)) not in hidden_keys]

        items_now = [
            it for it in items_now
            if item_company(it).strip().lower() not in hidden_companies
        ]

        # 4) Recruiting automatisch raus
        before_recruiting_filter = len(items_now)
        items_now = [it for it in items_now if not is_recruiting_posting(it)]

        removed_recruiting = before_recruiting_filter - len(items_now)

        # 4b) schlechte Keywords in allen Jobs raus (geändert, nicht nur Messtechnik!)
        items_now = [
            it for it in items_now
            if not any(
                bad in item_title(it).lower()
                for bad in BAD_MESSTECHNIK_TITLES
            )
        ]

        # 4c) Positivdefinition für bestimmte Profile
        items_now = [it for it in items_now if passes_profile_specific_filter(it)]

        # 4d) IT / Software hart rausfiltern
        items_now = [
            it for it in items_now
            if not any(
                bad in (
                    item_title(it) + " " + str(it.get("beschreibung", ""))
                ).lower()
                for bad in GLOBAL_BAD_KEYWORDS
            )
        ]
        
        # 5) Negative Jobs raus
        if hide_irrelevant:
            items_now = [it for it in items_now if not is_probably_irrelevant(it, NEGATIVE_KEYWORDS)]

        # 6) Einmalig anreichern
        items_now = [
            enrich_item(
                it,
                float(home_lat),
                float(home_lon),
                FOCUS_KEYWORDS,
                LEADERSHIP_KEYWORDS,
                NEGATIVE_KEYWORDS,
                int(ho_bonus),
                float(speed_kmh),
            )
            for it in items_now
        ]

        title_counter = {}
        industry_term_counter = {}
        
        for it in items_now:
            title = normalize_job_title(item_title(it))
            profile = str(it.get("_profile", "")).strip()
        
            if title:
                key = f"{title} [{profile}]"
                title_counter[key] = title_counter.get(key, 0) + 1
        
            text_for_terms = " ".join(
                [
                    str(item_title(it)),
                    str(it.get("kurzbeschreibung", "")),
                    str(it.get("beschreibung", "")),
                    str(item_company(it)),
                ]
            ).lower()
        
            for term in INDUSTRY_TERMS_TO_TRACK:
                if term in text_for_terms:
                    industry_term_counter[term] = industry_term_counter.get(term, 0) + 1    
        
        # 7) Mindestscore
        if only_focus:
            items_now = [it for it in items_now if int(it.get("_score", 0)) >= int(min_score)]

        prev_items = snap.get("items", [])
        prev_keys: Set[str] = {x.get("_key") or item_key(x) for x in prev_items if isinstance(x, dict)}
        now_keys: Set[str] = {x.get("_key") or item_key(x) for x in items_now}
        new_keys = now_keys - prev_keys

        def sort_key(it: Dict[str, Any]):
            org = it.get("_org")
            priority_rank = -1 if (org and org.get("priority") == "high") else 0
            dist = it.get("_distance_km")
            dist_rank = dist if dist is not None else 999999.0
            is_new_rank = 0 if (it.get("_key") in new_keys) else 1
            score = int(it.get("_score", 0))
            return (priority_rank, dist_rank, is_new_rank, -score, str(it.get("_title", "")).lower())

        # Radiusfilter
        items_now_filtered = []
        for it in items_now:
            dist = it.get("_distance_km")
            if dist is None:
                items_now_filtered.append(it)
                continue
            if dist <= float(max_distance_filter):
                items_now_filtered.append(it)

        items_sorted = sorted(items_now_filtered, key=sort_key)
       
        location_counter = {}
        location_distance = {}
        
        for it in items_sorted:
            loc = pretty_location(it)
            if not loc:
                continue
        
            city = loc.split(",")[0].strip()
            if not city:
                continue
        
            location_counter[city] = location_counter.get(city, 0) + 1
        
            dist = it.get("_distance_km")
            if dist is not None:
                if city not in location_distance:
                    location_distance[city] = dist
                else:
                    location_distance[city] = min(location_distance[city], dist)

        top_locations = sorted(
            location_counter.items(),
            key=lambda x: (
                location_distance.get(x[0], 999999),
                -x[1],
                x[0].lower()
            )
        )[:6]

        jump_target = st.session_state.get("jump_to_job")
        focus_company = st.session_state.get("focus_company")

        if jump_target:
            target_item = None
            other_items = []

            for it in items_sorted:
                k2 = it.get("_key") or item_key(it)
                if k2 == jump_target and target_item is None:
                    target_item = it
                else:
                    other_items.append(it)

            if target_item is not None:
                items_sorted = [target_item] + other_items

        elif focus_company:
            company_items = []
            other_items = []

            for it in items_sorted:
                comp_name = item_company(it).strip().lower()
                if comp_name == focus_company:
                    company_items.append(it)
                else:
                    other_items.append(it)

            items_sorted = company_items + other_items

        for i, it in enumerate(items_sorted, start=1):
            it["_idx"] = i

        if profile_counter:
            parts = [
                f"{p} {c}"
                for p, c in sorted(profile_counter.items(), key=lambda x: x[1], reverse=True)
            ]
            st.caption("Treffer pro Jobart: " + " | ".join(parts))
        if title_counter:
            top_titles = sorted(title_counter.items(), key=lambda x: x[1], reverse=True)[:10]
        
            parts = [f"{t} {c}" for t, c in top_titles]
        
            st.caption("Häufigste Jobtitel: " + " | ".join(parts))

        if industry_term_counter:
            top_terms = sorted(industry_term_counter.items(), key=lambda x: x[1], reverse=True)[:10]
            parts = [f"{term} {count}" for term, count in top_terms]
            st.caption("Top-Industriebegriffe: " + " | ".join(parts))
        
        company_counter: Dict[str, int] = {}
        for it in items_sorted:
            comp = item_company(it)
            if comp:
                company_counter[comp] = company_counter.get(comp, 0) + 1
        
        unique_companies = len(company_counter)
        
        if top_locations:
            loc_line = " | ".join(
                [
                    f"{city} {count}"
                    if city not in location_distance
                    else f"{city} {count} ({location_distance[city]:.0f} km)"
                    for city, count in top_locations
                ]
            )
                        
        # Firmen mit mehreren Treffern
        company_counter: Dict[str, int] = {}
        for it in items_sorted:
            comp = item_company(it)
            if comp:
                company_counter[comp] = company_counter.get(comp, 0) + 1
        unique_companies = len(company_counter)

        top_companies = sorted(
            [(c, n) for c, n in company_counter.items() if n > 1],
            key=lambda x: x[1],
            reverse=True
        )[:8]

        if top_companies:
            st.markdown(
                """
                <style>
            
                div[data-testid="stButton"] > button[kind="secondary"] {
                    border-radius: 999px;
                    padding: 0.18rem 0.6rem;
                    min-height: 0px;
                    font-size: 0.85rem;
                    line-height: 1.2;
                    text-align: left;
                    border: 1px solid rgba(128,128,128,0.25);
                    background: rgba(255,255,255,0.04);
                    font-weight: 500;
                }
            
                div[data-testid="stButton"] > button[kind="secondary"]:hover {
                    background: rgba(255,255,255,0.09);
                    border-color: rgba(128,128,128,0.45);
                }
            
                </style>
                """,
                unsafe_allow_html=True,
            )
            # st.caption("Klick auf eine Firma, um ihre Treffer nach oben zu holen.")
            st.markdown(
                """
                <style>
            
                div[data-testid="stButton"] > button[kind="secondary"] {
                    border-radius: 999px;
                    padding: 0.15rem 0.55rem;
                    min-height: 0px;
                    font-size: 0.85rem;
                    line-height: 1.2;
                    text-align: left;
                    border: 1px solid rgba(128,128,128,0.25);
                    background: rgba(255,255,255,0.03);
                    font-weight: 500;
                }
            
                div[data-testid="stButton"] > button[kind="secondary"]:hover {
                    border-color: rgba(128,128,128,0.45);
                    background: rgba(255,255,255,0.08);
                }
            
                </style>
                """,
                unsafe_allow_html=True,
            )

            if focus_company:
                c1, c2 = st.columns([5, 1.2])
                with c1:
                    st.info(f"🎯 Fokusfirma aktiv: {focus_company}")
                with c2:
                    if st.button("❌ Aufheben", key="clear_focus_company", use_container_width=True):
                        st.session_state["focus_company"] = None
                        st.session_state["jump_to_job"] = None
                        st.rerun()

            cols_per_row = 2
            rows = [top_companies[i:i + cols_per_row] for i in range(0, len(top_companies), cols_per_row)]

            for row_idx, row in enumerate(rows):
                cols = st.columns(cols_per_row)
                for col_idx, (comp, count) in enumerate(row):
                    with cols[col_idx]:
                        if count >= 5:
                            label = f"🔥 {comp} ({count})"
                        elif count >= 3:
                            label = f"⭐ {comp} ({count})"
                        else:
                            label = f"{comp} ({count})"
                        # label = f"{comp} · {count}"
                        if st.button(
                            label,
                            key=f"focus_company_chip_{row_idx}_{col_idx}_{comp}",
                            use_container_width=True,
                            type="secondary",
                        ):
                            st.session_state["focus_company"] = comp.strip().lower()
                            st.session_state["jump_to_job"] = None
                            st.rerun()

        # Beste Treffer
        top_items = sorted(
            items_sorted,
            key=lambda it: int(it.get("_score", 0)),
            reverse=True
        )[:5]

        if top_items:
            st.markdown(
                """
        <div style="
        font-size:1.1rem;
        font-weight:700;
        margin-top:6px;
        margin-bottom:2px;">
        ⭐ Beste Treffer
        </div>
        <div style="
        font-size:0.9rem;
        opacity:0.75;
        margin-bottom:10px;">
        Die aktuell relevantesten Treffer nach Score
        </div>
        """,
                unsafe_allow_html=True,
            )
        
            for rank, it in enumerate(top_items, start=1):    
                dist = it.get("_distance_km")
                dist_txt = f"{dist:.1f} km" if dist is not None else "— km"
                score_val = int(it.get("_score", 0))
                org = it.get("_org")

                target_tag = ""
                if org:
                    target_tag = " 🔥🎯" if org.get("priority") == "high" else " 🎯"

                k = it.get("_key") or item_key(it)
                fav_tag = " 📌" if is_favorited(k, favorites) else ""

                safe_title = it.get("_title_safe", sanitize_md_text(item_title(it)))
                safe_company = it.get("_company_safe", sanitize_md_text(item_company(it)))
                safe_location = it.get("_location_safe", sanitize_md_text(pretty_location(it)))

                with st.container(border=True):
                    st.markdown(
                        f"**{rank}. {safe_title}**{fav_tag}{target_tag}  \n"
                        f"{safe_company} · {safe_location}  \n"
                        f"Entfernung: {dist_txt} · Score: {score_val}"
                    )

                    if st.button("🔎 In Liste anzeigen", key=f"jump_{rank}_{it.get('_key', rank)}"):
                        st.session_state["jump_to_job"] = it.get("_key")
                        st.session_state["focus_company"] = None
                        st.rerun()

            #st.divider()

        # Merkliste
        with st.expander(f"📌 Merkliste ({len(favorites)})", expanded=False):
            if not favorites:
                st.info("Noch keine gemerkten Stellen.")
            else:
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

                        safe_title = it.get("_title_safe", sanitize_md_text(item_title(it)))
                        safe_company = it.get("_company_safe", sanitize_md_text(item_company(it)))
                        safe_location = it.get("_location_safe", sanitize_md_text(pretty_location(it)))

                        st.markdown(
                            f"**{safe_title}**  \n"
                            f"{safe_company} · {safe_location}  \n"
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

       
        # High Priority
        #st.divider()
        st.markdown(
            """
        <div style="
        font-size:1.1rem;
        font-weight:700;
        margin-bottom:2px;">
        🔥 High-Priority Treffer
        </div>
        <div style="
        font-size:0.9rem;
        opacity:0.75;
        margin-bottom:10px;">
        Unternehmen mit hoher Priorität in deinem Profil
        </div>
        """,
            unsafe_allow_html=True,
        )
        hp_items = [it for it in items_sorted if (it.get("_org") and it.get("_org", {}).get("priority") == "high")]

        if hp_items:
            for it in hp_items[:15]:
                st.write(f"• {it.get('_title', item_title(it))} – {it.get('_company', item_company(it))}")
        else:
            st.info("Aktuell keine High-Priority Treffer.")

        if st.session_state.get("save_snapshot_requested"):
            save_snapshot(items_sorted)
            st.session_state["save_snapshot_requested"] = False
            st.success("Snapshot gespeichert.")

        # Karte
        raw_markers: List[Dict[str, Any]] = []
        missing_coords = 0
        geocode_used = 0

        enable_job_geocode = bool(locals().get("enable_job_geocode", False))
        max_job_geocodes = int(locals().get("max_job_geocodes", 0))

        for it in items_sorted:
            ll = extract_latlon_from_item(it)

            if not ll:
                if enable_job_geocode and geocode_used < int(max_job_geocodes):
                    loc_text = it.get("_location", pretty_location(it))
                    ll = geocode_job_location(loc_text)
                    geocode_used += 1

                if not ll:
                    missing_coords += 1
                    continue

            dist = haversine_km(float(home_lat), float(home_lon), float(ll[0]), float(ll[1]))
            d = float(dist)
            bucket = distance_bucket(d, int(near_km), int(mid_km))

            raw_markers.append(
                {
                    "idx": int(it.get("_idx", 0)),
                    "lat": float(ll[0]),
                    "lon": float(ll[1]),
                    "title": it.get("_title", item_title(it)),
                    "company": it.get("_company", item_company(it)),
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

        last_page_count = len(items_local) if "items_local" in locals() else 0

        status_parts = [
            ("normal", f"🔎 {len(all_items)} Roh-Treffer"),
            ("success", f"✅ {len(items_sorted)} Treffer"),
            ("normal", f"📄 letzte Seite +{last_page_count}"),
            ("normal", f"🆕 {len(new_keys)} neu"),
            ("normal", f"🤖 {removed_recruiting} Recruiting entfernt"),
            ("normal", f"🏢 {unique_companies} Firmen"),
            ("normal", f"📍 {len(markers)} Marker"),
        ]
        
        if len(all_items) >= int(max_results):
            status_parts.insert(1, ("warn", "⚠ Limit erreicht"))
        
        badge_styles = {
            "normal": "background:rgba(128,128,128,0.12); color:inherit;",
            "success": "background:rgba(46,125,50,0.14); color:inherit;",
            "warn": "background:rgba(198,40,40,0.16); color:#b71c1c; font-weight:700;",
        }
        
        loc_line = ""
        if top_locations:
            loc_line = " | ".join(
                f"{city} {count} ({location_distance[city]:.0f} km)"
                for city, count in top_locations
            )
        
        chips = "".join([
            (
                f'<span style="display:inline-flex;align-items:center;'
                f'padding:5px 10px;border-radius:999px;font-size:0.9rem;'
                f'line-height:1.2;white-space:nowrap;{badge_styles[kind]}">{text}</span>'
            )
            for kind, text in status_parts
        ])
        
        location_html = ""

        if top_locations:
            location_chips = []
        
            for city, count in top_locations:
                dist = location_distance.get(city)
        
                if dist is None:
                    bg = "rgba(128,128,128,0.12)"
                elif dist <= near_km:
                    bg = "rgba(46,125,50,0.18)"
                elif dist <= mid_km:
                    bg = "rgba(249,168,37,0.22)"
                else:
                    bg = "rgba(198,40,40,0.18)"
        
                chip = (
                    f'<span style="display:inline-flex;align-items:center;gap:8px;'
                    f'padding:5px 10px;border-radius:999px;background:{bg};'
                    f'font-size:0.88rem;white-space:nowrap;">'
                    f'<span style="font-weight:600;">{city}</span>'
                    f'<span style="font-weight:700;">{count}</span>'
                    f'<span style="opacity:0.78;font-size:0.82rem;">{dist:.0f} km</span>'
                    f'</span>'
                )
        
                location_chips.append(chip)
        
            location_html = (
                f'<div style="display:flex;flex-wrap:wrap;gap:6px;margin-top:8px;">'
                f'{"".join(location_chips)}'
                f'</div>'
            )
        
        status_html = (
            f'<div style="padding:10px 12px;border:1px solid rgba(128,128,128,0.18);'
            f'border-radius:12px;background:rgba(255,255,255,0.03);margin-bottom:8px;">'
            f'<div style="display:flex;flex-wrap:wrap;gap:8px;align-items:center;">{chips}</div>'
            f'{location_html}'
            f'</div>'
        )
        
        status_top.markdown(status_html, unsafe_allow_html=True)
        
        # status_top.caption(" | ".join(status_parts))

        if markers:
            st.markdown(
                """
            <div style="
            font-size:1.1rem;
            font-weight:700;
            margin-top:6px;
            margin-bottom:2px;">
            🗺️ Karte
            </div>
            <div style="
            font-size:0.9rem;
            opacity:0.75;
            margin-bottom:10px;">
            Treffer nach Entfernung visualisiert
            </div>
            """,
                unsafe_allow_html=True,
            )
            st.markdown(
                """
            <div style="
            border:1px solid rgba(128,128,128,0.18);
            border-radius:12px;
            overflow:hidden;
            margin-bottom:10px;">
            """,
                unsafe_allow_html=True,
            )
            
            components.html(
                leaflet_map_html(
                    float(home_lat),
                    float(home_lon),
                    home_label,
                    markers[:80],
                    float(max_distance_filter),
                    height_px=700,
                ),
                height=740,
            )
            
            st.markdown("</div>", unsafe_allow_html=True)

        # Ergebnisse 
        st.divider()
        st.markdown(
            f'<div style="font-size:1.1rem;font-weight:700;margin-top:6px;margin-bottom:2px;">'
            f'📋 Ergebnisse ({len(items_sorted)})'
            f'</div>'
            f'<div style="font-size:0.9rem;opacity:0.75;margin-bottom:10px;">'
            f'Sortiert nach Priorität, Entfernung und Score'
            f'</div>',
            unsafe_allow_html=True,
        )

        current_filter = st.session_state.get("result_filter", "Alle")
        st.caption(f"{len(items_sorted)} Treffer gesamt · Filter: {current_filter}")        
        if "result_filter" not in st.session_state:
            st.session_state["result_filter"] = "Alle"
        
        fav_count_visible = sum(
            1 for it in items_sorted
            if is_favorited(it.get("_key") or item_key(it), favorites)
        )
        
        st.markdown(
            """
            <style>
            div[data-testid="stButton"] > button[kind="secondary"],
            div[data-testid="stButton"] > button[kind="primary"] {
                border-radius: 999px;
                padding: 0.14rem 0.62rem;
                font-size: 0.83rem;
                min-height: 0px;
                font-size: 0.85rem;
                line-height: 1.2;
                border: 1px solid rgba(128,128,128,0.22);
                background: rgba(255,255,255,0.03);
                box-shadow: none;
            }
        
            div[data-testid="stButton"] > button[kind="secondary"] {
                color: inherit;
                background: rgba(255,255,255,0.03);
                border: 1px solid rgba(128,128,128,0.22);
            }
        
            div[data-testid="stButton"] > button[kind="secondary"]:hover {
                background: rgba(255,255,255,0.08);
                border-color: rgba(128,128,128,0.40);
            }
        
            div[data-testid="stButton"] > button[kind="primary"] {
                color: inherit;
                background: rgba(46,125,50,0.14);
                border: 1px solid rgba(46,125,50,0.28);
                font-weight: 600;
            }
        
            div[data-testid="stButton"] > button[kind="primary"]:hover {
                background: rgba(46,125,50,0.20);
                border-color: rgba(46,125,50,0.42);
            }
            </style>
            """,
            unsafe_allow_html=True,
        )
        
        cF1, cF2, cF3, cF4 = st.columns([1, 1, 1.2, 6])
        
        with cF1:
            if st.button(
                "Alle",
                key="filter_all",
                type="primary" if st.session_state["result_filter"] == "Alle" else "secondary",
                use_container_width=True,
            ):
                st.session_state["result_filter"] = "Alle"
                st.rerun()
        
        with cF2:
            if st.button(
                f"Neu {len(new_keys)}",
                key="filter_new",
                type="primary" if st.session_state["result_filter"] == "Neu" else "secondary",
                use_container_width=True,
            ):
                st.session_state["result_filter"] = "Neu"
                st.rerun()
        
        with cF3:
            if st.button(
                f"Favoriten {fav_count_visible}",
                key="filter_fav",
                type="primary" if st.session_state["result_filter"] == "Favoriten" else "secondary",
                use_container_width=True,
            ):
                st.session_state["result_filter"] = "Favoriten"
                st.rerun()
        jump_target = st.session_state.get("jump_to_job")
        focus_company = st.session_state.get("focus_company")

        #for it in items_sorted:
        filtered_results = items_sorted

        if st.session_state.get("result_filter") == "Neu":
            filtered_results = [
                it for it in items_sorted
                if (it.get("_key") or item_key(it)) in new_keys
            ]
        elif st.session_state.get("result_filter") == "Favoriten":
            filtered_results = [
                it for it in items_sorted
                if is_favorited(it.get("_key") or item_key(it), favorites)
            ]

        for it in filtered_results:
            idx = int(it.get("_idx", 0) or 0)
            k = it.get("_key") or item_key(it)
            is_new = (k in new_keys)
            is_hidden = (k in hidden_keys)
            fav = is_favorited(k, favorites)
            is_focused_company = item_company(it).strip().lower() == focus_company if focus_company else False

            score = int(it.get("_score", 0))
            parts = it.get("_score_parts", [])
            dist = it.get("_distance_km")
            t_min = it.get("_travel_min")
            bucket = distance_bucket(dist, int(near_km), int(mid_km))
            emo = distance_emoji(bucket)

            star = "⭐ " if it.get("_is_leadership") else ""

            org = it.get("_org")
            target_tag = ""
            if org:
                target_tag = " 🔥🎯" if org.get("priority") == "high" else " 🎯"

            num_txt = f"{idx:02d}" if idx > 0 else "??"
            dist_txt = f"{dist:.1f} km" if dist is not None else "— km"

            pin = "📌 " if fav else ""
            focus_tag = " 🏢" if is_focused_company else ""

            safe_title = it.get("_title_safe", sanitize_md_text(item_title(it)))
            company_name = it.get("_company", item_company(it))
            location_name = it.get("_location", pretty_location(it))

            badges = []
            if is_new:
                badges.append("🆕")
            if fav:
                badges.append("📌")
            if it.get("_is_leadership"):
                badges.append("⭐")
            if is_focused_company:
                badges.append("🏢")
            if org:
                badges.append("🔥🎯" if org.get("priority") == "high" else "🎯")
            
            badge_prefix = " ".join(badges)
            badge_prefix = f"{badge_prefix} " if badge_prefix else ""
            
            label = f"{emo} {num_txt} · {dist_txt} · {badge_prefix}{safe_title}"
            
            meta_text = " · ".join(
                [
                    f"Score {score}",
                    it.get("_profile", ""),
                    it.get("_bucket", ""),
                    company_name,
                    location_name,
                ]
            )

            expanded = (jump_target == k)

            with st.expander(label, expanded=expanded):
                if is_focused_company:
                    st.info(f"Fokusfirma: {company_name}")

                badge = distance_badge_html(dist, t_min, int(near_km), int(mid_km))
                st.markdown(
                    badge
                    + f' <span style="opacity:0.78;font-size:0.92rem;">{meta_text}</span>',
                    unsafe_allow_html=True,
                )
                cFav1, cFav2 = st.columns([1.2, 3.8])
                with cFav1:
                    if not fav:
                        if st.button("📌 Merken", key=f"fav_add_{k}"):
                            favorites[k] = {
                                "added_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
                                "note": favorites.get(k, {}).get("note", ""),
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

                if company_name:
                    if st.button("🚫 Firma blockieren", key=f"hide_company_{k}"):
                        hidden_companies.add(company_name.lower())
                        save_hidden_companies(hidden_companies)
                        st.rerun()

                rid = item_id_raw(it) or "—"
                facts = [
                    ("Nr.", num_txt),
                    ("Distanz", dist_txt),
                    ("Fahrzeit (Schätzung)", f"~{t_min} min" if t_min is not None else "—"),
                    ("Ziel-Organisation", org["name"] if org else "—"),
                    ("Arbeitgeber", company_name or "—"),
                    ("Ort", location_name),
                    ("Profil", it.get("_profile", "")),
                    ("Quelle", it.get("_bucket", "")),
                    ("Score", str(score)),
                    ("RefNr/BA-ID", rid),
                ]
                render_fact_grid(facts)

                if org:
                    if org:
                        st.markdown(
                            '<div style="font-weight:600;margin-top:6px;margin-bottom:4px;">Karriereseite (Ziel-Organisation)</div>',
                            unsafe_allow_html=True,
                        )
                    try:
                        st.link_button("🏢 Karriereseite öffnen", org["url"])
                    except Exception:
                        st.markdown(f"[🏢 Karriereseite öffnen]({org['url']})")

                st.markdown(
                    '<div style="font-weight:600;margin-top:6px;margin-bottom:4px;">Score-Aufschlüsselung</div>',
                    unsafe_allow_html=True,
                )
                st.caption(" · ".join(parts))

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

        export_payload = {
            "exported_at": datetime.now().isoformat(timespec="seconds"),
            "today": today,
            "items": [],
        }
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
                company_state[org_name] = {
                    "last_checked": "",
                    "count": 0,
                    "prev_count": 0,
                    "notes": "",
                }

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
                    company_state[org_name] = {
                        "last_checked": "",
                        "count": 0,
                        "prev_count": 0,
                        "notes": "",
                    }
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
