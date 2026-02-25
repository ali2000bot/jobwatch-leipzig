import json
import math
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

import requests
import streamlit as st
import streamlit.components.v1 as components
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE = "https://rest.arbeitsagentur.de/jobboerse/jobsuche-service"
SEARCH_URL = f"{BASE}/pc/v4/app/jobs"
API_KEY_DEFAULT = "jobboerse-jobsuche"

STATE_DIR = ".jobwatch_state"
SNAPSHOT_FILE = os.path.join(STATE_DIR, "snapshot.json")

DEFAULT_HOME_LABEL = "06242 Braunsbedra"
DEFAULT_HOME_LAT = 51.2861
DEFAULT_HOME_LON = 11.8900


# ---------------------------------------------------
# ---------------- Snapshot -------------------------
# ---------------------------------------------------

def ensure_state_dir():
    os.makedirs(STATE_DIR, exist_ok=True)


def load_snapshot():
    if not os.path.exists(SNAPSHOT_FILE):
        return {"timestamp": None, "items": []}
    with open(SNAPSHOT_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_snapshot(items):
    ensure_state_dir()
    payload = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "items": items,
    }
    with open(SNAPSHOT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------
# ---------------- API ------------------------------
# ---------------------------------------------------

def headers(api_key: str):
    return {
        "User-Agent": "Jobsuche/2.9.2 Streamlit",
        "Host": "rest.arbeitsagentur.de",
        "X-API-Key": api_key,
        "Accept": "application/json",
    }


def extract_items(data):
    if isinstance(data.get("stellenangebote"), list):
        return data["stellenangebote"]
    emb = data.get("_embedded") or {}
    if isinstance(emb.get("jobs"), list):
        return emb["jobs"]
    return []


def item_id(it):
    return it.get("refnr") or it.get("hashId") or ""


def item_title(it):
    return it.get("titel") or it.get("beruf") or "Ohne Titel"


def item_company(it):
    return it.get("arbeitgeber") or it.get("arbeitgeberName") or ""


def extract_latlon_from_item(it):
    loc = it.get("arbeitsort") or {}
    coords = loc.get("koordinaten") if isinstance(loc, dict) else None
    if isinstance(coords, dict):
        lat = coords.get("lat")
        lon = coords.get("lon")
        if lat and lon:
            return float(lat), float(lon)
    return None


@st.cache_data(ttl=300)
def fetch_search(api_key, wo, umkreis, was, aktualitaet, size):
    params = {
        "wo": wo,
        "umkreis": str(umkreis),
        "was": was,
        "aktualitaet": str(aktualitaet),
        "size": str(size),
        "page": "1",
    }
    try:
        r = requests.get(
            SEARCH_URL,
            headers=headers(api_key),
            params=params,
            timeout=25,
            verify=False,
        )
        if r.status_code != 200:
            return [], f"HTTP {r.status_code}"
        return extract_items(r.json()), None
    except Exception as e:
        return [], str(e)


# ---------------------------------------------------
# ---------------- Distance -------------------------
# ---------------------------------------------------

def haversine_km(lat1, lon1, lat2, lon2):
    r = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    return r * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def distance_from_home(it, home_lat, home_lon):
    ll = extract_latlon_from_item(it)
    if not ll:
        return None
    return haversine_km(home_lat, home_lon, ll[0], ll[1])


# ---------------------------------------------------
# ---------------- Leaflet Map ----------------------
# ---------------------------------------------------

def leaflet_map_html(home_lat, home_lon, home_label, markers):

    markers_json = json.dumps(markers, ensure_ascii=False)

    return f"""
<!doctype html>
<html>
<head>
<meta charset="utf-8"/>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
#map {{ height: 500px; width: 100%; }}
.pin {{ transform: translate(-13px,-36px); }}
</style>
</head>
<body>
<div id="map"></div>
<script>
const markers = {markers_json};
const map = L.map('map');
L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png').addTo(map);

function makePin(color) {{
 return L.divIcon({{
   className:'pin',
   html:`<svg width="26" height="38">
     <path d="M13 0C6 0 0 6 0 13c0 10 13 25 13 25s13-15 13-25C26 6 20 0 13 0z"
      fill="${{color}}"/>
     <circle cx="13" cy="13" r="5" fill="white"/>
   </svg>`,
   iconSize:[26,38],
   iconAnchor:[13,36]
 }});
}}

const homeIcon = makePin("#1565c0");
L.marker([{home_lat},{home_lon}],{{icon:homeIcon}})
 .addTo(map)
 .bindPopup("<b>Wohnort</b><br/>{home_label}");

const fg = L.featureGroup().addTo(map);

markers.forEach(m=>{{
 let color = m.pin==="green"?"#2e7d32":
             m.pin==="yellow"?"#f9a825":"#c62828";
 let icon = makePin(color);

 let popup = `<b>${{m.title}}</b><br/>${{m.company}}<br/>Dist: ${{m.dist}} km
   <br/><button onclick="jump('${{m.jid}}')">➡️ In App anzeigen</button>`;

 L.marker([m.lat,m.lon],{{icon}}).addTo(fg).bindPopup(popup);
}});

map.fitBounds(fg.getBounds().pad(0.2));

function jump(id){{
 const base = window.location.href.split('?')[0];
 try {{
   window.top.location.assign(base+'?sel='+id);
 }} catch(e){{
   window.open(base+'?sel='+id,'_blank');
 }}
}}
</script>
</body>
</html>
"""


# ---------------------------------------------------
# ---------------- UI -------------------------------
# ---------------------------------------------------

st.set_page_config(layout="wide")
st.title("JobWatch Leipzig")

selected_id = st.query_params.get("sel","")

with st.sidebar:
    home_lat = st.number_input("Breitengrad", value=DEFAULT_HOME_LAT)
    home_lon = st.number_input("Längengrad", value=DEFAULT_HOME_LON)
    near_km = st.slider("Grün bis km", 5, 50, 25)
    mid_km = st.slider("Gelb bis km", 10, 150, 60)

    wo = st.text_input("Ort", "Leipzig")
    umkreis = st.selectbox("Umkreis", [25,50],1)
    aktualitaet = st.slider("Aktualität Tage", 0,365,60)
    size = st.selectbox("Treffer", [25,50,100],1)
    api_key = st.text_input("API Key", API_KEY_DEFAULT)

items, err = fetch_search(api_key, wo, umkreis, "", aktualitaet, size)

if err:
    st.error(err)

# Entfernung + Marker vorbereiten
markers = []
for it in items:
    ll = extract_latlon_from_item(it)
    if not ll:
        continue
    dist = distance_from_home(it, home_lat, home_lon)
    if dist is None:
        continue
    if dist <= near_km:
        pin="green"
    elif dist <= mid_km:
        pin="yellow"
    else:
        pin="red"

    markers.append({
        "lat":ll[0],
        "lon":ll[1],
        "title":item_title(it),
        "company":item_company(it),
        "dist":round(dist,1),
        "jid":item_id(it),
        "pin":pin
    })

# Sortieren
items_sorted = sorted(items, key=lambda x: distance_from_home(x, home_lat, home_lon) or 9999)

# Marker zuerst anzeigen
if markers:
    components.html(leaflet_map_html(home_lat, home_lon, DEFAULT_HOME_LABEL, markers), height=520)

st.divider()

# Trefferliste
for it in items_sorted:
    jid = item_id(it)
    expanded = (jid == selected_id)
    with st.expander(item_title(it), expanded=expanded):
        st.write("Arbeitgeber:", item_company(it))
        dist = distance_from_home(it, home_lat, home_lon)
        if dist:
            st.write("Entfernung:", round(dist,1),"km")
        web = f"https://www.arbeitsagentur.de/jobsuche/jobdetail/{jid}"
        st.link_button("🔗 BA Jobdetail", web)
