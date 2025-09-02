import asyncio
from datetime import datetime, timedelta, timezone
import os
from contextlib import asynccontextmanager
from dotenv import load_dotenv


import pandas as pd
import folium
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from telethon import TelegramClient, events
import stanza
import pymorphy3
from geopy.geocoders import Nominatim

import uvicorn


# ====================
# CONFIG
# ====================
load_dotenv()  # read .env

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
CHANNEL_USERNAME = os.environ["CHANNEL_USERNAME"]
SESSION_NAME = "user_session"

CSV_FILE = "locations.csv"
if not os.path.exists(CSV_FILE):
    # create empty CSV with headers
    df = pd.DataFrame(columns=["message", "place", "lat", "lon", "timestamp"])
    df.to_csv(CSV_FILE, index=False, encoding="utf-8")
    print(f"✅ Created empty CSV: {CSV_FILE}")

MAP_DIR = "static"
os.makedirs(MAP_DIR, exist_ok=True)  # ← create folder if missing
MAP_FILE = os.path.join(MAP_DIR, "kyiv_alerts.html")
if not os.path.exists(MAP_FILE):
    # create a simple empty map centered on Kyiv
    m = folium.Map(location=[50.4501, 30.5234], zoom_start=10)
    m.save(MAP_FILE)
    print(f"✅ Created default map: {MAP_FILE}")

# ====================
# INIT NLP TOOLS
# ====================
stanza.download("uk")  # download models if not yet
nlp = stanza.Pipeline("uk", processors="tokenize,ner")
morph = pymorphy3.MorphAnalyzer(lang="uk")
geolocator = Nominatim(user_agent="tg_listener")

# ====================
# HELPERS
# ====================
def extract_locations(text: str):
    """Extract and normalize all location entities"""
    doc = nlp(text)
    locs = []
    for ent in doc.ents:
        if ent.type == "LOC":
            normalized = normalize_phrase(ent.text)
            locs.append(normalized)
    return locs

def normalize_case(word: str) -> str:
    """Convert to nominative singular (if possible)"""
    p = morph.parse(word)
    if p:
        return p[0].normal_form
    return word

def normalize_phrase(phrase: str) -> str:
    words = phrase.split()
    norm_words = []
    for w in words:
        p = morph.parse(w)
        if p:
            norm_words.append(p[0].normal_form)
        else:
            norm_words.append(w)
    return " ".join(norm_words)

def geocode_location(name: str):
    """Geocode with OSM (Kyiv region priority)"""
    try:
        query = f"{name}"
        loc = geolocator.geocode(query)
        if loc:
            return loc.latitude, loc.longitude
    except Exception as e:
        print("⚠️ Geocoding error:", e)
    return None, None

def save_to_csv(message, place, lat, lon, timestamp=None, filename="locations.csv"):
    if timestamp is None:
        timestamp = datetime.now(timezone.utc)
    df = pd.DataFrame([[message, place, lat, lon, timestamp]],
                      columns=["message", "place", "lat", "lon", "timestamp"])
    df.to_csv(filename, mode="a", header=not pd.io.common.file_exists(filename),
              index=False, encoding="utf-8")

from datetime import datetime, timedelta

def update_map():
    if not os.path.exists(CSV_FILE):
        return

    df = pd.read_csv(CSV_FILE, parse_dates=["timestamp"])
    now = datetime.now(timezone.utc)

    # Keep only messages <= 1 hour
    df = df[now - df["timestamp"] <= timedelta(hours=1)]

    m = folium.Map(location=[50.4501, 30.5234], zoom_start=10)

    for _, row in df.iterrows():
        age = now - row["timestamp"]
        if age <= timedelta(minutes=15):
            color = "red"
        elif age <= timedelta(minutes=30):
            color = "yellow"
        else:
            color = "gray"  # 30–60 minutes

        folium.CircleMarker(
            location=[row["lat"], row["lon"]],
            radius=6,
            popup=row["message"],
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.7
        ).add_to(m)

    # Save cleaned CSV
    df.to_csv(CSV_FILE, index=False, encoding="utf-8")

    os.makedirs(MAP_DIR, exist_ok=True)
    m.save(MAP_FILE)
    print(f"✅ Map updated: {MAP_FILE} (cleaned old messages)")

# ====================
# TELEGRAM LISTENER
# ====================
client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

@client.on(events.NewMessage(chats=CHANNEL_USERNAME))
async def telegram_handler(event):
    text = event.message.text
    if not text:
        return
    locations = extract_locations(text)
    for loc in locations:
        lat, lon = geocode_location(loc)
        if lat and lon:
            print(f"📍 {text} → {loc} @ ({lat:.4f}, {lon:.4f})")
            save_to_csv(text, loc, lat, lon)
    if locations:
        update_map()

async def start_telegram_listener():
    await client.start()
    print("🔎 Telegram listener started...")
    await client.run_until_disconnected()

# ====================
# FASTAPI DASHBOARD
# ====================
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup code
    asyncio.create_task(start_telegram_listener())
    yield
    # Shutdown code (optional)
    await client.disconnect()


app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/", response_class=HTMLResponse)
def home():
    # regenerate map just in case
    update_map()
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Kyiv Alerts Map</title>
    </head>
    <body>
        <h2>Kyiv Region Alerts</h2>
        <iframe src="/static/kyiv_alerts.html" width="100%" height="800px"></iframe>
        <script>
            setInterval(function() {{
                document.querySelector("iframe").src = "/static/kyiv_alerts.html?rand=" + Math.random();
            }}, 30000);
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html)
# ====================
# START BOTH FASTAPI & TELEGRAM
# ====================

def run():
    loop = asyncio.get_event_loop()
    loop.create_task(start_telegram_listener())
    uvicorn.run(app, host="0.0.0.0", port=8000)

if __name__ == "__main__":
    run()