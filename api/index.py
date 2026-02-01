from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os
import re
import json
import time
import hashlib
import logging
import requests
from upstash_redis import Redis

# -----------------------
# Logging
# -----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# -----------------------
# App
# -----------------------
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------
# Constants
# -----------------------
VIDEO_EXT = (".mp4", ".mkv", ".avi", ".mov", ".webm", ".flv", ".ts")

URL_CACHE_TTL = 60 * 60 * 24            # 24h
AUTH_CACHE_TTL = 60 * 60 * 24 * 365     # 365 days
REFRESH_EARLY_SECONDS = 10 * 60         # 10 min (Go equivalent)

# -----------------------
# Redis
# -----------------------
redis = Redis(
    url=os.environ["UPSTASH_REDIS_REST_URL"],
    token=os.environ["UPSTASH_REDIS_REST_TOKEN"],
)

# -----------------------
# Device ID (GO-style)
# -----------------------
def get_device_id(seed: str) -> str:
    h = hashlib.sha256(seed.encode()).digest()[:16]
    h = bytearray(h)
    h[6] = (h[6] & 0x0F) | 0x40
    h[8] = (h[8] & 0x3F) | 0x80
    return (
        f"{h[0:4].hex()}"
        f"{h[4:6].hex()}"
        f"{h[6:8].hex()}"
        f"{h[8:10].hex()}"
        f"{h[10:].hex()}"
    )

# -----------------------
# Redis helpers
# -----------------------
def auth_key(device_id: str) -> str:
    return f"pikpak:auth:{device_id}"


def save_auth(device_id: str, auth: dict):
    redis.set(
        auth_key(device_id),
        json.dumps(auth),
        ex=AUTH_CACHE_TTL,
    )
    logging.info("[AUTH] saved auth to redis")


def load_auth(device_id: str):
    raw = redis.get(auth_key(device_id))
    if not raw:
        return None
    return json.loads(raw)


def is_expiring(auth: dict) -> bool:
    return auth["expires_at"] <= int(time.time()) + REFRESH_EARLY_SECONDS

# -----------------------
# Utils
# -----------------------
def normalize(text: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]", " ", text.lower())).strip()


def get_movie_info(imdb_id: str):
    r = requests.get(
        f"https://v3-cinemeta.strem.io/meta/movie/{imdb_id}.json",
        timeout=10,
    )
    meta = r.json().get("meta", {})
    return meta.get("name", ""), str(meta.get("year", ""))

# -----------------------
# PikPak Client
# -----------------------
client = None

async def get_client():
    global client
    from pikpakapi import PikPakApi

    EMAIL = os.environ["PIKPAK_EMAIL"]
    PASSWORD = os.environ["PIKPAK_PASSWORD"]

    device_id = get_device_id(EMAIL)
    client = PikPakApi(EMAIL, PASSWORD)

    auth = load_auth(device_id)

    # ---------- Restore ----------
    if auth:
        client.auth = auth
        logging.info("[AUTH] loaded from redis")

        # proactive refresh (GO behavior)
        if is_expiring(auth):
            try:
                await client.refresh_access_token()
                auth = client.auth
                save_auth(device_id, auth)
                logging.info("[AUTH] refreshed access token")
                return client
            except Exception as e:
                logging.warning(f"[AUTH] refresh failed: {e}")

        # validate
        try:
            await client.user_info()
            save_auth(device_id, client.auth)
            logging.info("[AUTH] access token valid")
            return client
        except Exception:
            logging.warning("[AUTH] access token invalid")

    # ---------- Full login ----------
    logging.warning("[AUTH] full login triggered")
    await client.login()
    save_auth(device_id, client.auth)
    return client

# -----------------------
# File traversal
# -----------------------
async def collect_files(pk, parent_id="", result=None):
    if result is None:
        result = []

    data = await pk.file_list(parent_id=parent_id)
    for f in data.get("files", []):
        if f.get("kind") == "drive#folder":
            await collect_files(pk, f["id"], result)
        else:
            result.append(f)

    return result

# -----------------------
# Routes
# -----------------------
@app.get("/")
async def root():
    return {"status": "ok", "manifest": "/manifest.json"}

@app.get("/manifest.json")
async def manifest():
    return {
        "id": "com.arun.pikpak",
        "version": "2.0.0",
        "name": "PikPak Cloud",
        "types": ["movie"],
        "resources": ["catalog", "stream"],
        "catalogs": [{
            "type": "movie",
            "id": "pikpak",
            "name": "My PikPak Files",
        }],
        "idPrefixes": ["tt", "pikpak"],
    }

# -----------------------
# Catalog
# -----------------------
@app.get("/catalog/movie/pikpak.json")
async def catalog():
    pk = await get_client()
    files = await collect_files(pk)

    metas = []
    for f in files:
        if f["name"].lower().endswith(VIDEO_EXT):
            metas.append({
                "id": f"pikpak:{f['id']}",
                "type": "movie",
                "name": f["name"],
                "poster": "https://upload.wikimedia.org/wikipedia/commons/8/8c/PikPak_logo.png",
            })
    return {"metas": metas}

# -----------------------
# Stream
# -----------------------
@app.get("/stream/movie/{id}.json")
async def stream(id: str):
    if not id.startswith("pikpak:"):
        return {"streams": []}

    file_id = id.replace("pikpak:", "")
    pk = await get_client()

    data = await pk.get_download_url(file_id)
    links = data.get("links", {})

    if "application/octet-stream" in links:
        url = links["application/octet-stream"]["url"]
    else:
        url = data["medias"][0]["link"]["url"]

    return {
        "streams": [{
            "name": "PikPak",
            "title": "Direct",
            "url": url,
        }]
    }