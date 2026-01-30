from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os
import re
import time
import asyncio
import requests
from upstash_redis import Redis

# --------------------------------------------------
# App
# --------------------------------------------------
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --------------------------------------------------
# Constants
# --------------------------------------------------
VIDEO_EXT = (".mp4", ".mkv", ".avi", ".mov", ".webm", ".flv", ".ts")
CACHE_TTL = 60 * 60 * 24  # 24h

# --------------------------------------------------
# Redis (Upstash)
# --------------------------------------------------
redis = Redis(
    url=os.environ.get("UPSTASH_REDIS_REST_URL"),
    token=os.environ.get("UPSTASH_REDIS_REST_TOKEN"),
)

def redis_get(key):
    try:
        return redis.get(key)
    except:
        return None

def redis_set(key, value, ttl=None):
    try:
        redis.set(key, value, ex=ttl)
    except:
        pass

def redis_del(key):
    try:
        redis.delete(key)
    except:
        pass

# --------------------------------------------------
# Utils
# --------------------------------------------------
def normalize(text: str) -> str:
    text = text.lower()
    text = re.sub(r"[^a-z0-9 ]", " ", text)
    return re.sub(r"\s+", " ", text).strip()

def get_movie_info(imdb_id: str):
    r = requests.get(
        f"https://v3-cinemeta.strem.io/meta/movie/{imdb_id}.json",
        timeout=10
    )
    meta = r.json().get("meta", {})
    return meta.get("name", ""), str(meta.get("year", ""))

# --------------------------------------------------
# PikPak Client (TOKEN + REFRESH + AUTH LOCK)
# --------------------------------------------------
async def get_client():
    from pikpakapi import PikPakApi

    EMAIL = os.environ.get("PIKPAK_EMAIL")
    PASSWORD = os.environ.get("PIKPAK_PASSWORD")

    if not EMAIL or not PASSWORD:
        raise Exception("Missing PikPak credentials")

    now = time.time()

    access_token = redis_get("pikpak:access_token")
    refresh_token = redis_get("pikpak:refresh_token")
    expires_at = redis_get("pikpak:expires_at")

    client = PikPakApi(EMAIL, PASSWORD)

    # 1️⃣ Access token valid
    if access_token and expires_at and now < float(expires_at):
        client.access_token = access_token
        return client

    # 2️⃣ Try refresh token
    if refresh_token:
        try:
            await client.refresh_token_login(refresh_token)

            redis_set("pikpak:access_token", client.access_token, 3600)
            redis_set("pikpak:refresh_token", client.refresh_token, 86400)
            redis_set(
                "pikpak:expires_at",
                str(now + client.expires_in - 60),
                3600
            )
            return client
        except Exception:
            pass

    # 3️⃣ AUTH LOCK (critical for Vercel)
    if redis_get("pikpak:auth_lock"):
        await asyncio.sleep(2)

        access_token = redis_get("pikpak:access_token")
        expires_at = redis_get("pikpak:expires_at")

        if access_token and expires_at and time.time() < float(expires_at):
            client.access_token = access_token
            return client

    # Acquire lock
    redis_set("pikpak:auth_lock", "1", 30)

    try:
        await client.login()

        redis_set("pikpak:access_token", client.access_token, 3600)
        redis_set("pikpak:refresh_token", client.refresh_token, 86400)
        redis_set(
            "pikpak:expires_at",
            str(time.time() + client.expires_in - 60),
            3600
        )

        return client

    finally:
        redis_del("pikpak:auth_lock")

# --------------------------------------------------
# Recursive File Listing
# --------------------------------------------------
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

# --------------------------------------------------
# Routes
# --------------------------------------------------
@app.get("/")
async def root():
    return {
        "status": "ok",
        "addon": "PikPak Stremio Addon",
        "manifest": "/manifest.json"
    }

# --------------------------------------------------
# Manifest
# --------------------------------------------------
@app.get("/manifest.json")
async def manifest():
    return {
        "id": "com.arun.pikpak",
        "version": "1.5.0",
        "name": "PikPak Cloud",
        "description": "Stable PikPak streaming with refresh-token & Redis lock",
        "types": ["movie"],
        "resources": ["catalog", "stream"],
        "catalogs": [
            {
                "type": "movie",
                "id": "pikpak",
                "name": "My PikPak Files"
            }
        ],
        "idPrefixes": ["tt", "pikpak"]
    }

# --------------------------------------------------
# Catalog
# --------------------------------------------------
@app.get("/catalog/{type}/{id}.json")
async def catalog(type: str, id: str):
    if type != "movie" or id != "pikpak":
        return {"metas": []}

    try:
        pk = await get_client()
        files = await collect_files(pk)
    except Exception as e:
        return {"metas": [], "error": str(e)}

    metas = []
    for f in files:
        name = f.get("name", "")
        if name.lower().endswith(VIDEO_EXT):
            metas.append({
                "id": f"pikpak:{f['id']}",
                "type": "movie",
                "name": name,
                "poster": "https://upload.wikimedia.org/wikipedia/commons/8/8c/PikPak_logo.png"
            })

    return {"metas": metas}

# --------------------------------------------------
# Stream
# --------------------------------------------------
@app.get("/stream/{type}/{id}.json")
async def stream(type: str, id: str):

    pk = await get_client()

    # Direct catalog playback
    if id.startswith("pikpak:"):
        file_id = id.replace("pikpak:", "")
        data = await pk.get_download_url(file_id)

        url = (
            data.get("links", {})
            .get("application/octet-stream", {})
            .get("url")
        )

        if not url:
            medias = data.get("medias", [])
            if medias:
                url = medias[0].get("link", {}).get("url")

        return {"streams": [{"name": "PikPak", "url": url}]} if url else {"streams": []}

    # IMDb matching
    if type != "movie":
        return {"streams": []}

    title, year = get_movie_info(id)
    title_n = normalize(title)

    files = await collect_files(pk)
    streams = []

    for f in files:
        name = f.get("name", "")
        if not name.lower().endswith(VIDEO_EXT):
            continue

        fn = normalize(name)
        if title_n not in fn or (year and year not in fn):
            continue

        data = await pk.get_download_url(f["id"])
        url = (
            data.get("links", {})
            .get("application/octet-stream", {})
            .get("url")
        )

        if url:
            streams.append({
                "name": "PikPak",
                "title": name,
                "url": url
            })

    return {"streams": streams}