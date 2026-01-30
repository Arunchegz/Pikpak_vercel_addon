from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os
import re
import time
import asyncio
import requests
from upstash_redis import Redis

app = FastAPI()

# -----------------------
# CORS (needed for Stremio)
# -----------------------
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
CACHE_TTL = 60 * 60 * 24  # 24 hours
LOGIN_TTL = 60 * 60      # 1 hour

# -----------------------
# Upstash Redis
# -----------------------
redis = Redis(
    url=os.environ.get("UPSTASH_REDIS_REST_URL"),
    token=os.environ.get("UPSTASH_REDIS_REST_TOKEN"),
)

def get_cached_url(file_id: str):
    try:
        return redis.get(f"pikpak:{file_id}")
    except:
        return None

def set_cached_url(file_id: str, url: str):
    try:
        redis.set(f"pikpak:{file_id}", url, ex=CACHE_TTL)
    except:
        pass

# -----------------------
# Utils
# -----------------------
def normalize(text: str) -> str:
    text = text.lower()
    text = re.sub(r"[^a-z0-9 ]", " ", text)
    return re.sub(r"\s+", " ", text).strip()

def get_movie_info(imdb_id: str):
    url = f"https://v3-cinemeta.strem.io/meta/movie/{imdb_id}.json"
    r = requests.get(url, timeout=10)
    meta = r.json().get("meta", {})
    return meta.get("name", ""), str(meta.get("year", ""))

# -----------------------
# PikPak client (SAFE LOGIN)
# -----------------------
client = None

async def get_client():
    global client
    from pikpakapi import PikPakApi

    EMAIL = os.environ.get("PIKPAK_EMAIL")
    PASSWORD = os.environ.get("PIKPAK_PASSWORD")

    if not EMAIL or not PASSWORD:
        raise Exception("PIKPAK_EMAIL or PIKPAK_PASSWORD is missing")

    now = time.time()
    expires_at = redis.get("pikpak:login_expires")

    # ✅ Reuse existing session if still valid
    if client is not None and expires_at and now < float(expires_at):
        return client

    # 🔒 Prevent parallel login (Stremio calls are parallel)
    if redis.get("pikpak:auth_lock"):
        await asyncio.sleep(2)
        expires_at = redis.get("pikpak:login_expires")
        if client is not None and expires_at and time.time() < float(expires_at):
            return client

    # Acquire lock
    redis.set("pikpak:auth_lock", "1", ex=30)

    try:
        client = PikPakApi(EMAIL, PASSWORD)
        await client.login()

        redis.set(
            "pikpak:login_expires",
            str(time.time() + LOGIN_TTL - 60),
            ex=LOGIN_TTL
        )

        return client

    finally:
        redis.delete("pikpak:auth_lock")

# -----------------------
# Recursive file listing
# -----------------------
async def collect_files(pk, parent_id="", result=None):
    if result is None:
        result = []

    data = await pk.file_list(parent_id=parent_id)
    files = data.get("files", [])

    for f in files:
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
    return {
        "status": "ok",
        "addon": "PikPak Stremio Addon",
        "manifest": "/manifest.json"
    }

# -----------------------
# Manifest
# -----------------------
@app.get("/manifest.json")
async def manifest():
    return {
        "id": "com.arun.pikpak",
        "version": "1.3.0",
        "name": "PikPak Cloud",
        "description": "Browse and stream files from your PikPak cloud",
        "types": ["movie"],
        "resources": ["stream", "catalog"],
        "catalogs": [
            {
                "type": "movie",
                "id": "pikpak",
                "name": "My PikPak Files"
            }
        ],
        "idPrefixes": ["tt", "pikpak"]
    }

# -----------------------
# Catalog (Discover)
# -----------------------
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
        name = f.get("name")
        fid = f.get("id")

        if not name or not fid:
            continue
        if not name.lower().endswith(VIDEO_EXT):
            continue

        metas.append({
            "id": f"pikpak:{fid}",
            "type": "movie",
            "name": name,
            "poster": "https://upload.wikimedia.org/wikipedia/commons/8/8c/PikPak_logo.png"
        })

    return {"metas": metas}

# -----------------------
# Stream
# -----------------------
@app.get("/stream/{type}/{id}.json")
async def stream(type: str, id: str):

    # Direct play from catalog
    if id.startswith("pikpak:"):
        file_id = id.replace("pikpak:", "")
        pk = await get_client()

        cached = get_cached_url(file_id)
        if cached:
            url = cached
        else:
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

            if not url:
                return {"streams": []}

            set_cached_url(file_id, url)

        return {
            "streams": [{
                "name": "PikPak",
                "title": "Direct Play",
                "url": url
            }]
        }

    # IMDb matching
    if type != "movie":
        return {"streams": []}

    title, year = get_movie_info(id)
    title_n = normalize(title)

    pk = await get_client()
    files = await collect_files(pk)
    streams = []

    for f in files:
        name = f.get("name", "")
        fid = f.get("id", "")

        if not name.lower().endswith(VIDEO_EXT):
            continue

        fn = normalize(name)
        if title_n not in fn:
            continue
        if year and year not in fn:
            continue

        cached = get_cached_url(fid)
        if cached:
            url = cached
        else:
            data = await pk.get_download_url(fid)
            url = (
                data.get("links", {})
                .get("application/octet-stream", {})
                .get("url")
            )

            if not url:
                continue

            set_cached_url(fid, url)

        streams.append({
            "name": "PikPak",
            "title": name,
            "url": url
        })

    return {"streams": streams}