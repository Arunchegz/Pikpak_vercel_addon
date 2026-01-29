from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os
import re
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
    data = r.json()
    meta = data.get("meta", {})
    title = meta.get("name", "")
    year = str(meta.get("year", ""))
    return title, year

# -----------------------
# PikPak client (auto relogin on 401)
# -----------------------
client = None

async def get_client():
    global client
    from pikpakapi import PikPakApi

    EMAIL = os.environ.get("PIKPAK_EMAIL")
    PASSWORD = os.environ.get("PIKPAK_PASSWORD")

    if not EMAIL or not PASSWORD:
        raise Exception("PIKPAK_EMAIL or PIKPAK_PASSWORD is missing")

    # First login
    if client is None:
        client = PikPakApi(EMAIL, PASSWORD)
        await client.login()
        return client

    # Check token validity
    try:
        await client.file_list(parent_id="root", limit=1)
    except Exception:
        # Session expired → re-login
        client = PikPakApi(EMAIL, PASSWORD)
        await client.login()

    return client

# -----------------------
# Collect files (ROOT FIX)
# -----------------------
async def collect_files(pk, parent_id="root", result=None):
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
        "version": "1.2.2",
        "name": "PikPak Cloud",
        "description": "Browse and stream files from your PikPak cloud",
        "resources": ["catalog", "stream"],
        "types": ["movie"],
        "catalogs": [
            {
                "type": "movie",
                "id": "pikpak",
                "name": "My PikPak Files"
            }
        ],
        "idPrefixes": ["pikpak"]
    }

# -----------------------
# Catalog (Discover Page)
# -----------------------
@app.get("/catalog/{type}/{id}.json")
async def catalog(type: str, id: str):
    if type != "movie" or id != "pikpak":
        return {"metas": []}

    try:
        pk = await get_client()
        files = await collect_files(pk, "root")
    except Exception as e:
        return {"metas": [], "error": str(e)}

    metas = []

    for f in files:
        name = f.get("name", "")
        file_id = f.get("id")

        if not name or not file_id:
            continue

        if not name.lower().endswith(VIDEO_EXT):
            continue

        metas.append({
            "id": f"pikpak:{file_id}",
            "type": "movie",
            "name": name,
            "poster": "https://upload.wikimedia.org/wikipedia/commons/8/8c/PikPak_logo.png",
            "posterShape": "poster"
        })

    return {"metas": metas}

# -----------------------
# Stream Endpoint
# -----------------------
@app.get("/stream/{type}/{id}.json")
async def stream(type: str, id: str):

    # Case 1: Direct play from PikPak catalog
    if id.startswith("pikpak:"):
        file_id = id.replace("pikpak:", "")
        pk = await get_client()

        cached = get_cached_url(file_id)
        if cached:
            url = cached
        else:
            data = await pk.get_download_url(file_id)

            url = None
            links = data.get("links", {})
            if "application/octet-stream" in links:
                url = links["application/octet-stream"].get("url")

            if not url:
                medias = data.get("medias", [])
