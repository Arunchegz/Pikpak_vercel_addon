from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os
import re
import requests
from upstash_redis import Redis

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

URL_CACHE_TTL = 60 * 60 * 24       # 24h stream URL cache
AUTH_CACHE_TTL = 60 * 60 * 24 * 7  # 7 days auth cache

# -----------------------
# Redis
# -----------------------
redis = Redis(
    url=os.environ.get("UPSTASH_REDIS_REST_URL"),
    token=os.environ.get("UPSTASH_REDIS_REST_TOKEN"),
)

# -----------------------
# Redis helpers
# -----------------------
def get_cached_url(file_id: str):
    try:
        return redis.get(f"pikpak:url:{file_id}")
    except:
        return None


def set_cached_url(file_id: str, url: str):
    try:
        redis.set(f"pikpak:url:{file_id}", url, ex=URL_CACHE_TTL)
    except:
        pass


def load_auth():
    try:
        return redis.get("pikpak:auth")
    except:
        return None


def save_auth(auth: dict):
    try:
        redis.set("pikpak:auth", auth, ex=AUTH_CACHE_TTL)
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
# PikPak client with refresh token
# -----------------------
client = None


async def get_client(force_login=False):
    """
    Auth order:
    1. Restore auth from Redis
    2. Validate
    3. Refresh token
    4. Full login (last resort)
    """
    global client
    from pikpakapi import PikPakApi

    EMAIL = os.environ.get("PIKPAK_EMAIL")
    PASSWORD = os.environ.get("PIKPAK_PASSWORD")

    if not EMAIL or not PASSWORD:
        raise Exception("Missing PIKPAK_EMAIL or PIKPAK_PASSWORD")

    if client and not force_login:
        return client

    client = PikPakApi(EMAIL, PASSWORD)

    auth = load_auth()

    # ---------- Restore token ----------
    if auth and not force_login:
        client.auth = auth

        # 1) Try using access token
        try:
            await client.user_info()
            return client
        except Exception:
            pass

        # 2) Try refresh token
        try:
            await client.refresh_access_token()
            save_auth(client.auth)
            return client
        except Exception:
            pass

    # ---------- Full login fallback ----------
    await client.login()
    save_auth(client.auth)
    return client


async def with_relogin(fn, *args, **kwargs):
    try:
        return await fn(*args, **kwargs)
    except Exception as e:
        msg = str(e).lower()

        if "401" in msg or "unauthorized" in msg:
            pk = await get_client(force_login=True)
            return await fn(*args, **kwargs)

        raise

# -----------------------
# File traversal
# -----------------------
async def collect_files(pk, parent_id="", result=None):
    if result is None:
        result = []

    data = await with_relogin(pk.file_list, parent_id=parent_id)
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
        "version": "1.4.0",
        "name": "PikPak Cloud",
        "description": "PikPak Stremio addon with refresh-token auth",
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

# -----------------------
# Catalog
# -----------------------
@app.get("/catalog/{type}/{id}.json")
async def catalog(type: str, id: str):
    if type != "movie" or id != "pikpak":
        return {"metas": []}

    pk = await get_client()
    files = await collect_files(pk)

    metas = []
    for f in files:
        name = f.get("name")
        file_id = f.get("id")

        if not name or not file_id:
            continue

        if not name.lower().endswith(VIDEO_EXT):
            continue

        metas.append({
            "id": f"pikpak:{file_id}",
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

    # Direct catalog playback
    if id.startswith("pikpak:"):
        file_id = id.replace("pikpak:", "")
        pk = await get_client()

        url = get_cached_url(file_id)
        if not url:
            data = await with_relogin(pk.get_download_url, file_id)

            links = data.get("links", {})
            if "application/octet-stream" in links:
                url = links["application/octet-stream"]["url"]
            else:
                medias = data.get("medias", [])
                if medias:
                    url = medias[0]["link"]["url"]

            if not url:
                return {"streams": []}

            set_cached_url(file_id, url)

        return {
            "streams": [{
                "name": "PikPak",
                "title": "PikPak Direct",
                "url": url
            }]
        }

    # IMDb matching
    if type != "movie":
        return {"streams": []}

    movie_title, movie_year = get_movie_info(id)
    movie_n = normalize(movie_title)

    pk = await get_client()
    files = await collect_files(pk)

    streams = []

    for f in files:
        name = f.get("name")
        file_id = f.get("id")

        if not name or not file_id:
            continue

        if not name.lower().endswith(VIDEO_EXT):
            continue

        file_n = normalize(name)

        if movie_n not in file_n:
            continue
        if movie_year and movie_year not in file_n:
            continue

        url = get_cached_url(file_id)
        if not url:
            data = await with_relogin(pk.get_download_url, file_id)

            links = data.get("links", {})
            if "application/octet-stream" in links:
                url = links["application/octet-stream"]["url"]
            else:
                medias = data.get("medias", [])
                if medias:
                    url = medias[0]["link"]["url"]

            if not url:
                continue

            set_cached_url(file_id, url)

        streams.append({
            "name": "PikPak",
            "title": name,
            "url": url
        })

    return {"streams": streams}
