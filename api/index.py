from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os
import re
import json
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

URL_CACHE_TTL = 60 * 60 * 24          # 24h
AUTH_CACHE_TTL = 60 * 60 * 24 * 365   # 365 days

# -----------------------
# Redis (ASYNC Upstash)
# -----------------------
REDIS_URL = os.environ.get("UPSTASH_REDIS_REST_URL")
REDIS_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN")

if not REDIS_URL or not REDIS_TOKEN:
    raise RuntimeError("❌ Upstash Redis env vars missing")

redis = Redis(url=REDIS_URL, token=REDIS_TOKEN)

# -----------------------
# Redis helpers (ASYNC!)
# -----------------------
async def get_cached_url(file_id: str):
    try:
        return await redis.get(f"pikpak:url:{file_id}")
    except Exception as e:
        print("❌ Redis get_cached_url:", e)
        return None


async def set_cached_url(file_id: str, url: str):
    try:
        await redis.set(f"pikpak:url:{file_id}", url, ex=URL_CACHE_TTL)
    except Exception as e:
        print("❌ Redis set_cached_url:", e)


async def load_auth():
    try:
        raw = await redis.get("pikpak:auth")
        print("🔎 Redis raw auth:", raw)

        if not raw:
            return None

        if isinstance(raw, dict):
            return raw

        return json.loads(raw)
    except Exception as e:
        print("❌ Redis load_auth error:", e)
        return None


async def save_auth(auth):
    try:
        await redis.set(
            "pikpak:auth",
            json.dumps(auth, default=str),
            ex=AUTH_CACHE_TTL
        )
        print("✅ Auth saved to Redis")
    except Exception as e:
        print("❌ Redis save_auth error:", e)

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
# PikPak client
# -----------------------
client = None


async def get_client(force_login=False):
    """
    restore → validate → refresh → login
    """
    global client
    from pikpakapi import PikPakApi

    EMAIL = os.environ.get("PIKPAK_EMAIL")
    PASSWORD = os.environ.get("PIKPAK_PASSWORD")

    if not EMAIL or not PASSWORD:
        raise RuntimeError("❌ PIKPAK_EMAIL or PIKPAK_PASSWORD missing")

    if client and not force_login:
        return client

    client = PikPakApi(EMAIL, PASSWORD)

    auth = await load_auth()

    # -----------------------
    # Restore session
    # -----------------------
    if auth and not force_login:
        client.auth = auth

        # Validate
        try:
            await client.user_info()
            await save_auth(client.auth)
            print("✅ PikPak session restored")
            return client
        except Exception as e:
            print("⚠️ Session invalid:", e)

        # Refresh
        try:
            await client.refresh_access_token()
            await save_auth(client.auth)
            print("♻️ Token refreshed")
            return client
        except Exception as e:
            print("⚠️ Refresh failed:", e)

    # -----------------------
    # Full login
    # -----------------------
    await client.login()
    await save_auth(client.auth)
    print("🔐 Full login done")
    return client


async def with_relogin(fn, *args, **kwargs):
    try:
        return await fn(*args, **kwargs)
    except Exception as e:
        if "401" in str(e).lower():
            print("🔁 Re-login triggered")
            await get_client(force_login=True)
            return await fn(*args, **kwargs)
        raise

# -----------------------
# Recursive file traversal
# -----------------------
async def collect_files(pk, parent_id="", result=None):
    if result is None:
        result = []

    data = await with_relogin(pk.file_list, parent_id=parent_id)

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
    return {"status": "ok"}

# -----------------------
# Debug Redis (IMPORTANT)
# -----------------------
@app.get("/debug/redis")
async def debug_redis():
    await redis.set("test:key", "hello", ex=60)
    return {
        "read": await redis.get("test:key"),
        "auth_present": bool(await redis.get("pikpak:auth"))
    }

# -----------------------
# Manifest
# -----------------------
@app.get("/manifest.json")
async def manifest():
    return {
        "id": "com.arun.pikpak",
        "version": "1.5.2",
        "name": "PikPak Cloud",
        "types": ["movie"],
        "resources": ["catalog", "stream"],
        "catalogs": [{
            "type": "movie",
            "id": "pikpak",
            "name": "My PikPak Files"
        }],
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

        if name and file_id and name.lower().endswith(VIDEO_EXT):
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

    if not id.startswith("pikpak:"):
        return {"streams": []}

    file_id = id.replace("pikpak:", "")
    pk = await get_client()

    url = await get_cached_url(file_id)
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

        await set_cached_url(file_id, url)

    return {
        "streams": [{
            "name": "PikPak",
            "title": "PikPak Direct",
            "url": url
        }]
    }
