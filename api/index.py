from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os
import re
import requests
from upstash_redis import Redis

print("[BOOT] Cold start - new Vercel instance")

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

URL_CACHE_TTL = 60 * 60 * 24
AUTH_CACHE_TTL = 60 * 60 * 24 * 365

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
def get_cached_url(file_id):
    try:
        return redis.get(f"pikpak:url:{file_id}")
    except Exception as e:
        print("[REDIS] url get failed:", e)
        return None


def set_cached_url(file_id, url):
    try:
        redis.set(f"pikpak:url:{file_id}", url, ex=URL_CACHE_TTL)
    except Exception as e:
        print("[REDIS] url set failed:", e)


def load_auth():
    try:
        return redis.get("pikpak:auth")
    except Exception as e:
        print("[REDIS] auth load failed:", e)
        return None


def save_auth(auth):
    if not auth:
        return
    try:
        redis.set("pikpak:auth", auth, ex=AUTH_CACHE_TTL)
    except Exception as e:
        print("[REDIS] auth save failed:", e)

# -----------------------
# Auth helpers (CRITICAL)
# -----------------------
def extract_auth(client):
    refresh = getattr(client, "refresh_token", None)
    if not refresh:
        print("[AUTH] No refresh token → not saving auth")
        return None

    return {
        "access_token": client.access_token,
        "refresh_token": refresh,
        "user_id": getattr(client, "user_id", None),
        "device_id": getattr(client, "device_id", None),
    }


def restore_auth(client, auth):
    client.access_token = auth.get("access_token")
    client.refresh_token = auth.get("refresh_token")

    if auth.get("user_id"):
        client.user_id = auth.get("user_id")
    if auth.get("device_id"):
        client.device_id = auth.get("device_id")

# -----------------------
# Utils
# -----------------------
def normalize(text):
    text = text.lower()
    text = re.sub(r"[^a-z0-9 ]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def get_movie_info(imdb_id):
    url = f"https://v3-cinemeta.strem.io/meta/movie/{imdb_id}.json"
    r = requests.get(url, timeout=10)
    meta = r.json().get("meta", {})
    return meta.get("name", ""), str(meta.get("year", ""))

# -----------------------
# PikPak Client
# -----------------------
client = None


async def get_client(force_login=False):
    global client
    from pikpakapi import PikPakApi

    EMAIL = os.environ.get("PIKPAK_EMAIL")
    PASSWORD = os.environ.get("PIKPAK_PASSWORD")

    if not EMAIL or not PASSWORD:
        raise Exception("Missing PIKPAK_EMAIL or PIKPAK_PASSWORD")

    if client and not force_login:
        print("[AUTH] Using in-memory client")
        return client

    client = PikPakApi(EMAIL, PASSWORD)
    auth = load_auth()

    # ---------- Restore ----------
    if auth and not force_login:
        print("[AUTH] Restoring auth from Redis")
        restore_auth(client, auth)

        # 1) Access token
        try:
            await client.user_info()
            print("[AUTH] Access token valid ✅")
            return client
        except Exception as e:
            print("[AUTH] Access token invalid ❌", e)

        # 2) Refresh ONLY if token exists
        if auth.get("refresh_token"):
            try:
                await client.refresh_access_token()
                save_auth(extract_auth(client))
                print("[AUTH] Refresh token success 🔄")
                return client
            except Exception as e:
                print("[AUTH] Refresh token failed ❌", e)
        else:
            print("[AUTH] No refresh token → skipping refresh")

    # ---------- Full login ----------
    print("[AUTH] FULL LOGIN using EMAIL + PASSWORD 🚨")
    await client.login()
    save_auth(extract_auth(client))
    return client


async def with_relogin(fn, *args, **kwargs):
    try:
        return await fn(*args, **kwargs)
    except Exception as e:
        msg = str(e).lower()
        if "401" in msg or "unauthorized" in msg:
            print("[AUTH] 401 → forcing FULL login")
            await get_client(force_login=True)
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
        "manifest": "/manifest.json",
    }


@app.get("/debug/auth")
async def debug_auth():
    auth = load_auth()
    return {
        "auth_in_redis": bool(auth),
        "keys": list(auth.keys()) if auth else None,
        "has_refresh": bool(auth.get("refresh_token")) if auth else False,
    }

# -----------------------
# Manifest
# -----------------------
@app.get("/manifest.json")
async def manifest():
    return {
        "id": "com.arun.pikpak",
        "version": "1.4.3",
        "name": "PikPak Cloud",
        "description": "PikPak Stremio addon (stable auth, Vercel-safe)",
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
@app.get("/catalog/{type}/{id}.json")
async def catalog(type, id):
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
            "poster": "https://upload.wikimedia.org/wikipedia/commons/8/8c/PikPak_logo.png",
        })

    return {"metas": metas}

# -----------------------
# Stream
# -----------------------
@app.get("/stream/{type}/{id}.json")
async def stream(type, id):
    if not id.startswith("pikpak:"):
        return {"streams": []}

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
            "url": url,
        }]
    }