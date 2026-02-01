from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os
import re
import json
import logging
import requests
from upstash_redis import Redis

# -----------------------
# Logging (Vercel-safe)
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

URL_CACHE_TTL = 60 * 60 * 24              # 24h
AUTH_CACHE_TTL = 60 * 60 * 24 * 365       # 365 days

# -----------------------
# Redis (Upstash)
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
    except Exception as e:
        logging.warning(f"[REDIS] get_cached_url error: {e}")
        return None


def set_cached_url(file_id: str, url: str):
    try:
        redis.set(
            f"pikpak:url:{file_id}",
            url,
            ex=URL_CACHE_TTL,
        )
    except Exception as e:
        logging.warning(f"[REDIS] set_cached_url error: {e}")


def save_auth(auth: dict):
    try:
        redis.set(
            "pikpak:auth",
            json.dumps(auth),     # ✅ JSON serialize
            ex=AUTH_CACHE_TTL,    # ✅ refresh TTL every time
        )
        logging.info("[AUTH] Auth saved to Redis")
    except Exception as e:
        logging.warning(f"[REDIS] save_auth error: {e}")


def load_auth():
    try:
        raw = redis.get("pikpak:auth")
        if not raw:
            return None
        if isinstance(raw, str):
            return json.loads(raw)
        return raw
    except Exception as e:
        logging.warning(f"[REDIS] load_auth error: {e}")
        return None

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


def log_auth_state(stage: str, auth):
    if not isinstance(auth, dict):
        logging.info(f"[AUTH] {stage} | invalid_auth_type={type(auth)}")
        return

    safe = {
        "user_id": auth.get("user_id"),
        "expires_at": auth.get("expires_at"),
        "refresh_expires_at": auth.get("refresh_expires_at"),
        "has_refresh_token": bool(auth.get("refresh_token")),
    }

    logging.info(f"[AUTH] {stage} | {json.dumps(safe)}")

# -----------------------
# PikPak client
# -----------------------
client = None


async def get_client(force_login: bool = False):
    """
    Auth flow (FIXED):
    1. Load auth from Redis
    2. Validate access token
       -> ALWAYS re-save auth (refresh TTL)
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

    # ---------- Restore auth ----------
    if auth and not force_login:
        client.auth = auth
        logging.info("[AUTH] Loaded auth from Redis")
        log_auth_state("restore", auth)

        # 1️⃣ Validate access token
        try:
            await client.user_info()
            logging.info("[AUTH] Access token valid")

            # 🔥 MISSING PIECE (NOW FIXED)
            # Always re-save auth to refresh TTL / persistence
            save_auth(client.auth)

            return client
        except Exception as e:
            logging.warning(f"[AUTH] Access token failed: {e}")

        # 2️⃣ Try refresh token
        try:
            await client.refresh_access_token()
            save_auth(client.auth)
            logging.info("[AUTH] Refresh token success")
            log_auth_state("refresh", client.auth)
            return client
        except Exception as e:
            logging.warning(f"[AUTH] Refresh token failed: {e}")

    # ---------- Full login ----------
    logging.warning("[AUTH] Full login triggered")
    await client.login()
    save_auth(client.auth)
    log_auth_state("full_login", client.auth)

    return client


async def with_relogin(fn, *args, **kwargs):
    try:
        return await fn(*args, **kwargs)
    except Exception as e:
        msg = str(e).lower()
        logging.warning(f"[AUTH] API error: {e}")

        if "401" in msg or "unauthorized" in msg:
            logging.warning("[AUTH] 401 detected → force login")
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

# -----------------------
# Manifest
# -----------------------
@app.get("/manifest.json")
async def manifest():
    return {
        "id": "com.arun.pikpak",
        "version": "1.4.2",
        "name": "PikPak Cloud",
        "description": "PikPak Stremio addon with persistent Redis auth",
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
            "poster": "https://upload.wikimedia.org/wikipedia/commons/8/8c/PikPak_logo.png",
        })

    return {"metas": metas}

# -----------------------
# Stream
# -----------------------
@app.get("/stream/{type}/{id}.json")
async def stream(type: str, id: str):

    if id.startswith("pikpak:"):
        file_id = id.replace("pikpak:", "")
        pk = await get_client()

        url = get_cached_url(file_id)
        if not url:
            logging.info(f"[STREAM] Generating URL for file_id={file_id}")
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

        return {"streams": [{
            "name": "PikPak",
            "title": "PikPak Direct",
            "url": url,
        }]}

    return {"streams": []}