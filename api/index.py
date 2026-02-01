from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os
import re
import json
import requests
from upstash_redis import Redis

# =======================
# FORCE NODE RUNTIME (Vercel)
# =======================
os.environ["VERCEL_RUNTIME"] = "nodejs"

# =======================
# App
# =======================
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =======================
# Constants
# =======================
VIDEO_EXT = (".mp4", ".mkv", ".avi", ".mov", ".webm", ".flv", ".ts")

URL_CACHE_TTL = 60 * 60 * 24
AUTH_CACHE_TTL = 60 * 60 * 24 * 365

# =======================
# Redis (Upstash REST)
# =======================
redis = Redis(
    url=os.environ.get("UPSTASH_REDIS_REST_URL"),
    token=os.environ.get("UPSTASH_REDIS_REST_TOKEN"),
)

# =======================
# Redis DEBUG TEST
# =======================
@app.get("/__redis_test")
async def redis_test():
    try:
        res = redis.set("debug:test", "hello", ex=60)
        val = redis.get("debug:test")
        return {
            "set_result": res,
            "get_value": val,
            "type": str(type(val)),
        }
    except Exception as e:
        return {"error": str(e)}

# =======================
# Redis helpers
# =======================
def get_cached_url(file_id):
    try:
        return redis.get(f"pikpak:url:{file_id}")
    except Exception as e:
        print("[REDIS URL GET ERROR]", e)
        return None


def set_cached_url(file_id, url):
    try:
        redis.set(f"pikpak:url:{file_id}", url, ex=URL_CACHE_TTL)
    except Exception as e:
        print("[REDIS URL SET ERROR]", e)


def minimal_auth(auth: dict):
    return {
        "access_token": auth.get("access_token"),
        "refresh_token": auth.get("refresh_token"),
        "token_type": auth.get("token_type"),
        "expires_in": auth.get("expires_in"),
        "user_id": auth.get("user_id"),
    }


def load_auth():
    try:
        raw = redis.get("pikpak:auth")
        print("[REDIS LOAD RAW]", raw)

        if not raw:
            return None

        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")

        if isinstance(raw, dict):
            return raw

        return json.loads(raw)
    except Exception as e:
        print("[REDIS LOAD ERROR]", e)
        return None


def save_auth(auth: dict):
    try:
        payload = json.dumps(auth)
        res = redis.set("pikpak:auth", payload, ex=AUTH_CACHE_TTL)

        print("[REDIS SAVE]", {
            "result": res,
            "size": len(payload),
            "keys": list(auth.keys())
        })

        # immediate verify
        verify = redis.get("pikpak:auth")
        print("[REDIS VERIFY AFTER SAVE]", verify)

    except Exception as e:
        print("[REDIS SAVE ERROR]", e)

# =======================
# Utils
# =======================
def normalize(text):
    text = text.lower()
    text = re.sub(r"[^a-z0-9 ]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def get_movie_info(imdb_id):
    r = requests.get(
        f"https://v3-cinemeta.strem.io/meta/movie/{imdb_id}.json",
        timeout=10
    )
    meta = r.json().get("meta", {})
    return meta.get("name", ""), str(meta.get("year", ""))

# =======================
# PikPak Client
# =======================
client = None

async def get_client(force_login=False):
    global client
    from pikpakapi import PikPakApi

    EMAIL = os.environ.get("PIKPAK_EMAIL")
    PASSWORD = os.environ.get("PIKPAK_PASSWORD")

    if not EMAIL or not PASSWORD:
        raise Exception("Missing PikPak credentials")

    if client and not force_login:
        return client

    client = PikPakApi(EMAIL, PASSWORD)
    auth = load_auth()

    # ---------- Restore ----------
    if auth and not force_login:
        print("[AUTH] restored from redis")
        client.auth = auth

        try:
            await client.user_info()
            print("[AUTH] access token valid")
            save_auth(minimal_auth(client.auth))
            return client
        except Exception as e:
            print("[AUTH] access token invalid", e)

        try:
            print("[AUTH] trying refresh token")
            await client.refresh_access_token()
            print("[AUTH] refresh token success")
            save_auth(minimal_auth(client.auth))
            return client
        except Exception as e:
            print("[AUTH] refresh failed", e)

    # ---------- Email login ----------
    print("[AUTH] EMAIL LOGIN")
    await client.login()

    print("[AUTH DEBUG]", {
        "has_access": bool(client.auth.get("access_token")),
        "has_refresh": bool(client.auth.get("refresh_token")),
        "expires": client.auth.get("expires_in"),
        "size": len(json.dumps(client.auth)),
    })

    auth_min = minimal_auth(client.auth)
    client.auth = auth_min
    save_auth(auth_min)

    return client


async def with_relogin(fn, *args, **kwargs):
    try:
        return await fn(*args, **kwargs)
    except Exception as e:
        if "401" in str(e).lower():
            print("[AUTH] 401 → relogin")
            await get_client(force_login=True)
            return await fn(*args, **kwargs)
        raise

# =======================
# File traversal
# =======================
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

# =======================
# Routes
# =======================
@app.get("/")
async def root():
    return {"status": "ok", "manifest": "/manifest.json"}


@app.get("/manifest.json")
async def manifest():
    return {
        "id": "com.arun.pikpak",
        "version": "1.5.3",
        "name": "PikPak Cloud",
        "description": "PikPak Stremio addon (auth debug HARD)",
        "types": ["movie"],
        "resources": ["catalog", "stream"],
        "catalogs": [{
            "type": "movie",
            "id": "pikpak",
            "name": "My PikPak Files"
        }],
        "idPrefixes": ["tt", "pikpak"]
    }


@app.get("/catalog/{type}/{id}.json")
async def catalog(type: str, id: str):
    if type != "movie" or id != "pikpak":
        return {"metas": []}

    pk = await get_client()
    files = await collect_files(pk)

    metas = []
    for f in files:
        if f.get("name", "").lower().endswith(VIDEO_EXT):
            metas.append({
                "id": f"pikpak:{f['id']}",
                "type": "movie",
                "name": f["name"],
                "poster": "https://upload.wikimedia.org/wikipedia/commons/8/8c/PikPak_logo.png"
            })

    return {"metas": metas}


@app.get("/stream/{type}/{id}.json")
async def stream(type: str, id: str):
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
            "url": url
        }]
    }