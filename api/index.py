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
URL_CACHE_TTL = 60 * 60 * 24
AUTH_CACHE_TTL = 60 * 60 * 24 * 365

# -----------------------
# Redis
# -----------------------
redis = Redis(
    url=os.environ["UPSTASH_REDIS_REST_URL"],
    token=os.environ["UPSTASH_REDIS_REST_TOKEN"],
)

# -----------------------
# Redis helpers
# -----------------------
def load_auth():
    try:
        raw = redis.get("pikpak:auth")
        if not raw:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode()
        return json.loads(raw)
    except:
        return None


def save_auth(auth: dict):
    redis.set("pikpak:auth", json.dumps(auth), ex=AUTH_CACHE_TTL)


def get_cached_url(fid):
    try:
        return redis.get(f"pikpak:url:{fid}")
    except:
        return None


def set_cached_url(fid, url):
    redis.set(f"pikpak:url:{fid}", url, ex=URL_CACHE_TTL)

# -----------------------
# Utils
# -----------------------
def normalize(text: str):
    text = text.lower()
    text = re.sub(r"[^a-z0-9 ]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def get_movie_info(imdb):
    r = requests.get(
        f"https://v3-cinemeta.strem.io/meta/movie/{imdb}.json",
        timeout=10
    )
    meta = r.json().get("meta", {})
    return meta.get("name", ""), str(meta.get("year", ""))

# -----------------------
# PikPak Client
# -----------------------
client = None

async def get_client(force_login=False):
    global client
    from pikpakapi import PikPakApi

    EMAIL = os.environ["PIKPAK_EMAIL"]
    PASSWORD = os.environ["PIKPAK_PASSWORD"]

    if client and not force_login:
        return client

    client = PikPakApi(EMAIL, PASSWORD)

    auth = load_auth()

    # ---------- Restore token ----------
    if auth and auth.get("access_token") and not force_login:
        client.session.headers["Authorization"] = f"Bearer {auth['access_token']}"
        try:
            await client.user_info()
            return client
        except:
            pass

    # ---------- Email login ----------
    await client.login()

    # 🔑 TOKEN LIVES HERE (NOT client.auth)
    auth_header = client.session.headers.get("Authorization", "")
    access_token = auth_header.replace("Bearer ", "")

    auth_data = {
        "access_token": access_token
    }

    save_auth(auth_data)

    return client


async def with_relogin(fn, *args, **kwargs):
    try:
        return await fn(*args, **kwargs)
    except Exception as e:
        if "401" in str(e):
            await get_client(force_login=True)
            return await fn(*args, **kwargs)
        raise

# -----------------------
# Recursive file scan
# -----------------------
async def collect_files(pk, parent_id="", out=None):
    if out is None:
        out = []

    data = await with_relogin(pk.file_list, parent_id=parent_id)
    for f in data.get("files", []):
        if f.get("kind") == "drive#folder":
            await collect_files(pk, f["id"], out)
        else:
            out.append(f)

    return out

# -----------------------
# Routes
# -----------------------
@app.get("/")
async def root():
    return {"status": "ok"}

@app.get("/manifest.json")
async def manifest():
    return {
        "id": "com.arun.pikpak",
        "version": "1.6.0",
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
        if f["name"].lower().endswith(VIDEO_EXT):
            metas.append({
                "id": f"pikpak:{f['id']}",
                "type": "movie",
                "name": f["name"],
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

    fid = id.replace("pikpak:", "")
    pk = await get_client()

    url = get_cached_url(fid)
    if not url:
        data = await with_relogin(pk.get_download_url, fid)

        links = data.get("links", {})
        if "application/octet-stream" in links:
            url = links["application/octet-stream"]["url"]
        else:
            url = data["medias"][0]["link"]["url"]

        set_cached_url(fid, url)

    return {
        "streams": [{
            "name": "PikPak",
            "title": "PikPak Direct",
            "url": url
        }]
    }