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
FILE_CACHE_TTL = 60 * 60 * 6          # 6h

FILE_CACHE_KEY = "pikpak:files"
AUTH_CACHE_KEY = "pikpak:auth"

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
def redis_get(key):
    try:
        return redis.get(key)
    except:
        return None

def redis_set(key, val, ttl=None):
    try:
        redis.set(key, val, ex=ttl)
    except:
        pass

def load_auth():
    raw = redis_get(AUTH_CACHE_KEY)
    return json.loads(raw) if raw else None

def save_auth(auth):
    redis_set(AUTH_CACHE_KEY, json.dumps(auth), AUTH_CACHE_TTL)

def get_cached_url(file_id):
    return redis_get(f"pikpak:url:{file_id}")

def set_cached_url(file_id, url):
    redis_set(f"pikpak:url:{file_id}", url, URL_CACHE_TTL)

def load_files_cache():
    raw = redis_get(FILE_CACHE_KEY)
    return json.loads(raw) if raw else None

def save_files_cache(files):
    redis_set(FILE_CACHE_KEY, json.dumps(files), FILE_CACHE_TTL)

# -----------------------
# Utils
# -----------------------
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

# -----------------------
# PikPak Client (STATELESS)
# -----------------------
async def get_client(force_login=False):
    from pikpakapi import PikPakApi

    email = os.environ.get("PIKPAK_EMAIL")
    password = os.environ.get("PIKPAK_PASSWORD")

    if not email or not password:
        raise Exception("Missing PikPak credentials")

    client = PikPakApi(email, password)

    auth = load_auth()
    if auth and not force_login:
        client.auth = auth

        # validate
        try:
            await client.user_info()
            save_auth(client.auth)
            return client
        except:
            pass

        # refresh
        try:
            await client.refresh_access_token()
            save_auth(client.auth)
            return client
        except:
            pass

    # full login fallback
    await client.login()
    save_auth(client.auth)
    return client

async def with_relogin(fn, *args, **kwargs):
    try:
        return await fn(*args, **kwargs)
    except Exception as e:
        if "401" in str(e).lower():
            client = await get_client(force_login=True)
            return await fn(*args, **kwargs)
        raise

# -----------------------
# File traversal
# -----------------------
async def collect_files(client, parent_id="", result=None):
    if result is None:
        result = []

    data = await with_relogin(client.file_list, parent_id=parent_id)
    for f in data.get("files", []):
        if f.get("kind") == "drive#folder":
            await collect_files(client, f["id"], result)
        else:
            result.append(f)

    return result

async def get_all_files(client):
    files = load_files_cache()
    if files:
        return files

    files = await collect_files(client)
    save_files_cache(files)
    return files

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
        "version": "1.6.0",
        "name": "PikPak Cloud",
        "description": "PikPak Stremio addon (catalog + IMDb streams)",
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

    client = await get_client()
    files = await get_all_files(client)

    metas = []
    for f in files:
        name = f.get("name")
        fid = f.get("id")
        if name and fid and name.lower().endswith(VIDEO_EXT):
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

    client = await get_client()

    # direct file
    if id.startswith("pikpak:"):
        fid = id.replace("pikpak:", "")
        url = get_cached_url(fid)

        if not url:
            data = await with_relogin(client.get_download_url, fid)
            links = data.get("links", {})
            if "application/octet-stream" in links:
                url = links["application/octet-stream"]["url"]
            elif data.get("medias"):
                url = data["medias"][0]["link"]["url"]

            if not url:
                return {"streams": []}

            set_cached_url(fid, url)

        return {"streams": [{
            "name": "PikPak",
            "title": "PikPak Direct",
            "url": url
        }]}

    # IMDb
    if type != "movie":
        return {"streams": []}

    title, year = get_movie_info(id)
    title_n = normalize(title)

    files = await get_all_files(client)
    streams = []

    for f in files:
        name = f.get("name")
        fid = f.get("id")

        if not name or not fid or not name.lower().endswith(VIDEO_EXT):
            continue

        n = normalize(name)
        if title_n not in n or (year and year not in n):
            continue

        url = get_cached_url(fid)
        if not url:
            data = await with_relogin(client.get_download_url, fid)
            links = data.get("links", {})
            if "application/octet-stream" in links:
                url = links["application/octet-stream"]["url"]
            elif data.get("medias"):
                url = data["medias"][0]["link"]["url"]
            else:
                continue

            set_cached_url(fid, url)

        streams.append({
            "name": "PikPak",
            "title": name,
            "url": url
        })

    return {"streams": streams}