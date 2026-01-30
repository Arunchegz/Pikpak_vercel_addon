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
# CORS
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
CACHE_TTL = 60 * 60 * 24
LOGIN_TTL = 60 * 60  # 1 hour

# -----------------------
# Redis
# -----------------------
redis = Redis(
    url=os.environ.get("UPSTASH_REDIS_REST_URL"),
    token=os.environ.get("UPSTASH_REDIS_REST_TOKEN"),
)

def get_cached_url(file_id):
    try:
        return redis.get(f"pikpak:url:{file_id}")
    except:
        return None

def set_cached_url(file_id, url):
    try:
        redis.set(f"pikpak:url:{file_id}", url, ex=CACHE_TTL)
    except:
        pass

# -----------------------
# Utils
# -----------------------
def normalize(text: str) -> str:
    text = text.lower()
    text = re.sub(r"[^a-z0-9 ]", " ", text)
    return re.sub(r"\s+", " ", text).strip()

JUNK_WORDS = [
    "1080p", "720p", "2160p", "4k",
    "bluray", "brrip", "webrip", "webdl", "hdrip",
    "x264", "x265", "h264", "h265", "hevc",
    "aac", "dts", "ddp", "atmos",
    "yts", "rarbg", "esubs", "subs"
]

def clean_filename(name: str) -> str:
    name = name.lower()
    name = re.sub(r"\.(mkv|mp4|avi|mov|webm|flv|ts)$", "", name)
    for word in JUNK_WORDS:
        name = name.replace(word, " ")
    name = re.sub(r"[^a-z0-9 ]", " ", name)
    name = re.sub(r"\s+", " ", name)
    return name.strip()

def loose_match(title: str, filename: str) -> bool:
    title_words = title.split()
    score = sum(1 for w in title_words if w in filename)
    return score >= max(2, len(title_words) // 2)

def get_movie_info(imdb_id: str):
    r = requests.get(
        f"https://v3-cinemeta.strem.io/meta/movie/{imdb_id}.json",
        timeout=10
    )
    meta = r.json().get("meta", {})
    return meta.get("name", ""), str(meta.get("year", ""))

# -----------------------
# PikPak client (safe login)
# -----------------------
client = None

async def get_client():
    global client
    from pikpakapi import PikPakApi

    EMAIL = os.environ.get("PIKPAK_EMAIL")
    PASSWORD = os.environ.get("PIKPAK_PASSWORD")

    if not EMAIL or not PASSWORD:
        raise Exception("PIKPAK_EMAIL or PIKPAK_PASSWORD missing")

    now = time.time()
    expires_at = redis.get("pikpak:login_expires")

    if client and expires_at and now < float(expires_at):
        return client

    if redis.get("pikpak:auth_lock"):
        await asyncio.sleep(2)
        expires_at = redis.get("pikpak:login_expires")
        if client and expires_at and time.time() < float(expires_at):
            return client

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
# Recursive file list
# -----------------------
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

# -----------------------
# Routes
# -----------------------
@app.get("/")
async def root():
    return {"status": "ok", "manifest": "/manifest.json"}

@app.get("/manifest.json")
async def manifest():
    return {
        "id": "com.arun.pikpak",
        "version": "1.4.0",
        "name": "PikPak Cloud",
        "description": "Stream your PikPak files + auto match IMDb movies",
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
# Catalog (My Files)
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
    pk = await get_client()

    # Direct play (catalog)
    if id.startswith("pikpak:"):
        file_id = id.split(":", 1)[1]
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

    # IMDb movie page matching
    if type != "movie":
        return {"streams": []}

    movie_title, movie_year = get_movie_info(id)
    title_clean = clean_filename(movie_title)

    files = await collect_files(pk)
    streams = []

    for f in files:
        name = f.get("name", "")
        fid = f.get("id", "")
        if not name.lower().endswith(VIDEO_EXT):
            continue

        file_clean = clean_filename(name)

        if not (
            title_clean in file_clean or
            loose_match(title_clean, file_clean)
        ):
            continue

        if movie_year and movie_year not in file_clean:
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
