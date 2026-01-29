from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os
import re
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

CACHE_TTL = 60 * 60 * 24        # Stream URL cache: 24h
FILES_CACHE_KEY = "pikpak:filelist"
FILES_CACHE_TTL = 60 * 60 * 24  # File list cache: 24h

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

def get_cached_files():
    try:
        return redis.get(FILES_CACHE_KEY)
    except:
        return None

def set_cached_files(files):
    try:
        redis.set(FILES_CACHE_KEY, files, ex=FILES_CACHE_TTL)
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
    return meta.get("name", ""), str(meta.get("year", ""))

# -----------------------
# PikPak Client
# -----------------------
client = None

async def get_client():
    global client
    from pikpakapi import PikPakApi

    EMAIL = os.environ.get("PIKPAK_EMAIL")
    PASSWORD = os.environ.get("PIKPAK_PASSWORD")

    if not EMAIL or not PASSWORD:
        raise Exception("Missing PikPak credentials")

    if client is None:
        client = PikPakApi(EMAIL, PASSWORD)
        await client.login()

    return client

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

async def get_files_with_auto_refresh(pk):
    cached = get_cached_files()
    if cached:
        return cached

    files = await collect_files(pk)
    set_cached_files(files)
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

@app.get("/manifest.json")
async def manifest():
    return {
        "id": "com.arun.pikpak",
        "version": "1.5.1",
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
# Catalog
# -----------------------
@app.get("/catalog/{type}/{id}.json")
async def catalog(type: str, id: str):
    if type != "movie" or id != "pikpak":
        return {"metas": []}

    pk = await get_client()
    files = await get_files_with_auto_refresh(pk)

    metas = []
    for f in files:
        name = f.get("name", "")
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

    # Direct play from catalog
    if id.startswith("pikpak:"):
        file_id = id.replace("pikpak:", "")

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
                if medias:
                    url = medias[0].get("link", {}).get("url")

            if not url:
                return {"streams": []}

            set_cached_url(file_id, url)

        return {"streams": [{
            "name": "PikPak",
            "title": "PikPak Direct Stream",
            "url": url
        }]}

    # IMDb movie matching
    if type != "movie":
        return {"streams": []}

    movie_title, movie_year = get_movie_info(id)
    movie_title_n = normalize(movie_title)

    files = await get_files_with_auto_refresh(pk)

    async def build_streams(file_list):
        streams = []
        for f in file_list:
            name = f.get("name", "")
            fid = f.get("id")

            if not name or not fid:
                continue
            if not name.lower().endswith(VIDEO_EXT):
                continue

            fn = normalize(name)
            if movie_title_n not in fn:
                continue
            if movie_year and movie_year not in fn:
                continue

            cached = get_cached_url(fid)
            if cached:
                url = cached
            else:
                data = await pk.get_download_url(fid)
                url = None

                links = data.get("links", {})
                if "application/octet-stream" in links:
                    url = links["application/octet-stream"].get("url")

                if not url:
                    medias = data.get("medias", [])
                    if medias:
                        url = medias[0].get("link", {}).get("url")

                if not url:
                    continue

                set_cached_url(fid, url)

            streams.append({
                "name": "PikPak",
                "title": name,
                "url": url
            })
        return streams

    # First try cache
    streams = await build_streams(files)

    # If nothing found → force refresh
    if not streams:
        fresh_files = await collect_files(pk)
        set_cached_files(fresh_files)
        streams = await build_streams(fresh_files)

    return {"streams": streams}
