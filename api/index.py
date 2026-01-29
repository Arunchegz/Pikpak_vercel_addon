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

# Stream URL cache
CACHE_TTL = 60 * 60 * 24  # 24 hours

# File list cache
FILES_CACHE_KEY = "pikpak:filelist"
FILES_CACHE_TTL = 60 * 60 * 24  # 24 hours

# -----------------------
# Redis
# -----------------------
redis = Redis(
    url=os.environ.get("UPSTASH_REDIS_REST_URL"),
    token=os.environ.get("UPSTASH_REDIS_REST_TOKEN"),
)

def get_cached_url(file_id: str):
    try:
        return redis.get(f"pikpak:url:{file_id}")
    except:
        return None

def set_cached_url(file_id: str, url: str):
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
    title = meta.get("name", "")
    year = str(meta.get("year", ""))
    return title, year

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
        raise Exception("PIKPAK_EMAIL or PIKPAK_PASSWORD is missing")

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
        "version": "1.4.1",
        "name": "PikPak Cloud",
        "description": "Browse and stream files from your PikPak cloud with Redis caching",
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

    try:
        pk = await get_client()
        files = await get_files_with_auto_refresh(pk)
    except Exception as e:
        return {"metas": [], "error": str(e)}

    if not files:
        files = await collect_files(pk)
        set_cached_files(files)

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
            "poster": "https://upload.wikimedia.org/wikipedia/commons/8/8c/PikPak_logo.png"
        })

    return {"metas": metas}

# -----------------------
# Stream
# -----------------------
@app.get("/stream/{type}/{id}.json")
async def stream(type: str, id: str):

    # Play directly from catalog
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
                if medias:
                    url = medias[0].get("link", {}).get("url")

            if not url:
                return {"streams": []}

            set_cached_url(file_id, url)

        return {
            "streams": [{
                "name": "PikPak",
                "title": "PikPak Direct Stream",
                "url": url
            }]
        }

    # IMDb matching
    if type != "movie":
        return {"streams": []}

    try:
        movie_title, movie_year = get_movie_info(id)
    except Exception as e:
        return {"streams": [], "error": str(e)}

    movie_title_n = normalize(movie_title)

    try:
        pk = await get_client()
        all_files = await get_files_with_auto_refresh(pk)
    except Exception as e:
        return {"streams": [], "error": str(e)}

    streams = []

    async def build_streams(files):
        result = []
        for f in files:
            name = f.get("name", "")
            file_id = f.get("id")

            if not name or not file_id:
                continue
            if not name.lower().endswith(VIDEO_EXT):
                continue

            file_n = normalize(name)
            if movie_title_n not in file_n:
                continue
            if movie_year and movie_year not in file_n:
                continue

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
                    continue

                set_cached_url(file_id, url)

            result.append({
                "name": "PikPak",
                "title": name,
                "url": url
            })
        return result

    # First try cached file list
    streams = await build_streams(all_files)

    # If not found → force refresh and retry
    if not streams:
        try:
            print("Cache miss → refreshing PikPak file list")
            fresh_files = await collect_files(pk)
            set_cached_files(fresh_files)
            streams = await build_streams(fresh_files)
        except Exception as e:
            print("Auto-refresh failed:", e)

    return {"streams": streams}
