from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os
import re
import requests
from upstash_redis import Redis

# ======================
# App
# ======================
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ======================
# Constants
# ======================
VIDEO_EXT = (".mp4", ".mkv", ".avi", ".mov", ".webm", ".flv", ".ts")

FILE_CACHE_TTL = 60 * 60 * 6     # 6 hours
URL_CACHE_TTL = 60 * 60 * 24     # 24 hours

# ======================
# Redis (cache ONLY)
# ======================
redis = Redis(
    url=os.environ["UPSTASH_REDIS_REST_URL"],
    token=os.environ["UPSTASH_REDIS_REST_TOKEN"],
)

# ======================
# Utils
# ======================
def normalize(text: str):
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

# ======================
# PikPak client (COOKIE BASED)
# ======================
client = None

async def get_client(force=False):
    """
    One login per cold start.
    Cookies live only in memory.
    """
    global client
    from pikpakapi import PikPakApi

    if client and not force:
        return client

    client = PikPakApi(
        os.environ["PIKPAK_EMAIL"],
        os.environ["PIKPAK_PASSWORD"]
    )

    print("[AUTH] login (cookie-based)")
    await client.login()
    return client


async def with_relogin(fn, *args, **kwargs):
    try:
        return await fn(*args, **kwargs)
    except Exception as e:
        if "401" in str(e):
            print("[AUTH] 401 → relogin")
            await get_client(force=True)
            return await fn(*args, **kwargs)
        raise

# ======================
# File traversal (cached)
# ======================
async def collect_files(pk):
    cached = redis.get("pikpak:files")
    if cached:
        return cached

    files = []

    async def walk(parent=""):
        data = await with_relogin(pk.file_list, parent_id=parent)
        for f in data.get("files", []):
            if f.get("kind") == "drive#folder":
                await walk(f["id"])
            else:
                files.append(f)

    await walk()

    redis.set("pikpak:files", files, ex=FILE_CACHE_TTL)
    return files

# ======================
# Routes
# ======================
@app.get("/")
async def root():
    return {"status": "ok", "manifest": "/manifest.json"}


@app.get("/manifest.json")
async def manifest():
    return {
        "id": "com.arun.pikpak",
        "version": "1.7.0",
        "name": "PikPak Cloud",
        "description": "PikPak Stremio addon (cookie-auth correct)",
        "types": ["movie"],
        "resources": ["catalog", "stream"],
        "catalogs": [{
            "type": "movie",
            "id": "pikpak",
            "name": "My PikPak Files"
        }],
        "idPrefixes": ["tt", "pikpak"]
    }

# ======================
# Catalog
# ======================
@app.get("/catalog/{type}/{id}.json")
async def catalog(type: str, id: str):
    if type != "movie" or id != "pikpak":
        return {"metas": []}

    pk = await get_client()
    files = await collect_files(pk)

    metas = []
    for f in files:
        name = f.get("name", "")
        if name.lower().endswith(VIDEO_EXT):
            metas.append({
                "id": f"pikpak:{f['id']}",
                "type": "movie",
                "name": name,
                "poster": "https://upload.wikimedia.org/wikipedia/commons/8/8c/PikPak_logo.png"
            })

    return {"metas": metas}

# ======================
# Stream
# ======================
@app.get("/stream/{type}/{id}.json")
async def stream(type: str, id: str):

    if not id.startswith("pikpak:"):
        return {"streams": []}

    file_id = id.replace("pikpak:", "")

    cached_url = redis.get(f"pikpak:url:{file_id}")
    if cached_url:
        return {
            "streams": [{
                "name": "PikPak",
                "title": "PikPak Direct",
                "url": cached_url
            }]
        }

    pk = await get_client()
    data = await with_relogin(pk.get_download_url, file_id)

    links = data.get("links", {})
    if "application/octet-stream" in links:
        url = links["application/octet-stream"]["url"]
    else:
        medias = data.get("medias", [])
        if not medias:
            return {"streams": []}
        url = medias[0]["link"]["url"]

    redis.set(f"pikpak:url:{file_id}", url, ex=URL_CACHE_TTL)

    return {
        "streams": [{
            "name": "PikPak",
            "title": "PikPak Direct",
            "url": url
        }]
    }