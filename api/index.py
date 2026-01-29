from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os
import re
import requests
from upstash_redis import Redis

app = FastAPI()

# -----------------------
# CORS (needed for Stremio)
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
CACHE_TTL = 60 * 60 * 24  # 24 hours

# -----------------------
# Upstash Redis
# -----------------------
redis = Redis(
    url=os.environ.get("UPSTASH_REDIS_REST_URL"),
    token=os.environ.get("UPSTASH_REDIS_REST_TOKEN"),
)

def get_cached_url(file_id: str):
    try:
        return redis.get(f"pikpak:{file_id}")
    except:
        return None

def set_cached_url(file_id: str, url: str):
    try:
        redis.set(f"pikpak:{file_id}", url, ex=CACHE_TTL)
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
# PikPak client
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
        "version": "1.1.0",
        "name": "PikPak Cloud",
        "description": "Stream files from your PikPak cloud (with Redis caching)",
        "types": ["movie", "series"],
        "resources": ["stream"],
        "idPrefixes": ["tt"]
    }

@app.get("/stream/{type}/{id}.json")
async def stream(type: str, id: str):
    if type != "movie":
        return {"streams": []}

    # Get movie metadata
    try:
        movie_title, movie_year = get_movie_info(id)
    except Exception as e:
        return {"streams": [], "error": str(e)}

    movie_title_n = normalize(movie_title)

    # Init PikPak
    try:
        pk = await get_client()
    except Exception as e:
        return {"streams": [], "error": f"PikPak init failed: {e}"}

    # Collect all files
    try:
        all_files = await collect_files(pk)
    except Exception as e:
        return {"streams": [], "error": f"File listing failed: {e}"}

    streams = []

    for f in all_files:
        try:
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

            # -----------------------
            # Redis cache check
            # -----------------------
            cached = get_cached_url(file_id)
            if cached:
                url = cached
            else:
                # Generate new link
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

                # Store in Redis for 24 hours
                set_cached_url(file_id, url)

            streams.append({
                "name": "PikPak",
                "title": name,
                "url": url
            })

        except Exception as e:
            print("Error processing file:", e)
            continue

    return {"streams": streams}
