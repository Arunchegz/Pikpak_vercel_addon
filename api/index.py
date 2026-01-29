from fastapi import FastAPI
import os
import re
import requests
import time
from upstash_redis import Redis

app = FastAPI()

# ---------------- CONFIG ----------------

VIDEO_EXT = (".mp4", ".mkv", ".avi", ".mov", ".webm", ".flv", ".ts")

PIKPAK_EMAIL = os.environ.get("PIKPAK_EMAIL")
PIKPAK_PASSWORD = os.environ.get("PIKPAK_PASSWORD")

UPSTASH_URL = os.environ.get("UPSTASH_REDIS_REST_URL")
UPSTASH_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN")

# Init Upstash Redis
redis = Redis(url=UPSTASH_URL, token=UPSTASH_TOKEN)

client = None


# ---------------- HELPERS ----------------

def normalize(text: str) -> str:
    text = text.lower()
    text = re.sub(r'[^a-z0-9 ]', ' ', text)
    return re.sub(r'\s+', ' ', text).strip()


def get_movie_info(imdb_id: str):
    url = f"https://v3-cinemeta.strem.io/meta/movie/{imdb_id}.json"
    r = requests.get(url, timeout=10)
    data = r.json()
    meta = data.get("meta", {})
    title = meta.get("name", "")
    year = str(meta.get("year", ""))
    return title, year


async def get_client():
    global client
    from pikpakapi import PikPakApi

    if not PIKPAK_EMAIL or not PIKPAK_PASSWORD:
        raise Exception("PIKPAK_EMAIL or PIKPAK_PASSWORD missing")

    if client is None:
        client = PikPakApi(PIKPAK_EMAIL, PIKPAK_PASSWORD)
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


# ---------------- CACHE (UPSTASH) ----------------

def get_cached_url(file_id):
    try:
        return redis.get(f"pikpak:{file_id}")
    except:
        return None


def set_cached_url(file_id, url, expires_at):
    try:
        ttl = max(60, expires_at - int(time.time()))
        redis.set(f"pikpak:{file_id}", url, ex=ttl)
    except:
        pass


# ---------------- ROUTES ----------------

@app.get("/")
async def root():
    return {
        "status": "ok",
        "message": "PikPak Stremio addon running",
        "manifest": "/manifest.json"
    }


@app.get("/manifest.json")
async def manifest():
    return {
        "id": "com.arun.pikpak",
        "version": "1.0.0",
        "name": "PikPak Cloud",
        "description": "Stream files from your PikPak cloud",
        "types": ["movie", "series"],
        "resources": ["stream"],
        "idPrefixes": ["tt"]
    }


@app.get("/stream/{type}/{id}.json")
async def stream(type: str, id: str):
    if type != "movie":
        return {"streams": []}

    # 1. Get movie info
    try:
        movie_title, movie_year = get_movie_info(id)
    except Exception as e:
        return {"streams": [], "error": "Cinemeta failed", "detail": str(e)}

    movie_title_n = normalize(movie_title)

    # 2. Init PikPak
    try:
        pk = await get_client()
    except Exception as e:
        return {"streams": [], "error": "PikPak login failed", "detail": str(e)}

    # 3. Collect PikPak files
    try:
        all_files = await collect_files(pk)
    except Exception as e:
        return {"streams": [], "error": "File traversal failed", "detail": str(e)}

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

            # Match movie title
            if movie_title_n not in file_n:
                continue

            # Match year if available
            if movie_year and movie_year not in file_n:
                continue

            # 4. Try cache first
            cached_url = get_cached_url(file_id)
            if cached_url:
                url = cached_url
            else:
                # 5. Generate new URL from PikPak
                data = await pk.get_download_url(file_id)

                url = None
                expires_at = int(time.time()) + 86400  # default 24 hour

                links = data.get("links", {})
                if "application/octet-stream" in links:
                    link_data = links["application/octet-stream"]
                    url = link_data.get("url")
                    expires_at = int(time.time()) + 3600

                if not url:
                    medias = data.get("medias", [])
                    if medias:
                        link = medias[0].get("link", {})
                        url = link.get("url")
                        expires_at = int(time.time()) + 3600

                if not url:
                    continue

                # 6. Save to Upstash
                set_cached_url(file_id, url, expires_at)

            streams.append({
                "name": "PikPak",
                "title": name,
                "url": url
            })

        except Exception as e:
            print("File error:", e)
            continue

    return {
        "streams": streams
    }