from fastapi import FastAPI
import os
import re
import requests
from upstash_redis import Redis

app = FastAPI()

# -------------------------------
# Upstash Redis setup
# -------------------------------
redis = Redis(
    url=os.environ.get("UPSTASH_REDIS_REST_URL"),
    token=os.environ.get("UPSTASH_REDIS_REST_TOKEN")
)

CACHE_TTL = 60 * 60 * 24  # 24 hours


def get_cached_url(file_id: str):
    try:
        return redis.get(f"pikpak:{file_id}")
    except Exception:
        return None


def set_cached_url(file_id: str, url: str):
    try:
        redis.set(f"pikpak:{file_id}", url, ex=CACHE_TTL)
    except Exception:
        pass


# -------------------------------
# App config
# -------------------------------
VIDEO_EXT = (".mp4", ".mkv", ".avi", ".mov", ".webm", ".flv", ".ts")

client = None


def normalize(text: str) -> str:
    text = text.lower()
    text = re.sub(r'[^a-z0-9 ]', ' ', text)
    return re.sub(r'\s+', ' ', text).strip()


def get_movie_info(imdb_id: str):
    """
    Get movie title and year from Stremio Cinemeta.
    """
    url = f"https://v3-cinemeta.strem.io/meta/movie/{imdb_id}.json"
    r = requests.get(url, timeout=10)
    data = r.json()
    meta = data.get("meta", {})
    title = meta.get("name", "")
    year = str(meta.get("year", ""))
    return title, year


async def get_client():
    global client
    try:
        from pikpakapi import PikPakApi
    except Exception as e:
        raise Exception(f"Failed to import pikpakapi: {e}")

    EMAIL = os.environ.get("PIKPAK_EMAIL")
    PASSWORD = os.environ.get("PIKPAK_PASSWORD")

    if not EMAIL or not PASSWORD:
        raise Exception("PIKPAK_EMAIL or PIKPAK_PASSWORD is missing")

    if client is None:
        try:
            client = PikPakApi(EMAIL, PASSWORD)
            await client.login()
        except Exception as e:
            raise Exception(f"PikPak login failed: {e}")

    return client


async def collect_files(pk, parent_id="", result=None):
    """
    Recursively collect all files from PikPak cloud.
    """
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


# -------------------------------
# Routes
# -------------------------------
@app.get("/")
async def root():
    return {
        "status": "ok",
        "message": "PikPak Stremio addon running",
        "manifest": "/manifest.json",
        "cache": "Upstash Redis (24h TTL)"
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
    # Only handle movies for now
    if type != "movie":
        return {"streams": []}

    # Get movie info from Cinemeta
    try:
        movie_title, movie_year = get_movie_info(id)
    except Exception as e:
        return {
            "streams": [],
            "error": "Failed to fetch movie metadata",
            "detail": str(e)
        }

    movie_title_n = normalize(movie_title)

    # Init PikPak client
    try:
        pk = await get_client()
    except Exception as e:
        return {
            "streams": [],
            "error": "Client init failed",
            "detail": str(e)
        }

    # Collect all files from PikPak
    try:
        all_files = await collect_files(pk, parent_id="")
    except Exception as e:
        return {
            "streams": [],
            "error": "File traversal failed",
            "detail": str(e)
        }

    streams = []

    for f in all_files:
        try:
            name = f.get("name", "")
            file_id = f.get("id")

            if not name or not file_id:
                continue

            # Only video files
            if not name.lower().endswith(VIDEO_EXT):
                continue

            file_n = normalize(name)

            # Must contain movie title
            if movie_title_n not in file_n:
                continue

            # If year exists, match year too
            if movie_year and movie_year not in file_n:
                continue

            # -----------------------
            # CACHING LOGIC
            # -----------------------

            # 1. Check Redis cache first
            cached = get_cached_url(file_id)
            if cached:
                url = cached
            else:
                # 2. Ask PikPak to generate download link
                try:
                    data = await pk.get_download_url(file_id)
                except Exception as e:
                    print("get_download_url failed for", name, ":", e)
                    continue

                url = None

                # Primary: links → application/octet-stream → url
                links = data.get("links", {})
                if "application/octet-stream" in links:
                    url = links["application/octet-stream"].get("url")

                # Fallback: medias → first → link → url
                if not url:
                    medias = data.get("medias", [])
                    if medias:
                        url = medias[0].get("link", {}).get("url")

                if not url:
                    print("No playable URL found for:", name)
                    continue

                # 3. Store in Redis for 24 hours
                set_cached_url(file_id, url)

            streams.append({
                "name": "PikPak",
                "title": name,
                "url": url
            })

        except Exception as e:
            print("File processing error:", e)
            continue

    return {
        "streams": streams
    }