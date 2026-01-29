import os
import time
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pikpakapi import PikPakApi
from upstash_redis import Redis

app = FastAPI()

# -------------------------------
# Upstash Redis init
# -------------------------------
redis = Redis(
    url=os.environ.get("UPSTASH_REDIS_REST_URL"),
    token=os.environ.get("UPSTASH_REDIS_REST_TOKEN")
)

# Cache TTL: 24 hours
CACHE_TTL = 60 * 60 * 24  # 86400 seconds


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
# PikPak client
# -------------------------------
def get_pikpak():
    email = os.environ.get("PIKPAK_EMAIL")
    password = os.environ.get("PIKPAK_PASSWORD")

    if not email or not password:
        raise Exception("PIKPAK_EMAIL or PIKPAK_PASSWORD not set")

    client = PikPakApi(email=email, password=password)
    return client


# -------------------------------
# Stremio manifest
# -------------------------------
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


# -------------------------------
# Stream endpoint
# -------------------------------
@app.get("/stream/{type}/{id}.json")
async def stream(type: str, id: str):
    """
    Shows all video files in PikPak for now.
    (You already filtered by specific movie earlier, keep your filter if needed)
    """

    pk = get_pikpak()
    await pk.login()

    # Get all files from root
    files = await pk.get_file_list(parent_id="")

    streams = []

    for f in files:
        if f.get("file_category") != "VIDEO":
            continue

        file_id = f["id"]
        title = f["name"]

        # 1. Try Redis cache
        cached = get_cached_url(file_id)
        if cached:
            url = cached
        else:
            # 2. Generate new URL from PikPak
            data = await pk.get_download_url(file_id)

            url = None

            links = data.get("links", {})
            if "application/octet-stream" in links:
                url = links["application/octet-stream"].get("url")

            if not url:
                medias = data.get("medias", [])
                if medias:
                    link = medias[0].get("link", {})
                    url = link.get("url")

            if not url:
                continue

            # 3. Save to Redis for 24 hours
            set_cached_url(file_id, url)

        streams.append({
            "name": "PikPak",
            "title": title,
            "url": url
        })

    return JSONResponse({"streams": streams})


# -------------------------------
# Root check
# -------------------------------
@app.get("/")
async def root():
    return {
        "status": "ok",
        "cache": "Upstash Redis (24h TTL)",
        "addon": "PikPak Stremio"
    }
