import os
import time
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pikpakapi import PikPakApi
from upstash_redis import Redis

app = FastAPI()

# ===============================
# Upstash Redis Init
# ===============================
redis = Redis(
    url=os.environ.get("UPSTASH_REDIS_REST_URL"),
    token=os.environ.get("UPSTASH_REDIS_REST_TOKEN")
)

CACHE_TTL = 60 * 60 * 24  # 24 hours


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


# ===============================
# PikPak Client
# ===============================
def get_pikpak():
    email = os.environ.get("PIKPAK_EMAIL")
    password = os.environ.get("PIKPAK_PASSWORD")

    if not email or not password:
        raise Exception("PIKPAK_EMAIL and PIKPAK_PASSWORD must be set")

    return PikPakApi(email=email, password=password)


# ===============================
# Manifest
# ===============================
@app.get("/manifest.json")
async def manifest():
    return {
        "id": "com.arun.pikpak",
        "version": "1.2.0",
        "name": "PikPak Cloud",
        "description": "Browse and stream your PikPak cloud inside Stremio",
        "types": ["movie"],
        "resources": ["stream", "catalog"],
        "catalogs": [
            {
                "type": "movie",
                "id": "pikpak",
                "name": "My PikPak Files"
            }
        ],
        "idPrefixes": []
    }


# ===============================
# Catalog Endpoint
# ===============================
@app.get("/catalog/{type}/{id}.json")
async def catalog(type: str, id: str):
    if id != "pikpak":
        return {"metas": []}

    pk = get_pikpak()
    await pk.login()

    files = await pk.get_file_list(parent_id="")

    metas = []
    for f in files:
        if f.get("file_category") != "VIDEO":
            continue

        metas.append({
            "id": f["id"],  # PikPak file ID
            "type": "movie",
            "name": f["name"],
            "poster": f.get("icon_link") or "https://static.mypikpak.com/39998a187e280e2ee9ceb5f58315a1bcc744fa64",
        })

    return {"metas": metas}


# ===============================
# Stream Endpoint
# ===============================
@app.get("/stream/{type}/{id}.json")
async def stream(type: str, id: str):
    """
    id here is the PikPak file ID coming from catalog
    """

    pk = get_pikpak()
    await pk.login()

    file_id = id

    # 1. Try cache
    cached = get_cached_url(file_id)
    if cached:
        url = cached
    else:
        # 2. Generate from PikPak
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

        # 3. Save to Redis (24 hours)
        set_cached_url(file_id, url)

    streams = [
        {
            "name": "PikPak",
            "title": "Play from PikPak",
            "url": url
        }
    ]

    return JSONResponse({"streams": streams})


# ===============================
# Root
# ===============================
@app.get("/")
async def root():
    return {
        "status": "ok",
        "addon": "PikPak Cloud",
        "cache": "Upstash Redis (24h TTL)"
    }