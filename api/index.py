import os
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pikpakapi import PikPakApi
from upstash_redis import Redis

app = FastAPI()

# ==========================
# Upstash Redis
# ==========================
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


# ==========================
# PikPak Client
# ==========================
def get_pikpak():
    email = os.environ.get("PIKPAK_EMAIL")
    password = os.environ.get("PIKPAK_PASSWORD")

    if not email or not password:
        raise Exception("PIKPAK_EMAIL and PIKPAK_PASSWORD must be set")

    # Login happens automatically here
    return PikPakApi(username=email, password=password)


# ==========================
# Manifest
# ==========================
@app.get("/manifest.json")
async def manifest():
    return {
        "id": "com.arun.pikpak",
        "version": "1.3.0",
        "name": "PikPak Cloud",
        "description": "Stream files from your PikPak cloud with caching",
        "types": ["movie"],
        "resources": ["stream", "catalog"],
        "catalogs": [
            {
                "type": "movie",
                "id": "pikpak",
                "name": "My PikPak Files"
            }
        ]
    }


# ==========================
# Catalog
# ==========================
@app.get("/catalog/{type}/{id}.json")
async def catalog(type: str, id: str):
    if id != "pikpak":
        return {"metas": []}

    pk = get_pikpak()
    files = pk.get_file_list(parent_id="")

    metas = []
    for f in files:
        if f.get("file_category") != "VIDEO":
            continue

        metas.append({
            "id": f["id"],
            "type": "movie",
            "name": f["name"],
        })

    return {"metas": metas}


# ==========================
# Stream
# ==========================
@app.get("/stream/{type}/{id}.json")
async def stream(type: str, id: str):
    pk = get_pikpak()
    file_id = id

    # 1. Check cache
    cached = get_cached_url(file_id)
    if cached:
        return {
            "streams": [{
                "name": "PikPak (Cached)",
                "title": "Cached URL",
                "url": cached
            }]
        }

    # 2. Generate fresh link
    data = pk.get_download_url(file_id)

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

    # 3. Cache it for 24h
    set_cached_url(file_id, url)

    return JSONResponse({
        "streams": [
            {
                "name": "PikPak",
                "title": "Fresh URL",
                "url": url
            }
        ]
    })


# ==========================
# Root
# ==========================
@app.get("/")
async def root():
    return {
        "status": "ok",
        "cache": "Upstash Redis enabled",
        "ttl": "24 hours"
    }