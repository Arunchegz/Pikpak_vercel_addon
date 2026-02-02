from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os
import re
import json
import requests
from upstash_redis.asyncio import Redis
from pikpakapi import PikPakApi

# -----------------------
# App
# -----------------------
app = FastAPI()

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
SESSION_TTL = 60 * 60 * 24 * 365   # 1 year
URL_CACHE_TTL = 60 * 60 * 24       # 24h

# -----------------------
# Redis (ASYNC)
# -----------------------
redis = Redis(
    url=os.environ["UPSTASH_REDIS_REST_URL"],
    token=os.environ["UPSTASH_REDIS_REST_TOKEN"],
)

# -----------------------
# Redis helpers
# -----------------------
async def save_session(client: PikPakApi):
    data = client.to_dict()
    await redis.set(
        "pikpak:session",
        json.dumps(data),
        ex=SESSION_TTL,
    )
    print("✅ Session saved to Redis")


async def load_session():
    raw = await redis.get("pikpak:session")
    if not raw:
        print("ℹ️ No session in Redis")
        return None
    print("✅ Session loaded from Redis")
    return PikPakApi.from_dict(json.loads(raw))


async def get_cached_url(file_id: str):
    return await redis.get(f"pikpak:url:{file_id}")


async def set_cached_url(file_id: str, url: str):
    await redis.set(f"pikpak:url:{file_id}", url, ex=URL_CACHE_TTL)

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
    meta = r.json().get("meta", {})
    return meta.get("name", ""), str(meta.get("year", ""))

# -----------------------
# PikPak client manager
# -----------------------
client: PikPakApi | None = None


async def get_client(force_login=False):
    global client

    if client and not force_login:
        return client

    # -----------------------
    # Try restore session
    # -----------------------
    if not force_login:
        restored = await load_session()
        if restored:
            try:
                await restored.refresh_access_token()
                client = restored
                await save_session(client)
                print("✅ PikPak session restored")
                return client
            except Exception as e:
                print("⚠️ Session restore failed:", e)

    # -----------------------
    # Full login
    # -----------------------
    client = PikPakApi(
        username=os.environ["PIKPAK_EMAIL"],
        password=os.environ["PIKPAK_PASSWORD"],
    )

    await client.login()
    await client.refresh_access_token()
    await save_session(client)

    print("🔐 Full login completed")
    return client


async def with_relogin(fn, *args, **kwargs):
    try:
        return await fn(*args, **kwargs)
    except Exception as e:
        if "401" in str(e).lower():
            await get_client(force_login=True)
            return await fn(*args, **kwargs)
        raise

# -----------------------
# Recursive file traversal
# -----------------------
async def collect_files(pk, parent_id="", result=None):
    if result is None:
        result = []

    data = await with_relogin(pk.file_list, parent_id=parent_id)
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
    return {"status": "ok"}

# -----------------------
# Debug
# -----------------------
@app.get("/debug/session")
async def debug_session():
    return {
        "session_exists": bool(await redis.get("pikpak:session"))
    }

# -----------------------
# Manifest
# -----------------------
@app.get("/manifest.json")
async def manifest():
    return {
        "id": "com.arun.pikpak",
        "version": "2.0.0",
        "name": "PikPak Cloud",
        "types": ["movie"],
        "resources": ["catalog", "stream"],
        "catalogs": [{
            "type": "movie",
            "id": "pikpak",
            "name": "My PikPak Files"
        }],
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
    files = await collect_files(pk)

    metas = []
    for f in files:
        name = f.get("name")
        fid = f.get("id")
        if name and fid and name.lower().endswith(VIDEO_EXT):
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
    if not id.startswith("pikpak:"):
        return {"streams": []}

    file_id = id.replace("pikpak:", "")
    pk = await get_client()

    url = await get_cached_url(file_id)
    if not url:
        data = await with_relogin(pk.get_download_url, file_id)
        links = data.get("links", {})
        if "application/octet-stream" in links:
            url = links["application/octet-stream"]["url"]
        else:
            medias = data.get("medias", [])
            if medias:
                url = medias[0]["link"]["url"]

        if not url:
            return {"streams": []}

        await set_cached_url(file_id, url)

    return {
        "streams": [{
            "name": "PikPak",
            "title": "PikPak Direct",
            "url": url
        }]
    }
