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
# Redis
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

    print("✅ Session saved")


async def load_session():
    raw = await redis.get("pikpak:session")

    if not raw:
        print("ℹ️ No session found")
        return None

    print("✅ Session restored")
    return PikPakApi.from_dict(json.loads(raw))


async def get_cached_url(file_id: str):
    return await redis.get(f"pikpak:url:{file_id}")


async def set_cached_url(file_id: str, url: str):
    await redis.set(
        f"pikpak:url:{file_id}",
        url,
        ex=URL_CACHE_TTL,
    )

# -----------------------
# Utils
# -----------------------
def normalize(text: str) -> str:
    text = text.lower()

    # remove symbols
    text = re.sub(r"[^a-z0-9 ]", " ", text)

    # collapse spaces
    return re.sub(r"\s+", " ", text).strip()


def get_movie_info(imdb_id: str):

    url = f"https://v3-cinemeta.strem.io/meta/movie/{imdb_id}.json"

    r = requests.get(url, timeout=10)

    meta = r.json().get("meta", {})

    return (
        meta.get("name", ""),
        str(meta.get("year", ""))
    )


def title_match(movie_title: str, filename: str):

    movie_n = normalize(movie_title)
    file_n = normalize(filename)

    title_words = movie_n.split()

    # Count matching words
    match_count = sum(1 for w in title_words if w in file_n)

    # Flexible matching
    required = max(2, len(title_words) // 2)

    return match_count >= required

# -----------------------
# PikPak client manager
# -----------------------
client: PikPakApi | None = None


async def get_client(force_login=False):

    global client

    # already active
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

                print("✅ Session refresh successful")

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

    print("🔐 Full login successful")

    return client


async def with_relogin(fn, *args, **kwargs):

    try:
        return await fn(*args, **kwargs)

    except Exception as e:

        if "401" in str(e).lower():

            print("🔄 Re-login triggered")

            await get_client(force_login=True)

            return await fn(*args, **kwargs)

        raise

# -----------------------
# Recursive file traversal
# -----------------------
async def collect_files(pk, parent_id="", result=None):

    if result is None:
        result = []

    data = await with_relogin(
        pk.file_list,
        parent_id=parent_id
    )

    for f in data.get("files", []):

        if f.get("kind") == "drive#folder":

            await collect_files(
                pk,
                f["id"],
                result
            )

        else:
            result.append(f)

    return result

# -----------------------
# Root
# -----------------------
@app.get("/")
async def root():
    return {
        "status": "ok"
    }

# -----------------------
# Debug
# -----------------------
@app.get("/debug/session")
async def debug_session():

    return {
        "session_exists": bool(
            await redis.get("pikpak:session")
        )
    }

# -----------------------
# Manifest
# -----------------------
@app.get("/manifest.json")
async def manifest():

    return {
        "id": "com.arun.pikpak",
        "version": "2.1.0",
        "name": "PikPak Cloud",

        "types": ["movie"],

        # IMPORTANT
        "resources": [
            "catalog",
            "stream",
            "meta"
        ],

        "catalogs": [
            {
                "type": "movie",
                "id": "pikpak",
                "name": "My PikPak Files"
            }
        ],

        "idPrefixes": [
            "tt",
            "pikpak"
        ]
    }

# -----------------------
# Meta
# -----------------------
@app.get("/meta/{type}/{id}.json")
async def meta(type: str, id: str):

    if type != "movie":
        return {"meta": {}}

    # -----------------------
    # IMDb Discover Items
    # -----------------------
    if id.startswith("tt"):

        movie_title, movie_year = get_movie_info(id)

        return {
            "meta": {
                "id": id,
                "type": "movie",
                "name": movie_title,
                "year": movie_year,
                "poster": "https://upload.wikimedia.org/wikipedia/commons/8/8c/PikPak_logo.png"
            }
        }

    # -----------------------
    # Direct PikPak Items
    # -----------------------
    if id.startswith("pikpak:"):

        file_id = id.replace("pikpak:", "")

        pk = await get_client()

        files = await collect_files(pk)

        for f in files:

            if f.get("id") == file_id:

                return {
                    "meta": {
                        "id": id,
                        "type": "movie",
                        "name": f.get("name"),
                        "poster": "https://upload.wikimedia.org/wikipedia/commons/8/8c/PikPak_logo.png"
                    }
                }

    return {"meta": {}}

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

    return {
        "metas": metas
    }

# -----------------------
# Stream
# -----------------------
@app.get("/stream/{type}/{id}.json")
async def stream(type: str, id: str):

    pk = await get_client()

    # -----------------------
    # Direct PikPak File
    # -----------------------
    if id.startswith("pikpak:"):

        file_id = id.replace("pikpak:", "")

        url = await get_cached_url(file_id)

        if not url:

            data = await with_relogin(
                pk.get_download_url,
                file_id
            )

            links = data.get("links", {})

            if "application/octet-stream" in links:

                url = links[
                    "application/octet-stream"
                ]["url"]

            else:

                medias = data.get("medias", [])

                if medias:
                    url = medias[0]["link"]["url"]

            if not url:
                return {"streams": []}

            await set_cached_url(file_id, url)

        return {
            "streams": [
                {
                    "name": "PikPak",
                    "title": "PikPak Direct",
                    "url": url
                }
            ]
        }

    # -----------------------
    # IMDb Movie Matching
    # -----------------------
    if type != "movie":
        return {"streams": []}

    movie_title, movie_year = get_movie_info(id)

    print(f"🎬 Searching for: {movie_title} ({movie_year})")

    files = await collect_files(pk)

    streams = []

    for f in files:

        name = f.get("name")
        file_id = f.get("id")

        if not name or not file_id:
            continue

        if not name.lower().endswith(VIDEO_EXT):
            continue

        # -----------------------
        # Flexible title match
        # -----------------------
        if not title_match(movie_title, name):
            continue

        # -----------------------
        # Year match
        # -----------------------
        if movie_year and movie_year not in name:
            continue

        print(f"✅ Match found: {name}")

        # -----------------------
        # Cached URL
        # -----------------------
        url = await get_cached_url(file_id)

        if not url:

            data = await with_relogin(
                pk.get_download_url,
                file_id
            )

            links = data.get("links", {})

            if "application/octet-stream" in links:

                url = links[
                    "application/octet-stream"
                ]["url"]

            else:

                medias = data.get("medias", [])

                if medias:
                    url = medias[0]["link"]["url"]

            if not url:
                continue

            await set_cached_url(file_id, url)

        streams.append({
            "name": "PikPak",
            "title": name,
            "url": url
        })

    print(f"🎯 Streams found: {len(streams)}")

    return {
        "streams": streams
    }
