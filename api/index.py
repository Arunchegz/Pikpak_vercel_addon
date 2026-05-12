from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

import os
import re
import json
import requests

from urllib.parse import unquote

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

SESSION_TTL = 60 * 60 * 24 * 365
URL_CACHE_TTL = 60 * 60 * 24

PIKPAK_POSTER = (
    "https://upload.wikimedia.org/wikipedia/commons/8/8c/PikPak_logo.png"
)

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
        print("ℹ️ No session")
        return None

    print("✅ Session loaded")

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
def normalize(text: str):

    text = text.lower()

    text = re.sub(r"[^a-z0-9]", "", text)

    return text.strip()


def get_movie_info(imdb_id: str):

    url = f"https://v3-cinemeta.strem.io/meta/movie/{imdb_id}.json"

    r = requests.get(url, timeout=10)

    meta = r.json().get("meta", {})

    return meta.get("name", ""), str(meta.get("year", ""))


def get_series_info(imdb_id: str):

    url = f"https://v3-cinemeta.strem.io/meta/series/{imdb_id}.json"

    r = requests.get(url, timeout=10)

    meta = r.json().get("meta", {})

    return meta.get("name", "")


def extract_title_year(filename: str):

    year_match = re.search(r"(19|20)\d{2}", filename)

    year = year_match.group(0) if year_match else ""

    title = re.sub(
        r"\.(mkv|mp4|avi|mov|webm|wmv|srt).*",
        "",
        filename,
        flags=re.I
    )

    title = re.sub(r"(19|20)\d{2}", "", title)

    title = re.sub(r"S\d{1,2}E\d{1,2}.*", "", title, flags=re.I)

    title = re.sub(r"\d{1,2}x\d{1,2}.*", "", title, flags=re.I)

    title = title.replace(".", " ")

    title = title.replace("_", " ")

    title = title.strip()

    return title, year


def extract_season_episode(filename: str):

    patterns = [
        r"S(\d{1,2})E(\d{1,2})",
        r"(\d{1,2})x(\d{1,2})",
    ]

    for pattern in patterns:

        match = re.search(pattern, filename, re.I)

        if match:
            return int(match.group(1)), int(match.group(2))

    return None, None


def detect_quality(filename):

    filename_lower = filename.lower()

    if "2160" in filename_lower or "4k" in filename_lower:
        return "4K"

    elif "1080" in filename_lower:
        return "1080p"

    elif "720" in filename_lower:
        return "720p"

    elif "480" in filename_lower:
        return "480p"

    return "Auto"

# -----------------------
# PikPak client
# -----------------------
client: PikPakApi | None = None


async def get_client(force_login=False):

    global client

    if client and not force_login:
        return client

    # -----------------------
    # Restore session
    # -----------------------
    if not force_login:

        restored = await load_session()

        if restored:

            try:

                await restored.refresh_access_token()

                client = restored

                await save_session(client)

                print("✅ Session restored")

                return client

            except Exception as e:

                print("⚠️ Restore failed:", e)

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

    print("🔐 Full login")

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

        "version": "3.0.0",

        "name": "PikPak Cloud",

        "description": "Stream PikPak files in Stremio",

        "resources": [
            "catalog",
            "meta",
            "stream"
        ],

        "types": [
            "movie",
            "series"
        ],

        "idPrefixes": [
            "tt",
            "pikpak"
        ],

        "catalogs": [
            {
                "type": "movie",
                "id": "pikpak",
                "name": "☁️ PikPak Movies"
            },
            {
                "type": "series",
                "id": "pikpak_series",
                "name": "☁️ PikPak Series"
            }
        ]
    }

# -----------------------
# Meta
# -----------------------
@app.get("/meta/{type}/{id}.json")
async def meta(type: str, id: str):

    pk = await get_client()

    files = await collect_files(pk)

    for f in files:

        name = f.get("name")

        if not name:
            continue

        if not name.lower().endswith(VIDEO_EXT):
            continue

        # -----------------------
        # SERIES META
        # -----------------------
        if type == "series":

            title, _ = extract_title_year(name)

            if normalize(title) == normalize(id):

                return {
                    "meta": {
                        "id": normalize(title),

                        "type": "series",

                        "name": title,

                        "poster": PIKPAK_POSTER,

                        "posterShape": "poster",

                        "description": title
                    }
                }

        # -----------------------
        # MOVIE META
        # -----------------------
        else:

            fid = f.get("id")

            if f"pikpak:{fid}" == id:

                return {
                    "meta": {
                        "id": id,

                        "type": "movie",

                        "name": name,

                        "poster": PIKPAK_POSTER,

                        "posterShape": "poster",

                        "description": name
                    }
                }

    return {"meta": {}}

# -----------------------
# Catalog
# -----------------------
@app.get("/catalog/{type}/{id}.json")
async def catalog(type: str, id: str):

    pk = await get_client()

    files = await collect_files(pk)

    metas = []

    # -----------------------
    # MOVIE CATALOG
    # -----------------------
    if type == "movie" and id == "pikpak":

        for f in files:

            name = f.get("name")

            fid = f.get("id")

            if not name or not fid:
                continue

            if not name.lower().endswith(VIDEO_EXT):
                continue

            season, episode = extract_season_episode(name)

            # Skip TV episodes
            if season is not None:
                continue

            metas.append({
                "id": f"pikpak:{fid}",

                "type": "movie",

                "name": name,

                "poster": PIKPAK_POSTER
            })

    # -----------------------
    # SERIES CATALOG
    # -----------------------
    elif type == "series" and id == "pikpak_series":

        added = set()

        for f in files:

            name = f.get("name")

            if not name:
                continue

            if not name.lower().endswith(VIDEO_EXT):
                continue

            season, episode = extract_season_episode(name)

            # Only TV episodes
            if season is None:
                continue

            title, _ = extract_title_year(name)

            normalized = normalize(title)

            if normalized in added:
                continue

            added.add(normalized)

            metas.append({
                "id": normalized,

                "type": "series",

                "name": title,

                "poster": PIKPAK_POSTER
            })

    return {"metas": metas}

# -----------------------
# Stream
# -----------------------
@app.get("/stream/{type}/{id}.json")
async def stream(type: str, id: str):

    pk = await get_client()

    files = await collect_files(pk)

    streams = []

    # ---------------------------------------------------
    # SERIES STREAMS
    # ---------------------------------------------------
    if type == "series":

        decoded_id = unquote(id)

        print("DECODED:", decoded_id)

        match = re.match(
            r"(tt\d+):(\d+):(\d+)",
            decoded_id
        )

        if not match:
            return {"streams": []}

        imdb_id = match.group(1)

        target_season = int(match.group(2))

        target_episode = int(match.group(3))

        series_title = get_series_info(imdb_id)

        normalized_series = normalize(series_title)

        print("SERIES:", series_title)

        print("SEASON:", target_season)

        print("EPISODE:", target_episode)

        for f in files:

            name = f.get("name")

            file_id = f.get("id")

            if not name or not file_id:
                continue

            if not name.lower().endswith(VIDEO_EXT):
                continue

            parsed_title, _ = extract_title_year(name)

            season, episode = extract_season_episode(name)

            if season is None:
                continue

            if normalize(parsed_title) != normalized_series:
                continue

            if season != target_season:
                continue

            if episode != target_episode:
                continue

            print("MATCHED:", name)

            url = await get_cached_url(file_id)

            if not url:

                data = await pk.get_download_url(file_id)

                links = data.get("links", {})

                if "application/octet-stream" in links:

                    url = links["application/octet-stream"]["url"]

                else:

                    medias = data.get("medias", [])

                    if medias:
                        url = medias[0]["link"]["url"]

                if not url:
                    continue

                await set_cached_url(file_id, url)

            quality = detect_quality(name)

            streams.append({
                "name": "☁️ PikPak",

                "title": (
                    f"📺 S{season:02d}E{episode:02d}\n"
                    f"⚡ {quality}\n"
                    f"📁 {name}"
                ),

                "url": url
            })

        return {"streams": streams}

    # ---------------------------------------------------
    # DIRECT PIKPAK ID
    # ---------------------------------------------------
    if id.startswith("pikpak:"):

        file_id = id.replace("pikpak:", "")

        url = await get_cached_url(file_id)

        if not url:

            data = await pk.get_download_url(file_id)

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
                "name": "☁️ PikPak",

                "title": "PikPak Direct",

                "url": url
            }]
        }

    # ---------------------------------------------------
    # IMDb MOVIE MATCHING
    # ---------------------------------------------------
    if type != "movie":
        return {"streams": []}

    movie_title, movie_year = get_movie_info(id)

    movie_n = normalize(movie_title)

    for f in files:

        name = f.get("name")

        file_id = f.get("id")

        if not name or not file_id:
            continue

        if not name.lower().endswith(VIDEO_EXT):
            continue

        season, episode = extract_season_episode(name)

        # Skip TV episodes
        if season is not None:
            continue

        file_n = normalize(name)

        if movie_n not in file_n:
            continue

        if movie_year and movie_year not in file_n:
            continue

        url = await get_cached_url(file_id)

        if not url:

            data = await pk.get_download_url(file_id)

            links = data.get("links", {})

            if "application/octet-stream" in links:

                url = links["application/octet-stream"]["url"]

            else:

                medias = data.get("medias", [])

                if medias:
                    url = medias[0]["link"]["url"]

            if not url:
                continue

            await set_cached_url(file_id, url)

        quality = detect_quality(name)

        streams.append({
            "name": "☁️ PikPak",

            "title": (
                f"⚡ {quality}\n"
                f"📁 {name}"
            ),

            "url": url
        })

    return {"streams": streams}
