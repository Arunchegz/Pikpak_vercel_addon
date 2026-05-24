from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os
import re
import json
import requests
from urllib.parse import unquote

from upstash_redis.asyncio import Redis
from pikpakapi import PikPakApi

# ---------------------------------------------------
# App
# ---------------------------------------------------

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------
# Constants
# ---------------------------------------------------

VIDEO_EXT = (
    ".mp4",
    ".mkv",
    ".avi",
    ".mov",
    ".webm",
    ".flv",
    ".ts"
)

SESSION_TTL = 60 * 60 * 24 * 365
URL_CACHE_TTL = 60 * 60 * 24

# ---------------------------------------------------
# Redis
# ---------------------------------------------------

redis = Redis(
    url=os.environ["UPSTASH_REDIS_REST_URL"],
    token=os.environ["UPSTASH_REDIS_REST_TOKEN"],
)

# ---------------------------------------------------
# Redis helpers
# ---------------------------------------------------

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

    return PikPakApi.from_dict(
        json.loads(raw)
    )


async def get_cached_url(file_id: str):

    return await redis.get(
        f"pikpak:url:{file_id}"
    )


async def set_cached_url(
    file_id: str,
    url: str
):

    await redis.set(
        f"pikpak:url:{file_id}",
        url,
        ex=URL_CACHE_TTL,
    )

# ---------------------------------------------------
# Helpers
# ---------------------------------------------------

def normalize(text: str):

    text = text.lower()

    text = re.sub(
        r"[^a-z0-9 ]",
        " ",
        text
    )

    return re.sub(
        r"\s+",
        " ",
        text
    ).strip()


def flexible_match(
    title: str,
    filename: str
):

    title_n = normalize(title)

    file_n = normalize(filename)

    words = title_n.split()

    matched = sum(
        1 for w in words
        if w in file_n
    )

    required = max(
        2,
        len(words) // 2
    )

    return matched >= required


def extract_title_year(filename: str):

    year_match = re.search(
        r"(19|20)\d{2}",
        filename
    )

    year = (
        year_match.group(0)
        if year_match else ""
    )

    title = re.sub(
        r"\.(mkv|mp4|avi|mov|webm|wmv|srt).*",
        "",
        filename,
        flags=re.I
    )

    title = re.sub(
        r"(19|20)\d{2}",
        "",
        title
    )

    title = re.sub(
        r"S\d{1,2}E\d{1,2}.*",
        "",
        title,
        flags=re.I
    )

    title = re.sub(
        r"\d{1,2}x\d{1,2}.*",
        "",
        title,
        flags=re.I
    )

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

        match = re.search(
            pattern,
            filename,
            re.I
        )

        if match:
            return (
                int(match.group(1)),
                int(match.group(2))
            )

    return None, None


def get_cinemeta(
    type_name: str,
    imdb_id: str
):

    url = (
        "https://v3-cinemeta.strem.io/"
        f"meta/{type_name}/{imdb_id}.json"
    )

    r = requests.get(
        url,
        timeout=10
    )

    meta = r.json().get("meta", {})

    return (
        meta.get("name", ""),
        str(meta.get("year", ""))
    )

# ---------------------------------------------------
# PikPak Client
# ---------------------------------------------------

client: PikPakApi | None = None


async def get_client(force_login=False):

    global client

    if client and not force_login:
        return client

    # Restore session
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

                print(
                    "⚠️ Session restore failed:",
                    e
                )

    # Full login
    client = PikPakApi(
        username=os.environ["PIKPAK_EMAIL"],
        password=os.environ["PIKPAK_PASSWORD"],
    )

    await client.login()

    await client.refresh_access_token()

    await save_session(client)

    print("🔐 Full login successful")

    return client


async def with_relogin(
    fn,
    *args,
    **kwargs
):

    try:

        return await fn(
            *args,
            **kwargs
        )

    except Exception as e:

        if "401" in str(e).lower():

            print("🔄 Re-login triggered")

            await get_client(
                force_login=True
            )

            return await fn(
                *args,
                **kwargs
            )

        raise

# ---------------------------------------------------
# Recursive traversal
# ---------------------------------------------------

async def collect_files(
    pk,
    parent_id="",
    result=None
):

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

# ---------------------------------------------------
# Root
# ---------------------------------------------------

@app.get("/")
async def root():

    return {
        "status": "ok"
    }

# ---------------------------------------------------
# Manifest
# ---------------------------------------------------

@app.get("/manifest.json")
async def manifest():

    return {
        "id": "com.arun.pikpak",
        "version": "3.1.0",

        "name": "PikPak Cloud",

        "description": (
            "Movies & Series from PikPak"
        ),

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
            "pikpak",
            "pikpakseries"
        ],

        "catalogs": [
            {
                "type": "movie",
                "id": "pikpak_movies",
                "name": "🎬 PikPak Movies"
            },
            {
                "type": "series",
                "id": "pikpak_series",
                "name": "📺 PikPak Series"
            }
        ]
    }

# ---------------------------------------------------
# Movie Catalog
# ---------------------------------------------------

@app.get("/catalog/movie/pikpak_movies.json")
async def movie_catalog():

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

        season, episode = extract_season_episode(
            name
        )

        # Skip TV episodes
        if season is not None:
            continue

        metas.append({
            "id": f"pikpak:{fid}",

            "type": "movie",

            "name": name,

            "poster": (
                "https://upload.wikimedia.org/"
                "wikipedia/commons/8/8c/"
                "PikPak_logo.png"
            )
        })

    return {
        "metas": metas
    }

# ---------------------------------------------------
# Series Catalog
# ---------------------------------------------------

@app.get("/catalog/series/pikpak_series.json")
async def series_catalog():

    pk = await get_client()

    files = await collect_files(pk)

    metas = []

    added = set()

    for f in files:

        name = f.get("name")

        if not name:
            continue

        if not name.lower().endswith(VIDEO_EXT):
            continue

        season, episode = extract_season_episode(
            name
        )

        # Only TV episodes
        if season is None:
            continue

        title, _ = extract_title_year(name)

        # Fallback
        if not title:

            title = name.split(".S")[0]

            title = title.replace(".", " ")

        normalized = normalize(title)

        if normalized in added:
            continue

        added.add(normalized)

        print("📺 SERIES:", title)

        metas.append({
            "id": (
                f"pikpakseries:{normalized}"
            ),

            "type": "series",

            "name": title,

            "poster": (
                "https://upload.wikimedia.org/"
                "wikipedia/commons/8/8c/"
                "PikPak_logo.png"
            ),

            "posterShape": "poster",

            "description": title
        })

    print("TOTAL SERIES:", len(metas))

    return {
        "metas": metas
    }

# ---------------------------------------------------
# Meta
# ---------------------------------------------------

@app.get("/meta/{type}/{id}.json")
async def meta(type: str, id: str):

    print("META:", type, id)

    # ---------------------------------------------------
    # IMDb Discover
    # ---------------------------------------------------

    if id.startswith("tt"):

        title, year = get_cinemeta(
            type,
            id
        )

        return {
            "meta": {
                "id": id,

                "type": type,

                "name": title,

                "year": year,

                "poster": (
                    "https://upload.wikimedia.org/"
                    "wikipedia/commons/8/8c/"
                    "PikPak_logo.png"
                )
            }
        }

    pk = await get_client()

    files = await collect_files(pk)

    # ---------------------------------------------------
    # SERIES META
    # ---------------------------------------------------

    if type == "series":

        clean_id = id.replace(
            "pikpakseries:",
            ""
        )

        videos = []

        added = set()

        series_name = None

        for f in files:

            name = f.get("name")

            if not name:
                continue

            if not name.lower().endswith(VIDEO_EXT):
                continue

            parsed_title, _ = extract_title_year(
                name
            )

            if not parsed_title:

                parsed_title = name.split(".S")[0]

                parsed_title = parsed_title.replace(
                    ".",
                    " "
                )

            normalized = normalize(
                parsed_title
            )

            if normalized != normalize(clean_id):
                continue

            season, episode = extract_season_episode(
                name
            )

            if season is None:
                continue

            key = (
                f"{season}-{episode}"
            )

            if key in added:
                continue

            added.add(key)

            series_name = parsed_title

            videos.append({
                "id": (
                    f"pikpakseries:"
                    f"{normalized}:"
                    f"{season}:"
                    f"{episode}"
                ),

                "title": (
                    f"S{season:02d}"
                    f"E{episode:02d}"
                ),

                "season": season,

                "episode": episode,

                "released": (
                    "2024-01-01T00:00:00.000Z"
                )
            })

        videos.sort(
            key=lambda x: (
                x["season"],
                x["episode"]
            )
        )

        return {
            "meta": {
                "id": (
                    f"pikpakseries:"
                    f"{clean_id}"
                ),

                "type": "series",

                "name": series_name,

                "poster": (
                    "https://upload.wikimedia.org/"
                    "wikipedia/commons/8/8c/"
                    "PikPak_logo.png"
                ),

                "posterShape": "poster",

                "description": series_name,

                "videos": videos
            }
        }

    # ---------------------------------------------------
    # MOVIE META
    # ---------------------------------------------------

    if id.startswith("pikpak:"):

        file_id = id.replace(
            "pikpak:",
            ""
        )

        for f in files:

            if f.get("id") == file_id:

                return {
                    "meta": {
                        "id": id,

                        "type": "movie",

                        "name": f.get("name"),

                        "poster": (
                            "https://upload.wikimedia.org/"
                            "wikipedia/commons/8/8c/"
                            "PikPak_logo.png"
                        )
                    }
                }

    return {
        "meta": {}
    }

# ---------------------------------------------------
# Stream
# ---------------------------------------------------

@app.get("/stream/{type}/{id}.json")
async def stream(type: str, id: str):

    pk = await get_client()

    streams = []

# ---------------------------------------------------
# Stream
# ---------------------------------------------------

@app.get("/stream/{type}/{id}.json")
async def stream(type: str, id: str):

    pk = await get_client()

    streams = []

    # ---------------------------------------------------
    # SERIES STREAM
    # ---------------------------------------------------

    if type == "series":

        decoded_id = unquote(id)

        print(
            "SERIES STREAM:",
            decoded_id
        )

        match = re.match(
            r"(pikpakseries:[^:]+):(\d+):(\d+)",
            decoded_id
        )

        if not match:
            return {"streams": []}

        series_id = match.group(1)

        target_season = int(
            match.group(2)
        )

        target_episode = int(
            match.group(3)
        )

        series_title = series_id.replace(
            "pikpakseries:",
            ""
        )

        files = await collect_files(pk)

        for f in files:

            name = f.get("name")
            file_id = f.get("id")

            if not name or not file_id:
                continue

            if not name.lower().endswith(VIDEO_EXT):
                continue

            parsed_title, _ = extract_title_year(
                name
            )

            season, episode = extract_season_episode(
                name
            )

            if season is None:
                continue

            if not flexible_match(
                series_title,
                parsed_title
            ):
                continue

            if season != target_season:
                continue

            if episode != target_episode:
                continue

            print(
                "✅ SERIES MATCH:",
                name
            )

            url = await get_cached_url(
                file_id
            )

            if not url:

                data = await with_relogin(
                    pk.get_download_url,
                    file_id
                )

                links = data.get(
                    "links",
                    {}
                )

                if (
                    "application/octet-stream"
                    in links
                ):

                    url = links[
                        "application/octet-stream"
                    ]["url"]

                else:

                    medias = data.get(
                        "medias",
                        []
                    )

                    if medias:

                        url = medias[0][
                            "link"
                        ]["url"]

                if not url:
                    continue

                await set_cached_url(
                    file_id,
                    url
                )

            streams.append({
                "name": "PikPak",
                "title": (
                    f"S{season:02d}E{episode:02d}\n{name}"
                ),
                "url": url
            })

        return {
            "streams": streams
        }

    # ---------------------------------------------------
    # DIRECT PIKPAK MOVIE
    # ---------------------------------------------------

    if id.startswith("pikpak:"):

        file_id = id.replace(
            "pikpak:",
            ""
        )

        url = await get_cached_url(
            file_id
        )

        if not url:

            data = await with_relogin(
                pk.get_download_url,
                file_id
            )

            links = data.get(
                "links",
                {}
            )

            if (
                "application/octet-stream"
                in links
            ):

                url = links[
                    "application/octet-stream"
                ]["url"]

            else:

                medias = data.get(
                    "medias",
                    []
                )

                if medias:

                    url = medias[0][
                        "link"
                    ]["url"]

            if not url:
                return {"streams": []}

            await set_cached_url(
                file_id,
                url
            )

        return {
            "streams": [
                {
                    "name": "PikPak",
                    "title": "PikPak Direct",
                    "url": url
                }
            ]
        }

    # ---------------------------------------------------
    # IMDb MOVIE MATCHING
    # ---------------------------------------------------

    if type != "movie":
        return {"streams": []}

    movie_title, movie_year = get_cinemeta(
        "movie",
        id
    )

    print(
        f"\n🎬 Searching for:"
        f" {movie_title} ({movie_year})"
    )

    files = await collect_files(pk)

    for f in files:

        name = f.get("name")
        file_id = f.get("id")

        if not name or not file_id:
            continue

        if not name.lower().endswith(VIDEO_EXT):
            continue

        season, episode = extract_season_episode(
            name
        )

        # Skip TV episodes
        if season is not None:
            continue

        parsed_title, parsed_year = (
            extract_title_year(name)
        )

        print(
            f"\nChecking:"
            f" {parsed_title}"
            f" ({parsed_year})"
        )

        # Flexible title match
        title_match = flexible_match(
            movie_title,
            parsed_title
        )

        # Flexible year match
        year_match = (
            not movie_year
            or not parsed_year
            or movie_year == parsed_year
        )

        if not title_match:

            print(
                "❌ Title mismatch"
            )

            continue

        if not year_match:

            print(
                "❌ Year mismatch"
            )

            continue

        print(
            f"✅ MATCH FOUND: {name}"
        )

        url = await get_cached_url(
            file_id
        )

        if not url:

            data = await with_relogin(
                pk.get_download_url,
                file_id
            )

            links = data.get(
                "links",
                {}
            )

            if (
                "application/octet-stream"
                in links
            ):

                url = links[
                    "application/octet-stream"
                ]["url"]

            else:

                medias = data.get(
                    "medias",
                    []
                )

                if medias:

                    url = medias[0][
                        "link"
                    ]["url"]

            if not url:
                continue

            await set_cached_url(
                file_id,
                url
            )

        streams.append({
            "name": "⚡ [PP]",
            "title": (
                f"{name}"
            ),
            "url": url
        })

    print(
        f"\n🎯 Streams found:"
        f" {len(streams)}"
    )

    return {
        "streams": streams
    }
    }
