from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from upstash_redis import Redis
import os, re, requests
from pikpakapi import PikPakApi

app = FastAPI()

# -----------------------
# CORS
# -----------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------
# Redis (optional)
# -----------------------
redis = Redis(
    url=os.environ.get("UPSTASH_REDIS_REST_URL"),
    token=os.environ.get("UPSTASH_REDIS_REST_TOKEN"),
)

# -----------------------
# Constants
# -----------------------
VIDEO_EXT = (".mp4", ".mkv", ".avi", ".mov", ".webm", ".ts")

JUNK_WORDS = [
    "1080p", "720p", "2160p", "4k",
    "hdrip", "webrip", "webdl", "bluray", "brrip",
    "x264", "x265", "h264", "h265", "hevc",
    "aac", "dts", "ddp", "atmos",
    "hindi", "tamil", "telugu", "malayalam",
    "esub", "sub", "subs", "kbps", "mbps"
]

# -----------------------
# Helpers
# -----------------------
def normalize(text: str):
    return re.sub(r"[^a-z0-9]", "", text.lower())


def extract_title_year(filename: str):
    name = filename.lower()

    # remove extension
    name = re.sub(r"\.(mkv|mp4|avi|mov|webm|ts)$", "", name)

    # extract year
    year_match = re.search(r"(19|20)\d{2}", name)
    year = year_match.group(0) if year_match else ""

    # remove year
    name = re.sub(r"(19|20)\d{2}", " ", name)

    # remove junk words
    for word in JUNK_WORDS:
        name = name.replace(word, " ")

    # cleanup
    name = re.sub(r"[^a-z0-9 ]", " ", name)
    name = re.sub(r"\s+", " ", name).strip()

    return name.title(), year


def get_movie_title(imdb_id: str):
    r = requests.get(
        f"https://v3-cinemeta.strem.io/meta/movie/{imdb_id}.json",
        timeout=10
    )
    meta = r.json().get("meta", {})
    return meta.get("name", ""), str(meta.get("year", ""))


# -----------------------
# PikPak client
# -----------------------
client = None

async def get_client():
    global client
    if client:
        return client

    EMAIL = os.environ.get("PIKPAK_EMAIL")
    PASSWORD = os.environ.get("PIKPAK_PASSWORD")

    if not EMAIL or not PASSWORD:
        raise Exception("PIKPAK_EMAIL or PIKPAK_PASSWORD missing")

    client = PikPakApi(EMAIL, PASSWORD)
    await client.login()
    return client


# -----------------------
# Walk PikPak files
# -----------------------
async def walk_files(pk, parent_id=""):
    data = await pk.file_list(parent_id=parent_id)
    for f in data.get("files", []):
        if f.get("kind") == "drive#folder":
            async for sub in walk_files(pk, f["id"]):
                yield sub
        else:
            yield f


# -----------------------
# Manifest
# -----------------------
@app.get("/manifest.json")
def manifest():
    return {
        "id": "com.arun.pikpak.debug",
        "version": "4.1.0",
        "name": "PikPak Personal Cloud (Debug)",
        "description": "Seedr-style PikPak addon with debug endpoints",
        "resources": ["catalog", "stream", "meta"],
        "types": ["movie"],
        "catalogs": [
            {
                "type": "movie",
                "id": "pikpak",
                "name": "My PikPak Movies"
            }
        ]
    }


# -----------------------
# Catalog
# -----------------------
@app.get("/catalog/movie/pikpak.json")
async def catalog():
    pk = await get_client()
    metas = []
    seen = set()

    async for f in walk_files(pk):
        name = f.get("name", "")
        if not name.lower().endswith(VIDEO_EXT):
            continue

        title, year = extract_title_year(name)
        if not title:
            continue

        meta_id = normalize(title + year)
        if meta_id in seen:
            continue

        seen.add(meta_id)

        metas.append({
            "id": meta_id,
            "type": "movie",
            "name": title,
            "year": year,
            "description": "From your PikPak account"
        })

    return {"metas": metas}


# -----------------------
# Meta
# -----------------------
@app.get("/meta/movie/{id}.json")
def meta(id: str):
    return {
        "meta": {
            "id": id,
            "type": "movie",
            "name": id
        }
    }


# -----------------------
# Stream (IMDb bridge)
# -----------------------
@app.get("/stream/{type}/{id}.json")
async def stream(type: str, id: str):
    if type != "movie":
        return {"streams": []}

    pk = await get_client()
    streams = []

    # IMDb → catalog ID
    if id.startswith("tt"):
        title, year = get_movie_title(id)
        id = normalize(title + year)

    async for f in walk_files(pk):
        name = f.get("name", "")
        if not name.lower().endswith(VIDEO_EXT):
            continue

        title, year = extract_title_year(name)
        file_id = normalize(title + year)

        if file_id != id:
            continue

        data = await pk.get_download_url(f["id"])
        url = data.get("links", {}).get("application/octet-stream", {}).get("url")

        if url:
            streams.append({
                "name": "PikPak",
                "title": name,
                "url": url
            })

    return {"streams": streams}


# =====================================================
# 🔍 DEBUG ENDPOINTS (THIS IS THE KEY PART)
# =====================================================

@app.get("/debug/match/{imdb_id}")
async def debug_match(imdb_id: str):
    pk = await get_client()

    title, year = get_movie_title(imdb_id)
    imdb_norm = normalize(title + year)

    result = {
        "imdb": {
            "id": imdb_id,
            "title": title,
            "year": year,
            "normalized_id": imdb_norm
        },
        "files": []
    }

    async for f in walk_files(pk):
        name = f.get("name", "")
        if not name.lower().endswith(VIDEO_EXT):
            continue

        f_title, f_year = extract_title_year(name)
        f_norm = normalize(f_title + f_year)

        result["files"].append({
            "filename": name,
            "extracted_title": f_title,
            "extracted_year": f_year,
            "normalized_id": f_norm,
            "match": f_norm == imdb_norm,
            "file_id": f.get("id")
        })

    return result


@app.get("/debug/url/{file_id}")
async def debug_url(file_id: str):
    pk = await get_client()
    return await pk.get_download_url(file_id)
