from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from upstash_redis import Redis
import os, re, requests, asyncio, time

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
# Redis
# -----------------------
redis = Redis(
    url=os.environ.get("UPSTASH_REDIS_REST_URL"),
    token=os.environ.get("UPSTASH_REDIS_REST_TOKEN"),
)

# -----------------------
# Helpers (Seedr-style)
# -----------------------
VIDEO_EXT = (".mp4", ".mkv", ".avi", ".mov", ".webm", ".ts")

def normalize(text: str):
    return re.sub(r"[^a-z0-9]", "", text.lower())

def extract_title_year(filename: str):
    year_match = re.search(r"(19|20)\d{2}", filename)
    year = year_match.group(0) if year_match else ""

    title = re.sub(r"\.(mkv|mp4|avi|mov|webm|ts).*", "", filename, flags=re.I)
    title = re.sub(r"(19|20)\d{2}", "", title)
    title = title.replace(".", " ").replace("_", " ").strip()

    return title, year

def get_movie_title(imdb_id: str):
    r = requests.get(
        f"https://v3-cinemeta.strem.io/meta/movie/{imdb_id}.json",
        timeout=10
    )
    meta = r.json().get("meta", {})
    return meta.get("name", ""), str(meta.get("year", ""))

# -----------------------
# PikPak client (safe reuse)
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
# Root
# -----------------------
@app.get("/")
def root():
    return {"status": "ok", "message": "PikPak Seedr-style addon running"}

# -----------------------
# Manifest
# -----------------------
@app.get("/manifest.json")
def manifest():
    return {
        "id": "com.arun.pikpak.seedrstyle",
        "version": "3.0.0",
        "name": "PikPak Personal Cloud",
        "description": "Seedr-style PikPak addon with IMDb support",
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
# Catalog (Seedr-style IDs)
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
# Meta (REQUIRED for custom IDs)
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
# Stream (Seedr + IMDb bridge)
# -----------------------
@app.get("/stream/{type}/{id}.json")
async def stream(type: str, id: str):
    if type != "movie":
        return {"streams": []}

    pk = await get_client()
    streams = []

    # 🔥 IMDb → catalog ID bridge
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
        url = (
            data.get("links", {})
            .get("application/octet-stream", {})
            .get("url")
        )

        if url:
            streams.append({
                "name": "PikPak",
                "title": name,
                "url": url
            })

    return {"streams": streams}
