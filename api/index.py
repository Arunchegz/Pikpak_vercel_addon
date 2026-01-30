from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from upstash_redis import Redis
import os, re, time, asyncio, requests, json

from pikpakapi import PikPakApi

app = FastAPI()

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
# Utils (Seedr-style)
# -----------------------
def normalize(text: str):
    return re.sub(r"[^a-z0-9]", "", text.lower())

def extract_title_year(filename: str):
    year_match = re.search(r"(19|20)\d{2}", filename)
    year = year_match.group(0) if year_match else ""

    title = re.sub(r"\.(mkv|mp4|avi|mov|webm|ts).*", "", filename, flags=re.I)
    title = re.sub(r"(19|20)\d{2}", "", title)
    title = title.replace(".", " ").replace("_", " ").strip()

    return title, year

# -----------------------
# PikPak client (safe login)
# -----------------------
client = None

async def get_client():
    global client
    if client:
        return client

    EMAIL = os.environ["PIKPAK_EMAIL"]
    PASSWORD = os.environ["PIKPAK_PASSWORD"]

    client = PikPakApi(EMAIL, PASSWORD)
    await client.login()
    return client

# -----------------------
# Walk files
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
        "id": "com.arun.pikpak.seedrstyle",
        "version": "2.0.0",
        "name": "PikPak Personal Cloud",
        "description": "Seedr-style PikPak addon (Movie pages supported)",
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
    metas = []
    pk = await get_client()

    async for f in walk_files(pk):
        if not f["name"].lower().endswith((".mp4", ".mkv", ".avi", ".mov", ".ts")):
            continue

        title, year = extract_title_year(f["name"])
        if not title:
            continue

        meta_id = normalize(title + year)

        metas.append({
            "id": meta_id,
            "type": "movie",
            "name": title,
            "year": year,
            "description": "From your PikPak account"
        })

    return {"metas": metas}

# -----------------------
# Meta (REQUIRED)
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
# Stream (Seedr-style)
# -----------------------
@app.get("/stream/{type}/{id}.json")
async def stream(type: str, id: str):
    streams = []
    if type != "movie":
        return {"streams": []}

    pk = await get_client()

    async for f in walk_files(pk):
        if not f["name"].lower().endswith((".mp4", ".mkv", ".avi", ".mov", ".ts")):
            continue

        title, year = extract_title_year(f["name"])
        file_id = normalize(title + year)

        if file_id == id:
            data = await pk.get_download_url(f["id"])
            url = (
                data.get("links", {})
                .get("application/octet-stream", {})
                .get("url")
            )

            if url:
                streams.append({
                    "name": "PikPak",
                    "title": f["name"],
                    "url": url
                })

    return {"streams": streams}
