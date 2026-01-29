import os
import re
import asyncio
import json
import requests
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from upstash_redis import Redis

app = FastAPI()

# -----------------------
# CORS & Configuration
# -----------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

VIDEO_EXT = (".mp4", ".mkv", ".avi", ".mov", ".webm", ".flv", ".ts")
URL_CACHE_TTL = 60 * 60 * 24  # 24 hours for streaming links
FILE_LIST_TTL = 60 * 30      # 30 minutes for the full drive crawl

redis = Redis(
    url=os.environ.get("UPSTASH_REDIS_REST_URL"),
    token=os.environ.get("UPSTASH_REDIS_REST_TOKEN"),
)

# -----------------------
# PikPak Client Singleton
# -----------------------
class PikPakManager:
    def __init__(self):
        self.client = None
        self.email = os.environ.get("PIKPAK_EMAIL")
        self.password = os.environ.get("PIKPAK_PASSWORD")

    async def get_client(self):
        from pikpakapi import PikPakApi
        if self.client is None:
            self.client = PikPakApi(self.email, self.password)
            await self.client.login()
        return self.client

    async def call(self, func_name, *args, **kwargs):
        """Wrapper to handle 401s and auto-retry after login"""
        pk = await self.get_client()
        func = getattr(pk, func_name)
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            if "401" in str(e):
                print("Session expired. Re-logging in...")
                await pk.login()
                return await func(*args, **kwargs)
            raise e

pk_manager = PikPakManager()

# -----------------------
# Utils & Caching Logic
# -----------------------
def normalize(text: str) -> str:
    text = text.lower()
    text = re.sub(r"[^a-z0-9 ]", " ", text)
    return re.sub(r"\s+", " ", text).strip()

async def get_all_files_cached():
    """Caches the entire drive structure to avoid constant API hitting"""
    cache_key = "pikpak:all_files_list"
    cached = redis.get(cache_key)
    if cached:
        return json.loads(cached)

    print("Cache miss: Crawling PikPak drive...")
    files = await collect_files_recursive()
    redis.set(cache_key, json.dumps(files), ex=FILE_LIST_TTL)
    return files

async def collect_files_recursive(parent_id="", result=None):
    if result is None: result = []
    # Using the manager wrapper to handle 401s
    data = await pk_manager.call("file_list", parent_id=parent_id)
    files = data.get("files", [])
    for f in files:
        if f.get("kind") == "drive#folder":
            await collect_files_recursive(f["id"], result)
        elif f.get("name", "").lower().endswith(VIDEO_EXT):
            result.append(f)
    return result

# -----------------------
# Routes
# -----------------------
@app.get("/manifest.json")
async def manifest():
    return {
        "id": "com.arun.pikpak",
        "version": "1.3.0",
        "name": "PikPak Cloud (Optimized)",
        "description": "Stream from PikPak with auto-login and Redis caching",
        "types": ["movie"],
        "resources": ["stream", "catalog"],
        "catalogs": [{"type": "movie", "id": "pikpak", "name": "My PikPak Files"}],
        "idPrefixes": ["tt", "pikpak"]
    }

@app.get("/catalog/{type}/{id}.json")
async def catalog(type: str, id: str):
    if type != "movie" or id != "pikpak":
        return {"metas": []}
    
    try:
        files = await get_all_files_cached()
        metas = [{
            "id": f"pikpak:{f['id']}",
            "type": "movie",
            "name": f.get("name"),
            "poster": "https://upload.wikimedia.org/wikipedia/commons/8/8c/PikPak_logo.png"
        } for f in files]
        return {"metas": metas}
    except Exception as e:
        return {"metas": [], "error": str(e)}

@app.get("/stream/{type}/{id}.json")
async def stream(type: str, id: str):
    file_id = None
    
    # 1. Resolve File ID
    if id.startswith("pikpak:"):
        file_id = id.replace("pikpak:", "")
    else:
        # IMDb matching logic
        target_title, target_year = get_movie_info(id) # Assuming your existing helper
        target_n = normalize(target_title)
        all_files = await get_all_files_cached()
        for f in all_files:
            name_n = normalize(f['name'])
            if target_n in name_n and (not target_year or target_year in name_n):
                file_id = f['id']
                break

    if not file_id:
        return {"streams": []}

    # 2. Get/Cache Download URL
    url = redis.get(f"url:{file_id}")
    if not url:
        data = await pk_manager.call("get_download_url", file_id)
        links = data.get("links", {})
        # Pick best available link
        url = links.get("application/octet-stream", {}).get("url") or \
              (data.get("medias")[0].get("link", {}).get("url") if data.get("medias") else None)
        
        if url:
            redis.set(f"url:{file_id}", url, ex=URL_CACHE_TTL)

    return {
        "streams": [{"name": "PikPak", "title": "Direct Stream", "url": url}]
    } if url else {"streams": []}

def get_movie_info(imdb_id: str):
    r = requests.get(f"https://v3-cinemeta.strem.io/meta/movie/{imdb_id}.json", timeout=5)
    meta = r.json().get("meta", {})
    return meta.get("name", ""), str(meta.get("year", ""))
