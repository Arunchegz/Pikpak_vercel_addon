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
URL_CACHE_TTL = 60 * 60 * 24   # 24 hours for stream links
FILE_LIST_TTL = 60 * 60        # 1 hour for the file list cache

redis = Redis(
    url=os.environ.get("UPSTASH_REDIS_REST_URL"),
    token=os.environ.get("UPSTASH_REDIS_REST_TOKEN"),
)

# -----------------------
# PikPak Client Manager (With 401 & 429 Resilience)
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
            # Mandatory sleep to avoid "too frequent" on login
            await asyncio.sleep(1)
            await self.client.login()
        return self.client

    async def call(self, func_name, *args, **kwargs):
        pk = await self.get_client()
        func = getattr(pk, func_name)
        try:
            # Gentle pacing: sleep briefly before every call
            await asyncio.sleep(0.5)
            return await func(*args, **kwargs)
        except Exception as e:
            err_msg = str(e)
            if "401" in err_msg or "expired" in err_msg.lower():
                print("Session expired. Re-logging...")
                await pk.login()
                return await func(*args, **kwargs)
            if "too frequent" in err_msg.lower():
                print("Rate limited. Forcing longer sleep...")
                await asyncio.sleep(3)
                return await func(*args, **kwargs)
            raise e

pk_manager = PikPakManager()

# -----------------------
# Utils
# -----------------------
def normalize(text: str) -> str:
    text = text.lower()
    text = re.sub(r"[^a-z0-9 ]", " ", text)
    return re.sub(r"\s+", " ", text).strip()

def get_movie_info(imdb_id: str):
    try:
        r = requests.get(f"https://v3-cinemeta.strem.io/meta/movie/{imdb_id}.json", timeout=5)
        meta = r.json().get("meta", {})
        return meta.get("name", ""), str(meta.get("year", ""))
    except:
        return "", ""

async def get_files_via_search():
    """Instead of crawling, use Search to find all videos at once."""
    cache_key = "pikpak:video_search_cache"
    cached = redis.get(cache_key)
    if cached:
        return json.loads(cached)

    print("Cache miss: Searching PikPak for video files...")
    # Search for all videos (this replaces the recursive crawl)
    # Most PikPak clients support searching by extension or empty keyword
    data = await pk_manager.call("search", keyword=".", limit=500)
    
    all_files = data.get("files", [])
    video_files = [
        f for f in all_files 
        if f.get("name", "").lower().endswith(VIDEO_EXT)
    ]
    
    if video_files:
        redis.set(cache_key, json.dumps(video_files), ex=FILE_LIST_TTL)
    
    return video_files

# -----------------------
# Routes
# -----------------------
@app.get("/manifest.json")
async def manifest():
    return {
        "id": "com.arun.pikpak",
        "version": "1.4.0",
        "name": "PikPak Cloud (Search Mode)",
        "description": "Rate-limit safe PikPak stream addon",
        "types": ["movie"],
        "resources": ["stream", "catalog"],
        "catalogs": [{"type": "movie", "id": "pikpak", "name": "My PikPak Cloud"}],
        "idPrefixes": ["tt", "pikpak"]
    }

@app.get("/catalog/{type}/{id}.json")
async def catalog(type: str, id: str):
    if id != "pikpak": return {"metas": []}
    try:
        files = await get_files_via_search()
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
    
    # Direct selection from Catalog
    if id.startswith("pikpak:"):
        file_id = id.replace("pikpak:", "")
    else:
        # Search match from IMDb
        movie_title, movie_year = get_movie_info(id)
        if not movie_title: return {"streams": []}
        
        target_n = normalize(movie_title)
        all_files = await get_files_via_search()
        
        for f in all_files:
            file_n = normalize(f.get("name", ""))
            if target_n in file_n:
                if not movie_year or movie_year in file_n:
                    file_id = f["id"]
                    break

    if not file_id: return {"streams": []}

    # Fetch/Cache Streaming Link
    cached_url = redis.get(f"url:{file_id}")
    if cached_url:
        url = cached_url
    else:
        data = await pk_manager.call("get_download_url", file_id)
        links = data.get("links", {})
        url = links.get("application/octet-stream", {}).get("url")
        if not url and data.get("medias"):
            url = data.get("medias")[0].get("link", {}).get("url")
        
        if url:
            redis.set(f"url:{file_id}", url, ex=URL_CACHE_TTL)

    return {
        "streams": [{"name": "PikPak 🚀", "title": "Direct Play", "url": url}]
    } if url else {"streams": []}
