from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os, re, json, requests
from upstash_redis.asyncio import Redis
from pikpakapi import PikPakApi

# -----------------------
# App
# -----------------------
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------
# Constants
# -----------------------
VIDEO_EXT = (".mp4", ".mkv", ".avi", ".mov", ".webm", ".flv", ".ts")

SESSION_TTL = 60 * 60 * 24 * 365   # 1 year
URL_CACHE_TTL = 60 * 60 * 12       # 12 hours
IMDB_CACHE_TTL = 60 * 60 * 24 * 7  # 7 days

# -----------------------
# Redis
# -----------------------
redis = Redis(
    url=os.environ["UPSTASH_REDIS_REST_URL"],
    token=os.environ["UPSTASH_REDIS_REST_TOKEN"],
)

# -----------------------
# Utils
# -----------------------
def normalize(text: str) -> str:
    text = text.lower()
    text = re.sub(r"[^a-z0-9 ]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def get_movie_info(imdb_id: str):
    r = requests.get(
        f"https://v3-cinemeta.strem.io/meta/movie/{imdb_id}.json",
        timeout=10,
    )
    meta = r.json().get("meta", {})
    return meta.get("name", ""), str(meta.get("year", ""))

# -----------------------
# Redis helpers
# -----------------------
async def save_session(client: PikPakApi):
    data = client.to_dict()
    data.pop("username", None)
    data.pop("password", None)
    await redis.set("pikpak:session", json.dumps(data), ex=SESSION_TTL)


async def load_session():
    raw = await redis.get("pikpak:session")
    if not raw:
        return None
    return PikPakApi.from_dict(json.loads(raw))


async def get_imdb_cache(imdb_id: str):
    raw = await redis.get(f"pikpak:imdb:{imdb_id}")
    return json.loads(raw) if raw else None


async def set_imdb_cache(imdb_id: str, files: list):
    await redis.set(
        f"pikpak:imdb:{imdb_id}",
        json.dumps(files),
        ex=IMDB_CACHE_TTL,
    )


async def get_cached_url(file_id: str):
    return await redis.get(f"pikpak:url:{file_id}")


async def set_cached_url(file_id: str, url: str):
    await redis.set(f"pikpak:url:{file_id}", url, ex=URL_CACHE_TTL)

# -----------------------
# PikPak client
# -----------------------
client: PikPakApi | None = None


async def get_client():
    global client
    if client:
        return client

    restored = await load_session()
    if restored:
        await restored.refresh_access_token()
        client = restored
        await save_session(client)
        return client

    client = PikPakApi(
        username=os.environ["PIKPAK_EMAIL"],
        password=os.environ["PIKPAK_PASSWORD"],
    )
    await client.login()
    await client.refresh_access_token()
    await save_session(client)
    return client

# -----------------------
# Recursive file listing
# -----------------------
async def collect_files(pk, parent_id="", result=None):
    if result is None:
        result = []

    data = await pk.file_list(parent_id=parent_id)
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

@app.get("/manifest.json")
async def manifest():
    return {
        "id": "com.arun.pikpak",
        "version": "3.0.0",
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
# Stream
# -----------------------
@app.get("/stream/{type}/{id}.json")
async def stream(type: str, id: str):

    pk = await get_client()

    # -----------------------
    # Direct PikPak ID
    # -----------------------
    if id.startswith("pikpak:"):
        file_id = id.replace("pikpak:", "")
        url = await get_cached_url(file_id)
        if not url:
            data = await pk.get_download_url(file_id)
            links = data.get("links", {})
            url = (
                links.get("application/octet-stream", {})
                .get("url")
                or data.get("medias", [{}])[0].get("link", {}).get("url")
            )
            if not url:
                return {"streams": []}
            await set_cached_url(file_id, url)

        return {"streams": [{
            "name": "PikPak",
            "title": "PikPak Direct",
            "url": url
        }]}

    # -----------------------
    # IMDb lookup
    # -----------------------
    if type != "movie":
        return {"streams": []}

    cached = await get_imdb_cache(id)
    files = []

    if cached:
        files = cached
    else:
        title, year = get_movie_info(id)
        title_n = normalize(title)

        all_files = await collect_files(pk)
        for f in all_files:
            name = f.get("name", "")
            if not name.lower().endswith(VIDEO_EXT):
                continue
            n = normalize(name)
            if title_n in n and (not year or year in n):
                files.append({"id": f["id"], "name": name})

        if files:
            await set_imdb_cache(id, files)

    streams = []
    for f in files:
        url = await get_cached_url(f["id"])
        if not url:
            data = await pk.get_download_url(f["id"])
            links = data.get("links", {})
            url = (
                links.get("application/octet-stream", {})
                .get("url")
                or data.get("medias", [{}])[0].get("link", {}).get("url")
            )
            if not url:
                continue
            await set_cached_url(f["id"], url)

        streams.append({
            "name": "PikPak",
            "title": f["name"],
            "url": url
        })

    return {"streams": streams}
