from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os, re, json, time, hashlib, logging, requests
from upstash_redis import Redis
from pikpakapi import PikPakApi

# -----------------------
# Logging
# -----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

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
AUTH_TTL = 60 * 60 * 24 * 365
REFRESH_EARLY = 10 * 60  # 10 minutes

# -----------------------
# Redis
# -----------------------
redis = Redis(
    url=os.environ["UPSTASH_REDIS_REST_URL"],
    token=os.environ["UPSTASH_REDIS_REST_TOKEN"],
)

# -----------------------
# Device ID (Go-style)
# -----------------------
def get_device_id(seed: str) -> str:
    h = hashlib.sha256(seed.encode()).digest()[:16]
    h = bytearray(h)
    h[6] = (h[6] & 0x0F) | 0x40
    h[8] = (h[8] & 0x3F) | 0x80
    return "".join(f"{b:02x}" for b in h)

def auth_key(device_id: str) -> str:
    return f"pikpak:auth:{device_id}"

# -----------------------
# Auth helpers
# -----------------------
def load_auth(device_id: str):
    raw = redis.get(auth_key(device_id))
    return json.loads(raw) if raw else None

def save_auth(device_id: str, auth: dict):
    redis.set(auth_key(device_id), json.dumps(auth), ex=AUTH_TTL)
    logging.info("[AUTH] saved auth to redis")

def is_expiring(auth: dict) -> bool:
    return auth["expires_at"] <= int(time.time()) + REFRESH_EARLY

# -----------------------
# PikPak client
# -----------------------
async def get_client():
    EMAIL = os.environ["PIKPAK_EMAIL"]
    PASSWORD = os.environ["PIKPAK_PASSWORD"]

    device_id = get_device_id(EMAIL)
    pk = PikPakApi(EMAIL, PASSWORD)

    auth = load_auth(device_id)

    # ---------- Restore from Redis ----------
    if auth:
        logging.info("[AUTH] loaded from redis")

        pk.access_token = auth["access_token"]
        pk.refresh_token = auth["refresh_token"]

        # Proactive refresh (Go-style)
        if is_expiring(auth):
            try:
                await pk.refresh_access_token()
                auth = {
                    "access_token": pk.access_token,
                    "refresh_token": pk.refresh_token,
                    "expires_at": int(time.time()) + 3600,
                }
                save_auth(device_id, auth)
                logging.info("[AUTH] refreshed token")
                return pk
            except Exception as e:
                logging.warning(f"[AUTH] refresh failed: {e}")

        # Validate token
        try:
            await pk.user_info()
            save_auth(device_id, auth)  # refresh TTL
            logging.info("[AUTH] access token valid")
            return pk
        except Exception:
            logging.warning("[AUTH] access token invalid")

    # ---------- Full login ----------
    logging.warning("[AUTH] full login triggered")
    await pk.login()
    auth = {
        "access_token": pk.access_token,
        "refresh_token": pk.refresh_token,
        "expires_at": int(time.time()) + 3600,
    }
    save_auth(device_id, auth)
    return pk

# -----------------------
# Utils
# -----------------------
def normalize(text: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]", " ", text.lower())).strip()

# -----------------------
# Routes
# -----------------------
@app.get("/")
async def root():
    return {"status": "ok", "manifest": "/manifest.json"}

@app.get("/manifest.json")
async def manifest():
    return {
        "id": "com.arun.pikpak",
        "version": "2.0.2",
        "name": "PikPak Cloud",
        "types": ["movie"],
        "resources": ["catalog", "stream"],
        "catalogs": [{
            "type": "movie",
            "id": "pikpak",
            "name": "My PikPak Files",
        }],
        "idPrefixes": ["tt", "pikpak"],
    }

# -----------------------
# Catalog
# -----------------------
@app.get("/catalog/movie/pikpak.json")
async def catalog():
    pk = await get_client()

    # ✅ IMPORTANT: must pass parent_id
    data = await pk.file_list(parent_id="")

    metas = []
    for f in data.get("files", []):
        if f.get("name", "").lower().endswith(VIDEO_EXT):
            metas.append({
                "id": f"pikpak:{f['id']}",
                "type": "movie",
                "name": f["name"],
            })

    return {"metas": metas}

# -----------------------
# Stream
# -----------------------
@app.get("/stream/movie/{id}.json")
async def stream(id: str):
    if not id.startswith("pikpak:"):
        return {"streams": []}

    pk = await get_client()
    file_id = id.replace("pikpak:", "")
    data = await pk.get_download_url(file_id)

    url = (
        data.get("links", {})
        .get("application/octet-stream", {})
        .get("url")
        or data["medias"][0]["link"]["url"]
    )

    return {
        "streams": [{
            "name": "PikPak",
            "title": "Direct",
            "url": url,
        }]
    }