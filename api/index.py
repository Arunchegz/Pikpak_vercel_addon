from fastapi import FastAPI
import os

app = FastAPI()

VIDEO_EXT = (".mp4", ".mkv", ".avi", ".mov", ".webm", ".flv", ".ts")

client = None


async def get_client():
    global client

    try:
        from pikpakapi import PikPakApi
    except Exception as e:
        raise Exception(f"Failed to import pikpakapi: {e}")

    EMAIL = os.environ.get("PIKPAK_EMAIL")
    PASSWORD = os.environ.get("PIKPAK_PASSWORD")

    if not EMAIL or not PASSWORD:
        raise Exception("PIKPAK_EMAIL or PIKPAK_PASSWORD is missing")

    if client is None:
        try:
            client = PikPakApi(EMAIL, PASSWORD)
            await client.login()   # <-- IMPORTANT: await
        except Exception as e:
            raise Exception(f"PikPak login failed: {e}")

    return client


@app.get("/")
async def root():
    return {
        "status": "ok",
        "message": "PikPak Stremio addon running",
        "manifest": "/manifest.json"
    }


@app.get("/manifest.json")
async def manifest():
    return {
        "id": "com.arun.pikpak",
        "version": "1.0.0",
        "name": "PikPak Cloud",
        "description": "Stream files from your PikPak cloud",
        "types": ["movie", "series"],
        "resources": ["stream"],
        "idPrefixes": ["tt"]
    }


@app.get("/stream/{type}/{id}.json")
async def stream(type: str, id: str):
    # Step 1: Init client
    try:
        pk = await get_client()
    except Exception as e:
        return {
            "streams": [],
            "error": "Client init failed",
            "detail": str(e)
        }

    # Step 2: List files
    try:
        data = await pk.file_list(parent_id="")   # <-- await
    except Exception as e:
        return {
            "streams": [],
            "error": "file_list failed",
            "detail": str(e)
        }

    files = data.get("files", [])
    streams = []

    # Step 3: Build streams
    for f in files:
        try:
            name = f.get("name", "")
            file_id = f.get("id")

            if not name or not file_id:
                continue

            if not name.lower().endswith(VIDEO_EXT):
                continue

            try:
                url = await pk.get_download_url(file_id)   # <-- await
            except Exception as e:
                print("Download URL failed:", e)
                continue

            streams.append({
                "name": "PikPak",
                "title": name,
                "url": url
            })

        except Exception as e:
            print("File processing error:", e)
            continue

    return {"streams": streams}