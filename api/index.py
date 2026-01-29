from fastapi import FastAPI
import os
from pikpakapi import PikPakApi

app = FastAPI()

# Read credentials from Vercel Environment Variables
EMAIL = os.environ.get("PIKPAK_EMAIL")
PASSWORD = os.environ.get("PIKPAK_PASSWORD")

# Global client (Vercel may reuse the container)
client = None

# Supported video extensions
VIDEO_EXT = (".mp4", ".mkv", ".avi", ".mov", ".webm", ".flv", ".ts")


def get_client():
    """
    Create and login PikPak client once.
    Reused across requests if the Vercel instance is warm.
    """
    global client
    if client is None:
        if not EMAIL or not PASSWORD:
            raise Exception("PIKPAK_EMAIL or PIKPAK_PASSWORD environment variable not set")

        client = PikPakApi(EMAIL, PASSWORD)
        client.login()
    return client


@app.get("/")
def root():
    return {
        "status": "ok",
        "message": "PikPak Stremio addon running",
        "manifest": "/manifest.json"
    }


@app.get("/manifest.json")
def manifest():
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
def stream(type: str, id: str):
    # Step 1: Login / get client
    try:
        pk = get_client()
    except Exception as e:
        return {
            "streams": [],
            "error": "Login failed",
            "detail": str(e)
        }

    # Step 2: List root files
    try:
        data = pk.file_list(parent_id="")
    except Exception as e:
        return {
            "streams": [],
            "error": "file_list failed",
            "detail": str(e)
        }

    files = data.get("files", [])
    streams = []

    # Step 3: Build Stremio streams
    for f in files:
        try:
            name = f.get("name", "")
            file_id = f.get("id")

            if not name or not file_id:
                continue

            lname = name.lower()
            if not lname.endswith(VIDEO_EXT):
                continue

            # Step 4: Get direct download link
            try:
                url = pk.get_download_url(file_id)
            except Exception as e:
                print("Download URL error for", name, ":", e)
                continue

            streams.append({
                "name": "PikPak",
                "title": name,
                "url": url
            })

        except Exception as e:
            # Never let a single broken file crash the addon
            print("File processing error:", f, "Error:", e)
            continue

    return {
        "streams": streams
    }