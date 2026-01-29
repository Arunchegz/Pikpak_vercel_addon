from fastapi import FastAPI
import os
from pikpakapi import PikPak   # based on Quan666/PikPakAPI

app = FastAPI()

# Environment variables
EMAIL = os.environ.get("PIKPAK_EMAIL")
PASSWORD = os.environ.get("PIKPAK_PASSWORD")

# Create client (lazy login is better for Vercel)
client = None

VIDEO_EXT = (".mp4", ".mkv", ".avi", ".mov", ".webm")

def get_client():
    global client
    if client is None:
        client = PikPak(EMAIL, PASSWORD)
        client.login()
    return client


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
    pk = get_client()

    # List root files (depends on PikPakAPI function names)
    files = pk.list_files()

    streams = []

    for f in files:
        name = f["name"].lower()
        if not name.endswith(VIDEO_EXT):
            continue

        # Get direct download URL
        url = pk.get_download_url(f["id"])

        streams.append({
            "name": "PikPak",
            "title": f["name"],
            "url": url
        })

    return {"streams": streams}
