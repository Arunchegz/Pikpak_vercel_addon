from fastapi import FastAPI
import os
from pikpakapi.pikpak import PikPakApi

app = FastAPI()

EMAIL = os.environ.get("PIKPAK_EMAIL")
PASSWORD = os.environ.get("PIKPAK_PASSWORD")

client = None

VIDEO_EXT = (".mp4", ".mkv", ".avi", ".mov", ".webm")


def get_client():
    global client
    if client is None:
        client = PikPakApi(EMAIL, PASSWORD)
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

    # In Quan666 API, root listing is usually:
    files = pk.file_list(parent_id="")

    streams = []

    for f in files.get("files", []):
        name = f["name"].lower()
        if not name.endswith(VIDEO_EXT):
            continue

        # Download URL call
        link = pk.get_download_url(f["id"])

        streams.append({
            "name": "PikPak",
            "title": f["name"],
            "url": link
        })

    return {"streams": streams}