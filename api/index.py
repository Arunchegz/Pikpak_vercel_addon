@app.get("/stream/{type}/{id}.json")
async def stream(type: str, id: str):

    pk = await get_client()

    # -----------------------
    # 1️⃣ Direct PikPak ID
    # -----------------------
    if id.startswith("pikpak:"):
        file_id = id.replace("pikpak:", "")

        url = await get_cached_url(file_id)
        if not url:
            data = await pk.get_download_url(file_id)

            links = data.get("links", {})
            if "application/octet-stream" in links:
                url = links["application/octet-stream"]["url"]
            else:
                medias = data.get("medias", [])
                if medias:
                    url = medias[0]["link"]["url"]

            if not url:
                return {"streams": []}

            await set_cached_url(file_id, url)

        return {
            "streams": [{
                "name": "PikPak",
                "title": "PikPak Direct",
                "url": url
            }]
        }

    # -----------------------
    # 2️⃣ IMDb movie matching
    # -----------------------
    if type != "movie":
        return {"streams": []}

    movie_title, movie_year = get_movie_info(id)
    movie_n = normalize(movie_title)

    files = await collect_files(pk)
    streams = []

    for f in files:
        name = f.get("name")
        file_id = f.get("id")

        if not name or not file_id:
            continue
        if not name.lower().endswith(VIDEO_EXT):
            continue

        file_n = normalize(name)

        if movie_n not in file_n:
            continue
        if movie_year and movie_year not in file_n:
            continue

        url = await get_cached_url(file_id)
        if not url:
            data = await pk.get_download_url(file_id)

            links = data.get("links", {})
            if "application/octet-stream" in links:
                url = links["application/octet-stream"]["url"]
            else:
                medias = data.get("medias", [])
                if medias:
                    url = medias[0]["link"]["url"]

            if not url:
                continue

            await set_cached_url(file_id, url)

        streams.append({
            "name": "PikPak",
            "title": name,
            "url": url
        })

    return {"streams": streams}
