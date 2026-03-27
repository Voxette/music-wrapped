"""Pipeline orchestrator — chains normalize → enrich → analyze with progress reporting."""

import csv
import json
import os
import queue
import threading
import time
import urllib.request
import urllib.parse

from normalize import normalize_spotify, normalize_lastfm_csv
from enrich import fetch_tags_for_artists, fetch_wikidata_for_artists
from analyze import run_analysis


def _fetch_pages(username, api_key, progress_queue, from_ts=None):
    """Fetch scrobble pages from Last.fm API. Returns list of track dicts."""
    API_BASE = "https://ws.audioscrobbler.com/2.0/"
    page = 1
    total_pages = 1
    all_tracks = []

    while page <= total_pages:
        params = {
            "method": "user.getrecenttracks",
            "user": username,
            "api_key": api_key,
            "format": "json",
            "limit": "200",
            "page": str(page),
        }
        if from_ts:
            params["from"] = str(from_ts + 1)  # exclusive lower bound
        url = f"{API_BASE}?{urllib.parse.urlencode(params)}"

        for attempt in range(5):
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "MusicWrapped/1.0"})
                with urllib.request.urlopen(req, timeout=30) as resp:
                    data = json.loads(resp.read())
                break
            except Exception as e:
                if attempt == 4:
                    raise RuntimeError(f"Failed to fetch page {page}: {e}")
                time.sleep(min(2 ** attempt, 30))

        rt = data.get("recenttracks", {})
        attr = rt.get("@attr", {})
        total_pages = int(attr.get("totalPages", 1))

        tracks = rt.get("track", [])
        if isinstance(tracks, dict):
            tracks = [tracks]

        for t in tracks:
            if t.get("@attr", {}).get("nowplaying"):
                continue
            date_info = t.get("date", {})
            all_tracks.append({
                "Artist": t.get("artist", {}).get("#text", ""),
                "Album": t.get("album", {}).get("#text", ""),
                "Track": t.get("name", ""),
                "Date": date_info.get("#text", ""),
                "Timestamp": date_info.get("uts", ""),
                "Loved": "1" if t.get("loved") == "1" else "0",
            })

        progress_queue.put({
            "stage": "fetch",
            "progress": page,
            "total": total_pages,
            "message": f"Fetching scrobbles: page {page}/{total_pages} ({len(all_tracks):,} tracks)",
        })

        page += 1
        time.sleep(0.25)

    return all_tracks


def fetch_lastfm_scrobbles(username, api_key, output_csv, progress_queue, cache_dir=None):
    """Fetch scrobbles, using a per-user cache and incrementally updating."""
    # Determine cache path
    cache_csv = None
    if cache_dir:
        safe_name = "".join(c if c.isalnum() else "_" for c in username.lower())
        cache_csv = os.path.join(cache_dir, f"scrobbles_{safe_name}.csv")

    existing_tracks = []
    last_ts = None

    # Load existing cache if present
    if cache_csv and os.path.exists(cache_csv):
        with open(cache_csv, 'r', encoding='utf-8') as f:
            rows = list(csv.DictReader(f))
        if rows:
            existing_tracks = rows
            timestamps = [int(r["Timestamp"]) for r in rows if r.get("Timestamp", "").isdigit()]
            if timestamps:
                last_ts = max(timestamps)
            progress_queue.put({
                "stage": "fetch",
                "progress": 0,
                "total": 1,
                "message": f"Found {len(existing_tracks):,} cached scrobbles — fetching new ones since last sync...",
            })

    # Fetch new scrobbles (all if no cache, incremental if cached)
    new_tracks = _fetch_pages(username, api_key, progress_queue, from_ts=last_ts)

    # Merge: new tracks first (most recent), then existing (dedup by timestamp)
    existing_ts = {r["Timestamp"] for r in existing_tracks if r.get("Timestamp")}
    new_tracks = [t for t in new_tracks if t["Timestamp"] not in existing_ts]
    all_tracks = new_tracks + existing_tracks

    # Sort by timestamp descending (Last.fm order)
    all_tracks.sort(key=lambda r: int(r["Timestamp"]) if r.get("Timestamp", "").isdigit() else 0, reverse=True)

    # Save to cache
    fieldnames = ["Artist", "Album", "Track", "Date", "Timestamp", "Loved"]
    if cache_csv:
        with open(cache_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_tracks)

    # Write session CSV
    with open(output_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_tracks)

    added = len(new_tracks)
    total = len(all_tracks)
    if last_ts:
        progress_queue.put({"stage": "fetch", "progress": 1, "total": 1,
                            "message": f"Updated: +{added:,} new scrobbles ({total:,} total)"})
    return total


def run_pipeline(session_id, source_type, input_data, progress_queue,
                 cache_dir="data/cache", session_dir=None, api_key=None, eras=None):
    """Run the full processing pipeline.

    Args:
        session_id: unique session identifier
        source_type: "lastfm_username", "lastfm_csv", or "spotify"
        input_data: username string or list of file paths
        progress_queue: queue.Queue for SSE progress events
        cache_dir: path to shared cache directory
        session_dir: path to session working directory
        api_key: Last.fm API key (required for username fetch and tag enrichment)
    """
    try:
        if session_dir is None:
            session_dir = f"data/sessions/{session_id}"
        os.makedirs(session_dir, exist_ok=True)
        os.makedirs(cache_dir, exist_ok=True)

        csv_path = os.path.join(session_dir, "scrobbles.csv")
        tags_path = os.path.join(session_dir, "tags.json")
        wikidata_path = os.path.join(session_dir, "wikidata.json")
        report_path = os.path.join(session_dir, "report.json")

        cache_tags_path = os.path.join(cache_dir, "tags.json")
        cache_wd_path = os.path.join(cache_dir, "wikidata.json")

        source = "lastfm" if source_type.startswith("lastfm") else "spotify"

        # --- Stage 1: Ingest ---
        progress_queue.put({"stage": "ingest", "progress": 0, "total": 1, "message": "Starting data ingest..."})

        if source_type == "lastfm_username":
            if not api_key:
                raise RuntimeError("Last.fm API key required for username fetch")
            count = fetch_lastfm_scrobbles(input_data, api_key, csv_path, progress_queue, cache_dir=cache_dir)
            progress_queue.put({"stage": "ingest", "progress": 1, "total": 1, "message": f"Loaded {count:,} scrobbles"})

        elif source_type == "lastfm_csv":
            stats = normalize_lastfm_csv(input_data, csv_path)
            progress_queue.put({"stage": "ingest", "progress": 1, "total": 1, "message": f"Loaded {stats['written']:,} scrobbles from CSV"})

        elif source_type == "spotify":
            stats = normalize_spotify(
                input_data, csv_path,
                progress_cb=lambda msg: progress_queue.put({"stage": "ingest", "progress": 0, "total": 1, "message": msg})
            )
            progress_queue.put({"stage": "ingest", "progress": 1, "total": 1,
                                "message": f"Normalized {stats['written']:,} scrobbles ({stats['skipped_short']} too short, {stats['skipped_podcast']} podcasts filtered)"})

        # --- Stage 2: Get unique artists ---
        artists = set()
        with open(csv_path, 'r', encoding='utf-8') as f:
            for row in csv.DictReader(f):
                if row['Artist']:
                    artists.add(row['Artist'])
        artists = sorted(artists)
        progress_queue.put({"stage": "enrich_tags", "progress": 0, "total": len(artists), "message": f"Found {len(artists):,} unique artists"})

        # --- Stage 3: Enrich Tags ---
        tag_cache = {}
        if os.path.exists(cache_tags_path):
            with open(cache_tags_path, 'r', encoding='utf-8') as f:
                tag_cache = json.load(f)

        if api_key:
            def tag_progress(fetched, total, name):
                progress_queue.put({"stage": "enrich_tags", "progress": fetched, "total": total, "message": f"Tags: {fetched}/{total} — {name}"})

            tag_cache = fetch_tags_for_artists(artists, api_key, cache=tag_cache,
                                                cache_path=cache_tags_path, progress_cb=tag_progress)
        else:
            progress_queue.put({"stage": "enrich_tags", "progress": len(artists), "total": len(artists),
                                "message": "Skipping tags (no API key)"})

        # Save session copy
        with open(tags_path, 'w', encoding='utf-8') as f:
            json.dump({a: tag_cache.get(a, []) for a in artists}, f, ensure_ascii=False)

        # --- Stage 4: Enrich Wikidata ---
        wd_cache = {}
        if os.path.exists(cache_wd_path):
            with open(cache_wd_path, 'r', encoding='utf-8') as f:
                wd_cache = json.load(f)

        def wd_progress(fetched, total, name):
            progress_queue.put({"stage": "enrich_wikidata", "progress": fetched, "total": total, "message": f"Wikidata: {fetched}/{total} — {name}"})

        wd_cache = fetch_wikidata_for_artists(artists, cache=wd_cache,
                                               cache_path=cache_wd_path, progress_cb=wd_progress)

        # Save session copy
        with open(wikidata_path, 'w', encoding='utf-8') as f:
            json.dump({a: wd_cache.get(a, {}) for a in artists}, f, ensure_ascii=False)

        # --- Stage 5: Analyze ---
        def analyze_progress(stage, msg):
            progress_queue.put({"stage": "analyze", "progress": 0, "total": 1, "message": msg})

        report = run_analysis(
            csv_path, tags_path, wikidata_path, report_path,
            progress_cb=analyze_progress, source=source, eras=eras,
        )

        progress_queue.put({
            "stage": "complete",
            "progress": 1,
            "total": 1,
            "message": "Analysis complete!",
            "report_url": f"/session/{session_id}/report.json",
        })

    except Exception as e:
        progress_queue.put({
            "stage": "error",
            "message": str(e),
        })


def run_pipeline_thread(session_id, source_type, input_data, progress_queue, **kwargs):
    """Run pipeline in a background thread."""
    t = threading.Thread(
        target=run_pipeline,
        args=(session_id, source_type, input_data, progress_queue),
        kwargs=kwargs,
        daemon=True,
    )
    t.start()
    return t
