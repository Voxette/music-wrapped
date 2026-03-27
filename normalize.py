"""Normalize music data from various sources into a unified CSV format.

Target CSV columns: Artist, Album, Track, Date, Timestamp, Loved
"""

import csv
import json
import os
import zipfile
from datetime import datetime, timezone


def normalize_spotify(input_paths, output_csv, progress_cb=None, min_ms=30000):
    """Convert Spotify extended streaming history JSON(s) to scrobble CSV.

    Args:
        input_paths: list of paths to JSON files or a single ZIP path
        output_csv: output CSV path
        progress_cb: optional callback(message)
        min_ms: minimum ms_played to count as a scrobble (default 30s)

    Returns:
        dict with stats: {total_entries, skipped_short, skipped_null, skipped_podcast, written}
    """
    stats = {'total_entries': 0, 'skipped_short': 0, 'skipped_null': 0, 'skipped_podcast': 0, 'written': 0}
    entries = []

    json_files = []
    for path in input_paths:
        if path.endswith('.zip'):
            # Extract JSON files from ZIP
            with zipfile.ZipFile(path, 'r') as zf:
                for name in zf.namelist():
                    if name.endswith('.json') and 'Streaming_History' in name:
                        json_files.append(('zip', zf.read(name), name))
        else:
            json_files.append(('file', path, path))

    for source_type, source, name in json_files:
        if progress_cb:
            progress_cb(f'Parsing {os.path.basename(name)}...')

        if source_type == 'zip':
            data = json.loads(source)
        else:
            with open(source, 'r', encoding='utf-8') as f:
                data = json.load(f)

        for entry in data:
            stats['total_entries'] += 1

            artist = entry.get('master_metadata_album_artist_name')
            track = entry.get('master_metadata_track_name')
            album = entry.get('master_metadata_album_album_name', '')
            ms_played = entry.get('ms_played', 0)
            ts_str = entry.get('ts', '')

            # Skip podcast episodes
            if entry.get('episode_name') or entry.get('episode_show_name'):
                stats['skipped_podcast'] += 1
                continue

            # Skip null artist/track
            if not artist or not track:
                stats['skipped_null'] += 1
                continue

            # Skip short plays
            if ms_played < min_ms:
                stats['skipped_short'] += 1
                continue

            # Parse timestamp
            try:
                dt = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
                timestamp = int(dt.timestamp())
                date_str = dt.strftime('%d %b %Y %H:%M')
            except (ValueError, AttributeError):
                timestamp = 0
                date_str = ''

            entries.append({
                'Artist': artist,
                'Album': album or '',
                'Track': track,
                'Date': date_str,
                'Timestamp': timestamp,
                'Loved': 0,
            })

    # Sort by timestamp
    entries.sort(key=lambda e: e['Timestamp'])

    # Write CSV
    with open(output_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['Artist', 'Album', 'Track', 'Date', 'Timestamp', 'Loved'])
        writer.writeheader()
        for entry in entries:
            writer.writerow(entry)
            stats['written'] += 1

    if progress_cb:
        progress_cb(f'Normalized {stats["written"]} scrobbles from Spotify export')

    return stats


def normalize_lastfm_csv(input_csv, output_csv, progress_cb=None):
    """Validate and normalize a Last.fm CSV export.

    Ensures required columns exist and copies to session directory.

    Returns:
        dict with stats: {total_rows, written, skipped}
    """
    stats = {'total_rows': 0, 'written': 0, 'skipped': 0}
    required = {'Artist', 'Track'}

    with open(input_csv, 'r', encoding='utf-8') as fin:
        reader = csv.DictReader(fin)

        if not required.issubset(set(reader.fieldnames or [])):
            raise ValueError(f'CSV missing required columns. Found: {reader.fieldnames}. Need at least: {required}')

        # Normalize column names and write
        fieldnames = ['Artist', 'Album', 'Track', 'Date', 'Timestamp', 'Loved']
        with open(output_csv, 'w', newline='', encoding='utf-8') as fout:
            writer = csv.DictWriter(fout, fieldnames=fieldnames)
            writer.writeheader()

            for row in reader:
                stats['total_rows'] += 1
                artist = row.get('Artist', '').strip()
                track = row.get('Track', '').strip()

                if not artist or not track:
                    stats['skipped'] += 1
                    continue

                writer.writerow({
                    'Artist': artist,
                    'Album': row.get('Album', '').strip(),
                    'Track': track,
                    'Date': row.get('Date', ''),
                    'Timestamp': row.get('Timestamp', ''),
                    'Loved': row.get('Loved', 0),
                })
                stats['written'] += 1

    if progress_cb:
        progress_cb(f'Validated {stats["written"]} scrobbles from Last.fm CSV')

    return stats
