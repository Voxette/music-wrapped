"""Enrich artist data from Last.fm tags and Wikidata.

Provides genre tags, country, gender, and type for artists.
Uses a shared cache to avoid re-fetching across sessions.
"""

import json
import os
import time
import urllib.request
import urllib.parse
import urllib.error

LASTFM_API = "https://ws.audioscrobbler.com/2.0/"
USER_AGENT = "MusicWrapped/1.0"

# --- Last.fm Tag Fetching ---

def fetch_artist_tags(artist, api_key):
    """Fetch top tags for a single artist from Last.fm."""
    params = urllib.parse.urlencode({
        "method": "artist.gettoptags",
        "artist": artist,
        "api_key": api_key,
        "format": "json",
    })
    url = f"{LASTFM_API}?{params}"

    for attempt in range(5):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read())
            if "error" in data:
                return []
            tags = data.get("toptags", {}).get("tag", [])
            if isinstance(tags, dict):
                tags = [tags]
            return [{"name": t["name"], "count": int(t["count"])} for t in tags[:10]]
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 504):
                time.sleep(min(2 ** attempt, 30))
                continue
            return []
        except (urllib.error.URLError, ConnectionError, TimeoutError):
            time.sleep(min(2 ** attempt, 30))
            continue
    return []


def fetch_tags_for_artists(artists, api_key, cache=None, cache_path=None, progress_cb=None):
    """Fetch tags for a list of artists, using cache.

    Args:
        artists: list of artist names
        api_key: Last.fm API key
        cache: dict of existing tag data (modified in-place)
        cache_path: path to save cache periodically
        progress_cb: callback(fetched, total, artist_name)

    Returns:
        dict mapping artist name → list of tag dicts
    """
    if cache is None:
        cache = {}

    remaining = [a for a in artists if a not in cache]
    total = len(artists)
    already = total - len(remaining)

    if progress_cb:
        progress_cb(already, total, f"Starting tag fetch ({already} cached, {len(remaining)} to fetch)")

    for i, artist in enumerate(remaining):
        tags = fetch_artist_tags(artist, api_key)
        cache[artist] = tags

        if progress_cb and ((i + 1) % 20 == 0 or i == len(remaining) - 1):
            progress_cb(already + i + 1, total, artist)

        # Save cache periodically
        if cache_path and ((i + 1) % 100 == 0 or i == len(remaining) - 1):
            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump(cache, f, ensure_ascii=False)

        time.sleep(0.2)  # 5 req/sec

    return cache


# --- Wikidata Enrichment ---

# Entity ID mappings
GENDER_MAP = {
    "Q6581097": "male", "Q6581072": "female", "Q1052281": "female (transgender)",
    "Q2449503": "male (transgender)", "Q1097630": "other", "Q48270": "non-binary",
}

COUNTRY_MAP = {
    "Q30": "US", "Q145": "GB", "Q183": "DE", "Q142": "FR", "Q17": "JP",
    "Q38": "IT", "Q29": "ES", "Q34": "SE", "Q20": "NO", "Q35": "DK",
    "Q55": "NL", "Q36": "PL", "Q159": "RU", "Q16": "CA", "Q408": "AU",
    "Q31": "BE", "Q39": "CH", "Q40": "AT", "Q33": "FI", "Q189": "IS",
    "Q37": "IE", "Q45": "PT", "Q184": "BY", "Q212": "UA", "Q218": "RO",
    "Q213": "CZ", "Q28": "HU", "Q214": "SK", "Q219": "BG", "Q215": "SI",
    "Q224": "HR", "Q217": "MD", "Q225": "BA", "Q229": "CY", "Q233": "MT",
    "Q211": "LV", "Q37": "LT", "Q191": "EE", "Q227": "AZ", "Q230": "GE",
    "Q399": "AM", "Q148": "CN", "Q884": "KR", "Q865": "TW", "Q252": "ID",
    "Q928": "PH", "Q869": "TH", "Q574": "TL", "Q334": "SG", "Q833": "MY",
    "Q668": "IN", "Q902": "BD", "Q843": "PK", "Q155": "BR", "Q96": "MX",
    "Q414": "AR", "Q298": "CL", "Q736": "EC", "Q739": "CO", "Q717": "VE",
    "Q733": "PY", "Q77": "UY", "Q419": "PE", "Q750": "BO", "Q241": "CU",
    "Q79": "EG", "Q262": "DZ", "Q1028": "MA", "Q948": "TN", "Q115": "ET",
    "Q114": "KE", "Q258": "ZA", "Q1033": "NG", "Q117": "GH",
    "Q664": "NZ", "Q794": "IR", "Q801": "IL", "Q851": "SA", "Q842": "OM",
    "Q878": "AE", "Q398": "BH", "Q846": "QA", "Q817": "KW", "Q810": "JO",
    "Q858": "SY", "Q796": "IQ", "Q232": "KZ",
}

MUSICIAN_TYPES = {
    "Q5": "Person", "Q215380": "Group", "Q2088357": "Group", "Q4438121": "Group",
    "Q56816954": "Group", "Q116856711": "Group", "Q28389": "Group",
    "Q846346": "Group", "Q641066": "Group",
}


def _wd_search(name):
    params = urllib.parse.urlencode({
        "action": "wbsearchentities", "search": name,
        "language": "en", "format": "json", "type": "item", "limit": "5",
    })
    url = f"https://www.wikidata.org/w/api.php?{params}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _wd_get_entity(entity_id):
    params = urllib.parse.urlencode({
        "action": "wbgetentities", "ids": entity_id,
        "props": "claims|labels|descriptions", "languages": "en", "format": "json",
    })
    url = f"https://www.wikidata.org/w/api.php?{params}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    return data.get("entities", {}).get(entity_id, {})


def _get_entity_ids(claims, prop):
    if prop not in claims:
        return []
    ids = []
    for c in claims[prop]:
        mv = c.get("mainsnak", {}).get("datavalue", {})
        if mv.get("type") == "wikibase-entityid":
            ids.append(mv["value"]["id"])
    return ids


def _get_time_value(claims, prop):
    if prop not in claims:
        return None
    for c in claims[prop]:
        mv = c.get("mainsnak", {}).get("datavalue", {})
        if mv.get("type") == "time":
            return mv["value"]["time"]
    return None


def search_artist_wikidata(name):
    """Search Wikidata for a musical artist. Returns structured metadata dict."""
    try:
        results = _wd_search(name)
    except Exception as e:
        return {"error": str(e)}

    candidates = results.get("search", [])
    if not candidates:
        return {"not_found": True}

    skip_words = ["village", "city", "district", "river", "mountain", "film",
                   "television", "protein", "species", "genus", "asteroid"]

    for candidate in candidates[:3]:
        qid = candidate["id"]
        desc = candidate.get("description", "").lower()

        if any(w in desc for w in skip_words):
            continue

        if candidate.get("label", "").lower() != name.lower():
            if name.lower() not in candidate.get("label", "").lower():
                continue

        try:
            entity = _wd_get_entity(qid)
        except Exception as e:
            return {"error": str(e)}

        claims = entity.get("claims", {})
        instances = _get_entity_ids(claims, "P31")
        occupations = _get_entity_ids(claims, "P106")

        is_person = "Q5" in instances
        is_group = any(i in MUSICIAN_TYPES and i != "Q5" for i in instances)

        music_occupations = {"Q177220", "Q639669", "Q488205", "Q36834", "Q183945", "Q130857", "Q855091", "Q386854", "Q584301"}
        is_music = any(o in music_occupations for o in occupations)

        if not is_person and not is_group and not is_music:
            continue

        gender_ids = _get_entity_ids(claims, "P21")
        country_ids = _get_entity_ids(claims, "P27") or _get_entity_ids(claims, "P495") or _get_entity_ids(claims, "P17")

        gender = next((GENDER_MAP[g] for g in gender_ids if g in GENDER_MAP), None)
        country = next((COUNTRY_MAP[c] for c in country_ids if c in COUNTRY_MAP), None)
        artist_type = next((MUSICIAN_TYPES[i] for i in instances if i in MUSICIAN_TYPES), None)

        return {
            "qid": qid,
            "name": candidate.get("label", name),
            "description": candidate.get("description", ""),
            "type": artist_type or ("Person" if is_person else "Group" if is_group else None),
            "gender": gender,
            "country": country,
            "born": _get_time_value(claims, "P569"),
            "formed": _get_time_value(claims, "P571"),
            "source": "wikidata",
        }

    return {"not_found": True}


def fetch_wikidata_for_artists(artists, cache=None, cache_path=None, progress_cb=None):
    """Fetch Wikidata metadata for a list of artists, using cache.

    Args:
        artists: list of artist names
        cache: dict of existing Wikidata data (modified in-place)
        cache_path: path to save cache periodically
        progress_cb: callback(fetched, total, artist_name)

    Returns:
        dict mapping artist name → metadata dict
    """
    if cache is None:
        cache = {}

    # Only fetch artists not in cache (or with errors)
    remaining = [a for a in artists if a not in cache or cache[a].get("error")]
    total = len(artists)
    already = total - len(remaining)

    if progress_cb:
        progress_cb(already, total, f"Starting Wikidata fetch ({already} cached, {len(remaining)} to fetch)")

    consecutive_errors = 0
    for i, artist in enumerate(remaining):
        result = search_artist_wikidata(artist)
        cache[artist] = result

        if result.get("error"):
            consecutive_errors += 1
        else:
            consecutive_errors = 0

        if progress_cb and ((i + 1) % 20 == 0 or i == len(remaining) - 1):
            progress_cb(already + i + 1, total, artist)

        if cache_path and ((i + 1) % 100 == 0 or i == len(remaining) - 1):
            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump(cache, f, ensure_ascii=False)

        if consecutive_errors >= 20:
            if progress_cb:
                progress_cb(already + i + 1, total, f"Aborting: {consecutive_errors} consecutive errors")
            break

        time.sleep(0.25)  # 4 req/sec

    return cache
