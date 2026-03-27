#!/usr/bin/env python3
"""Analyze Last.fm scrobble data and generate report JSON."""

import csv
import json
import math
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone, timedelta

def load_scrobbles(csv_file):
    """Load and parse scrobbles from CSV."""
    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    scrobbles = []
    for r in rows:
        ts = int(r["Timestamp"]) if r["Timestamp"] else 0
        if ts == 0:
            continue
        unknown_date = ts < 1199145600  # before 2008-01-01, broken/missing timestamps
        if unknown_date:
            scrobbles.append({
                "artist": r["Artist"],
                "track": r["Track"],
                "album": r["Album"],
                "timestamp": ts,
                "dt": None,
                "year": "Unknown",
                "month": None,
                "hour": None,
                "weekday": None,
                "date_str": None,
                "loved": r.get("Loved", "0") == "1",
                "unknown_date": True,
            })
        else:
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            scrobbles.append({
                "artist": r["Artist"],
                "track": r["Track"],
                "album": r["Album"],
                "timestamp": ts,
                "dt": dt,
                "year": dt.year,
                "month": dt.month,
                "hour": dt.hour,
                "weekday": dt.weekday(),
                "date_str": dt.strftime("%Y-%m-%d"),
                "loved": r.get("Loved", "0") == "1",
                "unknown_date": False,
            })

    scrobbles.sort(key=lambda x: x["timestamp"])
    return scrobbles


def top_n(counter, n=25):
    return [{"name": name, "count": count} for name, count in counter.most_common(n)]


def filter_period(scrobbles, start_year=None, end_year=None):
    """Filter scrobbles by year range. Unknown-year scrobbles are excluded from period filters."""
    return [s for s in scrobbles if
            isinstance(s["year"], int) and
            (start_year is None or s["year"] >= start_year) and
            (end_year is None or s["year"] <= end_year)]


def compute_top_stats(scrobbles, n=25):
    """Compute top artists, tracks, albums for a set of scrobbles."""
    artists = Counter(s["artist"] for s in scrobbles)
    tracks = Counter((s["artist"], s["track"]) for s in scrobbles)
    albums = Counter((s["artist"], s["album"]) for s in scrobbles if s["album"])

    top_tracks = [{"name": f"{a} — {t}", "artist": a, "track": t, "count": c}
                  for (a, t), c in tracks.most_common(n)]
    top_albums = [{"name": f"{a} — {al}", "artist": a, "album": al, "count": c}
                  for (a, al), c in albums.most_common(n)]

    return {
        "top_artists": top_n(artists, n),
        "top_tracks": top_tracks,
        "top_albums": top_albums,
    }


GENDER_TAGS = {
    "female vocalists": "Female", "female vocalist": "Female",
    "girl groups": "Female", "girl group": "Female",
    "male vocalists": "Male", "male vocalist": "Male",
}

# Wikidata ISO country code → display name for country stats
ISO_TO_COUNTRY_DISPLAY = {
    "US": "American", "GB": "British", "FR": "French", "DE": "German",
    "IT": "Italian", "SE": "Swedish", "RU": "Russian", "JP": "Japanese",
    "ES": "Spanish", "NL": "Dutch", "DK": "Danish", "FI": "Finnish",
    "AU": "Australian", "CA": "Canadian", "NO": "Norwegian", "RO": "Romanian",
    "PL": "Polish", "BE": "Belgian", "HU": "Hungarian", "KR": "Korean",
    "IE": "Irish", "GR": "Greek", "UA": "Ukrainian", "BR": "Brazilian",
    "IL": "Israeli", "IN": "Indian", "CN": "Chinese", "TR": "Turkish",
    "IS": "Icelandic", "EE": "Estonian", "CZ": "Czech", "RS": "Serbian",
    "HR": "Croatian", "BG": "Bulgarian", "TH": "Thai", "PT": "Portuguese",
    "SG": "Singaporean", "CH": "Swiss", "AT": "Austrian", "MX": "Mexican",
    "AR": "Argentinian", "CL": "Chilean", "CO": "Colombian", "CU": "Cuban",
    "NZ": "New Zealander", "ZA": "South African", "NG": "Nigerian",
    "EG": "Egyptian", "IR": "Iranian", "BY": "Belarusian", "LV": "Latvian",
    "LT": "Lithuanian", "SI": "Slovenian", "SK": "Slovak",
}

# Wikidata ISO → geo country name (for geographic center)
ISO_TO_GEO_COUNTRY = {
    "US": "USA", "GB": "UK", "FR": "France", "DE": "Germany",
    "IT": "Italy", "SE": "Sweden", "RU": "Russia", "JP": "Japan",
    "ES": "Spain", "NL": "Netherlands", "DK": "Denmark", "FI": "Finland",
    "AU": "Australia", "CA": "Canada", "NO": "Norway", "RO": "Romania",
    "PL": "Poland", "BE": "Belgium", "HU": "Hungary", "KR": "South Korea",
    "IE": "Ireland", "GR": "Greece", "UA": "Ukraine", "BR": "Brazil",
    "IL": "Israel", "IN": "India", "CN": "China", "TR": "Turkey",
    "IS": "Iceland", "EE": "Estonia", "CZ": "Czech Republic", "RS": "Serbia",
    "HR": "Croatia", "BG": "Bulgaria", "TH": "Thailand", "PT": "Portugal",
    "SG": "Singapore", "CH": "Switzerland", "AT": "Austria", "MX": "Mexico",
    "AR": "Argentina", "CL": "Chile", "CO": "Colombia", "CU": "Cuba",
    "NZ": "New Zealand", "ZA": "South Africa", "NG": "Nigeria",
    "EG": "Egypt", "IR": "Iran", "BY": "Belarus", "LV": "Latvia",
    "LT": "Lithuania", "SI": "Slovenia", "SK": "Slovakia",
}

# Wikidata gender value → display name
WIKIDATA_GENDER_MAP = {
    "male": "Male", "female": "Female",
    "female (transgender)": "Female", "male (transgender)": "Male",
    "non-binary": "Other",
}

COUNTRY_TAGS = {
    "french": "French", "france": "French", "chanson francaise": "French",
    "chanson": "French", "french pop": "French",
    "british": "British", "uk": "British", "scottish": "British",
    "german": "German", "germany": "German", "deutsch": "German", "schlager": "German",
    "italian": "Italian", "italy": "Italian", "italia": "Italian", "italian pop": "Italian",
    "swedish": "Swedish", "sweden": "Swedish", "melodifestivalen": "Swedish",
    "russian": "Russian", "russia": "Russian", "russian pop": "Russian",
    "japanese": "Japanese", "j-pop": "Japanese", "jpop": "Japanese", "j-rock": "Japanese",
    "spanish": "Spanish", "spain": "Spanish",
    "latin": "Latin", "latin pop": "Latin",
    "dutch": "Dutch", "danish": "Danish", "denmark": "Danish",
    "finnish": "Finnish", "australian": "Australian",
    "american": "American", "usa": "American",
    "canadian": "Canadian", "canada": "Canadian",
    "norwegian": "Norwegian", "norway": "Norwegian",
    "romanian": "Romanian", "romania": "Romanian",
    "polish": "Polish", "belgian": "Belgian", "belgium": "Belgian",
    "hungarian": "Hungarian", "korean": "Korean", "k-pop": "Korean",
    "irish": "Irish", "greek": "Greek", "ukrainian": "Ukrainian",
    "brazilian": "Brazilian",
}

# Tags that are NOT music genres (gender, country, decade, meta)
NON_GENRE_TAGS = set(GENDER_TAGS.keys()) | set(COUNTRY_TAGS.keys()) | {
    "80s", "90s", "70s", "60s", "50s", "00s",
    "all", "composers", "composer", "time", "delta", "scp",
    "under 2000 listeners", "lesser known yet streamable artists",
    "spotify", "artistes", "home collection", "initial d",
    "ddr", "i love disco diamonds", "clara moroni",
    "hi-nrg attack", "a beat c", "boom boom beat",
    "eurovision", "eurovision song contest",
}


# Expanded country tags for geographic mapping (tag -> country name)
GEO_COUNTRY_TAGS = {
    **{k: v for k, v in {
        "french": "France", "france": "France", "chanson francaise": "France",
        "chanson": "France", "french pop": "France",
        "british": "UK", "uk": "UK", "scottish": "UK",
        "german": "Germany", "germany": "Germany", "deutsch": "Germany", "schlager": "Germany",
        "italian": "Italy", "italy": "Italy", "italia": "Italy", "italian pop": "Italy",
        "swedish": "Sweden", "sweden": "Sweden", "melodifestivalen": "Sweden",
        "russian": "Russia", "russia": "Russia", "russian pop": "Russia",
        "japanese": "Japan", "j-pop": "Japan", "jpop": "Japan", "j-rock": "Japan",
        "spanish": "Spain", "spain": "Spain",
        "dutch": "Netherlands", "danish": "Denmark", "denmark": "Denmark",
        "finnish": "Finland", "australian": "Australia",
        "american": "USA", "usa": "USA",
        "canadian": "Canada", "canada": "Canada",
        "norwegian": "Norway", "norway": "Norway",
        "romanian": "Romania", "romania": "Romania",
        "polish": "Poland", "belgian": "Belgium", "belgium": "Belgium",
        "hungarian": "Hungary", "korean": "South Korea", "k-pop": "South Korea",
        "irish": "Ireland", "greek": "Greece", "ukrainian": "Ukraine",
        "brazilian": "Brazil",
        "israeli": "Israel", "hebrew": "Israel",
        "indian": "India", "bhangra": "India",
        "chinese": "China", "mandopop": "China",
        "turkish": "Turkey", "icelandic": "Iceland",
        "estonian": "Estonia", "czech": "Czech Republic",
        "serbian": "Serbia", "croatian": "Croatia", "bulgarian": "Bulgaria",
        "thai": "Thailand", "portuguese": "Portugal", "portugal": "Portugal",
        "singaporean": "Singapore", "swiss": "Switzerland",
        "austrian": "Austria", "latin": "Mexico",  # approximate
        "latin pop": "Mexico",
        # Additional mappings for better coverage
        "soviet": "Russia", "soviet pop": "Russia",
        "moldovan": "Romania", "moldova": "Romania", "moldavian": "Romania",
        "uk bass": "UK", "uk garage": "UK", "uk hardcore": "UK", "british punk": "UK",
        "nederlandstalig": "Netherlands",
        "city pop": "Japan", "anime": "Japan",
        "italo disco": "Italy", "italo-disco": "Italy", "italo": "Italy",
        "hi-nrg": "Italy",  # most Hi-NRG producers are Italian
        "eurobeat": "Italy",  # eurobeat is predominantly Italian-produced
        "spacesynth": "Italy", "space disco": "Italy",
        "kpop": "South Korea",
        "francais": "France", "française": "France",
        "deutsch punk": "Germany", "neue deutsche welle": "Germany", "ndw": "Germany",
    }.items()},
}

# Capital city coordinates (lat, lng)
CAPITAL_COORDS = {
    "France": (48.8566, 2.3522),
    "UK": (51.5074, -0.1278),
    "Germany": (52.5200, 13.4050),
    "Italy": (41.9028, 12.4964),
    "Sweden": (59.3293, 18.0686),
    "Russia": (55.7558, 37.6173),
    "Japan": (35.6762, 139.6503),
    "Spain": (40.4168, -3.7038),
    "Netherlands": (52.3676, 4.9041),
    "Denmark": (55.6761, 12.5683),
    "Finland": (60.1699, 24.9384),
    "Australia": (-33.8688, 151.2093),
    "USA": (38.9072, -77.0369),
    "Canada": (45.4215, -75.6972),
    "Norway": (59.9139, 10.7522),
    "Romania": (44.4268, 26.1025),
    "Poland": (52.2297, 21.0122),
    "Belgium": (50.8503, 4.3517),
    "Hungary": (47.4979, 19.0402),
    "South Korea": (37.5665, 126.9780),
    "Ireland": (53.3498, -6.2603),
    "Greece": (37.9838, 23.7275),
    "Ukraine": (50.4501, 30.5234),
    "Brazil": (-15.7975, -47.8919),
    "Israel": (31.7683, 35.2137),
    "India": (28.6139, 77.2090),
    "China": (39.9042, 116.4074),
    "Turkey": (39.9334, 32.8597),
    "Iceland": (64.1466, -21.9426),
    "Estonia": (59.4370, 24.7536),
    "Czech Republic": (50.0755, 14.4378),
    "Serbia": (44.7866, 20.4489),
    "Croatia": (45.8150, 15.9819),
    "Bulgaria": (42.6977, 23.3219),
    "Thailand": (13.7563, 100.5018),
    "Portugal": (38.7223, -9.1393),
    "Singapore": (1.3521, 103.8198),
    "Switzerland": (46.9480, 7.4474),
    "Austria": (48.2082, 16.3738),
    "Mexico": (19.4326, -99.1332),
}

# Large world cities for nearest-city lookup
MAJOR_CITIES = [
    ("London", 51.5074, -0.1278), ("Paris", 48.8566, 2.3522),
    ("Berlin", 52.5200, 13.4050), ("Moscow", 55.7558, 37.6173),
    ("Tokyo", 35.6762, 139.6503), ("New York", 40.7128, -74.0060),
    ("Stockholm", 59.3293, 18.0686), ("Copenhagen", 55.6761, 12.5683),
    ("Rome", 41.9028, 12.4964), ("Madrid", 40.4168, -3.7038),
    ("Amsterdam", 52.3676, 4.9041), ("Brussels", 50.8503, 4.3517),
    ("Vienna", 48.2082, 16.3738), ("Warsaw", 52.2297, 21.0122),
    ("Prague", 50.0755, 14.4378), ("Budapest", 47.4979, 19.0402),
    ("Bucharest", 44.4268, 26.1025), ("Helsinki", 60.1699, 24.9384),
    ("Oslo", 59.9139, 10.7522), ("Dublin", 53.3498, -6.2603),
    ("Zurich", 47.3769, 8.5417), ("Munich", 48.1351, 11.5820),
    ("Hamburg", 53.5511, 9.9937), ("Frankfurt", 50.1109, 8.6821),
    ("Cologne", 50.9375, 6.9603), ("Milan", 45.4642, 9.1900),
    ("Barcelona", 41.3874, 2.1686), ("Lisbon", 38.7223, -9.1393),
    ("Athens", 37.9838, 23.7275), ("Istanbul", 41.0082, 28.9784),
    ("Kyiv", 50.4501, 30.5234), ("Saint Petersburg", 59.9343, 30.3351),
    ("Minsk", 53.9006, 27.5590), ("Riga", 56.9496, 24.1052),
    ("Vilnius", 54.6872, 25.2797), ("Tallinn", 59.4370, 24.7536),
    ("Zagreb", 45.8150, 15.9819), ("Belgrade", 44.7866, 20.4489),
    ("Sofia", 42.6977, 23.3219), ("Bratislava", 48.1486, 17.1077),
    ("Ljubljana", 46.0569, 14.5058), ("Kraków", 50.0647, 19.9450),
    ("Lyon", 45.7640, 4.8357), ("Marseille", 43.2965, 5.3698),
    ("Manchester", 53.4808, -2.2426), ("Edinburgh", 55.9533, -3.1883),
    ("Gothenburg", 57.7089, 11.9746), ("Malmö", 55.6049, 13.0038),
    ("Toronto", 43.6532, -79.3832), ("Los Angeles", 34.0522, -118.2437),
    ("Chicago", 41.8781, -87.6298), ("Sydney", -33.8688, 151.2093),
    ("Melbourne", -37.8136, 144.9631), ("São Paulo", -23.5505, -46.6333),
    ("Buenos Aires", -34.6037, -58.3816), ("Mexico City", 19.4326, -99.1332),
    ("Seoul", 37.5665, 126.9780), ("Beijing", 39.9042, 116.4074),
    ("Shanghai", 31.2304, 121.4737), ("Mumbai", 19.0760, 72.8777),
    ("Delhi", 28.7041, 77.1025), ("Bangkok", 13.7563, 100.5018),
    ("Singapore", 1.3521, 103.8198), ("Dubai", 25.2048, 55.2708),
    ("Cairo", 30.0444, 31.2357), ("Johannesburg", -26.2041, 28.0473),
    ("Nairobi", -1.2921, 36.8219), ("Lagos", 6.5244, 3.3792),
    ("Ankara", 39.9334, 32.8597), ("Tel Aviv", 32.0853, 34.7818),
]


def compute_geographic_center(scrobbles, tags_data, enriched_data=None):
    """Compute the weighted geographic center using Wikidata (primary) + Last.fm tags (fallback)."""
    enriched_data = enriched_data or {}
    # Map each artist to a country
    artist_country = {}
    for artist in set(s["artist"] for s in scrobbles):
        # 1. Try Wikidata first
        wd = enriched_data.get(artist, {})
        iso = wd.get("country")
        if iso and iso in ISO_TO_GEO_COUNTRY:
            geo_country = ISO_TO_GEO_COUNTRY[iso]
            if geo_country in CAPITAL_COORDS:
                artist_country[artist] = geo_country
                continue

        # 2. Fall back to Last.fm tags
        artist_tags = tags_data.get(artist, [])
        for t in artist_tags[:10]:
            name_lower = t["name"].lower().strip()
            if name_lower in GEO_COUNTRY_TAGS:
                artist_country[artist] = GEO_COUNTRY_TAGS[name_lower]
                break

    # Count plays per country + per-country artist breakdown
    country_plays = Counter()
    country_artist_plays = defaultdict(Counter)
    mapped_plays = 0
    for s in scrobbles:
        c = artist_country.get(s["artist"])
        if c and c in CAPITAL_COORDS:
            country_plays[c] += 1
            country_artist_plays[c][s["artist"]] += 1
            mapped_plays += 1

    if not country_plays:
        return None

    total = sum(country_plays.values())

    # Weighted geographic midpoint using cartesian coordinates
    x, y, z = 0.0, 0.0, 0.0
    for country, plays in country_plays.items():
        lat, lng = CAPITAL_COORDS[country]
        lat_r = math.radians(lat)
        lng_r = math.radians(lng)
        weight = plays / total
        x += math.cos(lat_r) * math.cos(lng_r) * weight
        y += math.cos(lat_r) * math.sin(lng_r) * weight
        z += math.sin(lat_r) * weight

    center_lat = math.degrees(math.atan2(z, math.sqrt(x*x + y*y)))
    center_lng = math.degrees(math.atan2(y, x))

    # Find nearest major city
    def haversine(lat1, lng1, lat2, lng2):
        R = 6371
        dlat = math.radians(lat2 - lat1)
        dlng = math.radians(lng2 - lng1)
        a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlng/2)**2
        return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

    nearest_city = min(MAJOR_CITIES, key=lambda c: haversine(center_lat, center_lng, c[1], c[2]))
    dist_km = haversine(center_lat, center_lng, nearest_city[1], nearest_city[2])

    # Top countries for display (with artist breakdown + Gini coefficient)
    def gini_coefficient(values):
        """Compute Gini coefficient (0=perfectly equal, 1=one artist has all plays)."""
        if len(values) < 2:
            return 0.0
        sorted_v = sorted(values)
        n = len(sorted_v)
        total_v = sum(sorted_v)
        if total_v == 0:
            return 0.0
        cum = sum((2 * (i + 1) - n - 1) * v for i, v in enumerate(sorted_v))
        return round(cum / (n * total_v), 3)

    top_countries = []
    for c, p in country_plays.most_common(10):
        all_artist_plays = [v for v in country_artist_plays[c].values() if v > 5]
        top_artists = [{"name": a, "plays": pl}
                       for a, pl in country_artist_plays[c].most_common(10)]
        top_countries.append({
            "name": c, "plays": p, "pct": round(100 * p / total, 1),
            "artists": top_artists,
            "gini": gini_coefficient(all_artist_plays),
            "num_artists": len(all_artist_plays),
        })

    return {
        "center_lat": round(center_lat, 4),
        "center_lng": round(center_lng, 4),
        "nearest_city": nearest_city[0],
        "nearest_city_lat": nearest_city[1],
        "nearest_city_lng": nearest_city[2],
        "distance_km": round(dist_km),
        "mapped_plays": mapped_plays,
        "total_plays": len(scrobbles),
        "coverage_pct": round(100 * mapped_plays / len(scrobbles), 1),
        "top_countries": top_countries,
    }


def compute_genre_stats(scrobbles, tags_data, n=30):
    """Compute genre breakdown weighted by play count, excluding non-genre tags."""
    genre_counts = Counter()
    for s in scrobbles:
        artist_tags = tags_data.get(s["artist"], [])
        for tag in artist_tags[:5]:
            name_lower = tag["name"].lower().strip()
            if name_lower in NON_GENRE_TAGS:
                continue
            weight = tag["count"] / 100.0 if tag["count"] > 0 else 0.01
            genre_counts[tag["name"]] += weight

    return top_n(genre_counts, n)


def compute_gender_stats(scrobbles, tags_data, enriched_data=None):
    """Compute vocalist gender breakdown using Wikidata (primary) + Last.fm tags (fallback)."""
    enriched_data = enriched_data or {}
    artist_gender = {}
    for artist in set(s["artist"] for s in scrobbles):
        # 1. Try Wikidata first (authoritative structured data)
        wd = enriched_data.get(artist, {})
        wd_gender = wd.get("gender")
        if wd_gender and wd_gender in WIKIDATA_GENDER_MAP:
            # For groups, Wikidata doesn't have vocalist gender — fall through to tags
            if wd.get("type") != "Group":
                artist_gender[artist] = WIKIDATA_GENDER_MAP[wd_gender]
                continue

        # 2. Fall back to Last.fm tags
        artist_tags = tags_data.get(artist, [])
        tag_names = {t["name"].lower().strip() for t in artist_tags[:5]}
        if tag_names & {"female vocalists", "female vocalist", "girl groups", "girl group"}:
            if tag_names & {"male vocalists", "male vocalist"}:
                artist_gender[artist] = "Mixed"
            else:
                artist_gender[artist] = "Female"
        elif tag_names & {"male vocalists", "male vocalist"}:
            artist_gender[artist] = "Male"
        else:
            artist_gender[artist] = None  # untagged

    gender_counts = Counter()
    untagged = 0
    for s in scrobbles:
        g = artist_gender.get(s["artist"])
        if g:
            gender_counts[g] += 1
        else:
            untagged += 1

    result = [{"name": name, "count": count} for name, count in gender_counts.most_common()]
    result.append({"name": "Untagged", "count": untagged})
    total = sum(r["count"] for r in result)
    for r in result:
        r["pct"] = round(100 * r["count"] / total, 1) if total else 0
    return result


def compute_country_stats(scrobbles, tags_data, enriched_data=None):
    """Compute country/language breakdown using Wikidata (primary) + Last.fm tags (fallback)."""
    enriched_data = enriched_data or {}
    artist_country = {}
    for artist in set(s["artist"] for s in scrobbles):
        # 1. Try Wikidata first
        wd = enriched_data.get(artist, {})
        iso = wd.get("country")
        if iso and iso in ISO_TO_COUNTRY_DISPLAY:
            artist_country[artist] = ISO_TO_COUNTRY_DISPLAY[iso]
            continue

        # 2. Fall back to Last.fm tags
        artist_tags = tags_data.get(artist, [])
        for t in artist_tags[:5]:
            name_lower = t["name"].lower().strip()
            if name_lower in COUNTRY_TAGS:
                artist_country[artist] = COUNTRY_TAGS[name_lower]
                break

    country_counts = Counter()
    untagged = 0
    for s in scrobbles:
        c = artist_country.get(s["artist"])
        if c:
            country_counts[c] += 1
        else:
            untagged += 1

    result = [{"name": name, "count": count} for name, count in country_counts.most_common(15)]
    result.append({"name": "Other / Untagged", "count": untagged})
    total = sum(r["count"] for r in result)
    for r in result:
        r["pct"] = round(100 * r["count"] / total, 1) if total else 0
    return result


def compute_concentration(scrobbles):
    """Compute concentration stats: top 10/50/100 artist play distribution."""
    artist_counts = Counter(s["artist"] for s in scrobbles)
    sorted_counts = sorted(artist_counts.values(), reverse=True)
    total = sum(sorted_counts)
    if total == 0:
        return None
    top10 = sum(sorted_counts[:10])
    top50 = sum(sorted_counts[:50])
    top100 = sum(sorted_counts[:100])
    return {
        "top_10": {"plays": top10, "pct": round(100 * top10 / total, 1)},
        "top_50": {"plays": top50, "pct": round(100 * top50 / total, 1)},
        "top_100": {"plays": top100, "pct": round(100 * top100 / total, 1)},
        "rest": {"plays": total - top100, "pct": round(100 * (total - top100) / total, 1)},
        "total_artists": len(sorted_counts),
    }


# Artists to exclude from narrative highlights (still shown in charts/lists)
SUPPRESS_FROM_HIGHLIGHTS = {"Lolly"}

def compute_fun_highlights(scrobbles, dated, tags_data):
    """Compute fun/quirky Wrapped-style highlights."""
    highlights = []

    # 1. One-hit wonder: artist you played 100+ times but only 1 track
    artist_tracks = defaultdict(set)
    artist_counts = Counter()
    for s in scrobbles:
        artist_tracks[s["artist"]].add(s["track"])
        artist_counts[s["artist"]] += 1
    one_hit_wonders = [(a, artist_counts[a]) for a in artist_tracks
                       if len(artist_tracks[a]) == 1 and artist_counts[a] >= 50
                       and a not in SUPPRESS_FROM_HIGHLIGHTS]
    one_hit_wonders.sort(key=lambda x: -x[1])
    if one_hit_wonders:
        a, c = one_hit_wonders[0]
        track = list(artist_tracks[a])[0]
        highlights.append({
            "icon": "🔂",
            "title": "Ultimate One-Hit Wonder",
            "text": f"You played {a} — \"{track}\" {c:,} times. It's their only song in your library.",
        })

    # 2. Total listening time estimate (avg 3.5 min per track)
    total_minutes = len(scrobbles) * 3.5
    total_days = total_minutes / 1440
    highlights.append({
        "icon": "⏱️",
        "title": "Time Spent Listening",
        "text": f"~{total_days:,.0f} days of music ({total_minutes / 60:,.0f} hours). That's {total_days / 365:.1f} years of non-stop playback.",
    })

    # 3. Most obsessive day
    if dated:
        daily = Counter(s["date_str"] for s in dated)
        biggest_day, biggest_count = daily.most_common(1)[0]
        hours = biggest_count * 3.5 / 60
        highlights.append({
            "icon": "🤯",
            "title": "Most Obsessive Day",
            "text": f"On {biggest_day}, you scrobbled {biggest_count} tracks — roughly {hours:.1f} hours of music.",
        })

    # 4. Loyalty score — how many artists have 500+ plays
    loyal_artists = [a for a, c in artist_counts.items() if c >= 500]
    if loyal_artists:
        highlights.append({
            "icon": "💎",
            "title": "Ride or Die Artists",
            "text": f"You have {len(loyal_artists)} artists with 500+ plays. That's serious commitment.",
        })

    # 5. Most diverse year (most unique artists)
    if dated:
        year_artists = defaultdict(set)
        for s in dated:
            year_artists[s["year"]].add(s["artist"])
        most_diverse = max(year_artists.items(), key=lambda x: len(x[1]))
        highlights.append({
            "icon": "🌈",
            "title": "Most Adventurous Year",
            "text": f"In {most_diverse[0]}, you listened to {len(most_diverse[1]):,} different artists — your most diverse year.",
        })

    # 6. Late night owl vs early bird
    if dated:
        late_night = sum(1 for s in dated if s["hour"] is not None and 0 <= s["hour"] < 5)
        morning = sum(1 for s in dated if s["hour"] is not None and 6 <= s["hour"] < 10)
        pct_late = 100 * late_night / len(dated)
        if late_night > morning:
            highlights.append({
                "icon": "🦉",
                "title": "Night Owl",
                "text": f"{late_night:,} scrobbles between midnight and 5 AM ({pct_late:.1f}% of your listening). The night is your soundtrack.",
            })
        else:
            highlights.append({
                "icon": "🐦",
                "title": "Early Bird",
                "text": f"{morning:,} scrobbles between 6-10 AM. You start the day with music.",
            })

    # 7. Artist you discovered most tracks from
    most_tracks_artist = max(
        ((a, t) for a, t in artist_tracks.items() if a not in SUPPRESS_FROM_HIGHLIGHTS),
        key=lambda x: len(x[1])
    )
    highlights.append({
        "icon": "🗺️",
        "title": "Deepest Catalog Dive",
        "text": f"You've heard {len(most_tracks_artist[1]):,} different tracks from {most_tracks_artist[0]}.",
    })

    # 8. Mainstream vs underground ratio
    one_play_artists = sum(1 for c in artist_counts.values() if c == 1)
    pct_one = 100 * one_play_artists / len(artist_counts)
    highlights.append({
        "icon": "👻",
        "title": "Ghost Artists",
        "text": f"{one_play_artists:,} artists ({pct_one:.0f}%) you only played once. Drive-by listening.",
    })

    # 9. Longest gap between scrobbles (for dated)
    if len(dated) > 1:
        max_gap = 0
        gap_start = ""
        gap_end = ""
        for i in range(1, len(dated)):
            gap = dated[i]["timestamp"] - dated[i-1]["timestamp"]
            if gap > max_gap:
                max_gap = gap
                gap_start = dated[i-1]["date_str"]
                gap_end = dated[i]["date_str"]
        gap_days = max_gap / 86400
        if gap_days > 1:
            highlights.append({
                "icon": "🔇",
                "title": "Longest Silence",
                "text": f"{gap_days:.0f} days without a scrobble ({gap_start} → {gap_end}). What happened?",
            })

    # 10. Genre loyalty — top genre percentage
    if tags_data:
        genre_counts = Counter()
        total_tagged = 0
        for s in scrobbles:
            artist_tags = tags_data.get(s["artist"], [])
            for tag in artist_tags[:3]:
                name_lower = tag["name"].lower().strip()
                if name_lower not in NON_GENRE_TAGS:
                    genre_counts[tag["name"]] += 1
                    total_tagged += 1
                    break
        if total_tagged > 0:
            top_genre, top_count = genre_counts.most_common(1)[0]
            pct = 100 * top_count / total_tagged
            highlights.append({
                "icon": "🏆",
                "title": "Genre Identity",
                "text": f"\"{top_genre}\" dominates {pct:.0f}% of your tagged scrobbles. You know what you like.",
            })

    return highlights


def run_analysis(csv_path, tags_path=None, wikidata_path=None, output_path=None,
                  aliases=None, overrides=None, progress_cb=None, source="lastfm", eras=None):
    """Run the full analysis pipeline.

    Args:
        csv_path: path to normalized scrobble CSV
        tags_path: path to tags JSON (optional)
        wikidata_path: path to Wikidata enrichment JSON (optional)
        output_path: path to write report JSON (optional, returns report dict either way)
        aliases: dict of artist name aliases (optional)
        overrides: dict of manual artist overrides (optional)
        progress_cb: callback(stage, message) for progress reporting
        source: "lastfm" or "spotify" — affects which features to include

    Returns:
        report dict
    """
    def log(msg):
        if progress_cb:
            progress_cb("analyze", msg)
        else:
            print(msg)

    log("Loading scrobbles...")
    scrobbles = load_scrobbles(csv_path)

    # Artist aliases
    if aliases:
        merged = 0
        for s in scrobbles:
            if s["artist"] in aliases:
                s["artist"] = aliases[s["artist"]]
                merged += 1
        log(f"Applied {len(aliases)} artist aliases ({merged:,} scrobbles merged)")

    dated = [s for s in scrobbles if not s["unknown_date"]]
    undated = [s for s in scrobbles if s["unknown_date"]]
    log(f"Loaded {len(scrobbles):,} scrobbles ({len(dated):,} dated, {len(undated):,} unknown date)")

    # Load tags
    tags_data = {}
    if tags_path and os.path.exists(tags_path):
        with open(tags_path, "r", encoding="utf-8") as f:
            tags_data = json.load(f)
        log(f"Loaded tags for {len(tags_data):,} artists")

    # Load Wikidata enrichment
    enriched_data = {}
    if wikidata_path and os.path.exists(wikidata_path):
        with open(wikidata_path, "r", encoding="utf-8") as f:
            enriched_data = json.load(f)
        valid = sum(1 for v in enriched_data.values() if v.get("country") or v.get("gender"))
        log(f"Loaded Wikidata enrichment for {valid:,} artists")

    # Apply manual overrides
    if overrides:
        for artist, data in overrides.items():
            enriched_data[artist] = {**enriched_data.get(artist, {}), **data, "source": "manual_override"}
        log(f"Applied {len(overrides)} manual artist overrides")

    current_year = datetime.now().year
    first_year = min(s["year"] for s in dated)
    last_year = max(s["year"] for s in dated)

    report = {}

    # --- Overview ---
    unique_artists = len(set(s["artist"] for s in scrobbles))
    unique_tracks = len(set((s["artist"], s["track"]) for s in scrobbles))
    unique_albums = len(set((s["artist"], s["album"]) for s in scrobbles if s["album"]))
    date_range_days = (dated[-1]["dt"] - dated[0]["dt"]).days or 1
    loved_count = sum(1 for s in scrobbles if s["loved"])

    report["overview"] = {
        "total_scrobbles": len(scrobbles),
        "undated_scrobbles": len(undated),
        "unique_artists": unique_artists,
        "unique_tracks": unique_tracks,
        "unique_albums": unique_albums,
        "first_scrobble": dated[0]["dt"].strftime("%B %d, %Y"),
        "last_scrobble": dated[-1]["dt"].strftime("%B %d, %Y"),
        "years_active": last_year - first_year + 1,
        "avg_per_day": round(len(scrobbles) / date_range_days, 1),
        "loved_tracks": loved_count,
        "first_year": first_year,
        "last_year": last_year,
    }
    log("  Overview done")

    # --- All-time top stats ---
    report["all_time"] = compute_top_stats(scrobbles)
    log("  All-time stats done")

    # --- By year ---
    report["by_year"] = {}
    for year in range(first_year, last_year + 1):
        year_scrobbles = filter_period(scrobbles, year, year)
        if year_scrobbles:
            stats = compute_top_stats(year_scrobbles)
            stats["total"] = len(year_scrobbles)
            stats["unique_artists"] = len(set(s["artist"] for s in year_scrobbles))
            if tags_data:
                stats["genres"] = compute_genre_stats(year_scrobbles, tags_data)
            report["by_year"][str(year)] = stats

    # Unknown date bucket
    if undated:
        stats = compute_top_stats(undated)
        stats["total"] = len(undated)
        stats["unique_artists"] = len(set(s["artist"] for s in undated))
        if tags_data:
            stats["genres"] = compute_genre_stats(undated, tags_data)
        report["by_year"]["Unknown"] = stats
    log("  By-year stats done")

    # --- Period stats (last 3, 5, 10 years) ---
    report["periods"] = {}
    for period in [3, 5, 10]:
        start = current_year - period + 1
        period_scrobbles = filter_period(scrobbles, start)
        if period_scrobbles:
            stats = compute_top_stats(period_scrobbles)
            stats["total"] = len(period_scrobbles)
            if tags_data:
                stats["genres"] = compute_genre_stats(period_scrobbles, tags_data)
            report["periods"][f"last_{period}y"] = stats
    log("  Period stats done")

    # --- Genre/Tag breakdown (all-time, cleaned) ---
    if tags_data:
        report["genres_all_time"] = compute_genre_stats(scrobbles, tags_data, 40)
        print("  Genre stats done")

    # --- Vocalist gender breakdown (all-time + per-year + periods) ---
    if tags_data:
        report["gender"] = {"all_time": compute_gender_stats(scrobbles, tags_data, enriched_data), "by_year": {}, "periods": {}}
        for year in range(first_year, last_year + 1):
            ys = filter_period(scrobbles, year, year)
            if ys:
                report["gender"]["by_year"][str(year)] = compute_gender_stats(ys, tags_data, enriched_data)
        if undated:
            report["gender"]["by_year"]["Unknown"] = compute_gender_stats(undated, tags_data, enriched_data)
        for period in [3, 5, 10]:
            ps = filter_period(scrobbles, current_year - period + 1)
            if ps:
                report["gender"]["periods"][f"last_{period}y"] = compute_gender_stats(ps, tags_data, enriched_data)
        print("  Gender stats done")

    # --- Country/language breakdown (all-time + per-year + periods) ---
    if tags_data:
        report["countries"] = {"all_time": compute_country_stats(scrobbles, tags_data, enriched_data), "by_year": {}, "periods": {}}
        for year in range(first_year, last_year + 1):
            ys = filter_period(scrobbles, year, year)
            if ys:
                report["countries"]["by_year"][str(year)] = compute_country_stats(ys, tags_data, enriched_data)
        if undated:
            report["countries"]["by_year"]["Unknown"] = compute_country_stats(undated, tags_data, enriched_data)
        for period in [3, 5, 10]:
            ps = filter_period(scrobbles, current_year - period + 1)
            if ps:
                report["countries"]["periods"][f"last_{period}y"] = compute_country_stats(ps, tags_data, enriched_data)
        print("  Country stats done")

    # --- Geographic center (all-time + per-year + periods) ---
    if tags_data:
        geo = compute_geographic_center(scrobbles, tags_data, enriched_data)
        if geo:
            report["geographic_center"] = {"all_time": geo, "by_year": {}, "periods": {}}
            print(f"  Geographic center: near {geo['nearest_city']} ({geo['center_lat']}, {geo['center_lng']}) — {geo['coverage_pct']}% coverage")

            # Per year
            for year in range(first_year, last_year + 1):
                year_scrobbles = filter_period(scrobbles, year, year)
                if year_scrobbles:
                    yr_geo = compute_geographic_center(year_scrobbles, tags_data, enriched_data)
                    if yr_geo:
                        report["geographic_center"]["by_year"][str(year)] = yr_geo

            # Unknown date
            if undated:
                ud_geo = compute_geographic_center(undated, tags_data, enriched_data)
                if ud_geo:
                    report["geographic_center"]["by_year"]["Unknown"] = ud_geo

            # Periods (last 3, 5, 10 years)
            for period in [3, 5, 10]:
                start = current_year - period + 1
                period_scrobbles = filter_period(scrobbles, start)
                if period_scrobbles:
                    p_geo = compute_geographic_center(period_scrobbles, tags_data, enriched_data)
                    if p_geo:
                        report["geographic_center"]["periods"][f"last_{period}y"] = p_geo

            print("  Geographic center by-year done")

    # --- Fun highlights ---
    report["highlights"] = compute_fun_highlights(scrobbles, dated, tags_data)
    log("  Fun highlights done")

    # --- Timeline (monthly) ---
    monthly = Counter()
    for s in dated:
        key = f"{s['year']}-{s['month']:02d}"
        monthly[key] += 1

    months_sorted = sorted(monthly.keys())
    report["timeline"] = {
        "labels": months_sorted,
        "values": [monthly[m] for m in months_sorted],
    }
    log("  Timeline done")

    # --- Yearly totals ---
    yearly = Counter(s["year"] for s in dated)
    if undated:
        yearly["Unknown"] = len(undated)
    years_sorted = sorted([y for y in yearly.keys() if isinstance(y, int)])
    if "Unknown" in yearly:
        years_sorted.append("Unknown")
    report["yearly"] = {
        "labels": [str(y) for y in years_sorted],
        "values": [yearly[y] for y in years_sorted],
    }

    # --- Listening clock (hour of day) ---
    hourly = Counter(s["hour"] for s in dated)
    report["listening_clock"] = {
        "labels": [f"{h:02d}:00" for h in range(24)],
        "values": [hourly.get(h, 0) for h in range(24)],
    }
    log("  Listening clock done")

    # --- Day of week ---
    weekday_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    weekday_counts = Counter(s["weekday"] for s in dated)
    report["weekday"] = {
        "labels": weekday_names,
        "values": [weekday_counts.get(i, 0) for i in range(7)],
    }
    log("  Weekday stats done")

    # --- Discovery timeline ---
    first_listen = {}
    # Process undated first so they get "Unknown", then dated overwrite with real years
    for s in undated:
        if s["artist"] not in first_listen:
            first_listen[s["artist"]] = "Unknown"
    for s in dated:
        if s["artist"] not in first_listen:
            first_listen[s["artist"]] = s["year"]

    new_artists_per_year = Counter(first_listen.values())
    discovery_years = sorted([y for y in new_artists_per_year.keys() if isinstance(y, int)])
    if "Unknown" in new_artists_per_year:
        discovery_years.append("Unknown")
    report["discovery"] = {
        "labels": [str(y) for y in discovery_years],
        "values": [new_artists_per_year[y] for y in discovery_years],
    }
    log("  Discovery timeline done")

    # --- Streaks & Records ---
    daily = Counter(s["date_str"] for s in dated)

    # Biggest day
    biggest_day = daily.most_common(1)[0]
    biggest_day_scrobbles = [s for s in dated if s["date_str"] == biggest_day[0]]
    biggest_day_top_artist = Counter(s["artist"] for s in biggest_day_scrobbles).most_common(1)[0]

    # Most played track in a single day
    daily_tracks = Counter((s["date_str"], s["artist"], s["track"]) for s in dated)
    top_daily_track = daily_tracks.most_common(1)[0]

    # Longest streak of consecutive days with scrobbles
    all_dates = sorted(set(s["date_str"] for s in dated))
    max_streak = 1
    current_streak = 1
    streak_start = all_dates[0]
    best_streak_start = all_dates[0]
    best_streak_end = all_dates[0]

    for i in range(1, len(all_dates)):
        d1 = datetime.strptime(all_dates[i - 1], "%Y-%m-%d")
        d2 = datetime.strptime(all_dates[i], "%Y-%m-%d")
        if (d2 - d1).days == 1:
            current_streak += 1
            if current_streak > max_streak:
                max_streak = current_streak
                best_streak_start = streak_start
                best_streak_end = all_dates[i]
        else:
            current_streak = 1
            streak_start = all_dates[i]

    report["records"] = {
        "biggest_day": {
            "date": biggest_day[0],
            "count": biggest_day[1],
            "top_artist": biggest_day_top_artist[0],
            "top_artist_count": biggest_day_top_artist[1],
        },
        "most_played_track_in_day": {
            "date": top_daily_track[0][0],
            "artist": top_daily_track[0][1],
            "track": top_daily_track[0][2],
            "count": top_daily_track[1],
        },
        "longest_streak": {
            "days": max_streak,
            "start": best_streak_start,
            "end": best_streak_end,
        },
        "total_days_with_scrobbles": len(all_dates),
        "total_days_span": date_range_days,
    }
    log("  Records done")

    # --- Concentration (top N vs rest) — all_time + per-year + periods ---
    conc_all = compute_concentration(scrobbles)
    if conc_all:
        report["concentration"] = {"all_time": conc_all, "by_year": {}, "periods": {}}
        for year in range(first_year, last_year + 1):
            ys = filter_period(scrobbles, year, year)
            if ys:
                yr_conc = compute_concentration(ys)
                if yr_conc:
                    report["concentration"]["by_year"][str(year)] = yr_conc
        if undated:
            ud_conc = compute_concentration(undated)
            if ud_conc:
                report["concentration"]["by_year"]["Unknown"] = ud_conc
        for period in [3, 5, 10]:
            ps = filter_period(scrobbles, current_year - period + 1)
            if ps:
                p_conc = compute_concentration(ps)
                if p_conc:
                    report["concentration"]["periods"][f"last_{period}y"] = p_conc
    log("  Concentration done")

    # --- Loved tracks ---
    loved = [s for s in scrobbles if s["loved"]]
    if loved:
        loved_artists = Counter(s["artist"] for s in loved)
        report["loved"] = {
            "count": len(loved),
            "unique_tracks": len(set((s["artist"], s["track"]) for s in loved)),
            "top_artists": top_n(loved_artists, 10),
        }
    log("  Loved tracks done")

    # --- Hour x Weekday heatmap ---
    heatmap = defaultdict(int)
    for s in dated:
        heatmap[(s["weekday"], s["hour"])] += 1
    report["heatmap"] = {
        "data": [[heatmap.get((wd, h), 0) for h in range(24)] for wd in range(7)],
        "weekdays": weekday_names,
        "hours": [f"{h:02d}" for h in range(24)],
    }
    log("  Heatmap done")

    # --- High School Influence ---
    # Only compute if eras were provided
    hs_era = (eras or {}).get("high_school")
    hs_start = hs_era["start"] if hs_era else None
    hs_end = hs_era["end"] if hs_era else None
    hs_artists = set()
    if hs_start and hs_end:
      for s in scrobbles:
        y = s["year"] if not s["unknown_date"] else None
        if y is not None and (hs_start <= y <= hs_end):
            hs_artists.add(s["artist"])
    # Also count undated/import scrobbles as HS-era
    for s in undated:
        hs_artists.add(s["artist"])
    # And very early imports that got dated as 1970
    if hs_start:
        for s in dated:
            if s["year"] < hs_start:
                hs_artists.add(s["artist"])

    if hs_artists:
        # Per-year HS influence
        hs_by_year = {}
        for year in range(first_year, last_year + 1):
            ys = filter_period(scrobbles, year, year)
            if not ys:
                continue
            total = len(ys)
            from_hs = sum(1 for s in ys if s["artist"] in hs_artists)
            hs_by_year[str(year)] = {
                "total": total,
                "from_hs": from_hs,
                "pct": round(100 * from_hs / total, 1),
            }

        # Post-HS overall and recent
        post_hs = [s for s in dated if s["year"] > hs_end]
        post_hs_from = sum(1 for s in post_hs if s["artist"] in hs_artists)
        recent = [s for s in dated if s["year"] >= current_year - 2]
        recent_from = sum(1 for s in recent if s["artist"] in hs_artists)

        # HS artists still played recently (last 3 years)
        recent_hs_plays = Counter()
        for s in recent:
            if s["artist"] in hs_artists:
                recent_hs_plays[s["artist"]] += 1
        ride_or_die = [{"name": a, "plays": c} for a, c in recent_hs_plays.most_common(20)]

        # Abandoned HS favorites (50+ HS plays, 0 in last 4 years)
        hs_play_counts = Counter()
        recent_4y = set()
        for s in dated:
            if s["year"] <= hs_end or s["year"] < hs_start:
                hs_play_counts[s["artist"]] += 1
            if s["year"] >= current_year - 3:
                recent_4y.add(s["artist"])
        abandoned = [
            {"name": a, "hs_plays": c}
            for a, c in hs_play_counts.most_common(500)
            if c >= 30 and a not in recent_4y and a in hs_artists
        ][:20]

        # Biggest post-HS discoveries
        post_hs_only = set()
        for s in dated:
            if s["year"] > hs_end and s["artist"] not in hs_artists:
                post_hs_only.add(s["artist"])
        discovery_plays = Counter()
        discovery_first = {}
        for s in dated:
            if s["artist"] in post_hs_only:
                discovery_plays[s["artist"]] += 1
                if s["artist"] not in discovery_first or s["year"] < discovery_first[s["artist"]]:
                    discovery_first[s["artist"]] = s["year"]
        biggest_discoveries = [
            {"name": a, "plays": c, "first_year": discovery_first.get(a)}
            for a, c in discovery_plays.most_common(20)
        ]

        # Per-period HS influence
        hs_periods = {}
        for period in [3, 5, 10]:
            ps = filter_period(scrobbles, current_year - period + 1)
            if ps:
                total_p = len(ps)
                from_hs_p = sum(1 for s in ps if s["artist"] in hs_artists)
                hs_periods[f"last_{period}y"] = {
                    "total": total_p,
                    "from_hs": from_hs_p,
                    "pct": round(100 * from_hs_p / total_p, 1),
                }

        report["high_school"] = {
            "hs_years": f"{hs_start}-{hs_end}",
            "hs_artist_count": len(hs_artists),
            "post_hs_pct": round(100 * post_hs_from / len(post_hs), 1) if post_hs else 0,
            "recent_pct": round(100 * recent_from / len(recent), 1) if recent else 0,
            "by_year": hs_by_year,
            "periods": hs_periods,
            "ride_or_die": ride_or_die,
            "abandoned": abandoned,
            "discoveries": biggest_discoveries,
        }
    log("  High school influence done")

    # --- College Influence ---
    col_era = (eras or {}).get("college")
    col_start = col_era["start"] if col_era else None
    col_end = col_era["end"] if col_era else None
    col_artists = set()
    if col_start and col_end:
        for s in dated:
            if col_start <= s["year"] <= col_end:
                col_artists.add(s["artist"])

    # College-only = found in college but NOT in HS
    col_only_artists = col_artists - hs_artists
    # HS + College combined
    hs_col_artists = hs_artists | col_artists

    if col_artists:
        def compute_era_influence(era_artists, era_end_year, label):
            """Compute per-year influence, ride-or-die, abandoned, discoveries for an era."""
            by_year = {}
            for year in range(first_year, last_year + 1):
                ys = filter_period(scrobbles, year, year)
                if not ys:
                    continue
                total = len(ys)
                from_era = sum(1 for s in ys if s["artist"] in era_artists)
                by_year[str(year)] = {
                    "total": total,
                    "from_era": from_era,
                    "pct": round(100 * from_era / total, 1),
                }

            post_era = [s for s in dated if s["year"] > era_end_year]
            post_era_from = sum(1 for s in post_era if s["artist"] in era_artists)
            recent = [s for s in dated if s["year"] >= current_year - 2]
            recent_from = sum(1 for s in recent if s["artist"] in era_artists)

            recent_era_plays = Counter()
            for s in recent:
                if s["artist"] in era_artists:
                    recent_era_plays[s["artist"]] += 1
            ride_or_die = [{"name": a, "plays": c} for a, c in recent_era_plays.most_common(20)]

            era_play_counts = Counter()
            recent_4y = set()
            for s in dated:
                if s["year"] <= era_end_year:
                    if s["artist"] in era_artists:
                        era_play_counts[s["artist"]] += 1
                if s["year"] >= current_year - 3:
                    recent_4y.add(s["artist"])
            abandoned = [
                {"name": a, "era_plays": c}
                for a, c in era_play_counts.most_common(500)
                if c >= 20 and a not in recent_4y and a in era_artists
            ][:20]

            post_era_only = set()
            for s in dated:
                if s["year"] > era_end_year and s["artist"] not in era_artists:
                    post_era_only.add(s["artist"])
            discovery_plays = Counter()
            discovery_first = {}
            for s in dated:
                if s["artist"] in post_era_only:
                    discovery_plays[s["artist"]] += 1
                    if s["artist"] not in discovery_first or s["year"] < discovery_first[s["artist"]]:
                        discovery_first[s["artist"]] = s["year"]
            biggest_discoveries = [
                {"name": a, "plays": c, "first_year": discovery_first.get(a)}
                for a, c in discovery_plays.most_common(20)
            ]

            periods = {}
            for period in [3, 5, 10]:
                ps = filter_period(scrobbles, current_year - period + 1)
                if ps:
                    total_p = len(ps)
                    from_p = sum(1 for s in ps if s["artist"] in era_artists)
                    periods[f"last_{period}y"] = {
                        "total": total_p,
                        "from_era": from_p,
                        "pct": round(100 * from_p / total_p, 1),
                    }

            return {
                "artist_count": len(era_artists),
                "post_era_pct": round(100 * post_era_from / len(post_era), 1) if post_era else 0,
                "recent_pct": round(100 * recent_from / len(recent), 1) if recent else 0,
                "by_year": by_year,
                "periods": periods,
                "ride_or_die": ride_or_die,
                "abandoned": abandoned,
                "discoveries": biggest_discoveries,
            }

        report["college"] = {
            "col_years": f"{col_start}-{col_end}",
            "college_only": compute_era_influence(col_only_artists, col_end, "college_only"),
            "hs_and_college": compute_era_influence(hs_col_artists, col_end, "hs_and_college"),
        }
    log("  College influence done")

    # Add source metadata
    report["source"] = source

    # --- Write output ---
    if output_path:
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False)
        size = len(json.dumps(report))
        log(f"Done! Report written ({size / 1024:.0f} KB)")

    return report


def main():
    """CLI entry point for standalone use."""
    if len(sys.argv) < 2:
        print("Usage: python analyze.py <csv_file> [tags.json] [wikidata.json] [output.json]")
        sys.exit(1)

    csv_file = sys.argv[1]
    tags_file = sys.argv[2] if len(sys.argv) > 2 else None
    wikidata_file = sys.argv[3] if len(sys.argv) > 3 else None
    output_file = sys.argv[4] if len(sys.argv) > 4 else "lastfm_report_data.json"

    # Load optional aliases/overrides from same directory as CSV
    base_dir = os.path.dirname(csv_file)
    aliases = None
    overrides = None
    try:
        with open(os.path.join(base_dir, "artist_aliases.json"), "r", encoding="utf-8") as f:
            aliases = json.load(f)
    except FileNotFoundError:
        pass
    try:
        with open(os.path.join(base_dir, "artist_overrides.json"), "r", encoding="utf-8") as f:
            overrides = json.load(f)
    except FileNotFoundError:
        pass

    run_analysis(csv_file, tags_file, wikidata_file, output_file,
                 aliases=aliases, overrides=overrides)


if __name__ == "__main__":
    main()
