#!/usr/bin/env python3
"""Create an animated GIF with real map tiles, zoomed view, and world minimap."""

import json
import math
import os
import sys
import time
import urllib.request
import urllib.error
from PIL import Image, ImageDraw, ImageFont
import numpy as np
import imageio.v3 as iio

# --- Config ---
WIDTH, HEIGHT = 1080, 1080  # Square for social media
MAIN_MAP_RECT = (0, 100, WIDTH, HEIGHT - 80)  # Main zoomed map area
MINI_X, MINI_Y, MINI_W, MINI_H = WIDTH - 260, HEIGHT - 250, 240, 170  # Minimap corner
BG_COLOR = (13, 17, 23)
TEXT_COLOR = (230, 237, 243)
MUTED_COLOR = (139, 148, 158)
ACCENT = (213, 16, 7)
PINK = (255, 107, 157)

TILE_URL = "https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}.png"
TILE_CACHE_DIR = "/tmp/map_tiles"
TILE_SIZE = 256

CAPITAL_COORDS = {
    "France": (48.86, 2.35), "UK": (51.51, -0.13), "Germany": (52.52, 13.41),
    "Italy": (41.90, 12.50), "Sweden": (59.33, 18.07), "Russia": (55.76, 37.62),
    "Japan": (35.68, 139.65), "Spain": (40.42, -3.70), "Netherlands": (52.37, 4.90),
    "Denmark": (55.68, 12.57), "Finland": (60.17, 24.94), "Australia": (-33.87, 151.21),
    "USA": (38.91, -77.04), "Canada": (45.42, -75.70), "Norway": (59.91, 10.75),
    "Romania": (44.43, 26.10), "Poland": (52.23, 21.01), "Belgium": (50.85, 4.35),
    "Hungary": (47.50, 19.04), "South Korea": (37.57, 126.98), "Ireland": (53.35, -6.26),
    "Greece": (37.98, 23.73), "Ukraine": (50.45, 30.52), "Brazil": (-15.80, -47.89),
    "Israel": (31.77, 35.21), "India": (28.61, 77.21), "China": (39.90, 116.41),
    "Turkey": (39.93, 32.86), "Iceland": (64.15, -21.94), "Estonia": (59.44, 24.75),
    "Czech Republic": (50.08, 14.44), "Serbia": (44.79, 20.45), "Croatia": (45.82, 15.98),
    "Bulgaria": (42.70, 23.32), "Mexico": (19.43, -99.13), "Austria": (48.21, 16.37),
    "Switzerland": (46.95, 7.45), "Portugal": (38.72, -9.14),
}

# --- Tile math ---
def lat_lng_to_tile(lat, lng, zoom):
    """Convert lat/lng to tile coordinates."""
    n = 2 ** zoom
    x = (lng + 180) / 360 * n
    lat_rad = math.radians(lat)
    y = (1 - math.log(math.tan(lat_rad) + 1 / math.cos(lat_rad)) / math.pi) / 2 * n
    return x, y

def lat_lng_to_pixel(lat, lng, zoom, origin_x, origin_y):
    """Convert lat/lng to pixel position relative to a tile origin."""
    tx, ty = lat_lng_to_tile(lat, lng, zoom)
    px = (tx - origin_x) * TILE_SIZE
    py = (ty - origin_y) * TILE_SIZE
    return int(px), int(py)

def fetch_tile(z, x, y):
    """Fetch a single map tile, with caching."""
    cache_path = os.path.join(TILE_CACHE_DIR, f"{z}_{x}_{y}.png")
    if os.path.exists(cache_path):
        return Image.open(cache_path).convert("RGB")

    os.makedirs(TILE_CACHE_DIR, exist_ok=True)
    subdomain = ["a", "b", "c", "d"][(x + y) % 4]
    url = TILE_URL.replace("{s}", subdomain).replace("{z}", str(z)).replace("{x}", str(x)).replace("{y}", str(y))
    req = urllib.request.Request(url, headers={"User-Agent": "LastFMWrapped/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = resp.read()
        with open(cache_path, "wb") as f:
            f.write(data)
        return Image.open(cache_path).convert("RGB")
    except Exception as e:
        # Return dark tile on error
        return Image.new("RGB", (TILE_SIZE, TILE_SIZE), BG_COLOR)

def render_map(center_lat, center_lng, zoom, width, height):
    """Render a map region centered on lat/lng at given zoom."""
    cx, cy = lat_lng_to_tile(center_lat, center_lng, zoom)

    # How many tiles we need
    tiles_x = math.ceil(width / TILE_SIZE) + 2
    tiles_y = math.ceil(height / TILE_SIZE) + 2

    # Tile range
    start_tx = int(cx - tiles_x / 2)
    start_ty = int(cy - tiles_y / 2)

    # Create large canvas from tiles
    canvas_w = tiles_x * TILE_SIZE
    canvas_h = tiles_y * TILE_SIZE
    canvas = Image.new("RGB", (canvas_w, canvas_h), BG_COLOR)

    n = 2 ** zoom
    for dx in range(tiles_x):
        for dy in range(tiles_y):
            tx = (start_tx + dx) % n
            ty = start_ty + dy
            if ty < 0 or ty >= n:
                continue
            tile = fetch_tile(zoom, tx, ty)
            canvas.paste(tile, (dx * TILE_SIZE, dy * TILE_SIZE))

    # Crop to desired size, centered
    offset_px = int((cx - start_tx) * TILE_SIZE - width / 2)
    offset_py = int((cy - start_ty) * TILE_SIZE - height / 2)
    cropped = canvas.crop((offset_px, offset_py, offset_px + width, offset_py + height))

    # Return the map image and a projection function
    def proj(lat, lng):
        tx2, ty2 = lat_lng_to_tile(lat, lng, zoom)
        px = int((tx2 - start_tx) * TILE_SIZE - offset_px)
        py = int((ty2 - start_ty) * TILE_SIZE - offset_py)
        return px, py

    return cropped, proj


def draw_frame(year_label, geo_data, trail_history, all_geo_data, camera):
    """Draw a complete frame."""
    img = Image.new("RGBA", (WIDTH, HEIGHT), BG_COLOR + (255,))
    draw = ImageDraw.Draw(img)

    try:
        font_big = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 52)
        font_med = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 26)
        font_sm = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 18)
        font_xs = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 13)
        font_mini = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 11)
    except Exception:
        font_big = font_med = font_sm = font_xs = font_mini = ImageFont.load_default()

    # Title bar
    draw.text((WIDTH // 2, 15), "Your Musical Center of Gravity", fill=ACCENT, font=font_med, anchor="mt")
    draw.text((WIDTH // 2, 55), str(year_label), fill=TEXT_COLOR, font=font_big, anchor="mt")

    if not geo_data:
        draw.text((WIDTH // 2, HEIGHT // 2), "No data", fill=MUTED_COLOR, font=font_med, anchor="mm")
        return img

    # --- Main zoomed map ---
    map_x, map_y, map_x2, map_y2 = MAIN_MAP_RECT
    map_w, map_h = map_x2 - map_x, map_y2 - map_y

    # Use camera position passed in (stable across frames)
    cam_lat, cam_lng, zoom = camera

    map_img, proj = render_map(cam_lat, cam_lng, zoom, map_w, map_h)
    map_rgba = map_img.convert("RGBA")
    img.paste(map_rgba, (map_x, map_y))
    draw = ImageDraw.Draw(img)

    def map_proj(lat, lng):
        px, py = proj(lat, lng)
        return px + map_x, py + map_y

    # Draw lines from countries to center
    overlay = Image.new("RGBA", (WIDTH, HEIGHT), (0, 0, 0, 0))
    od = ImageDraw.Draw(overlay)
    cx, cy = map_proj(geo_data["center_lat"], geo_data["center_lng"])
    for c in geo_data.get("top_countries", []):
        coords = CAPITAL_COORDS.get(c["name"])
        if not coords:
            continue
        px, py = map_proj(coords[0], coords[1])
        od.line([(px, py), (cx, cy)], fill=PINK + (60,), width=1)
    img = Image.alpha_composite(img, overlay)
    draw = ImageDraw.Draw(img)

    # Country circles
    max_plays = max((c["plays"] for c in geo_data["top_countries"]), default=1)
    for c in geo_data.get("top_countries", []):
        coords = CAPITAL_COORDS.get(c["name"])
        if not coords:
            continue
        px, py = map_proj(coords[0], coords[1])
        if px < map_x - 20 or px > map_x2 + 20 or py < map_y - 20 or py > map_y2 + 20:
            continue
        r = max(5, int(math.sqrt(c["plays"] / max_plays) * 24))
        overlay = Image.new("RGBA", (WIDTH, HEIGHT), (0, 0, 0, 0))
        od = ImageDraw.Draw(overlay)
        od.ellipse([px - r, py - r, px + r, py + r], fill=ACCENT + (120,), outline=ACCENT + (220,), width=2)
        img = Image.alpha_composite(img, overlay)
        draw = ImageDraw.Draw(img)
        # Label
        if r >= 6:
            draw.text((px, py - r - 3), c["name"], fill=TEXT_COLOR, font=font_xs, anchor="mb")

    # Trail on main map
    for i in range(max(0, len(trail_history) - 6), len(trail_history) - 1):
        lat1, lng1 = trail_history[i][:2]
        lat2, lng2 = trail_history[i + 1][:2]
        px1, py1 = map_proj(lat1, lng1)
        px2, py2 = map_proj(lat2, lng2)
        age = len(trail_history) - 1 - i
        alpha = max(40, 200 - age * 35)
        overlay = Image.new("RGBA", (WIDTH, HEIGHT), (0, 0, 0, 0))
        od = ImageDraw.Draw(overlay)
        od.line([(px1, py1), (px2, py2)], fill=PINK[:3] + (alpha,), width=2)
        # Small dot at each past point
        od.ellipse([px1 - 3, py1 - 3, px1 + 3, py1 + 3], fill=PINK[:3] + (alpha,))
        img = Image.alpha_composite(img, overlay)
        draw = ImageDraw.Draw(img)

    # Center marker (glowing)
    cx, cy = map_proj(geo_data["center_lat"], geo_data["center_lng"])
    for gr in range(22, 6, -2):
        alpha = int(12 * (22 - gr) / 16)
        overlay = Image.new("RGBA", (WIDTH, HEIGHT), (0, 0, 0, 0))
        od = ImageDraw.Draw(overlay)
        od.ellipse([cx - gr, cy - gr, cx + gr, cy + gr], fill=PINK + (alpha,))
        img = Image.alpha_composite(img, overlay)
    draw = ImageDraw.Draw(img)
    draw.ellipse([cx - 8, cy - 8, cx + 8, cy + 8], fill=PINK)
    draw.ellipse([cx - 8, cy - 8, cx + 8, cy + 8], outline=(255, 255, 255), width=3)

    # City name on main map
    draw.text((cx + 14, cy - 2), geo_data["nearest_city"], fill=(255, 255, 255), font=font_med, anchor="lm")

    # --- World minimap (bottom-right corner) ---
    mini_img, mini_proj = render_map(30, 15, 1, MINI_W, MINI_H)
    mini_rgba = mini_img.convert("RGBA")

    # Darken minimap slightly
    dark_overlay = Image.new("RGBA", (MINI_W, MINI_H), (0, 0, 0, 60))
    mini_rgba = Image.alpha_composite(mini_rgba, dark_overlay)

    # Draw center dot on minimap
    mini_draw = ImageDraw.Draw(mini_rgba)
    mpx, mpy = mini_proj(geo_data["center_lat"], geo_data["center_lng"])
    mpx = max(4, min(MINI_W - 4, mpx))
    mpy = max(4, min(MINI_H - 4, mpy))
    mini_draw.ellipse([mpx - 5, mpy - 5, mpx + 5, mpy + 5], fill=PINK, outline=(255, 255, 255), width=2)

    # Draw trail on minimap
    for i in range(max(0, len(trail_history) - 10), len(trail_history) - 1):
        lat1, lng1 = trail_history[i][:2]
        lat2, lng2 = trail_history[i + 1][:2]
        p1x, p1y = mini_proj(lat1, lng1)
        p2x, p2y = mini_proj(lat2, lng2)
        mini_draw.line([(p1x, p1y), (p2x, p2y)], fill=PINK + (100,), width=1)

    # Border for minimap
    mini_draw.rectangle([0, 0, MINI_W - 1, MINI_H - 1], outline=(48, 54, 61), width=2)

    img.paste(mini_rgba, (MINI_X, MINI_Y), mini_rgba)
    draw = ImageDraw.Draw(img)

    # --- Bottom stats ---
    stats = f"{geo_data['nearest_city']}  ·  {geo_data['mapped_plays']:,} plays  ·  {geo_data['coverage_pct']}% mapped"
    draw.text((WIDTH // 2, HEIGHT - 25), stats, fill=MUTED_COLOR, font=font_sm, anchor="mb")

    return img


def generate(report_file, output_file):
    with open(report_file, "r", encoding="utf-8") as f:
        report = json.load(f)

    geo = report.get("geographic_center")
    if not geo:
        raise ValueError("No geographic_center data in report")

    by_year = geo.get("by_year", {})
    years = sorted([y for y in by_year.keys() if y != "Unknown"])

    all_time = geo.get("all_time", {})
    all_lats = [d["center_lat"] for d in by_year.values()]
    all_lngs = [d["center_lng"] for d in by_year.values()]
    cam_lat = (max(all_lats) + min(all_lats)) / 2
    cam_lng = (max(all_lngs) + min(all_lngs)) / 2
    cam_zoom = 5
    camera = (cam_lat, cam_lng, cam_zoom)

    frames = []
    trail_history = []

    for yi, year in enumerate(years):
        year_data = by_year.get(year)
        if year_data:
            trail_history.append((year_data["center_lat"], year_data["center_lng"], year))
        frame = draw_frame(year, year_data, trail_history, geo, camera)
        rgb = Image.new("RGB", frame.size, BG_COLOR)
        rgb.paste(frame, mask=frame.split()[3])
        frames.append(rgb)

    if all_time:
        trail_history.append((all_time["center_lat"], all_time["center_lng"], "All"))
        for _ in range(5):
            frame = draw_frame("All Time", all_time, trail_history, geo, camera)
            rgb = Image.new("RGB", frame.size, BG_COLOR)
            rgb.paste(frame, mask=frame.split()[3])
            frames.append(rgb)

    iio.imwrite(output_file, [np.array(f) for f in frames], duration=900, loop=0)


def main():
    report_file = sys.argv[1] if len(sys.argv) > 1 else "lastfm_report_data.json"
    output_file = sys.argv[2] if len(sys.argv) > 2 else "geo_center_animation.gif"
    generate(report_file, output_file)
    print(f"Done! {output_file} ({os.path.getsize(output_file)//1024} KB)")


if __name__ == "__main__":
    main()
