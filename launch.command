#!/usr/bin/env bash
# Music Wrapped launcher — Mac/Linux
set -e

DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

echo "Checking dependencies..."
python3 -m pip install -q -r requirements.txt 2>/dev/null || {
    echo "Could not install dependencies automatically."
    echo "Run: pip3 install -r requirements.txt"
    exit 1
}

echo "Starting Music Wrapped at http://127.0.0.1:8097 ..."
# Open browser after short delay
(sleep 1.5 && open "http://127.0.0.1:8097") &
python3 app.py
