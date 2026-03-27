#!/usr/bin/env bash
# Music Wrapped launcher — Mac/Linux

DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

# Check Python 3 is installed
if ! command -v python3 &>/dev/null; then
    echo ""
    echo "  Python 3 is not installed."
    echo ""
    echo "  Please install it from: https://www.python.org/downloads/"
    echo "  Then double-click launch.command again."
    echo ""
    read -p "Press Enter to open that page in your browser..."
    open "https://www.python.org/downloads/" 2>/dev/null || xdg-open "https://www.python.org/downloads/" 2>/dev/null
    exit 1
fi

PY_VER=$(python3 -c "import sys; print(sys.version_info.major * 10 + sys.version_info.minor)")
if [ "$PY_VER" -lt 38 ]; then
    echo ""
    echo "  Python 3.8 or newer is required."
    echo "  You have: $(python3 --version)"
    echo "  Please update from: https://www.python.org/downloads/"
    echo ""
    read -p "Press Enter to open that page in your browser..."
    open "https://www.python.org/downloads/" 2>/dev/null || xdg-open "https://www.python.org/downloads/" 2>/dev/null
    exit 1
fi

echo "Installing dependencies..."
python3 -m pip install -q -r requirements.txt 2>/dev/null
if [ $? -ne 0 ]; then
    echo ""
    echo "  Could not install dependencies automatically."
    echo "  Try running this in Terminal:"
    echo ""
    echo "      pip3 install Pillow imageio numpy"
    echo ""
    read -p "Press Enter to close..."
    exit 1
fi

echo "Starting Music Wrapped..."
# Open browser after short delay
(sleep 1.5 && (open "http://127.0.0.1:8097" 2>/dev/null || xdg-open "http://127.0.0.1:8097" 2>/dev/null)) &
python3 app.py
