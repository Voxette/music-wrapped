@echo off
cd /d "%~dp0"
echo Checking dependencies...
python -m pip install -q -r requirements.txt
echo Starting Music Wrapped...
start "" "http://127.0.0.1:8097"
timeout /t 2 /nobreak >nul
python app.py
pause
