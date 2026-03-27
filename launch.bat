@echo off
cd /d "%~dp0"

:: Check Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo.
    echo   Python 3 is not installed.
    echo.
    echo   Please install it from: https://www.python.org/downloads/
    echo   Make sure to check "Add Python to PATH" during installation.
    echo   Then double-click launch.bat again.
    echo.
    start "" "https://www.python.org/downloads/"
    pause
    exit /b 1
)

echo Installing dependencies...
python -m pip install -q -r requirements.txt
if errorlevel 1 (
    echo.
    echo   Could not install dependencies automatically.
    echo   Try running this in Command Prompt:
    echo.
    echo       pip install Pillow imageio numpy
    echo.
    pause
    exit /b 1
)

echo Starting Music Wrapped...
start "" "http://127.0.0.1:8097"
timeout /t 2 /nobreak >nul
python app.py
pause
