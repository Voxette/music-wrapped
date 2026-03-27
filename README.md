# Music Wrapped

Your listening history, analysed. Connect Last.fm or upload a Spotify export to see your top artists, genres, where your music comes from geographically, how your taste has evolved year by year, and more.

## Download & Run

1. **[Download the latest release](https://github.com/Voxette/music-wrapped/releases/latest)** and unzip it
2. Make sure **Python 3** is installed ([python.org](https://www.python.org/downloads/))
3. Run the launcher:
   - **Mac/Linux**: double-click `launch.sh`, or run `./launch.sh` in Terminal
   - **Windows**: double-click `launch.bat`
4. Your browser will open automatically at `http://127.0.0.1:8097`

## Getting your data

**Last.fm** — just enter your username. You'll need a free API key from [last.fm/api/account/create](https://www.last.fm/api/account/create) (takes 30 seconds to register).

**Spotify** — go to [spotify.com/account/privacy](https://www.spotify.com/us/account/privacy/), scroll down and request your **Extended streaming history**. Spotify emails you a download link within a few days (up to 30 days). Upload the `Streaming_History_Audio_*.json` files from that zip.

## What you get

- Top artists, tracks, and albums — all time and by year
- Genre breakdown
- Gender and country distribution of the artists you listen to
- Geographic centre of gravity, with an animated map showing how it's shifted over time
- Listening heatmap by hour and day of week
- Artist discovery timeline
- Listening concentration — how obsessive vs. eclectic you are
- Optional: high school and college era influence (how much of what you listened to then you still listen to now)
