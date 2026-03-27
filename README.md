# Music Wrapped

Your listening history, analysed. Supports Last.fm profiles and Spotify extended streaming history exports.

## Features

- **Last.fm**: Enter your username (fetches via API) or upload a CSV export
- **Spotify**: Upload your extended streaming history JSON files
- Top artists, tracks, and albums across all time and by year
- Genre breakdown using Last.fm tag data
- Gender and country distribution of your listening
- Geographic center of gravity with an animated GIF showing how it's shifted over time
- Listening heatmap by hour and day of week
- Artist discovery timeline
- Listening concentration (how obsessive vs. eclectic you are)
- Optional: high school / college era influence analysis

## Setup

```bash
pip install -r requirements.txt
export LASTFM_API_KEY=your_key_here  # get one free at https://www.last.fm/api/account/create
python app.py
```

Then open **http://127.0.0.1:8097** in your browser.

The API key is required for Last.fm username lookups and genre tag enrichment. Spotify CSV uploads work without it, but genre and country data will be limited.

## Getting your data

**Last.fm**: Just enter your username — or export your history as CSV from [benjaminbenben.com/lastfm-to-csv](https://benjaminbenben.com/lastfm-to-csv/).

**Spotify**: Go to [spotify.com/account/privacy](https://www.spotify.com/us/account/privacy/), request your **Extended streaming history** (not Account data), wait for the email (up to 30 days), then upload the `Streaming_History_Audio_*.json` files.
