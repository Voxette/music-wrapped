#!/usr/bin/env python3
"""Music Wrapped — Flask server with SSE progress reporting."""

import json
import os
import queue
import shutil
import sys
import time
import uuid
from datetime import datetime, timedelta
from http.server import HTTPServer, SimpleHTTPRequestHandler
from io import BytesIO
import threading
import cgi

from pipeline import run_pipeline_thread

# Config
PORT = 8097
LASTFM_API_KEY = os.environ.get("LASTFM_API_KEY", "")

# When bundled with PyInstaller, read-only assets are in sys._MEIPASS.
# Writable data (sessions, cache) must live next to the executable.
if getattr(sys, "frozen", False):
    _BUNDLE_DIR = sys._MEIPASS
    _EXEC_DIR = os.path.dirname(sys.executable)
else:
    _BUNDLE_DIR = os.path.dirname(os.path.abspath(__file__))
    _EXEC_DIR = _BUNDLE_DIR

STATIC_DIR = os.path.join(_BUNDLE_DIR, "static")
DATA_DIR = os.path.join(_EXEC_DIR, "data")
CACHE_DIR = os.path.join(DATA_DIR, "cache")
SESSIONS_DIR = os.path.join(DATA_DIR, "sessions")

# In-memory session tracking
sessions = {}  # session_id -> {"queue": Queue, "thread": Thread, "created": datetime}


def cleanup_old_sessions(max_age_hours=24):
    """Remove sessions older than max_age_hours."""
    cutoff = datetime.now() - timedelta(hours=max_age_hours)
    for sid in list(sessions.keys()):
        if sessions[sid]["created"] < cutoff:
            session_dir = os.path.join(SESSIONS_DIR, sid)
            if os.path.exists(session_dir):
                shutil.rmtree(session_dir, ignore_errors=True)
            del sessions[sid]


class MusicWrappedHandler(SimpleHTTPRequestHandler):
    """HTTP handler for Music Wrapped app."""

    def log_message(self, format, *args):
        pass  # Suppress default logging

    def do_GET(self):
        if self.path == "/" or self.path == "/index.html":
            self.serve_file(os.path.join(STATIC_DIR, "index.html"), "text/html")
        elif self.path.startswith("/progress/"):
            self.handle_sse()
        elif self.path.startswith("/results/"):
            self.serve_file(os.path.join(STATIC_DIR, "results.html"), "text/html")
        elif self.path.startswith("/session/") and self.path.endswith("/report.json"):
            sid = self.path.split("/")[2]
            report_path = os.path.join(SESSIONS_DIR, sid, "report.json")
            if os.path.exists(report_path):
                self.serve_file(report_path, "application/json")
            else:
                self.send_error(404, "Report not ready yet")
        elif self.path.startswith("/generate-gif/"):
            self.handle_generate_gif()
        elif self.path.startswith("/session/") and self.path.endswith("/geo_animation.gif"):
            sid = self.path.split("/")[2]
            gif_path = os.path.join(SESSIONS_DIR, sid, "geo_animation.gif")
            if os.path.exists(gif_path):
                self.send_response(200)
                self.send_header("Content-Type", "image/gif")
                self.send_header("Content-Disposition", "attachment; filename=music_wrapped_geo.gif")
                with open(gif_path, "rb") as f:
                    data = f.read()
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
            else:
                self.send_error(404, "GIF not generated yet")
        else:
            self.send_error(404)

    def do_POST(self):
        if self.path == "/start/lastfm":
            self.handle_start_lastfm()
        elif self.path == "/start/spotify":
            self.handle_start_spotify()
        else:
            self.send_error(404)

    def serve_file(self, path, content_type):
        try:
            with open(path, "rb") as f:
                data = f.read()
            self.send_response(200)
            self.send_header("Content-Type", f"{content_type}; charset=utf-8")
            self.send_header("Content-Length", len(data))
            self.end_headers()
            self.wfile.write(data)
        except FileNotFoundError:
            self.send_error(404)

    def read_body(self):
        length = int(self.headers.get("Content-Length", 0))
        return self.rfile.read(length)

    def json_response(self, data, status=200):
        body = json.dumps(data).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)

    def handle_start_lastfm(self):
        """Handle Last.fm start — username or CSV upload."""
        content_type = self.headers.get("Content-Type", "")
        cleanup_old_sessions()

        session_id = str(uuid.uuid4())[:8]
        session_dir = os.path.join(SESSIONS_DIR, session_id)
        os.makedirs(session_dir, exist_ok=True)

        pq = queue.Queue()
        sessions[session_id] = {"queue": pq, "thread": None, "created": datetime.now()}

        if "multipart/form-data" in content_type:
            # CSV file upload
            environ = {
                "REQUEST_METHOD": "POST",
                "CONTENT_TYPE": content_type,
                "CONTENT_LENGTH": self.headers.get("Content-Length"),
            }
            form = cgi.FieldStorage(fp=self.rfile, headers=self.headers, environ=environ)
            file_item = form["file"]
            csv_path = os.path.join(session_dir, "upload.csv")
            with open(csv_path, "wb") as f:
                f.write(file_item.file.read())

            # Prefer client-provided API key over server default
            client_key = form.getfirst("api_key", "")
            api_key = client_key or LASTFM_API_KEY
            eras_json = form.getfirst("eras", "")
            eras = json.loads(eras_json) if eras_json else None

            t = run_pipeline_thread(
                session_id, "lastfm_csv", csv_path, pq,
                cache_dir=CACHE_DIR, session_dir=session_dir, api_key=api_key, eras=eras,
            )
        else:
            # Username JSON body
            body = json.loads(self.read_body())
            username = body.get("username", "").strip()
            if not username:
                self.json_response({"error": "Username required"}, 400)
                return
            # Prefer client-provided API key over server default
            api_key = body.get("api_key", "").strip() or LASTFM_API_KEY
            if not api_key:
                self.json_response({"error": "No API key provided. Enter your Last.fm API key above."}, 400)
                return
            eras = body.get("eras")

            t = run_pipeline_thread(
                session_id, "lastfm_username", username, pq,
                cache_dir=CACHE_DIR, session_dir=session_dir, api_key=api_key, eras=eras,
            )

        sessions[session_id]["thread"] = t
        self.json_response({"session_id": session_id})

    def handle_start_spotify(self):
        """Handle Spotify JSON/ZIP upload."""
        content_type = self.headers.get("Content-Type", "")
        cleanup_old_sessions()

        session_id = str(uuid.uuid4())[:8]
        session_dir = os.path.join(SESSIONS_DIR, session_id)
        os.makedirs(session_dir, exist_ok=True)

        pq = queue.Queue()
        sessions[session_id] = {"queue": pq, "thread": None, "created": datetime.now()}

        if "multipart/form-data" not in content_type:
            self.json_response({"error": "File upload required"}, 400)
            return

        environ = {
            "REQUEST_METHOD": "POST",
            "CONTENT_TYPE": content_type,
            "CONTENT_LENGTH": self.headers.get("Content-Length"),
        }
        form = cgi.FieldStorage(fp=self.rfile, headers=self.headers, environ=environ)

        # Handle multiple files
        file_items = form["files"] if isinstance(form["files"], list) else [form["files"]]
        saved_paths = []
        for item in file_items:
            if hasattr(item, 'filename') and item.filename:
                filename = os.path.basename(item.filename)
                save_path = os.path.join(session_dir, filename)
                with open(save_path, "wb") as f:
                    f.write(item.file.read())
                saved_paths.append(save_path)

        # Prefer client-provided API key over server default
        client_key = form.getfirst("api_key", "")
        api_key = client_key or LASTFM_API_KEY
        eras_json = form.getfirst("eras", "")
        eras = json.loads(eras_json) if eras_json else None

        t = run_pipeline_thread(
            session_id, "spotify", saved_paths, pq,
            cache_dir=CACHE_DIR, session_dir=session_dir, api_key=api_key, eras=eras,
        )
        sessions[session_id]["thread"] = t
        self.json_response({"session_id": session_id})

    def handle_generate_gif(self):
        """Generate geo center animation GIF for a session."""
        session_id = self.path.split("/")[-1]
        session_dir = os.path.join(SESSIONS_DIR, session_id)
        report_path = os.path.join(session_dir, "report.json")
        gif_path = os.path.join(session_dir, "geo_animation.gif")

        if not os.path.exists(report_path):
            self.json_response({"error": "Report not found"}, 404)
            return

        if os.path.exists(gif_path):
            # Already generated
            self.json_response({"status": "ready", "url": f"/session/{session_id}/geo_animation.gif"})
            return

        try:
            import make_geo_animation
            make_geo_animation.generate(report_path, gif_path)
            self.json_response({"status": "ready", "url": f"/session/{session_id}/geo_animation.gif"})
        except Exception as e:
            self.json_response({"error": f"GIF generation failed: {str(e)}"}, 500)

    def handle_sse(self):
        """Server-Sent Events endpoint for progress."""
        session_id = self.path.split("/")[-1]
        if session_id not in sessions:
            self.send_error(404, "Session not found")
            return

        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()

        pq = sessions[session_id]["queue"]
        try:
            while True:
                try:
                    event = pq.get(timeout=30)
                    data = json.dumps(event)
                    self.wfile.write(f"data: {data}\n\n".encode("utf-8"))
                    self.wfile.flush()

                    if event.get("stage") in ("complete", "error"):
                        break
                except queue.Empty:
                    # Send keepalive
                    self.wfile.write(b": keepalive\n\n")
                    self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass


class ThreadedHTTPServer(HTTPServer):
    """Handle requests in threads."""
    allow_reuse_address = True

    def process_request(self, request, client_address):
        t = threading.Thread(target=self._handle, args=(request, client_address))
        t.daemon = True
        t.start()

    def _handle(self, request, client_address):
        try:
            self.finish_request(request, client_address)
        except Exception:
            self.handle_error(request, client_address)
        finally:
            self.shutdown_request(request)


def main():
    os.makedirs(CACHE_DIR, exist_ok=True)
    os.makedirs(SESSIONS_DIR, exist_ok=True)

    if not LASTFM_API_KEY:
        print("⚠ No LASTFM_API_KEY set — username fetch and tag enrichment will be disabled")
        print("  Set it: export LASTFM_API_KEY=your_key_here")

    server = ThreadedHTTPServer(("127.0.0.1", PORT), MusicWrappedHandler)
    print(f"Music Wrapped running at http://127.0.0.1:{PORT}")
    print(f"Cache dir: {CACHE_DIR}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.shutdown()


if __name__ == "__main__":
    main()
