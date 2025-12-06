#!/usr/bin/env python3
"""
Very thin DuckDB backend:
- Serves index.html and other static files from the current directory
- Lists parquet files
- Executes SQL provided by the frontend and returns (columns, rows)
All query logic and SQL text live in the frontend.
"""

import json
import logging
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from urllib.parse import urlparse
import duckdb
import webbrowser

# Configuration
HOST = "localhost"
PORT = 8765
PARQUET_DIR = Path(".").resolve()
LOG_LEVEL = logging.INFO

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class DuckDBRequestHandler(SimpleHTTPRequestHandler):
    """
    HTTP handler that:
    - Serves static files (index.html, parquet, etc.)
    - GET /api/files -> list of parquet files (no SQL)
    - POST /api/sql  -> execute arbitrary SQL and return rows
    """

    db = None  # shared DuckDB connection for all requests

    @classmethod
    def init_db(cls):
        """Initialise the DuckDB connection once."""
        if cls.db is None:
            logger.info("Initialising DuckDB in-memory database")
            cls.db = duckdb.connect(":memory:")
            # Basic performance tuning; adjust as needed
            cls.db.execute("SET threads TO 8")
            cls.db.execute("SET memory_limit = '4GB'")

    def __init__(self, *args, **kwargs):
        # Ensure DB is initialised
        self.init_db()
        # Serve files from PARQUET_DIR
        super().__init__(*args, directory=str(PARQUET_DIR), **kwargs)

    # ---------- Helpers ----------

    def log_message(self, format, *args):
        logger.info("%s - %s", self.address_string(), format % args)

    def _send_cors_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def _send_json(self, data, status=200):
        body = json.dumps(data, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self._send_cors_headers()
        self.end_headers()
        self.wfile.write(body)

    def _send_error(self, status, message):
        logger.error("Error %s: %s", status, message)
        self._send_json({"error": message}, status=status)

    # ---------- HTTP methods ----------

    def do_OPTIONS(self):
        self.send_response(200)
        self._send_cors_headers()
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/api/files":
            self._handle_list_files()
            return

        # Default: static files (and index.html at "/")
        if path == "/":
            self.path = "/index.html"
        return super().do_GET()

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/api/sql":
            self._handle_sql()
            return

        self._send_error(404, "Endpoint not found")

    # ---------- Endpoint handlers ----------

    def _handle_list_files(self):
        """Return basic info about all .parquet files in PARQUET_DIR."""
        files = []
        for f in PARQUET_DIR.glob("*.parquet"):
            try:
                stat = f.stat()
                files.append(
                    {
                        "name": f.name,
                        "path": str(f),
                        "size_bytes": stat.st_size,
                        "size_mb": round(stat.st_size / (1024 * 1024), 2),
                    }
                )
            except OSError as exc:
                logger.warning("Error reading file %s: %s", f, exc)

        self._send_json({"files": files})

    def _handle_sql(self):
        """Execute arbitrary SQL and return columns + rows as JSON."""
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            self._send_error(400, "Invalid Content-Length")
            return

        try:
            raw = self.rfile.read(length)
            payload = json.loads(raw.decode("utf-8") or "{}")
        except json.JSONDecodeError:
            self._send_error(400, "Invalid JSON")
            return

        query = payload.get("query")
        params = payload.get("params") or []

        if not isinstance(query, str) or not query.strip():
            self._send_error(400, "Missing 'query' string in request body")
            return

        try:
            logger.debug("Executing SQL: %s", query[:500])
            cur = self.db.execute(query, params)

            if cur.description is None:
                # Non-SELECT statement
                self._send_json({"columns": [], "rows": []})
                return

            columns = [col[0] for col in cur.description]
            rows = cur.fetchall()

            self._send_json({"columns": columns, "rows": rows})
        except Exception as exc:
            logger.exception("SQL execution failed")
            self._send_error(500, str(exc))


def main():
    server_address = (HOST, PORT)
    httpd = HTTPServer(server_address, DuckDBRequestHandler)
    logger.info("Serving %s on http://%s:%d", PARQUET_DIR, HOST, PORT)
    webbrowser.open(f"http://{HOST}:{PORT}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down server...")
        httpd.server_close()


if __name__ == "__main__":
    main()
