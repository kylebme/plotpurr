#!/usr/bin/env python3
"""
Very thin ChDB (ClickHouse) backend:
- Serves index.html and other static files from the current directory
- Lists parquet files
- Executes SQL provided by the frontend and returns (columns, rows)
All query logic and SQL text live in the frontend.
"""

import json
import logging
import os
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from urllib.parse import urlparse
import chdb
import webbrowser

# Configuration
HOST = "localhost"
PORT = 8765
PARQUET_DIR = Path(".").resolve()
LOG_LEVEL = logging.INFO

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SELECTED_PATHS = []


class ChDBRequestHandler(SimpleHTTPRequestHandler):
    """
    HTTP handler that:
    - Serves static files (index.html, parquet, etc.)
    - GET /api/files -> list of parquet files (no SQL)
    - POST /api/sql  -> execute arbitrary SQL and return rows
    """

    def __init__(self, *args, **kwargs):
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
        if path == "/api/set_paths":
            self._handle_set_paths()
            return

        self._send_error(404, "Endpoint not found")

    # ---------- Endpoint handlers ----------

    def _collect_parquet_files(self):
        """Return a list of Path objects for configured parquet files."""
        paths = SELECTED_PATHS or [str(p) for p in PARQUET_DIR.glob("*.parquet")]

        files = []
        seen = set()
        for raw in paths:
            p = Path(raw).expanduser().resolve()
            if p in seen:
                continue
            if p.is_dir():
                for child in sorted(p.glob("*.parquet")):
                    if child not in seen:
                        files.append(child)
                        seen.add(child)
            elif p.is_file() and p.suffix.lower() == ".parquet":
                files.append(p)
                seen.add(p)
        return files

    def _handle_list_files(self):
        """Return basic info about parquet files from the selected paths or default directory."""
        files = []
        for f in self._collect_parquet_files():
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

    def _handle_set_paths(self):
        """Configure which parquet files/directories should be exposed."""
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

        paths = payload.get("paths")
        if paths is None:
            self._send_error(400, "Missing 'paths' array")
            return

        if not isinstance(paths, list):
            self._send_error(400, "'paths' must be an array")
            return

        normalized = []
        for p in paths:
            try:
                normalized.append(str(Path(p).expanduser().resolve()))
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning("Failed to normalize path %s: %s", p, exc)

        global SELECTED_PATHS
        SELECTED_PATHS = normalized
        logger.info("Updated parquet search paths: %s", SELECTED_PATHS)
        self._send_json({"ok": True, "count": len(SELECTED_PATHS)})

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

        if params:
            # ChDB bindings currently do not accept positional parameters; ignore if present.
            logger.warning("Query parameters were provided but are not supported with ChDB; ignoring.")

        try:
            prepared_query = str(query).strip()
            logger.debug("Executing SQL: %s", prepared_query[:500])
            raw = chdb.query(prepared_query, "JSON")

            # chdb returns a query_result with a .data() method that yields the output string.
            data_attr = getattr(raw, "data", None)
            if callable(data_attr):
                result_str = data_attr()
            elif isinstance(data_attr, str):
                result_str = data_attr
            else:
                # Fallback to repr if an unexpected type is returned.
                result_str = str(raw)

            parsed = json.loads(result_str or "{}")

            meta = parsed.get("meta") or []
            data = parsed.get("data") or []
            columns = [col.get("name") for col in meta if isinstance(col, dict)]

            rows = []
            if columns and isinstance(data, list):
                for row in data:
                    if isinstance(row, dict):
                        rows.append([row.get(col) for col in columns])
                    else:
                        rows.append(row)
            self._send_json({"columns": columns, "rows": rows})
        except Exception as exc:
            logger.exception("SQL execution failed")
            self._send_error(500, str(exc))


def main():
    server_address = (HOST, PORT)
    httpd = HTTPServer(server_address, ChDBRequestHandler)
    logger.info("Serving %s on http://%s:%d", PARQUET_DIR, HOST, PORT)
    if not os.environ.get("NO_BROWSER"):
        webbrowser.open(f"http://{HOST}:{PORT}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down server...")
        httpd.server_close()


if __name__ == "__main__":
    main()
