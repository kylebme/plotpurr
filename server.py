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
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from urllib.parse import urlparse
from tkinter import Tk, filedialog
import chdb
import webbrowser

# Configuration
HOST = "localhost"
PORT = 8765
PARQUET_DIR = Path(".").resolve()
LOG_LEVEL = logging.INFO
SELECTED_FILES = []  # type: list[Path]
LAST_DIALOG_DIR = PARQUET_DIR

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


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

        if path == "/api/select-paths":
            self._handle_select_paths()
            return

        if path == "/api/sql":
            self._handle_sql()
            return

        self._send_error(404, "Endpoint not found")

    # ---------- Endpoint handlers ----------

    def _handle_select_paths(self):
        """Open a native file/folder dialog (tkinter) and update selected parquet files."""
        global SELECTED_FILES, LAST_DIALOG_DIR

        try:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length)
            payload = json.loads(raw.decode("utf-8") or "{}")
        except (ValueError, json.JSONDecodeError):
            payload = {}

        mode = str(payload.get("mode") or "files").lower()
        if mode not in ("files", "directory"):
            self._send_error(400, "Invalid mode. Use 'files' or 'directory'.")
            return

        try:
            selected, selected_dir = _open_parquet_dialog(mode=mode, initial_dir=LAST_DIALOG_DIR)
        except Exception as exc:
            logger.exception("Failed to open selection dialog")
            self._send_error(500, f"Unable to open selection dialog: {exc}")
            return

        made_selection = selected_dir is not None or bool(selected)

        if made_selection:
            SELECTED_FILES = selected
            if selected_dir:
                LAST_DIALOG_DIR = selected_dir
            logger.info("Selected %d parquet files", len(SELECTED_FILES))
        else:
            logger.info("No selection made; keeping previous parquet files")

        self._send_json(
            {
                "files": _serialize_files(SELECTED_FILES),
                "selection_mode": mode,
                "updated": made_selection,
            }
        )

    def _handle_list_files(self):
        """Return basic info about selected .parquet files (empty until chosen)."""
        self._send_json({"files": _serialize_files(SELECTED_FILES)})

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


def _open_parquet_dialog(mode="files", initial_dir=Path.home()):
    """Open a native selection dialog for parquet files or a directory.

    Returns a tuple of (paths, selected_dir). selected_dir is None when the user cancels.
    """
    root = Tk()
    root.withdraw()
    try:
        # Keep the dialog on top and focused so it is visible immediately.
        root.attributes("-topmost", True)
        root.lift()
        root.focus_force()
        root.update_idletasks()
        root.update()
    except Exception:
        # attributes may not be supported in some environments; ignore.
        pass

    try:
        root.update()
        if mode == "directory":
            directory = filedialog.askdirectory(
                title="Select folder containing Parquet files", initialdir=str(initial_dir)
            )
            if not directory:
                return [], None
            dir_path = Path(directory).expanduser().resolve()
            if not dir_path.is_dir():
                return [], None
            return sorted(p for p in dir_path.glob("*.parquet") if p.is_file()), dir_path

        file_paths = filedialog.askopenfilenames(
            title="Select Parquet file(s)",
            filetypes=[("Parquet files", "*.parquet")],
            initialdir=str(initial_dir),
        )
        paths = []
        seen = set()
        for raw in file_paths or []:
            if not raw:
                continue
            path = Path(raw).expanduser().resolve()
            if path.suffix.lower() != ".parquet" or not path.is_file():
                continue
            key = str(path)
            if key in seen:
                continue
            seen.add(key)
            paths.append(path)
        selected_dir = Path(file_paths[0]).expanduser().resolve().parent if file_paths else None
        return paths, selected_dir
    finally:
        try:
            root.destroy()
        except Exception:
            pass


def _serialize_files(files):
    serialized = []
    for path in files:
        try:
            stat = path.stat()
            serialized.append(
                {
                    "name": path.name,
                    "path": str(path),
                    "size_bytes": stat.st_size,
                    "size_mb": round(stat.st_size / (1024 * 1024), 2),
                }
            )
        except OSError as exc:
            logger.warning("Error reading file %s: %s", path, exc)
    return serialized


def main():
    server_address = (HOST, PORT)
    httpd = HTTPServer(server_address, ChDBRequestHandler)
    logger.info("Serving %s on http://%s:%d", PARQUET_DIR, HOST, PORT)
    webbrowser.open(f"http://{HOST}:{PORT}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down server...")
        httpd.server_close()


if __name__ == "__main__":
    main()
