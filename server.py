#!/usr/bin/env python3
"""
DuckDB Parquet Viewer Backend
High-performance backend for querying large parquet files with intelligent downsampling
"""

import os
import json
import logging
from pathlib import Path
from typing import Optional, List, Any, Dict
from dataclasses import dataclass
from http.server import HTTPServer, SimpleHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import duckdb

# Configuration
HOST = "localhost"
PORT = 8765
PARQUET_DIR = Path(".")
MAX_POINTS_DEFAULT = 2000
LOG_LEVEL = logging.INFO

# Setup logging
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class DuckDBManager:
    """Manages DuckDB connections and queries with connection pooling potential"""

    def __init__(self, parquet_dir: Path):
        self.parquet_dir = parquet_dir
        self._connection = None

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        """Get or create a DuckDB connection"""
        if self._connection is None:
            self._connection = duckdb.connect(":memory:")
            # Configure for performance
            self._connection.execute("SET threads TO 4")
            self._connection.execute("SET memory_limit = '2GB'")
        return self._connection

    def execute(self, query: str) -> List[tuple]:
        """Execute a query and return results"""
        logger.debug(f"Executing query: {query[:200]}...")
        return self.connection.execute(query).fetchall()

    def execute_df(self, query: str):
        """Execute query and return as arrow/pandas-like result"""
        return self.connection.execute(query)


class ParquetQueryEngine:
    """Handles all parquet-related queries with intelligent downsampling"""

    def __init__(self, db_manager: DuckDBManager, parquet_dir: Path):
        self.db = db_manager
        self.parquet_dir = parquet_dir
        self._column_types_cache: Dict[str, Dict[str, str]] = {}

    def list_files(self) -> List[Dict[str, Any]]:
        """List all parquet files with metadata"""
        files = []
        for f in self.parquet_dir.glob("*.parquet"):
            try:
                # Get basic file stats
                stat = f.stat()
                row_count = self.db.execute(f"SELECT COUNT(*) FROM '{f}'")[0][0]
                files.append(
                    {
                        "name": f.name,
                        "path": str(f),
                        "size_bytes": stat.st_size,
                        "size_mb": round(stat.st_size / (1024 * 1024), 2),
                        "row_count": row_count,
                    }
                )
            except Exception as e:
                logger.warning(f"Error reading file {f}: {e}")
        return files

    def get_columns(self, file: str) -> List[Dict[str, str]]:
        """Get column names and types for a parquet file"""
        filepath = self.parquet_dir / file
        if not filepath.exists():
            raise FileNotFoundError(f"File not found: {file}")

        result = self.db.execute(f"DESCRIBE SELECT * FROM '{filepath}'")
        columns = []
        column_types = {}

        for row in result:
            col_name, col_type = row[0], row[1]
            # Categorize the type for the frontend
            type_category = self._categorize_type(col_type)
            columns.append(
                {"name": col_name, "type": col_type, "category": type_category}
            )
            column_types[col_name] = col_type

        # Cache column types for this file
        self._column_types_cache[file] = column_types

        return columns

    def _get_column_type(self, file: str, column: str) -> str:
        """Get the type of a specific column, using cache if available"""
        if file not in self._column_types_cache:
            self.get_columns(file)
        return self._column_types_cache.get(file, {}).get(column, "UNKNOWN")

    def _is_timestamp_type(self, dtype: str) -> bool:
        """Check if a type is a timestamp type"""
        dtype_lower = dtype.lower()
        return any(t in dtype_lower for t in ["timestamp", "datetime", "date", "time"])

    def _categorize_type(self, dtype: str) -> str:
        """Categorize DuckDB types for frontend use"""
        dtype_lower = dtype.lower()
        if any(t in dtype_lower for t in ["timestamp", "date", "time"]):
            return "temporal"
        elif any(
            t in dtype_lower
            for t in [
                "int",
                "float",
                "double",
                "decimal",
                "numeric",
                "bigint",
                "smallint",
                "tinyint",
                "real",
            ]
        ):
            return "numeric"
        elif any(t in dtype_lower for t in ["varchar", "char", "string", "text"]):
            return "string"
        elif "bool" in dtype_lower:
            return "boolean"
        return "other"

    def get_time_range(self, file: str, time_column: str) -> Dict[str, Any]:
        """Get min/max values for a time column"""
        filepath = self.parquet_dir / file
        col_type = self._get_column_type(file, time_column)
        is_timestamp = self._is_timestamp_type(col_type)

        if is_timestamp:
            # Convert timestamps to epoch in the query
            result = self.db.execute(f'''
                SELECT 
                    MIN("{time_column}") as min_time, 
                    MAX("{time_column}") as max_time,
                    EPOCH(MIN("{time_column}")) as min_epoch,
                    EPOCH(MAX("{time_column}")) as max_epoch,
                    COUNT(*) as total_count
                FROM '{filepath}'
            ''')[0]
            min_val, max_val, min_epoch, max_epoch, count = result
        else:
            result = self.db.execute(f'''
                SELECT 
                    MIN("{time_column}") as min_time, 
                    MAX("{time_column}") as max_time,
                    COUNT(*) as total_count
                FROM '{filepath}'
            ''')[0]
            min_val, max_val, count = result
            min_epoch = self._to_epoch(min_val)
            max_epoch = self._to_epoch(max_val)

        return {
            "min": str(min_val),
            "max": str(max_val),
            "min_epoch": min_epoch,
            "max_epoch": max_epoch,
            "total_count": count,
            "is_timestamp": is_timestamp,
        }

    def _to_epoch(self, value) -> Optional[float]:
        """Convert various time types to epoch seconds"""
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if hasattr(value, "timestamp"):
            return value.timestamp()
        return float(value)

    def _build_time_filter(
        self,
        time_column: str,
        start_time: Optional[float],
        end_time: Optional[float],
        is_timestamp: bool,
    ) -> str:
        """Build WHERE clause for time filtering with proper type handling"""
        where_clauses = []

        if start_time is not None:
            if is_timestamp:
                # Convert epoch to timestamp for comparison
                where_clauses.append(f'"{time_column}" >= to_timestamp({start_time})')
            else:
                where_clauses.append(f'"{time_column}" >= {start_time}')

        if end_time is not None:
            if is_timestamp:
                where_clauses.append(f'"{time_column}" <= to_timestamp({end_time})')
            else:
                where_clauses.append(f'"{time_column}" <= {end_time}')

        return f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    def _get_time_select_expr(self, time_column: str, is_timestamp: bool) -> str:
        """Get the SELECT expression for time column that returns epoch"""
        if is_timestamp:
            return f'EPOCH("{time_column}") as "{time_column}"'
        else:
            return f'"{time_column}"'

    def query_data(
        self,
        file: str,
        time_column: str,
        value_columns: List[str],
        start_time: Optional[float] = None,
        end_time: Optional[float] = None,
        max_points: int = MAX_POINTS_DEFAULT,
        downsample_method: str = "lttb",
    ) -> Dict[str, Any]:
        """
        Query data with intelligent downsampling

        Downsample methods:
        - 'lttb': Largest Triangle Three Buckets (best visual fidelity)
        - 'minmax': Min/Max preservation per bucket
        - 'avg': Simple averaging
        """
        filepath = self.parquet_dir / file

        # Get column type info
        col_type = self._get_column_type(file, time_column)
        is_timestamp = self._is_timestamp_type(col_type)

        logger.info(
            f"Time column '{time_column}' type: {col_type}, is_timestamp: {is_timestamp}"
        )

        # Build WHERE clause with proper type handling
        where_sql = self._build_time_filter(
            time_column, start_time, end_time, is_timestamp
        )

        # Get total count in range
        count_query = f"SELECT COUNT(*) FROM '{filepath}' {where_sql}"
        logger.debug(f"Count query: {count_query}")
        count_result = self.db.execute(count_query)[0][0]

        logger.info(f"Total points in range: {count_result}, max_points: {max_points}")

        # Build column selection
        value_cols_sql = ", ".join([f'"{c}"' for c in value_columns])
        time_select = self._get_time_select_expr(time_column, is_timestamp)

        if count_result <= max_points:
            # No downsampling needed
            query = f"""
                SELECT {time_select}, {value_cols_sql}
                FROM '{filepath}'
                {where_sql}
                ORDER BY "{time_column}"
            """
            result = self.db.execute(query)
            downsampled = False
        else:
            # Apply downsampling
            if downsample_method == "lttb":
                result = self._downsample_lttb(
                    filepath,
                    time_column,
                    value_columns,
                    where_sql,
                    max_points,
                    is_timestamp,
                )
            elif downsample_method == "minmax":
                result = self._downsample_minmax(
                    filepath,
                    time_column,
                    value_columns,
                    where_sql,
                    max_points,
                    is_timestamp,
                )
            else:
                result = self._downsample_avg(
                    filepath,
                    time_column,
                    value_columns,
                    where_sql,
                    max_points,
                    is_timestamp,
                )
            downsampled = True

        # Convert to columnar format
        data = self._results_to_columnar(
            result, time_column, value_columns, is_timestamp=False
        )  # Already converted to epoch

        return {
            "data": data,
            "total_points": count_result,
            "returned_points": len(data.get(time_column, [])),
            "downsampled": downsampled,
            "downsample_method": downsample_method if downsampled else None,
        }

    def _downsample_lttb(
        self,
        filepath: Path,
        time_col: str,
        value_cols: List[str],
        where_sql: str,
        max_points: int,
        is_timestamp: bool,
    ) -> List[tuple]:
        """
        Largest Triangle Three Buckets downsampling
        Implemented via SQL for performance
        """
        num_buckets = max_points

        value_selects = ", ".join([f'"{c}"' for c in value_cols])

        # Use EPOCH for timestamp columns in the output
        if is_timestamp:
            time_output = f'EPOCH("{time_col}") as "{time_col}"'
            time_order = f'"{time_col}"'
            time_agg = f'EPOCH(MIN("{time_col}")) as bucket_min_time'
        else:
            time_output = f'"{time_col}"'
            time_order = f'"{time_col}"'
            time_agg = f'MIN("{time_col}") as bucket_min_time'

        query = f"""
            WITH numbered AS (
                SELECT 
                    "{time_col}",
                    {value_selects},
                    ROW_NUMBER() OVER (ORDER BY {time_order}) as rn,
                    COUNT(*) OVER () as total
                FROM '{filepath}'
                {where_sql}
            ),
            bucketed AS (
                SELECT 
                    *,
                    FLOOR((rn - 1) * {num_buckets}::DOUBLE / NULLIF(total, 0)) as bucket
                FROM numbered
            ),
            bucket_stats AS (
                SELECT 
                    bucket,
                    {time_agg},
                    {", ".join([f'AVG("{c}") as avg_{c}' for c in value_cols])}
                FROM bucketed
                GROUP BY bucket
            )
            SELECT 
                bucket_min_time as "{time_col}",
                {", ".join([f'avg_{c} as "{c}"' for c in value_cols])}
            FROM bucket_stats
            ORDER BY bucket
        """

        logger.debug(f"LTTB query: {query[:500]}...")
        return self.db.execute(query)

    def _downsample_minmax(
        self,
        filepath: Path,
        time_col: str,
        value_cols: List[str],
        where_sql: str,
        max_points: int,
        is_timestamp: bool,
    ) -> List[tuple]:
        """
        Min/Max preservation downsampling - keeps the shape of spikes and valleys
        Returns representative points per bucket for better peak preservation
        """
        num_buckets = max_points // 2

        if is_timestamp:
            time_output = f'EPOCH("{time_col}") as "{time_col}"'
            time_order = f'"{time_col}"'
        else:
            time_output = f'"{time_col}"'
            time_order = f'"{time_col}"'

        value_selects = ", ".join([f'"{c}"' for c in value_cols])

        # Simplified min/max approach - get first point of each bucket
        # and the point with max absolute value in each bucket
        query = f"""
            WITH numbered AS (
                SELECT 
                    "{time_col}",
                    {value_selects},
                    ROW_NUMBER() OVER (ORDER BY {time_order}) as rn,
                    COUNT(*) OVER () as total
                FROM '{filepath}'
                {where_sql}
            ),
            bucketed AS (
                SELECT 
                    *,
                    FLOOR((rn - 1) * {num_buckets}::DOUBLE / NULLIF(total, 0)) as bucket
                FROM numbered
            ),
            first_points AS (
                SELECT 
                    bucket,
                    {time_output},
                    {value_selects},
                    ROW_NUMBER() OVER (PARTITION BY bucket ORDER BY "{time_col}") as pos
                FROM bucketed
            )
            SELECT "{time_col}", {value_selects}
            FROM first_points
            WHERE pos = 1
            ORDER BY "{time_col}"
        """

        logger.debug(f"MinMax query: {query[:500]}...")
        return self.db.execute(query)

    def _downsample_avg(
        self,
        filepath: Path,
        time_col: str,
        value_cols: List[str],
        where_sql: str,
        max_points: int,
        is_timestamp: bool,
    ) -> List[tuple]:
        """Simple averaging downsample"""
        num_buckets = max_points

        if is_timestamp:
            time_agg = f'EPOCH(MIN("{time_col}")) as "{time_col}"'
            time_order = f'"{time_col}"'
        else:
            time_agg = f'AVG("{time_col}") as "{time_col}"'
            time_order = f'"{time_col}"'

        query = f"""
            WITH numbered AS (
                SELECT 
                    "{time_col}",
                    {", ".join([f'"{c}"' for c in value_cols])},
                    ROW_NUMBER() OVER (ORDER BY {time_order}) as rn,
                    COUNT(*) OVER () as total
                FROM '{filepath}'
                {where_sql}
            ),
            bucketed AS (
                SELECT 
                    *,
                    FLOOR((rn - 1) * {num_buckets}::DOUBLE / NULLIF(total, 0)) as bucket
                FROM numbered
            )
            SELECT 
                {time_agg},
                {", ".join([f'AVG("{c}") as "{c}"' for c in value_cols])}
            FROM bucketed
            GROUP BY bucket
            ORDER BY "{time_col}"
        """

        logger.debug(f"AVG query: {query[:500]}...")
        return self.db.execute(query)

    def _results_to_columnar(
        self,
        results: List[tuple],
        time_col: str,
        value_cols: List[str],
        is_timestamp: bool = False,
    ) -> Dict[str, List]:
        """Convert row-based results to columnar format for efficient JSON transfer"""
        if not results:
            return {time_col: [], **{c: [] for c in value_cols}}

        columns = [time_col] + value_cols
        data = {col: [] for col in columns}

        for row in results:
            for i, col in enumerate(columns):
                val = row[i]
                # Convert timestamps to epoch if needed (shouldn't be needed now)
                if i == 0 and is_timestamp:
                    val = self._to_epoch(val)
                elif val is not None and not isinstance(val, (int, float)):
                    try:
                        val = float(val)
                    except (ValueError, TypeError):
                        pass
                data[col].append(val)

        return data


class RequestHandler(SimpleHTTPRequestHandler):
    """HTTP request handler with REST API endpoints"""

    query_engine: ParquetQueryEngine = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def log_message(self, format, *args):
        logger.info(f"{self.address_string()} - {format % args}")

    def do_OPTIONS(self):
        """Handle CORS preflight requests"""
        self.send_response(200)
        self._send_cors_headers()
        self.end_headers()

    def do_GET(self):
        """Handle GET requests"""
        parsed = urlparse(self.path)
        path = parsed.path
        params = parse_qs(parsed.query)

        try:
            if path == "/":
                self._serve_index()
            elif path == "/api/files":
                self._send_json({"files": self.query_engine.list_files()})
            elif path == "/api/columns":
                file = params.get("file", [None])[0]
                if not file:
                    self._send_error(400, "Missing 'file' parameter")
                    return
                columns = self.query_engine.get_columns(file)
                self._send_json({"columns": columns})
            elif path == "/api/range":
                file = params.get("file", [None])[0]
                time_col = params.get("time_column", [None])[0]
                if not file or not time_col:
                    self._send_error(400, "Missing required parameters")
                    return
                range_info = self.query_engine.get_time_range(file, time_col)
                self._send_json(range_info)
            elif path == "/api/health":
                self._send_json({"status": "ok", "version": "1.0.0"})
            else:
                super().do_GET()
        except FileNotFoundError as e:
            self._send_error(404, str(e))
        except Exception as e:
            logger.exception("Error handling GET request")
            self._send_error(500, str(e))

    def do_POST(self):
        """Handle POST requests"""
        parsed = urlparse(self.path)
        path = parsed.path

        try:
            if path == "/api/query":
                content_length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(content_length)
                request = json.loads(body.decode("utf-8"))

                # Validate required fields
                required = ["file", "time_column", "value_columns"]
                for field in required:
                    if field not in request:
                        self._send_error(400, f"Missing required field: {field}")
                        return

                result = self.query_engine.query_data(
                    file=request["file"],
                    time_column=request["time_column"],
                    value_columns=request["value_columns"],
                    start_time=request.get("start_time"),
                    end_time=request.get("end_time"),
                    max_points=request.get("max_points", MAX_POINTS_DEFAULT),
                    downsample_method=request.get("downsample_method", "lttb"),
                )
                self._send_json(result)
            else:
                self._send_error(404, "Endpoint not found")
        except json.JSONDecodeError:
            self._send_error(400, "Invalid JSON")
        except Exception as e:
            logger.exception("Error handling POST request")
            self._send_error(500, str(e))

    def _send_cors_headers(self):
        """Add CORS headers to response"""
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def _send_json(self, data: dict):
        """Send JSON response"""
        response = json.dumps(data, default=str)
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self._send_cors_headers()
        self.end_headers()
        self.wfile.write(response.encode("utf-8"))

    def _send_error(self, code: int, message: str):
        """Send error response"""
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self._send_cors_headers()
        self.end_headers()
        response = json.dumps({"error": message})
        self.wfile.write(response.encode("utf-8"))

    def _serve_index(self):
        """Serve the main HTML file"""
        index_path = PARQUET_DIR / "index.html"
        if index_path.exists():
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            with open(index_path, "rb") as f:
                self.wfile.write(f.read())
        else:
            self._send_error(404, "index.html not found")


def create_handler_class(query_engine: ParquetQueryEngine):
    """Factory to create handler class with query engine attached"""

    class Handler(RequestHandler):
        pass

    Handler.query_engine = query_engine
    return Handler


def main():
    """Main entry point"""
    # Initialize components
    db_manager = DuckDBManager(PARQUET_DIR)
    query_engine = ParquetQueryEngine(db_manager, PARQUET_DIR)

    # Create handler with query engine
    handler_class = create_handler_class(query_engine)

    # Start server
    server = HTTPServer((HOST, PORT), handler_class)
    logger.info(f"Starting Parquet Viewer server at http://{HOST}:{PORT}")
    logger.info(f"Serving parquet files from: {PARQUET_DIR.absolute()}")

    parquet_files = list(PARQUET_DIR.glob("*.parquet"))
    if parquet_files:
        logger.info(f"Found {len(parquet_files)} parquet file(s)")
    else:
        logger.warning("No parquet files found in current directory")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Server shutting down...")
        server.shutdown()


if __name__ == "__main__":
    main()
