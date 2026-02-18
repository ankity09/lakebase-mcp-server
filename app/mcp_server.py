"""Reusable Lakebase MCP Server — Databricks App.

Exposes 6 MCP tools (list_tables, describe_table, read_query,
insert_record, update_records, delete_records) over StreamableHTTP
at /mcp.  Connects to Lakebase PostgreSQL via env vars injected by
the database resource in app.yaml.
"""

import contextlib
import json
import logging
import os
import re
from datetime import date, datetime
from decimal import Decimal

import psycopg2
import psycopg2.pool
import uvicorn
from databricks.sdk import WorkspaceClient
from mcp.server.lowlevel import Server
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from mcp.types import TextContent, Tool
from pathlib import Path

from starlette.applications import Starlette
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, RedirectResponse
from starlette.routing import Mount, Route

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("lakebase-mcp")

# ── Globals ──────────────────────────────────────────────────────────

_ws = None
_pg_pool = None
MAX_READ_ROWS = 500

# ── Workspace client ─────────────────────────────────────────────────


def _get_ws():
    global _ws
    if _ws is None:
        _ws = WorkspaceClient()
        logger.info("SDK initialized: host=%s auth=%s", _ws.config.host, _ws.config.auth_type)
    return _ws


# ── Lakebase connection pool ─────────────────────────────────────────


def _get_pg_token():
    """Get OAuth token from Databricks SDK for Lakebase auth."""
    w = _get_ws()
    header_factory = w.config._header_factory
    if callable(header_factory):
        result = header_factory()
        if isinstance(result, dict):
            return result.get("Authorization", "").removeprefix("Bearer ")
    return ""


def _init_pg_pool():
    """Initialize the PostgreSQL connection pool."""
    global _pg_pool
    pg_host = os.environ.get("PGHOST")
    if not pg_host:
        raise RuntimeError("PGHOST not set — database resource missing from app.yaml")
    if _pg_pool:
        try:
            _pg_pool.closeall()
        except Exception:
            pass
    _pg_pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=1,
        maxconn=5,
        host=pg_host,
        port=int(os.environ.get("PGPORT", "5432")),
        dbname=os.environ.get("PGDATABASE", ""),
        user=os.environ.get("PGUSER", ""),
        password=_get_pg_token(),
        sslmode=os.environ.get("PGSSLMODE", "require"),
    )
    logger.info("Lakebase pool initialized: %s:%s/%s",
                pg_host, os.environ.get("PGPORT", "5432"), os.environ.get("PGDATABASE", ""))


def _get_conn():
    """Get a connection from the pool, re-init on pool errors."""
    global _pg_pool
    if _pg_pool is None:
        _init_pg_pool()
    try:
        return _pg_pool.getconn()
    except psycopg2.pool.PoolError:
        _init_pg_pool()
        return _pg_pool.getconn()


def _put_conn(conn, close=False):
    """Return a connection to the pool."""
    if _pg_pool is not None and conn is not None:
        try:
            _pg_pool.putconn(conn, close=close)
        except Exception:
            pass


def _serialize_value(val):
    """Serialize a single value for JSON output."""
    if val is None:
        return None
    if isinstance(val, Decimal):
        return float(val)
    if isinstance(val, (date, datetime)):
        return val.isoformat()
    if isinstance(val, dict):
        return val
    if isinstance(val, list):
        return val
    return val


def _rows_to_dicts(cur):
    """Convert cursor results to list of dicts with serialization."""
    if not cur.description:
        return []
    columns = [desc[0] for desc in cur.description]
    return [
        {columns[i]: _serialize_value(v) for i, v in enumerate(row)}
        for row in cur.fetchall()
    ]


def _execute_read(sql, params=None):
    """Execute a read query and return rows as dicts."""
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return _rows_to_dicts(cur)
    except psycopg2.OperationalError:
        _put_conn(conn, close=True)
        conn = None
        # Retry once on stale connection
        conn = _get_conn()
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return _rows_to_dicts(cur)
    finally:
        if conn:
            _put_conn(conn)


def _execute_write(sql, params=None):
    """Execute a write query with commit, return affected rows."""
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = _rows_to_dicts(cur)
            conn.commit()
            return rows
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            _put_conn(conn)


# ── Table name validation ────────────────────────────────────────────

_valid_tables_cache = None


def _get_valid_tables():
    """Fetch list of valid public table names from pg_tables."""
    global _valid_tables_cache
    if _valid_tables_cache is None:
        rows = _execute_read(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
        )
        _valid_tables_cache = {r["tablename"] for r in rows}
        logger.info("Valid tables: %s", _valid_tables_cache)
    return _valid_tables_cache


def _validate_table(table_name):
    """Validate that table_name exists in public schema."""
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table_name):
        raise ValueError(f"Invalid table name: {table_name}")
    valid = _get_valid_tables()
    if table_name not in valid:
        raise ValueError(f"Table '{table_name}' not found. Valid tables: {sorted(valid)}")
    return table_name


def _invalidate_table_cache():
    """Clear the table cache (e.g. after DDL)."""
    global _valid_tables_cache
    _valid_tables_cache = None


# ── JSONB detection ──────────────────────────────────────────────────


def _get_jsonb_columns(table_name):
    """Return set of column names that are jsonb type."""
    rows = _execute_read(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'public' AND table_name = %s AND data_type = 'jsonb'",
        (table_name,),
    )
    return {r["column_name"] for r in rows}


# ── MCP Server ───────────────────────────────────────────────────────

mcp_server = Server("lakebase-mcp")

TOOLS = [
    Tool(
        name="list_tables",
        description="List all tables in the Lakebase database with row counts and column counts.",
        inputSchema={
            "type": "object",
            "properties": {},
            "required": [],
        },
    ),
    Tool(
        name="describe_table",
        description="Get column names, data types, and constraints for a specific table.",
        inputSchema={
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Name of the table to describe.",
                },
            },
            "required": ["table_name"],
        },
    ),
    Tool(
        name="read_query",
        description=(
            "Execute a read-only SELECT query against the Lakebase database. "
            f"Returns up to {MAX_READ_ROWS} rows. Only SELECT statements are allowed."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "A SELECT SQL query to execute.",
                },
            },
            "required": ["sql"],
        },
    ),
    Tool(
        name="insert_record",
        description="Insert a single record into a table. Values are parameterized for safety.",
        inputSchema={
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Name of the table to insert into.",
                },
                "record": {
                    "oneOf": [
                        {"type": "object", "additionalProperties": True},
                        {"type": "string", "description": "JSON-encoded object"},
                    ],
                    "description": "Column-value pairs for the new record (object or JSON string).",
                },
            },
            "required": ["table_name", "record"],
        },
    ),
    Tool(
        name="update_records",
        description=(
            "Update rows in a table. SET values are parameterized. "
            "The WHERE clause is required to prevent accidental full-table updates."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Name of the table to update.",
                },
                "set_values": {
                    "oneOf": [
                        {"type": "object", "additionalProperties": True},
                        {"type": "string", "description": "JSON-encoded object"},
                    ],
                    "description": "Column-value pairs to SET (object or JSON string).",
                },
                "where": {
                    "type": "string",
                    "description": "WHERE clause (without the WHERE keyword). e.g. 'id = 42'",
                },
            },
            "required": ["table_name", "set_values", "where"],
        },
    ),
    Tool(
        name="delete_records",
        description=(
            "Delete rows from a table matching a WHERE condition. "
            "The WHERE clause is required to prevent accidental full-table deletes."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Name of the table to delete from.",
                },
                "where": {
                    "type": "string",
                    "description": "WHERE clause (without the WHERE keyword). e.g. 'id = 42'",
                },
            },
            "required": ["table_name", "where"],
        },
    ),
]


@mcp_server.list_tools()
async def handle_list_tools():
    return TOOLS


@mcp_server.call_tool()
async def handle_call_tool(name: str, arguments: dict):
    try:
        if name == "list_tables":
            return _tool_list_tables()
        elif name == "describe_table":
            return _tool_describe_table(arguments["table_name"])
        elif name == "read_query":
            return _tool_read_query(arguments["sql"])
        elif name == "insert_record":
            return _tool_insert_record(arguments["table_name"], arguments["record"])
        elif name == "update_records":
            return _tool_update_records(
                arguments["table_name"], arguments["set_values"], arguments["where"]
            )
        elif name == "delete_records":
            return _tool_delete_records(arguments["table_name"], arguments["where"])
        else:
            return [TextContent(type="text", text=f"Unknown tool: {name}")]
    except Exception as e:
        logger.exception("Tool %s failed", name)
        return [TextContent(type="text", text=f"Error: {e}")]


# ── Parameter coercion ────────────────────────────────────────────────


def _ensure_dict(val, param_name="value"):
    """Coerce a value to a dict — handles JSON strings from MAS agents."""
    if isinstance(val, dict):
        return val
    if isinstance(val, str):
        try:
            parsed = json.loads(val)
            if isinstance(parsed, dict):
                return parsed
        except (json.JSONDecodeError, TypeError):
            pass
        raise ValueError(f"{param_name} must be a JSON object, got string: {val[:100]}")
    raise ValueError(f"{param_name} must be a JSON object, got {type(val).__name__}")


# ── Tool implementations ─────────────────────────────────────────────


def _tool_list_tables():
    rows = _execute_read("""
        SELECT
            t.tablename AS table_name,
            (SELECT count(*) FROM information_schema.columns c
             WHERE c.table_schema = 'public' AND c.table_name = t.tablename) AS column_count
        FROM pg_tables t
        WHERE t.schemaname = 'public'
        ORDER BY t.tablename
    """)
    # Get row counts per table
    for row in rows:
        try:
            count_rows = _execute_read(
                f'SELECT count(*) AS cnt FROM "{row["table_name"]}"'
            )
            row["row_count"] = count_rows[0]["cnt"] if count_rows else 0
        except Exception:
            row["row_count"] = -1
    return [TextContent(type="text", text=json.dumps(rows, indent=2))]


def _tool_describe_table(table_name):
    table_name = _validate_table(table_name)
    columns = _execute_read(
        """
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position
        """,
        (table_name,),
    )
    # Get primary key columns
    pk = _execute_read(
        """
        SELECT a.attname AS column_name
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = %s::regclass AND i.indisprimary
        """,
        (table_name,),
    )
    pk_cols = {r["column_name"] for r in pk}
    for col in columns:
        col["is_primary_key"] = col["column_name"] in pk_cols
    result = {"table_name": table_name, "columns": columns}
    return [TextContent(type="text", text=json.dumps(result, indent=2))]


def _tool_read_query(sql):
    # Only allow SELECT statements
    stripped = sql.strip().rstrip(";").strip()
    if not stripped.upper().startswith("SELECT"):
        return [TextContent(
            type="text",
            text="Error: Only SELECT queries are allowed. Use insert_record, update_records, or delete_records for writes."
        )]
    # Enforce row limit
    upper = stripped.upper()
    if "LIMIT" not in upper:
        stripped = f"{stripped} LIMIT {MAX_READ_ROWS}"
    rows = _execute_read(stripped)
    return [TextContent(type="text", text=json.dumps(rows, indent=2, default=str))]


def _tool_insert_record(table_name, record):
    table_name = _validate_table(table_name)
    record = _ensure_dict(record, "record")
    if not record:
        return [TextContent(type="text", text="Error: record must contain at least one column.")]

    jsonb_cols = _get_jsonb_columns(table_name)
    columns = list(record.keys())
    values = []
    for col in columns:
        val = record[col]
        if col in jsonb_cols and not isinstance(val, str):
            values.append(json.dumps(val))
        else:
            values.append(val)

    col_list = ", ".join(f'"{c}"' for c in columns)
    placeholders = ", ".join(["%s"] * len(values))
    sql = f'INSERT INTO "{table_name}" ({col_list}) VALUES ({placeholders}) RETURNING *'

    rows = _execute_write(sql, values)
    return [TextContent(type="text", text=json.dumps(
        {"inserted": len(rows), "rows": rows}, indent=2, default=str
    ))]


def _tool_update_records(table_name, set_values, where):
    table_name = _validate_table(table_name)
    set_values = _ensure_dict(set_values, "set_values")
    if not set_values:
        return [TextContent(type="text", text="Error: set_values must contain at least one column.")]
    if not where or not where.strip():
        return [TextContent(type="text", text="Error: WHERE clause is required.")]

    jsonb_cols = _get_jsonb_columns(table_name)
    set_parts = []
    params = []
    for col, val in set_values.items():
        set_parts.append(f'"{col}" = %s')
        if col in jsonb_cols and not isinstance(val, str):
            params.append(json.dumps(val))
        else:
            params.append(val)

    set_clause = ", ".join(set_parts)
    sql = f'UPDATE "{table_name}" SET {set_clause} WHERE {where} RETURNING *'

    rows = _execute_write(sql, params)
    return [TextContent(type="text", text=json.dumps(
        {"updated": len(rows), "rows": rows}, indent=2, default=str
    ))]


def _tool_delete_records(table_name, where):
    table_name = _validate_table(table_name)
    if not where or not where.strip():
        return [TextContent(type="text", text="Error: WHERE clause is required.")]

    sql = f'DELETE FROM "{table_name}" WHERE {where} RETURNING *'
    rows = _execute_write(sql)
    return [TextContent(type="text", text=json.dumps(
        {"deleted": len(rows), "rows": rows}, indent=2, default=str
    ))]


# ── REST API endpoints (for frontend UI) ────────────────────────────

_FRONTEND_DIR = Path(__file__).parent / "frontend"


async def api_tables(request: Request):
    """List all tables with row/column counts."""
    try:
        result = _tool_list_tables()
        return JSONResponse(json.loads(result[0].text))
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


async def api_table_detail(request: Request):
    """Describe a single table."""
    table_name = request.path_params["table_name"]
    try:
        result = _tool_describe_table(table_name)
        return JSONResponse(json.loads(result[0].text))
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)


async def api_table_sample(request: Request):
    """Get sample rows from a table."""
    table_name = request.path_params["table_name"]
    limit = int(request.query_params.get("limit", "20"))
    try:
        _validate_table(table_name)
        result = _tool_read_query(f'SELECT * FROM "{table_name}" LIMIT {min(limit, 100)}')
        return JSONResponse(json.loads(result[0].text))
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)


async def api_query(request: Request):
    """Execute a read-only SELECT query."""
    body = await request.json()
    sql = body.get("sql", "")
    try:
        result = _tool_read_query(sql)
        data = json.loads(result[0].text)
        if isinstance(data, str) and data.startswith("Error"):
            return JSONResponse({"error": data}, status_code=400)
        return JSONResponse(data)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)


async def api_insert(request: Request):
    """Insert a record."""
    body = await request.json()
    try:
        result = _tool_insert_record(body["table_name"], body["record"])
        return JSONResponse(json.loads(result[0].text))
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)


async def api_update(request: Request):
    """Update records."""
    body = await request.json()
    try:
        result = _tool_update_records(body["table_name"], body["set_values"], body["where"])
        return JSONResponse(json.loads(result[0].text))
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)


async def api_delete(request: Request):
    """Delete records."""
    body = await request.json()
    try:
        result = _tool_delete_records(body["table_name"], body["where"])
        return JSONResponse(json.loads(result[0].text))
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)


async def api_tools(request: Request):
    """List available MCP tools with schemas."""
    tools_list = []
    for t in TOOLS:
        tools_list.append({
            "name": t.name,
            "description": t.description,
            "inputSchema": t.inputSchema,
        })
    return JSONResponse(tools_list)


async def api_info(request: Request):
    """Server info: database, instance, connection status."""
    pg_ok = False
    try:
        _execute_read("SELECT 1")
        pg_ok = True
    except Exception:
        pass
    return JSONResponse({
        "server": "lakebase-mcp",
        "mcp_endpoint": "/mcp/",
        "database": os.environ.get("PGDATABASE", ""),
        "host": os.environ.get("PGHOST", ""),
        "lakebase_connected": pg_ok,
        "tools_count": len(TOOLS),
    })


async def serve_frontend(request: Request):
    """Serve the frontend HTML."""
    index_path = _FRONTEND_DIR / "index.html"
    if index_path.exists():
        return HTMLResponse(index_path.read_text())
    return HTMLResponse("<h1>Lakebase MCP Server</h1><p>Frontend not found. Visit <a href='/health'>/health</a> or <a href='/api/tools'>/api/tools</a>.</p>")


# ── Starlette app ────────────────────────────────────────────────────

session_manager = StreamableHTTPSessionManager(
    app=mcp_server,
    stateless=True,
)


async def health(request: Request):
    """Health check endpoint."""
    pg_ok = False
    try:
        _execute_read("SELECT 1")
        pg_ok = True
    except Exception:
        pass
    return JSONResponse({"status": "ok" if pg_ok else "degraded", "lakebase": pg_ok})


@contextlib.asynccontextmanager
async def lifespan(app: Starlette):
    logger.info("Starting lakebase-mcp server")
    try:
        _init_pg_pool()
        logger.info("Lakebase pool ready")
    except Exception as e:
        logger.warning("Lakebase pool init deferred: %s", e)
    async with session_manager.run():
        yield
    # Cleanup
    if _pg_pool:
        _pg_pool.closeall()
        logger.info("Lakebase pool closed")


async def handle_mcp(scope, receive, send):
    await session_manager.handle_request(scope, receive, send)


async def mcp_redirect(request: Request):
    """Redirect /mcp to /mcp/ to avoid Starlette's localhost redirect."""
    return RedirectResponse(url="/mcp/", status_code=307)


app = Starlette(
    routes=[
        # Frontend
        Route("/", serve_frontend, methods=["GET"]),
        # REST API
        Route("/api/info", api_info, methods=["GET"]),
        Route("/api/tools", api_tools, methods=["GET"]),
        Route("/api/tables", api_tables, methods=["GET"]),
        Route("/api/tables/{table_name}", api_table_detail, methods=["GET"]),
        Route("/api/tables/{table_name}/sample", api_table_sample, methods=["GET"]),
        Route("/api/query", api_query, methods=["POST"]),
        Route("/api/insert", api_insert, methods=["POST"]),
        Route("/api/update", api_update, methods=["PATCH"]),
        Route("/api/delete", api_delete, methods=["DELETE"]),
        # Health & MCP
        Route("/health", health, methods=["GET"]),
        Route("/mcp", mcp_redirect, methods=["GET", "POST", "DELETE"]),
        Mount("/mcp", app=handle_mcp),
    ],
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Mcp-Session-Id"],
)

# ── Entry point ──────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.getenv("DATABRICKS_APP_PORT", "8000"))
    logger.info("Starting on port %d", port)
    uvicorn.run(app, host="0.0.0.0", port=port)
