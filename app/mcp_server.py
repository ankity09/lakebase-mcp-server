"""Reusable Lakebase MCP Server — Databricks App.

Exposes 34 MCP tools (with annotations), 2 resources, and 3 prompts over
StreamableHTTP at /mcp.  Supports both provisioned and autoscaling Lakebase.

Connection modes (auto-detected):
  - Provisioned: Set PGHOST via app.yaml database resource (standard)
  - Autoscaling: Set LAKEBASE_PROJECT env var (+ optional LAKEBASE_BRANCH,
    LAKEBASE_ENDPOINT, LAKEBASE_DATABASE). Token refresh is automatic.

Tools:
  READ:      list_tables, describe_table, read_query, list_schemas, get_connection_info
  WRITE:     insert_record, update_records, delete_records, batch_insert
  SQL:       execute_sql, execute_transaction, explain_query
  DDL:       create_table, drop_table, alter_table
  PERF:      list_slow_queries
  INFRA:     list_projects, describe_project, get_connection_string, list_branches, list_endpoints, get_endpoint_status
  BRANCH:    create_branch, delete_branch
  SCALE:     configure_autoscaling, configure_scale_to_zero
  QUALITY:   profile_table
  MIGRATION: prepare_database_migration, complete_database_migration
  TUNING:    prepare_query_tuning, complete_query_tuning
  EXPLORE:   describe_branch, compare_database_schema, search
"""

import contextlib
import contextvars
import json
import logging
import os
import re
import threading
import time
import uuid
from datetime import date, datetime
from decimal import Decimal

import psycopg2
import psycopg2.pool
import uvicorn
from databricks.sdk import WorkspaceClient
from mcp.server.lowlevel import Server
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from mcp.types import (
    GetPromptResult,
    Prompt,
    PromptArgument,
    PromptMessage,
    Resource,
    ResourceTemplate,
    TextContent,
    Tool,
)
try:
    from mcp.types import ToolAnnotations
except ImportError:
    ToolAnnotations = None
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
_ws_lock = threading.Lock()
_pg_pool = None
MAX_READ_ROWS = int(os.environ.get("MAX_ROWS", "1000"))
MAX_MIGRATION_STATES = 100
MAX_DATABASE_POOLS = 20

# Connection mode: "provisioned" (app.yaml PGHOST) or "autoscaling" (LAKEBASE_PROJECT)
_connection_mode = None        # set by _detect_connection_mode()
_autoscale_endpoint = None     # full endpoint resource name (autoscaling only)
_token_timestamp = 0.0         # time.time() when current token was generated
_TOKEN_REFRESH_SECONDS = 50 * 60  # refresh token every 50 min (expires at 60)
_current_database = None       # override for database switcher (None = use env default)
_current_instance = None       # override for instance switcher (None = use app.yaml default)
_current_instance_host = None  # PGHOST override when switching instances

# Per-request database context (for /db/{database}/mcp/ routing)
_request_database: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "_request_database", default=None
)

# Per-database connection pool registry (lazy, concurrent-safe)
_database_pools: dict[str, psycopg2.pool.ThreadedConnectionPool] = {}
_database_pools_lock = threading.Lock()

# Migration/tuning state for two-phase workflows (keyed by migration/tuning ID)
_migration_state: dict[str, dict] = {}

# ── Workspace client ─────────────────────────────────────────────────


def _get_ws():
    global _ws
    if _ws is None:
        with _ws_lock:
            if _ws is None:
                _ws = WorkspaceClient()
                logger.info("SDK initialized: host=%s auth=%s", _ws.config.host, _ws.config.auth_type)
    return _ws


# ── Connection mode detection ────────────────────────────────────────


def _detect_connection_mode():
    """Auto-detect provisioned vs autoscaling based on env vars."""
    global _connection_mode
    if os.environ.get("PGHOST"):
        _connection_mode = "provisioned"
    elif os.environ.get("LAKEBASE_PROJECT"):
        _connection_mode = "autoscaling"
    else:
        raise RuntimeError(
            "No Lakebase config found. Set PGHOST (provisioned via app.yaml database resource) "
            "or LAKEBASE_PROJECT (autoscaling via env vars)."
        )
    logger.info("Connection mode: %s", _connection_mode)
    return _connection_mode


# ── Lakebase connection pool ─────────────────────────────────────────


def _get_pg_token_provisioned():
    """Get OAuth token from Databricks SDK for provisioned Lakebase."""
    w = _get_ws()
    header_factory = w.config._header_factory
    if callable(header_factory):
        result = header_factory()
        if isinstance(result, dict):
            return result.get("Authorization", "").removeprefix("Bearer ")
    return ""


def _get_autoscale_credentials():
    """Discover autoscaling endpoint and generate a fresh credential."""
    global _autoscale_endpoint, _token_timestamp
    w = _get_ws()
    project = os.environ["LAKEBASE_PROJECT"]
    branch = os.environ.get("LAKEBASE_BRANCH", "production")
    endpoint_id = os.environ.get("LAKEBASE_ENDPOINT", "")

    branch_path = f"projects/{project}/branches/{branch}"

    # Build or discover endpoint name using REST API (w.postgres.* SDK methods are unreliable)
    if _autoscale_endpoint:
        endpoint_name = _autoscale_endpoint
    elif endpoint_id:
        endpoint_name = f"{branch_path}/endpoints/{endpoint_id}"
    else:
        # Auto-discover first endpoint on the branch
        resp = w.api_client.do("GET", f"/api/2.0/postgres/{branch_path}/endpoints")
        endpoints = resp.get("endpoints", [])
        if not endpoints:
            raise RuntimeError(f"No endpoints found on branch: {branch_path}")
        ep0 = endpoints[0]
        endpoint_name = ep0.get("name") if isinstance(ep0, dict) else getattr(ep0, "name", str(ep0))
        logger.info("Auto-discovered endpoint: %s", endpoint_name)

    _autoscale_endpoint = endpoint_name

    # Get host from endpoint
    ep_detail = w.api_client.do("GET", f"/api/2.0/postgres/{endpoint_name}")
    ep_status = ep_detail.get("status", {})
    ep_hosts = ep_status.get("hosts", {}) if isinstance(ep_status, dict) else {}
    host = ep_hosts.get("host", "") if isinstance(ep_hosts, dict) else ""
    if not host:
        raise RuntimeError(f"Endpoint {endpoint_name} has no host — is it running?")

    # Generate credential
    cred_resp = w.api_client.do("POST", "/api/2.0/postgres/credentials", body={"endpoint": endpoint_name})
    _token_timestamp = time.time()
    pg_pass = cred_resp.get("token", "")
    if not pg_pass:
        raise RuntimeError(f"Failed to generate credential for endpoint '{endpoint_name}'")

    # User = current workspace user
    user = w.current_user.me().user_name

    return {
        "host": host,
        "port": 5432,
        "user": user,
        "password": pg_pass,
        "database": os.environ.get("LAKEBASE_DATABASE", "databricks_postgres"),
    }


def _init_pg_pool():
    """Initialize the PostgreSQL connection pool. Supports both provisioned and autoscaling."""
    global _pg_pool, _connection_mode, _token_timestamp

    if _connection_mode is None:
        _detect_connection_mode()

    if _pg_pool:
        try:
            _pg_pool.closeall()
        except Exception:
            pass

    dbname_override = _current_database  # database switcher override

    if _connection_mode == "provisioned":
        pg_host = _current_instance_host or os.environ.get("PGHOST")
        pg_port = int(os.environ.get("PGPORT", "5432"))
        pg_db = dbname_override or os.environ.get("PGDATABASE", "")
        pg_user = os.environ.get("PGUSER", "")
        pg_pass = _get_pg_token_provisioned()
        pg_ssl = os.environ.get("PGSSLMODE", "require")
        _token_timestamp = time.time()
    else:  # autoscaling
        creds = _get_autoscale_credentials()
        pg_host = creds["host"]
        pg_port = creds["port"]
        pg_db = dbname_override or creds["database"]
        pg_user = creds["user"]
        pg_pass = creds["password"]
        pg_ssl = "require"

    _pg_pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=1,
        maxconn=5,
        host=pg_host,
        port=pg_port,
        dbname=pg_db,
        user=pg_user,
        password=pg_pass,
        sslmode=pg_ssl,
    )
    logger.info("Lakebase pool initialized [%s]: %s:%s/%s", _connection_mode, pg_host, pg_port, pg_db)


def _get_or_create_db_pool(database: str) -> psycopg2.pool.ThreadedConnectionPool:
    """Get or lazily create a connection pool for a specific database."""
    with _database_pools_lock:
        if database in _database_pools:
            return _database_pools[database]
        if len(_database_pools) >= MAX_DATABASE_POOLS:
            raise RuntimeError(
                f"Maximum number of database pools ({MAX_DATABASE_POOLS}) reached. "
                "Close unused database connections or increase MAX_DATABASE_POOLS."
            )

    # Create new pool for this database
    if _connection_mode is None:
        _detect_connection_mode()

    if _connection_mode == "provisioned":
        pg_host = _current_instance_host or os.environ.get("PGHOST")
        pg_port = int(os.environ.get("PGPORT", "5432"))
        pg_user = os.environ.get("PGUSER", "")
        pg_pass = _get_pg_token_provisioned()
        pg_ssl = os.environ.get("PGSSLMODE", "require")
    else:
        creds = _get_autoscale_credentials()
        pg_host, pg_port, pg_user, pg_pass, pg_ssl = (
            creds["host"], creds["port"], creds["user"], creds["password"], "require",
        )

    pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=1, maxconn=5,
        host=pg_host, port=pg_port, dbname=database,
        user=pg_user, password=pg_pass, sslmode=pg_ssl,
    )
    with _database_pools_lock:
        # Double-check: another thread may have created it while we were connecting
        if database not in _database_pools:
            _database_pools[database] = pool
        else:
            pool.closeall()
            pool = _database_pools[database]
    logger.info("Created pool for database: %s", database)
    return pool


def _maybe_refresh_token():
    """Reinitialize pool if OAuth token is about to expire."""
    global _pg_pool
    if _token_timestamp and (time.time() - _token_timestamp) > _TOKEN_REFRESH_SECONDS:
        logger.info("Token approaching expiry, refreshing pools...")
        # Save references to old pools before creating new ones
        old_pg_pool = _pg_pool
        old_db_pools = {}
        with _database_pools_lock:
            old_db_pools = dict(_database_pools)

        # Create new default pool first (this sets _pg_pool)
        _init_pg_pool()

        # Swap per-database pools atomically (clear so they get recreated with fresh tokens)
        with _database_pools_lock:
            _database_pools.clear()

        # Now close the old pools after the swap
        if old_pg_pool and old_pg_pool is not _pg_pool:
            try:
                old_pg_pool.closeall()
            except Exception:
                pass
        for db_name, pool in old_db_pools.items():
            try:
                pool.closeall()
            except Exception:
                pass
        logger.info("Per-database pools refreshed for token renewal")


def _get_conn():
    """Get a connection — uses request-scoped database if set, else global pool."""
    _maybe_refresh_token()  # ensure token is fresh for all pool types
    req_db = _request_database.get(None)
    if req_db:
        pool = _get_or_create_db_pool(req_db)
        try:
            return pool.getconn()
        except psycopg2.pool.PoolError:
            # Pool exhausted — recreate
            with _database_pools_lock:
                _database_pools.pop(req_db, None)
            pool = _get_or_create_db_pool(req_db)
            return pool.getconn()

    # Fall back to default global pool
    global _pg_pool
    if _pg_pool is None:
        _init_pg_pool()
    try:
        return _pg_pool.getconn()
    except psycopg2.pool.PoolError:
        _init_pg_pool()
        return _pg_pool.getconn()


def _put_conn(conn, close=False):
    """Return connection to the appropriate pool."""
    req_db = _request_database.get(None)
    if req_db and req_db in _database_pools:
        try:
            _database_pools[req_db].putconn(conn, close=close)
        except Exception:
            pass
        return
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


def _execute_write_with_info(sql, params=None):
    """Execute a write/DDL query, return (rows, rowcount, statusmessage)."""
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = _rows_to_dicts(cur)
            rowcount = cur.rowcount
            statusmessage = cur.statusmessage
            conn.commit()
            return rows, rowcount, statusmessage
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            _put_conn(conn)


# ── SQL classification ───────────────────────────────────────────────

_DDL_KEYWORDS = {"CREATE", "DROP", "ALTER", "TRUNCATE", "COMMENT", "GRANT", "REVOKE"}
_WRITE_KEYWORDS = {"INSERT", "UPDATE", "DELETE", "UPSERT", "MERGE"}


def _classify_sql(sql):
    """Classify SQL as 'read', 'write', or 'ddl' by first keyword."""
    # Strip SQL comments before classification to prevent bypass
    cleaned = re.sub(r'--[^\n]*', '', sql)
    cleaned = re.sub(r'/\*.*?\*/', '', cleaned, flags=re.DOTALL)
    stripped = cleaned.strip().rstrip(";").strip()
    first_word = stripped.split()[0].upper() if stripped else ""
    # Handle WITH (CTE): scan past the CTE to find the actual DML/DDL keyword
    if first_word == "WITH":
        # Find the actual statement keyword after the CTE
        cte_keywords = {"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "DROP"}
        words = stripped.upper().split()
        for word in words[1:]:
            if word in cte_keywords:
                first_word = word
                break
    if first_word in _DDL_KEYWORDS:
        return "ddl"
    if first_word in _WRITE_KEYWORDS:
        return "write"
    return "read"


# ── Table name validation ────────────────────────────────────────────

_valid_tables_cache: dict = {}


def _get_current_db_name() -> str:
    """Get the current database name for cache keying."""
    req_db = _request_database.get(None)
    if req_db:
        return req_db
    return _current_database or os.environ.get("PGDATABASE", "") or os.environ.get("LAKEBASE_DATABASE", "default")


def _get_valid_tables():
    """Fetch list of valid public table names from pg_tables."""
    db_name = _get_current_db_name()
    entry = _valid_tables_cache.get(db_name)
    if entry is not None:
        return entry["tables"]
    rows = _execute_read(
        "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
    )
    tables = {r["tablename"] for r in rows}
    _valid_tables_cache[db_name] = {"tables": tables, "time": time.time()}
    logger.info("Valid tables for %s: %s", db_name, tables)
    return tables


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
    db_name = _get_current_db_name()
    _valid_tables_cache.pop(db_name, None)


# ── JSONB detection ──────────────────────────────────────────────────


def _get_jsonb_columns(table_name):
    """Return set of column names that are jsonb type."""
    rows = _execute_read(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'public' AND table_name = %s AND data_type = 'jsonb'",
        (table_name,),
    )
    return {r["column_name"] for r in rows}


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


def _ensure_list(val, param_name="value"):
    """Coerce a value to a list — handles JSON strings from MAS agents."""
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        try:
            parsed = json.loads(val)
            if isinstance(parsed, list):
                return parsed
        except (json.JSONDecodeError, TypeError):
            pass
        raise ValueError(f"{param_name} must be a JSON array, got string: {val[:100]}")
    raise ValueError(f"{param_name} must be a JSON array, got {type(val).__name__}")


# ── SQL injection sanitization ────────────────────────────────────────


def _sanitize_where(where: str) -> str:
    """Reject WHERE clause values containing SQL injection patterns."""
    if ";" in where:
        raise ValueError("WHERE clause must not contain semicolons (;). Use parameterized queries for complex conditions.")
    if "--" in where:
        raise ValueError("WHERE clause must not contain line comments (--).")
    if "/*" in where:
        raise ValueError("WHERE clause must not contain block comments (/*).")
    return where


def _validate_column_type(ctype: str) -> str:
    """Validate a column type string against a whitelist pattern."""
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_\s\(\),\[\]]+$', ctype):
        raise ValueError(f"Invalid column type: {ctype!r}. Only alphanumeric characters, spaces, parentheses, brackets, and commas are allowed.")
    return ctype


def _validate_constraints(constraints: str) -> str:
    """Reject constraint strings containing SQL injection patterns."""
    if ";" in constraints:
        raise ValueError("Constraints must not contain semicolons (;).")
    if "--" in constraints:
        raise ValueError("Constraints must not contain line comments (--).")
    if "/*" in constraints:
        raise ValueError("Constraints must not contain block comments (/*).")
    return constraints


# ── SDK proto serialization ──────────────────────────────────────────


def _serialize_proto(obj):
    """Convert Databricks SDK proto objects to JSON-serializable dicts."""
    if obj is None:
        return None
    # Try SDK's built-in serialization
    if hasattr(obj, "as_dict"):
        return obj.as_dict()
    if hasattr(obj, "to_dict"):
        return obj.to_dict()
    # Manual field extraction fallback
    if hasattr(obj, "__dict__"):
        result = {}
        for k, v in obj.__dict__.items():
            if k.startswith("_"):
                continue
            if hasattr(v, "as_dict") or hasattr(v, "to_dict"):
                result[k] = _serialize_proto(v)
            elif isinstance(v, list):
                result[k] = [_serialize_proto(i) if hasattr(i, "as_dict") or hasattr(i, "to_dict") or hasattr(i, "__dict__") else i for i in v]
            elif isinstance(v, (str, int, float, bool, type(None))):
                result[k] = v
            else:
                result[k] = str(v)
        return result
    return str(obj)


def _resolve_project(identifier: str) -> str:
    """Accept project name or full resource path, return full path."""
    if identifier.startswith("projects/"):
        return identifier
    return f"projects/{identifier}"


def _resolve_branch(project: str, branch: str = "production") -> str:
    """Build branch resource path from project + branch name."""
    project_path = _resolve_project(project)
    if branch.startswith(project_path):
        return branch
    return f"{project_path}/branches/{branch}"


# ── MCP Server ───────────────────────────────────────────────────────

mcp_server = Server("lakebase-mcp")

TOOLS = [
    # ── Original 6 tools ─────────────────────────────────────────────
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
    # ── P0 tools: General SQL ────────────────────────────────────────
    Tool(
        name="execute_sql",
        description=(
            "Execute any SQL statement (SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, DROP). "
            "Auto-detects statement type and returns structured results. "
            f"Read queries return up to {MAX_READ_ROWS} rows. "
            "For writes/DDL, returns rowcount and status message."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "SQL statement to execute.",
                },
            },
            "required": ["sql"],
        },
    ),
    Tool(
        name="execute_transaction",
        description=(
            "Execute multiple SQL statements in a single atomic transaction. "
            "All statements succeed or all are rolled back. "
            "Returns results for each statement. On error, reports which statement failed."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "statements": {
                    "oneOf": [
                        {"type": "array", "items": {"type": "string"}},
                        {"type": "string", "description": "JSON-encoded array of SQL strings"},
                    ],
                    "description": "Array of SQL statements to execute atomically.",
                },
            },
            "required": ["statements"],
        },
    ),
    Tool(
        name="explain_query",
        description=(
            "Run EXPLAIN ANALYZE on a SQL query and return the execution plan as JSON. "
            "For write statements, wraps in a transaction and rolls back to prevent side effects."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "SQL query to analyze.",
                },
            },
            "required": ["sql"],
        },
    ),
    Tool(
        name="create_table",
        description=(
            "Create a new table with the specified columns. "
            "Supports IF NOT EXISTS. Column definitions include name, type, and optional constraints."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Name of the table to create.",
                },
                "columns": {
                    "oneOf": [
                        {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "name": {"type": "string"},
                                    "type": {"type": "string"},
                                    "constraints": {"type": "string"},
                                },
                                "required": ["name", "type"],
                            },
                        },
                        {"type": "string", "description": "JSON-encoded array of column objects"},
                    ],
                    "description": "Column definitions: [{name, type, constraints?}, ...]",
                },
                "if_not_exists": {
                    "type": "boolean",
                    "description": "Add IF NOT EXISTS clause. Default: true.",
                    "default": True,
                },
            },
            "required": ["table_name", "columns"],
        },
    ),
    Tool(
        name="drop_table",
        description=(
            "Drop a table from the database. Requires confirm=true as a safety measure. "
            "Supports IF EXISTS."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Name of the table to drop.",
                },
                "confirm": {
                    "type": "boolean",
                    "description": "Must be true to confirm the drop. Safety measure.",
                },
                "if_exists": {
                    "type": "boolean",
                    "description": "Add IF EXISTS clause. Default: true.",
                    "default": True,
                },
            },
            "required": ["table_name", "confirm"],
        },
    ),
    Tool(
        name="alter_table",
        description=(
            "Alter a table: add column, drop column, rename column, or alter column type. "
            "Specify exactly one operation."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Name of the table to alter.",
                },
                "operation": {
                    "type": "string",
                    "enum": ["add_column", "drop_column", "rename_column", "alter_type"],
                    "description": "The ALTER operation to perform.",
                },
                "column_name": {
                    "type": "string",
                    "description": "Column to operate on.",
                },
                "new_column_name": {
                    "type": "string",
                    "description": "New name (for rename_column only).",
                },
                "column_type": {
                    "type": "string",
                    "description": "Column type (for add_column or alter_type).",
                },
                "constraints": {
                    "type": "string",
                    "description": "Optional constraints (for add_column). e.g. 'NOT NULL DEFAULT 0'",
                },
            },
            "required": ["table_name", "operation", "column_name"],
        },
    ),
    # ── P1 tools ─────────────────────────────────────────────────────
    Tool(
        name="list_slow_queries",
        description=(
            "List slow queries from pg_stat_statements (if the extension is enabled). "
            "Returns top N queries by total execution time."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "limit": {
                    "type": "integer",
                    "description": "Number of slow queries to return. Default: 10.",
                    "default": 10,
                },
            },
            "required": [],
        },
    ),
    Tool(
        name="batch_insert",
        description=(
            "Insert multiple records into a table in a single statement. "
            "More efficient than multiple insert_record calls. JSONB-aware."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Name of the table to insert into.",
                },
                "records": {
                    "oneOf": [
                        {"type": "array", "items": {"type": "object", "additionalProperties": True}},
                        {"type": "string", "description": "JSON-encoded array of objects"},
                    ],
                    "description": "Array of column-value pairs to insert.",
                },
            },
            "required": ["table_name", "records"],
        },
    ),
    Tool(
        name="list_schemas",
        description="List all schemas in the database (not just public).",
        inputSchema={
            "type": "object",
            "properties": {},
            "required": [],
        },
    ),
    Tool(
        name="get_connection_info",
        description="Return connection info: host, port, database, user. No password is returned.",
        inputSchema={
            "type": "object",
            "properties": {},
            "required": [],
        },
    ),
    # ── Infrastructure tools ────────────────────────────────────────
    Tool(
        name="list_projects",
        description="List all Lakebase projects with their status, branch count, and endpoint info.",
        inputSchema={
            "type": "object",
            "properties": {},
            "required": [],
        },
    ),
    Tool(
        name="describe_project",
        description="Get detailed info about a Lakebase project including branches, endpoints, and configuration.",
        inputSchema={
            "type": "object",
            "properties": {
                "project": {
                    "type": "string",
                    "description": "Project name or full resource path (e.g. 'my-project' or 'projects/my-project').",
                },
            },
            "required": ["project"],
        },
    ),
    Tool(
        name="get_connection_string",
        description="Build a psql or psycopg2 connection string for a Lakebase endpoint.",
        inputSchema={
            "type": "object",
            "properties": {
                "endpoint": {
                    "type": "string",
                    "description": "Endpoint name or full resource path.",
                },
                "format": {
                    "type": "string",
                    "enum": ["psql", "psycopg2", "jdbc"],
                    "description": "Connection string format. Default: psql.",
                    "default": "psql",
                },
            },
            "required": ["endpoint"],
        },
    ),
    Tool(
        name="list_branches",
        description="List branches on a Lakebase project with their state and creation time.",
        inputSchema={
            "type": "object",
            "properties": {
                "project": {
                    "type": "string",
                    "description": "Project name or full resource path.",
                },
            },
            "required": ["project"],
        },
    ),
    Tool(
        name="list_endpoints",
        description="List endpoints on a Lakebase branch with their state, host, and compute config.",
        inputSchema={
            "type": "object",
            "properties": {
                "project": {
                    "type": "string",
                    "description": "Project name or full resource path.",
                },
                "branch": {
                    "type": "string",
                    "description": "Branch name. Default: production.",
                    "default": "production",
                },
            },
            "required": ["project"],
        },
    ),
    Tool(
        name="get_endpoint_status",
        description="Get the current status, host, and compute configuration of a Lakebase endpoint.",
        inputSchema={
            "type": "object",
            "properties": {
                "endpoint": {
                    "type": "string",
                    "description": "Full endpoint resource path (e.g. projects/X/branches/Y/endpoints/Z).",
                },
            },
            "required": ["endpoint"],
        },
    ),
    # ── Branch management tools ─────────────────────────────────────
    Tool(
        name="create_branch",
        description="Create a new development or test branch on a Lakebase project.",
        inputSchema={
            "type": "object",
            "properties": {
                "project": {
                    "type": "string",
                    "description": "Project name or full resource path.",
                },
                "branch_id": {
                    "type": "string",
                    "description": "Branch ID (slug). e.g. 'dev', 'staging', 'feature-x'.",
                },
                "parent_branch": {
                    "type": "string",
                    "description": "Parent branch to fork from. Default: production.",
                    "default": "production",
                },
            },
            "required": ["project", "branch_id"],
        },
    ),
    Tool(
        name="delete_branch",
        description="Delete a Lakebase branch. Cannot delete the production branch.",
        inputSchema={
            "type": "object",
            "properties": {
                "branch": {
                    "type": "string",
                    "description": "Full branch resource path (e.g. projects/X/branches/dev).",
                },
                "confirm": {
                    "type": "boolean",
                    "description": "Must be true to confirm deletion. Safety measure.",
                },
            },
            "required": ["branch", "confirm"],
        },
    ),
    # ── Autoscaling config tools ────────────────────────────────────
    Tool(
        name="configure_autoscaling",
        description="Configure autoscaling min/max compute units (CU) on a Lakebase endpoint.",
        inputSchema={
            "type": "object",
            "properties": {
                "endpoint": {
                    "type": "string",
                    "description": "Full endpoint resource path.",
                },
                "min_cu": {
                    "type": "number",
                    "description": "Minimum compute units (e.g. 0.25).",
                },
                "max_cu": {
                    "type": "number",
                    "description": "Maximum compute units (e.g. 4).",
                },
            },
            "required": ["endpoint", "min_cu", "max_cu"],
        },
    ),
    Tool(
        name="configure_scale_to_zero",
        description="Enable or disable scale-to-zero (suspend) on a Lakebase endpoint with idle timeout.",
        inputSchema={
            "type": "object",
            "properties": {
                "endpoint": {
                    "type": "string",
                    "description": "Full endpoint resource path.",
                },
                "enabled": {
                    "type": "boolean",
                    "description": "Enable (true) or disable (false) scale-to-zero.",
                },
                "idle_timeout_seconds": {
                    "type": "integer",
                    "description": "Seconds of idle time before suspending. Default: 300.",
                    "default": 300,
                },
            },
            "required": ["endpoint", "enabled"],
        },
    ),
    # ── Data quality tools ──────────────────────────────────────────
    Tool(
        name="profile_table",
        description=(
            "Generate column-level profiling statistics for a table: "
            "row count, null counts, distinct counts, min/max, and average for numeric columns."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Name of the table to profile.",
                },
            },
            "required": ["table_name"],
        },
    ),
    # ── Neon-parity tools ──────────────────────────────────────────
    Tool(
        name="describe_branch",
        description=(
            "Get a tree view of all objects in a database: schemas, tables, views, "
            "functions, sequences, and indexes. Works with any connected database."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "database": {
                    "type": "string",
                    "description": "Database name. Default: current database.",
                },
            },
            "required": [],
        },
    ),
    Tool(
        name="compare_database_schema",
        description=(
            "Compare schemas between two databases and return a unified diff. "
            "Shows tables/columns added, removed, or modified between source and target."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "source_database": {
                    "type": "string",
                    "description": "Source database name (baseline).",
                },
                "target_database": {
                    "type": "string",
                    "description": "Target database name (to compare against source).",
                },
            },
            "required": ["source_database", "target_database"],
        },
    ),
    Tool(
        name="prepare_database_migration",
        description=(
            "Phase 1 of safe migration: validates migration SQL by running it in a "
            "transaction that gets rolled back. Returns the execution plan and affected "
            "objects without making changes. Use complete_database_migration to apply."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "migration_sql": {
                    "type": "string",
                    "description": "The SQL migration to validate (CREATE, ALTER, DROP, INSERT, etc.).",
                },
            },
            "required": ["migration_sql"],
        },
    ),
    Tool(
        name="complete_database_migration",
        description=(
            "Phase 2 of safe migration: applies the validated migration SQL for real, "
            "or discards it. Must be called after prepare_database_migration."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "migration_id": {
                    "type": "string",
                    "description": "Migration ID returned by prepare_database_migration.",
                },
                "apply_changes": {
                    "type": "boolean",
                    "description": "True to apply the migration, false to discard.",
                    "default": True,
                },
            },
            "required": ["migration_id"],
        },
    ),
    Tool(
        name="prepare_query_tuning",
        description=(
            "Phase 1 of query tuning: analyzes a query with EXPLAIN ANALYZE, identifies "
            "bottlenecks, and suggests index/query improvements. Returns tuning suggestions."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "The SQL query to tune.",
                },
            },
            "required": ["sql"],
        },
    ),
    Tool(
        name="complete_query_tuning",
        description=(
            "Phase 2 of query tuning: applies suggested DDL changes (e.g. CREATE INDEX) "
            "or discards them. Must be called after prepare_query_tuning."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "tuning_id": {
                    "type": "string",
                    "description": "Tuning ID returned by prepare_query_tuning.",
                },
                "apply_changes": {
                    "type": "boolean",
                    "description": "True to apply suggested DDL, false to discard.",
                    "default": False,
                },
                "ddl_statements": {
                    "oneOf": [
                        {"type": "array", "items": {"type": "string"}},
                        {"type": "string", "description": "JSON-encoded array of DDL strings"},
                    ],
                    "description": "DDL statements to apply (e.g. CREATE INDEX). Defaults to suggestions from prepare step.",
                },
            },
            "required": ["tuning_id"],
        },
    ),
    Tool(
        name="search",
        description=(
            "Search across all Lakebase instances, projects, and databases by keyword. "
            "Returns matching instances, databases, and tables."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Search query (min 2 characters). Matches instance names, database names, and table names.",
                },
            },
            "required": ["query"],
        },
    ),
]


# ── MCP Tool Annotations ─────────────────────────────────────────────
_TOOL_ANNOTATIONS = {
    # READ tools
    "list_tables":              {"readOnlyHint": True,  "destructiveHint": False, "idempotentHint": True},
    "describe_table":           {"readOnlyHint": True,  "destructiveHint": False, "idempotentHint": True},
    "read_query":               {"readOnlyHint": True,  "destructiveHint": False, "idempotentHint": True},
    "list_schemas":             {"readOnlyHint": True,  "destructiveHint": False, "idempotentHint": True},
    "get_connection_info":      {"readOnlyHint": True,  "destructiveHint": False, "idempotentHint": True},
    # WRITE tools
    "insert_record":            {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False},
    "update_records":           {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False},
    "delete_records":           {"readOnlyHint": False, "destructiveHint": True,  "idempotentHint": False},
    "batch_insert":             {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False},
    # SQL tools
    "execute_sql":              {"readOnlyHint": False, "destructiveHint": True,  "idempotentHint": False},
    "execute_transaction":      {"readOnlyHint": False, "destructiveHint": True,  "idempotentHint": False},
    "explain_query":            {"readOnlyHint": True,  "destructiveHint": False, "idempotentHint": True},
    # DDL tools
    "create_table":             {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": True},
    "drop_table":               {"readOnlyHint": False, "destructiveHint": True,  "idempotentHint": False},
    "alter_table":              {"readOnlyHint": False, "destructiveHint": True,  "idempotentHint": False},
    # PERF tools
    "list_slow_queries":        {"readOnlyHint": True,  "destructiveHint": False, "idempotentHint": True},
    # INFRA tools
    "list_projects":            {"readOnlyHint": True,  "destructiveHint": False, "idempotentHint": True},
    "describe_project":         {"readOnlyHint": True,  "destructiveHint": False, "idempotentHint": True},
    "get_connection_string":    {"readOnlyHint": True,  "destructiveHint": False, "idempotentHint": True},
    "list_branches":            {"readOnlyHint": True,  "destructiveHint": False, "idempotentHint": True},
    "list_endpoints":           {"readOnlyHint": True,  "destructiveHint": False, "idempotentHint": True},
    "get_endpoint_status":      {"readOnlyHint": True,  "destructiveHint": False, "idempotentHint": True},
    # BRANCH tools
    "create_branch":            {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False},
    "delete_branch":            {"readOnlyHint": False, "destructiveHint": True,  "idempotentHint": False},
    # SCALE tools
    "configure_autoscaling":    {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": True},
    "configure_scale_to_zero":  {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": True},
    # QUALITY tools
    "profile_table":            {"readOnlyHint": True,  "destructiveHint": False, "idempotentHint": True},
    # Neon-parity tools
    "describe_branch":          {"readOnlyHint": True,  "destructiveHint": False, "idempotentHint": True},
    "compare_database_schema":  {"readOnlyHint": True,  "destructiveHint": False, "idempotentHint": True},
    "prepare_database_migration": {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False},
    "complete_database_migration": {"readOnlyHint": False, "destructiveHint": True, "idempotentHint": False},
    "prepare_query_tuning":     {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False},
    "complete_query_tuning":    {"readOnlyHint": False, "destructiveHint": True,  "idempotentHint": False},
    "search":                   {"readOnlyHint": True,  "destructiveHint": False, "idempotentHint": True},
}


@mcp_server.list_tools()
async def handle_list_tools():
    if ToolAnnotations is None:
        return TOOLS
    annotated = []
    for t in TOOLS:
        ann = _TOOL_ANNOTATIONS.get(t.name)
        if ann:
            annotated.append(Tool(
                name=t.name,
                description=t.description,
                inputSchema=t.inputSchema,
                annotations=ToolAnnotations(**ann),
            ))
        else:
            annotated.append(t)
    return annotated


@mcp_server.call_tool()
async def handle_call_tool(name: str, arguments: dict):
    try:
        # Original 6 tools
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
        # P0 tools
        elif name == "execute_sql":
            return _tool_execute_sql(arguments["sql"])
        elif name == "execute_transaction":
            return _tool_execute_transaction(arguments["statements"])
        elif name == "explain_query":
            return _tool_explain_query(arguments["sql"])
        elif name == "create_table":
            return _tool_create_table(
                arguments["table_name"],
                arguments["columns"],
                arguments.get("if_not_exists", True),
            )
        elif name == "drop_table":
            return _tool_drop_table(
                arguments["table_name"],
                arguments.get("confirm", False),
                arguments.get("if_exists", True),
            )
        elif name == "alter_table":
            return _tool_alter_table(
                arguments["table_name"],
                arguments["operation"],
                arguments["column_name"],
                new_column_name=arguments.get("new_column_name"),
                column_type=arguments.get("column_type"),
                constraints=arguments.get("constraints"),
            )
        # P1 tools
        elif name == "list_slow_queries":
            return _tool_list_slow_queries(arguments.get("limit", 10))
        elif name == "batch_insert":
            return _tool_batch_insert(arguments["table_name"], arguments["records"])
        elif name == "list_schemas":
            return _tool_list_schemas()
        elif name == "get_connection_info":
            return _tool_get_connection_info()
        # Infrastructure tools
        elif name == "list_projects":
            return _tool_list_projects()
        elif name == "describe_project":
            return _tool_describe_project(arguments["project"])
        elif name == "get_connection_string":
            return _tool_get_connection_string(
                arguments["endpoint"], arguments.get("format", "psql")
            )
        elif name == "list_branches":
            return _tool_list_branches(arguments["project"])
        elif name == "list_endpoints":
            return _tool_list_endpoints(
                arguments["project"], arguments.get("branch", "production")
            )
        elif name == "get_endpoint_status":
            return _tool_get_endpoint_status(arguments["endpoint"])
        # Branch management tools
        elif name == "create_branch":
            return _tool_create_branch(
                arguments["project"],
                arguments["branch_id"],
                arguments.get("parent_branch", "production"),
            )
        elif name == "delete_branch":
            return _tool_delete_branch(
                arguments["branch"], arguments.get("confirm", False)
            )
        # Autoscaling config tools
        elif name == "configure_autoscaling":
            return _tool_configure_autoscaling(
                arguments["endpoint"], arguments["min_cu"], arguments["max_cu"]
            )
        elif name == "configure_scale_to_zero":
            return _tool_configure_scale_to_zero(
                arguments["endpoint"],
                arguments["enabled"],
                arguments.get("idle_timeout_seconds", 300),
            )
        # Data quality tools
        elif name == "profile_table":
            return _tool_profile_table(arguments["table_name"])
        # Neon-parity tools
        elif name == "describe_branch":
            return _tool_describe_branch(arguments.get("database"))
        elif name == "compare_database_schema":
            return _tool_compare_database_schema(
                arguments["source_database"], arguments["target_database"]
            )
        elif name == "prepare_database_migration":
            return _tool_prepare_database_migration(arguments["migration_sql"])
        elif name == "complete_database_migration":
            return _tool_complete_database_migration(
                arguments["migration_id"], arguments.get("apply_changes", True)
            )
        elif name == "prepare_query_tuning":
            return _tool_prepare_query_tuning(arguments["sql"])
        elif name == "complete_query_tuning":
            return _tool_complete_query_tuning(
                arguments["tuning_id"],
                arguments.get("apply_changes", False),
                arguments.get("ddl_statements"),
            )
        elif name == "search":
            return _tool_search(arguments["query"])
        else:
            return [TextContent(type="text", text=f"Unknown tool: {name}")]
    except Exception as e:
        logger.exception("Tool %s failed", name)
        return [TextContent(type="text", text=f"Error: {e}")]


# ── MCP Resources ────────────────────────────────────────────────────

@mcp_server.list_resources()
async def handle_list_resources():
    return [
        Resource(
            uri="lakebase://tables",
            name="All Tables",
            description="JSON list of all tables in the Lakebase database with row and column counts.",
            mimeType="application/json",
        ),
    ]


@mcp_server.list_resource_templates()
async def handle_list_resource_templates():
    return [
        ResourceTemplate(
            uriTemplate="lakebase://tables/{table_name}/schema",
            name="Table Schema",
            description="Column definitions for a specific table.",
            mimeType="application/json",
        ),
    ]


@mcp_server.read_resource()
async def handle_read_resource(uri):
    uri_str = str(uri)
    if uri_str == "lakebase://tables":
        result = _tool_list_tables()
        return result[0].text
    # Match lakebase://tables/{name}/schema
    m = re.match(r"^lakebase://tables/([^/]+)/schema$", uri_str)
    if m:
        table_name = m.group(1)
        result = _tool_describe_table(table_name)
        return result[0].text
    raise ValueError(f"Unknown resource URI: {uri_str}")


# ── MCP Prompts ──────────────────────────────────────────────────────

@mcp_server.list_prompts()
async def handle_list_prompts():
    return [
        Prompt(
            name="explore_database",
            description="Explore the Lakebase database: list tables, describe schemas, and suggest useful queries.",
            arguments=[],
        ),
        Prompt(
            name="design_schema",
            description="Design a database schema for a given description.",
            arguments=[
                PromptArgument(
                    name="description",
                    description="Description of the data model to design.",
                    required=True,
                ),
            ],
        ),
        Prompt(
            name="optimize_query",
            description="Analyze a SQL query with EXPLAIN ANALYZE and suggest improvements.",
            arguments=[
                PromptArgument(
                    name="sql",
                    description="The SQL query to optimize.",
                    required=True,
                ),
            ],
        ),
    ]


@mcp_server.get_prompt()
async def handle_get_prompt(name: str, arguments: dict | None = None):
    if name == "explore_database":
        return GetPromptResult(
            description="Explore the Lakebase database",
            messages=[
                PromptMessage(
                    role="user",
                    content=TextContent(
                        type="text",
                        text=(
                            "Explore this Lakebase database. Start by listing all tables, "
                            "then describe the schema of each table. Based on the schema, "
                            "suggest 5 useful analytical queries that would help understand "
                            "the data. Run at least 2 of those queries to show sample results."
                        ),
                    ),
                ),
            ],
        )
    elif name == "design_schema":
        description = (arguments or {}).get("description", "a general-purpose application")
        return GetPromptResult(
            description=f"Design schema for: {description}",
            messages=[
                PromptMessage(
                    role="user",
                    content=TextContent(
                        type="text",
                        text=(
                            f"Design a PostgreSQL database schema for: {description}\n\n"
                            "Requirements:\n"
                            "1. Use appropriate data types (including JSONB where useful)\n"
                            "2. Include primary keys, foreign keys, and indexes\n"
                            "3. Add created_at/updated_at timestamps\n"
                            "4. Provide the CREATE TABLE statements\n"
                            "5. Explain your design decisions\n"
                            "6. Offer to create the tables using the create_table tool"
                        ),
                    ),
                ),
            ],
        )
    elif name == "optimize_query":
        sql = (arguments or {}).get("sql", "SELECT 1")
        return GetPromptResult(
            description=f"Optimize query",
            messages=[
                PromptMessage(
                    role="user",
                    content=TextContent(
                        type="text",
                        text=(
                            f"Analyze and optimize this SQL query:\n\n```sql\n{sql}\n```\n\n"
                            "Steps:\n"
                            "1. Run EXPLAIN ANALYZE on the query using the explain_query tool\n"
                            "2. Identify performance bottlenecks (seq scans, high cost, etc.)\n"
                            "3. Suggest specific improvements (indexes, query rewrites, etc.)\n"
                            "4. If you suggest index creation, provide the CREATE INDEX statement"
                        ),
                    ),
                ),
            ],
        )
    raise ValueError(f"Unknown prompt: {name}")


# ── Tool implementations (original 6) ────────────────────────────────


def _tool_list_tables():
    rows = _execute_read("""
        SELECT t.table_name,
               t.table_schema,
               COALESCE(s.n_live_tup, 0) AS approx_row_count,
               (SELECT count(*) FROM information_schema.columns c
                WHERE c.table_name = t.table_name AND c.table_schema = t.table_schema) AS column_count
        FROM information_schema.tables t
        LEFT JOIN pg_stat_user_tables s ON s.relname = t.table_name AND s.schemaname = t.table_schema
        WHERE t.table_schema = 'public' AND t.table_type = 'BASE TABLE'
        ORDER BY t.table_name
    """)
    # Reshape to match expected output format
    result = []
    for row in rows:
        result.append({
            "table_name": row["table_name"],
            "row_count": row["approx_row_count"],
            "column_count": row["column_count"],
        })
    return [TextContent(type="text", text=json.dumps(result, indent=2))]


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
    # Reject semicolons to prevent statement stacking
    if ";" in stripped:
        return [TextContent(
            type="text",
            text="Error: Multiple statements (semicolons) are not allowed in read_query. Use execute_sql for multi-statement queries."
        )]
    # Enforce row limit
    upper = stripped.upper()
    if "LIMIT" not in upper:
        stripped = f"{stripped} LIMIT {MAX_READ_ROWS}"
    # Use SET TRANSACTION READ ONLY to enforce read-only mode
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION READ ONLY")
            cur.execute(stripped)
            rows = _rows_to_dicts(cur)
            conn.commit()
        return [TextContent(type="text", text=json.dumps(rows, indent=2, default=str))]
    except psycopg2.OperationalError:
        _put_conn(conn, close=True)
        conn = None
        # Retry once on stale connection
        conn = _get_conn()
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION READ ONLY")
            cur.execute(stripped)
            rows = _rows_to_dicts(cur)
            conn.commit()
        return [TextContent(type="text", text=json.dumps(rows, indent=2, default=str))]
    finally:
        if conn:
            _put_conn(conn)


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
    _sanitize_where(where)

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
    _sanitize_where(where)

    sql = f'DELETE FROM "{table_name}" WHERE {where} RETURNING *'
    rows = _execute_write(sql)
    return [TextContent(type="text", text=json.dumps(
        {"deleted": len(rows), "rows": rows}, indent=2, default=str
    ))]


# ── Tool implementations (P0: General SQL) ───────────────────────────


def _tool_execute_sql(sql):
    """Execute any SQL statement with auto-detection."""
    stripped = sql.strip().rstrip(";").strip()
    if not stripped:
        return [TextContent(type="text", text="Error: Empty SQL statement.")]

    sql_type = _classify_sql(stripped)

    if sql_type == "read":
        upper = stripped.upper()
        if "LIMIT" not in upper:
            stripped = f"{stripped} LIMIT {MAX_READ_ROWS}"
        rows = _execute_read(stripped)
        return [TextContent(type="text", text=json.dumps({
            "type": "read",
            "row_count": len(rows),
            "rows": rows,
        }, indent=2, default=str))]

    elif sql_type == "write":
        rows, rowcount, statusmessage = _execute_write_with_info(stripped)
        return [TextContent(type="text", text=json.dumps({
            "type": "write",
            "rowcount": rowcount,
            "status": statusmessage,
            "rows": rows,
        }, indent=2, default=str))]

    else:  # ddl
        _rows, rowcount, statusmessage = _execute_write_with_info(stripped)
        _invalidate_table_cache()
        return [TextContent(type="text", text=json.dumps({
            "type": "ddl",
            "status": statusmessage,
        }, indent=2, default=str))]


def _tool_execute_transaction(statements):
    """Execute multiple statements in one transaction."""
    statements = _ensure_list(statements, "statements")
    if not statements:
        return [TextContent(type="text", text="Error: No statements provided.")]

    conn = _get_conn()
    results = []
    has_ddl = False
    try:
        with conn.cursor() as cur:
            for i, stmt in enumerate(statements):
                stmt = stmt.strip().rstrip(";").strip()
                if not stmt:
                    continue
                try:
                    cur.execute(stmt)
                    rows = _rows_to_dicts(cur)
                    rowcount = cur.rowcount
                    statusmessage = cur.statusmessage
                    results.append({
                        "statement_index": i,
                        "sql": stmt[:200],
                        "rowcount": rowcount,
                        "status": statusmessage,
                        "rows": rows,
                    })
                    if _classify_sql(stmt) == "ddl":
                        has_ddl = True
                except Exception as e:
                    conn.rollback()
                    results.append({
                        "statement_index": i,
                        "sql": stmt[:200],
                        "error": str(e),
                    })
                    return [TextContent(type="text", text=json.dumps({
                        "status": "rolled_back",
                        "failed_at": i,
                        "error": str(e),
                        "results": results,
                    }, indent=2, default=str))]
            conn.commit()
            if has_ddl:
                _invalidate_table_cache()
            return [TextContent(type="text", text=json.dumps({
                "status": "committed",
                "statement_count": len(results),
                "results": results,
            }, indent=2, default=str))]
    except Exception as e:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            _put_conn(conn)


def _tool_explain_query(sql):
    """EXPLAIN ANALYZE a query; rollback writes to prevent side effects."""
    stripped = sql.strip().rstrip(";").strip()
    if not stripped:
        return [TextContent(type="text", text="Error: Empty SQL statement.")]

    sql_type = _classify_sql(stripped)
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            explain_sql = f"EXPLAIN (ANALYZE, FORMAT JSON) {stripped}"
            cur.execute(explain_sql)
            plan = _rows_to_dicts(cur)
            if sql_type in ("write", "ddl"):
                # Rollback to undo side effects of write/DDL
                conn.rollback()
            else:
                conn.commit()
            return [TextContent(type="text", text=json.dumps({
                "sql": stripped[:500],
                "type": sql_type,
                "rolled_back": sql_type in ("write", "ddl"),
                "plan": plan,
            }, indent=2, default=str))]
    except Exception as e:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            _put_conn(conn)


# ── Tool implementations (P0: DDL) ──────────────────────────────────


def _tool_create_table(table_name, columns, if_not_exists=True):
    """Create a table with column definitions."""
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table_name):
        return [TextContent(type="text", text=f"Error: Invalid table name: {table_name}")]

    columns = _ensure_list(columns, "columns")
    if not columns:
        return [TextContent(type="text", text="Error: At least one column is required.")]

    col_defs = []
    for col in columns:
        col = _ensure_dict(col, "column")
        name = col.get("name")
        ctype = col.get("type")
        if not name or not ctype:
            return [TextContent(type="text", text=f"Error: Each column needs 'name' and 'type'. Got: {col}")]
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
            return [TextContent(type="text", text=f"Error: Invalid column name: {name}")]
        _validate_column_type(ctype)
        constraint = col.get("constraints", "")
        if constraint:
            _validate_constraints(constraint)
        col_defs.append(f'"{name}" {ctype} {constraint}'.strip())

    exists_clause = "IF NOT EXISTS " if if_not_exists else ""
    ddl = f'CREATE TABLE {exists_clause}"{table_name}" (\n  ' + ",\n  ".join(col_defs) + "\n)"

    _rows, _rowcount, statusmessage = _execute_write_with_info(ddl)
    _invalidate_table_cache()
    return [TextContent(type="text", text=json.dumps({
        "status": statusmessage,
        "sql": ddl,
    }, indent=2))]


def _tool_drop_table(table_name, confirm, if_exists=True):
    """Drop a table with safety confirmation."""
    if not confirm:
        return [TextContent(type="text", text="Error: confirm must be true to drop a table. This is a safety measure.")]
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table_name):
        return [TextContent(type="text", text=f"Error: Invalid table name: {table_name}")]

    exists_clause = "IF EXISTS " if if_exists else ""
    ddl = f'DROP TABLE {exists_clause}"{table_name}"'

    _rows, _rowcount, statusmessage = _execute_write_with_info(ddl)
    _invalidate_table_cache()
    return [TextContent(type="text", text=json.dumps({
        "status": statusmessage,
        "table": table_name,
        "dropped": True,
    }, indent=2))]


def _tool_alter_table(table_name, operation, column_name, new_column_name=None, column_type=None, constraints=None):
    """Alter a table: add/drop/rename column or change type."""
    table_name = _validate_table(table_name)
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', column_name):
        return [TextContent(type="text", text=f"Error: Invalid column name: {column_name}")]

    if operation == "add_column":
        if not column_type:
            return [TextContent(type="text", text="Error: column_type is required for add_column.")]
        _validate_column_type(column_type)
        if constraints:
            _validate_constraints(constraints)
        constraint_str = f" {constraints}" if constraints else ""
        ddl = f'ALTER TABLE "{table_name}" ADD COLUMN "{column_name}" {column_type}{constraint_str}'
    elif operation == "drop_column":
        ddl = f'ALTER TABLE "{table_name}" DROP COLUMN "{column_name}"'
    elif operation == "rename_column":
        if not new_column_name:
            return [TextContent(type="text", text="Error: new_column_name is required for rename_column.")]
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', new_column_name):
            return [TextContent(type="text", text=f"Error: Invalid new column name: {new_column_name}")]
        ddl = f'ALTER TABLE "{table_name}" RENAME COLUMN "{column_name}" TO "{new_column_name}"'
    elif operation == "alter_type":
        if not column_type:
            return [TextContent(type="text", text="Error: column_type is required for alter_type.")]
        _validate_column_type(column_type)
        ddl = f'ALTER TABLE "{table_name}" ALTER COLUMN "{column_name}" TYPE {column_type}'
    else:
        return [TextContent(type="text", text=f"Error: Unknown operation: {operation}. Use add_column, drop_column, rename_column, or alter_type.")]

    _rows, _rowcount, statusmessage = _execute_write_with_info(ddl)
    _invalidate_table_cache()
    return [TextContent(type="text", text=json.dumps({
        "status": statusmessage,
        "sql": ddl,
    }, indent=2))]


# ── Tool implementations (P1) ───────────────────────────────────────


def _tool_list_slow_queries(limit=10):
    """Query pg_stat_statements for slow queries."""
    try:
        rows = _execute_read(
            """
            SELECT
                query,
                calls,
                total_exec_time AS total_time_ms,
                mean_exec_time AS mean_time_ms,
                rows
            FROM pg_stat_statements
            ORDER BY total_exec_time DESC
            LIMIT %s
            """,
            (limit,),
        )
        return [TextContent(type="text", text=json.dumps(rows, indent=2, default=str))]
    except Exception as e:
        err = str(e)
        if "pg_stat_statements" in err.lower() or "does not exist" in err.lower():
            return [TextContent(type="text", text=json.dumps({
                "error": "pg_stat_statements extension is not enabled on this database.",
                "hint": "Run: CREATE EXTENSION IF NOT EXISTS pg_stat_statements;",
            }, indent=2))]
        raise


def _tool_batch_insert(table_name, records):
    """Insert multiple records in one statement."""
    table_name = _validate_table(table_name)
    records = _ensure_list(records, "records")
    if not records:
        return [TextContent(type="text", text="Error: records must contain at least one record.")]

    # Ensure all records are dicts
    records = [_ensure_dict(r, f"records[{i}]") for i, r in enumerate(records)]

    # Get column union from all records, preserving order from first record
    all_columns = list(records[0].keys())
    col_set = set(all_columns)
    for r in records[1:]:
        for k in r.keys():
            if k not in col_set:
                all_columns.append(k)
                col_set.add(k)

    jsonb_cols = _get_jsonb_columns(table_name)
    col_list = ", ".join(f'"{c}"' for c in all_columns)
    row_placeholders = []
    all_values = []

    for record in records:
        placeholders = []
        for col in all_columns:
            val = record.get(col)
            if col in jsonb_cols and val is not None and not isinstance(val, str):
                all_values.append(json.dumps(val))
            else:
                all_values.append(val)
            placeholders.append("%s")
        row_placeholders.append(f"({', '.join(placeholders)})")

    values_clause = ", ".join(row_placeholders)
    sql = f'INSERT INTO "{table_name}" ({col_list}) VALUES {values_clause} RETURNING *'

    rows = _execute_write(sql, all_values)
    return [TextContent(type="text", text=json.dumps({
        "inserted": len(rows),
        "rows": rows,
    }, indent=2, default=str))]


def _tool_list_schemas():
    """List all schemas in the database."""
    rows = _execute_read(
        """
        SELECT
            schema_name,
            schema_owner
        FROM information_schema.schemata
        ORDER BY schema_name
        """
    )
    return [TextContent(type="text", text=json.dumps(rows, indent=2))]


def _tool_get_connection_info():
    """Return connection info (no password)."""
    info = {
        "connection_mode": _connection_mode or "unknown",
        "host": os.environ.get("PGHOST", ""),
        "port": int(os.environ.get("PGPORT", "5432")),
        "database": _current_database or os.environ.get("PGDATABASE", ""),
        "user": os.environ.get("PGUSER", ""),
        "sslmode": os.environ.get("PGSSLMODE", "require"),
    }
    if _connection_mode == "autoscaling":
        info["project"] = os.environ.get("LAKEBASE_PROJECT", "")
        info["branch"] = os.environ.get("LAKEBASE_BRANCH", "production")
        info["endpoint"] = _autoscale_endpoint or ""
    return [TextContent(type="text", text=json.dumps(info, indent=2))]


# ── Tool implementations (Infrastructure) ────────────────────────────


def _tool_list_projects():
    """List all Lakebase instances (provisioned) and projects (autoscaling)."""
    w = _get_ws()
    results = []

    # Try provisioned instances via REST API (most reliable across SDK versions)
    try:
        resp = w.api_client.do("GET", "/api/2.0/database/instances")
        instances = resp.get("database_instances", [])
        for inst in instances:
            inst["instance_type"] = "provisioned"
            results.append(inst)
    except Exception as e:
        logger.debug("Provisioned instances REST API failed: %s", e)
        # Fallback: try SDK method
        try:
            instances = list(w.database.list_instances())
            for inst in instances:
                entry = _serialize_proto(inst)
                if isinstance(entry, dict):
                    entry["instance_type"] = "provisioned"
                    if "state" in entry:
                        entry["state"] = str(entry["state"]).replace("DatabaseInstanceState.", "")
                else:
                    entry = {"raw": str(inst), "instance_type": "provisioned"}
                results.append(entry)
        except Exception as e2:
            logger.debug("Provisioned instances SDK failed: %s", e2)

    # Try autoscaling projects via REST API
    try:
        resp = w.api_client.do("GET", "/api/2.0/postgres/projects")
        projects = resp.get("projects", [])
        for p in projects:
            p["instance_type"] = "autoscaling"
            results.append(p)
    except Exception as e:
        logger.debug("Autoscaling projects REST API failed: %s", e)
        # Fallback: try SDK method
        try:
            projects = list(w.postgres.list_projects())
            for p in projects:
                entry = _serialize_proto(p)
                if isinstance(entry, dict):
                    entry["instance_type"] = "autoscaling"
                else:
                    entry = {"raw": str(p), "instance_type": "autoscaling"}
                results.append(entry)
        except Exception as e2:
            logger.debug("Autoscaling projects SDK failed: %s", e2)

    if not results:
        return [TextContent(type="text", text=json.dumps({
            "error": "No Lakebase instances or projects found.",
            "hint": "Ensure the workspace has Lakebase resources and the app SP has permissions.",
        }, indent=2))]

    return [TextContent(type="text", text=json.dumps(results, indent=2, default=str))]


def _tool_describe_project(project: str):
    """Get detailed info about a project or provisioned instance."""
    w = _get_ws()

    # Try provisioned instance via REST API
    try:
        resp = w.api_client.do("GET", f"/api/2.0/database/instances/{project}")
        resp["instance_type"] = "provisioned"
        return [TextContent(type="text", text=json.dumps(resp, indent=2, default=str))]
    except Exception as e:
        logger.debug("Provisioned instance REST describe failed for %s: %s", project, e)

    # Try autoscaling project
    try:
        project_path = _resolve_project(project)
        result = w.api_client.do("GET", f"/api/2.0/postgres/{project_path}")
        if isinstance(result, dict):
            result["instance_type"] = "autoscaling"
        # Also list branches
        try:
            br_resp = w.api_client.do("GET", f"/api/2.0/postgres/{project_path}/branches")
            result["branches"] = br_resp.get("branches", [])
        except Exception:
            result["branches"] = []
        return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]
    except Exception as e:
        return [TextContent(type="text", text=json.dumps({"error": str(e)}, indent=2))]


def _tool_get_connection_string(endpoint: str, fmt: str = "psql"):
    """Build a connection string for an endpoint."""
    try:
        w = _get_ws()
        ep = w.api_client.do("GET", f"/api/2.0/postgres/{endpoint}")
        ep_status = ep.get("status", {})
        ep_hosts = ep_status.get("hosts", {}) if isinstance(ep_status, dict) else {}
        host = ep_hosts.get("host", "") if isinstance(ep_hosts, dict) else ""
        if not host:
            return [TextContent(type="text", text=json.dumps({
                "error": f"Endpoint {endpoint} has no host. State: {ep.get('state', 'unknown')}",
            }, indent=2))]
        port = 5432
        user = "<your-username>"
        db = "databricks_postgres"
        if fmt == "psql":
            conn_str = f"psql 'host={host} port={port} dbname={db} user={user} sslmode=require'"
        elif fmt == "psycopg2":
            conn_str = f"psycopg2.connect(host='{host}', port={port}, dbname='{db}', user='{user}', password='<token>', sslmode='require')"
        elif fmt == "jdbc":
            conn_str = f"jdbc:postgresql://{host}:{port}/{db}?sslmode=require&user={user}"
        else:
            conn_str = f"host={host} port={port} dbname={db} user={user} sslmode=require"
        return [TextContent(type="text", text=json.dumps({
            "format": fmt,
            "connection_string": conn_str,
            "host": host,
            "port": port,
            "endpoint": endpoint,
        }, indent=2))]
    except Exception as e:
        return [TextContent(type="text", text=json.dumps({"error": str(e)}, indent=2))]


def _tool_list_branches(project: str):
    """List branches on a project."""
    try:
        w = _get_ws()
        project_path = _resolve_project(project)
        resp = w.api_client.do("GET", f"/api/2.0/postgres/{project_path}/branches")
        branches = resp.get("branches", [])
        return [TextContent(type="text", text=json.dumps(branches, indent=2, default=str))]
    except Exception as e:
        return [TextContent(type="text", text=json.dumps({"error": str(e)}, indent=2))]


def _tool_list_endpoints(project: str, branch: str = "production"):
    """List endpoints on a branch."""
    try:
        w = _get_ws()
        branch_path = _resolve_branch(project, branch)
        resp = w.api_client.do("GET", f"/api/2.0/postgres/{branch_path}/endpoints")
        endpoints = resp.get("endpoints", [])
        return [TextContent(type="text", text=json.dumps(endpoints, indent=2, default=str))]
    except Exception as e:
        return [TextContent(type="text", text=json.dumps({"error": str(e)}, indent=2))]


def _tool_get_endpoint_status(endpoint: str):
    """Get endpoint state, host, compute config."""
    try:
        w = _get_ws()
        resp = w.api_client.do("GET", f"/api/2.0/postgres/{endpoint}")
        return [TextContent(type="text", text=json.dumps(resp, indent=2, default=str))]
    except Exception as e:
        return [TextContent(type="text", text=json.dumps({"error": str(e)}, indent=2))]


# ── Tool implementations (Branch management) ─────────────────────────


def _tool_create_branch(project: str, branch_id: str, parent_branch: str = "production"):
    """Create a new branch."""
    try:
        w = _get_ws()
        project_path = _resolve_project(project)
        parent_path = _resolve_branch(project, parent_branch)
        resp = w.api_client.do("POST", f"/api/2.0/postgres/{project_path}/branches", body={
            "branch": {"parent_branch": parent_path},
            "branch_id": branch_id,
        })
        return [TextContent(type="text", text=json.dumps(resp, indent=2, default=str))]
    except Exception as e:
        return [TextContent(type="text", text=json.dumps({"error": str(e)}, indent=2))]


def _tool_delete_branch(branch: str, confirm: bool = False):
    """Delete a branch."""
    if not confirm:
        return [TextContent(type="text", text="Error: confirm must be true to delete a branch. This is a safety measure.")]
    if branch.endswith("/production"):
        return [TextContent(type="text", text="Error: Cannot delete the production branch.")]
    try:
        w = _get_ws()
        w.api_client.do("DELETE", f"/api/2.0/postgres/{branch}")
        return [TextContent(type="text", text=json.dumps({
            "deleted": True,
            "branch": branch,
        }, indent=2))]
    except Exception as e:
        return [TextContent(type="text", text=json.dumps({"error": str(e)}, indent=2))]


# ── Tool implementations (Autoscaling config) ────────────────────────


def _tool_configure_autoscaling(endpoint: str, min_cu: float, max_cu: float):
    """Configure autoscaling min/max CU."""
    try:
        w = _get_ws()
        resp = w.api_client.do("PATCH", f"/api/2.0/postgres/{endpoint}", body={
            "endpoint": {
                "name": endpoint,
                "autoscaling": {"min_cu": min_cu, "max_cu": max_cu},
            },
            "update_mask": "autoscaling.min_cu,autoscaling.max_cu",
        })
        return [TextContent(type="text", text=json.dumps(resp, indent=2, default=str))]
    except Exception as e:
        return [TextContent(type="text", text=json.dumps({"error": str(e)}, indent=2))]


def _tool_configure_scale_to_zero(endpoint: str, enabled: bool, idle_timeout_seconds: int = 300):
    """Enable/disable scale-to-zero with idle timeout."""
    try:
        w = _get_ws()
        resp = w.api_client.do("PATCH", f"/api/2.0/postgres/{endpoint}", body={
            "endpoint": {
                "name": endpoint,
                "autoscaling": {
                    "scale_to_zero": {
                        "enabled": enabled,
                        "idle_timeout_seconds": idle_timeout_seconds,
                    },
                },
            },
            "update_mask": "autoscaling.scale_to_zero",
        })
        return [TextContent(type="text", text=json.dumps(resp, indent=2, default=str))]
    except Exception as e:
        return [TextContent(type="text", text=json.dumps({"error": str(e)}, indent=2))]


# ── Tool implementations (Data quality) ──────────────────────────────


def _tool_profile_table(table_name: str):
    """Generate column-level profiling statistics."""
    table_name = _validate_table(table_name)

    # Get columns and types
    columns = _execute_read(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position
        """,
        (table_name,),
    )
    if not columns:
        return [TextContent(type="text", text=json.dumps({"error": f"No columns found for table {table_name}"}, indent=2))]

    # Build profiling query
    numeric_types = {"integer", "bigint", "smallint", "numeric", "real", "double precision", "decimal"}
    select_parts = [f'count(*) AS "total_rows"']
    for col in columns:
        cn = col["column_name"]
        safe_cn = cn.replace('"', '""')
        select_parts.append(f'count("{safe_cn}") AS "{safe_cn}__non_null"')
        select_parts.append(f'count(DISTINCT "{safe_cn}") AS "{safe_cn}__distinct"')
        if col["data_type"] in numeric_types:
            select_parts.append(f'min("{safe_cn}") AS "{safe_cn}__min"')
            select_parts.append(f'max("{safe_cn}") AS "{safe_cn}__max"')
            select_parts.append(f'avg("{safe_cn}")::numeric(20,4) AS "{safe_cn}__avg"')
        else:
            select_parts.append(f'min("{safe_cn}"::text) AS "{safe_cn}__min"')
            select_parts.append(f'max("{safe_cn}"::text) AS "{safe_cn}__max"')

    profile_sql = f'SELECT {", ".join(select_parts)} FROM "{table_name}"'
    raw = _execute_read(profile_sql)
    if not raw:
        return [TextContent(type="text", text=json.dumps({"error": "Profile query returned no results"}, indent=2))]

    row = raw[0]
    total_rows = row["total_rows"]

    # Reshape into per-column stats
    profile = []
    for col in columns:
        cn = col["column_name"]
        non_null = row.get(f"{cn}__non_null", 0)
        stats = {
            "column": cn,
            "type": col["data_type"],
            "total_rows": total_rows,
            "non_null": non_null,
            "null_count": total_rows - non_null,
            "null_pct": round((total_rows - non_null) / total_rows * 100, 1) if total_rows > 0 else 0,
            "distinct": row.get(f"{cn}__distinct", 0),
            "min": row.get(f"{cn}__min"),
            "max": row.get(f"{cn}__max"),
        }
        if col["data_type"] in numeric_types:
            stats["avg"] = row.get(f"{cn}__avg")
        profile.append(stats)

    return [TextContent(type="text", text=json.dumps({
        "table": table_name,
        "total_rows": total_rows,
        "columns": profile,
    }, indent=2, default=str))]


# ── Tool implementations (Neon-parity) ───────────────────────────────


def _get_schema_snapshot(database: str | None = None):
    """Get a full schema snapshot for a database. Uses request-scoped pool if database specified."""
    token = None
    if database:
        token = _request_database.set(database)
    try:
        schemas = _execute_read(
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast') "
            "ORDER BY schema_name"
        )
        result = {}
        for s in schemas:
            sn = s["schema_name"]
            tables = _execute_read(
                "SELECT table_name, table_type FROM information_schema.tables "
                "WHERE table_schema = %s ORDER BY table_name", (sn,)
            )
            columns_by_table = {}
            for t in tables:
                cols = _execute_read(
                    "SELECT column_name, data_type, is_nullable, column_default "
                    "FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = %s "
                    "ORDER BY ordinal_position", (sn, t["table_name"])
                )
                columns_by_table[t["table_name"]] = {
                    "type": t["table_type"],
                    "columns": cols,
                }
            result[sn] = columns_by_table
        return result
    finally:
        if token is not None:
            _request_database.reset(token)


def _tool_describe_branch(database: str | None = None):
    """Tree view of all objects in a database."""
    try:
        if database and not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', database):
            return [TextContent(type="text", text=f"Error: Invalid database name: {database}")]
        db_name = database or _current_database or os.environ.get("PGDATABASE", "") or os.environ.get("LAKEBASE_DATABASE", "")
        token = None
        if database:
            token = _request_database.set(database)
        try:
            tree = {"database": db_name, "schemas": {}}
            schemas = _execute_read(
                "SELECT schema_name FROM information_schema.schemata "
                "WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast') "
                "ORDER BY schema_name"
            )
            for s in schemas:
                sn = s["schema_name"]
                schema_obj = {"tables": [], "views": [], "functions": [], "sequences": [], "indexes": []}
                # Tables and views
                tables = _execute_read(
                    "SELECT table_name, table_type FROM information_schema.tables "
                    "WHERE table_schema = %s ORDER BY table_name", (sn,)
                )
                for t in tables:
                    cols = _execute_read(
                        "SELECT column_name, data_type, is_nullable "
                        "FROM information_schema.columns "
                        "WHERE table_schema = %s AND table_name = %s "
                        "ORDER BY ordinal_position", (sn, t["table_name"])
                    )
                    entry = {"name": t["table_name"], "columns": cols}
                    if t["table_type"] == "VIEW":
                        schema_obj["views"].append(entry)
                    else:
                        schema_obj["tables"].append(entry)
                # Functions
                funcs = _execute_read(
                    "SELECT routine_name, routine_type, data_type AS return_type "
                    "FROM information_schema.routines "
                    "WHERE routine_schema = %s ORDER BY routine_name", (sn,)
                )
                schema_obj["functions"] = funcs
                # Sequences
                seqs = _execute_read(
                    "SELECT sequence_name FROM information_schema.sequences "
                    "WHERE sequence_schema = %s ORDER BY sequence_name", (sn,)
                )
                schema_obj["sequences"] = [s["sequence_name"] for s in seqs]
                # Indexes
                idxs = _execute_read(
                    "SELECT indexname, tablename, indexdef FROM pg_indexes "
                    "WHERE schemaname = %s ORDER BY indexname", (sn,)
                )
                schema_obj["indexes"] = idxs
                tree["schemas"][sn] = schema_obj
            return [TextContent(type="text", text=json.dumps(tree, indent=2, default=str))]
        finally:
            if token is not None:
                _request_database.reset(token)
    except Exception as e:
        return [TextContent(type="text", text=json.dumps({"error": str(e)}, indent=2))]


def _tool_compare_database_schema(source_database: str, target_database: str):
    """Compare schemas between two databases, return unified diff."""
    for db in (source_database, target_database):
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', db):
            return [TextContent(type="text", text=f"Error: Invalid database name: {db}")]
    try:
        source_schema = _get_schema_snapshot(source_database)
        target_schema = _get_schema_snapshot(target_database)

        diffs = []
        all_schemas = sorted(set(list(source_schema.keys()) + list(target_schema.keys())))

        for schema_name in all_schemas:
            src_tables = source_schema.get(schema_name, {})
            tgt_tables = target_schema.get(schema_name, {})
            all_tables = sorted(set(list(src_tables.keys()) + list(tgt_tables.keys())))

            for table_name in all_tables:
                src = src_tables.get(table_name)
                tgt = tgt_tables.get(table_name)

                if src and not tgt:
                    diffs.append({
                        "schema": schema_name,
                        "table": table_name,
                        "change": "dropped",
                        "detail": f"Table {schema_name}.{table_name} exists in {source_database} but not in {target_database}",
                    })
                elif tgt and not src:
                    diffs.append({
                        "schema": schema_name,
                        "table": table_name,
                        "change": "added",
                        "detail": f"Table {schema_name}.{table_name} exists in {target_database} but not in {source_database}",
                        "columns": tgt.get("columns", []),
                    })
                else:
                    # Compare columns
                    src_cols = {c["column_name"]: c for c in (src or {}).get("columns", [])}
                    tgt_cols = {c["column_name"]: c for c in (tgt or {}).get("columns", [])}
                    col_diffs = []
                    for cn in sorted(set(list(src_cols.keys()) + list(tgt_cols.keys()))):
                        sc = src_cols.get(cn)
                        tc = tgt_cols.get(cn)
                        if sc and not tc:
                            col_diffs.append({"column": cn, "change": "dropped"})
                        elif tc and not sc:
                            col_diffs.append({"column": cn, "change": "added", "type": tc.get("data_type")})
                        elif sc and tc:
                            changes = {}
                            if sc.get("data_type") != tc.get("data_type"):
                                changes["type"] = {"from": sc.get("data_type"), "to": tc.get("data_type")}
                            if sc.get("is_nullable") != tc.get("is_nullable"):
                                changes["nullable"] = {"from": sc.get("is_nullable"), "to": tc.get("is_nullable")}
                            if changes:
                                col_diffs.append({"column": cn, "change": "modified", **changes})
                    if col_diffs:
                        diffs.append({
                            "schema": schema_name,
                            "table": table_name,
                            "change": "modified",
                            "column_diffs": col_diffs,
                        })

        return [TextContent(type="text", text=json.dumps({
            "source": source_database,
            "target": target_database,
            "diff_count": len(diffs),
            "diffs": diffs,
        }, indent=2, default=str))]
    except Exception as e:
        return [TextContent(type="text", text=json.dumps({"error": str(e)}, indent=2))]


def _split_sql_statements(sql: str) -> list[str]:
    """Split SQL on semicolons, respecting single-quoted and dollar-quoted strings."""
    stmts = []
    current = []
    in_single_quote = False
    in_dollar_quote = False
    dollar_tag = ""
    i = 0
    while i < len(sql):
        ch = sql[i]
        if in_single_quote:
            current.append(ch)
            if ch == "'" and (i + 1 >= len(sql) or sql[i + 1] != "'"):
                in_single_quote = False
            elif ch == "'" and i + 1 < len(sql) and sql[i + 1] == "'":
                current.append(sql[i + 1])
                i += 1  # skip escaped quote
        elif in_dollar_quote:
            current.append(ch)
            if ch == "$" and sql[i:i + len(dollar_tag)] == dollar_tag:
                current.extend(list(dollar_tag[1:]))
                i += len(dollar_tag) - 1
                in_dollar_quote = False
        elif ch == "'":
            in_single_quote = True
            current.append(ch)
        elif ch == "$" and not in_single_quote:
            # Check for dollar-quoting like $$ or $tag$
            end = sql.find("$", i + 1)
            if end != -1:
                tag = sql[i:end + 1]
                if re.match(r'^\$[a-zA-Z0-9_]*\$$', tag):
                    dollar_tag = tag
                    in_dollar_quote = True
                    current.extend(list(tag))
                    i = end
                else:
                    current.append(ch)
            else:
                current.append(ch)
        elif ch == ";":
            stmt = "".join(current).strip()
            if stmt:
                stmts.append(stmt)
            current = []
        else:
            current.append(ch)
        i += 1
    last = "".join(current).strip()
    if last:
        stmts.append(last)
    return stmts


def _tool_prepare_database_migration(migration_sql: str):
    """Phase 1: validate migration by dry-running in a rolled-back transaction."""
    # Clean up stale entries older than 1 hour
    now = time.time()
    stale = [k for k, v in _migration_state.items() if now - v.get("created_at", 0) > 3600]
    for k in stale:
        _migration_state.pop(k, None)

    # Enforce maximum migration state entries
    if len(_migration_state) >= MAX_MIGRATION_STATES:
        raise RuntimeError(
            f"Maximum number of pending migrations/tuning sessions ({MAX_MIGRATION_STATES}) reached. "
            "Complete or discard existing migrations before creating new ones."
        )

    migration_id = str(uuid.uuid4())[:8]
    stripped = migration_sql.strip()
    if not stripped:
        return [TextContent(type="text", text="Error: Empty migration SQL.")]

    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            # Capture pre-migration state
            cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename")
            pre_tables = {r[0] for r in cur.fetchall()}

            # Run migration in transaction (split respecting string literals)
            statements = _split_sql_statements(stripped)
            results = []
            for i, stmt in enumerate(statements):
                try:
                    cur.execute(stmt)
                    results.append({
                        "index": i,
                        "sql": stmt[:200],
                        "status": cur.statusmessage,
                        "rowcount": cur.rowcount,
                    })
                except Exception as e:
                    conn.rollback()
                    return [TextContent(type="text", text=json.dumps({
                        "migration_id": migration_id,
                        "status": "validation_failed",
                        "failed_at": i,
                        "error": str(e),
                        "sql": stmt[:200],
                        "results": results,
                    }, indent=2, default=str))]

            # Capture post-migration state
            cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename")
            post_tables = {r[0] for r in cur.fetchall()}

            added_tables = sorted(post_tables - pre_tables)
            dropped_tables = sorted(pre_tables - post_tables)

            # Rollback — no actual changes
            conn.rollback()

        # Store state for complete step
        _migration_state[migration_id] = {
            "migration_sql": stripped,
            "statements": statements,
            "created_at": time.time(),
        }

        return [TextContent(type="text", text=json.dumps({
            "migration_id": migration_id,
            "status": "validated",
            "statement_count": len(statements),
            "results": results,
            "tables_added": added_tables,
            "tables_dropped": dropped_tables,
            "hint": f"Call complete_database_migration(migration_id='{migration_id}', apply_changes=true) to apply.",
        }, indent=2, default=str))]
    except Exception as e:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            _put_conn(conn)


def _tool_complete_database_migration(migration_id: str, apply_changes: bool = True):
    """Phase 2: apply or discard a validated migration."""
    state = _migration_state.pop(migration_id, None)
    if not state:
        return [TextContent(type="text", text=json.dumps({
            "error": f"Migration '{migration_id}' not found. It may have expired or already been completed.",
            "hint": "Run prepare_database_migration again to create a new migration.",
        }, indent=2))]

    if not apply_changes:
        return [TextContent(type="text", text=json.dumps({
            "migration_id": migration_id,
            "status": "discarded",
            "message": "Migration discarded. No changes were made.",
        }, indent=2))]

    # Apply for real
    statements = state["statements"]
    conn = _get_conn()
    results = []
    try:
        with conn.cursor() as cur:
            for i, stmt in enumerate(statements):
                try:
                    cur.execute(stmt)
                    results.append({
                        "index": i,
                        "sql": stmt[:200],
                        "status": cur.statusmessage,
                        "rowcount": cur.rowcount,
                    })
                except Exception as e:
                    conn.rollback()
                    return [TextContent(type="text", text=json.dumps({
                        "migration_id": migration_id,
                        "status": "failed",
                        "failed_at": i,
                        "error": str(e),
                        "results": results,
                    }, indent=2, default=str))]
            conn.commit()
        _invalidate_table_cache()
        return [TextContent(type="text", text=json.dumps({
            "migration_id": migration_id,
            "status": "applied",
            "statement_count": len(results),
            "results": results,
        }, indent=2, default=str))]
    except Exception as e:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            _put_conn(conn)


def _tool_prepare_query_tuning(sql: str):
    """Phase 1: analyze query and suggest optimizations."""
    stripped = sql.strip().rstrip(";").strip()
    if not stripped:
        return [TextContent(type="text", text="Error: Empty SQL statement.")]

    # Clean up stale entries older than 1 hour
    now = time.time()
    stale = [k for k, v in _migration_state.items() if now - v.get("created_at", 0) > 3600]
    for k in stale:
        _migration_state.pop(k, None)

    # Enforce maximum migration state entries
    if len(_migration_state) >= MAX_MIGRATION_STATES:
        raise RuntimeError(
            f"Maximum number of pending migrations/tuning sessions ({MAX_MIGRATION_STATES}) reached. "
            "Complete or discard existing sessions before creating new ones."
        )

    tuning_id = str(uuid.uuid4())[:8]
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            # Get execution plan
            cur.execute(f"EXPLAIN (ANALYZE, FORMAT JSON) {stripped}")
            plan_rows = _rows_to_dicts(cur)
            conn.rollback()  # rollback in case of writes

            plan = plan_rows[0] if plan_rows else {}
            plan_json = plan.get("QUERY PLAN", plan)

            # Analyze for sequential scans and suggest indexes
            suggestions = []

            # Detect seq scans on large tables
            cur.execute(f"EXPLAIN (FORMAT JSON) {stripped}")
            explain_rows = _rows_to_dicts(cur)
            explain_json = json.dumps(explain_rows)

            if "Seq Scan" in explain_json:
                # Find tables with seq scans
                cur.execute(f"EXPLAIN (FORMAT TEXT) {stripped}")
                text_plan = cur.fetchall()
                for row in text_plan:
                    line = row[0] if row else ""
                    if "Seq Scan on" in line:
                        table_match = re.search(r'Seq Scan on (\w+)', line)
                        if table_match:
                            table = table_match.group(1)
                            suggestions.append({
                                "type": "index",
                                "reason": f"Sequential scan detected on table '{table}'",
                                "suggestion": f"Consider adding an index on '{table}' for the columns used in WHERE/JOIN clauses",
                            })

            # Check for missing indexes on WHERE columns
            upper_sql = stripped.upper()
            if "WHERE" in upper_sql:
                suggestions.append({
                    "type": "general",
                    "suggestion": "Review WHERE clause columns — adding indexes on frequently filtered columns can improve performance",
                })

            # Store for complete step
            _migration_state[tuning_id] = {
                "type": "tuning",
                "sql": stripped,
                "suggestions": suggestions,
                "created_at": time.time(),
            }

            return [TextContent(type="text", text=json.dumps({
                "tuning_id": tuning_id,
                "sql": stripped[:500],
                "plan": plan_json,
                "suggestions": suggestions,
                "hint": f"Call complete_query_tuning(tuning_id='{tuning_id}', apply_changes=true, ddl_statements=[...]) to apply.",
            }, indent=2, default=str))]
    except Exception as e:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            _put_conn(conn)


def _tool_complete_query_tuning(tuning_id: str, apply_changes: bool = False, ddl_statements=None):
    """Phase 2: apply or discard query tuning suggestions."""
    state = _migration_state.pop(tuning_id, None)
    if not state:
        return [TextContent(type="text", text=json.dumps({
            "error": f"Tuning session '{tuning_id}' not found.",
            "hint": "Run prepare_query_tuning again.",
        }, indent=2))]

    if not apply_changes:
        return [TextContent(type="text", text=json.dumps({
            "tuning_id": tuning_id,
            "status": "discarded",
        }, indent=2))]

    # Apply DDL statements
    if ddl_statements is None:
        return [TextContent(type="text", text=json.dumps({
            "error": "ddl_statements required when apply_changes=true",
            "hint": "Provide CREATE INDEX or other DDL statements to apply.",
        }, indent=2))]

    ddl_statements = _ensure_list(ddl_statements, "ddl_statements")
    conn = _get_conn()
    results = []
    try:
        with conn.cursor() as cur:
            for i, ddl in enumerate(ddl_statements):
                ddl = ddl.strip().rstrip(";").strip()
                if not ddl:
                    continue
                try:
                    cur.execute(ddl)
                    results.append({"index": i, "sql": ddl[:200], "status": cur.statusmessage})
                except Exception as e:
                    conn.rollback()
                    return [TextContent(type="text", text=json.dumps({
                        "tuning_id": tuning_id,
                        "status": "failed",
                        "failed_at": i,
                        "error": str(e),
                        "results": results,
                    }, indent=2, default=str))]
            conn.commit()

        # Re-run EXPLAIN to show improvement (only safe for read queries)
        new_plan = {}
        if _classify_sql(state['sql']) == 'read':
            conn2 = _get_conn()
            try:
                with conn2.cursor() as cur2:
                    cur2.execute(f"EXPLAIN (ANALYZE, FORMAT JSON) {state['sql']}")
                    plan_rows = _rows_to_dicts(cur2)
                    new_plan = plan_rows[0] if plan_rows else {}
                    conn2.rollback()
            finally:
                _put_conn(conn2)

        return [TextContent(type="text", text=json.dumps({
            "tuning_id": tuning_id,
            "status": "applied",
            "ddl_applied": len(results),
            "results": results,
            "new_plan": new_plan,
        }, indent=2, default=str))]
    except Exception as e:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            _put_conn(conn)


def _tool_search(query: str):
    """Search across instances, databases, and tables."""
    if len(query) < 2:
        return [TextContent(type="text", text="Error: Search query must be at least 2 characters.")]

    query_lower = query.lower()
    results = {"instances": [], "databases": [], "tables": []}

    # Search instances/projects
    try:
        w = _get_ws()
        # Provisioned instances
        try:
            resp = w.api_client.do("GET", "/api/2.0/database/instances")
            for inst in resp.get("database_instances", []):
                name = inst.get("name", "")
                if query_lower in name.lower():
                    results["instances"].append({
                        "name": name,
                        "type": "provisioned",
                        "state": inst.get("state", ""),
                        "host": inst.get("read_write_dns", ""),
                    })
        except Exception:
            pass
        # Autoscaling projects
        try:
            resp = w.api_client.do("GET", "/api/2.0/postgres/projects")
            for p in resp.get("projects", []):
                name = p.get("display_name", p.get("name", ""))
                if query_lower in name.lower():
                    results["instances"].append({
                        "name": name,
                        "type": "autoscaling",
                        "state": p.get("state", ""),
                    })
        except Exception:
            pass
    except Exception:
        pass

    # Search databases on current instance
    try:
        dbs = _execute_read(
            "SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname"
        )
        for db in dbs:
            if query_lower in db["datname"].lower():
                results["databases"].append(db["datname"])
    except Exception:
        pass

    # Search tables in current database
    try:
        tables = _execute_read(
            "SELECT tablename, schemaname FROM pg_tables "
            "WHERE schemaname NOT IN ('pg_catalog', 'information_schema') "
            "ORDER BY tablename"
        )
        for t in tables:
            if query_lower in t["tablename"].lower():
                results["tables"].append({
                    "table": t["tablename"],
                    "schema": t["schemaname"],
                })
    except Exception:
        pass

    total = len(results["instances"]) + len(results["databases"]) + len(results["tables"])
    return [TextContent(type="text", text=json.dumps({
        "query": query,
        "total_matches": total,
        **results,
    }, indent=2, default=str))]


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
        text = result[0].text
        if text.startswith("Error"):
            return JSONResponse({"error": text}, status_code=400)
        data = json.loads(text)
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


async def api_execute(request: Request):
    """Execute any SQL statement."""
    body = await request.json()
    sql = body.get("sql", "")
    try:
        result = _tool_execute_sql(sql)
        return JSONResponse(json.loads(result[0].text))
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)


async def api_transaction(request: Request):
    """Execute a transaction."""
    body = await request.json()
    statements = body.get("statements", [])
    try:
        result = _tool_execute_transaction(statements)
        return JSONResponse(json.loads(result[0].text))
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)


async def api_explain(request: Request):
    """Explain a query."""
    body = await request.json()
    sql = body.get("sql", "")
    try:
        result = _tool_explain_query(sql)
        return JSONResponse(json.loads(result[0].text))
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)


async def api_create_table(request: Request):
    """Create a table."""
    body = await request.json()
    try:
        result = _tool_create_table(
            body["table_name"],
            body["columns"],
            body.get("if_not_exists", True),
        )
        return JSONResponse(json.loads(result[0].text))
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)


async def api_drop_table(request: Request):
    """Drop a table."""
    body = await request.json()
    try:
        result = _tool_drop_table(
            body["table_name"],
            body.get("confirm", False),
            body.get("if_exists", True),
        )
        return JSONResponse(json.loads(result[0].text))
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)


async def api_alter_table(request: Request):
    """Alter a table."""
    body = await request.json()
    try:
        result = _tool_alter_table(
            body["table_name"],
            body["operation"],
            body["column_name"],
            new_column_name=body.get("new_column_name"),
            column_type=body.get("column_type"),
            constraints=body.get("constraints"),
        )
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


async def api_databases(request: Request):
    """List all databases on the Lakebase instance."""
    try:
        rows = _execute_read(
            "SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname"
        )
        current = _current_database or os.environ.get("PGDATABASE", "") or os.environ.get("LAKEBASE_DATABASE", "")
        # Determine current instance name
        instance = _current_instance
        if not instance:
            # Try to extract from app.yaml config — fall back to PGHOST
            pg_host = os.environ.get("PGHOST", "")
            instance = pg_host.split(".")[0].replace("instance-", "") if pg_host else "unknown"
        return JSONResponse({
            "current": current,
            "instance": instance,
            "databases": [r["datname"] for r in rows],
        })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


async def api_switch_database(request: Request):
    """Switch to a different database on the same Lakebase instance."""
    global _current_database, _pg_pool
    body = await request.json()
    new_db = body.get("database", "").strip()
    if not new_db:
        return JSONResponse({"error": "database name is required"}, status_code=400)
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', new_db):
        return JSONResponse({"error": f"Invalid database name: {new_db}"}, status_code=400)
    try:
        old_db = _current_database or os.environ.get("PGDATABASE", "") or os.environ.get("LAKEBASE_DATABASE", "")
        _current_database = new_db
        # Close existing pool and reinitialize with new database
        if _pg_pool:
            try:
                _pg_pool.closeall()
            except Exception:
                pass
            _pg_pool = None
        _invalidate_table_cache()
        _init_pg_pool()
        # Verify connectivity
        _execute_read("SELECT 1")
        logger.info("Switched database: %s -> %s", old_db, new_db)
        return JSONResponse({"switched": True, "from": old_db, "to": new_db})
    except Exception as e:
        # Roll back to old database
        _current_database = None
        if _pg_pool:
            try:
                _pg_pool.closeall()
            except Exception:
                pass
            _pg_pool = None
        try:
            _init_pg_pool()
        except Exception:
            pass
        return JSONResponse({"error": f"Failed to switch to '{new_db}': {e}"}, status_code=400)


async def api_switch_instance(request: Request):
    """Switch to a different Lakebase instance (provisioned or autoscaling)."""
    global _current_instance, _current_instance_host, _current_database, _pg_pool, _token_timestamp
    body = await request.json()
    instance_name = body.get("instance", "").strip()
    database = body.get("database", "").strip()  # optional, default to first available
    if not instance_name:
        return JSONResponse({"error": "instance name is required"}, status_code=400)
    if not re.match(r'^[a-zA-Z0-9_-]+$', instance_name):
        return JSONResponse({"error": f"Invalid instance name: {instance_name}"}, status_code=400)

    old_instance = _current_instance
    old_host = _current_instance_host
    old_db = _current_database
    try:
        w = _get_ws()
        new_host = None
        pg_pass = None
        pg_user = None
        instance_type = "provisioned"

        # ── Try provisioned instance first ────────────────────────
        try:
            resp = w.api_client.do("GET", f"/api/2.0/database/instances/{instance_name}")
            new_host = resp.get("read_write_dns", "")
            if new_host:
                cred_resp = w.api_client.do("POST", "/api/2.0/database/credentials", body={
                    "instance_names": [instance_name],
                    "request_id": str(uuid.uuid4()),
                })
                pg_pass = cred_resp.get("token", "")
                pg_user = os.environ.get("PGUSER", "")
                if not pg_user:
                    try:
                        pg_user = w.current_user.me().user_name
                    except Exception:
                        pg_user = "postgres"
        except Exception:
            pass

        # ── Fall back to autoscaling project (REST API) ─────────────
        if not new_host or not pg_pass:
            try:
                instance_type = "autoscaling"
                project_path = f"projects/{instance_name}"
                # List branches to find the production endpoint
                br_resp = w.api_client.do("GET", f"/api/2.0/postgres/{project_path}/branches")
                branches = br_resp.get("branches", [])
                if not branches:
                    return JSONResponse({"error": f"No branches found for project '{instance_name}'"}, status_code=400)
                # Use production branch (or first available)
                branch = None
                for b in branches:
                    b_name = b.get("name", "") if isinstance(b, dict) else getattr(b, "name", str(b))
                    if "production" in b_name:
                        branch = b
                        break
                if not branch:
                    branch = branches[0]
                branch_name = branch.get("name", str(branch)) if isinstance(branch, dict) else getattr(branch, "name", str(branch))
                # Get endpoints on this branch
                ep_resp = w.api_client.do("GET", f"/api/2.0/postgres/{branch_name}/endpoints")
                endpoints = ep_resp.get("endpoints", [])
                if not endpoints:
                    return JSONResponse({"error": f"No endpoints found on branch '{branch_name}'"}, status_code=400)
                ep = endpoints[0]
                ep_name = ep.get("name", str(ep)) if isinstance(ep, dict) else getattr(ep, "name", str(ep))
                # Get endpoint detail for host
                ep_detail = w.api_client.do("GET", f"/api/2.0/postgres/{ep_name}")
                ep_status = ep_detail.get("status", {})
                ep_hosts = ep_status.get("hosts", {}) if isinstance(ep_status, dict) else {}
                ep_host = ep_hosts.get("host", "") if isinstance(ep_hosts, dict) else ""
                if not ep_host:
                    return JSONResponse({"error": f"Endpoint '{ep_name}' has no host"}, status_code=400)
                new_host = ep_host
                # Generate credential
                cred_resp = w.api_client.do("POST", "/api/2.0/postgres/credentials", body={"endpoint": ep_name})
                pg_pass = cred_resp.get("token", "")
                if not pg_pass:
                    return JSONResponse({"error": f"Failed to generate credential for endpoint '{ep_name}'"}, status_code=400)
                try:
                    pg_user = w.current_user.me().user_name
                except Exception:
                    pg_user = os.environ.get("PGUSER", "postgres")
            except Exception as e2:
                return JSONResponse(
                    {"error": f"Instance '{instance_name}' not found as provisioned or autoscaling: {e2}"},
                    status_code=400,
                )

        if not new_host or not pg_pass:
            return JSONResponse({"error": f"Could not resolve host/credentials for '{instance_name}'"}, status_code=400)

        # ── Close existing pools and connect ──────────────────────
        if _pg_pool:
            try:
                _pg_pool.closeall()
            except Exception:
                pass
            _pg_pool = None
        # Clear per-database pools (they point to the old instance)
        with _database_pools_lock:
            for _db_name, _pool in _database_pools.items():
                try:
                    _pool.closeall()
                except Exception:
                    pass
            _database_pools.clear()

        _current_instance = instance_name
        _current_instance_host = new_host
        _invalidate_table_cache()
        _token_timestamp = time.time()

        # Connect to discover databases
        target_db = database or "postgres"
        _pg_pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=1, maxconn=5,
            host=new_host, port=5432, dbname=target_db,
            user=pg_user, password=pg_pass, sslmode="require",
        )
        _current_database = target_db
        logger.info("Connected to %s instance %s at %s/%s", instance_type, instance_name, new_host, target_db)

        # Discover databases on this instance
        dbs = _execute_read("SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname")
        db_names = [r["datname"] for r in dbs]

        # If no database was specified, switch to the first non-system database
        if not database:
            preferred = [d for d in db_names if d not in ("postgres", "template0", "template1", "databricks_postgres")]
            if not preferred:
                preferred = [d for d in db_names if d not in ("postgres", "template0", "template1")]
            final_db = preferred[0] if preferred else db_names[0] if db_names else "postgres"
            if final_db != target_db:
                _pg_pool.closeall()
                _pg_pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn=1, maxconn=5,
                    host=new_host, port=5432, dbname=final_db,
                    user=pg_user, password=pg_pass, sslmode="require",
                )
                _current_database = final_db
                target_db = final_db

        _execute_read("SELECT 1")  # verify
        logger.info("Switched instance: %s -> %s (db: %s)", old_instance, instance_name, target_db)
        return JSONResponse({
            "switched": True,
            "instance": instance_name,
            "instance_type": instance_type,
            "database": target_db,
            "databases": db_names,
            "host": new_host,
        })
    except Exception as e:
        # Roll back
        _current_instance = old_instance
        _current_instance_host = old_host
        _current_database = old_db
        if _pg_pool:
            try:
                _pg_pool.closeall()
            except Exception:
                pass
            _pg_pool = None
        try:
            _init_pg_pool()
        except Exception:
            pass
        return JSONResponse({"error": f"Failed to switch to instance '{instance_name}': {e}"}, status_code=400)


async def api_info(request: Request):
    """Server info: database, instance, connection status."""
    pg_ok = False
    try:
        _execute_read("SELECT 1")
        pg_ok = True
    except Exception:
        pass
    current_db = _current_database or os.environ.get("PGDATABASE", "") or os.environ.get("LAKEBASE_DATABASE", "")
    info = {
        "server": "lakebase-mcp",
        "mcp_endpoint": "/mcp/",
        "connection_mode": _connection_mode or "unknown",
        "database": current_db,
        "host": os.environ.get("PGHOST", ""),
        "lakebase_connected": pg_ok,
        "tools_count": len(TOOLS),
    }
    if _connection_mode == "autoscaling":
        info["project"] = os.environ.get("LAKEBASE_PROJECT", "")
        info["branch"] = os.environ.get("LAKEBASE_BRANCH", "production")
    return JSONResponse(info)


# ── REST API endpoints (infrastructure) ──────────────────────────────


async def api_projects(request: Request):
    """List all Lakebase projects."""
    try:
        result = _tool_list_projects()
        return JSONResponse(json.loads(result[0].text))
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


async def api_project_detail(request: Request):
    """Describe a Lakebase project."""
    project_id = request.path_params["project_id"]
    try:
        result = _tool_describe_project(project_id)
        return JSONResponse(json.loads(result[0].text))
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)


async def api_branches(request: Request):
    """List branches on a project."""
    project = request.query_params.get("project", "")
    if not project:
        return JSONResponse({"error": "project query parameter required"}, status_code=400)
    try:
        result = _tool_list_branches(project)
        return JSONResponse(json.loads(result[0].text))
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)


async def api_create_branch(request: Request):
    """Create a new branch."""
    body = await request.json()
    try:
        result = _tool_create_branch(
            body["project"], body["branch_id"], body.get("parent_branch", "production")
        )
        return JSONResponse(json.loads(result[0].text))
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)


async def api_delete_branch(request: Request):
    """Delete a branch."""
    branch_name = request.path_params["branch_name"]
    try:
        body = await request.json()
    except Exception:
        body = {}
    confirm = body.get("confirm", False)
    if not confirm:
        return JSONResponse(
            {"error": "confirm must be true in the request body to delete a branch. This is a safety measure."},
            status_code=400,
        )
    try:
        result = _tool_delete_branch(branch_name, confirm=confirm)
        return JSONResponse(json.loads(result[0].text))
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)


async def api_endpoints(request: Request):
    """List endpoints on a project/branch."""
    project = request.query_params.get("project", "")
    branch = request.query_params.get("branch", "production")
    if not project:
        return JSONResponse({"error": "project query parameter required"}, status_code=400)
    try:
        result = _tool_list_endpoints(project, branch)
        return JSONResponse(json.loads(result[0].text))
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)


async def api_endpoint_config(request: Request):
    """Configure autoscaling or scale-to-zero on an endpoint."""
    endpoint_name = request.path_params["endpoint_name"]
    body = await request.json()
    results = {}
    try:
        if "min_cu" in body and "max_cu" in body:
            r = _tool_configure_autoscaling(endpoint_name, body["min_cu"], body["max_cu"])
            results["autoscaling"] = json.loads(r[0].text)
        if "scale_to_zero" in body:
            s2z = body["scale_to_zero"]
            r = _tool_configure_scale_to_zero(
                endpoint_name, s2z.get("enabled", False), s2z.get("idle_timeout_seconds", 300)
            )
            results["scale_to_zero"] = json.loads(r[0].text)
        if not results:
            return JSONResponse({"error": "Provide min_cu+max_cu and/or scale_to_zero config"}, status_code=400)
        return JSONResponse(results)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)


async def api_profile_table(request: Request):
    """Profile a table."""
    table_name = request.path_params["table_name"]
    try:
        result = _tool_profile_table(table_name)
        return JSONResponse(json.loads(result[0].text))
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)


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


async def db_health(request: Request):
    """Health check for a specific database."""
    database = request.path_params["database"]
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', database):
        return JSONResponse(
            {"error": f"Invalid database name: {database}"},
            status_code=400,
        )
    token = _request_database.set(database)
    try:
        _execute_read("SELECT 1")
        return JSONResponse({"status": "ok", "database": database})
    except Exception as e:
        return JSONResponse(
            {"status": "error", "database": database, "error": str(e)},
            status_code=500,
        )
    finally:
        _request_database.reset(token)


class DatabaseMCPRouter:
    """ASGI app that extracts database from /db/{database}/mcp/ path,
    sets the ContextVar, and forwards to the MCP session manager."""

    def __init__(self, session_mgr):
        self._session_mgr = session_mgr

    async def __call__(self, scope, receive, send):
        if scope["type"] not in ("http", "websocket"):
            return
        path = scope.get("path", "")
        # Mount at /db strips the /db prefix, so path = /{database}/mcp/...
        parts = path.strip("/").split("/")
        if len(parts) >= 2 and parts[1] == "mcp":
            database = parts[0]
            # Validate database name
            if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', database):
                resp = JSONResponse(
                    {"error": f"Invalid database name: {database}"},
                    status_code=400,
                )
                await resp(scope, receive, send)
                return
            # Rewrite scope path to /mcp/...
            new_path = "/" + "/".join(parts[1:])
            if not new_path.endswith("/"):
                new_path += "/"
            scope = dict(scope, path=new_path)
            token = _request_database.set(database)
            try:
                await self._session_mgr.handle_request(scope, receive, send)
            finally:
                _request_database.reset(token)
        else:
            resp = JSONResponse(
                {"error": "Expected /db/{database}/mcp/"},
                status_code=400,
            )
            await resp(scope, receive, send)


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
    # Cleanup — close all pools
    if _pg_pool:
        _pg_pool.closeall()
        logger.info("Default Lakebase pool closed")
    with _database_pools_lock:
        for db_name, pool in _database_pools.items():
            try:
                pool.closeall()
                logger.info("Pool closed for database: %s", db_name)
            except Exception:
                pass
        _database_pools.clear()


async def handle_mcp(scope, receive, send):
    await session_manager.handle_request(scope, receive, send)


async def mcp_redirect(request: Request):
    """Redirect /mcp to /mcp/ to avoid Starlette's localhost redirect."""
    return RedirectResponse(url="/mcp/", status_code=307)


app = Starlette(
    routes=[
        # Frontend
        Route("/", serve_frontend, methods=["GET"]),
        # REST API — original
        Route("/api/info", api_info, methods=["GET"]),
        Route("/api/tools", api_tools, methods=["GET"]),
        Route("/api/databases", api_databases, methods=["GET"]),
        Route("/api/databases/switch", api_switch_database, methods=["POST"]),
        Route("/api/instances/switch", api_switch_instance, methods=["POST"]),
        Route("/api/tables", api_tables, methods=["GET"]),
        Route("/api/tables/create", api_create_table, methods=["POST"]),
        Route("/api/tables/drop", api_drop_table, methods=["POST"]),
        Route("/api/tables/alter", api_alter_table, methods=["POST"]),
        Route("/api/tables/{table_name}", api_table_detail, methods=["GET"]),
        Route("/api/tables/{table_name}/sample", api_table_sample, methods=["GET"]),
        Route("/api/query", api_query, methods=["POST"]),
        Route("/api/insert", api_insert, methods=["POST"]),
        Route("/api/update", api_update, methods=["PATCH"]),
        Route("/api/delete", api_delete, methods=["DELETE"]),
        Route("/api/execute", api_execute, methods=["POST"]),
        Route("/api/transaction", api_transaction, methods=["POST"]),
        Route("/api/explain", api_explain, methods=["POST"]),
        # REST API — infrastructure
        Route("/api/projects", api_projects, methods=["GET"]),
        Route("/api/projects/{project_id:path}", api_project_detail, methods=["GET"]),
        Route("/api/branches", api_branches, methods=["GET"]),
        Route("/api/branches/create", api_create_branch, methods=["POST"]),
        Route("/api/branches/{branch_name:path}", api_delete_branch, methods=["DELETE"]),
        Route("/api/endpoints", api_endpoints, methods=["GET"]),
        Route("/api/endpoints/{endpoint_name:path}/config", api_endpoint_config, methods=["PATCH"]),
        Route("/api/profile/{table_name}", api_profile_table, methods=["GET"]),
        # Health & MCP
        Route("/health", health, methods=["GET"]),
        # Multi-database MCP routing: /db/{database}/mcp/
        Route("/db/{database}/health", db_health, methods=["GET"]),
        Mount("/db", app=DatabaseMCPRouter(session_manager)),
        # Default MCP (backward compatible — uses global pool)
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
