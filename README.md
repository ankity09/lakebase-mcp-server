# Lakebase MCP Server

A reusable [Model Context Protocol](https://modelcontextprotocol.io) (MCP) server for [Databricks Lakebase](https://docs.databricks.com/en/database/lakebase/index.html) (PostgreSQL-compatible databases). Deploy as a Databricks App to give AI agents full read/write/DDL access to your operational data.

## What It Does

- Exposes **27 MCP tools** for schema inspection, CRUD, general SQL, DDL, transactions, query analysis, performance monitoring, infrastructure management, branch management, autoscaling config, and data quality profiling
- **2 MCP resources** for table and schema discovery
- **3 MCP prompts** for database exploration, schema design, and query optimization
- Supports both **provisioned** and **autoscaling** Lakebase instances with automatic token refresh
- Works as an **External MCP Server** agent in Databricks Multi-Agent Supervisors (MAS)
- Includes a **web UI** with database explorer, SQL editor, tool playground, database switcher, and documentation
- Provides a **REST API** (29 endpoints) for custom integrations

## Architecture

```
AI Agent (MAS / Claude Code / etc.)
    |
    | MCP Protocol (StreamableHTTP)
    v
Lakebase MCP Server (Databricks App)
    |-- /          Web UI (explorer, SQL editor, tool playground, docs)
    |-- /mcp/      MCP endpoint (stateless StreamableHTTP)
    |-- /api/*     REST API (29 endpoints)
    |-- /health    Health check
    |
    | psycopg2 (PostgreSQL wire protocol + auto token refresh)
    v
Lakebase Instance (Provisioned or Autoscaling)
```

## Connection Modes

The server auto-detects its connection mode based on environment variables. No code changes needed.

### Provisioned Lakebase (default)

Uses the standard Databricks Apps database resource. The platform auto-injects `PGHOST`, `PGPORT`, `PGDATABASE`, and `PGUSER` environment variables.

```yaml
# app.yaml
command:
  - python
  - mcp_server.py

resources:
  - name: database
    database:
      instance_name: my-instance      # your provisioned Lakebase instance
      database_name: my_database       # your database name
      permission: CAN_CONNECT_AND_CREATE
```

### Autoscaling Lakebase

Set `LAKEBASE_PROJECT` as an environment variable. The server automatically:
1. Discovers the endpoint on the specified branch
2. Generates OAuth credentials via the Databricks SDK
3. Refreshes tokens every 50 minutes (before the 1-hour expiry)

```yaml
# app.yaml
command:
  - python
  - mcp_server.py

env:
  - name: LAKEBASE_PROJECT
    value: "my-project"
  - name: LAKEBASE_BRANCH
    value: "production"            # optional, default: production
  - name: LAKEBASE_DATABASE
    value: "my_db"                 # optional, default: databricks_postgres
  - name: LAKEBASE_ENDPOINT
    value: "ep-primary"            # optional, auto-discovered if omitted
```

No `resources` block needed for autoscaling — the server handles endpoint discovery and authentication internally.

### Switching Between Modes

| | Provisioned | Autoscaling |
|---|---|---|
| **Config** | `app.yaml` database resource | `app.yaml` env vars |
| **Detection** | `PGHOST` is set | `LAKEBASE_PROJECT` is set |
| **Auth** | Platform-injected token | SDK-generated token (auto-refresh) |
| **Scale-to-zero** | No | Yes (configurable) |
| **Branching** | No | Yes (dev/staging/prod) |

To switch modes, update `app.yaml` and redeploy. The server detects the mode automatically on startup.

## Deployment

### Prerequisites

- Databricks CLI configured with a workspace profile
- A Lakebase instance (provisioned or autoscaling) already created

### Step 1: Create the App

```bash
databricks apps create lakebase-mcp-server --profile=<PROFILE>
```

### Step 2: Configure `app.yaml`

Edit `app/app.yaml` with your Lakebase instance details (see [Connection Modes](#connection-modes) above).

### Step 3: Sync and Deploy

```bash
# Sync local code to workspace
databricks sync ./app /Workspace/Users/<email>/lakebase-mcp-server/app \
  --profile=<PROFILE> --watch=false

# Deploy the app
databricks apps deploy lakebase-mcp-server \
  --source-code-path /Workspace/Users/<email>/lakebase-mcp-server/app \
  --profile=<PROFILE>
```

### Step 4: Grant Permissions

```bash
# Required for MAS MCP proxy to access the app
databricks api patch /api/2.0/permissions/apps/lakebase-mcp-server \
  --json '{"access_control_list":[{"group_name":"users","permission_level":"CAN_USE"}]}' \
  --profile=<PROFILE>

# Grant table access to the app's service principal
# (get SP client ID from: databricks apps get lakebase-mcp-server --profile=<PROFILE>)
databricks psql my-instance --profile=<PROFILE> -- -d my_database -c "
GRANT ALL ON ALL TABLES IN SCHEMA public TO \"<app-sp-client-id>\";
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO \"<app-sp-client-id>\";
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO \"<app-sp-client-id>\";
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO \"<app-sp-client-id>\";
"
```

### Step 5: Verify

```bash
curl https://<app-url>/health
# {"status":"ok","lakebase":true}

curl https://<app-url>/api/tools | python3 -c "import sys,json; print(len(json.load(sys.stdin)), 'tools')"
# 27 tools
```

Visit `https://<app-url>/` for the web UI.

## Connecting to MAS

Once deployed, this MCP server can be used by a [Multi-Agent Supervisor (MAS)](https://docs.databricks.com/en/generative-ai/agent-framework/multi-agent-supervisor.html) to read and write Lakebase tables as part of an agentic workflow.

### Step 1: Create a UC HTTP Connection

In your Databricks workspace:

1. Go to **Catalog > External Connections > Create Connection**
2. Connection type: **HTTP**
3. Configure:
   - **Host**: `<app-url>` (e.g. `lakebase-mcp-server-1234567890.aws.databricksapps.com`)
   - **Port**: `443`
   - **Base path**: `/mcp/`
   - **Auth type**: Databricks OAuth M2M
   - **Client ID**: Service principal application ID
   - **Client secret**: SP OAuth secret (create at **Account Console > App Connections**)
   - **OAuth scope**: `all-apis`

### Step 2: Add External MCP Server Agent to MAS

In your MAS Supervisor configuration:

1. Click **Add Agent > External MCP Server**
2. Select the UC HTTP connection from Step 1
3. Set the agent description (this is what the supervisor uses for routing):
   > "Execute Lakebase database operations — read and write to operational tables including inventory, shipments, orders, and supply chain data."
4. Click **Rediscover tools** — the supervisor will detect all 27 MCP tools

### Step 3: Test

In the MAS playground, try:
- "What tables are in the database?"
- "Show me the top 10 rows from the orders table"
- "Insert a new record into the notes table"
- "Create a table called alerts with columns: id, severity, message, created_at"

The supervisor will route these requests to the Lakebase MCP agent, which executes them against your database and returns structured results.

### MAS Agent Description Tips

The agent description determines when the supervisor routes requests to this agent. Be specific about what data lives in your Lakebase:

```
# Generic (works but less precise routing)
"Read and write to the Lakebase operational database"

# Specific (better routing for supply chain use case)
"Execute database operations on supply chain tables: inventory_levels, shipments,
purchase_orders, demand_forecasts, routes, warehouses, notes. Supports reads, writes,
DDL, transactions, query analysis, infrastructure management, and data quality profiling."
```

## Multi-Database Routing

A single deployed MCP server can serve multiple databases on the same Lakebase instance. Each database gets its own URL namespace — no redeployment needed.

### URL Patterns

| Pattern | Description |
|---------|-------------|
| `/db/{database}/mcp/` | MCP endpoint scoped to `{database}` |
| `/db/{database}/health` | Health check for `{database}` (verifies pool connectivity) |
| `/mcp/` | MCP endpoint using the default database (backward compatible) |

### How It Works

- **ContextVar per-request scoping** — Each request to `/db/{database}/mcp/` sets a `ContextVar` with the target database name. All tool handlers read from this variable, so concurrent requests to different databases are fully isolated.
- **Lazy per-database connection pools** — Pools are created on first request to a database and reused thereafter. Token refresh applies to all pools.
- **Backward compatible** — `/mcp/` continues to work with the default database from `PGDATABASE` or `LAKEBASE_DATABASE`. Existing MAS connections need no changes.

### Example: Two Demos Sharing One Server

Deploy one Lakebase MCP Server app, then create separate UC HTTP connections for each demo:

| Demo | UC HTTP Connection `base_path` | Database |
|------|-------------------------------|----------|
| Supply Chain | `/db/supply_chain_db/mcp/` | `supply_chain_db` |
| Finance | `/db/finance_db/mcp/` | `finance_db` |
| Browser UI | `/mcp/` (default) | From `app.yaml` |

Each MAS Supervisor connects to the same app URL but with a different `base_path`. The server creates isolated connection pools per database on demand.

### UC HTTP Connection for Multi-Database

When creating the UC HTTP connection (see [Connecting to MAS](#connecting-to-mas)), set:

- **Base path**: `/db/{database}/mcp/` (instead of `/mcp/`)

Everything else (host, port, auth) stays the same.

### Verify Per-Database Health

```bash
curl https://<app-url>/db/supply_chain_db/health
# {"status":"ok","lakebase":true,"database":"supply_chain_db"}

curl https://<app-url>/db/finance_db/health
# {"status":"ok","lakebase":true,"database":"finance_db"}
```

## MCP Tools (27)

### Schema Inspection (READ)

| Tool | Description |
|------|-------------|
| `list_tables` | List all tables with row counts and column counts |
| `describe_table` | Column names, data types, nullability, defaults, primary keys |
| `list_schemas` | List all schemas in the database (not just public) |
| `get_connection_info` | Host, port, database, user, connection mode (no password) |

### Data Operations (READ/WRITE)

| Tool | Description |
|------|-------------|
| `read_query` | Execute read-only SELECT queries (configurable row limit via `MAX_ROWS` env, default 1000) |
| `insert_record` | Insert a single record with parameterized values. JSONB-aware |
| `update_records` | Update rows matching a WHERE condition (WHERE required) |
| `delete_records` | Delete rows matching a WHERE condition (WHERE required) |
| `batch_insert` | Insert multiple records in one statement. JSONB-aware |

### General SQL

| Tool | Description |
|------|-------------|
| `execute_sql` | Execute any SQL (SELECT/INSERT/UPDATE/DELETE/DDL). Auto-detects type, returns structured results |
| `execute_transaction` | Multi-statement atomic transaction. All succeed or all roll back. Returns per-statement results |
| `explain_query` | EXPLAIN ANALYZE with JSON output. Wraps writes in transaction + rollback to prevent side effects |

### DDL

| Tool | Description |
|------|-------------|
| `create_table` | CREATE TABLE with column definitions array. Supports IF NOT EXISTS |
| `drop_table` | DROP TABLE with `confirm=true` safety gate. Supports IF EXISTS |
| `alter_table` | Add column, drop column, rename column, or alter column type |

### Performance

| Tool | Description |
|------|-------------|
| `list_slow_queries` | Top N slow queries from pg_stat_statements. Graceful error if extension not enabled |

### Infrastructure Management (INFRA)

| Tool | Description |
|------|-------------|
| `list_projects` | List all Lakebase projects with status, branch count, endpoint info |
| `describe_project` | Project details including branches, endpoints, and configuration |
| `get_connection_string` | Build psql/psycopg2/jdbc connection string for an endpoint |
| `list_branches` | List branches on a project with state and creation time |
| `list_endpoints` | List endpoints on a branch with host, state, compute config |
| `get_endpoint_status` | Endpoint state, host, and compute configuration |

### Branch Management (BRANCH)

| Tool | Description |
|------|-------------|
| `create_branch` | Create a dev/test branch from a parent branch |
| `delete_branch` | Delete a branch with `confirm=true` safety gate (cannot delete production) |

### Autoscaling Configuration (SCALE)

| Tool | Description |
|------|-------------|
| `configure_autoscaling` | Set min/max compute units (CU) on an autoscaling endpoint |
| `configure_scale_to_zero` | Enable/disable scale-to-zero (suspend) with configurable idle timeout |

### Data Quality (QUALITY)

| Tool | Description |
|------|-------------|
| `profile_table` | Column-level profiling: row count, null counts/%, distinct counts, min/max, avg for numerics |

## MCP Resources (2)

| Resource URI | Description |
|---|---|
| `lakebase://tables` | JSON list of all tables with row and column counts |
| `lakebase://tables/{name}/schema` | Column definitions for a specific table |

## MCP Prompts (3)

| Prompt | Arguments | Description |
|--------|-----------|-------------|
| `explore_database` | none | List tables, describe schemas, suggest and run queries |
| `design_schema` | `description` | Design a PostgreSQL schema for a given use case |
| `optimize_query` | `sql` | Run EXPLAIN ANALYZE and suggest improvements |

## Web UI

The root URL (`/`) serves a Neon.tech-inspired single-page application with fixed sidebar navigation:

**Database**
- **Database Explorer** — Browse tables, view column schemas and constraints, sample data
- **SQL Query** — Run read-only queries with tabular results (Ctrl/Cmd+Enter to execute)
- **MCP Tools** — Tool reference cards categorized by type + interactive playground for all 27 tools

**Infrastructure**
- **Projects** — Card grid of Lakebase projects with status badges and branch counts
- **Branches** — Branch management table with create/delete operations
- **Endpoints** — Endpoint cards with autoscaling CU sliders and scale-to-zero config

**Quality**
- **Profiler** — Table selector with column-level stats: nulls, distinct, min/max, avg

**Reference**
- **Documentation** — Deployment guide, MAS connection instructions, REST API reference

The sidebar includes a **database switcher** dropdown and live connection status indicator.

## REST API (29 endpoints)

### Core

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/info` | GET | Server info, connection mode, and status |
| `/api/tools` | GET | List all MCP tools with input schemas |
| `/health` | GET | Health check |

### Database Management

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/databases` | GET | List available databases on the instance |
| `/api/databases/switch` | POST | Switch to a different database |

### Tables

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/tables` | GET | List tables with row/column counts |
| `/api/tables/{name}` | GET | Describe table schema |
| `/api/tables/{name}/sample` | GET | Sample rows (`?limit=20`) |
| `/api/tables/create` | POST | Create a table |
| `/api/tables/drop` | POST | Drop a table |
| `/api/tables/alter` | POST | Alter a table |

### Data Operations

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/query` | POST | Execute SELECT query |
| `/api/insert` | POST | Insert record |
| `/api/update` | PATCH | Update records |
| `/api/delete` | DELETE | Delete records |
| `/api/execute` | POST | Execute any SQL statement |
| `/api/transaction` | POST | Execute multi-statement transaction |
| `/api/explain` | POST | EXPLAIN ANALYZE a query |

### Infrastructure Management

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/projects` | GET | List all Lakebase projects |
| `/api/projects/{id}` | GET | Describe a project with branches |
| `/api/branches` | GET | List branches (`?project=...`) |
| `/api/branches/create` | POST | Create a new branch |
| `/api/branches/{name}` | DELETE | Delete a branch |
| `/api/endpoints` | GET | List endpoints (`?project=...&branch=...`) |
| `/api/endpoints/{name}/config` | PATCH | Configure autoscaling / scale-to-zero |

### Data Quality

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/profile/{table}` | GET | Column-level profiling statistics |

## Reuse Across Demos

This server is fully generic. To point it at a different Lakebase database:

1. **Provisioned**: Change `instance_name` and `database_name` in `app.yaml`
2. **Autoscaling**: Change the `LAKEBASE_PROJECT` / `LAKEBASE_DATABASE` env vars

No code changes needed. The web UI, MCP tools, and REST API work with any Lakebase database.

## Local Development

### Provisioned mode

```bash
export PGHOST=instance-<uid>.database.cloud.databricks.com
export PGPORT=5432
export PGDATABASE=my_database
export PGUSER=<your-email>
export PGSSLMODE=require

pip install -r app/requirements.txt
python app/mcp_server.py
```

### Autoscaling mode

```bash
export LAKEBASE_PROJECT=my-project
export LAKEBASE_BRANCH=production
export LAKEBASE_DATABASE=my_db

pip install -r app/requirements.txt
python app/mcp_server.py
```

Server starts on port 8000. MCP endpoint: `http://localhost:8000/mcp/`. Web UI: `http://localhost:8000/`.

## Known Gotchas

1. **Trailing slash**: MCP endpoint is at `/mcp/` (with trailing slash). `/mcp` auto-redirects.
2. **MAS sends JSON strings**: MAS agents serialize nested objects as JSON strings. Handled automatically by `_ensure_dict()` and `_ensure_list()`.
3. **Table permissions**: Tables created via `databricks psql` are owned by the user. Grant access to the app's service principal after creating tables.
4. **OAuth M2M for UC connection**: SP OAuth secrets must be created at the **Account Console** level, not via workspace API.
5. **CAN_USE for users group**: The MAS MCP proxy needs `CAN_USE` on the app. Grant to the `users` group.
6. **Autoscaling scale-to-zero**: If the autoscaling endpoint is suspended, the first request may take 2-5 seconds while compute wakes up.
7. **Database switcher**: Switching databases reinitializes the connection pool. The table cache is cleared automatically.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PGHOST` | — | Lakebase host (set by app.yaml database resource for provisioned) |
| `PGPORT` | `5432` | PostgreSQL port |
| `PGDATABASE` | — | Database name (provisioned mode) |
| `PGUSER` | — | PostgreSQL user |
| `PGSSLMODE` | `require` | SSL mode |
| `LAKEBASE_PROJECT` | — | Autoscaling project name (triggers autoscaling mode) |
| `LAKEBASE_BRANCH` | `production` | Autoscaling branch name |
| `LAKEBASE_DATABASE` | `databricks_postgres` | Database name (autoscaling mode) |
| `LAKEBASE_ENDPOINT` | auto-discovered | Autoscaling endpoint ID |
| `MAX_ROWS` | `1000` | Maximum rows returned by read queries |
| `DATABRICKS_APP_PORT` | `8000` | Server port |

## License

MIT
