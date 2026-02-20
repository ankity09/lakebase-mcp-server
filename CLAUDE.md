# Lakebase MCP Server — Databricks App

Reusable MCP server that exposes Lakebase (PostgreSQL) read/write operations as 16 MCP tools, 2 resources, and 3 prompts. Supports both provisioned Lakebase (via app.yaml database resource) and autoscaling Lakebase (via env vars). Token refresh is automatic. Deploy as a Databricks App, then connect it to a MAS Supervisor as an External MCP Server agent. Includes a web UI for exploring the database and testing tools.

## Architecture

```
AI Agent (MAS / Claude / etc.)
    |
    | MCP Protocol (StreamableHTTP)
    v
Lakebase MCP Server (Databricks App)
    |-- /          Web UI (database explorer, SQL query, tool playground, docs)
    |-- /mcp/      MCP endpoint (StreamableHTTP, stateless)
    |-- /api/*     REST API (tables, query, insert, update, delete)
    |-- /health    Health check
    |
    | psycopg2 (PostgreSQL wire protocol)
    v
Lakebase Instance (PostgreSQL-compatible)
```

## Connection Modes

The server auto-detects the connection mode based on environment variables:

### Provisioned (default)
Uses the standard `app.yaml` database resource. Databricks Apps auto-injects `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`.

```yaml
resources:
  - name: database
    database:
      instance_name: my-instance
      database_name: my_database
      permission: CAN_CONNECT_AND_CREATE
```

### Autoscaling
Set `LAKEBASE_PROJECT` as an env var. The server auto-discovers the endpoint, generates credentials, and refreshes tokens every 50 minutes.

```yaml
env:
  - name: LAKEBASE_PROJECT
    value: "my-project"
  - name: LAKEBASE_BRANCH
    value: "production"          # optional, default: production
  - name: LAKEBASE_DATABASE
    value: "my_db"               # optional, default: databricks_postgres
  - name: LAKEBASE_ENDPOINT
    value: "ep-primary"          # optional, auto-discovered if omitted
```

No database resource needed — the server handles everything including endpoint discovery and token refresh.

## MCP Tools (16)

| Tool | Type | Description |
|------|------|-------------|
| `list_tables` | READ | List all tables with row counts |
| `describe_table` | READ | Column names, types, constraints |
| `read_query` | READ | SELECT queries (max configurable via env var `MAX_ROWS`, default 1000) |
| `insert_record` | WRITE | Insert a record (parameterized) |
| `update_records` | WRITE | Update rows (parameterized SET + WHERE) |
| `delete_records` | WRITE | Delete rows matching WHERE condition |
| `execute_sql` | SQL | Execute any SQL (SELECT/INSERT/UPDATE/DELETE/DDL) with auto-detection |
| `execute_transaction` | SQL | Multi-statement atomic transaction with rollback on error |
| `explain_query` | SQL | EXPLAIN ANALYZE with JSON output, rollback for writes |
| `create_table` | DDL | CREATE TABLE with column defs, IF NOT EXISTS |
| `drop_table` | DDL | DROP TABLE with confirm=true safety |
| `alter_table` | DDL | Add/drop/rename columns, alter types |
| `list_slow_queries` | PERF | Top slow queries from pg_stat_statements |
| `batch_insert` | WRITE | Multi-row INSERT, JSONB-aware |
| `list_schemas` | READ | List all schemas (not just public) |
| `get_connection_info` | READ | Host/port/database/user (no password) |

## MCP Resources (2)

| Resource | URI | Description |
|----------|-----|-------------|
| All Tables | `lakebase://tables` | JSON list of all tables |
| Table Schema | `lakebase://tables/{name}/schema` | Column definitions per table |

## MCP Prompts (3)

| Prompt | Arguments | Description |
|--------|-----------|-------------|
| `explore_database` | none | List tables, describe schemas, suggest queries |
| `design_schema` | `description` | Design a schema for a given description |
| `optimize_query` | `sql` | EXPLAIN ANALYZE + suggest improvements |

## REST API (for UI and custom integrations)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/info` | GET | Server info and connection status |
| `/api/tools` | GET | List MCP tools with schemas |
| `/api/tables` | GET | List tables with row/column counts |
| `/api/tables/{name}` | GET | Describe table schema |
| `/api/tables/{name}/sample` | GET | Sample rows (?limit=20) |
| `/api/query` | POST | Execute SELECT query |
| `/api/insert` | POST | Insert record |
| `/api/update` | PATCH | Update records |
| `/api/delete` | DELETE | Delete records |
| `/api/execute` | POST | Execute any SQL statement |
| `/api/transaction` | POST | Execute multi-statement transaction |
| `/api/explain` | POST | EXPLAIN ANALYZE a query |
| `/api/tables/create` | POST | Create a table |
| `/api/tables/drop` | POST | Drop a table |
| `/api/tables/alter` | POST | Alter a table |
| `/api/databases` | GET | List available databases on the instance |
| `/api/databases/switch` | POST | Switch to a different database |

## Web UI

The root URL (`/`) serves a single-page application with 4 tabs:
1. **Database Explorer** — Browse tables, view schemas, sample data
2. **SQL Query** — Execute read-only queries with tabular results
3. **MCP Tools** — Tool reference + interactive playground to test each tool
4. **Documentation** — Deployment guide, MAS connection instructions, gotchas

The header includes a database switcher dropdown to switch between databases on the same Lakebase instance without redeployment.

## Reuse Across Demos

Change the `instance_name` and `database_name` in `app/app.yaml`:

```yaml
resources:
  - name: database
    database:
      instance_name: my-instance      # change per demo
      database_name: my_database       # change per demo
      permission: CAN_CONNECT_AND_CREATE
```

No code changes needed. The UI and tools work with any Lakebase database.

## Deployment

### Prerequisites
- Databricks CLI configured with workspace profile
- A Lakebase instance already created

### Step 1: Create the app

```bash
databricks apps create lakebase-mcp-server --profile=<PROFILE>
```

### Step 2: Sync code to workspace

```bash
databricks sync ./app /Workspace/Users/<email>/lakebase-mcp-server/app --profile=<PROFILE> --watch=false
```

### Step 3: Deploy

```bash
databricks apps deploy lakebase-mcp-server \
  --source-code-path /Workspace/Users/<email>/lakebase-mcp-server/app \
  --profile=<PROFILE>
```

### Step 4: Grant permissions

```bash
# Grant CAN_USE to users group (required for MAS MCP proxy)
databricks api patch /api/2.0/permissions/apps/lakebase-mcp-server \
  --json '{"access_control_list":[{"group_name":"users","permission_level":"CAN_USE"}]}' \
  --profile=<PROFILE>

# Grant Lakebase table access to app SP
databricks psql my-instance --profile=<PROFILE> -- -d my_database -c "
GRANT ALL ON ALL TABLES IN SCHEMA public TO \"<app-sp-client-id>\";
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO \"<app-sp-client-id>\";
"
```

### Step 5: Verify

```bash
curl https://<app-url>/health
# Expected: {"status":"ok","lakebase":true}
```

Visit `https://<app-url>/` for the web UI.

## Connecting to MAS

1. Create a UC HTTP connection:
   - URL: `https://<mcp-app-url>/mcp`
   - Auth: Databricks OAuth M2M (SP OAuth secret from Account Console)
   - Connection fields: host, port=443, base_path=/mcp/, client_id, client_secret, oauth_scope=all-apis

2. In MAS Supervisor config, add agent:
   - Type: **External MCP Server**
   - Connection: the UC HTTP connection above
   - Description: "Execute Lakebase database operations — CRUD on operational tables"

3. Click "Rediscover tools" in MAS config to detect the 16 MCP tools.

## Known Gotchas

1. **Trailing slash:** MCP endpoint is at `/mcp/` (with trailing slash). `/mcp` redirects to `/mcp/`.
2. **MAS sends JSON strings:** MAS agents serialize nested objects as JSON strings. Handled by `_ensure_dict()`.
3. **Table permissions:** Tables created via `databricks psql` are owned by the user. Grant to app SP after creating tables.
4. **OAuth M2M for UC connection:** SP OAuth secrets must be created at Account Console level.
5. **CAN_USE for users group:** MAS MCP proxy needs CAN_USE on the app. Grant to `users` group.
6. **Autoscaling scale-to-zero:** If the autoscaling endpoint is suspended, the first request may take 2-5 seconds while compute wakes up.
7. **Database switcher:** Switching databases reinitializes the connection pool. The table cache is cleared automatically.

## Local Development

```bash
export PGHOST=instance-<uid>.database.cloud.databricks.com
export PGPORT=5432
export PGDATABASE=my_database
export PGUSER=<your-email>
export PGSSLMODE=require

pip install -r app/requirements.txt
python app/mcp_server.py
```

Server starts on port 8000. MCP endpoint: `http://localhost:8000/mcp/`. UI: `http://localhost:8000/`

### Autoscaling mode (local)

```bash
# Autoscaling mode (no PGHOST needed)
export LAKEBASE_PROJECT=my-project
export LAKEBASE_BRANCH=production
export LAKEBASE_DATABASE=my_db

pip install -r app/requirements.txt
python app/mcp_server.py
```
