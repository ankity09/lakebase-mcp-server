# Lakebase MCP Server

A reusable [Model Context Protocol](https://modelcontextprotocol.io) (MCP) server for [Databricks Lakebase](https://docs.databricks.com/en/database/lakebase/index.html) (PostgreSQL-compatible databases). Deploy as a Databricks App to give AI agents full read/write/DDL access to your operational data.

## What It Does

- Exposes **34 MCP tools** with safety annotations for schema inspection, CRUD, general SQL, DDL, transactions, query analysis, performance monitoring, infrastructure management, branch management, autoscaling config, data quality profiling, safe migrations, query tuning, and cross-database search
- **2 MCP resources** for table and schema discovery
- **3 MCP prompts** for database exploration, schema design, and query optimization
- **MCP tool annotations** (`readOnlyHint`, `destructiveHint`, `idempotentHint`) on every tool for client-side safety UI
- Supports both **provisioned** and **autoscaling** Lakebase instances with automatic token refresh
- **Multi-instance switching** — browse and switch between all Lakebase instances in the workspace from the UI
- Works as an **External MCP Server** agent in Databricks Multi-Agent Supervisors (MAS)
- Includes a **web UI** with database explorer, SQL editor, tool playground, instance/database switcher, and documentation
- Provides a **REST API** (29+ endpoints) for custom integrations

## Architecture

```
AI Agent (MAS / Claude Code / etc.)
    |
    | MCP Protocol (StreamableHTTP)
    v
Lakebase MCP Server (Databricks App)
    |-- /          Web UI (explorer, SQL editor, tool playground, docs)
    |-- /mcp/      MCP endpoint (stateless StreamableHTTP)
    |-- /db/{db}/mcp/  MCP endpoint scoped to a specific database
    |-- /api/*     REST API (29+ endpoints)
    |-- /health    Health check
    |
    | psycopg2 (PostgreSQL wire protocol + auto token refresh)
    v
Lakebase Instance(s) (Provisioned or Autoscaling)
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

Edit `app/app.yaml` with your Lakebase instance details (see [Connection Modes](#connection-modes) above). You can configure multiple database resources if you want the platform to auto-inject credentials for several instances:

```yaml
command:
  - python
  - mcp_server.py

resources:
  - name: database
    database:
      instance_name: my-instance
      database_name: my_database
      permission: CAN_CONNECT_AND_CREATE
  # Optional: add more databases for auto-configured access
  - name: database-2
    database:
      instance_name: another-instance
      database_name: another_db
      permission: CAN_CONNECT_AND_CREATE
```

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

### Step 4: Grant App Permissions

```bash
# Required for MAS MCP proxy to access the app
databricks api patch /api/2.0/permissions/apps/lakebase-mcp-server \
  --json '{"access_control_list":[{"group_name":"users","permission_level":"CAN_USE"}]}' \
  --profile=<PROFILE>
```

### Step 5: Grant the App SP Access to Lakebase Instances

The app runs as a service principal (SP). You need to grant this SP access on every Lakebase instance it will connect to. There are two parts: **creating the role** and **configuring the security label** for OAuth authentication.

#### 5a: Get the App's Service Principal Info

```bash
databricks apps get lakebase-mcp-server --profile=<PROFILE>
```

From the output, note:
- `service_principal_client_id` — e.g. `ecbb98b7-7186-43cf-ab8d-ac56bffc0f8e` (used as the PostgreSQL role name)
- `service_principal_id` — e.g. `70866217708507` (numeric ID, used in the security label)

#### 5b: For Every Lakebase Instance the App Will Connect To

> **Important:** The instance switcher in the UI always connects via a generated OAuth token — even for instances listed in `app.yaml` resources. The `resources:` block only handles the app's startup connection; it does **not** automatically set up the role/security label for token-based switcher connections. You must run the SQL below on every instance the app needs to switch to.

#### 5c: For Each Instance (including those in `app.yaml`)

If you want the app to switch to Lakebase instances that are **not** in `app.yaml` (via the UI instance switcher), you must manually create the role, security label, and grants on each one:

```bash
# Replace these with your values
SP_CLIENT_ID="ecbb98b7-7186-43cf-ab8d-ac56bffc0f8e"   # from step 5a
SP_ID="70866217708507"                                   # from step 5a
INSTANCE="my-other-instance"                             # target instance name
PROFILE="my-profile"                                     # Databricks CLI profile

databricks psql $INSTANCE --profile=$PROFILE -- -c "
-- 1. Create the role (idempotent — skips if exists)
DO \$\$ BEGIN
  CREATE ROLE \"$SP_CLIENT_ID\" WITH LOGIN;
EXCEPTION WHEN duplicate_object THEN NULL;
END \$\$;

-- 2. Configure OAuth security label (required for token auth)
SECURITY LABEL FOR \"databricks_auth\" ON ROLE \"$SP_CLIENT_ID\"
  IS 'id=$SP_ID,type=SERVICE_PRINCIPAL';

-- 3. Grant access to existing objects
GRANT ALL ON ALL TABLES IN SCHEMA public TO \"$SP_CLIENT_ID\";
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO \"$SP_CLIENT_ID\";

-- 4. Grant access to future objects
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT ALL ON TABLES TO \"$SP_CLIENT_ID\";
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT USAGE, SELECT ON SEQUENCES TO \"$SP_CLIENT_ID\";
"
```

**All three steps are required:**
1. `CREATE ROLE ... WITH LOGIN` — creates the PostgreSQL role
2. `SECURITY LABEL FOR "databricks_auth"` — maps the OAuth token to the role (without this, you get `no role security label is configured`)
3. `GRANT` statements — gives the role permission to read/write tables

To grant access on **all** instances at once:

```bash
SP_CLIENT_ID="ecbb98b7-7186-43cf-ab8d-ac56bffc0f8e"
SP_ID="70866217708507"
PROFILE="my-profile"

for INSTANCE in instance-a instance-b instance-c; do
  echo "=== $INSTANCE ==="
  databricks psql $INSTANCE --profile=$PROFILE -- -c "
  DO \$\$ BEGIN
    CREATE ROLE \"$SP_CLIENT_ID\" WITH LOGIN;
  EXCEPTION WHEN duplicate_object THEN NULL;
  END \$\$;
  SECURITY LABEL FOR \"databricks_auth\" ON ROLE \"$SP_CLIENT_ID\"
    IS 'id=$SP_ID,type=SERVICE_PRINCIPAL';
  GRANT ALL ON ALL TABLES IN SCHEMA public TO \"$SP_CLIENT_ID\";
  GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO \"$SP_CLIENT_ID\";
  "
done
```

### Step 6: Verify

```bash
curl https://<app-url>/health
# {"status":"ok","lakebase":true}

curl https://<app-url>/api/tools | python3 -c "import sys,json; print(len(json.load(sys.stdin)), 'tools')"
# 34 tools
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
4. Click **Rediscover tools** — the supervisor will detect all 34 MCP tools

### Step 3: Test

In the MAS playground, try:
- "What tables are in the database?"
- "Show me the top 10 rows from the orders table"
- "Insert a new record into the notes table"
- "Create a table called alerts with columns: id, severity, message, created_at"
- "Prepare a migration to add a status column to the orders table"

The supervisor will route these requests to the Lakebase MCP agent, which executes them against your database and returns structured results.

### MAS Agent Description Tips

The agent description determines when the supervisor routes requests to this agent. Be specific about what data lives in your Lakebase:

```
# Generic (works but less precise routing)
"Read and write to the Lakebase operational database"

# Specific (better routing for supply chain use case)
"Execute database operations on supply chain tables: inventory_levels, shipments,
purchase_orders, demand_forecasts, routes, warehouses, notes. Supports reads, writes,
DDL, transactions, query analysis, safe migrations, query tuning, and data quality profiling."
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

## MCP Tools (34)

All tools include MCP annotations (`readOnlyHint`, `destructiveHint`, `idempotentHint`) so clients can display safety warnings before destructive operations.

### Schema Inspection (READ)

| Tool | Annotations | Description |
|------|-------------|-------------|
| `list_tables` | read-only, idempotent | List all tables with row counts and column counts |
| `describe_table` | read-only, idempotent | Column names, data types, nullability, defaults, primary keys |
| `list_schemas` | read-only, idempotent | List all schemas in the database (not just public) |
| `get_connection_info` | read-only, idempotent | Host, port, database, user, connection mode (no password) |

### Data Operations (READ/WRITE)

| Tool | Annotations | Description |
|------|-------------|-------------|
| `read_query` | read-only, idempotent | Execute read-only SELECT queries (configurable row limit via `MAX_ROWS` env, default 1000) |
| `insert_record` | write | Insert a single record with parameterized values. JSONB-aware |
| `update_records` | write | Update rows matching a WHERE condition (WHERE required) |
| `delete_records` | write, destructive | Delete rows matching a WHERE condition (WHERE required) |
| `batch_insert` | write | Insert multiple records in one statement. JSONB-aware |

### General SQL

| Tool | Annotations | Description |
|------|-------------|-------------|
| `execute_sql` | write, destructive | Execute any SQL (SELECT/INSERT/UPDATE/DELETE/DDL). Auto-detects type, returns structured results |
| `execute_transaction` | write, destructive | Multi-statement atomic transaction. All succeed or all roll back. Returns per-statement results |
| `explain_query` | read-only, idempotent | EXPLAIN ANALYZE with JSON output. Wraps writes in transaction + rollback to prevent side effects |

### DDL

| Tool | Annotations | Description |
|------|-------------|-------------|
| `create_table` | write, idempotent | CREATE TABLE with column definitions array. Supports IF NOT EXISTS |
| `drop_table` | write, destructive | DROP TABLE with `confirm=true` safety gate. Supports IF EXISTS |
| `alter_table` | write, destructive | Add column, drop column, rename column, or alter column type |

### Performance

| Tool | Annotations | Description |
|------|-------------|-------------|
| `list_slow_queries` | read-only, idempotent | Top N slow queries from pg_stat_statements. Graceful error if extension not enabled |

### Infrastructure Management (INFRA)

| Tool | Annotations | Description |
|------|-------------|-------------|
| `list_projects` | read-only, idempotent | List all Lakebase projects (provisioned + autoscaling) with status |
| `describe_project` | read-only, idempotent | Project details including branches, endpoints, and configuration |
| `get_connection_string` | read-only, idempotent | Build psql/psycopg2/jdbc connection string for an endpoint |
| `list_branches` | read-only, idempotent | List branches on a project with state and creation time |
| `list_endpoints` | read-only, idempotent | List endpoints on a branch with host, state, compute config |
| `get_endpoint_status` | read-only, idempotent | Endpoint state, host, and compute configuration |

### Branch Management (BRANCH)

| Tool | Annotations | Description |
|------|-------------|-------------|
| `create_branch` | write | Create a dev/test branch from a parent branch |
| `delete_branch` | write, destructive | Delete a branch with `confirm=true` safety gate (cannot delete production) |

### Autoscaling Configuration (SCALE)

| Tool | Annotations | Description |
|------|-------------|-------------|
| `configure_autoscaling` | write, idempotent | Set min/max compute units (CU) on an autoscaling endpoint |
| `configure_scale_to_zero` | write, idempotent | Enable/disable scale-to-zero (suspend) with configurable idle timeout |

### Data Quality (QUALITY)

| Tool | Annotations | Description |
|------|-------------|-------------|
| `profile_table` | read-only, idempotent | Column-level profiling: row count, null counts/%, distinct counts, min/max, avg for numerics |

### Safe Migrations (MIGRATION)

Two-phase migration workflow: validate first, then apply or discard.

| Tool | Annotations | Description |
|------|-------------|-------------|
| `prepare_database_migration` | write | Phase 1: Dry-run migration SQL in a rolled-back transaction. Returns validation results, affected tables, and a `migration_id` |
| `complete_database_migration` | write, destructive | Phase 2: Apply the validated migration for real, or discard it. Requires `migration_id` from phase 1 |

**Usage:**
```
1. Call prepare_database_migration(migration_sql="ALTER TABLE orders ADD COLUMN status TEXT DEFAULT 'pending'")
   → Returns: migration_id, validation results, tables affected
2. Review the results
3. Call complete_database_migration(migration_id="abc123", apply_changes=true)
   → Applies the migration for real
```

### Query Tuning (TUNING)

Two-phase query tuning: analyze and suggest indexes first, then apply or discard.

| Tool | Annotations | Description |
|------|-------------|-------------|
| `prepare_query_tuning` | write | Phase 1: Run EXPLAIN ANALYZE, detect seq scans, suggest indexes. Returns `tuning_id` and suggestions |
| `complete_query_tuning` | write, destructive | Phase 2: Apply suggested DDL (e.g. CREATE INDEX) or discard. Shows before/after execution plan |

**Usage:**
```
1. Call prepare_query_tuning(sql="SELECT * FROM orders WHERE customer_id = 42")
   → Returns: tuning_id, execution plan, suggestions (e.g. "add index on customer_id")
2. Review suggestions
3. Call complete_query_tuning(tuning_id="xyz789", apply_changes=true, ddl_statements=["CREATE INDEX idx_orders_customer ON orders(customer_id)"])
   → Applies the index and shows the improved execution plan
```

### Exploration (EXPLORE)

| Tool | Annotations | Description |
|------|-------------|-------------|
| `describe_branch` | read-only, idempotent | Tree view of all objects in a database: schemas, tables, views, functions, sequences, indexes |
| `compare_database_schema` | read-only, idempotent | Compare schemas between two databases. Returns unified diff of added/dropped/modified tables and columns |
| `search` | read-only, idempotent | Search across all Lakebase instances, databases, and tables by keyword |

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
- **MCP Tools** — Tool reference cards categorized by type + interactive playground for all 34 tools

**Infrastructure**
- **Projects** — Card grid of Lakebase projects with status badges and branch counts
- **Branches** — Branch management table with create/delete operations
- **Endpoints** — Endpoint cards with autoscaling CU sliders and scale-to-zero config

**Quality**
- **Profiler** — Table selector with column-level stats: nulls, distinct, min/max, avg

**Reference**
- **Documentation** — Deployment guide, MAS connection instructions, REST API reference

The sidebar includes an **instance switcher** (switch between all Lakebase instances), a **database switcher** dropdown, and a live connection status indicator.

## REST API (29+ endpoints)

### Core

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/info` | GET | Server info, connection mode, and status |
| `/api/tools` | GET | List all MCP tools with input schemas |
| `/health` | GET | Health check |

### Instance & Database Management

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/databases` | GET | List available databases on the current instance |
| `/api/databases/switch` | POST | Switch to a different database |
| `/api/instances/switch` | POST | Switch to a different Lakebase instance (provisioned or autoscaling) |

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
3. **Multi-instance**: Add multiple `resources` in `app.yaml`, or grant the app SP on additional instances (see [Step 5](#step-5-grant-the-app-sp-access-to-lakebase-instances))

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
8. **Instance switcher — role + security label required on ALL instances**: The switcher connects via generated OAuth token even for instances in `app.yaml` resources. The `resources:` block only handles startup; it does not auto-configure the role/security label for token-based switching. Run the `CREATE ROLE` + `SECURITY LABEL` + `GRANT` SQL (see [Step 5c](#5c-for-each-instance-including-those-in-appyaml)) on every instance the app will switch to. Without the security label: `no role security label is configured`.
9. **Migration state expiry**: Prepared migrations/tuning sessions expire after 1 hour if not completed. Call `prepare_*` again to create a new session.

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
