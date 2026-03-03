# Lakebase MCP Server — Databricks App

Reusable MCP server that exposes Lakebase (PostgreSQL) read/write operations as 34 MCP tools (with safety annotations), 2 resources, and 3 prompts. Supports both provisioned Lakebase (via app.yaml database resource) and autoscaling Lakebase (via env vars). Token refresh is automatic. Deploy as a Databricks App, then connect it to a MAS Supervisor as an External MCP Server agent. Includes a web UI for exploring the database and testing tools.

## Architecture

```
MAS Demo A → /db/demo_a_db/mcp/  ─┐
MAS Demo B → /db/demo_b_db/mcp/  ─┤→ Lakebase MCP Server (single app)
Browser UI  → /mcp/ (default db)  ─┘     ContextVar per-request → lazy pool per database
    |
    |-- /                       Web UI (database explorer, SQL query, tool playground, docs)
    |-- /mcp/                   MCP endpoint (default database, StreamableHTTP, stateless)
    |-- /db/{database}/mcp/     MCP endpoint scoped to a specific database
    |-- /api/*                  REST API (tables, query, insert, update, delete)
    |-- /health                 Health check
    |-- /db/{database}/health   Per-database health check
    |
    | psycopg2 (PostgreSQL wire protocol)
    v
Lakebase Instance (PostgreSQL-compatible)
```

## Multi-Database Routing

A single deployed MCP server can serve multiple databases on the same Lakebase instance without redeployment. Each database gets its own URL namespace.

### URL Patterns

| Pattern | Description |
|---------|-------------|
| `/db/{database}/mcp/` | MCP endpoint scoped to `{database}` |
| `/db/{database}/health` | Health check for `{database}` (verifies pool connectivity) |
| `/mcp/` | MCP endpoint using the default database (from `PGDATABASE` or `LAKEBASE_DATABASE`) |
| `/health` | Health check for the default database |

### How It Works

1. **ContextVar per-request scoping** — Each incoming request to `/db/{database}/mcp/` sets a `ContextVar` with the target database name. All downstream code (tool handlers, connection helpers) reads from this `ContextVar` to determine which database to query. This is concurrent-safe: simultaneous requests to different databases are fully isolated.

2. **Lazy per-database connection pools** — The server maintains a dictionary of connection pools keyed by database name. A pool is created on first request to that database and reused for subsequent requests. Token refresh applies to all pools.

3. **Backward compatibility** — The original `/mcp/` endpoint still works and routes to the default database configured via `PGDATABASE` (provisioned) or `LAKEBASE_DATABASE` (autoscaling). Existing MAS connections do not need any changes.

### Example: Two MAS demos sharing one server

```
MAS Demo A  →  UC HTTP Connection with base_path=/db/supply_chain_db/mcp/
MAS Demo B  →  UC HTTP Connection with base_path=/db/finance_db/mcp/
Browser UI  →  /mcp/ (default database from app.yaml)
```

All three route to the same Databricks App. The server creates separate connection pools for `supply_chain_db` and `finance_db` on demand.

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
Set `LAKEBASE_PROJECT` as an env var. The server auto-discovers the endpoint, generates credentials, and refreshes tokens every 50 minutes. No `resources:` block needed.

```yaml
command:
  - python
  - mcp_server.py

env:
  - name: LAKEBASE_PROJECT_NAME
    value: "Manufacturing EPL"                       # human-readable label shown in the UI
  - name: LAKEBASE_PROJECT
    value: "b10eb92b-dc0e-4ccf-ba26-0653c5e7ebec"   # project ID (UUID from Lakebase project URL)
  - name: LAKEBASE_BRANCH
    value: "br-cool-haze-d2hbxj2m"                  # branch ID (from branch overview page, NOT the display name)
  - name: LAKEBASE_DATABASE
    value: "databricks_postgres"                     # optional, default: databricks_postgres
```

**Finding the values:**
- `LAKEBASE_PROJECT_NAME`: Any human-readable label you want shown in the web UI (e.g. `"Manufacturing EPL"`, `"Supply Chain Demo"`)
- `LAKEBASE_PROJECT`: In Databricks → Lakebase → click your project → the UUID is in the page URL or shown in the branch overview breadcrumb
- `LAKEBASE_BRANCH`: In Databricks → Lakebase → your project → click the branch → **ID** field on the branch overview (looks like `br-cool-haze-d2hbxj2m`). This is **not** the display name.
- `LAKEBASE_ENDPOINT` is not needed — the server auto-discovers the first active endpoint on the branch

## MCP Tools (34)

All tools include MCP annotations (`readOnlyHint`, `destructiveHint`, `idempotentHint`) for client-side safety UI.

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
| `list_projects` | INFRA | List all Lakebase projects with status |
| `describe_project` | INFRA | Project details + branches + endpoints |
| `get_connection_string` | INFRA | Build psql/psycopg2/jdbc connection string for an endpoint |
| `list_branches` | INFRA | List branches on a project with state |
| `list_endpoints` | INFRA | List endpoints on a branch with host and compute config |
| `get_endpoint_status` | INFRA | Endpoint state, host, compute configuration |
| `create_branch` | BRANCH | Create a dev/test branch from a parent |
| `delete_branch` | BRANCH | Delete a branch (not production) with confirm safety |
| `configure_autoscaling` | SCALE | Set min/max compute units on an endpoint |
| `configure_scale_to_zero` | SCALE | Enable/disable suspend + idle timeout |
| `profile_table` | QUALITY | Column-level profiling: nulls, distinct, min/max, avg |
| `describe_branch` | EXPLORE | Tree view of all objects in a database: schemas, tables, views, functions, sequences, indexes |
| `compare_database_schema` | EXPLORE | Compare schemas between two databases, returns unified diff |
| `prepare_database_migration` | MIGRATION | Phase 1: Dry-run migration SQL in rolled-back transaction, returns migration_id |
| `complete_database_migration` | MIGRATION | Phase 2: Apply or discard a validated migration using migration_id |
| `prepare_query_tuning` | TUNING | Phase 1: EXPLAIN ANALYZE + seq scan detection + index suggestions, returns tuning_id |
| `complete_query_tuning` | TUNING | Phase 2: Apply suggested DDL (e.g. CREATE INDEX) or discard, shows before/after plan |
| `search` | EXPLORE | Search across all Lakebase instances, databases, and tables by keyword |

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
| `/api/projects` | GET | List all Lakebase projects |
| `/api/projects/{id}` | GET | Describe a Lakebase project with branches |
| `/api/branches` | GET | List branches on a project (?project=...) |
| `/api/branches/create` | POST | Create a new branch |
| `/api/branches/{name}` | DELETE | Delete a branch |
| `/api/endpoints` | GET | List endpoints (?project=...&branch=...) |
| `/api/endpoints/{name}/config` | PATCH | Configure autoscaling/scale-to-zero |
| `/api/profile/{table}` | GET | Profile a table (column-level stats) |
| `/api/setup-role-sql` | GET | Generate OAuth role SQL for the app SP (autoscaling only) |

## Web UI

The root URL (`/`) serves a single-page application with Neon.tech-inspired sidebar navigation:

**Database**
1. **Database Explorer** — Browse tables, view schemas, sample data
2. **SQL Query** — Execute read-only queries with tabular results
3. **MCP Tools** — Tool reference + interactive playground to test each tool (categorized by type)

**Infrastructure**
4. **Projects** — Card grid of Lakebase projects with status badges
5. **Branches** — Branch management with create/delete operations
6. **Endpoints** — Endpoint cards with autoscaling and scale-to-zero configuration

**Quality**
7. **Profiler** — Column-level data profiling (nulls, distinct, min/max, avg)

**Reference**
8. **Documentation** — Deployment guide, MAS connection instructions, gotchas

The sidebar includes a database switcher dropdown and connection status indicator.

Infrastructure tools use the `w.postgres.*` SDK methods and require autoscaling Lakebase or latest SDK. They return clear error messages when not available.

## Reuse Across Demos

**Provisioned:** Change `instance_name` and `database_name` in `app/app.yaml`:

```yaml
resources:
  - name: database
    database:
      instance_name: my-instance      # change per demo
      database_name: my_database      # change per demo
      permission: CAN_CONNECT_AND_CREATE
```

**Autoscaling:** Change the env vars in `app/app.yaml`:

```yaml
env:
  - name: LAKEBASE_PROJECT
    value: "b10eb92b-dc0e-4ccf-ba26-0653c5e7ebec"  # your project UUID
  - name: LAKEBASE_BRANCH
    value: "br-cool-haze-d2hbxj2m"                 # your branch ID
  - name: LAKEBASE_DATABASE
    value: "my_database"
```

No code changes needed. The UI and tools work with any Lakebase database.

## Deployment

### Prerequisites

- A Lakebase instance already created in your workspace
- Code pushed to GitHub (or another location accessible from the workspace file browser)

### Option A: Deploy via the Databricks UI (recommended)

**Step 1: Upload code to workspace**

In the Databricks UI, go to **Workspace** and navigate to your user folder. Upload the `app/` directory (or clone the repo via Git in the workspace file browser).

**Step 2: Configure app.yaml**

Edit `app/app.yaml` with your Lakebase details (see [Reuse Across Demos](#reuse-across-demos) above). Use provisioned mode for named databases or autoscaling mode for Neon-based instances.

**Step 3: Create and deploy the app**

1. In the Databricks UI, go to **Apps** → **Create App**
2. Select **Custom** → point to your `app/` directory in the workspace
3. Click **Deploy**

**Step 4: Grant app permissions**

In Apps → your app → **Permissions** tab:
- Add **All workspace users** → `Can Use` (required for MAS MCP proxy)

**Step 5: Grant Lakebase access to the app SP**

The app runs as an auto-created service principal. Find its UUID in Apps → your app → **Authorization** tab.

**Both provisioned and autoscaling instances require the same three-step SQL setup** — `CREATE ROLE`, `SECURITY LABEL`, and `GRANT`. The security label is always required because the instance switcher connects via OAuth token regardless of instance type.

Use the built-in helper to generate the correct SQL with the integer SCIM ID pre-filled:

```
GET https://<app-url>/api/setup-role-sql
```

Copy the returned SQL and run it in the instance's SQL editor (provisioned: **Edit data** on the instance page; autoscaling: branch → **Roles & Databases** → **Edit data**).

The SQL looks like:
```sql
CREATE ROLE "<app-sp-uuid>" WITH LOGIN;
SECURITY LABEL FOR databricks_auth ON ROLE "<app-sp-uuid>"
  IS 'id=<integer-scim-id>,type=service_principal';
GRANT ALL ON DATABASE "<database>" TO "<app-sp-uuid>";
GRANT ALL ON ALL TABLES IN SCHEMA public TO "<app-sp-uuid>";
GRANT USAGE ON SCHEMA public TO "<app-sp-uuid>";
```

Run this SQL on **every** Lakebase instance the app needs to connect to — including instances listed in `app.yaml` resources if you want to use the instance switcher on them.

> **Who can run this SQL?** `CREATE ROLE` and `SECURITY LABEL` require **PostgreSQL superuser** privileges. In Databricks Lakebase, the workspace admin or the user who created the Lakebase project has superuser access. Run the SQL while logged in as that user — in the Lakebase UI "Edit data" editor, your own credentials (email) are used, so you must be the project owner or a workspace admin. Regular workspace users cannot run these statements.

**Step 6: Verify**

Visit `https://<app-url>/health` — expected: `{"status":"ok","lakebase":true}`

Visit `https://<app-url>/` for the full web UI.

---

### Option B: Deploy via Databricks CLI

```bash
# Step 1: Create the app
databricks apps create lakebase-mcp-server --profile=<PROFILE>

# Step 2: Sync code to workspace
databricks sync ./app /Workspace/Users/<email>/lakebase-mcp-server/app \
  --profile=<PROFILE> --watch=false

# Step 3: Deploy
databricks apps deploy lakebase-mcp-server \
  --source-code-path /Workspace/Users/<email>/lakebase-mcp-server/app \
  --profile=<PROFILE>

# Step 4: Grant CAN_USE to users group (required for MAS MCP proxy)
databricks api patch /api/2.0/permissions/apps/lakebase-mcp-server \
  --json '{"access_control_list":[{"group_name":"users","permission_level":"CAN_USE"}]}' \
  --profile=<PROFILE>
```

Then follow Step 5 (Lakebase access) and Step 6 (verify) from Option A above.

## Connecting to MAS

1. Create a UC HTTP connection:
   - URL: `https://<mcp-app-url>/mcp`
   - Auth: Databricks OAuth M2M (SP OAuth secret from Account Console)
   - Connection fields: host, port=443, base_path=/mcp/, client_id, client_secret, oauth_scope=all-apis
   - For multi-database setups, use `base_path=/db/{database}/mcp/` to scope the connection to a specific database

2. In MAS Supervisor config, add agent:
   - Type: **External MCP Server**
   - Connection: the UC HTTP connection above
   - Description: "Execute Lakebase database operations — CRUD on operational tables"

3. Click "Rediscover tools" in MAS config to detect the 34 MCP tools.

## Known Gotchas

1. **Trailing slash:** MCP endpoint is at `/mcp/` (with trailing slash). `/mcp` redirects to `/mcp/`.
2. **MAS sends JSON strings:** MAS agents serialize nested objects as JSON strings. Handled by `_ensure_dict()`.
3. **Table permissions:** Tables created via `databricks psql` are owned by the user. Grant to app SP after creating tables.
4. **OAuth M2M for UC connection:** SP OAuth secrets must be created at Account Console level.
5. **CAN_USE for users group:** MAS MCP proxy needs CAN_USE on the app. Grant to `users` group.
6. **Autoscaling scale-to-zero:** If the autoscaling endpoint is suspended, the first request may take 2-5 seconds while compute wakes up.
7. **Database switcher:** Switching databases reinitializes the connection pool. The table cache is cleared automatically.
8. **Instance switcher — role + security label required on ALL instances:** The instance switcher always connects via a generated OAuth token (even for instances listed in `app.yaml` resources). This means the app SP needs `CREATE ROLE` + `SECURITY LABEL FOR databricks_auth` + `GRANT` on every instance it will switch to — not just "extra" ones. The `app.yaml` resources block only handles the startup connection, not the switcher. Without the security label: `no role security label is configured`. Use `GET /api/setup-role-sql` to generate the correct SQL.
9. **Migration state expiry:** Prepared migrations/tuning sessions expire after 1 hour if not completed.
10. **Autoscaling branch ID vs display name:** `LAKEBASE_BRANCH` must be the branch **ID** (e.g. `br-cool-haze-d2hbxj2m`), not the display name (e.g. `production`). Find the ID in Databricks → Lakebase → your project → click the branch → **ID** field.
11. **Autoscaling OAuth role — integer SCIM ID required:** The PostgreSQL security label requires the SP's integer SCIM ID (`id=<int64>`), **not** the UUID application ID. Use `GET /api/setup-role-sql` to auto-generate the correct SQL with the SCIM ID pre-filled, instead of looking it up manually.
12. **Autoscaling credential generation:** The server uses `POST /api/2.0/postgres/credentials` (REST) for token generation. The `w.postgres.*` SDK methods fail in deployed environments; the server handles this internally via `w.api_client.do()`.
13. **Autoscaling project names:** The Lakebase autoscaling API does not return human-readable project names — only UUIDs. Set `LAKEBASE_PROJECT_NAME` in `app.yaml` to display a friendly name in the web UI.
14. **OAuth role requires superuser:** `CREATE ROLE` and `SECURITY LABEL` require PostgreSQL superuser. In Lakebase, only the workspace admin or the project owner has superuser access. Run the setup SQL in the Lakebase UI while logged in as that user.

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
export LAKEBASE_PROJECT=b10eb92b-dc0e-4ccf-ba26-0653c5e7ebec  # project UUID (from URL)
export LAKEBASE_BRANCH=br-cool-haze-d2hbxj2m                   # branch ID (not display name)
export LAKEBASE_DATABASE=my_db

pip install -r app/requirements.txt
python app/mcp_server.py
```
