# Lakebase MCP Server — Databricks App

Reusable MCP server that exposes Lakebase (PostgreSQL) read/write operations as MCP tools. Deploy as a Databricks App, then connect it to a MAS Supervisor as an External MCP Server agent.

## Architecture

```
MAS Supervisor
  ├── Genie Space agent (reads Delta Lake)
  └── Lakebase MCP Server (reads/writes Lakebase) ← THIS APP
          └── Starlette + MCP SDK (StreamableHTTP)
              └── psycopg2 → Lakebase PostgreSQL
```

## Tools (6)

| Tool | Type | Description |
|------|------|-------------|
| `list_tables` | READ | List all tables with row counts |
| `describe_table` | READ | Column names, types, constraints |
| `read_query` | READ | SELECT queries (max 500 rows) |
| `insert_record` | WRITE | Insert a record (parameterized) |
| `update_records` | WRITE | Update rows (parameterized SET + WHERE) |
| `delete_records` | WRITE | Delete rows matching WHERE condition |

## Reuse Across Demos

Change the `instance_name` and `database_name` in `app/app.yaml`:

```yaml
resources:
  - name: database
    database:
      instance_name: smartagri       # ← change per demo
      database_name: smartagri_db    # ← change per demo
      permission: CAN_CONNECT_AND_CREATE
```

## Deployment

### Prerequisites
- Databricks CLI configured with appropriate profile
- A Lakebase instance already created

### Step 1: Create the app

```bash
databricks apps create lakebase-mcp-server --profile=<PROFILE>
```

### Step 2: Sync code to workspace

```bash
databricks sync ./app /Workspace/Users/<email>/lakebase-mcp-server/app --profile=<PROFILE>
```

### Step 3: Deploy

```bash
databricks apps deploy lakebase-mcp-server \
  --source-code-path /Workspace/Users/<email>/lakebase-mcp-server/app \
  --profile=<PROFILE>
```

### Step 4: Add database resource

The database resource in app.yaml should trigger automatic resource provisioning. If not, add it via the Apps UI or PATCH API:

```bash
databricks api patch /api/2.0/apps/lakebase-mcp-server \
  --json '{"resources":[{"name":"database","database":{"instance_name":"supply-chain","database_name":"supply_chain_db","permission":"CAN_CONNECT_AND_CREATE"}}]}' \
  --profile=<PROFILE>
```

Then authorize the resource in the Databricks UI (App Settings > Resources).

### Step 5: Verify

```bash
# Health check
curl https://<app-url>/health

# Test with DatabricksMCPClient
python -c "
from databricks.sdk import WorkspaceClient
w = WorkspaceClient(profile='<PROFILE>')
# Use the MCP client to list_tools, call list_tables, etc.
"
```

## Connecting to MAS

1. Create a UC HTTP connection:
   - URL: `https://<mcp-app-url>/mcp`
   - Auth: Databricks OAuth (automatic)

2. In MAS Supervisor config, add agent:
   - Type: **External MCP Server**
   - Connection: the UC HTTP connection above
   - Description: "Execute Lakebase database operations — create, read, update, and delete records in operational tables"

## Lakebase Table Permissions

Tables created via `databricks psql` are owned by the user, not the app service principal. After creating tables, grant access:

```sql
-- Get the app SP client ID from: databricks apps get lakebase-mcp-server
GRANT ALL ON ALL TABLES IN SCHEMA public TO "<app-sp-client-id>";
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO "<app-sp-client-id>";
```

## Local Development

```bash
# Set env vars (from Lakebase instance details)
export PGHOST=instance-<uid>.database.cloud.databricks.com
export PGPORT=5432
export PGDATABASE=supply_chain_db
export PGUSER=<your-email>
export PGSSLMODE=require

pip install -r app/requirements.txt
python app/mcp_server.py
```

The server starts on port 8000 (or `DATABRICKS_APP_PORT`). MCP endpoint: `http://localhost:8000/mcp`
