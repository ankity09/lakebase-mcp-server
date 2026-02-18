# Lakebase MCP Server

A reusable [Model Context Protocol](https://modelcontextprotocol.io) (MCP) server for [Databricks Lakebase](https://docs.databricks.com/en/database/lakebase/index.html) (PostgreSQL-compatible databases). Deploy as a Databricks App to give AI agents read/write access to your operational data.

## What it does

- Exposes **6 MCP tools** (list tables, describe schema, SELECT queries, insert, update, delete)
- Connects to any Lakebase instance via Databricks App database resources
- Works as an **External MCP Server** agent in Databricks Multi-Agent Supervisors (MAS)
- Includes a **web UI** for database exploration, SQL queries, tool testing, and documentation
- Provides a **REST API** for custom integrations

## Architecture

```
AI Agent (MAS / Claude / etc.)
    |
    | MCP Protocol (StreamableHTTP)
    v
Lakebase MCP Server (Databricks App)
    |-- /          Web UI
    |-- /mcp/      MCP endpoint
    |-- /api/*     REST API
    |
    v
Lakebase Instance (PostgreSQL-compatible)
```

## Quick Start

### 1. Configure `app/app.yaml`

Point to your Lakebase instance:

```yaml
resources:
  - name: database
    database:
      instance_name: my-instance    # your Lakebase instance name
      database_name: my_database     # your database name
      permission: CAN_CONNECT_AND_CREATE
```

### 2. Deploy

```bash
# Create the app
databricks apps create lakebase-mcp-server --profile=<PROFILE>

# Sync code
databricks sync ./app /Workspace/Users/<email>/lakebase-mcp-server/app \
  --profile=<PROFILE> --watch=false

# Deploy
databricks apps deploy lakebase-mcp-server \
  --source-code-path /Workspace/Users/<email>/lakebase-mcp-server/app \
  --profile=<PROFILE>
```

### 3. Grant Permissions

```bash
# Required for MAS MCP proxy to access the app
databricks api patch /api/2.0/permissions/apps/lakebase-mcp-server \
  --json '{"access_control_list":[{"group_name":"users","permission_level":"CAN_USE"}]}' \
  --profile=<PROFILE>

# Grant table access to app service principal
databricks psql my-instance --profile=<PROFILE> -- -d my_database -c "
GRANT ALL ON ALL TABLES IN SCHEMA public TO \"<app-sp-client-id>\";
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO \"<app-sp-client-id>\";
"
```

### 4. Verify

```bash
curl https://<app-url>/health
# {"status":"ok","lakebase":true}
```

Visit `https://<app-url>/` for the web UI.

## MCP Tools

| Tool | Type | Description |
|------|------|-------------|
| `list_tables` | READ | List all tables with row counts and column counts |
| `describe_table` | READ | Get column names, data types, and constraints |
| `read_query` | READ | Execute read-only SELECT queries (max 500 rows) |
| `insert_record` | WRITE | Insert a single record with parameterized values |
| `update_records` | WRITE | Update rows matching a WHERE condition |
| `delete_records` | WRITE | Delete rows matching a WHERE condition |

## Web UI

The root URL serves a single-page app with:
- **Database Explorer** - Browse tables, view column schemas, sample data
- **SQL Query** - Run read-only queries with tabular results (Ctrl/Cmd+Enter)
- **MCP Tools** - Tool reference cards + interactive playground
- **Documentation** - Deployment guide, MAS connection instructions

## Connecting to MAS

1. Create a **UC HTTP Connection** with OAuth M2M auth pointing to `https://<app-url>/mcp`
2. Add an **External MCP Server** agent to your MAS using that connection
3. Click "Rediscover tools" to detect the 6 MCP tools

See the Documentation tab in the web UI for detailed instructions.

## REST API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/info` | GET | Server info and connection status |
| `/api/tools` | GET | List MCP tools with schemas |
| `/api/tables` | GET | List tables |
| `/api/tables/{name}` | GET | Describe table |
| `/api/tables/{name}/sample` | GET | Sample rows |
| `/api/query` | POST | Execute SELECT query |
| `/api/insert` | POST | Insert record |
| `/api/update` | PATCH | Update records |
| `/api/delete` | DELETE | Delete records |

## Reuse

This server is fully generic. Change only `instance_name` and `database_name` in `app.yaml` to point to any Lakebase database. No code changes needed.

## License

MIT
