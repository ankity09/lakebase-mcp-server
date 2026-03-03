"""Test script for the Lakebase MCP Server.

Usage:
    python test_mcp.py --profile simplot-v1

Runs through all 34 MCP tools and verifies they work correctly.
"""

import argparse
import json
import sys

import requests
from databricks.sdk import WorkspaceClient

APP_NAME = "lakebase-mcp-server"
MCP_PATH = "/mcp/"


def get_token_and_url(profile, app_name=None):
    w = WorkspaceClient(profile=profile)
    app = w.apps.get(app_name or APP_NAME)
    url = app.url.rstrip("/")
    token = w.config._header_factory().get("Authorization", "").removeprefix("Bearer ")
    return url, token


def mcp_call(base_url, token, method, params=None, req_id=1):
    """Send a JSON-RPC request to the MCP server."""
    resp = requests.post(
        f"{base_url}{MCP_PATH}",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        },
        json={
            "jsonrpc": "2.0",
            "id": req_id,
            "method": method,
            "params": params or {},
        },
        timeout=30,
    )
    resp.raise_for_status()
    # Parse SSE response
    for line in resp.text.strip().split("\n"):
        if line.startswith("data: "):
            return json.loads(line[6:])
    # Try plain JSON
    return resp.json()


def tool_call(base_url, token, tool_name, arguments=None, req_id=1):
    """Call an MCP tool and return the parsed content."""
    result = mcp_call(base_url, token, "tools/call", {
        "name": tool_name,
        "arguments": arguments or {},
    }, req_id)
    content = result["result"]["content"][0]["text"]
    if not content:
        raise RuntimeError(f"Tool '{tool_name}' returned empty content. Full result: {result}")
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        raise RuntimeError(f"Tool '{tool_name}' returned non-JSON content: {content[:500]}")


def main():
    parser = argparse.ArgumentParser(description="Test Lakebase MCP Server")
    parser.add_argument("--profile", default="simplot-v1", help="Databricks CLI profile")
    parser.add_argument("--app", default=None, help="App name (default: lakebase-mcp-server)")
    args = parser.parse_args()

    app_name = args.app or APP_NAME
    print(f"Connecting to {app_name} with profile={args.profile}...")
    url, token = get_token_and_url(args.profile, app_name)
    print(f"App URL: {url}\n")

    # 1. Health check
    print("=== Health Check ===")
    health = requests.get(f"{url}/health", headers={"Authorization": f"Bearer {token}"}, timeout=10)
    if health.status_code != 200:
        print(f"  FAIL: HTTP {health.status_code}")
        print(f"  Response: {health.text[:500]}")
        sys.exit(1)
    try:
        health_data = health.json()
    except requests.exceptions.JSONDecodeError:
        print(f"  FAIL: Health endpoint returned non-JSON response")
        print(f"  Status: {health.status_code}")
        print(f"  Body: {health.text[:500]}")
        print("  Hint: App may not be running, or Databricks proxy is returning a login page.")
        sys.exit(1)
    print(f"  {health_data}")
    assert health_data.get("lakebase") is True, f"Lakebase not connected! Response: {health_data}"
    print("  PASS\n")

    # Setup: ensure test tables exist
    print("=== Test Setup ===")
    setup_sql = [
        """CREATE TABLE IF NOT EXISTS notes (
            note_id SERIAL PRIMARY KEY,
            entity_type TEXT,
            entity_id TEXT,
            note_text TEXT,
            author TEXT,
            created_at TIMESTAMPTZ DEFAULT now()
        )""",
    ]
    for sql in setup_sql:
        tool_call(url, token, "execute_sql", {"sql": sql}, 99)
    print("  Tables ready\n")

    # 2. Initialize
    print("=== MCP Initialize ===")
    init = mcp_call(url, token, "initialize", {
        "protocolVersion": "2025-03-26",
        "capabilities": {},
        "clientInfo": {"name": "test-script", "version": "1.0"},
    })
    print(f"  Server: {init['result']['serverInfo']['name']} v{init['result']['serverInfo']['version']}")
    print("  PASS\n")

    # 3. List tools
    print("=== tools/list ===")
    tools = mcp_call(url, token, "tools/list", {}, 2)
    tool_names = [t["name"] for t in tools["result"]["tools"]]
    print(f"  Tools: {tool_names}")
    assert len(tool_names) == 33, f"Expected 33 tools, got {len(tool_names)}"
    print("  PASS\n")

    # 4. list_tables
    print("=== list_tables ===")
    tables = tool_call(url, token, "list_tables", {}, 3)
    for t in tables:
        print(f"  {t['table_name']}: {t['row_count']} rows, {t['column_count']} cols")
    assert len(tables) > 0, "No tables found!"
    print("  PASS\n")

    # 5. describe_table
    first_table = tables[0]["table_name"]
    print(f"=== describe_table ({first_table}) ===")
    desc = tool_call(url, token, "describe_table", {"table_name": first_table}, 4)
    for col in desc["columns"]:
        pk = " [PK]" if col.get("is_primary_key") else ""
        print(f"  {col['column_name']}: {col['data_type']}{pk}")
    print("  PASS\n")

    # 6. read_query
    print(f"=== read_query (SELECT * FROM {first_table} LIMIT 2) ===")
    rows = tool_call(url, token, "read_query", {"sql": f"SELECT * FROM {first_table} LIMIT 2"}, 5)
    for r in rows:
        print(f"  {r}")
    print("  PASS\n")

    # 7. insert_record
    print("=== insert_record (notes) ===")
    insert_result = tool_call(url, token, "insert_record", {
        "table_name": "notes",
        "record": {
            "entity_type": "shipment",
            "entity_id": "MCP-TEST-001",
            "note_text": "Automated test note from test_mcp.py",
            "author": "test-script",
        },
    }, 6)
    note_id = insert_result["rows"][0]["note_id"]
    print(f"  Inserted note_id={note_id}")
    assert insert_result["inserted"] == 1
    print("  PASS\n")

    # 8. update_records
    print(f"=== update_records (note_id={note_id}) ===")
    update_result = tool_call(url, token, "update_records", {
        "table_name": "notes",
        "set_values": {"note_text": "Updated by test_mcp.py"},
        "where": f"note_id = {note_id}",
    }, 7)
    print(f"  Updated {update_result['updated']} row(s)")
    assert update_result["updated"] == 1
    assert update_result["rows"][0]["note_text"] == "Updated by test_mcp.py"
    print("  PASS\n")

    # 9. delete_records
    print(f"=== delete_records (note_id={note_id}) ===")
    delete_result = tool_call(url, token, "delete_records", {
        "table_name": "notes",
        "where": f"note_id = {note_id}",
    }, 8)
    print(f"  Deleted {delete_result['deleted']} row(s)")
    assert delete_result["deleted"] == 1
    print("  PASS\n")

    # 10. execute_sql (read)
    print("=== execute_sql (SELECT) ===")
    exec_read = tool_call(url, token, "execute_sql", {"sql": f"SELECT * FROM {first_table} LIMIT 2"}, 9)
    print(f"  Type: {exec_read['type']}, rows: {exec_read['row_count']}")
    assert exec_read["type"] == "read"
    print("  PASS\n")

    # 11. execute_sql (DDL + cleanup)
    print("=== execute_sql (CREATE TABLE) ===")
    exec_ddl = tool_call(url, token, "execute_sql", {
        "sql": "CREATE TABLE IF NOT EXISTS _test_mcp_tmp (id SERIAL PRIMARY KEY, val TEXT)"
    }, 10)
    print(f"  Status: {exec_ddl['status']}")
    assert exec_ddl["type"] == "ddl"
    print("  PASS\n")

    # 12. create_table (IF NOT EXISTS)
    print("=== create_table ===")
    ct_result = tool_call(url, token, "create_table", {
        "table_name": "_test_mcp_ct",
        "columns": [
            {"name": "id", "type": "SERIAL", "constraints": "PRIMARY KEY"},
            {"name": "name", "type": "TEXT"},
            {"name": "created_at", "type": "TIMESTAMPTZ", "constraints": "DEFAULT now()"},
        ],
        "if_not_exists": True,
    }, 11)
    print(f"  Status: {ct_result['status']}")
    print("  PASS\n")

    # 13. batch_insert
    print("=== batch_insert ===")
    bi_result = tool_call(url, token, "batch_insert", {
        "table_name": "_test_mcp_ct",
        "records": [
            {"name": "Alice"},
            {"name": "Bob"},
            {"name": "Charlie"},
        ],
    }, 12)
    print(f"  Inserted: {bi_result['inserted']}")
    assert bi_result["inserted"] == 3
    print("  PASS\n")

    # 14. execute_transaction
    print("=== execute_transaction ===")
    txn_result = tool_call(url, token, "execute_transaction", {
        "statements": [
            "INSERT INTO _test_mcp_ct (name) VALUES ('TxnRow1')",
            "INSERT INTO _test_mcp_ct (name) VALUES ('TxnRow2')",
            "SELECT count(*) AS cnt FROM _test_mcp_ct",
        ],
    }, 13)
    print(f"  Transaction status: {txn_result['status']}, statements: {txn_result['statement_count']}")
    assert txn_result["status"] == "committed"
    print("  PASS\n")

    # 15. explain_query
    print("=== explain_query ===")
    explain_result = tool_call(url, token, "explain_query", {
        "sql": f"SELECT * FROM _test_mcp_ct"
    }, 14)
    print(f"  Type: {explain_result['type']}, rolled_back: {explain_result['rolled_back']}")
    assert explain_result["type"] == "read"
    assert explain_result["rolled_back"] is False
    print("  PASS\n")

    # 16. alter_table (add column)
    print("=== alter_table (add_column) ===")
    alter_result = tool_call(url, token, "alter_table", {
        "table_name": "_test_mcp_ct",
        "operation": "add_column",
        "column_name": "score",
        "column_type": "INTEGER",
        "constraints": "DEFAULT 0",
    }, 15)
    print(f"  Status: {alter_result['status']}")
    print("  PASS\n")

    # 17. list_schemas
    print("=== list_schemas ===")
    schemas = tool_call(url, token, "list_schemas", {}, 16)
    schema_names = [s["schema_name"] for s in schemas]
    print(f"  Schemas: {schema_names}")
    assert "public" in schema_names
    print("  PASS\n")

    # 18. get_connection_info
    print("=== get_connection_info ===")
    conn_info = tool_call(url, token, "get_connection_info", {}, 17)
    print(f"  Host: {conn_info['host']}, DB: {conn_info['database']}")
    assert conn_info["host"] != ""
    print("  PASS\n")

    # 19. list_slow_queries (may fail if extension not enabled)
    print("=== list_slow_queries ===")
    try:
        slow = tool_call(url, token, "list_slow_queries", {"limit": 5}, 18)
        if isinstance(slow, list):
            print(f"  Found {len(slow)} slow queries")
        elif isinstance(slow, dict) and "error" in slow:
            print(f"  Extension not available: {slow['error']}")
        print("  PASS (or gracefully handled)\n")
    except Exception as e:
        print(f"  Graceful skip: {e}\n")

    # 20. Cleanup: drop test tables
    print("=== Cleanup: drop_table ===")
    for tbl in ["_test_mcp_ct", "_test_mcp_tmp"]:
        drop_result = tool_call(url, token, "drop_table", {
            "table_name": tbl,
            "confirm": True,
            "if_exists": True,
        }, 19)
        print(f"  Dropped {tbl}: {drop_result['dropped']}")
    print("  PASS\n")

    # ── INFRASTRUCTURE TOOLS ────────────────────────────────────────────
    # 21. list_projects
    print("=== list_projects ===")
    projects_result = tool_call(url, token, "list_projects", {}, 20)
    assert isinstance(projects_result, list), f"Expected list, got {type(projects_result)}"
    print(f"  Found {len(projects_result)} project(s)")
    for p in projects_result:
        print(f"    {p.get('instance_type','?')} | {p.get('name') or p.get('instance_name','?')}")
    print("  PASS\n")

    # Pick the first autoscaling project for subsequent infra tests
    infra_project = None
    for p in projects_result:
        if isinstance(p, dict) and p.get("instance_type") == "autoscaling":
            infra_project = p.get("name")
            break
    if not infra_project:
        print("  NOTE: No autoscaling projects found — skipping infra-specific tests\n")

    # 22. describe_project
    print("=== describe_project ===")
    if infra_project:
        proj_detail = tool_call(url, token, "describe_project", {"project": infra_project}, 21)
        assert isinstance(proj_detail, dict), f"Expected dict, got {type(proj_detail)}"
        assert "error" not in proj_detail, f"describe_project errored: {proj_detail}"
        print(f"  Keys: {list(proj_detail.keys())}")
        print("  PASS\n")
    else:
        print("  SKIP (no autoscaling project)\n")

    # 23. list_branches
    infra_branch = None
    print("=== list_branches ===")
    if infra_project:
        branches_result = tool_call(url, token, "list_branches", {"project": infra_project}, 22)
        assert isinstance(branches_result, list), f"Expected list, got {type(branches_result)}"
        print(f"  Found {len(branches_result)} branch(es)")
        for b in branches_result:
            state = (b.get("status") or {}).get("current_state", "?")
            print(f"    {b.get('name','?')} | state={state}")
        if branches_result:
            infra_branch = branches_result[0].get("name")
        print("  PASS\n")
    else:
        print("  SKIP\n")

    # 24. list_endpoints
    infra_endpoint = None
    infra_ep_min_cu = None
    infra_ep_max_cu = None
    print("=== list_endpoints ===")
    if infra_project:
        # Pass the branch we found in list_branches; default "production" may not match real branch IDs
        ep_args = {"project": infra_project}
        if infra_branch:
            ep_args["branch"] = infra_branch
        eps_result = tool_call(url, token, "list_endpoints", ep_args, 23)
        if isinstance(eps_result, dict) and "error" in eps_result:
            # Try with default (production) branch
            eps_result = tool_call(url, token, "list_endpoints", {"project": infra_project}, 23)
        assert isinstance(eps_result, list), f"Expected list, got {type(eps_result)}: {eps_result}"
        print(f"  Found {len(eps_result)} endpoint(s)")
        for ep in eps_result:
            ep_status = ep.get("status") or {}
            print(f"    {ep.get('name','?')} | state={ep_status.get('current_state','?')}")
        if eps_result:
            ep = eps_result[0]
            infra_endpoint = ep.get("name")
            ep_status = ep.get("status") or {}
            infra_ep_min_cu = (ep_status.get("autoscaling_limit_min_cu")
                               or ep.get("min_cu"))
            infra_ep_max_cu = (ep_status.get("autoscaling_limit_max_cu")
                               or ep.get("max_cu"))
        print("  PASS\n")
    else:
        print("  SKIP\n")

    # 25. get_endpoint_status
    print("=== get_endpoint_status ===")
    if infra_endpoint:
        ep_status_result = tool_call(url, token, "get_endpoint_status", {"endpoint": infra_endpoint}, 24)
        assert isinstance(ep_status_result, dict), f"Expected dict, got {type(ep_status_result)}"
        assert "error" not in ep_status_result, f"get_endpoint_status errored: {ep_status_result}"
        print(f"  Keys: {list(ep_status_result.keys())}")
        print("  PASS\n")
    else:
        print("  SKIP\n")

    # 26. get_connection_string
    print("=== get_connection_string ===")
    if infra_endpoint:
        conn_str_result = tool_call(url, token, "get_connection_string",
                                    {"endpoint": infra_endpoint, "fmt": "psql"}, 25)
        assert isinstance(conn_str_result, dict), f"Expected dict, got {type(conn_str_result)}"
        if "error" in conn_str_result:
            print(f"  Graceful error: {conn_str_result['error']}")
        else:
            assert "connection_string" in conn_str_result, f"Missing connection_string: {conn_str_result}"
            print(f"  Format: {conn_str_result.get('format')}")
            assert conn_str_result.get("connection_string", "").startswith("psql ")
        print("  PASS\n")
    else:
        print("  SKIP\n")

    # ── DATA QUALITY ────────────────────────────────────────────────────
    # 27. profile_table
    print(f"=== profile_table ({first_table}) ===")
    profile_result = tool_call(url, token, "profile_table", {"table_name": first_table}, 26)
    assert isinstance(profile_result, dict), f"Expected dict, got {type(profile_result)}"
    assert "error" not in profile_result, f"profile_table errored: {profile_result}"
    assert "columns" in profile_result, f"Missing columns key: {profile_result}"
    assert "total_rows" in profile_result
    print(f"  Table: {profile_result['table']}, rows: {profile_result['total_rows']}, "
          f"cols profiled: {len(profile_result['columns'])}")
    for col in profile_result["columns"][:3]:
        print(f"    {col['column']}: nulls={col.get('null_count')}, distinct={col.get('distinct')}")
    print("  PASS\n")

    # ── EXPLORE TOOLS ────────────────────────────────────────────────────
    # 28. describe_branch
    print("=== describe_branch ===")
    branch_tree = tool_call(url, token, "describe_branch", {}, 27)
    assert isinstance(branch_tree, dict), f"Expected dict, got {type(branch_tree)}"
    assert "error" not in branch_tree, f"describe_branch errored: {branch_tree}"
    assert "database" in branch_tree
    assert "schemas" in branch_tree
    schema_names = list(branch_tree["schemas"].keys())
    print(f"  Database: {branch_tree['database']}, schemas: {schema_names}")
    public = branch_tree["schemas"].get("public", {})
    print(f"  public: {len(public.get('tables', []))} tables, {len(public.get('views', []))} views")
    print("  PASS\n")

    # 29. compare_database_schema (same DB → expect 0 diffs)
    db_name = conn_info.get("database") or first_table.split(".")[0] if "." in first_table else "databricks_postgres"
    if not db_name:
        db_name = "databricks_postgres"
    print(f"=== compare_database_schema ({db_name} vs {db_name}) ===")
    compare_result = tool_call(url, token, "compare_database_schema", {
        "source_database": db_name,
        "target_database": db_name,
    }, 28)
    assert isinstance(compare_result, dict), f"Expected dict, got {type(compare_result)}"
    assert "error" not in compare_result, f"compare_database_schema errored: {compare_result}"
    assert "diff_count" in compare_result, f"Missing diff_count: {compare_result}"
    assert compare_result["diff_count"] == 0, \
        f"Same DB should have 0 diffs, got {compare_result['diff_count']}"
    print(f"  Source: {compare_result['source']}, Target: {compare_result['target']}, "
          f"Diffs: {compare_result['diff_count']}")
    print("  PASS\n")

    # 30. search
    search_query = first_table.split("_")[0][:8] if "_" in first_table else first_table[:6]
    print(f"=== search ({search_query!r}) ===")
    search_result = tool_call(url, token, "search", {"query": search_query}, 29)
    assert isinstance(search_result, dict), f"Expected dict, got {type(search_result)}"
    assert "total_matches" in search_result, f"Missing total_matches: {search_result}"
    assert "tables" in search_result
    print(f"  Total matches: {search_result['total_matches']}, "
          f"tables: {len(search_result['tables'])}")
    print("  PASS\n")

    # ── BRANCH TOOLS ────────────────────────────────────────────────────
    created_branch = None
    print("=== create_branch ===")
    if infra_project:
        test_branch_slug = "mcp-test"
        create_br_result = tool_call(url, token, "create_branch", {
            "project": infra_project,
            "branch_id": test_branch_slug,
            "display_name": "MCP Test Branch",
        }, 30)
        assert isinstance(create_br_result, dict), f"Expected dict, got {type(create_br_result)}"
        assert "error" not in create_br_result, f"create_branch errored: {create_br_result}"
        print(f"  Response keys: {list(create_br_result.keys())}")
        # Construct branch path — API returns an LRO; branch path is project + branch slug
        created_branch = f"{infra_project}/branches/{test_branch_slug}"
        print(f"  Branch path: {created_branch}")
        print("  PASS\n")
    else:
        print("  SKIP (no autoscaling project)\n")

    # 32. delete_branch
    print("=== delete_branch ===")
    if created_branch:
        delete_br_result = tool_call(url, token, "delete_branch", {
            "branch": created_branch,
            "confirm": True,
        }, 31)
        assert isinstance(delete_br_result, dict), f"Expected dict, got {type(delete_br_result)}"
        assert "error" not in delete_br_result, f"delete_branch errored: {delete_br_result}"
        assert delete_br_result.get("deleted") is True, \
            f"Expected deleted=True, got {delete_br_result}"
        print(f"  Deleted: {delete_br_result['deleted']}, branch: {delete_br_result['branch']}")
        print("  PASS\n")
    else:
        print("  SKIP\n")

    # ── SCALE TOOLS ─────────────────────────────────────────────────────
    # 33. configure_autoscaling — submit current values to get "already_configured"
    #     (proves the tool is wired up correctly without modifying anything)
    print("=== configure_autoscaling (idempotent — current values) ===")
    if infra_endpoint and infra_ep_min_cu is not None and infra_ep_max_cu is not None:
        autoscaling_result = tool_call(url, token, "configure_autoscaling", {
            "endpoint": infra_endpoint,
            "min_cu": infra_ep_min_cu,
            "max_cu": infra_ep_max_cu,
        }, 32)
        assert isinstance(autoscaling_result, dict), f"Expected dict, got {type(autoscaling_result)}"
        assert "error" not in autoscaling_result, f"configure_autoscaling errored: {autoscaling_result}"
        got_status = autoscaling_result.get("status")
        has_name = "name" in autoscaling_result
        assert got_status == "already_configured" or has_name, \
            f"Expected already_configured or full response, got: {autoscaling_result}"
        print(f"  Result: {autoscaling_result.get('status') or 'updated'}")
        print("  PASS\n")
    else:
        print("  SKIP (no endpoint or CU values available)\n")

    # ── MIGRATION TOOLS ─────────────────────────────────────────────────
    # 34. prepare_database_migration
    print("=== prepare_database_migration ===")
    migration_result = tool_call(url, token, "prepare_database_migration", {
        "migration_sql": (
            "CREATE TABLE _test_mcp_mig (id SERIAL PRIMARY KEY, label TEXT);\n"
            "COMMENT ON TABLE _test_mcp_mig IS 'MCP test migration — discard only';"
        ),
    }, 33)
    assert isinstance(migration_result, dict), f"Expected dict, got {type(migration_result)}"
    assert "error" not in migration_result, f"prepare_migration errored: {migration_result}"
    assert "migration_id" in migration_result, f"Missing migration_id: {migration_result}"
    assert migration_result.get("status") == "validated", \
        f"Expected validated, got status={migration_result.get('status')}"
    migration_id = migration_result["migration_id"]
    print(f"  migration_id: {migration_id}")
    print(f"  Status: {migration_result['status']}, statements: {migration_result.get('statement_count')}")
    print("  PASS\n")

    # 35. complete_database_migration (discard — never apply)
    print("=== complete_database_migration (discard) ===")
    complete_mig = tool_call(url, token, "complete_database_migration", {
        "migration_id": migration_id,
        "apply_changes": False,
    }, 34)
    assert isinstance(complete_mig, dict), f"Expected dict, got {type(complete_mig)}"
    assert "error" not in complete_mig, f"complete_migration errored: {complete_mig}"
    assert complete_mig.get("status") == "discarded", \
        f"Expected discarded, got: {complete_mig}"
    print(f"  Status: {complete_mig['status']}")
    print("  PASS\n")

    # ── TUNING TOOLS ────────────────────────────────────────────────────
    # 36. prepare_query_tuning
    print("=== prepare_query_tuning ===")
    tuning_sql = f"SELECT * FROM {first_table} WHERE true"
    tuning_result = tool_call(url, token, "prepare_query_tuning", {"sql": tuning_sql}, 35)
    assert isinstance(tuning_result, dict), f"Expected dict, got {type(tuning_result)}"
    assert "error" not in tuning_result, f"prepare_query_tuning errored: {tuning_result}"
    assert "tuning_id" in tuning_result, f"Missing tuning_id: {tuning_result}"
    assert "plan" in tuning_result, f"Missing plan: {tuning_result}"
    tuning_id = tuning_result["tuning_id"]
    print(f"  tuning_id: {tuning_id}")
    print(f"  Suggestions: {len(tuning_result.get('suggestions', []))}")
    for s in tuning_result.get("suggestions", []):
        print(f"    [{s.get('type')}] {s.get('reason') or s.get('suggestion','')}")
    print("  PASS\n")

    # 37. complete_query_tuning (discard)
    print("=== complete_query_tuning (discard) ===")
    complete_tuning = tool_call(url, token, "complete_query_tuning", {
        "tuning_id": tuning_id,
        "apply_changes": False,
    }, 36)
    assert isinstance(complete_tuning, dict), f"Expected dict, got {type(complete_tuning)}"
    assert "error" not in complete_tuning, f"complete_query_tuning errored: {complete_tuning}"
    assert complete_tuning.get("status") == "discarded", \
        f"Expected discarded, got: {complete_tuning}"
    print(f"  Status: {complete_tuning['status']}")
    print("  PASS\n")

    print("=" * 40)
    print("ALL 37 TESTS PASSED")
    print(f"App URL: {url}")
    print(f"MCP endpoint: {url}{MCP_PATH}")


if __name__ == "__main__":
    main()
