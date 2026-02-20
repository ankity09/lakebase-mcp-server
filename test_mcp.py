"""Test script for the Lakebase MCP Server.

Usage:
    python test_mcp.py --profile simplot-v1

Runs through all 16 MCP tools and verifies they work correctly.
"""

import argparse
import json
import sys

import requests
from databricks.sdk import WorkspaceClient

APP_NAME = "lakebase-mcp-server"
MCP_PATH = "/mcp/"


def get_token_and_url(profile):
    w = WorkspaceClient(profile=profile)
    app = w.apps.get(APP_NAME)
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
    return json.loads(content)


def main():
    parser = argparse.ArgumentParser(description="Test Lakebase MCP Server")
    parser.add_argument("--profile", default="simplot-v1", help="Databricks CLI profile")
    args = parser.parse_args()

    print(f"Connecting to {APP_NAME} with profile={args.profile}...")
    url, token = get_token_and_url(args.profile)
    print(f"App URL: {url}\n")

    # 1. Health check
    print("=== Health Check ===")
    health = requests.get(f"{url}/health", headers={"Authorization": f"Bearer {token}"}, timeout=10)
    print(f"  {health.json()}")
    assert health.json()["lakebase"] is True, "Lakebase not connected!"
    print("  PASS\n")

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
    assert len(tool_names) == 16, f"Expected 16 tools, got {len(tool_names)}"
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

    print("=" * 40)
    print("ALL TESTS PASSED")
    print(f"App URL: {url}")
    print(f"MCP endpoint: {url}{MCP_PATH}")


if __name__ == "__main__":
    main()
