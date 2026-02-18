"""Test script for the Lakebase MCP Server.

Usage:
    python test_mcp.py --profile simplot-v1

Runs through all 6 MCP tools and verifies they work correctly.
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
    assert len(tool_names) == 6, f"Expected 6 tools, got {len(tool_names)}"
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

    print("=" * 40)
    print("ALL TESTS PASSED")
    print(f"App URL: {url}")
    print(f"MCP endpoint: {url}{MCP_PATH}")


if __name__ == "__main__":
    main()
