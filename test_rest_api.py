"""Test script for Lakebase MCP Server REST API endpoints.

Usage:
    python test_rest_api.py --profile ay-sandbox

Tests all /api/* REST endpoints independently of the MCP protocol.
"""

import argparse
import json
import sys

import requests
from databricks.sdk import WorkspaceClient

APP_NAME = "lakebase-mcp-server"


def get_token_and_url(profile, app_name=None):
    w = WorkspaceClient(profile=profile)
    app = w.apps.get(app_name or APP_NAME)
    url = app.url.rstrip("/")
    token = w.config._header_factory().get("Authorization", "").removeprefix("Bearer ")
    return url, token


def api_get(base_url, token, path, params=None):
    resp = requests.get(
        f"{base_url}{path}",
        headers={"Authorization": f"Bearer {token}"},
        params=params,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def api_post(base_url, token, path, body=None):
    resp = requests.post(
        f"{base_url}{path}",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json=body or {},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def api_patch(base_url, token, path, body=None):
    resp = requests.patch(
        f"{base_url}{path}",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json=body or {},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="ay-sandbox")
    parser.add_argument("--app", default=None)
    args = parser.parse_args()

    url, token = get_token_and_url(args.profile, args.app)
    print(f"App URL: {url}\n")

    # R1. GET /api/info
    print("=== GET /api/info ===")
    info = api_get(url, token, "/api/info")
    assert "server" in info, f"Missing 'server' key: {info}"
    assert "connection_mode" in info, f"Missing 'connection_mode' key: {info}"
    print(f"  Server: {info['server']}, Mode: {info.get('connection_mode')}")
    print("  PASS\n")

    # R2. GET /api/tools
    print("=== GET /api/tools ===")
    tools_resp = api_get(url, token, "/api/tools")
    assert isinstance(tools_resp, list), f"Expected list, got {type(tools_resp)}"
    assert len(tools_resp) == 33, f"Expected 33 tools, got {len(tools_resp)}"
    print(f"  Tools: {len(tools_resp)}")
    print("  PASS\n")

    # R3. GET /api/tables
    print("=== GET /api/tables ===")
    tables_resp = api_get(url, token, "/api/tables")
    assert isinstance(tables_resp, list), f"Expected list, got {type(tables_resp)}"
    assert len(tables_resp) > 0, "No tables found"
    first_table = next(
        (t["table_name"] for t in tables_resp if not t["table_name"].startswith("_")),
        tables_resp[0]["table_name"],
    )
    print(f"  Tables: {len(tables_resp)}, using: {first_table}")
    print("  PASS\n")

    # R4. GET /api/tables/{table_name}
    print(f"=== GET /api/tables/{first_table} ===")
    table_detail = api_get(url, token, f"/api/tables/{first_table}")
    assert "columns" in table_detail, f"Missing columns: {table_detail}"
    print(f"  Columns: {len(table_detail['columns'])}")
    print("  PASS\n")

    # R5. GET /api/tables/{table_name}/sample
    print(f"=== GET /api/tables/{first_table}/sample ===")
    sample = api_get(url, token, f"/api/tables/{first_table}/sample", {"limit": 5})
    assert isinstance(sample, list), f"Expected list, got {type(sample)}"
    print(f"  Sample rows: {len(sample)}")
    print("  PASS\n")

    # Setup test table
    api_post(url, token, "/api/execute",
             {"sql": "CREATE TABLE IF NOT EXISTS _test_rest_api (id SERIAL PRIMARY KEY, val TEXT)"})

    # R6. POST /api/insert
    print("=== POST /api/insert ===")
    ins = api_post(url, token, "/api/insert",
                   {"table_name": "_test_rest_api", "record": {"val": "rest-test"}})
    assert "inserted" in ins, f"Missing inserted: {ins}"
    assert ins["inserted"] == 1
    row_id = ins["rows"][0]["id"]
    print(f"  Inserted id={row_id}")
    print("  PASS\n")

    # R7. POST /api/query
    print("=== POST /api/query ===")
    qr = api_post(url, token, "/api/query",
                  {"sql": "SELECT * FROM _test_rest_api LIMIT 5"})
    assert isinstance(qr, list), f"Expected list, got {type(qr)}"
    print(f"  Rows: {len(qr)}")
    print("  PASS\n")

    # R8. PATCH /api/update
    print("=== PATCH /api/update ===")
    upd = api_patch(url, token, "/api/update",
                    {"table_name": "_test_rest_api",
                     "set_values": {"val": "updated"},
                     "where": f"id = {row_id}"})
    assert upd.get("updated") == 1, f"Expected updated=1: {upd}"
    print(f"  Updated: {upd['updated']}")
    print("  PASS\n")

    # R9. DELETE /api/delete
    print("=== DELETE /api/delete ===")
    del_resp = requests.delete(
        f"{url}/api/delete",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"table_name": "_test_rest_api", "where": f"id = {row_id}"},
        timeout=30,
    )
    del_resp.raise_for_status()
    del_data = del_resp.json()
    assert del_data.get("deleted") == 1, f"Expected deleted=1: {del_data}"
    print(f"  Deleted: {del_data['deleted']}")
    print("  PASS\n")

    # R10. POST /api/execute
    print("=== POST /api/execute ===")
    ex = api_post(url, token, "/api/execute",
                  {"sql": f"SELECT count(*) AS cnt FROM {first_table}"})
    assert "type" in ex, f"Missing type: {ex}"
    print(f"  Type: {ex['type']}, row_count: {ex.get('row_count')}")
    print("  PASS\n")

    # R11. POST /api/transaction
    print("=== POST /api/transaction ===")
    txn = api_post(url, token, "/api/transaction",
                   {"statements": [
                       "INSERT INTO _test_rest_api (val) VALUES ('txn1')",
                       "INSERT INTO _test_rest_api (val) VALUES ('txn2')",
                   ]})
    assert txn.get("status") == "committed", f"Expected committed: {txn}"
    print(f"  Status: {txn['status']}")
    print("  PASS\n")

    # R12. POST /api/explain
    print("=== POST /api/explain ===")
    expl = api_post(url, token, "/api/explain",
                    {"sql": f"SELECT * FROM {first_table}"})
    assert "plan" in expl or "type" in expl, f"Missing plan/type: {expl}"
    print(f"  Keys: {list(expl.keys())[:5]}")
    print("  PASS\n")

    # R13. POST /api/tables/create
    print("=== POST /api/tables/create ===")
    create_r = api_post(url, token, "/api/tables/create",
                        {"table_name": "_test_rest_ct",
                         "columns": [{"name": "id", "type": "SERIAL", "constraints": "PRIMARY KEY"},
                                     {"name": "name", "type": "TEXT"}],
                         "if_not_exists": True})
    assert "status" in create_r, f"Missing status: {create_r}"
    print(f"  Status: {create_r['status']}")
    print("  PASS\n")

    # R14. POST /api/tables/alter (add_column)
    print("=== POST /api/tables/alter (add_column) ===")
    alter_r = api_post(url, token, "/api/tables/alter",
                       {"table_name": "_test_rest_ct",
                        "operation": "add_column",
                        "column_name": "score",
                        "column_type": "INTEGER",
                        "constraints": "DEFAULT 0"})
    assert "status" in alter_r, f"Missing status: {alter_r}"
    print(f"  Status: {alter_r['status']}")
    print("  PASS\n")

    # R15. POST /api/tables/drop — cleanup both test tables
    print("=== POST /api/tables/drop ===")
    for tbl in ["_test_rest_ct", "_test_rest_api"]:
        drop_r = api_post(url, token, "/api/tables/drop",
                          {"table_name": tbl, "confirm": True, "if_exists": True})
        assert drop_r.get("dropped") is True, f"Expected dropped=True: {drop_r}"
        print(f"  Dropped {tbl}: {drop_r['dropped']}")
    print("  PASS\n")

    # R16. GET /api/databases
    print("=== GET /api/databases ===")
    dbs_resp = api_get(url, token, "/api/databases")
    # Response is either a list or a dict with a "databases" key
    if isinstance(dbs_resp, list):
        dbs = dbs_resp
    else:
        assert isinstance(dbs_resp, dict), f"Unexpected databases response: {dbs_resp}"
        dbs = dbs_resp.get("databases", [])
    assert len(dbs) > 0, "No databases found"
    print(f"  Databases: {dbs}")
    print("  PASS\n")

    # R17. GET /api/projects
    print("=== GET /api/projects ===")
    projs = api_get(url, token, "/api/projects")
    assert isinstance(projs, list), f"Expected list: {projs}"
    print(f"  Projects: {len(projs)}")
    print("  PASS\n")

    # R18. GET /api/setup-role-sql
    print("=== GET /api/setup-role-sql ===")
    role_sql = api_get(url, token, "/api/setup-role-sql")
    assert "sql" in role_sql or "error" in role_sql, f"Unexpected response: {role_sql}"
    print(f"  Keys: {list(role_sql.keys())}")
    print("  PASS\n")

    # R19. GET /api/profile/{table_name}
    print(f"=== GET /api/profile/{first_table} ===")
    profile_r = api_get(url, token, f"/api/profile/{first_table}")
    assert "columns" in profile_r, f"Missing columns: {profile_r}"
    print(f"  Profiled {len(profile_r['columns'])} column(s)")
    print("  PASS\n")

    print("=" * 40)
    print("ALL REST API TESTS PASSED")
    print(f"App URL: {url}")


if __name__ == "__main__":
    main()
