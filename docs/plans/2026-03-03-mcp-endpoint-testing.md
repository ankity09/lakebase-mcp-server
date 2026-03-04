# MCP Endpoint Testing — End-to-End Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Run and verify all 33 MCP tools, 2 resources, 3 prompts, and all REST API endpoints on the deployed Lakebase MCP Server, adding missing test coverage and fixing any failures.

**Architecture:** The server is a Starlette + MCP StreamableHTTP app deployed as a Databricks App. `test_mcp.py` connects via the Databricks SDK (fetches app URL + bearer token), then sends JSON-RPC requests to `/mcp/`. The existing script covers tests 1–37 (all 33 tools). Gaps are: MCP resources, MCP prompts, REST API endpoints, multi-DB routing, and extra `alter_table` ops.

**Tech Stack:** Python 3.11+, `requests`, `databricks-sdk`, Databricks CLI profile (`simplot-v1`)

---

## Scope Map

| Coverage | Status |
|----------|--------|
| Health (`/health`, `/db/{db}/health`) | Tested (test 1) |
| MCP initialize | Tested (test 2) |
| tools/list (33 tools) | Tested (test 3) |
| All 33 MCP tools (tests 4–37) | Tested in `test_mcp.py` |
| MCP resources (`list_resources`, `read_resource`) | **MISSING** |
| MCP prompts (`list_prompts`, `get_prompt` ×3) | **MISSING** |
| REST API endpoints (`/api/*`) | **MISSING** |
| Multi-DB routing (`/db/{database}/mcp/`) | **MISSING** |
| `alter_table` rename/alter_type ops | **MISSING** |

---

## Task 1: Run Existing Tests — Establish Baseline

**Files:**
- Run: `test_mcp.py`

**Step 1: Run the existing test suite**

```bash
cd /Users/ankit.yadav/Desktop/Databricks/Customers/Simplot/demos/lakebase-mcp-server
python test_mcp.py --profile simplot-v1
```

Expected output: numbered test sections with PASS/FAIL/SKIP per test.

**Step 2: Record failures**

Copy the full terminal output. Note every line that says FAIL or shows an exception traceback.
Group by failure type:
- JSON parse error → server returns non-JSON
- Assertion error → unexpected value in response
- HTTP error → wrong status code
- KeyError → response missing expected key

**Step 3: Identify root cause for each failure**

For each failure, read the relevant `_tool_*` function in `app/mcp_server.py`.
Key functions by tool name can be found starting at line 1772.

**No commit yet — this is a discovery step.**

---

## Task 2: Fix Any Failing MCP Tool Tests

*(Skip if all 37 tests passed in Task 1.)*

**Files:**
- Modify: `app/mcp_server.py`
- Modify: `test_mcp.py`

**Step 1: For each FAIL, write the expected assertion**

Before fixing the server, write out what the correct behavior should be:
```
Tool: insert_record
Expected: result["inserted"] == 1 and "rows" in result
Got: KeyError: 'rows'
Fix: ensure _tool_insert_record returns {"inserted": N, "rows": [...]}
```

**Step 2: Fix the server function**

Edit only the minimal code needed. Example for a missing key:
```python
# Before (mcp_server.py ~line 1880)
return [TextContent(type="text", text=json.dumps({"inserted": rowcount}))]

# After
return [TextContent(type="text", text=json.dumps({"inserted": rowcount, "rows": rows}))]
```

**Step 3: Re-run the specific failing test**

```bash
python test_mcp.py --profile simplot-v1 2>&1 | grep -A 3 "=== <failing_tool> ==="
```

Expected: PASS

**Step 4: Run full suite to confirm no regressions**

```bash
python test_mcp.py --profile simplot-v1
```

Expected: all previously passing tests still pass.

**Step 5: Commit**

```bash
git add app/mcp_server.py test_mcp.py
git commit -m "fix: repair failing MCP tool tests"
git push
```

---

## Task 3: Add MCP Resources and Prompts Tests

**Files:**
- Modify: `test_mcp.py` (add after line 589, before `print("ALL 37 TESTS PASSED")`)

**Step 1: Understand the resource/prompt protocol**

Resources use `resources/list` and `resources/read` JSON-RPC methods.
Prompts use `prompts/list` and `prompts/get`.

**Step 2: Write the failing assertions first (TDD)**

Add this block to `test_mcp.py` before the final summary print:

```python
# ── MCP RESOURCES ──────────────────────────────────────────────────
# 38. resources/list
print("=== resources/list ===")
resources = mcp_call(url, token, "resources/list", {}, 37)
resource_uris = [r["uri"] for r in resources["result"]["resources"]]
print(f"  Resources: {resource_uris}")
assert "lakebase://tables" in resource_uris, f"Missing lakebase://tables: {resource_uris}"
print("  PASS\n")

# 39. resources/templates/list
print("=== resources/templates/list ===")
templates = mcp_call(url, token, "resources/templates/list", {}, 38)
template_uris = [t["uriTemplate"] for t in templates["result"]["resourceTemplates"]]
print(f"  Templates: {template_uris}")
assert any("tables" in u for u in template_uris), f"Missing table schema template: {template_uris}"
print("  PASS\n")

# 40. resources/read — lakebase://tables
print("=== resources/read (lakebase://tables) ===")
res_read = mcp_call(url, token, "resources/read", {"uri": "lakebase://tables"}, 39)
content = res_read["result"]["contents"][0]["text"]
table_list = json.loads(content)
assert isinstance(table_list, list), f"Expected list, got: {type(table_list)}"
assert len(table_list) > 0, "Table list resource returned empty"
print(f"  Tables in resource: {len(table_list)}")
print("  PASS\n")

# 41. resources/read — lakebase://tables/{name}/schema
print(f"=== resources/read (lakebase://tables/{first_table}/schema) ===")
schema_read = mcp_call(url, token, "resources/read",
                       {"uri": f"lakebase://tables/{first_table}/schema"}, 40)
schema_content = schema_read["result"]["contents"][0]["text"]
schema = json.loads(schema_content)
assert "columns" in schema, f"Missing columns in schema resource: {schema}"
print(f"  Columns in schema: {len(schema['columns'])}")
print("  PASS\n")

# ── MCP PROMPTS ────────────────────────────────────────────────────
# 42. prompts/list
print("=== prompts/list ===")
prompts = mcp_call(url, token, "prompts/list", {}, 41)
prompt_names = [p["name"] for p in prompts["result"]["prompts"]]
print(f"  Prompts: {prompt_names}")
assert "explore_database" in prompt_names
assert "design_schema" in prompt_names
assert "optimize_query" in prompt_names
print("  PASS\n")

# 43. prompts/get — explore_database
print("=== prompts/get (explore_database) ===")
explore_prompt = mcp_call(url, token, "prompts/get",
                          {"name": "explore_database", "arguments": {}}, 42)
messages = explore_prompt["result"]["messages"]
assert len(messages) > 0, "explore_database prompt returned no messages"
assert messages[0]["role"] == "user"
print(f"  Messages: {len(messages)}")
print("  PASS\n")

# 44. prompts/get — design_schema
print("=== prompts/get (design_schema) ===")
design_prompt = mcp_call(url, token, "prompts/get",
                         {"name": "design_schema",
                          "arguments": {"description": "A table for storing test results"}}, 43)
messages = design_prompt["result"]["messages"]
assert len(messages) > 0, "design_schema prompt returned no messages"
print(f"  Messages: {len(messages)}")
print("  PASS\n")

# 45. prompts/get — optimize_query
print("=== prompts/get (optimize_query) ===")
optimize_prompt = mcp_call(url, token, "prompts/get",
                           {"name": "optimize_query",
                            "arguments": {"sql": f"SELECT * FROM {first_table}"}}, 44)
messages = optimize_prompt["result"]["messages"]
assert len(messages) > 0, "optimize_query prompt returned no messages"
print(f"  Messages: {len(messages)}")
print("  PASS\n")
```

**Step 3: Run to see failures**

```bash
python test_mcp.py --profile simplot-v1 2>&1 | tail -40
```

Expected: new tests either PASS or fail with a specific error.

**Step 4: Fix server if needed**

If `resources/list` or `prompts/list` returns unexpected structure, inspect:
- `handle_list_resources()` at line 1628
- `handle_list_resource_templates()` at line 1640
- `handle_list_prompts()` at line 1670
- `handle_get_prompt()` at line 1702

**Step 5: Update final summary count**

Change `print("ALL 37 TESTS PASSED")` to `print("ALL 45 TESTS PASSED")` (or actual count).

**Step 6: Run full suite**

```bash
python test_mcp.py --profile simplot-v1
```

Expected: all tests PASS.

**Step 7: Commit**

```bash
git add test_mcp.py app/mcp_server.py
git commit -m "test: add MCP resources and prompts tests (tests 38-45)"
git push
```

---

## Task 4: Add REST API Endpoint Tests

**Files:**
- Create: `test_rest_api.py` (separate script — REST API is independent of MCP protocol)

**Step 1: Write the failing test structure**

Create `test_rest_api.py`:

```python
"""Test script for Lakebase MCP Server REST API endpoints.

Usage:
    python test_rest_api.py --profile simplot-v1

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


def api_delete(base_url, token, path):
    resp = requests.delete(
        f"{base_url}{path}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="simplot-v1")
    parser.add_argument("--app", default=None)
    args = parser.parse_args()

    url, token = get_token_and_url(args.profile, args.app)
    print(f"App URL: {url}\n")

    # R1. GET /api/info
    print("=== GET /api/info ===")
    info = api_get(url, token, "/api/info")
    assert "server" in info, f"Missing 'server' key: {info}"
    assert "connection" in info, f"Missing 'connection' key: {info}"
    print(f"  Server: {info['server']}, Mode: {info['connection'].get('mode')}")
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
    first_table = tables_resp[0]["table_name"]
    print(f"  Tables: {len(tables_resp)}, first: {first_table}")
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
    # Use requests directly since api_delete sends body differently
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

    # R15. POST /api/tables/drop
    print("=== POST /api/tables/drop ===")
    # cleanup both test tables
    for tbl in ["_test_rest_ct", "_test_rest_api"]:
        drop_r = api_post(url, token, "/api/tables/drop",
                          {"table_name": tbl, "confirm": True, "if_exists": True})
        assert drop_r.get("dropped") is True, f"Expected dropped=True: {drop_r}"
        print(f"  Dropped {tbl}: {drop_r['dropped']}")
    print("  PASS\n")

    # R16. GET /api/databases
    print("=== GET /api/databases ===")
    dbs = api_get(url, token, "/api/databases")
    assert isinstance(dbs, list), f"Expected list: {dbs}"
    print(f"  Databases: {dbs}")
    assert len(dbs) > 0, "No databases found"
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
```

**Step 2: Run test to see failures**

```bash
python test_rest_api.py --profile simplot-v1
```

**Step 3: Fix any server-side issues**

For each failure, identify the REST handler in `mcp_server.py`:
- `/api/info` → `api_info()` at line 3649
- `/api/tools` → `api_tools()` at line 3389
- `/api/tables` → `api_tables()` at line 3233
- `/api/insert` → `api_insert()` at line 3281
- `/api/query` → `api_query()` at line 3264
- etc.

**Step 4: Verify all pass**

```bash
python test_rest_api.py --profile simplot-v1
```

Expected: `ALL REST API TESTS PASSED`

**Step 5: Commit**

```bash
git add test_rest_api.py app/mcp_server.py
git commit -m "test: add REST API endpoint tests (19 endpoints)"
git push
```

---

## Task 5: Add Multi-Database Routing Tests

**Files:**
- Modify: `test_mcp.py` (append after resources/prompts tests)

**Step 1: Understand the multi-DB routing**

`/db/{database}/mcp/` sets a ContextVar with the target database, then forwards to the MCP session manager. To test: call `list_tables` via `/db/databricks_postgres/mcp/` and verify the same result as the default endpoint.

**Step 2: Add multi-DB tests to `test_mcp.py`**

Add this function before `main()`:

```python
def mcp_call_db(base_url, token, database, method, params=None, req_id=1):
    """Send a JSON-RPC request to the per-database MCP endpoint."""
    resp = requests.post(
        f"{base_url}/db/{database}/mcp/",
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
    for line in resp.text.strip().split("\n"):
        if line.startswith("data: "):
            return json.loads(line[6:])
    return resp.json()
```

And add this at the end of `main()` before the summary:

```python
# ── MULTI-DATABASE ROUTING ──────────────────────────────────────────
# 46. /db/{database}/mcp/ — list_tables
db_name = conn_info.get("database", "databricks_postgres")
print(f"=== /db/{db_name}/mcp/ (multi-DB routing) ===")
db_init = mcp_call_db(url, token, db_name, "initialize", {
    "protocolVersion": "2025-03-26",
    "capabilities": {},
    "clientInfo": {"name": "test-script", "version": "1.0"},
})
assert "result" in db_init, f"Multi-DB initialize failed: {db_init}"
db_tools = mcp_call_db(url, token, db_name, "tools/call", {
    "name": "list_tables",
    "arguments": {},
}, 2)
db_content = json.loads(db_tools["result"]["content"][0]["text"])
assert isinstance(db_content, list), f"Multi-DB list_tables returned {type(db_content)}"
print(f"  Tables via /db/{db_name}/mcp/: {len(db_content)}")
print("  PASS\n")

# 47. /db/{database}/health
print(f"=== /db/{db_name}/health ===")
db_health_resp = requests.get(
    f"{url}/db/{db_name}/health",
    headers={"Authorization": f"Bearer {token}"},
    timeout=10,
)
db_health_resp.raise_for_status()
db_health_data = db_health_resp.json()
assert db_health_data.get("status") == "ok", f"DB health not ok: {db_health_data}"
assert db_health_data.get("database") == db_name
print(f"  Status: {db_health_data['status']}, DB: {db_health_data['database']}")
print("  PASS\n")
```

**Step 3: Run to see failures**

```bash
python test_mcp.py --profile simplot-v1 2>&1 | grep -A 5 "multi-DB\|FAIL\|Error"
```

**Step 4: Fix if needed**

If multi-DB routing fails, inspect `DatabaseMCPRouter.__call__()` at line 3987.
Common issue: ContextVar not properly scoped between concurrent requests.

**Step 5: Run full suite**

```bash
python test_mcp.py --profile simplot-v1
```

**Step 6: Update summary count to 47+**

**Step 7: Commit**

```bash
git add test_mcp.py app/mcp_server.py
git commit -m "test: add multi-database routing tests (tests 46-47)"
git push
```

---

## Task 6: Add Missing alter_table Operations

**Files:**
- Modify: `test_mcp.py` (extend the alter_table section, currently tests only `add_column`)

The `alter_table` tool supports 3 operations: `add_column`, `rename_column`, `alter_type`.
Only `add_column` is tested in test 16. Add the other two.

**Step 1: Locate the existing test 16 block in test_mcp.py (~line 262)**

The existing block:
```python
# 16. alter_table (add column)
print("=== alter_table (add_column) ===")
alter_result = tool_call(url, token, "alter_table", {
    "table_name": "_test_mcp_ct",
    "operation": "add_column",
    "column_name": "score",
    "column_type": "INTEGER",
    "constraints": "DEFAULT 0",
}, 15)
```

**Step 2: Add rename and alter_type tests after the existing alter test**

Insert before the `# 17. list_schemas` section:

```python
# 16b. alter_table (rename_column)
print("=== alter_table (rename_column) ===")
rename_result = tool_call(url, token, "alter_table", {
    "table_name": "_test_mcp_ct",
    "operation": "rename_column",
    "column_name": "score",
    "new_column_name": "score_v2",
}, 15)
assert "status" in rename_result, f"Missing status: {rename_result}"
print(f"  Status: {rename_result['status']}")
print("  PASS\n")

# 16c. alter_table (alter_type)
print("=== alter_table (alter_type) ===")
alter_type_result = tool_call(url, token, "alter_table", {
    "table_name": "_test_mcp_ct",
    "operation": "alter_type",
    "column_name": "score_v2",
    "column_type": "BIGINT",
}, 15)
assert "status" in alter_type_result, f"Missing status: {alter_type_result}"
print(f"  Status: {alter_type_result['status']}")
print("  PASS\n")
```

**Step 3: Verify `_tool_alter_table` supports these operations**

Read `app/mcp_server.py` around line 2122. Confirm `rename_column` and `alter_type` cases are handled. Fix if missing.

**Step 4: Run just the alter section**

```bash
python test_mcp.py --profile simplot-v1 2>&1 | grep -A 5 "alter_table"
```

Expected: all 3 PASS.

**Step 5: Full run**

```bash
python test_mcp.py --profile simplot-v1
```

**Step 6: Commit**

```bash
git add test_mcp.py app/mcp_server.py
git commit -m "test: add alter_table rename_column and alter_type coverage"
git push
```

---

## Task 7: Final Verification Run — All Tests

**Files:**
- Run: `test_mcp.py` + `test_rest_api.py`

**Step 1: Run MCP test suite**

```bash
python test_mcp.py --profile simplot-v1
```

Expected: `ALL 49 TESTS PASSED` (or actual final count)

**Step 2: Run REST API test suite**

```bash
python test_rest_api.py --profile simplot-v1
```

Expected: `ALL REST API TESTS PASSED`

**Step 3: Verify no error logs in the app**

```bash
databricks apps logs lakebase-mcp-server --profile simplot-v1 --tail 50
```

Look for ERROR or CRITICAL lines. Investigate any unexpected errors.

**Step 4: Commit final state**

```bash
git add test_mcp.py test_rest_api.py app/mcp_server.py
git commit -m "test: complete MCP endpoint test coverage — all tools, resources, prompts, REST API, multi-DB routing"
git push
```

---

## Quick Reference — Key File Locations

| What | File | Lines |
|------|------|-------|
| All tool dispatch | `app/mcp_server.py` | 1505–1623 |
| Tool functions | `app/mcp_server.py` | 1772–3230 |
| MCP resources | `app/mcp_server.py` | 1628–1664 |
| MCP prompts | `app/mcp_server.py` | 1670–1780 |
| REST API handlers | `app/mcp_server.py` | 3233–3930 |
| Multi-DB router | `app/mcp_server.py` | 3980–4018 |
| App routes | `app/mcp_server.py` | 4054–4096 |
| Existing MCP tests | `test_mcp.py` | 1–599 |

## Tool Categories (all 33)

| Category | Tools |
|----------|-------|
| READ (5) | list_tables, describe_table, read_query, list_schemas, get_connection_info |
| WRITE (4) | insert_record, update_records, delete_records, batch_insert |
| SQL (3) | execute_sql, execute_transaction, explain_query |
| DDL (3) | create_table, drop_table, alter_table |
| PERF (1) | list_slow_queries |
| INFRA (6) | list_projects, describe_project, get_connection_string, list_branches, list_endpoints, get_endpoint_status |
| BRANCH (2) | create_branch, delete_branch |
| SCALE (1) | configure_autoscaling |
| QUALITY (1) | profile_table |
| MIGRATION (2) | prepare_database_migration, complete_database_migration |
| TUNING (2) | prepare_query_tuning, complete_query_tuning |
| EXPLORE (3) | describe_branch, compare_database_schema, search |
