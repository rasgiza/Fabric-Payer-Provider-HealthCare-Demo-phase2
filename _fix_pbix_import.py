"""Patch Cell 8b in Healthcare_Launcher.ipynb:
Add PBIX connection rewriting so the .pbix works in any workspace, not just
the original author's workspace.
"""
import json

NB_PATH = "Healthcare_Launcher.ipynb"

with open(NB_PATH, "r", encoding="utf-8") as f:
    nb = json.load(f)

# Find Cell 8b — contains "POWER BI REPORT DEPLOYMENT"
target_cell = None
for i, cell in enumerate(nb["cells"]):
    src = "".join(cell.get("source", []))
    if "POWER BI REPORT DEPLOYMENT" in src and "Import .pbix" in src:
        target_cell = i
        break

if target_cell is None:
    print("ERROR: Could not find Cell 8b")
    exit(1)

print(f"Found Cell 8b at index {target_cell}")
src_lines = nb["cells"][target_cell]["source"]

# Find the line:  print(f"  PBIX size: {len(pbix_bytes):,} bytes")
# We'll insert the patching code right after it
insert_after = None
for j, line in enumerate(src_lines):
    if 'PBIX size:' in line and 'pbix_bytes' in line:
        insert_after = j
        break

if insert_after is None:
    print("ERROR: Could not find PBIX size print line")
    exit(1)

print(f"Inserting patch code after line {insert_after}")

# The new code block to insert
patch_code = '''
        # --- 2b. Patch PBIX connections for target workspace ---
        # The .pbix contains hardcoded workspace/dataset IDs from the author's
        # environment. We rewrite the Connections file so the import works in
        # ANY workspace, not just the original one.
        import zipfile, urllib.parse
        try:
            # Discover the tenant ID for connection string
            tenant_resp = requests.get(
                "https://api.fabric.microsoft.com/v1/admin/tenants",
                headers=headers
            )
            # Build the connection string for THIS workspace
            ws_name_encoded = urllib.parse.quote(workspace_name) if workspace_name else workspace_id
            new_conn_str = (
                f"Data Source=powerbi://api.powerbi.com/v1.0/myorg/{ws_name_encoded};"
                f"Initial Catalog={SM_NAME};Integrated Security=ClaimsToken"
            )
            new_connections = {
                "Version": 6,
                "Connections": [{
                    "Name": "EntityDataSource",
                    "ConnectionString": new_conn_str,
                    "ConnectionType": "pbiServiceXmlaStyleLive",
                    "PbiModelVirtualServerName": "sobe_wowvirtualserver",
                    "PbiModelDatabaseName": sm_id
                }],
                "RemoteArtifacts": [{"DatasetId": sm_id}],
                "OriginalWorkspaceObjectId": workspace_id
            }
            conn_json = json.dumps(new_connections, separators=(",", ":"))

            # Rebuild PBIX with patched Connections file
            old_zip = zipfile.ZipFile(io.BytesIO(pbix_bytes), "r")
            buf = io.BytesIO()
            with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as new_zip:
                for item in old_zip.infolist():
                    if item.filename == "Connections":
                        new_zip.writestr(item, conn_json.encode("utf-8"))
                    else:
                        new_zip.writestr(item, old_zip.read(item.filename))
            old_zip.close()
            pbix_bytes = buf.getvalue()
            print(f"  Patched PBIX connections for workspace {workspace_id[:8]}...")
            print(f"  Patched PBIX size: {len(pbix_bytes):,} bytes")
        except Exception as ex:
            print(f"  [WARN] Could not patch PBIX connections: {ex}")
            print(f"  Proceeding with original PBIX (may fail in other workspaces)")

'''

# Split the patch code into source lines (preserving notebook format)
new_lines = [line + "\n" for line in patch_code.split("\n")]

# Insert after the PBIX size line
src_lines[insert_after + 1:insert_after + 1] = new_lines

nb["cells"][target_cell]["source"] = src_lines

with open(NB_PATH, "w", encoding="utf-8", newline="\n") as f:
    json.dump(nb, f, indent=1, ensure_ascii=False)
    f.write("\n")

print("Done. Patched Cell 8b with PBIX connection rewriting.")
