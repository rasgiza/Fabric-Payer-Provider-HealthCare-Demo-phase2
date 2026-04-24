"""Patch Healthcare_Launcher.ipynb:
1. Update Cell 12 manual-step box to include OneLake availability instruction
2. Add shortcut creation logic to Cell 13 (before pipeline trigger)
3. Update Cell 13 completion summary
"""
import json

NB_PATH = "Healthcare_Launcher.ipynb"

with open(NB_PATH, "r", encoding="utf-8") as f:
    nb = json.load(f)

cells = nb["cells"]

# ── Find Cell 12 (RTI deployment) ──────────────────────────────────────
cell12_idx = None
for i, c in enumerate(cells):
    src = "".join(c.get("source", []))
    if "CELL 12" in src and "Deploy Real-Time Intelligence" in src:
        cell12_idx = i
        break

if cell12_idx is None:
    print("ERROR: Could not find Cell 12")
    exit(1)

print(f"Found Cell 12 at index {cell12_idx}")

# Replace the manual-step box in Cell 12
old_box = [
    '        print("  ┌─────────────────────────────────────────────────────────┐")\n',
    '        print("  │  EVENTSTREAM TOPOLOGY WIRED — READY FOR STREAMING      │")\n',
    '        print("  │                                                        │")\n',
    '        print("  │  1. Open the Eventstream URL above in your browser     │")\n',
    '        print("  │  2. Click HealthcareCustomEndpoint → copy Conn String  │")\n',
    '        print("  │  3. Paste into the NEXT CELL → run it                  │")\n',
    '        print("  │                                                        │")\n',
    '        print("  │  That cell triggers PL_Healthcare_RTI which runs:      │")\n',
    '        print("  │    Simulator → Fraud + CareGap + HighCost (parallel)   │")\n',
    '        print("  │                                                        │")\n',
    '        print("  │  One paste, one cell — everything else is automatic.   │")\n',
    '        print("  └─────────────────────────────────────────────────────────┘")\n',
]

new_box = [
    '        print("  ┌──────────────────────────────────────────────────────────────┐")\n',
    '        print("  │  EVENTSTREAM TOPOLOGY WIRED — TWO QUICK STEPS REMAINING     │")\n',
    '        print("  │                                                              │")\n',
    '        print("  │  STEP A — Enable OneLake Availability (one-time, in portal)  │")\n',
    '        print("  │    1. Open Healthcare_RTI_DB in the Fabric portal            │")\n',
    '        print("  │    2. In the Database details pane → OneLake section         │")\n',
    '        print("  │    3. Set Availability → Enabled                             │")\n',
    '        print("  │    4. Check \'Apply to existing tables\' → confirm             │")\n',
    '        print("  │    This exposes KQL tables as Delta in OneLake so scoring    │")\n',
    '        print("  │    notebooks can read them via Spark.                        │")\n',
    '        print("  │                                                              │")\n',
    '        print("  │  STEP B — Copy the Eventstream connection string             │")\n',
    '        print("  │    1. Open the Eventstream URL above in your browser         │")\n',
    '        print("  │    2. Click HealthcareCustomEndpoint → copy Conn String      │")\n',
    '        print("  │    3. Paste into the NEXT CELL → run it                      │")\n',
    '        print("  │                                                              │")\n',
    '        print("  │  That cell creates OneLake shortcuts + triggers the          │")\n',
    '        print("  │  PL_Healthcare_RTI pipeline (Simulator → Scoring).           │")\n',
    '        print("  │                                                              │")\n',
    '        print("  │  Two steps, then everything else is automatic.               │")\n',
    '        print("  └──────────────────────────────────────────────────────────────┘")\n',
]

src12 = cells[cell12_idx]["source"]
# Find the old box in the source
old_box_str = "".join(old_box)
src12_str = "".join(src12)

if old_box_str in src12_str:
    src12_str = src12_str.replace(old_box_str, "".join(new_box))
    cells[cell12_idx]["source"] = src12_str.splitlines(True)
    print("  [OK] Updated Cell 12 instruction box")
else:
    print("  [WARN] Could not find old box in Cell 12 — trying line-by-line match")
    # Try matching the key line
    found = False
    for j, line in enumerate(src12):
        if "READY FOR STREAMING" in line:
            # Find the block boundaries
            start = j - 1  # the ┌ line
            end = j + 11    # the └ line (12 lines total)
            src12[start:end] = new_box
            found = True
            break
    if found:
        print("  [OK] Updated Cell 12 instruction box (line-by-line)")
    else:
        print("  [FAIL] Could not find instruction box in Cell 12")

# ── Find Cell 13 (pipeline trigger) ────────────────────────────────────
cell13_idx = None
for i, c in enumerate(cells):
    src = "".join(c.get("source", []))
    if "CELL 13" in src and "PL_Healthcare_RTI" in src and "ES_CONNECTION_STRING" in src:
        cell13_idx = i
        break

if cell13_idx is None:
    print("ERROR: Could not find Cell 13 (pipeline trigger)")
    exit(1)

print(f"Found Cell 13 at index {cell13_idx}")

src13 = cells[cell13_idx]["source"]

# Add shortcut creation after the _api line
shortcut_code = [
    "\n",
    "    # ── Create OneLake Shortcuts: KQL tables → lh_gold_curated ──────\n",
    "    # OneLake availability must be enabled on Healthcare_RTI_DB first.\n",
    "    # This creates shortcuts so scoring notebooks can read KQL data via\n",
    "    # spark.table(\"lh_gold_curated.rti_claims_events\") etc.\n",
    "    print(\"=\" * 60)\n",
    "    print(\"  CREATING ONELAKE SHORTCUTS (KQL → lh_gold_curated)\")\n",
    "    print(\"=\" * 60)\n",
    "\n",
    "    # Find Healthcare_RTI_DB (KQL Database)\n",
    "    _kql_db = None\n",
    "    _r = requests.get(f\"{_api}/items?type=KQLDatabase\", headers=_hdrs)\n",
    "    if _r.status_code == 200:\n",
    "        _kql_db = next((i for i in _r.json().get(\"value\", [])\n",
    "                        if i[\"displayName\"] == \"Healthcare_RTI_DB\"), None)\n",
    "    if _kql_db:\n",
    "        _kql_db_id = _kql_db[\"id\"]\n",
    "        _shortcut_map = {\n",
    "            \"rti_claims_events\": \"claims_events\",\n",
    "            \"rti_adt_events\":    \"adt_events\",\n",
    "            \"rti_rx_events\":     \"rx_events\",\n",
    "        }\n",
    "        _sc_ok = 0\n",
    "        for _sc_name, _kql_table in _shortcut_map.items():\n",
    "            _sc_body = {\n",
    "                \"name\": _sc_name,\n",
    "                \"path\": \"Tables\",\n",
    "                \"target\": {\n",
    "                    \"oneLake\": {\n",
    "                        \"workspaceId\": workspace_id,\n",
    "                        \"itemId\": _kql_db_id,\n",
    "                        \"path\": f\"Tables/{_kql_table}\"\n",
    "                    }\n",
    "                }\n",
    "            }\n",
    "            _sc_r = requests.post(\n",
    "                f\"{_api}/items/{lh_gold_id}/shortcuts\",\n",
    "                headers=_hdrs,\n",
    "                json=_sc_body\n",
    "            )\n",
    "            if _sc_r.status_code in (200, 201):\n",
    "                print(f\"  [OK] Shortcut: lh_gold_curated.{_sc_name} → KQL {_kql_table}\")\n",
    "                _sc_ok += 1\n",
    "            elif _sc_r.status_code == 409:\n",
    "                print(f\"  [OK] Shortcut already exists: {_sc_name}\")\n",
    "                _sc_ok += 1\n",
    "            else:\n",
    "                _msg = _sc_r.json().get(\"message\", _sc_r.text[:200])\n",
    "                print(f\"  [WARN] Shortcut {_sc_name} failed (HTTP {_sc_r.status_code}): {_msg}\")\n",
    "                if \"Target path\" in str(_msg):\n",
    "                    print(f\"         → Enable OneLake Availability on Healthcare_RTI_DB first!\")\n",
    "                    print(f\"         → Database details pane → OneLake → Availability → Enabled\")\n",
    "        if _sc_ok == len(_shortcut_map):\n",
    "            print(f\"  All {_sc_ok} shortcuts ready — scoring notebooks can read KQL data via Spark\")\n",
    "        print()\n",
    "    else:\n",
    "        print(\"  [WARN] Healthcare_RTI_DB not found — skipping shortcut creation\")\n",
    "        print()\n",
    "\n",
]

# Find the line "    # Find PL_Healthcare_RTI pipeline" to insert before it
insert_idx = None
for j, line in enumerate(src13):
    if "# Find PL_Healthcare_RTI pipeline" in line:
        insert_idx = j
        break

if insert_idx is not None:
    src13[insert_idx:insert_idx] = shortcut_code
    print("  [OK] Added shortcut creation to Cell 13")
else:
    print("  [FAIL] Could not find insertion point in Cell 13")

# Update the pipeline completion message
for j, line in enumerate(src13):
    if "Events streamed via Eventstream" in line and "Lakehouse" in line:
        src13[j] = '            print("  OneLake shortcuts: KQL tables → lh_gold_curated")\n'
        src13[j+1] = '            print("  Events streamed via Eventstream → Eventhouse")\n'
        src13[j+2] = '            print("  Scoring notebooks read KQL data via OneLake shortcuts")\n'
        print("  [OK] Updated completion summary in Cell 13")
        break

# Write back
with open(NB_PATH, "w", encoding="utf-8") as f:
    json.dump(nb, f, indent=1, ensure_ascii=False)

print("\nDone! Healthcare_Launcher.ipynb updated.")
