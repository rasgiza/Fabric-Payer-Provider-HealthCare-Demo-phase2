"""
Update Cell 12 ending + add PL_Healthcare_RTI trigger cell.

Changes:
1. In cells[14] (Cell 12): Replace the 5-step manual instructions with a
   simplified message pointing to the next cell
2. Insert a new cell after cells[14] that triggers PL_Healthcare_RTI
   with the ES_CONNECTION_STRING parameter
"""
import json

nb = json.load(open("Healthcare_Launcher.ipynb", "r", encoding="utf-8"))
cell12 = nb["cells"][14]
src = cell12["source"]

# ── 1. Find and replace the NEXT STEPS block in Cell 12 ──────────────
# We're looking for the big box with manual instructions and replacing it
# with a shorter message pointing to the pipeline trigger cell.

# Find the start of the box (the "NEXT:" line)
box_start = None
box_end = None
for i, line in enumerate(src):
    if "NEXT: Copy the connection string and run simulator:" in line:
        # Walk back to find the box top
        for j in range(i, max(0, i-5), -1):
            if "EVENTSTREAM TOPOLOGY WIRED" in src[j]:
                box_start = j - 1  # include the ┌ line
                break
        if box_start is None:
            box_start = i - 2
    if box_start is not None and "You can re-run these notebooks anytime." in line:
        # Walk forward to find the box bottom └
        for j in range(i, min(len(src), i+3)):
            if "└" in src[j]:
                box_end = j + 1  # include the └ line
                break
        if box_end is None:
            box_end = i + 1
        break

if box_start is not None and box_end is not None:
    # Build replacement block
    replacement = [
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
    src[box_start:box_end] = replacement
    print(f"Replaced box at lines {box_start}-{box_end} with {len(replacement)} lines")
else:
    print(f"WARNING: Could not find the instruction box (start={box_start}, end={box_end})")

# Also replace the trailing NEXT STEPS block at the very end
trailing_start = None
trailing_end = None
for i, line in enumerate(src):
    if '    print("  NEXT STEPS:")' in line:
        trailing_start = i
    if trailing_start is not None and "Re-run the simulator and scoring notebooks anytime" in line:
        trailing_end = i + 1
        break

if trailing_start is not None and trailing_end is not None:
    replacement2 = [
        '    print("  NEXT: Paste the connection string into the next cell and run it.")\n',
        '    print("  PL_Healthcare_RTI orchestrates: Simulator → Scoring (parallel)")\n',
    ]
    src[trailing_start:trailing_end] = replacement2
    print(f"Replaced trailing NEXT STEPS at lines {trailing_start}-{trailing_end}")
else:
    print(f"WARNING: Could not find trailing NEXT STEPS (start={trailing_start}, end={trailing_end})")

cell12["source"] = src

# ── 2. Insert a new cell after cells[14] for pipeline trigger ────────
pipeline_cell_source = [
    "# ============================================================================\n",
    "# CELL 13 — Run PL_Healthcare_RTI Pipeline\n",
    "# ============================================================================\n",
    "# Paste the Eventstream connection string below, then run this cell.\n",
    "# The pipeline orchestrates: Simulator → Fraud + CareGap + HighCost (parallel).\n",
    "# ============================================================================\n",
    "\n",
    'ES_CONNECTION_STRING = ""   # <── PASTE connection string here\n',
    "\n",
    "if not ES_CONNECTION_STRING:\n",
    '    print("=" * 60)\n',
    '    print("  ES_CONNECTION_STRING is empty.")\n',
    '    print()\n',
    '    print("  To get the connection string:")\n',
    '    print("  1. Open Healthcare_RTI_Eventstream in the Fabric portal")\n',
    '    print("  2. Click the HealthcareCustomEndpoint source node")\n',
    '    print("  3. Copy the Connection String")\n',
    '    print("  4. Paste it into ES_CONNECTION_STRING above")\n',
    '    print("  5. Re-run this cell")\n',
    '    print("=" * 60)\n',
    "else:\n",
    "    import requests, time, json\n",
    "\n",
    '    _token = notebookutils.credentials.getToken("pbi")\n',
    '    _hdrs = {"Authorization": f"Bearer {_token}", "Content-Type": "application/json"}\n',
    '    _api = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}"\n',
    "\n",
    "    # Find PL_Healthcare_RTI pipeline\n",
    '    print("Looking up PL_Healthcare_RTI pipeline...")\n',
    '    _resp = requests.get(f"{_api}/items?type=DataPipeline", headers=_hdrs)\n',
    "    _resp.raise_for_status()\n",
    '    _pl = next((p for p in _resp.json().get("value", []) if p["displayName"] == "PL_Healthcare_RTI"), None)\n',
    "\n",
    "    if not _pl:\n",
    '        print("WARNING: PL_Healthcare_RTI not found in workspace.")\n',
    '        print("Sync from Git or create it manually.")\n',
    "    else:\n",
    '        _pl_id = _pl["id"]\n',
    '        print(f"Pipeline: {_pl_id}")\n',
    "\n",
    "        # Trigger pipeline with ES_CONNECTION_STRING\n",
    '        print("Triggering PL_Healthcare_RTI...")\n',
    '        print("  Simulator → Fraud + CareGap + HighCost (parallel)")\n',
    "        _trigger_body = {\n",
    '            "executionData": {\n',
    '                "parameters": {\n',
    '                    "ES_CONNECTION_STRING": ES_CONNECTION_STRING,\n',
    '                    "STREAM_BATCHES": 10\n',
    "                }\n",
    "            }\n",
    "        }\n",
    "        _resp = requests.post(\n",
    '            f"{_api}/items/{_pl_id}/jobs/instances?jobType=Pipeline",\n',
    "            headers=_hdrs,\n",
    "            json=_trigger_body,\n",
    "        )\n",
    "\n",
    "        if _resp.status_code in (200, 202):\n",
    '            _location = _resp.headers.get("Location", "")\n',
    '            print(f"Pipeline triggered. Polling for completion...")\n',
    '            print(f"(This takes ~5 minutes for 10 batches + scoring)\\n")\n',
    "\n",
    "            # Poll until complete\n",
    "            _max_polls = 60  # 60 * 15s = 15 min max\n",
    "            for _poll in range(_max_polls):\n",
    "                time.sleep(15)\n",
    "                try:\n",
    "                    if _location:\n",
    "                        _poll_r = requests.get(_location, headers=_hdrs)\n",
    "                    else:\n",
    "                        _poll_r = requests.get(\n",
    '                            f"{_api}/items/{_pl_id}/jobs/instances",\n',
    "                            headers=_hdrs,\n",
    "                        )\n",
    "                    if _poll_r.status_code == 200:\n",
    "                        _job = _poll_r.json()\n",
    '                        _status = _job.get("status", "Unknown")\n',
    '                        if _status in ("Completed", "Succeeded"):\n',
    '                            print(f"  Pipeline COMPLETED after {(_poll+1)*15}s")\n',
    "                            break\n",
    '                        elif _status in ("Failed", "Cancelled"):\n',
    '                            print(f"  Pipeline {_status} after {(_poll+1)*15}s")\n',
    '                            print(f"  Check the pipeline run history for details.")\n',
    "                            break\n",
    "                        else:\n",
    "                            if _poll % 4 == 0:\n",
    '                                print(f"  [{(_poll+1)*15}s] Status: {_status}...")\n',
    "                except Exception as _e:\n",
    "                    if _poll % 4 == 0:\n",
    '                        print(f"  [{(_poll+1)*15}s] Polling... ({_e})")\n',
    "            else:\n",
    '                print("  Pipeline still running after 15 min. Check workspace for status.")\n',
    "\n",
    '            print()\n',
    '            print("=" * 60)\n',
    '            print("  RTI PIPELINE COMPLETE")\n',
    '            print("=" * 60)\n',
    '            print("  Events streamed via Eventstream → Eventhouse + Lakehouse")\n',
    '            print("  Scoring notebooks ran: Fraud, CareGap, HighCost")\n',
    '            print("  Re-run this cell anytime for fresh data.")\n',
    '            print("=" * 60)\n',
    "        else:\n",
    '            print(f"  Pipeline trigger returned {_resp.status_code}: {_resp.text[:500]}")\n',
    '            print("  You can run PL_Healthcare_RTI manually from the workspace.")\n',
]

# Create the new cell
new_cell = {
    "cell_type": "code",
    "execution_count": None,
    "metadata": {},
    "outputs": [],
    "source": pipeline_cell_source,
}

# Insert after cells[14]
nb["cells"].insert(15, new_cell)
print(f"Inserted pipeline trigger cell at index 15")

# ── 3. Save and validate ──────────────────────────────────────────────
json.dump(nb, open("Healthcare_Launcher.ipynb", "w", encoding="utf-8"), indent=1, ensure_ascii=False)
print("Saved Healthcare_Launcher.ipynb")

# Validate syntax of both cells
cell12_src = "".join(nb["cells"][14]["source"])
compile(cell12_src, "cell12", "exec")
print("Cell 12 syntax: OK")

cell13_src = "".join(nb["cells"][15]["source"])
compile(cell13_src, "cell13", "exec")
print("Cell 13 syntax: OK")

# Also update _cell12_fixed.py
with open("_cell12_fixed.py", "w", encoding="utf-8") as f:
    f.write(cell12_src)
print(f"Updated _cell12_fixed.py ({len(cell12_src)} chars)")
