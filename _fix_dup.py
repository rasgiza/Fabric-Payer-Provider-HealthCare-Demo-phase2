"""Remove duplicate shortcut code from Cell 16 (RTI Dashboard cell)."""
import json

NB_PATH = "Healthcare_Launcher.ipynb"

with open(NB_PATH, "r", encoding="utf-8") as f:
    nb = json.load(f)

cells = nb["cells"]
c16 = cells[16]
src = c16["source"]
full = "".join(src)

# Cell 16 should be the RTI Dashboard cell — remove the injected shortcut code
if "CREATING ONELAKE SHORTCUTS" in full and "Deploy Real-Time Dashboard" in full:
    # Find the shortcut block boundaries
    new_lines = []
    skip = False
    for line in src:
        if "# ── Create OneLake Shortcuts" in line:
            skip = True
            continue
        if skip and "# Find PL_Healthcare_RTI pipeline" in line:
            skip = False
            new_lines.append(line)
            continue
        if not skip:
            new_lines.append(line)
    
    c16["source"] = new_lines
    removed = len(src) - len(new_lines)
    print(f"Removed {removed} lines of injected shortcut code from Cell 16")
    
    # Also remove the duplicate completion message changes if present
    for j, line in enumerate(c16["source"]):
        if "OneLake shortcuts: KQL tables" in line:
            c16["source"][j] = '            print("  Events streamed via Eventstream → Eventhouse + Lakehouse")\n'
        if "Scoring notebooks read KQL data" in line:
            c16["source"][j] = '            print("  Re-run this cell anytime for fresh data.")\n'
    
    with open(NB_PATH, "w", encoding="utf-8") as f:
        json.dump(nb, f, indent=1, ensure_ascii=False)
    
    print("Fixed Cell 16 (RTI Dashboard)")
else:
    print("Cell 16 is clean — no duplicates found")
    if "CREATING ONELAKE SHORTCUTS" in full:
        print("  (has shortcuts but not dashboard cell)")
    if "Deploy Real-Time Dashboard" in full:
        print("  (has dashboard but no shortcuts)")
