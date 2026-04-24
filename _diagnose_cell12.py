"""Diagnose and fix Cell 12 syntax issues in Healthcare_Launcher.ipynb."""
import json

nb = json.load(open("Healthcare_Launcher.ipynb", "r", encoding="utf-8"))

# Find Cell 12 (RTI deployment)
cell_idx = None
for i, cell in enumerate(nb["cells"]):
    src = "".join(cell["source"])
    if "Deploy Real-Time Intelligence" in src:
        cell_idx = i
        break

if cell_idx is None:
    print("Cell 12 not found!")
    exit(1)

src = "".join(nb["cells"][cell_idx]["source"])
print(f"Found RTI cell at cells[{cell_idx}] (Cell {cell_idx+1})")
print(f"Source length: {len(src)} chars, {len(nb['cells'][cell_idx]['source'])} lines")

# Check for known bugs
has_needs_lh = "_needs_lh" in src
print(f"  _needs_lh defined: {has_needs_lh}")

streams_count = src.count('"streams": _es_streams,')
print(f"  'streams: _es_streams' occurrences: {streams_count}")

# Try compiling
try:
    compile(src, "cell12", "exec")
    print("  COMPILES: OK")
except SyntaxError as e:
    print(f"  COMPILES: ERROR at line {e.lineno}: {e.msg}")
    lines = src.split("\n")
    if e.lineno:
        for j in range(max(0, e.lineno - 3), min(len(lines), e.lineno + 2)):
            marker = ">>>" if j == e.lineno - 1 else "   "
            print(f"    {marker} {j+1:4d}: {lines[j][:90]}")
