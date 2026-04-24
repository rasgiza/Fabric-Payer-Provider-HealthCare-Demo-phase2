"""Diagnose and fix Cell 15 (index 14) syntax issues."""
import json

nb = json.load(open("Healthcare_Launcher.ipynb", "r", encoding="utf-8"))
cell = nb["cells"][14]
src = "".join(cell["source"])
lines = src.split("\n")

print(f"Cell 15 (index 14): {len(lines)} lines, {len(src)} chars")

# Try compiling
try:
    compile(src, "cell15", "exec")
    print("COMPILES: OK")
except SyntaxError as e:
    print(f"COMPILES: ERROR at line {e.lineno}: {e.msg}")
    if e.lineno:
        for j in range(max(0, e.lineno - 4), min(len(lines), e.lineno + 3)):
            marker = ">>>" if j == e.lineno - 1 else "   "
            print(f"  {marker} {j+1:4d}: {lines[j][:100]}")

# Check for known issues
print()
print("=== Known issue checks ===")

# 1. _lh_deps dict closure
for i, line in enumerate(lines):
    if "_lh_deps = {" in line:
        print(f"  _lh_deps starts at line {i+1}")
    if "_needs_lh" in line:
        print(f"  _needs_lh at line {i+1}: {line.strip()[:80]}")

# 2. Orphaned notebook names
for i, line in enumerate(lines):
    stripped = line.strip()
    if stripped.startswith('"NB_RTI_') and not stripped.startswith('"NB_RTI_') == False:
        context = lines[i-1].strip() if i > 0 else ""
        print(f"  NB_RTI reference at line {i+1}: {stripped[:80]}")
        print(f"    prev line: {context[:80]}")

# 3. Duplicate streams
streams_lines = [(i+1, line) for i, line in enumerate(lines) if '"streams": _es_streams' in line]
print(f"  'streams: _es_streams' at lines: {[l[0] for l in streams_lines]}")
