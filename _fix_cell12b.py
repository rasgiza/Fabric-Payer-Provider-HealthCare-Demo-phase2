"""Fix #2: Remove orphaned 'streams' line in Cell 12."""
import json

NB_PATH = "Healthcare_Launcher.ipynb"

with open(NB_PATH, encoding="utf-8") as f:
    nb = json.load(f)

cell = nb["cells"][14]
src_lines = cell["source"]

removed = False
for i, line in enumerate(src_lines):
    stripped = line.strip()
    if stripped == '"streams": _es_streams,' and i > 0:
        # Check context: should be after the print(...Topology...) line
        prev = src_lines[i - 1]
        if "Topology" in prev or "_dest_names" in prev:
            print(f"Found orphaned line at index {i}: {repr(line)}")
            del src_lines[i]
            removed = True
            break

if removed:
    cell["source"] = src_lines
    nb["cells"][14] = cell
    with open(NB_PATH, "w", encoding="utf-8", newline="") as f:
        json.dump(nb, f, indent=1, ensure_ascii=False)
        f.write("\n")
    print("Removed orphaned line. Saved.")
else:
    print("Could not find orphaned line.")

# Verify
with open(NB_PATH, encoding="utf-8") as f:
    nb2 = json.load(f)
src2 = "".join(nb2["cells"][14].get("source", []))
try:
    compile(src2, "cell12", "exec")
    print("SYNTAX OK - cell 12 compiles cleanly")
except SyntaxError as e:
    print(f"Still has syntax error: {e}")
