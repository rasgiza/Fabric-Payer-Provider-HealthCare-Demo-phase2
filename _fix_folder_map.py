"""Fix FOLDER_MAP in Healthcare_Launcher.ipynb:
1. Use type tuple for SemanticModel (avoid name collision with default dataset)
2. Use type tuple for Report (avoid name collision)
3. Add Eventstream to Real-Time Intelligence folder
"""
import json

NB_PATH = "Healthcare_Launcher.ipynb"

with open(NB_PATH, "r", encoding="utf-8") as f:
    nb = json.load(f)

# Find the cell containing FOLDER_MAP and ORGANIZE WORKSPACE FOLDERS
target_cell = None
for i, cell in enumerate(nb["cells"]):
    src = "".join(cell.get("source", []))
    if "FOLDER_MAP" in src and "Semantic Models" in src and "ORGANIZE WORKSPACE FOLDERS" in src:
        target_cell = i
        break

if target_cell is None:
    print("ERROR: Could not find FOLDER_MAP cell")
    exit(1)

print(f"Found FOLDER_MAP in cell {target_cell + 1}")
src_lines = nb["cells"][target_cell]["source"]

changes = 0
j = 0
while j < len(src_lines):
    line = src_lines[j]

    # 1. Fix Semantic Models entry: plain string -> type tuple
    if '"HealthcareDemoHLS",' in line.strip() and j > 0 and "Semantic Models" in src_lines[j - 1]:
        old = line
        src_lines[j] = line.replace('"HealthcareDemoHLS",', '("HealthcareDemoHLS", "SemanticModel"),')
        print(f"  Fixed SM: {old.rstrip()} -> {src_lines[j].rstrip()}")
        changes += 1

    # 2. Fix Reports entry: plain string -> type tuple
    if '"Healthcare Analytics Dashboard",' in line.strip() and j > 0 and "Reports" in src_lines[j - 1]:
        old = line
        src_lines[j] = line.replace(
            '"Healthcare Analytics Dashboard",',
            '("Healthcare Analytics Dashboard", "Report"),'
        )
        print(f"  Fixed Report: {old.rstrip()} -> {src_lines[j].rstrip()}")
        changes += 1

    # 3. Add Eventstream after Eventhouse in RTI section
    if '("Healthcare_RTI_Eventhouse", "Eventhouse")' in line:
        next_line = src_lines[j + 1] if j + 1 < len(src_lines) else ""
        if "Eventstream" not in next_line:
            indent = line[: len(line) - len(line.lstrip())]
            new_line = indent + '("Healthcare_RTI_Eventstream", "Eventstream"),\n'
            src_lines.insert(j + 1, new_line)
            print(f"  Added Eventstream: {new_line.rstrip()}")
            changes += 1
            j += 1  # skip the line we just inserted

    j += 1

nb["cells"][target_cell]["source"] = src_lines

with open(NB_PATH, "w", encoding="utf-8", newline="\n") as f:
    json.dump(nb, f, indent=1, ensure_ascii=False)
    f.write("\n")

print(f"\nDone. {changes} changes applied.")
