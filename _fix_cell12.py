"""Fix syntax error in Cell 12 of Healthcare_Launcher.ipynb.

Problem: _lh_deps dict is unclosed and _needs_lh list is missing.
Also rti_notebooks list is empty (should contain NB_RTI_Setup_Eventhouse).
"""
import json

NB_PATH = "Healthcare_Launcher.ipynb"

with open(NB_PATH, encoding="utf-8") as f:
    nb = json.load(f)

cell = nb["cells"][14]
src_lines = cell["source"]

# --- Fix 1: Close _lh_deps dict and add _needs_lh list ---
# Find the broken sequence: line ending with '}' after known_lakehouses,
# followed by orphaned list items
fix1_done = False
for i, line in enumerate(src_lines):
    # Look for the unclosed _lh_deps: line with just "        }\n" after known_lakehouses
    if line.strip() == "}" and i > 0:
        # Check if next line has the orphaned NB_RTI list
        if i + 1 < len(src_lines) and "NB_RTI_Care_Gap_Alerts" in src_lines[i + 1]:
            # Find how many orphaned lines there are
            orphan_end = i + 1
            while orphan_end < len(src_lines) and "NB_RTI" in src_lines[orphan_end]:
                orphan_end += 1

            # Replace: close dict properly + add _needs_lh list
            new_lines = [
                "        }\n",
                "    }\n",
                "\n",
                '    _needs_lh = ["NB_RTI_Setup_Eventhouse", "NB_RTI_Event_Simulator",\n',
                '                 "NB_RTI_Fraud_Detection",\n',
                '                 "NB_RTI_Care_Gap_Alerts", "NB_RTI_HighCost_Trajectory",\n',
                '                 "NB_RTI_Operations_Agent"]\n',
            ]
            src_lines[i:orphan_end] = new_lines
            fix1_done = True
            print(f"Fix 1: Replaced lines {i}-{orphan_end-1} with proper _lh_deps close + _needs_lh list")
            break

if not fix1_done:
    print("Fix 1: Could not find broken _lh_deps/orphaned list pattern")

# --- Fix 2: Populate rti_notebooks list ---
fix2_done = False
for i, line in enumerate(src_lines):
    if "rti_notebooks = [" in line:
        # Check if next line closes the empty list
        if i + 1 < len(src_lines) and src_lines[i + 1].strip() == "]":
            new_lines = [
                "    rti_notebooks = [\n",
                '        "NB_RTI_Setup_Eventhouse",\n',
                "    ]\n",
            ]
            src_lines[i:i + 2] = new_lines
            fix2_done = True
            print(f"Fix 2: Populated rti_notebooks with NB_RTI_Setup_Eventhouse")
            break

if not fix2_done:
    print("Fix 2: Could not find empty rti_notebooks pattern")

cell["source"] = src_lines
nb["cells"][14] = cell

with open(NB_PATH, "w", encoding="utf-8", newline="") as f:
    json.dump(nb, f, indent=1, ensure_ascii=False)
    f.write("\n")

print("Done. Saved Healthcare_Launcher.ipynb")
