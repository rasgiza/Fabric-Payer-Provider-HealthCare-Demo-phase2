"""
Fix ALL notebook.run() calls in Healthcare_Launcher.ipynb to handle the known
Fabric result-parse bug (NoSuchElementException / mssparkutilsrun-result+json).

The child notebook succeeds but notebookutils can't parse the result metadata.
We detect this specific exception and treat it as success.
"""
import json

FABRIC_BUG_MARKERS = ("mssparkutilsrun-result+json", "NoSuchElementException")

def is_fabric_parse_bug(var="e"):
    """Return the if-check string for detecting the Fabric bug."""
    return (
        f'if "mssparkutilsrun-result+json" in str({var}) '
        f'or "NoSuchElementException" in str({var})'
    )

nb = json.load(open("Healthcare_Launcher.ipynb", "r", encoding="utf-8"))
fixes = 0

# --- Cell index 7 (Cell 8): NB_Generate_Sample_Data ---
# Already fixed by _fix_cell6.py, just verify
c7_src = "".join(nb["cells"][7]["source"])
if "mssparkutilsrun-result+json" in c7_src:
    print("Cell 8 (NB_Generate_Sample_Data): already patched")
else:
    print("Cell 8: WARNING - not patched, re-check _fix_cell6.py output")

# --- Cell index 11 (Cell 12): NB_Deploy_Graph_Model ---
cell12 = nb["cells"][11]
src12 = cell12["source"]
patched12 = []
i = 0
while i < len(src12):
    line = src12[i]
    patched12.append(line)

    # Find: except Exception as _gm_err:  after the graph model run
    if "except Exception as _gm_err:" in line and i > 0 and "NB_Deploy_Graph_Model" in "".join(src12[max(0,i-5):i]):
        # Next line should be: graph_st = "[FAIL]"
        # We need to insert the Fabric bug check before that
        i += 1
        # Read remaining lines of the except block
        indent = "            "  # 12 spaces (3 levels)
        patched12.append(f'{indent}if "mssparkutilsrun-result+json" in str(_gm_err) or "NoSuchElementException" in str(_gm_err):\n')
        patched12.append(f'{indent}    graph_st = "[OK]"\n')
        patched12.append(f'{indent}    print()\n')
        patched12.append(f'{indent}    print(f"  [OK] NB_Deploy_Graph_Model completed (ignoring Fabric result-parse bug)")\n')
        patched12.append(f'{indent}else:\n')
        # Now include the original fail handling, indented one more level
        while i < len(src12):
            orig = src12[i]
            stripped = orig.lstrip()
            if stripped == "" or stripped == "\n":
                patched12.append(orig)
                i += 1
                # Check if next non-empty line is at same or lower indent (end of except)
                continue
            curr_indent = len(orig) - len(orig.lstrip())
            if curr_indent < len(indent):
                # We've left the except block
                break
            # Add one extra level of indent (4 spaces)
            patched12.append(indent + "    " + stripped)
            if not stripped.endswith("\n"):
                patched12[-1] += "\n"
            i += 1
        fixes += 1
        print("Cell 12 (NB_Deploy_Graph_Model): patched")
        continue
    i += 1

cell12["source"] = patched12

# --- Cell index 14 (Cell 15): RTI notebook runs + Operations Agent ---
cell15 = nb["cells"][14]
src15 = cell15["source"]
patched15 = []
i = 0
while i < len(src15):
    line = src15[i]
    patched15.append(line)

    # Pattern 1: except Exception as e: after generic nb_name run
    if "except Exception as e:" in line:
        # Check context: is this after a notebook.run(nb_name, ...) ?
        context = "".join(src15[max(0,i-5):i])
        
        if "notebook.run(nb_name" in context:
            # Next lines: print FAILED, print manual run hint
            i += 1
            indent = "            "  # 12 spaces
            patched15.append(f'{indent}if "mssparkutilsrun-result+json" in str(e) or "NoSuchElementException" in str(e):\n')
            patched15.append(f'{indent}    print(f"  -> {{nb_name}}: OK (ignoring Fabric result-parse bug)")\n')
            patched15.append(f'{indent}else:\n')
            # Include original lines with extra indent
            while i < len(src15):
                orig = src15[i]
                stripped = orig.lstrip()
                if stripped == "" or stripped == "\n":
                    patched15.append(orig)
                    i += 1
                    continue
                curr_indent = len(orig) - len(orig.lstrip())
                if curr_indent < len(indent):
                    break
                patched15.append(indent + "    " + stripped)
                if not stripped.endswith("\n"):
                    patched15[-1] += "\n"
                i += 1
            fixes += 1
            print("Cell 15 (RTI notebook.run loop): patched")
            continue

        elif "NB_RTI_Operations_Agent" in context:
            # Next lines: print WARN, print expected, print re-run
            i += 1
            indent = "        "  # 8 spaces
            patched15.append(f'{indent}if "mssparkutilsrun-result+json" in str(e) or "NoSuchElementException" in str(e):\n')
            patched15.append(f'{indent}    print("  [OK] Operations Agent notebook completed (ignoring Fabric result-parse bug)")\n')
            patched15.append(f'{indent}else:\n')
            while i < len(src15):
                orig = src15[i]
                stripped = orig.lstrip()
                if stripped == "" or stripped == "\n":
                    patched15.append(orig)
                    i += 1
                    continue
                curr_indent = len(orig) - len(orig.lstrip())
                if curr_indent < len(indent):
                    break
                patched15.append(indent + "    " + stripped)
                if not stripped.endswith("\n"):
                    patched15[-1] += "\n"
                i += 1
            fixes += 1
            print("Cell 15 (NB_RTI_Operations_Agent): patched")
            continue
    
    i += 1

cell15["source"] = patched15

# Save
json.dump(nb, open("Healthcare_Launcher.ipynb", "w", encoding="utf-8"), indent=1, ensure_ascii=False)
print(f"\nSaved. Total new patches: {fixes}")

# Verify all patched cells compile
for idx in [7, 11, 14]:
    src = "".join(nb["cells"][idx]["source"])
    try:
        compile(src, f"cell_{idx+1}", "exec")
        print(f"Cell {idx+1}: SYNTAX OK")
    except SyntaxError as se:
        print(f"Cell {idx+1}: SYNTAX ERROR at line {se.lineno}: {se.msg}")
