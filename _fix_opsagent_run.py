"""Fix NB_RTI_Operations_Agent notebook.run call to include useRootDefaultLakehouse."""
import json

nb = json.load(open("Healthcare_Launcher.ipynb", "r", encoding="utf-8"))
cell = nb["cells"][14]
src = cell["source"]

fixed = 0
for i, line in enumerate(src):
    if 'notebookutils.notebook.run("NB_RTI_Operations_Agent", timeout_seconds=600)' in line:
        src[i] = line.replace(
            'notebookutils.notebook.run("NB_RTI_Operations_Agent", timeout_seconds=600)',
            'notebookutils.notebook.run("NB_RTI_Operations_Agent", 600, {"useRootDefaultLakehouse": True})'
        )
        print(f"Fixed line {i}: {src[i].rstrip()}")
        fixed += 1

cell["source"] = src
json.dump(nb, open("Healthcare_Launcher.ipynb", "w", encoding="utf-8"), indent=1, ensure_ascii=False)
print(f"Fixed {fixed} call(s). Saved.")

# Also update _cell12_fixed.py
full_src = "".join(src)
with open("_cell12_fixed.py", "w", encoding="utf-8") as f:
    f.write(full_src)

compile(full_src, "cell12", "exec")
print("SYNTAX: OK")
