"""Fix cell 12 indentation after Fabric bug patch."""
import json

nb = json.load(open("Healthcare_Launcher.ipynb", "r", encoding="utf-8"))
cell = nb["cells"][11]
src = cell["source"]

# Lines 615, 616, 618, 619 need 4 more spaces of indent (20 total)
for idx in [615, 616, 618, 619]:
    old = src[idx]
    stripped = old.lstrip()
    src[idx] = " " * 20 + stripped
    if not src[idx].endswith("\n"):
        src[idx] += "\n"
    print(f"Line {idx}: re-indented to 20 spaces")

cell["source"] = src
json.dump(nb, open("Healthcare_Launcher.ipynb", "w", encoding="utf-8"), indent=1, ensure_ascii=False)

# Verify
compile("".join(src), "cell12", "exec")
print("Cell 12: SYNTAX OK")
