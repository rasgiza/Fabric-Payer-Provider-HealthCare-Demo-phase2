"""Remove the leftover tenant_resp line."""
import json

NB_PATH = "Healthcare_Launcher.ipynb"

with open(NB_PATH, "r", encoding="utf-8") as f:
    nb = json.load(f)

lines = nb["cells"][10]["source"]

# Find and remove the orphaned "tenant_resp = requests.get(" line
for j, line in enumerate(lines):
    if "tenant_resp = requests.get(" in line:
        removed = lines.pop(j)
        print(f"Removed line {j}: {removed.rstrip()}")
        break

nb["cells"][10]["source"] = lines

with open(NB_PATH, "w", encoding="utf-8", newline="\n") as f:
    json.dump(nb, f, indent=1, ensure_ascii=False)
    f.write("\n")

print("Done.")
