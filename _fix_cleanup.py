"""Remove the unused tenant_resp call from the PBIX patch code."""
import json

NB_PATH = "Healthcare_Launcher.ipynb"

with open(NB_PATH, "r", encoding="utf-8") as f:
    nb = json.load(f)

lines = nb["cells"][10]["source"]

# Remove the tenant_resp lines (3-4 lines: comment + request call)
remove_indices = []
for j, line in enumerate(lines):
    if "Discover the tenant ID" in line:
        remove_indices.append(j)
    elif "admin/tenants" in line:
        remove_indices.append(j)
    elif "tenant_resp" in line and "headers" in line and j not in remove_indices:
        remove_indices.append(j)

# Also remove the closing paren line if it's part of the tenant call
# Find the block: tenant_resp = requests.get(\n  "url",\n  headers=headers\n )
for j, line in enumerate(lines):
    if "tenant_resp = requests.get(" in line:
        # Multi-line call: find the closing )
        k = j + 1
        while k < len(lines) and ")" not in lines[k]:
            if k not in remove_indices:
                remove_indices.append(k)
            k += 1
        if k < len(lines) and k not in remove_indices:
            remove_indices.append(k)

# Remove in reverse order
for idx in sorted(set(remove_indices), reverse=True):
    removed = lines.pop(idx)
    print(f"  Removed line {idx}: {removed.rstrip()}")

nb["cells"][10]["source"] = lines

with open(NB_PATH, "w", encoding="utf-8", newline="\n") as f:
    json.dump(nb, f, indent=1, ensure_ascii=False)
    f.write("\n")

print("Done. Removed dead tenant_resp code.")
