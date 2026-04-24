"""Fix the workspace_name reference in the PBIX patch code."""
import json

NB_PATH = "Healthcare_Launcher.ipynb"

with open(NB_PATH, "r", encoding="utf-8") as f:
    nb = json.load(f)

lines = nb["cells"][10]["source"]
fixed = False
for j, line in enumerate(lines):
    if "workspace_name" in line and "ws_name_encoded" in line:
        # Replace the single line with workspace name lookup
        lines[j] = (
            '            ws_resp = requests.get(\n'
        )
        lines.insert(j + 1, '                f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}",\n')
        lines.insert(j + 2, '                headers=headers_json\n')
        lines.insert(j + 3, '            )\n')
        lines.insert(j + 4, '            ws_name = ws_resp.json().get("displayName", "") if ws_resp.status_code == 200 else ""\n')
        lines.insert(j + 5, '            ws_name_encoded = urllib.parse.quote(ws_name) if ws_name else workspace_id\n')
        print(f"Fixed workspace_name at line {j}")
        fixed = True
        break

if not fixed:
    print("Could not find line to fix!")
    exit(1)

nb["cells"][10]["source"] = lines

with open(NB_PATH, "w", encoding="utf-8", newline="\n") as f:
    json.dump(nb, f, indent=1, ensure_ascii=False)
    f.write("\n")

print("Done. workspace_name now resolved via API.")
