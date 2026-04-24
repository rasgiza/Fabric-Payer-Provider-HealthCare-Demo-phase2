import json
nb = json.load(open('Healthcare_Launcher.ipynb', encoding='utf-8'))

# Show cell 15 and 16 first lines
for idx in [15, 16]:
    c = nb['cells'][idx]
    src = ''.join(c['source'])
    lines = c['source']
    print(f"=== Cell {idx} ({len(lines)} lines) ===")
    print(src[:500])
    print("...\n")
