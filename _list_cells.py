import json
nb = json.load(open('Healthcare_Launcher.ipynb', encoding='utf-8'))
for i, c in enumerate(nb['cells']):
    first = c['source'][0][:120].strip() if c['source'] else '(empty)'
    print(f"Cell {i}: {c['cell_type']} - {first}")
