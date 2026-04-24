import json
nb = json.load(open('Healthcare_Launcher.ipynb', encoding='utf-8'))
for i, c in enumerate(nb['cells']):
    src = ''.join(c.get('source', []))
    first = src.strip().split('\n')[0][:100]
    nlines = len(c['source'])
    has_sc = ' ** HAS SHORTCUTS **' if 'CREATING ONELAKE SHORTCUTS' in src else ''
    has_box = ' ** HAS BOX **' if 'TWO QUICK STEPS' in src else ''
    print(f"Cell {i} ({nlines} lines){has_sc}{has_box}: {first}")
