"""Push updated Lakehouse minimumRows to live Eventstream."""
import subprocess, json, requests, base64, time

token = json.loads(subprocess.run(
    ['cmd', '/c', 'az', 'account', 'get-access-token', '--resource', 'https://api.fabric.microsoft.com'],
    capture_output=True, text=True
).stdout)['accessToken']
H = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
API = 'https://api.fabric.microsoft.com/v1'
WS = '1cee1797-ef2f-4911-b4e5-577eb64bd452'
ES = 'fa5858fa-102a-4d80-8d5d-4f074b1f30de'

# Get current definition
print("Getting current definition...")
r = requests.post(f'{API}/workspaces/{WS}/eventstreams/{ES}/getDefinition', headers=H)
if r.status_code == 202:
    loc = r.headers.get('Location', '')
    for i in range(15):
        time.sleep(3)
        r = requests.get(loc, headers=H)
        if r.status_code == 200:
            break
    else:
        print("Timeout waiting for getDefinition LRO")
        exit(1)

if r.status_code != 200:
    print(f"getDefinition failed: HTTP {r.status_code}")
    exit(1)

defn = r.json()
parts = defn.get('definition', {}).get('parts', [])

# Find and update eventstream.json
for part in parts:
    if part['path'] == 'eventstream.json':
        es_def = json.loads(base64.b64decode(part['payload']))
        for dest in es_def.get('destinations', []):
            if dest.get('type') == 'Lakehouse':
                old_val = dest['properties'].get('minimumRows', '?')
                dest['properties']['minimumRows'] = 1000
                print(f"  {dest['name']}: minimumRows {old_val} -> 1000")
        part['payload'] = base64.b64encode(json.dumps(es_def, indent=2).encode()).decode()
        break

# Push updated definition
print("Pushing updated definition...")
update_body = {"definition": {"parts": parts}}
r2 = requests.post(
    f'{API}/workspaces/{WS}/eventstreams/{ES}/updateDefinition?updateMetadata=true',
    headers=H, json=update_body
)
print(f"updateDefinition: HTTP {r2.status_code}")
if r2.status_code == 200:
    print("[OK] Lakehouse destination updated — minimumRows=1000")
elif r2.status_code == 202:
    loc2 = r2.headers.get('Location', '')
    for i in range(15):
        time.sleep(3)
        r3 = requests.get(loc2, headers=H)
        if r3.status_code == 200:
            print("[OK] Lakehouse destination updated (LRO) — minimumRows=1000")
            break
else:
    print(f"Error: {r2.text[:500]}")

# Verify
print("\nVerifying topology...")
time.sleep(5)
r4 = requests.get(f'{API}/workspaces/{WS}/eventstreams/{ES}/topology', headers=H)
if r4.status_code == 200:
    topo = r4.json()
    for d in topo.get('destinations', []):
        mr = d.get('properties', {}).get('minimumRows', '?')
        print(f"  {d['name']} ({d['type']}) - {d.get('status','?')} - minimumRows={mr}")
