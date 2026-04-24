"""Check Eventstream topology status."""
import subprocess, json, requests

token = json.loads(subprocess.run(
    ['cmd', '/c', 'az', 'account', 'get-access-token', '--resource', 'https://api.fabric.microsoft.com'],
    capture_output=True, text=True
).stdout)['accessToken']
H = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
API = 'https://api.fabric.microsoft.com/v1'
WS = '1cee1797-ef2f-4911-b4e5-577eb64bd452'
ES = 'fa5858fa-102a-4d80-8d5d-4f074b1f30de'

r = requests.get(f'{API}/workspaces/{WS}/eventstreams/{ES}/topology', headers=H)
if r.status_code == 200:
    topo = r.json()
    print("SOURCES:")
    for s in topo.get('sources', []):
        print(f"  {s['name']} ({s['type']}) - {s.get('status','?')}")
        print(f"    props: {json.dumps(s.get('properties',{}))[:300]}")
    print()
    print("STREAMS:")
    for s in topo.get('streams', []):
        print(f"  {s['name']} ({s['type']}) - {s.get('status','?')}")
        print(f"    inputNodes: {s.get('inputNodes',[])}")
    print()
    print("DESTINATIONS:")
    for d in topo.get('destinations', []):
        print(f"  {d['name']} ({d['type']}) - {d.get('status','?')}")
        print(f"    inputNodes: {d.get('inputNodes',[])}")
        print(f"    props: {json.dumps(d.get('properties',{}), indent=4)[:600]}")
        print()
else:
    print(f"Topology: HTTP {r.status_code} - {r.text[:300]}")

# Also check if lh_bronze_raw exists
print("\nLAKEHOUSES:")
r2 = requests.get(f'{API}/workspaces/{WS}/lakehouses', headers=H)
if r2.status_code == 200:
    for lh in r2.json().get('value', []):
        print(f"  {lh['displayName']} - {lh['id']}")
else:
    print(f"  HTTP {r2.status_code}")
