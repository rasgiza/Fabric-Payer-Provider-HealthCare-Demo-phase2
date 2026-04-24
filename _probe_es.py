"""Probe Eventstream API for connection string discoverability."""
import subprocess, json, requests, base64, time

token = json.loads(subprocess.run(
    ['cmd', '/c', 'az', 'account', 'get-access-token', '--resource', 'https://api.fabric.microsoft.com'],
    capture_output=True, text=True
).stdout)['accessToken']
H = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
API = 'https://api.fabric.microsoft.com/v1'
WS = '1cee1797-ef2f-4911-b4e5-577eb64bd452'
ES = 'fa5858fa-102a-4d80-8d5d-4f074b1f30de'

# 1. Topology
print('=== TOPOLOGY ===')
r = requests.get(f'{API}/workspaces/{WS}/eventstreams/{ES}/topology', headers=H)
if r.status_code == 200:
    topo = r.json()
    for src in topo.get('sources', []):
        name = src.get('name', '?')
        stype = src.get('type', '?')
        status = src.get('status', '?')
        print(f'Source: {name} ({stype}) - Status: {status}')
        props = src.get('properties', {})
        print(f'  Properties keys: {list(props.keys())}')
        for k, v in props.items():
            print(f'  {k}: {str(v)[:300]}')
else:
    print(f'Topology: HTTP {r.status_code}')

# 2. getDefinition
print('\n=== GET DEFINITION ===')
r2 = requests.post(f'{API}/workspaces/{WS}/eventstreams/{ES}/getDefinition', headers=H)

def parse_definition(defn):
    for part in defn.get('definition', {}).get('parts', []):
        path = part.get('path', '?')
        print(f'Part: {path}')
        payload = part.get('payload', '')
        if payload:
            try:
                decoded = json.loads(base64.b64decode(payload))
                if 'sources' in decoded:
                    for src in decoded['sources']:
                        print(f'  Source: {src.get("name")}')
                        props = src.get('properties', {})
                        for k, v in props.items():
                            print(f'    {k}: {str(v)[:300]}')
                # Also dump top-level keys
                if path == 'eventstream.json':
                    for k in decoded:
                        if k not in ('sources', 'destinations', 'streams', 'operators'):
                            print(f'  top-level: {k} = {str(decoded[k])[:200]}')
            except Exception as e:
                print(f'  (decode error: {e})')

if r2.status_code == 200:
    parse_definition(r2.json())
elif r2.status_code == 202:
    loc = r2.headers.get('Location', '')
    print(f'LRO: polling...')
    for i in range(15):
        time.sleep(3)
        r3 = requests.get(loc, headers=H)
        if r3.status_code == 200:
            parse_definition(r3.json())
            break
        elif r3.status_code != 202:
            print(f'Poll: HTTP {r3.status_code}')
            break
else:
    print(f'getDefinition: HTTP {r2.status_code} - {r2.text[:300]}')
