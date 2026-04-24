"""Enable OneLake availability via Fabric REST API (not KQL)."""
import subprocess, json, requests, time

token = json.loads(subprocess.run(
    ['cmd', '/c', 'az', 'account', 'get-access-token', '--resource', 'https://api.fabric.microsoft.com'],
    capture_output=True, text=True
).stdout)['accessToken']
H = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
API = 'https://api.fabric.microsoft.com/v1'
WS = '1cee1797-ef2f-4911-b4e5-577eb64bd452'
EVENTHOUSE_ID = 'd85e778f-e500-44c2-b9d4-1745bda288ae'
KQL_DB_ID = '5a9b9b29-31d1-42f4-a51c-4eb857798ddd'
LH_GOLD_ID = 'a277d255-ebcf-495c-8ed3-d15cf62e683b'

# 1. Get Eventhouse properties
print("=== Eventhouse Properties ===")
r = requests.get(f'{API}/workspaces/{WS}/eventhouses/{EVENTHOUSE_ID}', headers=H)
print(f"  HTTP {r.status_code}")
if r.status_code == 200:
    print(json.dumps(r.json(), indent=2)[:800])

# 2. Get KQL Database properties
print("\n=== KQL Database Properties ===")
r2 = requests.get(f'{API}/workspaces/{WS}/kqlDatabases/{KQL_DB_ID}', headers=H)
print(f"  HTTP {r2.status_code}")
if r2.status_code == 200:
    data = r2.json()
    print(json.dumps(data, indent=2)[:800])
    
    # Check if oneLakeCachingPeriod or oneLakeAvailability exists
    props = data.get('properties', {})
    print(f"\n  All property keys: {list(props.keys())}")
    
    # 3. Try PATCH to enable OneLake availability
    print("\n=== Enabling OneLake via PATCH ===")
    patch_body = {
        "properties": {
            "oneLakeStandardAvailability": True,
            "oneLakeCachingEnabled": True
        }
    }
    r3 = requests.patch(
        f'{API}/workspaces/{WS}/kqlDatabases/{KQL_DB_ID}',
        headers=H,
        json=patch_body
    )
    print(f"  PATCH: HTTP {r3.status_code}")
    if r3.status_code in (200, 202):
        print(f"  [OK] {r3.text[:300]}")
    else:
        print(f"  {r3.text[:500]}")
    
    # 4. Also try updateDefinition approach
    print("\n=== Trying updateDefinition ===")
    import base64
    r4 = requests.post(
        f'{API}/workspaces/{WS}/kqlDatabases/{KQL_DB_ID}/getDefinition',
        headers=H
    )
    print(f"  getDefinition: HTTP {r4.status_code}")
    
    if r4.status_code == 200:
        defn = r4.json()
        print(f"  Parts: {[p['path'] for p in defn.get('definition',{}).get('parts',[])]}")
        for part in defn.get('definition',{}).get('parts',[]):
            if part.get('payload'):
                try:
                    decoded = json.loads(base64.b64decode(part['payload']))
                    print(f"  {part['path']}: {json.dumps(decoded, indent=2)[:500]}")
                except:
                    print(f"  {part['path']}: (binary)")
    elif r4.status_code == 202:
        loc = r4.headers.get('Location', '')
        print(f"  LRO: {loc[:100]}")
        for i in range(10):
            time.sleep(3)
            r5 = requests.get(loc, headers=H)
            if r5.status_code == 200:
                defn = r5.json()
                print(f"  Parts: {[p['path'] for p in defn.get('definition',{}).get('parts',[])]}")
                for part in defn.get('definition',{}).get('parts',[]):
                    if part.get('payload'):
                        try:
                            decoded = json.loads(base64.b64decode(part['payload']))
                            print(f"  {part['path']}: {json.dumps(decoded, indent=2)[:600]}")
                        except:
                            print(f"  {part['path']}: (binary)")
                break
    else:
        print(f"  {r4.text[:300]}")

# 5. Check OneLake paths with proper DFS API format
print("\n=== Checking OneLake DFS paths ===")
ol_token = json.loads(subprocess.run(
    ['cmd', '/c', 'az', 'account', 'get-access-token', '--resource', 'https://storage.azure.com'],
    capture_output=True, text=True
).stdout)['accessToken']
OL_H = {'Authorization': f'Bearer {ol_token}'}

# Proper ADLS Gen2 list format for OneLake
r6 = requests.get(
    f'https://onelake.dfs.fabric.microsoft.com/{WS}',
    headers=OL_H,
    params={'resource': 'account', 'maxResults': '50'}
)
print(f"  List filesystems: HTTP {r6.status_code}")
if r6.status_code == 200:
    for fs in r6.json().get('filesystems', [])[:10]:
        print(f"    {fs.get('name', '?')}")

# Try listing inside the KQL database
r7 = requests.get(
    f'https://onelake.dfs.fabric.microsoft.com/{WS}/{KQL_DB_ID}',
    headers=OL_H,
    params={'resource': 'filesystem', 'recursive': 'true', 'maxResults': '20'}
)
print(f"\n  List KQL DB filesystem: HTTP {r7.status_code}")
if r7.status_code == 200:
    for p in r7.json().get('paths', [])[:20]:
        print(f"    {p.get('name', '?')} {'(dir)' if p.get('isDirectory') == 'true' else ''}")
else:
    print(f"  {r7.text[:300]}")
