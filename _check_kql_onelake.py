"""Check what's inside the KQL DB Tables folder in OneLake."""
import subprocess, json, requests

ol_token = json.loads(subprocess.run(
    ['cmd', '/c', 'az', 'account', 'get-access-token', '--resource', 'https://storage.azure.com'],
    capture_output=True, text=True
).stdout)['accessToken']
OL_H = {'Authorization': f'Bearer {ol_token}'}

token = json.loads(subprocess.run(
    ['cmd', '/c', 'az', 'account', 'get-access-token', '--resource', 'https://api.fabric.microsoft.com'],
    capture_output=True, text=True
).stdout)['accessToken']
H = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
API = 'https://api.fabric.microsoft.com/v1'

WS = '1cee1797-ef2f-4911-b4e5-577eb64bd452'
KQL_DB_ID = '5a9b9b29-31d1-42f4-a51c-4eb857798ddd'
LH_GOLD_ID = 'a277d255-ebcf-495c-8ed3-d15cf62e683b'

# List Tables/ inside the KQL database OneLake path
print("=== KQL DB Tables/ in OneLake ===")
r = requests.get(
    f'https://onelake.dfs.fabric.microsoft.com/{WS}/{KQL_DB_ID}',
    headers=OL_H,
    params={'resource': 'filesystem', 'directory': f'{KQL_DB_ID}/Tables', 'recursive': 'true', 'maxResults': '100'}
)
print(f"  HTTP {r.status_code}")
if r.status_code == 200:
    paths = r.json().get('paths', [])
    if paths:
        for p in paths[:30]:
            name = p.get('name', '?')
            # Strip the prefix
            short = name.replace(f'{KQL_DB_ID}/Tables/', '')
            is_dir = p.get('isDirectory') == 'true'
            print(f"  {'[DIR] ' if is_dir else '      '}{short}")
    else:
        print("  (empty — no tables in OneLake)")
        print("  OneLake availability is likely NOT enabled for this database")
else:
    print(f"  {r.text[:500]}")

# Also check what the Lakehouse gold has
print("\n=== lh_gold_curated Tables/ in OneLake ===")
r2 = requests.get(
    f'https://onelake.dfs.fabric.microsoft.com/{WS}/{LH_GOLD_ID}',
    headers=OL_H,
    params={'resource': 'filesystem', 'directory': f'{LH_GOLD_ID}/Tables', 'recursive': 'false', 'maxResults': '50'}
)
print(f"  HTTP {r2.status_code}")
if r2.status_code == 200:
    paths = r2.json().get('paths', [])
    for p in paths[:30]:
        name = p.get('name', '?')
        short = name.replace(f'{LH_GOLD_ID}/Tables/', '')
        is_dir = p.get('isDirectory') == 'true'
        print(f"  {'[DIR] ' if is_dir else '      '}{short}")
else:
    print(f"  {r2.text[:300]}")

# Try creating shortcut pointing to KQL DB Tables path
print("\n=== Creating shortcut: rti_claims_events -> KQL claims_events ===")
shortcut_body = {
    "name": "rti_claims_events",
    "path": "Tables",
    "target": {
        "oneLake": {
            "workspaceId": WS,
            "itemId": KQL_DB_ID,
            "path": f"Tables/claims_events"
        }
    }
}
r3 = requests.post(
    f'{API}/workspaces/{WS}/items/{LH_GOLD_ID}/shortcuts',
    headers=H,
    json=shortcut_body
)
print(f"  HTTP {r3.status_code}")
if r3.status_code in (200, 201):
    print(f"  [OK] Created!")
    print(json.dumps(r3.json(), indent=2)[:300])
elif r3.status_code == 409:
    print("  Already exists")
else:
    print(f"  {r3.text[:500]}")
    
    # Try with Shortcut/ path instead of Tables/
    print("\n  Retrying with Shortcut/ source path...")
    shortcut_body2 = {
        "name": "rti_claims_events",
        "path": "Tables",
        "target": {
            "oneLake": {
                "workspaceId": WS,
                "itemId": KQL_DB_ID,
                "path": f"Shortcut/claims_events"
            }
        }
    }
    r4 = requests.post(
        f'{API}/workspaces/{WS}/items/{LH_GOLD_ID}/shortcuts',
        headers=H,
        json=shortcut_body2
    )
    print(f"  HTTP {r4.status_code}")
    if r4.status_code in (200, 201):
        print(f"  [OK] Created!")
    else:
        print(f"  {r4.text[:500]}")
