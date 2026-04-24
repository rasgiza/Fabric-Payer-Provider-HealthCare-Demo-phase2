"""
Probe OneLake availability on Eventhouse KQL DB and test shortcut creation.
"""
import subprocess, json, requests, time

token = json.loads(subprocess.run(
    ['cmd', '/c', 'az', 'account', 'get-access-token', '--resource', 'https://api.fabric.microsoft.com'],
    capture_output=True, text=True
).stdout)['accessToken']
H = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
API = 'https://api.fabric.microsoft.com/v1'
WS = '1cee1797-ef2f-4911-b4e5-577eb64bd452'
KQL_DB_ID = '5a9b9b29-31d1-42f4-a51c-4eb857798ddd'
LH_GOLD_ID = 'a277d255-ebcf-495c-8ed3-d15cf62e683b'

# 1. Check current OneLake availability status via KQL command
print("=== Checking OneLake availability policy ===")
r = requests.post(
    f'{API}/workspaces/{WS}/kqlDatabases/{KQL_DB_ID}/runCommand',
    headers=H,
    json={"script": ".show database policy onelake_availability"}
)
print(f"  HTTP {r.status_code}")
if r.status_code == 200:
    print(f"  Response: {json.dumps(r.json(), indent=2)[:500]}")
else:
    print(f"  {r.text[:300]}")

# 2. Try enabling OneLake availability
print("\n=== Enabling OneLake availability ===")
r2 = requests.post(
    f'{API}/workspaces/{WS}/kqlDatabases/{KQL_DB_ID}/runCommand',
    headers=H,
    json={"script": '.alter database Healthcare_RTI_DB policy onelake_availability @\'{"enabled": true}\''}
)
print(f"  HTTP {r2.status_code}")
if r2.status_code == 200:
    print(f"  Response: {json.dumps(r2.json(), indent=2)[:500]}")
else:
    print(f"  {r2.text[:500]}")

# 3. List existing tables
print("\n=== KQL Tables ===")
r3 = requests.post(
    f'{API}/workspaces/{WS}/kqlDatabases/{KQL_DB_ID}/runCommand',
    headers=H,
    json={"script": ".show tables | project TableName"}
)
if r3.status_code == 200:
    data = r3.json()
    for frame in data.get('results', []):
        for row in frame.get('rows', []):
            tbl = row[0] if isinstance(row, list) else row
            print(f"  - {tbl}")
else:
    print(f"  HTTP {r3.status_code}: {r3.text[:300]}")

# 4. Check existing shortcuts in lh_gold_curated
print("\n=== Existing shortcuts in lh_gold_curated ===")
r4 = requests.get(
    f'{API}/workspaces/{WS}/items/{LH_GOLD_ID}/shortcuts',
    headers=H
)
print(f"  HTTP {r4.status_code}")
if r4.status_code == 200:
    shortcuts = r4.json().get('value', [])
    if shortcuts:
        for sc in shortcuts:
            print(f"  - {sc.get('name')} -> {sc.get('target', {})}")
    else:
        print("  (none)")
else:
    print(f"  {r4.text[:300]}")

# 5. Try creating a test shortcut
print("\n=== Creating test shortcut: rti_claims_events -> claims_events ===")
shortcut_body = {
    "name": "rti_claims_events",
    "path": "Tables",
    "target": {
        "oneLake": {
            "workspaceId": WS,
            "itemId": KQL_DB_ID,
            "path": "Tables/claims_events"
        }
    }
}
r5 = requests.post(
    f'{API}/workspaces/{WS}/items/{LH_GOLD_ID}/shortcuts',
    headers=H,
    json=shortcut_body
)
print(f"  HTTP {r5.status_code}")
if r5.status_code in (200, 201):
    print(f"  [OK] Shortcut created!")
    print(f"  {json.dumps(r5.json(), indent=2)[:300]}")
elif r5.status_code == 409:
    print(f"  Already exists (409)")
else:
    print(f"  {r5.text[:500]}")
