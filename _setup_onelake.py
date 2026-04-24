"""Discover current RTI items and enable OneLake availability."""
import subprocess, json, requests, time

token = json.loads(subprocess.run(
    ['cmd', '/c', 'az', 'account', 'get-access-token', '--resource', 'https://api.fabric.microsoft.com'],
    capture_output=True, text=True
).stdout)['accessToken']
H = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
API = 'https://api.fabric.microsoft.com/v1'
WS = '1cee1797-ef2f-4911-b4e5-577eb64bd452'

# 1. Discover all items
print("=== Workspace Items ===")
r = requests.get(f'{API}/workspaces/{WS}/items', headers=H)
r.raise_for_status()
items = r.json().get('value', [])

eventhouse_id = None
kql_db_id = None
lh_gold_id = None
kql_db_name = None

for it in items:
    itype = it['type']
    iname = it['displayName']
    if itype in ('Eventhouse', 'KQLDatabase', 'Lakehouse'):
        print(f"  {itype}: {iname} -> {it['id']}")
    if itype == 'Eventhouse' and 'RTI' in iname:
        eventhouse_id = it['id']
    if itype == 'KQLDatabase':
        kql_db_id = it['id']
        kql_db_name = iname
    if itype == 'Lakehouse' and iname == 'lh_gold_curated':
        lh_gold_id = it['id']

print(f"\n  Eventhouse: {eventhouse_id}")
print(f"  KQL DB: {kql_db_id} ({kql_db_name})")
print(f"  lh_gold_curated: {lh_gold_id}")

if not kql_db_id:
    print("ERROR: No KQL Database found")
    exit(1)

# 2. Try the runCommand API on KQL DB
print(f"\n=== Running .show tables on {kql_db_id} ===")
# Try both API formats
for api_path in [
    f'{API}/workspaces/{WS}/kqlDatabases/{kql_db_id}/runCommand',
    f'{API}/workspaces/{WS}/items/{kql_db_id}/jobs/instances',
]:
    r2 = requests.post(api_path, headers=H, json={"script": ".show tables | project TableName"})
    print(f"  {api_path.split('/')[-1]}: HTTP {r2.status_code}")
    if r2.status_code == 200:
        print(f"  {json.dumps(r2.json(), indent=2)[:500]}")
        break
    else:
        print(f"  {r2.text[:300]}")

# 3. Try the Kusto query endpoint directly
print(f"\n=== Trying Kusto query endpoint ===")
# Get the query URI from eventhouse properties
if eventhouse_id:
    r3 = requests.get(f'{API}/workspaces/{WS}/eventhouses/{eventhouse_id}', headers=H)
    if r3.status_code == 200:
        props = r3.json().get('properties', r3.json())
        query_uri = props.get('queryServiceUri', '')
        ingest_uri = props.get('ingestionServiceUri', '')
        print(f"  Query URI: {query_uri}")
        print(f"  Ingest URI: {ingest_uri}")
        
        if query_uri:
            # Get Kusto token
            kusto_token = json.loads(subprocess.run(
                ['cmd', '/c', 'az', 'account', 'get-access-token', '--resource', query_uri],
                capture_output=True, text=True
            ).stdout)['accessToken']
            
            # Run .show tables via Kusto REST
            kusto_h = {
                'Authorization': f'Bearer {kusto_token}',
                'Content-Type': 'application/json'
            }
            r4 = requests.post(
                f'{query_uri}/v1/rest/mgmt',
                headers=kusto_h,
                json={"csl": ".show tables | project TableName", "db": kql_db_name}
            )
            print(f"  Kusto mgmt: HTTP {r4.status_code}")
            if r4.status_code == 200:
                data = r4.json()
                frames = data.get('Tables', data.get('results', []))
                for frame in frames[:1]:
                    rows = frame.get('Rows', frame.get('rows', []))
                    for row in rows:
                        tbl = row[0] if isinstance(row, list) else row
                        print(f"    - {tbl}")
            else:
                print(f"  {r4.text[:500]}")
            
            # 4. Enable OneLake availability
            print(f"\n=== Enabling OneLake availability ===")
            r5 = requests.post(
                f'{query_uri}/v1/rest/mgmt',
                headers=kusto_h,
                json={"csl": f'.alter database ["{kql_db_name}"] policy onelake_availability @\'{{\"enabled\": true}}\'', "db": kql_db_name}
            )
            print(f"  HTTP {r5.status_code}")
            if r5.status_code == 200:
                print(f"  [OK] OneLake availability enabled!")
                print(f"  {json.dumps(r5.json(), indent=2)[:300]}")
            else:
                print(f"  {r5.text[:500]}")
    else:
        print(f"  Eventhouse props: HTTP {r3.status_code}")

# 5. After enabling OneLake, try creating shortcut again
if lh_gold_id and kql_db_id:
    print(f"\n=== Creating shortcuts ===")
    time.sleep(5)  # Wait for OneLake availability to propagate
    
    shortcuts = [
        ("rti_claims_events", "claims_events"),
        ("rti_adt_events", "adt_events"),
        ("rti_rx_events", "rx_events"),
    ]
    
    for sc_name, kql_table in shortcuts:
        body = {
            "name": sc_name,
            "path": "Tables",
            "target": {
                "oneLake": {
                    "workspaceId": WS,
                    "itemId": kql_db_id,
                    "path": f"Tables/{kql_table}"
                }
            }
        }
        r6 = requests.post(
            f'{API}/workspaces/{WS}/items/{lh_gold_id}/shortcuts',
            headers=H,
            json=body
        )
        if r6.status_code in (200, 201):
            print(f"  [OK] {sc_name} -> KQL:{kql_table}")
        elif r6.status_code == 409:
            print(f"  [EXISTS] {sc_name} already exists")
        else:
            print(f"  [{r6.status_code}] {sc_name}: {r6.text[:300]}")
