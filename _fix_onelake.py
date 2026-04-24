"""Fix OneLake availability and shortcuts."""
import subprocess, json, requests, time

token = json.loads(subprocess.run(
    ['cmd', '/c', 'az', 'account', 'get-access-token', '--resource', 'https://api.fabric.microsoft.com'],
    capture_output=True, text=True
).stdout)['accessToken']
H = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
API = 'https://api.fabric.microsoft.com/v1'
WS = '1cee1797-ef2f-4911-b4e5-577eb64bd452'
KQL_DB_ID = '5a9b9b29-31d1-42f4-a51c-4eb857798ddd'
KQL_DB_NAME = 'Healthcare_RTI_DB'
LH_GOLD_ID = 'a277d255-ebcf-495c-8ed3-d15cf62e683b'
QUERY_URI = 'https://trd-p1yddmtpy7uur9hxvn.z9.kusto.fabric.microsoft.com'

kusto_token = json.loads(subprocess.run(
    ['cmd', '/c', 'az', 'account', 'get-access-token', '--resource', QUERY_URI],
    capture_output=True, text=True
).stdout)['accessToken']
KH = {'Authorization': f'Bearer {kusto_token}', 'Content-Type': 'application/json'}

def run_kql(csl):
    r = requests.post(f'{QUERY_URI}/v1/rest/mgmt', headers=KH,
                       json={"csl": csl, "db": KQL_DB_NAME})
    return r

# 1. Try different OneLake availability syntaxes
print("=== Enabling OneLake availability ===")

commands_to_try = [
    # Table-level
    '.alter table claims_events policy onelake_availability @\'{"enabled": true}\'',
    # Database-level with bracket quoting
    '.alter database Healthcare_RTI_DB policy onelake_availability @\'{"enabled": true}\'',
    # Without brackets or quotes
    '.alter database policy onelake_availability @\'{"enabled": true}\'',
    # Show what commands are available
    '.show database policy onelake_availability',
]

for cmd in commands_to_try:
    r = run_kql(cmd)
    status = "OK" if r.status_code == 200 else f"ERR {r.status_code}"
    detail = ""
    if r.status_code == 200:
        tables = r.json().get('Tables', [])
        if tables and tables[0].get('Rows'):
            detail = str(tables[0]['Rows'][0])[:200]
    else:
        detail = r.text[:200]
    print(f"  [{status}] {cmd[:80]}...")
    if detail:
        print(f"    -> {detail}")
    if r.status_code == 200 and 'alter' in cmd.lower():
        print("    SUCCESS!")
        break

# 2. Check if data exists in OneLake path for KQL DB
print("\n=== Checking OneLake paths ===")
# Try listing files at the KQL DB OneLake path
onelake_base = f"https://onelake.dfs.fabric.microsoft.com/{WS}/{KQL_DB_ID}"
ol_token = json.loads(subprocess.run(
    ['cmd', '/c', 'az', 'account', 'get-access-token', '--resource', 'https://storage.azure.com'],
    capture_output=True, text=True
).stdout)['accessToken']
OL_H = {'Authorization': f'Bearer {ol_token}'}

# List the root of the KQL database in OneLake
r = requests.get(
    f'{onelake_base}?resource=filesystem&recursive=false',
    headers=OL_H
)
print(f"  List KQL DB root: HTTP {r.status_code}")
if r.status_code == 200:
    paths = r.json().get('paths', [])
    for p in paths[:20]:
        print(f"    {p.get('name', '?')} {'(dir)' if p.get('isDirectory') == 'true' else ''}")
else:
    print(f"  {r.text[:300]}")

# Try Tables/ subfolder
r2 = requests.get(
    f'{onelake_base}/Tables?resource=filesystem&recursive=false',
    headers=OL_H
)
print(f"\n  List Tables/: HTTP {r2.status_code}")
if r2.status_code == 200:
    paths = r2.json().get('paths', [])
    for p in paths[:20]:
        print(f"    {p.get('name', '?')} {'(dir)' if p.get('isDirectory') == 'true' else ''}")
elif r2.status_code == 404:
    print("  Tables/ folder doesn't exist — OneLake availability not enabled yet")
else:
    print(f"  {r2.text[:300]}")
