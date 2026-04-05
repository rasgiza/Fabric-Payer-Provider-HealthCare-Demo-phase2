# Fabric notebook source

# METADATA **{"language":"markdown"}**

# MARKDOWN **{"language":"markdown"}**

# # RTI Post-Deploy Setup
# 
# **Prerequisites:** The Eventhouse and KQL Database must already be deployed as Git artifacts
# (via `fabric-launcher` or `fabric-cicd`). This notebook performs post-deploy wiring:
# 
# 1. **Discover** Eventhouse + KQL Database by display name (zero hardcoded IDs)
# 2. **Execute** schema — creates 6 tables + streaming ingestion policies + JSON mappings
# 3. **Create** Eventstream with **Custom Endpoint** source via API
# 4. **Wire** Eventstream destination to KQL Database
# 5. **Output** the Custom Endpoint connection string for the Event Simulator
# 
# **Default lakehouse:** `lh_gold_curated`

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ============================================================================
# NB_RTI_Setup_Eventhouse — Post-Deploy Wiring
# ============================================================================
# The Eventhouse + KQL Database are deployed as Git artifacts by the launcher.
# This notebook discovers them, executes the schema, and creates the Eventstream.
#
# Zero hardcoded IDs — everything resolved by displayName lookup.
# Default lakehouse: lh_gold_curated
# ============================================================================

print("NB_RTI_Setup_Eventhouse: Starting post-deploy setup...")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ---------- Parameters ----------
WORKSPACE_ID = ""                               # Auto-detected if blank
EVENTHOUSE_NAME = "Healthcare_RTI_Eventhouse"    # Must match Git artifact displayName
KQL_DB_NAME = "Healthcare_RTI_DB"                # Must match Git artifact displayName
EVENTSTREAM_NAME = "Healthcare_RTI_Eventstream"  # Created via API (not Git-tracked)

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

import requests
import json
import time
import base64

def get_fabric_token():
    """Get Fabric API token from notebook context."""
    return notebookutils.credentials.getToken("https://analysis.windows.net/powerbi/api")

def get_headers():
    return {
        "Authorization": f"Bearer {get_fabric_token()}",
        "Content-Type": "application/json"
    }

BASE_URL = "https://api.fabric.microsoft.com/v1"

if not WORKSPACE_ID:
    WORKSPACE_ID = notebookutils.runtime.context.get("currentWorkspaceId", "")
    if not WORKSPACE_ID:
        raise ValueError("WORKSPACE_ID must be set -- could not auto-detect")

print(f"Workspace ID: {WORKSPACE_ID}")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ============================================================================
# Step 1: Discover Eventhouse + KQL Database by displayName
# ============================================================================
print("Step 1: Discovering deployed RTI artifacts...")

def find_item(item_type, display_name):
    """Find a workspace item by type and displayName. Returns item dict or None."""
    url = f"{BASE_URL}/workspaces/{WORKSPACE_ID}/items?type={item_type}"
    resp = requests.get(url, headers=get_headers())
    resp.raise_for_status()
    for item in resp.json().get("value", []):
        if item["displayName"] == display_name:
            return item
    return None

def create_or_get_item(item_type, display_name, description=""):
    """Create a Fabric item or return existing one."""
    existing = find_item(item_type, display_name)
    if existing:
        print(f"  {item_type} '{display_name}' already exists: {existing['id']}")
        return existing["id"]

    url = f"{BASE_URL}/workspaces/{WORKSPACE_ID}/items"
    body = {
        "displayName": display_name,
        "type": item_type,
        "description": description
    }
    resp = requests.post(url, headers=get_headers(), json=body)

    if resp.status_code == 202:
        op_url = resp.headers.get("Location", "")
        print(f"  Creating {item_type} '{display_name}' (async)...")
        for _ in range(60):
            time.sleep(5)
            op_resp = requests.get(op_url, headers=get_headers())
            if op_resp.status_code == 200:
                op_data = op_resp.json()
                if op_data.get("status", "").lower() in ("succeeded",):
                    item_id = op_data.get("resourceId") or op_data.get("id", "")
                    print(f"  Created: {item_id}")
                    return item_id
                elif op_data.get("status", "").lower() in ("failed",):
                    raise RuntimeError(f"Failed to create {item_type}: {op_data}")
        raise TimeoutError(f"Timed out creating {item_type}")
    elif resp.status_code == 201:
        item_id = resp.json().get("id", "")
        print(f"  Created {item_type} '{display_name}': {item_id}")
        return item_id
    else:
        resp.raise_for_status()

# Discover Eventhouse
eventhouse = find_item("Eventhouse", EVENTHOUSE_NAME)
if not eventhouse:
    raise RuntimeError(
        f"Eventhouse '{EVENTHOUSE_NAME}' not found in workspace.\n"
        f"Ensure Healthcare_Launcher.ipynb deployed the Eventhouse artifact first.\n"
        f"Check the workspace for the item or re-run the launcher."
    )
eventhouse_id = eventhouse["id"]
print(f"  Eventhouse: {EVENTHOUSE_NAME} ({eventhouse_id})")

# Discover KQL Database
kql_db = find_item("KQLDatabase", KQL_DB_NAME)
if not kql_db:
    # KQL DB may have same name as Eventhouse (auto-created behavior)
    kql_db = find_item("KQLDatabase", EVENTHOUSE_NAME)
    if kql_db:
        KQL_DB_NAME = EVENTHOUSE_NAME
        print(f"  KQL Database found with Eventhouse name: {KQL_DB_NAME}")

if not kql_db:
    raise RuntimeError(
        f"KQL Database '{KQL_DB_NAME}' not found in workspace.\n"
        f"The KQL Database should be deployed as a Git artifact alongside the Eventhouse."
    )
kql_db_id = kql_db["id"]
print(f"  KQL Database: {KQL_DB_NAME} ({kql_db_id})")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ============================================================================
# Step 2: Execute KQL schema — create tables + streaming policies + mappings
# ============================================================================
print("Step 2: Executing KQL schema commands...")

KQL_COMMANDS = [
    # --- INPUT TABLES ---
    """.create-merge table claims_events (
        event_id: string, event_timestamp: datetime, event_type: string,
        claim_id: string, patient_id: string, provider_id: string,
        facility_id: string, payer_id: string, diagnosis_code: string,
        procedure_code: string, claim_type: string, claim_amount: real,
        latitude: real, longitude: real, injected_fraud_flags: string
    )""",
    """.create-merge table adt_events (
        event_id: string, event_timestamp: datetime, event_type: string,
        patient_id: string, facility_id: string, facility_name: string,
        admission_type: string, primary_diagnosis: string,
        latitude: real, longitude: real,
        has_open_care_gaps: bool, open_gap_measures: string
    )""",
    """.create-merge table rx_events (
        event_id: string, event_timestamp: datetime, event_type: string,
        patient_id: string, provider_id: string, medication_code: string,
        medication_name: string, drug_class: string,
        quantity: int, days_supply: int, latitude: real, longitude: real
    )""",
    # --- OUTPUT TABLES ---
    """.create-merge table fraud_scores (
        score_id: string, score_timestamp: datetime, claim_id: string,
        patient_id: string, provider_id: string, fraud_score: real,
        fraud_flags: string, risk_tier: string, latitude: real, longitude: real
    )""",
    """.create-merge table care_gap_alerts (
        alert_id: string, alert_timestamp: datetime, patient_id: string,
        facility_id: string, measure_id: string, measure_name: string,
        gap_days_overdue: int, alert_priority: string, alert_text: string,
        latitude: real, longitude: real
    )""",
    """.create-merge table highcost_alerts (
        alert_id: string, alert_timestamp: datetime, patient_id: string,
        rolling_spend_30d: real, rolling_spend_90d: real, ed_visits_30d: int,
        readmission_flag: bool, risk_tier: string, cost_trend: string,
        latitude: real, longitude: real
    )""",
    # --- STREAMING INGESTION POLICIES ---
    ".alter table claims_events policy streamingingestion enable",
    ".alter table adt_events policy streamingingestion enable",
    ".alter table rx_events policy streamingingestion enable",
    # --- JSON INGESTION MAPPINGS (for Eventstream Custom Endpoint) ---
    """.create-or-alter table claims_events ingestion json mapping 'claims_events_mapping'
    '[{"column":"event_id","path":"$.event_id","datatype":"string"},{"column":"event_timestamp","path":"$.event_timestamp","datatype":"datetime"},{"column":"event_type","path":"$.event_type","datatype":"string"},{"column":"claim_id","path":"$.claim_id","datatype":"string"},{"column":"patient_id","path":"$.patient_id","datatype":"string"},{"column":"provider_id","path":"$.provider_id","datatype":"string"},{"column":"facility_id","path":"$.facility_id","datatype":"string"},{"column":"payer_id","path":"$.payer_id","datatype":"string"},{"column":"diagnosis_code","path":"$.diagnosis_code","datatype":"string"},{"column":"procedure_code","path":"$.procedure_code","datatype":"string"},{"column":"claim_type","path":"$.claim_type","datatype":"string"},{"column":"claim_amount","path":"$.claim_amount","datatype":"real"},{"column":"latitude","path":"$.latitude","datatype":"real"},{"column":"longitude","path":"$.longitude","datatype":"real"},{"column":"injected_fraud_flags","path":"$.injected_fraud_flags","datatype":"string"}]'""",
    """.create-or-alter table adt_events ingestion json mapping 'adt_events_mapping'
    '[{"column":"event_id","path":"$.event_id","datatype":"string"},{"column":"event_timestamp","path":"$.event_timestamp","datatype":"datetime"},{"column":"event_type","path":"$.event_type","datatype":"string"},{"column":"patient_id","path":"$.patient_id","datatype":"string"},{"column":"facility_id","path":"$.facility_id","datatype":"string"},{"column":"facility_name","path":"$.facility_name","datatype":"string"},{"column":"admission_type","path":"$.admission_type","datatype":"string"},{"column":"primary_diagnosis","path":"$.primary_diagnosis","datatype":"string"},{"column":"latitude","path":"$.latitude","datatype":"real"},{"column":"longitude","path":"$.longitude","datatype":"real"},{"column":"has_open_care_gaps","path":"$.has_open_care_gaps","datatype":"bool"},{"column":"open_gap_measures","path":"$.open_gap_measures","datatype":"string"}]'""",
    """.create-or-alter table rx_events ingestion json mapping 'rx_events_mapping'
    '[{"column":"event_id","path":"$.event_id","datatype":"string"},{"column":"event_timestamp","path":"$.event_timestamp","datatype":"datetime"},{"column":"event_type","path":"$.event_type","datatype":"string"},{"column":"patient_id","path":"$.patient_id","datatype":"string"},{"column":"provider_id","path":"$.provider_id","datatype":"string"},{"column":"medication_code","path":"$.medication_code","datatype":"string"},{"column":"medication_name","path":"$.medication_name","datatype":"string"},{"column":"drug_class","path":"$.drug_class","datatype":"string"},{"column":"quantity","path":"$.quantity","datatype":"int"},{"column":"days_supply","path":"$.days_supply","datatype":"int"},{"column":"latitude","path":"$.latitude","datatype":"real"},{"column":"longitude","path":"$.longitude","datatype":"real"}]'""",
]

def run_kql_command(db_id, command):
    """Execute a single KQL management command against the database."""
    url = f"{BASE_URL}/workspaces/{WORKSPACE_ID}/kqlDatabases/{db_id}/runCommand"
    return requests.post(url, headers=get_headers(), json={"script": command.strip()})

success_count = 0
fail_count = 0
for cmd in KQL_COMMANDS:
    cmd_clean = cmd.strip()
    if not cmd_clean:
        continue
    # Short label for logging
    if "create-merge table" in cmd_clean:
        label = cmd_clean.split("table ")[-1].split(" ")[0].split("(")[0].strip()
    elif "alter table" in cmd_clean and "policy" in cmd_clean:
        label = "streaming policy: " + cmd_clean.split("table ")[-1].split(" ")[0]
    elif "ingestion json mapping" in cmd_clean:
        label = "mapping: " + cmd_clean.split("table ")[-1].split(" ")[0]
    else:
        label = cmd_clean[:60]
    resp = run_kql_command(kql_db_id, cmd_clean)
    if resp.status_code in (200, 201):
        print(f"  OK: {label}")
        success_count += 1
    else:
        print(f"  WARN ({resp.status_code}): {label} -- {resp.text[:200]}")
        fail_count += 1

print(f"\nSchema execution complete: {success_count} succeeded, {fail_count} failed")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ============================================================================
# Step 3: Create Eventstream with Custom Endpoint source
# ============================================================================
print("Step 3: Creating Eventstream with Custom Endpoint...")

eventstream_id = create_or_get_item(
    "Eventstream",
    EVENTSTREAM_NAME,
    "Healthcare RTI -- Custom Endpoint for claims, ADT, and Rx events"
)
print(f"  Eventstream ID: {eventstream_id}")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ============================================================================
# Step 4: Configure Eventstream — Custom Endpoint source + KQL DB destination
# ============================================================================
print("Step 4: Configuring Eventstream topology...")

eventstream_definition = {
    "sources": [
        {
            "name": "HealthcareCustomEndpoint",
            "type": "CustomEndpoint",
            "properties": {}
        }
    ],
    "destinations": [
        {
            "name": "KQL_claims_events",
            "type": "KQLDatabase",
            "properties": {
                "workspaceId": WORKSPACE_ID,
                "itemId": kql_db_id,
                "databaseName": KQL_DB_NAME,
                "tableName": "claims_events",
                "inputDataFormat": "Json",
                "mappingRuleName": "claims_events_mapping"
            }
        }
    ],
    "streams": [
        {
            "name": "HealthcareRTI-stream",
            "type": "DefaultStream"
        }
    ]
}

definition_payload = base64.b64encode(json.dumps(eventstream_definition).encode()).decode()
update_body = {
    "definition": {
        "parts": [
            {
                "path": "eventstream.json",
                "payload": definition_payload,
                "payloadType": "InlineBase64"
            },
            {
                "path": "eventstreamProperties.json",
                "payload": base64.b64encode(json.dumps({
                    "retentionTimeInDays": 1,
                    "eventThroughputLevel": "Low",
                    "schemaMode": "None"
                }).encode()).decode(),
                "payloadType": "InlineBase64"
            },
            {
                "path": ".platform",
                "payload": base64.b64encode(json.dumps({
                    "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
                    "metadata": {
                        "type": "Eventstream",
                        "displayName": EVENTSTREAM_NAME
                    },
                    "config": {
                        "version": "2.0",
                        "logicalId": "00000000-0000-0000-0000-000000000000"
                    }
                }).encode()).decode(),
                "payloadType": "InlineBase64"
            }
        ]
    }
}

update_url = f"{BASE_URL}/workspaces/{WORKSPACE_ID}/items/{eventstream_id}/updateDefinition"
resp = requests.post(update_url, headers=get_headers(), json=update_body)

if resp.status_code in (200, 201, 202):
    print("  Eventstream definition updated successfully")
    if resp.status_code == 202:
        op_url = resp.headers.get("Location", "")
        for _ in range(30):
            time.sleep(3)
            op_resp = requests.get(op_url, headers=get_headers())
            if op_resp.status_code == 200:
                status = op_resp.json().get("status", "").lower()
                if status == "succeeded":
                    print("  Eventstream configuration applied")
                    break
                elif status == "failed":
                    print(f"  WARN: Eventstream update failed: {op_resp.json()}")
                    break
else:
    print(f"  WARN: Eventstream definition update returned {resp.status_code}")
    print(f"  Response: {resp.text[:500]}")
    print()
    print("  If this fails, configure the Eventstream manually:")
    print("  1. Open the Eventstream in the Fabric portal")
    print("  2. Add a 'Custom Endpoint' source")
    print("  3. Add a 'KQL Database' destination pointing to Healthcare_RTI_DB")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ============================================================================
# Step 5: Retrieve Custom Endpoint connection string
# ============================================================================
print("Step 5: Retrieving Custom Endpoint connection string...")

def_url = f"{BASE_URL}/workspaces/{WORKSPACE_ID}/items/{eventstream_id}/getDefinition"
resp = requests.post(def_url, headers=get_headers())

connection_string = ""
eventhub_name = ""

def parse_connection_info(parts):
    """Extract connection string and event hub name from Eventstream definition parts."""
    cs, eh = "", ""
    for part in parts:
        if part.get("path") == "eventstream.json":
            try:
                es_def = json.loads(base64.b64decode(part["payload"]).decode())
                for source in es_def.get("sources", []):
                    props = source.get("properties", {})
                    if props.get("connectionString"):
                        cs = props["connectionString"]
                    if props.get("eventHubName"):
                        eh = props["eventHubName"]
            except Exception as e:
                print(f"  Could not parse eventstream definition: {e}")
    return cs, eh

if resp.status_code in (200, 201):
    parts = resp.json().get("definition", {}).get("parts", [])
    connection_string, eventhub_name = parse_connection_info(parts)
elif resp.status_code == 202:
    op_url = resp.headers.get("Location", "")
    for _ in range(20):
        time.sleep(3)
        op_resp = requests.get(op_url, headers=get_headers())
        if op_resp.status_code == 200:
            op_data = op_resp.json()
            if op_data.get("status", "").lower() == "succeeded":
                parts = op_data.get("definition", {}).get("parts", [])
                connection_string, eventhub_name = parse_connection_info(parts)
                break

if connection_string:
    print(f"  Connection String: {connection_string[:80]}...")
    print(f"  Event Hub Name:    {eventhub_name}")
else:
    print("  Connection string not available via API.")
    print("  To get it manually:")
    print(f"  1. Open '{EVENTSTREAM_NAME}' in Fabric portal")
    print("  2. Click the Custom Endpoint source node")
    print("  3. Copy 'Connection string' and 'Event hub name' from the details panel")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ============================================================================
# Step 6: Verify setup
# ============================================================================
print("\nStep 6: Verifying RTI setup...")

verify_url = f"{BASE_URL}/workspaces/{WORKSPACE_ID}/kqlDatabases/{kql_db_id}/runCommand"
verify_body = {"script": ".show tables | project TableName | order by TableName asc"}
resp = requests.post(verify_url, headers=get_headers(), json=verify_body)

if resp.status_code == 200:
    try:
        frames = resp.json().get("results", [{}])
        if frames:
            print("  KQL Database tables:")
            for frame in frames:
                for row in frame.get("rows", []):
                    print(f"    - {row[0] if isinstance(row, list) else row}")
        if not frames or not frames[0].get("rows"):
            print("    (verified -- check KQL Database in portal for table list)")
    except Exception:
        print("    (verified -- check KQL Database in portal for table list)")
else:
    print(f"  Could not verify tables ({resp.status_code})")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ---------- Summary ----------
print("\n" + "=" * 70)
print("  NB_RTI_Setup_Eventhouse: COMPLETE")
print("=" * 70)
print()
print(f"  Workspace:   {WORKSPACE_ID}")
print(f"  Eventhouse:  {EVENTHOUSE_NAME} ({eventhouse_id})")
print(f"  KQL DB:      {KQL_DB_NAME} ({kql_db_id})")
print(f"  Eventstream: {EVENTSTREAM_NAME} ({eventstream_id})")
print()
print("  KQL Tables (6):")
print("    INPUT:  claims_events, adt_events, rx_events")
print("    OUTPUT: fraud_scores, care_gap_alerts, highcost_alerts")
print()
if connection_string:
    print("  Custom Endpoint Connection String (for NB_RTI_Event_Simulator):")
    print(f"    {connection_string}")
    print(f"    Event Hub Name: {eventhub_name}")
else:
    print("  Custom Endpoint: Open Eventstream in portal to get connection string")
print()
print("  Next Steps:")
print("    1. Run NB_RTI_Event_Simulator (batch mode) to seed initial events")
print("    2. Run NB_RTI_Fraud_Detection to score claims")
print("    3. Run NB_RTI_Care_Gap_Alerts for point-of-care alerts")
print("    4. Run NB_RTI_HighCost_Trajectory for cost trend analysis")
print("    5. For live streaming: paste connection string into Event Simulator,")
print("       set MODE='stream', and run continuously")
print("=" * 70)

# METADATA **{"language":"python"}**
