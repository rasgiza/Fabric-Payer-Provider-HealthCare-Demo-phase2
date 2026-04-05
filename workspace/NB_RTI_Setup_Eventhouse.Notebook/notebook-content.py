# Fabric notebook source

# METADATA **{"language":"markdown"}**

# MARKDOWN **{"language":"markdown"}**

# # RTI Setup: Eventhouse + KQL Database + Eventstream
# 
# Creates the **Real-Time Intelligence** infrastructure for 3 use cases:
# 1. Eventhouse item
# 2. KQL Database with tables for claims, ADT, Rx events, and scoring outputs
# 3. Eventstream with Custom App source → KQL DB destination
# 
# **Run once** before the scoring notebooks. Requires workspace admin or contributor role.
# 
# **Default lakehouse:** `lh_gold_curated` (for reading reference data)

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ============================================================================
# NB_RTI_Setup_Eventhouse
# ============================================================================
# Creates Fabric RTI infrastructure via REST API:
#   - Eventhouse with KQL Database
#   - Eventstream (Custom App source → Eventhouse destination)
#   - KQL tables matching event schemas
#
# Requires: Fabric workspace contributor+ permissions
# Default lakehouse: lh_gold_curated
# ============================================================================

print("NB_RTI_Setup_Eventhouse: Starting...")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ---------- Parameters ----------
WORKSPACE_ID = ""        # Set by launcher or manually
EVENTHOUSE_NAME = "Healthcare_RTI_Eventhouse"
KQL_DB_NAME = "Healthcare_RTI_DB"
EVENTSTREAM_NAME = "Healthcare_RTI_Eventstream"

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

import requests
import json
import time

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
    # Auto-detect from notebook context
    WORKSPACE_ID = notebookutils.runtime.context.get("currentWorkspaceId", "")
    if not WORKSPACE_ID:
        raise ValueError("WORKSPACE_ID must be set — could not auto-detect from context")

print(f"Workspace ID: {WORKSPACE_ID}")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ============================================================================
# Step 1: Create Eventhouse
# ============================================================================
def create_or_get_item(item_type, display_name, description=""):
    """Create a Fabric item or return existing one."""
    url = f"{BASE_URL}/workspaces/{WORKSPACE_ID}/items"

    # Check if item already exists
    resp = requests.get(url, headers=get_headers())
    resp.raise_for_status()
    for item in resp.json().get("value", []):
        if item["displayName"] == display_name and item["type"] == item_type:
            print(f"  {item_type} '{display_name}' already exists: {item['id']}")
            return item["id"]

    # Create new item
    body = {
        "displayName": display_name,
        "type": item_type,
        "description": description
    }
    resp = requests.post(url, headers=get_headers(), json=body)

    if resp.status_code == 202:
        # Long-running operation
        op_url = resp.headers.get("Location", "")
        print(f"  Creating {item_type} '{display_name}' (async)...")
        for _ in range(60):
            time.sleep(5)
            op_resp = requests.get(op_url, headers=get_headers())
            if op_resp.status_code == 200:
                op_data = op_resp.json()
                if op_data.get("status") in ("Succeeded", "succeeded"):
                    item_id = op_data.get("resourceId") or op_data.get("id", "")
                    print(f"  Created: {item_id}")
                    return item_id
                elif op_data.get("status") in ("Failed", "failed"):
                    raise RuntimeError(f"Failed to create {item_type}: {op_data}")
        raise TimeoutError(f"Timed out creating {item_type}")
    elif resp.status_code == 201:
        item_id = resp.json().get("id", "")
        print(f"  Created {item_type} '{display_name}': {item_id}")
        return item_id
    else:
        resp.raise_for_status()

print("Step 1: Creating Eventhouse...")
eventhouse_id = create_or_get_item("Eventhouse", EVENTHOUSE_NAME, "Healthcare RTI — real-time scoring and alerting")
print(f"  Eventhouse ID: {eventhouse_id}")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ============================================================================
# Step 2: Get or Create KQL Database
# ============================================================================
print("Step 2: Getting KQL Database...")

# Eventhouse auto-creates a KQL database with the same name
# List KQL databases and find the one linked to our Eventhouse
url = f"{BASE_URL}/workspaces/{WORKSPACE_ID}/items?type=KQLDatabase"
resp = requests.get(url, headers=get_headers())
resp.raise_for_status()

kql_db_id = None
for item in resp.json().get("value", []):
    if EVENTHOUSE_NAME in item["displayName"] or KQL_DB_NAME == item["displayName"]:
        kql_db_id = item["id"]
        print(f"  Found KQL Database: {item['displayName']} ({kql_db_id})")
        break

if not kql_db_id:
    print(f"  KQL Database not found — it may take a moment after Eventhouse creation")
    print(f"  Check the Fabric portal for the Eventhouse '{EVENTHOUSE_NAME}'")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ============================================================================
# Step 3: Create KQL Tables
# ============================================================================
print("Step 3: Creating KQL tables...")

KQL_TABLE_COMMANDS = [
    # Claims events table (fraud detection input)
    """
    .create-merge table claims_events (
        event_id: string,
        event_timestamp: datetime,
        event_type: string,
        claim_id: string,
        patient_id: string,
        provider_id: string,
        facility_id: string,
        payer_id: string,
        diagnosis_code: string,
        procedure_code: string,
        claim_type: string,
        claim_amount: real,
        latitude: real,
        longitude: real,
        injected_fraud_flags: string
    )
    """,

    # ADT events table (care gap closure input)
    """
    .create-merge table adt_events (
        event_id: string,
        event_timestamp: datetime,
        event_type: string,
        patient_id: string,
        facility_id: string,
        facility_name: string,
        admission_type: string,
        primary_diagnosis: string,
        latitude: real,
        longitude: real,
        has_open_care_gaps: bool,
        open_gap_measures: string
    )
    """,

    # Prescription events table
    """
    .create-merge table rx_events (
        event_id: string,
        event_timestamp: datetime,
        event_type: string,
        patient_id: string,
        provider_id: string,
        medication_code: string,
        medication_name: string,
        drug_class: string,
        quantity: int,
        days_supply: int,
        latitude: real,
        longitude: real
    )
    """,

    # Fraud scores output table
    """
    .create-merge table fraud_scores (
        score_id: string,
        score_timestamp: datetime,
        claim_id: string,
        patient_id: string,
        provider_id: string,
        fraud_score: real,
        fraud_flags: string,
        risk_tier: string,
        latitude: real,
        longitude: real
    )
    """,

    # Care gap alerts output table
    """
    .create-merge table care_gap_alerts (
        alert_id: string,
        alert_timestamp: datetime,
        patient_id: string,
        facility_id: string,
        measure_id: string,
        measure_name: string,
        gap_days_overdue: int,
        alert_priority: string,
        alert_text: string,
        latitude: real,
        longitude: real
    )
    """,

    # High-cost trajectory output table
    """
    .create-merge table highcost_alerts (
        alert_id: string,
        alert_timestamp: datetime,
        patient_id: string,
        rolling_spend_30d: real,
        rolling_spend_90d: real,
        ed_visits_30d: int,
        readmission_flag: bool,
        risk_tier: string,
        cost_trend: string,
        latitude: real,
        longitude: real
    )
    """,

    # Enable streaming ingestion on event tables
    ".alter table claims_events policy streamingingestion enable",
    ".alter table adt_events policy streamingingestion enable",
    ".alter table rx_events policy streamingingestion enable",
]

if kql_db_id:
    # Execute KQL commands via the query API
    kql_url = f"{BASE_URL}/workspaces/{WORKSPACE_ID}/kqlDatabases/{kql_db_id}/executeCommand"

    for cmd in KQL_TABLE_COMMANDS:
        cmd_clean = cmd.strip()
        if not cmd_clean:
            continue
        body = {"command": cmd_clean}
        resp = requests.post(kql_url, headers=get_headers(), json=body)
        if resp.status_code in (200, 201):
            table_name = cmd_clean.split("table ")[-1].split(" ")[0].split("(")[0].strip() if "table" in cmd_clean else cmd_clean[:50]
            print(f"  OK: {table_name}")
        else:
            print(f"  WARN: {resp.status_code} — {resp.text[:200]}")
else:
    print("  Skipping KQL table creation — no KQL Database ID found")
    print("  Run this cell again after the Eventhouse is fully provisioned")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ============================================================================
# Step 4: Create Eventstream
# ============================================================================
print("Step 4: Creating Eventstream...")

eventstream_id = create_or_get_item(
    "Eventstream",
    EVENTSTREAM_NAME,
    "Healthcare RTI — ingests claims, ADT, and Rx events"
)
print(f"  Eventstream ID: {eventstream_id}")

print("""
  MANUAL STEP REQUIRED:
  1. Open the Eventstream in the Fabric portal
  2. Add a 'Custom App' source (this generates the connection string)
  3. Add a 'KQL Database' destination pointing to '{kql_db}'
  4. Map the event fields to the KQL table columns
  5. Copy the Custom App connection string to NB_RTI_Event_Simulator
""".format(kql_db=KQL_DB_NAME))

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ---------- Summary ----------
print("\n" + "=" * 60)
print("NB_RTI_Setup_Eventhouse: COMPLETE")
print("=" * 60)
print(f"  Eventhouse:  {EVENTHOUSE_NAME} ({eventhouse_id})")
print(f"  KQL DB:      {KQL_DB_NAME} ({kql_db_id or 'pending'})")
print(f"  Eventstream: {EVENTSTREAM_NAME} ({eventstream_id})")
print()
print("KQL Tables Created:")
print("  INPUT:  claims_events, adt_events, rx_events")
print("  OUTPUT: fraud_scores, care_gap_alerts, highcost_alerts")
print()
print("Next: Run NB_RTI_Event_Simulator to generate events")
print("=" * 60)

# METADATA **{"language":"python"}**
