# Fabric notebook source

# METADATA **{"language":"markdown"}**

# MARKDOWN **{"language":"markdown"}**

# # RTI Use Case 4: Operations Agent — Unified Triage, Monitoring & Action
# 
# **Status: FUTURE — Architecture stub for next phase**
# 
# The Operations Agent is an AI-powered operational intelligence layer that sits on top
# of the three RTI scoring outputs (fraud, care gaps, high-cost trajectory) and provides:
# 
# ### 1. Unified Alert Triage
# - Reads `fraud_scores`, `care_gap_alerts`, and `highcost_alerts` from the KQL Database
# - Ranks and deduplicates across alert types (a patient can trigger all 3 simultaneously)
# - Produces a **priority-ordered worklist** for ops teams — one queue, not three
# 
# ### 2. SLA & Throughput Monitoring
# - Tracks data freshness: how stale are the KQL input tables? (claims, ADT, Rx)
# - Monitors pipeline SLA: did the batch ETL complete within its window?
# - Measures alert-to-action latency: how long between event ingestion and alert generation?
# - Fires **operational alerts** when thresholds breach (e.g., no claims events in 15 min)
# 
# ### 3. Automated Remediation & Action Routing
# - **Fraud → SIU queue:** CRITICAL fraud scores auto-routed to Special Investigations Unit
# - **Care gaps → provider fax/EHR alert:** CRITICAL gap alerts sent to facility care team
# - **High-cost → care management referral:** CRITICAL trajectory alerts trigger CM assignment
# - Uses Fabric Reflex / Data Activator for event-driven action triggers
# 
# ### Architecture
# ```
# ┌─────────────────────────────────────────────────────────────────────┐
# │  Operations Agent                                                  │
# │                                                                    │
# │  ┌──────────────┐   ┌──────────────┐   ┌──────────────────────┐   │
# │  │ KQL Database  │──▶│ Alert Triage │──▶│ Priority Worklist    │   │
# │  │ (3 scoring    │   │ (deduplicate │   │ (single pane of     │   │
# │  │  outputs)     │   │  + rank)     │   │  glass for ops)     │   │
# │  └──────────────┘   └──────────────┘   └──────────────────────┘   │
# │                                                                    │
# │  ┌──────────────┐   ┌──────────────┐   ┌──────────────────────┐   │
# │  │ SLA Monitor   │──▶│ Freshness    │──▶│ Ops Alerts           │   │
# │  │ (data age,    │   │ Checks +     │   │ (Teams, Email,       │   │
# │  │  pipeline)    │   │ Thresholds   │   │  Data Activator)     │   │
# │  └──────────────┘   └──────────────┘   └──────────────────────┘   │
# │                                                                    │
# │  ┌──────────────┐   ┌──────────────┐   ┌──────────────────────┐   │
# │  │ Action Router │──▶│ Foundry/     │──▶│ Downstream Systems   │   │
# │  │ (rule-based + │   │ Reflex       │   │ (SIU, EHR, CM,      │   │
# │  │  AI-assisted) │   │ Triggers     │   │  Payer portals)      │   │
# │  └──────────────┘   └──────────────┘   └──────────────────────┘   │
# └─────────────────────────────────────────────────────────────────────┘
# ```
# 
# ### Integration Points
# - **Azure AI Foundry Agent:** Natural language interface for ops teams
#   ("What are today's top 10 priorities across all alert types?")
# - **Fabric Data Activator (Reflex):** Event-driven triggers for automated routing
# - **Microsoft Teams:** Alert notifications via webhook or Power Automate
# - **Power BI Real-Time Dashboard:** Operational KPI tiles (alert volume, SLA, freshness)
# 
# ### KQL Queries (Planned)
# The agent will execute these KQL queries against the Eventhouse:
# - Unified worklist: merge + rank across all 3 alert tables
# - Data freshness: `max(event_timestamp)` per input table vs `now()`
# - Alert velocity: alerts per minute/hour trend (detect surges)
# - Action completion: track which alerts have been routed/resolved
# 
# **Default lakehouse:** `lh_gold_curated`

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ============================================================================
# NB_RTI_Operations_Agent — Unified Triage, Monitoring & Action
# ============================================================================
# STATUS: FUTURE — Architecture stub
#
# This notebook will be the operational intelligence layer combining:
#   - Alert triage across fraud, care gap, and high-cost outputs
#   - SLA monitoring for data freshness and pipeline health
#   - Automated action routing via Reflex / Data Activator
#
# Default lakehouse: lh_gold_curated
# ============================================================================

print("NB_RTI_Operations_Agent: Architecture stub — implementation planned")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ---------- Parameters ----------
WORKSPACE_ID = ""                               # Auto-detected if blank
EVENTHOUSE_NAME = "Healthcare_RTI_Eventhouse"
KQL_DB_NAME = "Healthcare_RTI_DB"

# SLA thresholds (minutes)
FRESHNESS_WARN_MIN = 15     # Warn if no events in 15 min
FRESHNESS_CRIT_MIN = 60     # Critical if no events in 1 hour
PIPELINE_SLA_HOURS = 4      # Batch pipeline must complete within 4 hours

# Action routing
ENABLE_TEAMS_WEBHOOK = False
TEAMS_WEBHOOK_URL = ""      # Microsoft Teams incoming webhook URL
ENABLE_REFLEX = False       # Requires Data Activator setup

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

import requests
import json
import time
from datetime import datetime, timedelta

def get_fabric_token():
    return notebookutils.credentials.getToken("https://analysis.windows.net/powerbi/api")

def get_headers():
    return {
        "Authorization": f"Bearer {get_fabric_token()}",
        "Content-Type": "application/json"
    }

BASE_URL = "https://api.fabric.microsoft.com/v1"

if not WORKSPACE_ID:
    WORKSPACE_ID = notebookutils.runtime.context.get("currentWorkspaceId", "")

print(f"Workspace ID: {WORKSPACE_ID}")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ============================================================================
# Module 1: Unified Alert Triage
# ============================================================================
# Reads all 3 scoring output tables and produces a single priority worklist.
# Deduplicates patients who appear in multiple alert streams.
# ============================================================================

print("Module 1: Unified Alert Triage")
print("  [STUB] Will query fraud_scores, care_gap_alerts, highcost_alerts")
print("  [STUB] Will deduplicate by patient_id across alert types")
print("  [STUB] Will produce ranked worklist with composite priority score")

# --- Planned KQL ---
# UNIFIED_WORKLIST_KQL = """
# let fraud = fraud_scores
#     | where score_timestamp > ago(24h)
#     | project patient_id, alert_type="FRAUD", priority=case(
#         risk_tier=="CRITICAL", 1, risk_tier=="HIGH", 2, risk_tier=="MEDIUM", 3, 4),
#       detail=strcat("Fraud score: ", tostring(fraud_score), " | Flags: ", fraud_flags),
#       timestamp=score_timestamp, lat=latitude, lon=longitude;
# let gaps = care_gap_alerts
#     | where alert_timestamp > ago(24h)
#     | project patient_id, alert_type="CARE_GAP", priority=case(
#         alert_priority=="CRITICAL", 1, alert_priority=="HIGH", 2,
#         alert_priority=="MEDIUM", 3, 4),
#       detail=strcat("Gap: ", measure_name, " | Overdue: ", tostring(gap_days_overdue), "d"),
#       timestamp=alert_timestamp, lat=latitude, lon=longitude;
# let costs = highcost_alerts
#     | where alert_timestamp > ago(24h)
#     | project patient_id, alert_type="HIGH_COST", priority=case(
#         risk_tier=="CRITICAL", 1, risk_tier=="HIGH", 2, risk_tier=="MEDIUM", 3, 4),
#       detail=strcat("30d spend: $", tostring(rolling_spend_30d),
#         " | ED visits: ", tostring(ed_visits_30d), " | Trend: ", cost_trend),
#       timestamp=alert_timestamp, lat=latitude, lon=longitude;
# union fraud, gaps, costs
# | summarize alert_types=make_set(alert_type),
#            max_priority=min(priority),
#            alerts=make_list(pack("type", alert_type, "detail", detail, "time", timestamp)),
#            any(lat), any(lon)
#   by patient_id
# | order by max_priority asc, array_length(alert_types) desc
# | take 50
# """

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ============================================================================
# Module 2: SLA & Throughput Monitoring
# ============================================================================
# Checks data freshness across input tables and pipeline completion status.
# ============================================================================

print("Module 2: SLA & Throughput Monitoring")
print("  [STUB] Will query max(event_timestamp) per input table")
print("  [STUB] Will check pipeline run history via Fabric API")
print("  [STUB] Will compute alert-to-action latency metrics")

# --- Planned KQL ---
# DATA_FRESHNESS_KQL = """
# let claims_age = toscalar(claims_events | summarize max(event_timestamp));
# let adt_age = toscalar(adt_events | summarize max(event_timestamp));
# let rx_age = toscalar(rx_events | summarize max(event_timestamp));
# print claims_minutes_ago=datetime_diff('minute', now(), claims_age),
#       adt_minutes_ago=datetime_diff('minute', now(), adt_age),
#       rx_minutes_ago=datetime_diff('minute', now(), rx_age)
# """
#
# ALERT_VELOCITY_KQL = """
# union claims_events, adt_events, rx_events
# | where event_timestamp > ago(1h)
# | summarize event_count=count() by bin(event_timestamp, 5m), event_type
# | order by event_timestamp desc
# """

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ============================================================================
# Module 3: Action Routing
# ============================================================================
# Routes alerts to downstream systems based on alert type and priority.
#
# Planned integrations:
#   - CRITICAL fraud → SIU investigation queue
#   - CRITICAL care gap → Provider EHR/fax alert
#   - CRITICAL high-cost → Care management referral
#   - All CRITICAL → Microsoft Teams notification
#   - Fabric Data Activator (Reflex) for event-driven triggers
# ============================================================================

print("Module 3: Action Routing")
print("  [STUB] Will route CRITICAL fraud alerts to SIU queue")
print("  [STUB] Will route CRITICAL care gap alerts to provider care team")
print("  [STUB] Will route CRITICAL high-cost alerts to care management")
print("  [STUB] Will send Teams notifications for all CRITICAL alerts")

# --- Planned: Teams webhook notification ---
# def send_teams_alert(webhook_url, title, message, priority):
#     """Send an alert card to Microsoft Teams via incoming webhook."""
#     card = {
#         "@type": "MessageCard",
#         "themeColor": "FF0000" if priority == "CRITICAL" else "FFA500",
#         "summary": title,
#         "sections": [{
#             "activityTitle": f"🚨 {title}",
#             "text": message,
#             "facts": [
#                 {"name": "Priority", "value": priority},
#                 {"name": "Time", "value": datetime.utcnow().isoformat()},
#             ]
#         }]
#     }
#     resp = requests.post(webhook_url, json=card)
#     return resp.status_code == 200

# --- Planned: Reflex / Data Activator trigger ---
# def trigger_reflex_action(alert_type, patient_id, details):
#     """Trigger a Fabric Data Activator reflex for automated remediation."""
#     # Reflex items are configured in the Fabric portal with trigger conditions
#     # This function would invoke the Reflex API to create an event
#     pass

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ============================================================================
# Module 4: Foundry Agent Integration (Natural Language Ops)
# ============================================================================
# Enables ops teams to query the unified alert stream in natural language:
#   "What are today's top priorities?"
#   "Show me all CRITICAL fraud alerts from the last hour"
#   "Which facilities have the most care gap alerts?"
#   "Is the claims pipeline running on time?"
#
# Integration approach:
#   - Azure AI Foundry agent with KQL tool access
#   - Agent instructions reference the unified worklist KQL
#   - Agent can also query Gold lakehouse for historical context
# ============================================================================

print("Module 4: Foundry Agent Integration")
print("  [STUB] Will connect to Azure AI Foundry for natural language ops queries")
print("  [STUB] Will expose KQL queries as agent tools")
print("  [STUB] Will combine real-time alerts with historical Gold lakehouse context")

# --- Planned agent instructions excerpt ---
# AGENT_SYSTEM_PROMPT = """
# You are the Healthcare Operations Agent. You help ops teams monitor and triage
# real-time alerts across three domains:
#
# 1. FRAUD DETECTION — Claims flagged for velocity bursts, amount outliers,
#    geographic anomalies, or upcoding patterns
# 2. CARE GAP CLOSURE — Patients at facilities with open HEDIS care gaps
#    that could be closed during the current visit
# 3. HIGH-COST TRAJECTORY — Members with escalating 30/90-day spend,
#    ED superutilization, or readmission patterns
#
# You have access to:
# - KQL queries against Healthcare_RTI_DB (real-time scoring outputs)
# - SQL queries against lh_gold_curated (historical batch data)
# - Pipeline status API (data freshness and SLA monitoring)
#
# Always prioritize CRITICAL alerts first. When asked for a worklist,
# deduplicate patients across alert types and rank by composite severity.
# """

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ---------- Summary ----------
print("\n" + "=" * 70)
print("  NB_RTI_Operations_Agent: ARCHITECTURE STUB")
print("=" * 70)
print()
print("  This notebook outlines the future Operations Agent with 4 modules:")
print()
print("  Module 1: Unified Alert Triage")
print("    - Merge fraud + care gap + high-cost alerts into single worklist")
print("    - Deduplicate by patient, rank by composite priority")
print()
print("  Module 2: SLA & Throughput Monitoring")
print("    - Data freshness checks (claims, ADT, Rx input tables)")
print("    - Pipeline SLA tracking (batch ETL completion)")
print("    - Alert-to-action latency metrics")
print()
print("  Module 3: Action Routing")
print("    - CRITICAL fraud → SIU queue")
print("    - CRITICAL care gaps → Provider EHR/fax alerts")
print("    - CRITICAL high-cost → Care management referral")
print("    - All CRITICAL → Teams webhook notification")
print("    - Fabric Data Activator (Reflex) integration")
print()
print("  Module 4: Foundry Agent Integration")
print("    - Natural language ops: 'What are today's top 10 priorities?'")
print("    - KQL tool access for real-time + Gold SQL for historical context")
print()
print("  To implement: Replace [STUB] sections with live KQL queries,")
print("  configure Teams webhook, set up Reflex triggers, deploy Foundry agent.")
print("=" * 70)

# METADATA **{"language":"python"}**
