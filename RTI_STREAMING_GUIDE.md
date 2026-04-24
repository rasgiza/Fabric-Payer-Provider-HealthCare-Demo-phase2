# Real-Time Intelligence (RTI) Streaming Guide

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│  Cell 12 (Healthcare_Launcher) — ONE-TIME SETUP                    │
│  • Creates Eventhouse + KQL DB + Eventstream                       │
│  • Wires Eventstream topology (source → destinations) via API      │
│  • Prints 2 manual steps:                                          │
│    Step A: Enable OneLake Availability on KQL DB (portal)          │
│    Step B: Copy Eventstream connection string                      │
└──────────────────────────────┬──────────────────────────────────────┘
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Cell 13 (Healthcare_Launcher) — RUN THE PIPELINE                  │
│  1. Creates OneLake shortcuts in lh_gold_curated:                  │
│     rti_claims_events → KQL claims_events                          │
│     rti_adt_events    → KQL adt_events                             │
│     rti_rx_events     → KQL rx_events                              │
│  2. Triggers PL_Healthcare_RTI pipeline                            │
└──────────────────────────────┬──────────────────────────────────────┘
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│  PL_Healthcare_RTI Pipeline                                        │
│                                                                    │
│  ┌──────────────────────────────────┐                              │
│  │  NB_RTI_Event_Simulator          │                              │
│  │  • Reads dims from lh_gold_curated (patients, providers, etc)   │
│  │  • Generates 10 batches × 500 claims + 250 ADT + 166 Rx        │
│  │  • Pushes JSON events → Eventstream Custom Endpoint             │
│  │  • Each event tagged with _table field for routing              │
│  └──────────────┬───────────────────┘                              │
│                 ▼                                                   │
│  ┌──────────────────────────────────────────────────┐              │
│  │  Eventstream (Healthcare_RTI_Eventstream)        │              │
│  │  Routes by _table field:                         │              │
│  │  ├─→ Eventhouse/KQL: claims_events, adt_events,  │              │
│  │  │   rx_events tables (real-time, sub-second)    │              │
│  │  └─→ Activator/Reflex (alerts, optional)         │              │
│  └──────────────┬───────────────────────────────────┘              │
│                 ▼                                                   │
│  ╔══════════════════════════════════════════════════╗               │
│  ║  OneLake Availability (enabled on KQL DB)        ║               │
│  ║  KQL tables → Delta Parquet files in OneLake     ║               │
│  ║  (adaptive batching, ~5 min with mirroring policy)║              │
│  ╚══════════════╤═══════════════════════════════════╝               │
│                 ▼                                                   │
│  ╔══════════════════════════════════════════════════╗               │
│  ║  OneLake Shortcuts (in lh_gold_curated)          ║               │
│  ║  rti_claims_events → KQL claims_events           ║               │
│  ║  rti_adt_events    → KQL adt_events              ║               │
│  ║  rti_rx_events     → KQL rx_events               ║               │
│  ╚══════════════╤═══════════════════════════════════╝               │
│                 ▼                                                   │
│  ┌────────────────────┐ ┌────────────────────┐ ┌────────────────┐  │
│  │ NB_RTI_Fraud_      │ │ NB_RTI_Care_Gap_   │ │ NB_RTI_High    │  │
│  │ Detection          │ │ Alerts             │ │ Cost_Trajectory│  │
│  │ (parallel)         │ │ (parallel)         │ │ (parallel)     │  │
│  │                    │ │                    │ │                │  │
│  │ READ:              │ │ READ:              │ │ READ:          │  │
│  │ rti_claims_events  │ │ rti_adt_events     │ │ rti_claims     │  │
│  │ dim_provider       │ │ care_gaps          │ │ rti_adt        │  │
│  │ fact_claim (hist)  │ │ hedis_measures     │ │ dim_patient    │  │
│  │                    │ │                    │ │                │  │
│  │ WRITE:             │ │ WRITE:             │ │ WRITE:         │  │
│  │ rti_fraud_scores   │ │ rti_care_gap_alerts│ │ rti_highcost_  │  │
│  │ (Delta + KQL)      │ │ (Delta + KQL)      │ │ alerts         │  │
│  └────────────────────┘ └────────────────────┘ └────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

## Data Flow Summary

| Step | Source | Destination | Mechanism |
|------|--------|-------------|-----------|
| Simulator → Eventstream | Generated events | Eventstream Custom Endpoint | EventHub SDK (`azure-eventhub`) |
| Eventstream → KQL | Eventstream | `claims_events`, `adt_events`, `rx_events` KQL tables | Eventstream routing by `_table` field |
| KQL → OneLake | KQL tables | Delta Parquet files in OneLake | OneLake Availability (mirroring policy) |
| OneLake → lh_gold_curated | OneLake Delta files | `rti_claims_events`, `rti_adt_events`, `rti_rx_events` | OneLake Shortcuts (created by Cell 13) |
| Scoring notebooks READ | `lh_gold_curated.rti_*` | Spark DataFrames | `spark.table()` reads shortcut as Delta |
| Scoring notebooks WRITE | Scored results | `lh_gold_curated.rti_fraud_scores` (Delta) + `fraud_scores` (KQL) | `saveAsTable` + Kusto SDK ingestion |

## Setup Steps

### Prerequisites
- Healthcare_Launcher Cell 1–11 completed (lakehouses, notebooks, data deployed)
- `DEPLOY_STREAMING = True` in the Configuration cell

### Step 1: Run Cell 12 (RTI Deployment)
Cell 12 deploys all RTI artifacts:
- Eventhouse + KQL Database (from Git artifacts)
- Runs `NB_RTI_Setup_Eventhouse` (creates 6 KQL tables + streaming policies + mirroring policies)
- Creates and wires the Eventstream topology via API

### Step 2: Enable OneLake Availability (Portal — One-Time)
1. Open **Healthcare_RTI_DB** in the Fabric portal
2. In the **Database details** pane → **OneLake** section
3. Set **Availability** → **Enabled**
4. Check **"Apply to existing tables"** → confirm

This exposes KQL tables as Delta Parquet in OneLake. The mirroring policy (set by `NB_RTI_Setup_Eventhouse`) flushes data every 5 minutes.

> **Why can't this be automated?** OneLake Availability is a portal-only toggle — there is no public REST API or KQL command to enable it programmatically as of April 2026.

### Step 3: Copy the Eventstream Connection String
1. Open **Healthcare_RTI_Eventstream** in the Fabric portal
2. Click the **HealthcareCustomEndpoint** source node
3. Copy the **Connection String**

### Step 4: Run Cell 13 (Pipeline Trigger)
1. Paste the connection string into `ES_CONNECTION_STRING`
2. Run Cell 13

Cell 13 will:
1. **Create OneLake shortcuts** in `lh_gold_curated` pointing to the KQL tables
2. **Trigger PL_Healthcare_RTI** which runs:
   - `NB_RTI_Event_Simulator` — streams 10 batches of events
   - `NB_RTI_Fraud_Detection` — scores claims for fraud (parallel)
   - `NB_RTI_Care_Gap_Alerts` — checks ADT events against HEDIS gaps (parallel)
   - `NB_RTI_HighCost_Trajectory` — rolling cost analysis (parallel)

## Timing & Latency

| Component | Latency |
|-----------|---------|
| Simulator → Eventstream | ~1 second per batch |
| Eventstream → KQL tables | Sub-second (streaming ingestion) |
| KQL → OneLake (Delta) | ~5 minutes (mirroring policy `TargetLatencyInMinutes=5`) |
| OneLake → Shortcut read | Instant (same OneLake path) |

The scoring notebooks include **wait/retry logic** — they poll the shortcut tables every 30 seconds for up to 6 minutes, waiting for OneLake to flush the Delta files after the simulator completes.

## Artifacts

| Artifact | Type | Purpose |
|----------|------|---------|
| `Healthcare_RTI_Eventhouse` | Eventhouse | Hosts the KQL database |
| `Healthcare_RTI_DB` | KQL Database | 6 tables (3 input + 3 scored output) |
| `Healthcare_RTI_Eventstream` | Eventstream | Routes events from Custom Endpoint → KQL |
| `NB_RTI_Setup_Eventhouse` | Notebook | Creates KQL tables, streaming + mirroring policies |
| `NB_RTI_Event_Simulator` | Notebook | Generates streaming claims/ADT/Rx events |
| `NB_RTI_Fraud_Detection` | Notebook | Scores claims for fraud risk |
| `NB_RTI_Care_Gap_Alerts` | Notebook | Checks ADT events against open HEDIS gaps |
| `NB_RTI_HighCost_Trajectory` | Notebook | Rolling 30d/90d spend + ED visit analysis |
| `NB_RTI_Operations_Agent` | Notebook | AI agent for RTI operational queries |
| `PL_Healthcare_RTI` | Pipeline | Orchestrates Simulator → 3 scoring notebooks |
| `Healthcare RTI Dashboard` | KQL Dashboard | 4-page real-time dashboard (30s auto-refresh) |

## KQL Tables

### Input Tables (from Eventstream)
| Table | Events | Key Fields |
|-------|--------|------------|
| `claims_events` | Claims submissions | claim_id, patient_id, provider_id, claim_amount, injected_fraud_flags |
| `adt_events` | Admit/Discharge/Transfer | patient_id, facility_id, admission_type, has_open_care_gaps |
| `rx_events` | Prescription fills | patient_id, medication_code, drug_class, quantity |

### Output Tables (from Scoring Notebooks)
| Table | Scores | Key Fields |
|-------|--------|------------|
| `fraud_scores` | Fraud risk per claim | fraud_score (0-100), risk_tier, fraud_flags |
| `care_gap_alerts` | Care gap notifications | measure_id, gap_days_overdue, alert_priority |
| `highcost_alerts` | High-cost member flags | rolling_spend_30d/90d, ed_visits_30d, cost_trend |

## Troubleshooting

**Scoring notebooks fail with "table not found" or empty data:**
1. Verify OneLake Availability is **enabled** on Healthcare_RTI_DB (portal)
2. Verify the simulator ran and pushed events (check Eventstream metrics)
3. Verify shortcuts exist in lh_gold_curated (Tables tab should show `rti_claims_events`, `rti_adt_events`, `rti_rx_events`)
4. Wait 5 minutes — OneLake mirroring may not have flushed yet

**Eventstream connection string not working:**
1. Open Healthcare_RTI_Eventstream → click HealthcareCustomEndpoint
2. Ensure the source status is "Created" (not "Creating" or "Error")
3. Copy the full connection string including the EntityPath

**Pipeline times out:**
- The simulator runs 10 batches with 5-second intervals (~50 seconds)
- Scoring notebooks wait up to 6 minutes for OneLake sync
- Total expected time: ~8-10 minutes
