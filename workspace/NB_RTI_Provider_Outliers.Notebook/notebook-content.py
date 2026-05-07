# Fabric notebook source

# METADATA **{"language":"markdown"}**

# MARKDOWN **{"language":"markdown"}**

# # RTI Use Case 5: Provider Outlier Scoring (Phase 4c)
#
# Real-time provider-level outlier detection across four signals:
# - **over_prescriber**     -- Controlled-substance Rx volume above peer mean + 2σ within rolling window
# - **denial_spike**        -- Claim denial rate above 25% within rolling window
# - **fraud_threshold**     -- Avg fraud_score >= 0.30 (HIGH/CRITICAL) within rolling window
# - **network_leakage**     -- Out-of-network claim share above 40% within rolling window
#
# **Inputs:** `claims_events`, `rx_events`, `fraud_scores` (KQL direct query)
# **Outputs:** `provider_alerts` (KQL streaming) + `rti_provider_alerts` (Delta)
#
# **Default lakehouse:** `lh_gold_curated`

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

print("NB_RTI_Provider_Outliers: Starting...")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ---------- Attach default lakehouse (self-healing) ----------
import requests as _req
_ws_id = notebookutils.runtime.context.get("currentWorkspaceId", "")
_tok = notebookutils.credentials.getToken("pbi")
_hdr = {"Authorization": f"Bearer {_tok}"}
_lh_resp = _req.get(f"https://api.fabric.microsoft.com/v1/workspaces/{_ws_id}/lakehouses", headers=_hdr)
if _lh_resp.status_code == 200:
    for _lh in _lh_resp.json().get("value", []):
        if _lh["displayName"] == "lh_gold_curated":
            _lh_id = _lh["id"]
            try:
                notebookutils.lakehouse.setDefaultLakehouse(_ws_id, _lh_id)
                print(f"  Attached lh_gold_curated ({_lh_id[:8]}...)")
            except Exception:
                import re as _re_mod
                _abfss = f"abfss://{_ws_id}@onelake.dfs.fabric.microsoft.com/{_lh_id}/Tables"
                _orig_sql = spark.sql
                def _patched_sql(query, _base=_abfss, _orig=_orig_sql):
                    query = _re_mod.sub(r'\blh_gold_curated\.(\w+)\b',
                                        lambda m: f'delta.`{_base}/{m.group(1)}`', query)
                    return _orig(query)
                spark.sql = _patched_sql
                from pyspark.sql import DataFrameWriter as _DFW
                _orig_sat = _DFW.saveAsTable
                def _patched_sat(self, name, _base=_abfss, _orig=_orig_sat, **kwargs):
                    if name.startswith('lh_gold_curated.'):
                        self.save(f'{_base}/{name.split(".",1)[1]}'); return
                    return _orig(self, name, **kwargs)
                _DFW.saveAsTable = _patched_sat
                print(f"  Registered lh_gold_curated via ABFSS rewriter ({_lh_id[:8]}...)")
            break
del _req, _ws_id, _tok, _hdr, _lh_resp

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

%pip install azure-kusto-data azure-kusto-ingest azure-core>=1.31.0 --quiet

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

import requests, json, uuid, io
from datetime import datetime, timezone
import time as _wait_time
import subprocess, sys
try:
    from azure.kusto.data import KustoConnectionStringBuilder, KustoClient, DataFormat
    from azure.kusto.ingest import ManagedStreamingIngestClient, IngestionProperties
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "azure-kusto-data", "azure-kusto-ingest"])
    for _m in list(sys.modules.keys()):
        if _m.startswith("azure"):
            del sys.modules[_m]
    from azure.kusto.data import KustoConnectionStringBuilder, KustoClient, DataFormat
    from azure.kusto.ingest import ManagedStreamingIngestClient, IngestionProperties

BASE_URL = "https://api.fabric.microsoft.com/v1"
WORKSPACE_ID = notebookutils.runtime.context.get("currentWorkspaceId", "")
KQL_DB_NAME = "Healthcare_RTI_DB"

def get_fabric_token():
    return notebookutils.credentials.getToken("https://analysis.windows.net/powerbi/api")

def get_kusto_token():
    return notebookutils.credentials.getToken("kusto")

# Discover Eventhouse URIs
KUSTO_QUERY_URI = ""
KUSTO_INGEST_URI = ""
_h = {"Authorization": f"Bearer {get_fabric_token()}", "Content-Type": "application/json"}
_resp = requests.get(f"{BASE_URL}/workspaces/{WORKSPACE_ID}/items?type=Eventhouse", headers=_h)
if _resp.status_code == 200:
    for _item in _resp.json().get("value", []):
        if "Healthcare" in _item.get("displayName", ""):
            _props_resp = requests.get(
                f"{BASE_URL}/workspaces/{WORKSPACE_ID}/eventhouses/{_item['id']}", headers=_h
            )
            if _props_resp.status_code == 200:
                _props = _props_resp.json().get("properties", _props_resp.json())
                KUSTO_QUERY_URI = _props.get("queryServiceUri", "")
                KUSTO_INGEST_URI = _props.get("ingestionServiceUri", "")
                if not KUSTO_INGEST_URI and KUSTO_QUERY_URI:
                    KUSTO_INGEST_URI = KUSTO_QUERY_URI.replace("https://", "https://ingest-")
            break

if not KUSTO_QUERY_URI:
    raise RuntimeError("Healthcare_RTI_Eventhouse not found. Run NB_RTI_Setup_Eventhouse first.")

_kcsb_q = KustoConnectionStringBuilder.with_aad_user_token_authentication(KUSTO_QUERY_URI, get_kusto_token())
_kclient = KustoClient(_kcsb_q)

def _kql(query):
    _kc = KustoClient(KustoConnectionStringBuilder.with_aad_user_token_authentication(KUSTO_QUERY_URI, get_kusto_token()))
    r = _kc.execute(KQL_DB_NAME, query)
    p = r.primary_results[0] if r.primary_results else None
    if not p:
        return [], []
    return [c.column_name for c in p.columns], [[v for v in row] for row in p]

print(f"Eventhouse query URI: {KUSTO_QUERY_URI[:60]}...")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ============================================================================
# Outlier scoring -- compute four metrics per provider over last 4 hours
# ============================================================================
WINDOW = "4h"
THRESH_DENIAL = 0.25
THRESH_FRAUD = 0.30
THRESH_OON = 0.40
RX_PEER_SIGMA = 2.0

print(f"Computing provider outliers over last {WINDOW}...")

# 1. Denial-rate spike (per provider, per specialty)
denial_q = f"""
claims_events
| where event_timestamp > ago({WINDOW})
| summarize total = count(), denied = countif(is_denied == true),
            specialty = any(provider_specialty),
            lat = any(latitude), lon = any(longitude)
            by provider_id
| where total >= 5
| extend metric_value = todouble(denied) / todouble(total)
| where metric_value > {THRESH_DENIAL}
| project provider_id, provider_specialty=specialty, metric_value, threshold={THRESH_DENIAL},
          alert_type='denial_spike', latitude=lat, longitude=lon
"""

# 2. Network leakage (out-of-network share)
leak_q = f"""
claims_events
| where event_timestamp > ago({WINDOW})
| summarize total = count(), oon = countif(provider_network_status == 'out_of_network'),
            specialty = any(provider_specialty),
            lat = any(latitude), lon = any(longitude)
            by provider_id
| where total >= 5
| extend metric_value = todouble(oon) / todouble(total)
| where metric_value > {THRESH_OON}
| project provider_id, provider_specialty=specialty, metric_value, threshold={THRESH_OON},
          alert_type='network_leakage', latitude=lat, longitude=lon
"""

# 3. Fraud threshold (avg fraud_score in window)
fraud_q = f"""
fraud_scores
| where score_timestamp > ago({WINDOW})
| summarize metric_value = avg(fraud_score),
            specialty = any(provider_specialty),
            lat = any(latitude), lon = any(longitude),
            n = count()
            by provider_id
| where n >= 3 and metric_value >= {THRESH_FRAUD}
| project provider_id, provider_specialty=specialty, metric_value, threshold={THRESH_FRAUD},
          alert_type='fraud_threshold', latitude=lat, longitude=lon
"""

# 4. Over-prescriber (controlled-substance Rx volume vs peer mean + 2σ within specialty)
overrx_q = f"""
let peer_stats =
    rx_events
    | where event_timestamp > ago({WINDOW}) and is_controlled_substance == true
    | summarize cs_count = count() by provider_id, provider_specialty
    | summarize peer_mean = avg(cs_count), peer_std = stdev(cs_count) by provider_specialty;
rx_events
| where event_timestamp > ago({WINDOW}) and is_controlled_substance == true
| summarize cs_count = count(), lat = any(latitude), lon = any(longitude)
            by provider_id, provider_specialty
| join kind=inner peer_stats on provider_specialty
| where cs_count > peer_mean + ({RX_PEER_SIGMA} * coalesce(peer_std, 1.0)) and cs_count >= 5
| extend metric_value = todouble(cs_count), threshold = peer_mean + ({RX_PEER_SIGMA} * coalesce(peer_std, 1.0))
| project provider_id, provider_specialty, metric_value, threshold,
          alert_type='over_prescriber', latitude=lat, longitude=lon
"""

alerts = []
now = datetime.now(timezone.utc)

for label, q in [("denial_spike", denial_q), ("network_leakage", leak_q),
                 ("fraud_threshold", fraud_q), ("over_prescriber", overrx_q)]:
    try:
        cols, rows = _kql(q)
        idx = {c: i for i, c in enumerate(cols)}
        for r in rows:
            mv = float(r[idx["metric_value"]]) if r[idx["metric_value"]] is not None else 0.0
            th = float(r[idx["threshold"]]) if r[idx["threshold"]] is not None else 0.0
            spec = r[idx["provider_specialty"]] or ""
            pid = r[idx["provider_id"]] or ""
            lat = r[idx.get("latitude", -1)] if "latitude" in idx else None
            lon = r[idx.get("longitude", -1)] if "longitude" in idx else None
            # Priority: high if metric exceeds threshold by 50%+, else medium
            ratio = (mv / th) if th > 0 else 1.0
            priority = "HIGH" if ratio >= 1.5 else "MEDIUM"
            text = f"{label.replace('_',' ').title()}: provider {pid} ({spec}) -- metric={mv:.3f}, threshold={th:.3f}"
            alerts.append({
                "alert_id": str(uuid.uuid4()),
                "alert_timestamp": now.isoformat(),
                "provider_id": pid,
                "provider_specialty": spec,
                "alert_type": label,
                "metric_value": round(mv, 4),
                "threshold": round(th, 4),
                "time_window": WINDOW,
                "priority": priority,
                "alert_text": text,
                "latitude": float(lat) if lat is not None else 0.0,
                "longitude": float(lon) if lon is not None else 0.0,
            })
        print(f"  {label}: {len(rows)} alerts")
    except Exception as e:
        print(f"  WARN {label}: {e}")

print(f"Total provider alerts: {len(alerts)}")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ============================================================================
# Persist to Delta (rti_provider_alerts) + KQL (provider_alerts streaming)
# ============================================================================
if alerts:
    import pandas as _pd
    df_alerts = _pd.DataFrame(alerts)
    sdf = spark.createDataFrame(df_alerts)
    sdf.write.format("delta").mode("append").saveAsTable("lh_gold_curated.rti_provider_alerts")
    print(f"Delta: {len(alerts)} alerts written to lh_gold_curated.rti_provider_alerts")

    # Push to KQL provider_alerts table
    try:
        _engine_kcsb = KustoConnectionStringBuilder.with_aad_user_token_authentication(KUSTO_QUERY_URI, get_kusto_token())
        _dm_kcsb = KustoConnectionStringBuilder.with_aad_user_token_authentication(KUSTO_INGEST_URI, get_kusto_token())

        _mgmt = KustoClient(_engine_kcsb)
        _cmds = [
            """.create-merge table provider_alerts (alert_id:string,alert_timestamp:datetime,provider_id:string,provider_specialty:string,alert_type:string,metric_value:real,threshold:real,time_window:string,priority:string,alert_text:string,latitude:real,longitude:real)""",
            """.alter table provider_alerts policy streamingingestion enable""",
            """.create-or-alter table provider_alerts ingestion json mapping 'provider_alerts_mapping' '[{"column":"alert_id","path":"$.alert_id","datatype":"string"},{"column":"alert_timestamp","path":"$.alert_timestamp","datatype":"datetime"},{"column":"provider_id","path":"$.provider_id","datatype":"string"},{"column":"provider_specialty","path":"$.provider_specialty","datatype":"string"},{"column":"alert_type","path":"$.alert_type","datatype":"string"},{"column":"metric_value","path":"$.metric_value","datatype":"real"},{"column":"threshold","path":"$.threshold","datatype":"real"},{"column":"time_window","path":"$.time_window","datatype":"string"},{"column":"priority","path":"$.priority","datatype":"string"},{"column":"alert_text","path":"$.alert_text","datatype":"string"},{"column":"latitude","path":"$.latitude","datatype":"real"},{"column":"longitude","path":"$.longitude","datatype":"real"}]'""",
        ]
        for _c in _cmds:
            try:
                _mgmt.execute_mgmt(KQL_DB_NAME, _c.strip())
            except Exception as _me:
                print(f"  KQL WARN: {_me}")

        _wait_time.sleep(8)
        _client = ManagedStreamingIngestClient(_engine_kcsb, _dm_kcsb)
        _props = IngestionProperties(database=KQL_DB_NAME, table="provider_alerts",
                                     data_format=DataFormat.JSON,
                                     ingestion_mapping_reference="provider_alerts_mapping")
        _json = df_alerts.to_json(orient="records", lines=True, date_format="iso")
        _client.ingest_from_stream(io.StringIO(_json), ingestion_properties=_props)
        print(f"KQL: {len(alerts)} alerts streamed -> provider_alerts")
    except Exception as e:
        print(f"KQL WARN: provider_alerts ingestion failed: {e}")
else:
    print("No outliers detected in window -- nothing to write.")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ============================================================================
# Summary
# ============================================================================
if alerts:
    import pandas as _pd
    _df = _pd.DataFrame(alerts)
    print("\n" + "=" * 60)
    print("PROVIDER OUTLIER ALERTS")
    print("=" * 60)
    print(_df.groupby(["alert_type", "priority"]).size().reset_index(name="count").to_string(index=False))
    print("\nTop 10 by metric_value:")
    print(_df.nlargest(10, "metric_value")[["provider_id","provider_specialty","alert_type","metric_value","threshold","priority"]].to_string(index=False))

print("NB_RTI_Provider_Outliers: COMPLETE")
print("=" * 60)

# METADATA **{"language":"python"}**
