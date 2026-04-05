# Fabric notebook source

# METADATA **{"language":"markdown"}**

# MARKDOWN **{"language":"markdown"}**

# # RTI Event Simulator — Streaming Healthcare Events
# 
# Generates realistic real-time events for **3 RTI use cases**:
# 1. **Claims Fraud Detection** — Claims submissions with amounts, diagnoses, geo
# 2. **Care Gap Closure** — ADT admit/discharge events triggering gap checks
# 3. **High-Cost Member Trajectory** — Claims + ED visits for rolling cost analysis
# 
# **Modes:**
# - `batch` — Generate one batch and save as Delta tables in `lh_gold_curated`
# - `stream` — Continuously push events to Eventstream Custom App endpoint
# 
# **Default lakehouse:** `lh_gold_curated`

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ============================================================================
# NB_RTI_Event_Simulator
# ============================================================================
# Generates streaming healthcare events for 3 Fabric RTI use cases:
#   - Claims Fraud Detection (claims_events)
#   - Care Gap Closure (adt_events)
#   - High-Cost Member Trajectory (claims + ED events)
#
# Reads dimension/fact tables from lh_gold_curated, produces event batches.
# Default lakehouse: lh_gold_curated
# ============================================================================

print("NB_RTI_Event_Simulator: Starting...")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ---------- Parameters (override from pipeline or %run) ----------
MODE = "batch"           # "batch" = single batch to Delta | "stream" = continuous
BATCH_SIZE = 500         # events per batch
STREAM_INTERVAL_SEC = 5  # seconds between stream batches
EVENTSTREAM_CONN_STR = ""  # Eventstream Custom App connection string (stream mode)
EVENTHUB_NAME = ""         # Eventstream Custom App event hub name

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import uuid
import json

np.random.seed(None)  # Truly random for each run — not reproducible on purpose

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ---------- Load dimension/fact data from Gold lakehouse ----------
print("Loading dimension tables from lh_gold_curated...")

df_patients = spark.sql("SELECT patient_id, first_name, last_name, gender, date_of_birth, zip_code FROM lh_gold_curated.dim_patient WHERE is_current = true").toPandas()
df_providers = spark.sql("SELECT provider_id, provider_name, specialty, facility_id FROM lh_gold_curated.dim_provider WHERE is_current = true").toPandas()
df_facilities = spark.sql("SELECT facility_id, facility_name, facility_type, latitude, longitude FROM lh_gold_curated.dim_facility").toPandas()
df_diagnoses = spark.sql("SELECT DISTINCT diagnosis_code, diagnosis_description FROM lh_gold_curated.dim_diagnosis").toPandas()
df_medications = spark.sql("SELECT DISTINCT medication_code, medication_name, drug_class FROM lh_gold_curated.dim_medication").toPandas()
df_payers = spark.sql("SELECT payer_id, payer_name, plan_type FROM lh_gold_curated.dim_payer").toPandas()

# Load care gaps for Care Gap Closure use case
try:
    df_care_gaps = spark.sql("SELECT patient_id, measure_id, measure_name, is_gap_open, gap_days_overdue FROM lh_gold_curated.care_gaps WHERE is_gap_open = true").toPandas()
    print(f"  Loaded {len(df_care_gaps)} open care gaps")
except Exception:
    # Fall back to bronze if gold table not available
    df_care_gaps = pd.DataFrame(columns=["patient_id", "measure_id", "measure_name", "is_gap_open", "gap_days_overdue"])
    print("  No care_gaps table found — care gap alerts will be empty")

print(f"  Patients: {len(df_patients)}, Providers: {len(df_providers)}, Facilities: {len(df_facilities)}")
print(f"  Diagnoses: {len(df_diagnoses)}, Medications: {len(df_medications)}, Payers: {len(df_payers)}")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ---------- Reference data for realistic event generation ----------

CLAIM_TYPES = ["professional", "institutional", "pharmacy"]
ADT_EVENT_TYPES = ["ADMIT", "DISCHARGE", "TRANSFER", "OBSERVATION"]
ADMISSION_TYPES = ["EMERGENCY", "URGENT", "ELECTIVE", "NEWBORN"]

# Procedure code pools by specialty
PROCEDURE_CODES = {
    "Cardiology": ["99213", "99214", "93000", "93306", "93458"],
    "Orthopedics": ["99213", "99214", "27447", "27130", "29881"],
    "Oncology": ["99214", "99215", "96413", "77386", "88305"],
    "Primary Care": ["99213", "99214", "99215", "99396", "90471"],
    "Neurology": ["99213", "99214", "95819", "70553", "95910"],
    "Emergency Medicine": ["99281", "99282", "99283", "99284", "99285"],
    "Pediatrics": ["99213", "99214", "99392", "99393", "90471"],
    "Internal Medicine": ["99213", "99214", "99215", "99396", "80053"],
}

# Fraud patterns — injected at a controlled rate
FRAUD_PATTERNS = {
    "velocity": 0.03,       # 3% of providers submit bursts
    "geo_anomaly": 0.02,    # 2% of claims have patient far from provider
    "amount_outlier": 0.04, # 4% of claims are unusually high
    "upcoding": 0.03,       # 3% always use highest E&M code
}

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ============================================================================
# Event Generator Functions
# ============================================================================

def generate_claims_events(n: int) -> pd.DataFrame:
    """
    Generate n claims submission events.
    Includes controlled fraud pattern injection for fraud detection use case.
    """
    events = []
    now = datetime.utcnow()

    # Select a small set of "fraudulent" providers for velocity bursts
    fraud_velocity_providers = set(
        df_providers.sample(max(1, int(len(df_providers) * FRAUD_PATTERNS["velocity"])))["provider_id"]
    )

    for i in range(n):
        # Pick random patient, provider, payer
        patient = df_patients.sample(1).iloc[0]
        provider = df_providers.sample(1).iloc[0]
        payer = df_payers.sample(1).iloc[0]

        # Get facility geo
        fac = df_facilities[df_facilities["facility_id"] == provider["facility_id"]]
        lat = float(fac["latitude"].iloc[0]) if len(fac) > 0 and pd.notna(fac["latitude"].iloc[0]) else 42.96
        lon = float(fac["longitude"].iloc[0]) if len(fac) > 0 and pd.notna(fac["longitude"].iloc[0]) else -85.67

        # Diagnosis
        diag = df_diagnoses.sample(1).iloc[0] if len(df_diagnoses) > 0 else {"diagnosis_code": "Z00.00", "diagnosis_description": "General exam"}

        # Procedure code based on specialty
        specialty = provider.get("specialty", "Primary Care")
        proc_codes = PROCEDURE_CODES.get(specialty, PROCEDURE_CODES["Primary Care"])
        proc_code = random.choice(proc_codes)

        # Base claim amount by claim type
        claim_type = random.choice(CLAIM_TYPES)
        if claim_type == "institutional":
            base_amount = round(random.gauss(8500, 3000), 2)
        elif claim_type == "pharmacy":
            base_amount = round(random.gauss(250, 150), 2)
        else:
            base_amount = round(random.gauss(350, 200), 2)
        base_amount = max(25.0, base_amount)

        # --- Fraud pattern injection ---
        fraud_flags = []

        # Velocity burst: shift timestamps to cluster within 1 hour
        if provider["provider_id"] in fraud_velocity_providers and random.random() < 0.5:
            event_ts = now - timedelta(minutes=random.randint(0, 60))
            fraud_flags.append("velocity_burst")
        else:
            event_ts = now - timedelta(minutes=random.randint(0, 1440))

        # Amount outlier
        if random.random() < FRAUD_PATTERNS["amount_outlier"]:
            base_amount *= random.uniform(3.0, 8.0)
            fraud_flags.append("amount_outlier")

        # Upcoding — always use highest E&M
        if random.random() < FRAUD_PATTERNS["upcoding"]:
            proc_code = "99215"  # highest outpatient E&M
            fraud_flags.append("upcoding")

        # Geo anomaly — randomise lat/lon far from facility
        if random.random() < FRAUD_PATTERNS["geo_anomaly"]:
            lat += random.uniform(3, 8) * random.choice([-1, 1])
            lon += random.uniform(3, 8) * random.choice([-1, 1])
            fraud_flags.append("geo_anomaly")

        events.append({
            "event_id": str(uuid.uuid4()),
            "event_timestamp": event_ts.isoformat(),
            "event_type": "CLAIM_SUBMITTED",
            "claim_id": f"CLM-{uuid.uuid4().hex[:10].upper()}",
            "patient_id": patient["patient_id"],
            "provider_id": provider["provider_id"],
            "facility_id": provider.get("facility_id", ""),
            "payer_id": payer["payer_id"],
            "diagnosis_code": diag["diagnosis_code"] if isinstance(diag, dict) else diag.get("diagnosis_code", "Z00.00"),
            "procedure_code": proc_code,
            "claim_type": claim_type,
            "claim_amount": round(base_amount, 2),
            "latitude": round(lat, 6),
            "longitude": round(lon, 6),
            "injected_fraud_flags": "|".join(fraud_flags) if fraud_flags else "",
        })

    return pd.DataFrame(events)


def generate_adt_events(n: int) -> pd.DataFrame:
    """
    Generate n ADT (Admit/Discharge/Transfer) events.
    These drive the Care Gap Closure use case — when a patient arrives,
    check if they have open care gaps and alert the care team.
    """
    events = []
    now = datetime.utcnow()

    for i in range(n):
        patient = df_patients.sample(1).iloc[0]
        facility = df_facilities.sample(1).iloc[0]

        event_type = random.choices(
            ADT_EVENT_TYPES,
            weights=[0.35, 0.35, 0.15, 0.15],
            k=1
        )[0]

        admission_type = random.choices(
            ADMISSION_TYPES,
            weights=[0.30, 0.25, 0.35, 0.10],
            k=1
        )[0]

        diag = df_diagnoses.sample(1).iloc[0] if len(df_diagnoses) > 0 else {"diagnosis_code": "Z00.00", "diagnosis_description": "General exam"}

        lat = float(facility["latitude"]) if pd.notna(facility.get("latitude")) else 42.96
        lon = float(facility["longitude"]) if pd.notna(facility.get("longitude")) else -85.67

        # Check for open care gaps for this patient
        patient_gaps = df_care_gaps[df_care_gaps["patient_id"] == patient["patient_id"]]
        has_open_gaps = len(patient_gaps) > 0
        open_gap_measures = "|".join(patient_gaps["measure_id"].tolist()) if has_open_gaps else ""

        events.append({
            "event_id": str(uuid.uuid4()),
            "event_timestamp": (now - timedelta(minutes=random.randint(0, 1440))).isoformat(),
            "event_type": event_type,
            "patient_id": patient["patient_id"],
            "facility_id": facility["facility_id"],
            "facility_name": facility.get("facility_name", ""),
            "admission_type": admission_type,
            "primary_diagnosis": diag["diagnosis_code"] if isinstance(diag, dict) else diag.get("diagnosis_code", "Z00.00"),
            "latitude": round(lat, 6),
            "longitude": round(lon, 6),
            "has_open_care_gaps": has_open_gaps,
            "open_gap_measures": open_gap_measures,
        })

    return pd.DataFrame(events)


def generate_rx_events(n: int) -> pd.DataFrame:
    """
    Generate n prescription fill events.
    Supports medication adherence tracking in the High-Cost Trajectory use case.
    """
    events = []
    now = datetime.utcnow()

    for i in range(n):
        patient = df_patients.sample(1).iloc[0]
        provider = df_providers.sample(1).iloc[0]
        med = df_medications.sample(1).iloc[0] if len(df_medications) > 0 else {"medication_code": "RX0001", "medication_name": "Generic Med", "drug_class": "Other"}

        fac = df_facilities[df_facilities["facility_id"] == provider["facility_id"]]
        lat = float(fac["latitude"].iloc[0]) if len(fac) > 0 and pd.notna(fac["latitude"].iloc[0]) else 42.96
        lon = float(fac["longitude"].iloc[0]) if len(fac) > 0 and pd.notna(fac["longitude"].iloc[0]) else -85.67

        events.append({
            "event_id": str(uuid.uuid4()),
            "event_timestamp": (now - timedelta(minutes=random.randint(0, 1440))).isoformat(),
            "event_type": "RX_FILL",
            "patient_id": patient["patient_id"],
            "provider_id": provider["provider_id"],
            "medication_code": med["medication_code"] if isinstance(med, dict) else med.get("medication_code", "RX0001"),
            "medication_name": med["medication_name"] if isinstance(med, dict) else med.get("medication_name", "Generic"),
            "drug_class": med.get("drug_class", "Other") if isinstance(med, dict) else "Other",
            "quantity": random.choice([30, 60, 90]),
            "days_supply": random.choice([30, 60, 90]),
            "latitude": round(lat, 6),
            "longitude": round(lon, 6),
        })

    return pd.DataFrame(events)

print("Event generators ready.")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ============================================================================
# Batch Mode — Generate one batch, save as Delta tables
# ============================================================================
if MODE == "batch":
    print(f"Generating batch: {BATCH_SIZE} claims, {BATCH_SIZE // 2} ADT, {BATCH_SIZE // 3} Rx events...")

    claims_pdf = generate_claims_events(BATCH_SIZE)
    adt_pdf = generate_adt_events(BATCH_SIZE // 2)
    rx_pdf = generate_rx_events(BATCH_SIZE // 3)

    # Convert to Spark DataFrames and write as Delta
    claims_sdf = spark.createDataFrame(claims_pdf)
    adt_sdf = spark.createDataFrame(adt_pdf)
    rx_sdf = spark.createDataFrame(rx_pdf)

    claims_sdf.write.format("delta").mode("overwrite").saveAsTable("rti_claims_events")
    adt_sdf.write.format("delta").mode("overwrite").saveAsTable("rti_adt_events")
    rx_sdf.write.format("delta").mode("overwrite").saveAsTable("rti_rx_events")

    print(f"  rti_claims_events: {claims_sdf.count()} rows")
    print(f"  rti_adt_events:    {adt_sdf.count()} rows")
    print(f"  rti_rx_events:     {rx_sdf.count()} rows")
    print("Batch mode complete — Delta tables written to lh_gold_curated.")

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ============================================================================
# Stream Mode — Continuously push events to Eventstream Custom App
# ============================================================================
if MODE == "stream":
    if not EVENTSTREAM_CONN_STR or not EVENTHUB_NAME:
        raise ValueError(
            "EVENTSTREAM_CONN_STR and EVENTHUB_NAME must be set for stream mode.\n\n"
            "To get these values:\n"
            "  1. Run NB_RTI_Setup_Eventhouse first — it creates the Eventstream\n"
            "  2. If the connection string was printed, copy it above\n"
            "  3. Otherwise, open the Eventstream in the Fabric portal:\n"
            "     - Click the Custom Endpoint source node\n"
            "     - Copy 'Connection string' → set EVENTSTREAM_CONN_STR\n"
            "     - Copy 'Event hub name'    → set EVENTHUB_NAME\n"
        )

    from azure.eventhub import EventHubProducerClient, EventData
    import time

    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENTSTREAM_CONN_STR,
        eventhub_name=EVENTHUB_NAME
    )

    batch_num = 0
    print(f"Streaming events every {STREAM_INTERVAL_SEC}s (Ctrl+C to stop)...")

    try:
        while True:
            batch_num += 1

            # Generate mixed event batch
            claims_pdf = generate_claims_events(BATCH_SIZE)
            adt_pdf = generate_adt_events(BATCH_SIZE // 2)
            rx_pdf = generate_rx_events(BATCH_SIZE // 3)

            # Send claims events
            event_batch = producer.create_batch()
            for _, row in claims_pdf.iterrows():
                event_batch.add(EventData(json.dumps(row.to_dict())))
            producer.send_batch(event_batch)

            # Send ADT events
            event_batch = producer.create_batch()
            for _, row in adt_pdf.iterrows():
                event_batch.add(EventData(json.dumps(row.to_dict())))
            producer.send_batch(event_batch)

            # Send Rx events
            event_batch = producer.create_batch()
            for _, row in rx_pdf.iterrows():
                event_batch.add(EventData(json.dumps(row.to_dict())))
            producer.send_batch(event_batch)

            total = len(claims_pdf) + len(adt_pdf) + len(rx_pdf)
            print(f"  Batch {batch_num}: sent {total} events")

            # Also append to Delta tables for historical tracking
            spark.createDataFrame(claims_pdf).write.format("delta").mode("append").saveAsTable("rti_claims_events")
            spark.createDataFrame(adt_pdf).write.format("delta").mode("append").saveAsTable("rti_adt_events")
            spark.createDataFrame(rx_pdf).write.format("delta").mode("append").saveAsTable("rti_rx_events")

            time.sleep(STREAM_INTERVAL_SEC)

    except KeyboardInterrupt:
        print(f"Streaming stopped after {batch_num} batches.")
    finally:
        producer.close()

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# ---------- Summary ----------
print("\n" + "=" * 60)
print("NB_RTI_Event_Simulator: COMPLETE")
print("=" * 60)
if MODE == "batch":
    print("Tables written to lh_gold_curated:")
    print("  - rti_claims_events  (claims + injected fraud patterns)")
    print("  - rti_adt_events     (ADT + open care gap flags)")
    print("  - rti_rx_events      (prescription fills)")
    print("\nFraud pattern injection rates:")
    for k, v in FRAUD_PATTERNS.items():
        print(f"  - {k}: {v*100:.0f}%")
print("=" * 60)

# METADATA **{"language":"python"}**
