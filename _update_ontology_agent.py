"""
Update Healthcare Ontology Agent:
1. Remove Vitals from AI instructions + datasource
2. Fix Rule 6 (pick via relationships, not orphan nodes)
3. Expand fewshots from 11 to 29
"""
import json, pathlib

BASE = pathlib.Path("data_agents/Healthcare Ontology Agent.DataAgent/Files/Config")

# ============================================================
# 1. FIX AI INSTRUCTIONS (stage_config.json)
# ============================================================
for stage in ["published", "draft"]:
    p = BASE / stage / "stage_config.json"
    data = json.loads(p.read_text(encoding="utf-8"))
    ai = data["aiInstructions"]

    # Remove vitals from opening line
    ai = ai.replace(", vitals, and SDOH", ", and SDOH")

    # Update counts 12→11, 18→17
    ai = ai.replace("12 entities, 18 relationships", "11 entities, 17 relationships")

    # Remove Vitals schema line
    ai = ai.replace("Vitals —[vitalsTakenFor]→ Patient\n", "")

    # Remove Vitals properties line
    ai = ai.replace(
        "Vitals: avg_heart_rate, avg_bp_systolic, avg_spo2, avg_temperature, risk_flag\n",
        "",
    )

    # Remove Vitals clinical rule
    ai = ai.replace(
        "- Vitals abnormal: BP systolic >= 140, HR > 100 or < 60, SpO2 < 95%, Temp > 100.4°F, RR > 20\n",
        "",
    )

    # Fix Rule 6 — pick via relationship, not orphan node
    ai = ai.replace(
        "6. When a user says 'pick one' or does not specify an entity, add LIMIT 1 to find a single starting entity first.",
        "6. When a user says 'pick one' or does not specify an entity, NEVER query the node table directly (e.g., MATCH (p:Provider) RETURN p LIMIT 1) — orphan nodes with no relationships will return empty results. Instead, ALWAYS pick via a relationship that proves connected data exists:\n"
        "   - Provider: MATCH (e:Encounter)-[:serves]->(p:Provider) RETURN DISTINCT p LIMIT 1\n"
        "   - Patient: MATCH (e:Encounter)-[:involves]->(pat:Patient) RETURN DISTINCT pat LIMIT 1\n"
        "   - Payer: MATCH (c:Claim)-[:ClaimHasPayer]->(pay:Payer) RETURN DISTINCT pay LIMIT 1\n"
        "   - Claim: MATCH (c:Claim)-[:submittedBy]->(p:Provider) RETURN c LIMIT 1\n"
        "   This guarantees the picked entity has connected data to explore.",
    )

    data["aiInstructions"] = ai
    p.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"{stage} stage_config: vitals removed, Rule 6 fixed")


# ============================================================
# 2. FIX DATASOURCE (datasource.json)
# ============================================================
for stage in ["published", "draft"]:
    p = BASE / stage / "graph-Healthcare_Demo_Graph" / "datasource.json"
    data = json.loads(p.read_text(encoding="utf-8"))

    # Remove Vitals entity + vitalsTakenFor edge from elements
    data["elements"] = [
        e
        for e in data["elements"]
        if e["display_name"] not in ("Vitals", "vitalsTakenFor")
    ]

    # Clean datasource instructions
    dsi = data["dataSourceInstructions"]
    dsi = dsi.replace("12 entity types, 18 relationships", "11 entity types, 17 relationships")
    dsi = dsi.replace("  Vitals — patient vital signs (PK: patient_key + encounter_key)\n", "")
    dsi = dsi.replace(
        "  Vitals → Patient (vitalsTakenFor, patient_key) — vitals for which patient\n", ""
    )
    data["dataSourceInstructions"] = dsi

    # Clean user description
    ud = data["userDescription"]
    ud = ud.replace("12 entity types and 18 relationships", "11 entity types and 17 relationships")
    ud = ud.replace(", vitals, SDOH", ", SDOH")
    data["userDescription"] = ud

    p.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"{stage} datasource: vitals removed, {len(data['elements'])} elements remain")


# ============================================================
# 3. BUILD ALL 29 FEWSHOTS
# ============================================================
fewshots = [
    # --- Provider Operations & Network (1-6) ---
    {
        "question": "Show me a full provider profile — patients treated, encounters, claims submitted, and prescriptions written.",
        "answer": (
            "I'll build a full provider profile using SEPARATE queries to avoid a Cartesian product.\n\n"
            "**Step 1 — Pick a provider with connected data** (never from the node table directly — orphan nodes exist):\n"
            "MATCH (e:Encounter)-[:serves]->(p:Provider) RETURN DISTINCT p.provider_id, p.display_name, p.specialty LIMIT 1\n\n"
            "**Step 2 — Encounters**: MATCH (e:Encounter)-[:serves]->(p:Provider) WHERE p.provider_id = 'X' RETURN e.encounter_id, e.encounter_type, e.total_charges, e.readmission_risk_category LIMIT 10\n\n"
            "**Step 3 — Claims**: MATCH (c:Claim)-[:submittedBy]->(p:Provider) WHERE p.provider_id = 'X' RETURN c.claim_id, c.claim_status, c.billed_amount, c.denial_flag, c.primary_denial_reason LIMIT 10\n\n"
            "**Step 4 — Prescriptions**: MATCH (rx:Prescription)-[:prescribedBy]->(p:Provider) WHERE p.provider_id = 'X' MATCH (rx)-[:dispenses]->(m:Medication) RETURN rx.prescription_id, m.medication_name, rx.total_cost LIMIT 10\n\n"
            "**Step 5 — Patients**: MATCH (e:Encounter)-[:serves]->(p:Provider) WHERE p.provider_id = 'X' MATCH (e)-[:involves]->(pat:Patient) RETURN DISTINCT pat.patient_id, pat.first_name, pat.last_name LIMIT 10\n\n"
            "I present the provider's specialty and department, then summarize each category. I NEVER combine encounters, claims, and prescriptions in a single MATCH — that creates a Cartesian product."
        ),
    },
    {
        "question": "Which providers have submitted claims that were denied? Show each provider with their denied claims, denial reasons, payers, and patients.",
        "answer": (
            "I'll start from denied claims and trace back to providers.\n\n"
            "**Traversal plan:**\n"
            "1. MATCH (c:Claim)-[:submittedBy]->(p:Provider), (c)-[:ClaimHasPayer]->(pay:Payer) WHERE c.denial_flag = 1 RETURN c.claim_id, c.billed_amount, c.primary_denial_reason, c.recommended_action, p.display_name, p.specialty, pay.payer_name LIMIT 10\n"
            "2. For each claim, separately get the patient: MATCH (c:Claim)-[:billsFor]->(e:Encounter)-[:involves]->(pat:Patient) WHERE c.claim_id = 'X' RETURN pat.first_name, pat.last_name LIMIT 1\n\n"
            "This works because Claim→submittedBy→Provider and Claim→ClaimHasPayer→Payer share the same starting entity (Claim), so combining them is safe. I group results by provider."
        ),
    },
    {
        "question": "Pick a provider and show me their patient panel — encounters, diagnoses, and outcomes for their patients.",
        "answer": (
            "**Step 1 — Pick a provider via relationship** (guarantees connected data):\n"
            "MATCH (e:Encounter)-[:serves]->(p:Provider) RETURN DISTINCT p.provider_id, p.display_name, p.specialty LIMIT 1\n\n"
            "**Step 2 — Get encounters + patients**: MATCH (e:Encounter)-[:serves]->(p:Provider) WHERE p.provider_id = 'X' MATCH (e)-[:involves]->(pat:Patient) RETURN e.encounter_id, e.encounter_type, e.total_charges, e.readmission_risk_category, pat.patient_id, pat.first_name LIMIT 10\n\n"
            "**Step 3 — Get diagnoses for those patients** (separate query): MATCH (pd:PatientDiagnosis)-[:affects]->(pat:Patient), (pd)-[:references]->(d:Diagnosis) WHERE pat.patient_id IN ['list from step 2'] RETURN pat.patient_id, d.icd_code, d.icd_description, d.is_chronic, pd.diagnosis_type LIMIT 10\n\n"
            "I present each patient with their encounter details, diagnoses, and readmission risk as outcomes."
        ),
    },
    {
        "question": "Show me prescribing patterns for providers in a given specialty — what medications, which patients, and which payers cover the prescriptions?",
        "answer": (
            "**Step 1 — Find providers in the specialty**: MATCH (e:Encounter)-[:serves]->(p:Provider) WHERE p.specialty = 'Cardiology' RETURN DISTINCT p.provider_id, p.display_name LIMIT 5\n\n"
            "**Step 2 — Get prescriptions + medications**: MATCH (rx:Prescription)-[:prescribedBy]->(p:Provider), (rx)-[:dispenses]->(m:Medication) WHERE p.specialty = 'Cardiology' RETURN p.display_name, m.medication_name, m.drug_class, m.is_chronic, rx.total_cost LIMIT 10\n\n"
            "**Step 3 — Get payer coverage**: MATCH (rx:Prescription)-[:prescribedBy]->(p:Provider), (rx)-[:PrescriptionHasPayer]->(pay:Payer) WHERE p.specialty = 'Cardiology' RETURN p.display_name, pay.payer_name, rx.payer_paid, rx.patient_copay LIMIT 10\n\n"
            "I present each provider with their prescribing patterns: medications, drug classes, and covering payers. I highlight chronic medications.\n\n"
            "Tip: Try any specialty — Cardiology, Internal Medicine, Orthopedics, Neurology, Oncology, Pediatrics, Family Practice."
        ),
    },
    {
        "question": "Which providers treat patients with high readmission risk? Show the provider, their high-risk patients, and conditions.",
        "answer": (
            "**Step 1 — Find high-risk encounters with provider + patient**:\n"
            "MATCH (e:Encounter)-[:serves]->(p:Provider), (e)-[:involves]->(pat:Patient) WHERE e.readmission_risk_category = 'High' RETURN p.display_name, p.specialty, pat.patient_id, pat.first_name, pat.last_name, e.encounter_type, e.readmission_risk_score LIMIT 10\n\n"
            "**Step 2 — Get diagnoses for those patients** (separate query):\n"
            "MATCH (pd:PatientDiagnosis)-[:affects]->(pat:Patient), (pd)-[:references]->(d:Diagnosis) WHERE pat.patient_id IN ['list from step 1'] RETURN pat.patient_id, d.icd_description, d.is_chronic LIMIT 10\n\n"
            "I present each provider with their high-readmission-risk patients and their conditions. I flag chronic conditions as higher intervention priority."
        ),
    },
    {
        "question": "Which providers serve patients in socially vulnerable communities? Show the SDOH profile alongside provider details.",
        "answer": (
            "I'll trace from high-risk communities to patients to providers using separate queries.\n\n"
            "**Step 1 — Find patients in vulnerable communities**:\n"
            "MATCH (pat:Patient)-[:livesIn]->(ch:CommunityHealth) WHERE ch.risk_tier = 'High' RETURN pat.patient_id, pat.first_name, ch.zip_code, ch.poverty_rate, ch.social_vulnerability_index, ch.food_desert_flag LIMIT 10\n"
            "IMPORTANT: Filter by risk_tier = 'High', NOT by social_vulnerability_index > 0.8 (SVI max is ~0.53).\n\n"
            "**Step 2 — Find their providers**:\n"
            "MATCH (e:Encounter)-[:involves]->(pat:Patient), (e)-[:serves]->(prov:Provider) WHERE pat.patient_id IN ['list from step 1'] RETURN DISTINCT prov.display_name, prov.specialty, pat.patient_id LIMIT 10\n\n"
            "I present each provider with the SDOH profile of the communities they serve."
        ),
    },
    # --- Payer Portfolio & Coverage (7-11) ---
    {
        "question": "Show me all claims for a specific payer — which providers submitted them, which patients, and what were the outcomes?",
        "answer": (
            "**Step 1 — Find the payer** (if not specified, pick one via relationship):\n"
            "MATCH (c:Claim)-[:ClaimHasPayer]->(pay:Payer) RETURN DISTINCT pay.payer_name, pay.payer_type LIMIT 5\n\n"
            "**Step 2 — Get claims with provider**:\n"
            "MATCH (c:Claim)-[:ClaimHasPayer]->(pay:Payer), (c)-[:submittedBy]->(p:Provider) WHERE pay.payer_name = 'Aetna' RETURN c.claim_id, c.claim_status, c.billed_amount, c.paid_amount, c.denial_flag, p.display_name LIMIT 10\n\n"
            "**Step 3 — Get patients for those claims** (separate to avoid 3-way join):\n"
            "MATCH (c:Claim)-[:billsFor]->(e:Encounter)-[:involves]->(pat:Patient) WHERE c.claim_id IN ['list from step 2'] RETURN c.claim_id, pat.first_name, pat.last_name LIMIT 10\n\n"
            "I group by outcome (paid vs denied) with financial picture: billed vs paid amounts."
        ),
    },
    {
        "question": "Which payers have the most denied claims? For each payer show the denials with provider, patient, encounter, and denial reason.",
        "answer": (
            "**Step 1 — Count denied claims per payer** (bounded aggregation):\n"
            "MATCH (c:Claim)-[:ClaimHasPayer]->(pay:Payer) WHERE c.denial_flag = 1 RETURN pay.payer_name, COUNT(c) AS denied_count LIMIT 20\n\n"
            "**Step 2 — For the top payer, get denial details**:\n"
            "MATCH (c:Claim)-[:ClaimHasPayer]->(pay:Payer), (c)-[:submittedBy]->(p:Provider) WHERE c.denial_flag = 1 AND pay.payer_name = 'TopPayer' RETURN c.claim_id, c.billed_amount, c.primary_denial_reason, c.recommended_action, p.display_name LIMIT 10\n\n"
            "**Step 3 — Get patients + encounters** (separate query):\n"
            "MATCH (c:Claim)-[:billsFor]->(e:Encounter)-[:involves]->(pat:Patient) WHERE c.claim_id IN ['list from step 2'] RETURN c.claim_id, e.encounter_type, pat.first_name, pat.last_name LIMIT 10\n\n"
            "I rank payers by denial count and then drill into details for the highest-denial payer."
        ),
    },
    {
        "question": "What prescriptions does each payer cover? Show the medication, prescribing provider, and patient.",
        "answer": (
            "**Step 1 — Pick a payer via relationship**:\n"
            "MATCH (rx:Prescription)-[:PrescriptionHasPayer]->(pay:Payer) RETURN DISTINCT pay.payer_name LIMIT 5\n\n"
            "**Step 2 — Get prescriptions + medications for that payer**:\n"
            "MATCH (rx:Prescription)-[:PrescriptionHasPayer]->(pay:Payer), (rx)-[:dispenses]->(m:Medication) WHERE pay.payer_name = 'Aetna' RETURN m.medication_name, m.drug_class, m.is_chronic, rx.total_cost, rx.payer_paid, rx.patient_copay LIMIT 10\n\n"
            "**Step 3 — Get prescribing providers** (separate query):\n"
            "MATCH (rx:Prescription)-[:PrescriptionHasPayer]->(pay:Payer), (rx)-[:prescribedBy]->(p:Provider) WHERE pay.payer_name = 'Aetna' RETURN rx.prescription_id, p.display_name, p.specialty LIMIT 10\n\n"
            "I present each payer's covered medications with drug class, chronic flag, and prescribing providers."
        ),
    },
    {
        "question": "Show me the provider network that bills a specific payer — which providers submit claims and for what encounter types?",
        "answer": (
            "**Step 1 — List payers** (if none specified):\n"
            "MATCH (c:Claim)-[:ClaimHasPayer]->(pay:Payer) RETURN DISTINCT pay.payer_name LIMIT 5\n\n"
            "**Step 2 — Get providers billing that payer**:\n"
            "MATCH (c:Claim)-[:ClaimHasPayer]->(pay:Payer), (c)-[:submittedBy]->(p:Provider) WHERE pay.payer_name = 'Aetna' RETURN DISTINCT p.display_name, p.specialty, COUNT(c) AS claim_count LIMIT 20\n\n"
            "**Step 3 — Get encounter types**:\n"
            "MATCH (c:Claim)-[:ClaimHasPayer]->(pay:Payer), (c)-[:billsFor]->(e:Encounter) WHERE pay.payer_name = 'Aetna' RETURN DISTINCT e.encounter_type, COUNT(e) AS encounter_count LIMIT 10\n\n"
            "I present the provider network: which providers bill this payer, their specialties, and the encounter types generating claims."
        ),
    },
    {
        "question": "Which payers cover patients in high SDOH risk areas? Show the payer, the patients, and their social risk factors.",
        "answer": (
            "**Step 1 — Find patients in high-risk communities**:\n"
            "MATCH (pat:Patient)-[:livesIn]->(ch:CommunityHealth) WHERE ch.risk_tier = 'High' RETURN pat.patient_id, pat.first_name, ch.zip_code, ch.poverty_rate, ch.food_desert_flag LIMIT 10\n"
            "IMPORTANT: Filter by risk_tier = 'High', NOT raw SVI values.\n\n"
            "**Step 2 — Find payers covering those patients via claims**:\n"
            "MATCH (c:Claim)-[:covers]->(pat:Patient), (c)-[:ClaimHasPayer]->(pay:Payer) WHERE pat.patient_id IN ['list from step 1'] RETURN DISTINCT pay.payer_name, pay.payer_type, pat.patient_id LIMIT 10\n\n"
            "I present each payer with the high-SDOH-risk patients they cover and the community risk profile."
        ),
    },
    # --- Claims & Financial Investigation (12-16) ---
    {
        "question": "Trace a claim end-to-end — from patient through encounter, provider, and payer.",
        "answer": (
            "**Step 1 — Pick a claim via relationship** (guarantees connected data):\n"
            "MATCH (c:Claim)-[:submittedBy]->(p:Provider) RETURN c.claim_id, c.claim_status, c.denial_flag LIMIT 1\n"
            "Tip: Say 'pick a denied claim' and I'll add WHERE c.denial_flag = 1.\n\n"
            "**Step 2 — Fan out from that claim** (safe — all paths share the same Claim start):\n"
            "MATCH (c:Claim)-[:submittedBy]->(p:Provider), (c)-[:ClaimHasPayer]->(pay:Payer) WHERE c.claim_id = 'X' RETURN c.billed_amount, c.paid_amount, c.primary_denial_reason, p.display_name, p.specialty, pay.payer_name LIMIT 1\n\n"
            "**Step 3 — Get patient via encounter**:\n"
            "MATCH (c:Claim)-[:billsFor]->(e:Encounter)-[:involves]->(pat:Patient) WHERE c.claim_id = 'X' RETURN e.encounter_type, e.total_charges, e.length_of_stay, pat.first_name, pat.last_name, pat.insurance_type LIMIT 1\n\n"
            "I present the complete claim story: Patient → Encounter → Provider → Payer with financials."
        ),
    },
    {
        "question": "Show me all denied claims with the complete story — which provider submitted, to which payer, for which patient, and why denied.",
        "answer": (
            "**Step 1 — Get denied claims with provider + payer** (two outbound paths from Claim — safe):\n"
            "MATCH (c:Claim)-[:submittedBy]->(p:Provider), (c)-[:ClaimHasPayer]->(pay:Payer) WHERE c.denial_flag = 1 RETURN c.claim_id, c.billed_amount, c.primary_denial_reason, c.recommended_action, p.display_name, pay.payer_name LIMIT 10\n\n"
            "**Step 2 — Get patients for each claim**:\n"
            "MATCH (c:Claim)-[:billsFor]->(e:Encounter)-[:involves]->(pat:Patient) WHERE c.claim_id IN ['list from step 1'] RETURN c.claim_id, pat.first_name, pat.last_name, e.encounter_type LIMIT 10\n\n"
            "For each denied claim: Patient had encounter treated by Dr. [name], claim for $[amount] submitted to [payer], denied for [reason]. Recommended action: [action]."
        ),
    },
    {
        "question": "What claims are linked to a specific encounter? Show the financial picture including payer and provider.",
        "answer": (
            "**Step 1 — Pick an encounter via relationship**:\n"
            "MATCH (e:Encounter)-[:involves]->(pat:Patient) RETURN e.encounter_id, e.encounter_type, e.total_charges, pat.first_name LIMIT 1\n\n"
            "**Step 2 — Get claims for that encounter**:\n"
            "MATCH (c:Claim)-[:billsFor]->(e:Encounter) WHERE e.encounter_id = 'X' RETURN c.claim_id, c.claim_status, c.billed_amount, c.paid_amount, c.denial_flag LIMIT 10\n\n"
            "**Step 3 — Get payer + provider for each claim**:\n"
            "MATCH (c:Claim)-[:submittedBy]->(p:Provider), (c)-[:ClaimHasPayer]->(pay:Payer) WHERE c.claim_id IN ['list from step 2'] RETURN c.claim_id, p.display_name, pay.payer_name LIMIT 10\n\n"
            "I present the encounter with its patient, then each linked claim with billed vs paid, payer, provider, and denial status."
        ),
    },
    {
        "question": "Show me denied claims for high-risk readmission patients — which providers and payers are involved.",
        "answer": (
            "**Step 1 — Find high-readmission encounters with denied claims** (two paths from Claim — safe):\n"
            "MATCH (c:Claim)-[:billsFor]->(e:Encounter) WHERE c.denial_flag = 1 AND e.readmission_risk_category = 'High' RETURN c.claim_id, c.billed_amount, c.primary_denial_reason, e.encounter_type, e.readmission_risk_score LIMIT 10\n\n"
            "**Step 2 — Get provider + payer**:\n"
            "MATCH (c:Claim)-[:submittedBy]->(p:Provider), (c)-[:ClaimHasPayer]->(pay:Payer) WHERE c.claim_id IN ['list from step 1'] RETURN c.claim_id, p.display_name, p.specialty, pay.payer_name LIMIT 10\n\n"
            "**Step 3 — Get patients**:\n"
            "MATCH (c:Claim)-[:billsFor]->(e:Encounter)-[:involves]->(pat:Patient) WHERE c.claim_id IN ['list from step 1'] RETURN c.claim_id, pat.first_name, pat.last_name LIMIT 10\n\n"
            "I flag these as high-priority: denied claims for patients already at high readmission risk."
        ),
    },
    {
        "question": "Show me high-cost encounters — who was the provider, what diagnoses, what payer, and was the claim denied.",
        "answer": (
            "**Step 1 — Find high-cost encounters** (top by total_charges):\n"
            "MATCH (e:Encounter)-[:serves]->(p:Provider), (e)-[:involves]->(pat:Patient) RETURN e.encounter_id, e.total_charges, e.encounter_type, p.display_name, pat.first_name LIMIT 10\n"
            "(The agent returns the first 10 encounters — I review for highest charges.)\n\n"
            "**Step 2 — Get claims for those encounters**:\n"
            "MATCH (c:Claim)-[:billsFor]->(e:Encounter), (c)-[:ClaimHasPayer]->(pay:Payer) WHERE e.encounter_id IN ['list from step 1'] RETURN c.claim_id, c.billed_amount, c.denial_flag, c.primary_denial_reason, pay.payer_name LIMIT 10\n\n"
            "**Step 3 — Get diagnoses**:\n"
            "MATCH (pd:PatientDiagnosis)-[:occursIn]->(e:Encounter), (pd)-[:references]->(d:Diagnosis) WHERE e.encounter_id IN ['list from step 1'] RETURN e.encounter_id, d.icd_code, d.icd_description LIMIT 10\n\n"
            "I present each high-cost encounter with the complete picture: provider, diagnoses, payer, and claim denial status."
        ),
    },
    # --- Provider-Payer Network Analysis (17-20) ---
    {
        "question": "How are providers and payers connected through claims? Show me the provider-payer claims network.",
        "answer": (
            "**Traversal** — two outbound paths from Claim (safe, no Cartesian):\n"
            "MATCH (c:Claim)-[:submittedBy]->(p:Provider), (c)-[:ClaimHasPayer]->(pay:Payer) RETURN p.display_name, p.specialty, pay.payer_name, COUNT(c) AS claim_count LIMIT 20\n\n"
            "I build a network view showing which providers submit claims to which payers, with claim counts. I highlight high-volume provider-payer pairs."
        ),
    },
    {
        "question": "Which chronic medications are prescribed across the provider network and which payers reimburse them?",
        "answer": (
            "**Step 1 — Get chronic medications with prescribing providers**:\n"
            "MATCH (rx:Prescription)-[:dispenses]->(m:Medication), (rx)-[:prescribedBy]->(p:Provider) WHERE m.is_chronic = 1 RETURN m.medication_name, m.drug_class, p.display_name, p.specialty LIMIT 10\n\n"
            "**Step 2 — Get payer coverage for those chronic meds**:\n"
            "MATCH (rx:Prescription)-[:dispenses]->(m:Medication), (rx)-[:PrescriptionHasPayer]->(pay:Payer) WHERE m.is_chronic = 1 RETURN m.medication_name, pay.payer_name, rx.payer_paid, rx.patient_copay LIMIT 10\n\n"
            "I present each chronic medication with which providers prescribe it and which payers reimburse, including cost breakdown."
        ),
    },
    {
        "question": "Show me the relationship between claim denials and patient diagnoses — what conditions are associated with denied claims.",
        "answer": (
            "**Step 1 — Get denied claims with their encounters**:\n"
            "MATCH (c:Claim)-[:billsFor]->(e:Encounter) WHERE c.denial_flag = 1 RETURN c.claim_id, c.primary_denial_reason, e.encounter_id, e.encounter_type LIMIT 10\n\n"
            "**Step 2 — Get diagnoses for those encounters**:\n"
            "MATCH (pd:PatientDiagnosis)-[:occursIn]->(e:Encounter), (pd)-[:references]->(d:Diagnosis) WHERE e.encounter_id IN ['list from step 1'] RETURN e.encounter_id, d.icd_code, d.icd_description, d.icd_category, d.is_chronic LIMIT 10\n\n"
            "I map each denial reason to the associated diagnoses, looking for patterns: Are certain ICD categories more likely to be denied? I present the correlation between diagnosis type and denial reason."
        ),
    },
    {
        "question": "Trace prescription costs by payer — which payers bear the highest prescription burden and for what drug classes?",
        "answer": (
            "**Step 1 — Get prescription costs by payer**:\n"
            "MATCH (rx:Prescription)-[:PrescriptionHasPayer]->(pay:Payer) RETURN pay.payer_name, COUNT(rx) AS rx_count LIMIT 20\n\n"
            "**Step 2 — Break down by drug class for the top payer**:\n"
            "MATCH (rx:Prescription)-[:PrescriptionHasPayer]->(pay:Payer), (rx)-[:dispenses]->(m:Medication) WHERE pay.payer_name = 'TopPayer' RETURN m.drug_class, m.therapeutic_area, COUNT(rx) AS rx_count LIMIT 20\n\n"
            "**Step 3 — Get cost details**:\n"
            "MATCH (rx:Prescription)-[:PrescriptionHasPayer]->(pay:Payer), (rx)-[:dispenses]->(m:Medication) WHERE pay.payer_name = 'TopPayer' RETURN m.drug_class, rx.total_cost, rx.payer_paid, rx.patient_copay LIMIT 10\n\n"
            "I present payer prescription burden ranked by count, then drill into drug class breakdown with cost sharing details."
        ),
    },
    # --- Clinical Operations & Interventions (21-24) ---
    {
        "question": "Show me patients with poor medication adherence — who are their providers and which payers cover them?",
        "answer": (
            "**Step 1 — Find non-adherent patients with medications**:\n"
            "MATCH (ma:MedicationAdherence)-[:adherenceFor]->(pat:Patient), (ma)-[:adherenceMedication]->(med:Medication) WHERE ma.adherence_category = 'Non-Adherent' RETURN pat.patient_id, pat.first_name, pat.last_name, med.medication_name, med.is_chronic, ma.pdc_score LIMIT 10\n\n"
            "**Step 2 — Find their providers** (separate query):\n"
            "MATCH (e:Encounter)-[:involves]->(pat:Patient), (e)-[:serves]->(prov:Provider) WHERE pat.patient_id IN ['list from step 1'] RETURN DISTINCT pat.patient_id, prov.display_name, prov.specialty LIMIT 10\n\n"
            "**Step 3 — Find their payers** (separate query):\n"
            "MATCH (c:Claim)-[:covers]->(pat:Patient), (c)-[:ClaimHasPayer]->(pay:Payer) WHERE pat.patient_id IN ['list from step 1'] RETURN DISTINCT pat.patient_id, pay.payer_name LIMIT 10\n\n"
            "I flag chronic medication non-adherence as requiring clinical intervention and present each patient with their providers and payers."
        ),
    },
    {
        "question": "Show me which payers cover prescriptions for each drug class — break down total cost, payer-paid, and patient copay.",
        "answer": (
            "**Step 1 — Get payer coverage by drug class**:\n"
            "MATCH (rx:Prescription)-[:dispenses]->(m:Medication), (rx)-[:PrescriptionHasPayer]->(pay:Payer) RETURN m.drug_class, pay.payer_name, rx.total_cost, rx.payer_paid, rx.patient_copay LIMIT 20\n\n"
            "I group by drug class, then by payer, and present a cost breakdown table:\n"
            "- Drug class → Payer → Avg total cost, Avg payer paid, Avg patient copay\n"
            "I highlight drug classes with high patient copay relative to total cost."
        ),
    },
    {
        "question": "Find our most vulnerable population — patients in high SDOH risk areas who are non-adherent with high readmission risk.",
        "answer": (
            "I'll combine three filters using separate queries to stay performant.\n\n"
            "**Step 1 — Non-adherent patients**:\n"
            "MATCH (ma:MedicationAdherence)-[:adherenceFor]->(pat:Patient) WHERE ma.adherence_category = 'Non-Adherent' RETURN pat.patient_id, pat.first_name, ma.pdc_score LIMIT 10\n\n"
            "**Step 2 — Check SDOH for those patients**:\n"
            "MATCH (pat:Patient)-[:livesIn]->(ch:CommunityHealth) WHERE ch.risk_tier = 'High' AND pat.patient_id IN ['list from step 1'] RETURN pat.patient_id, ch.zip_code, ch.poverty_rate, ch.food_desert_flag LIMIT 10\n"
            "IMPORTANT: Filter by risk_tier = 'High', NOT by SVI > 0.8.\n\n"
            "**Step 3 — Check readmission risk**:\n"
            "MATCH (e:Encounter)-[:involves]->(pat:Patient) WHERE pat.patient_id IN ['filtered list'] AND e.readmission_risk_category = 'High' RETURN pat.patient_id, e.encounter_type, e.readmission_risk_score LIMIT 10\n\n"
            "**Step 4 — Get providers and payers** for the most vulnerable:\n"
            "Separate queries for providers via Encounter→serves and payers via Claim→ClaimHasPayer.\n\n"
            "I present the most vulnerable patients with their SDOH profile, adherence scores, readmission risk, and care team."
        ),
    },
    {
        "question": "What is the overall denial rate by payer or the average cost per provider across the network?",
        "answer": (
            "I answer aggregation questions by scoping to specific entities first.\n\n"
            "**Denial rate by payer**:\n"
            "MATCH (c:Claim)-[:ClaimHasPayer]->(pay:Payer) WHERE pay.payer_name = 'Aetna' RETURN c.denial_flag, COUNT(c) AS cnt LIMIT 20\n"
            "From results: X denied out of Y total = Z% denial rate. I repeat for each payer.\n\n"
            "**Average cost per provider** (scoped):\n"
            "MATCH (c:Claim)-[:submittedBy]->(p:Provider) WHERE c.denial_flag = 1 RETURN p.display_name, COUNT(c) AS denied_count LIMIT 20\n\n"
            "**Average charges for a provider**:\n"
            "MATCH (e:Encounter)-[:serves]->(p:Provider) WHERE p.display_name CONTAINS 'Smith' RETURN e.total_charges LIMIT 20\n"
            "I compute the average from returned values.\n\n"
            "For broad questions like 'overall denial rate', I break down by payer or provider so each query stays scoped and fast. Show the math: 'X denied out of Y total = Z%'."
        ),
    },
    # --- Prescription-Payer Relationship Traversal (25-29) ---
    {
        "question": "For a specific patient, trace all their prescriptions — which provider prescribed each, which payer covers it, and what's the patient copay?",
        "answer": (
            "**Step 1 — Pick a patient via relationship** (guarantees connected data):\n"
            "MATCH (e:Encounter)-[:involves]->(pat:Patient) RETURN DISTINCT pat.patient_id, pat.first_name, pat.last_name LIMIT 1\n\n"
            "**Step 2 — Get prescriptions + medications + providers**:\n"
            "MATCH (rx:Prescription)-[:serves]->(pat:Patient), (rx)-[:dispenses]->(m:Medication), (rx)-[:prescribedBy]->(p:Provider) WHERE pat.patient_id = 'X' RETURN rx.prescription_id, m.medication_name, m.drug_class, p.display_name, rx.total_cost, rx.patient_copay LIMIT 10\n\n"
            "**Step 3 — Get payer for each prescription** (separate query):\n"
            "MATCH (rx:Prescription)-[:serves]->(pat:Patient), (rx)-[:PrescriptionHasPayer]->(pay:Payer) WHERE pat.patient_id = 'X' RETURN rx.prescription_id, pay.payer_name, rx.payer_paid LIMIT 10\n\n"
            "I present each prescription with: medication, prescribing provider, covering payer, and copay breakdown."
        ),
    },
    {
        "question": "Which payers cover the highest-cost specialty medications? Show the drug, prescribing provider, and payer for the top 20 most expensive prescriptions.",
        "answer": (
            "**Step 1 — Get highest-cost prescriptions with medications**:\n"
            "MATCH (rx:Prescription)-[:dispenses]->(m:Medication) RETURN rx.prescription_id, m.medication_name, m.drug_class, m.therapeutic_area, rx.total_cost LIMIT 20\n"
            "(I review for highest total_cost values.)\n\n"
            "**Step 2 — Get payer + provider for those prescriptions**:\n"
            "MATCH (rx:Prescription)-[:PrescriptionHasPayer]->(pay:Payer), (rx)-[:prescribedBy]->(p:Provider) WHERE rx.prescription_id IN ['list from step 1'] RETURN rx.prescription_id, pay.payer_name, p.display_name, rx.payer_paid, rx.patient_copay LIMIT 20\n\n"
            "I present the top prescriptions sorted by cost: medication, drug class, prescribing provider, and covering payer with cost breakdown."
        ),
    },
    {
        "question": "Show me the full prescription lifecycle for a chronic medication — from prescribing provider to payer reimbursement to patient out-of-pocket cost.",
        "answer": (
            "**Step 1 — Find a chronic medication**:\n"
            "MATCH (rx:Prescription)-[:dispenses]->(m:Medication) WHERE m.is_chronic = 1 RETURN DISTINCT m.medication_name, m.drug_class, m.therapeutic_area LIMIT 5\n\n"
            "**Step 2 — Trace prescriptions for that medication**:\n"
            "MATCH (rx:Prescription)-[:dispenses]->(m:Medication), (rx)-[:prescribedBy]->(p:Provider) WHERE m.medication_name = 'Metformin' RETURN rx.prescription_id, p.display_name, p.specialty, rx.total_cost, rx.days_supply, rx.quantity_dispensed, rx.is_generic LIMIT 10\n\n"
            "**Step 3 — Get payer reimbursement**:\n"
            "MATCH (rx:Prescription)-[:dispenses]->(m:Medication), (rx)-[:PrescriptionHasPayer]->(pay:Payer) WHERE m.medication_name = 'Metformin' RETURN rx.prescription_id, pay.payer_name, rx.payer_paid, rx.patient_copay, rx.pharmacy_type LIMIT 10\n\n"
            "I present the full lifecycle: Provider prescribes → Payer reimburses → Patient pays copay, with total cost, payer share, and patient share."
        ),
    },
    {
        "question": "Which providers prescribe the most non-generic medications and which payers are absorbing that cost?",
        "answer": (
            "**Step 1 — Find non-generic prescriptions by provider**:\n"
            "MATCH (rx:Prescription)-[:dispenses]->(m:Medication), (rx)-[:prescribedBy]->(p:Provider) WHERE rx.is_generic = 0 RETURN p.display_name, p.specialty, COUNT(rx) AS non_generic_count LIMIT 20\n\n"
            "**Step 2 — Get payer cost for non-generics by top provider**:\n"
            "MATCH (rx:Prescription)-[:prescribedBy]->(p:Provider), (rx)-[:PrescriptionHasPayer]->(pay:Payer) WHERE p.display_name = 'TopProvider' AND rx.is_generic = 0 RETURN pay.payer_name, rx.total_cost, rx.payer_paid LIMIT 10\n\n"
            "I present providers ranked by non-generic prescribing volume, then show which payers bear the brand-name cost."
        ),
    },
    {
        "question": "For patients in food desert zip codes, trace their prescriptions — are payers covering their chronic medications adequately or are copays high?",
        "answer": (
            "**Step 1 — Find patients in food deserts**:\n"
            "MATCH (pat:Patient)-[:livesIn]->(ch:CommunityHealth) WHERE ch.food_desert_flag = 1 RETURN pat.patient_id, pat.first_name, ch.zip_code, ch.poverty_rate LIMIT 10\n\n"
            "**Step 2 — Get their chronic medication prescriptions**:\n"
            "MATCH (rx:Prescription)-[:serves]->(pat:Patient), (rx)-[:dispenses]->(m:Medication) WHERE pat.patient_id IN ['list from step 1'] AND m.is_chronic = 1 RETURN pat.patient_id, m.medication_name, rx.total_cost, rx.patient_copay LIMIT 10\n\n"
            "**Step 3 — Get payer coverage**:\n"
            "MATCH (rx:Prescription)-[:serves]->(pat:Patient), (rx)-[:PrescriptionHasPayer]->(pay:Payer) WHERE pat.patient_id IN ['list from step 1'] RETURN pat.patient_id, pay.payer_name, rx.payer_paid, rx.patient_copay LIMIT 10\n\n"
            "I flag patients where copay is a high percentage of total cost — these are patients in food deserts who may face financial barriers to medication adherence."
        ),
    },
]

# Write fewshots to both published and draft
for stage in ["published", "draft"]:
    p = BASE / stage / "graph-Healthcare_Demo_Graph" / "fewshots.json"
    out = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/fewShots/1.0.0/schema.json",
        "fewShots": fewshots,
    }
    p.write_text(json.dumps(out, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"{stage} fewshots: {len(fewshots)} fewshots written")

print("\nDone! All updates applied.")
