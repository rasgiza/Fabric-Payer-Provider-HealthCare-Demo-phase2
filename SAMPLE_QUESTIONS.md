# Sample Questions — Healthcare Data Agents

## Executive Pain-Point Questions (Boardroom & C-Suite)

These are the questions an executive sponsor will actually ask in a demo or steering committee — framed in the language of revenue, risk, regulatory exposure, and member outcomes. Each block maps to one of the six pain points called out in the [README](README.md#why-this-demo--the-payer--provider-pain-points). **Use HealthcareHLSAgent for these** (they're aggregations / rates / rankings). For network-traversal follow-ups, switch to the Healthcare Ontology Agent.

> **Demo flow tip:** ask the executive question → agent gives the headline number → drill in with the per-domain questions further down this file → end with a "what would we do about it?" question to surface intervention populations.

### CFO / VP Revenue Cycle — "Where is money leaking?"
| # | Question |
|---|----------|
| E1 | What is our total revenue at risk right now from claims that are denied or flagged high denial-risk but not yet denied? Break it down by payer. |
| E2 | What is our denial rate this period vs. industry benchmark (~5–10%), and which 3 payers and 3 denial reasons account for the biggest dollar losses? |
| E3 | Show me the reimbursement gap by payer — billed vs. allowed vs. paid — and rank payers by the percentage we're leaving on the table. |
| E4 | If we cleaned up the top 3 denial reasons, how many dollars would we recover this quarter? |
| E5 | Which providers have the highest denial rates and what is their dollar impact? Are these clinical documentation issues, coding issues, or payer-specific issues? |

### CMO / CMIO — "Where are we hurting patients?"
| # | Question |
|---|----------|
| E6 | What is our 30-day readmission rate overall and for the CMS HRRP-penalized conditions (CHF, COPD, AMI, pneumonia)? How does it trend month over month? |
| E7 | Which patients are at highest readmission risk right now AND non-adherent to their chronic medications? These are our highest-acuity intervention targets — list the top 25 with their providers and payers. |
| E8 | What is our estimated HRRP penalty exposure based on current readmission rates? |
| E9 | Which chronic conditions drive the most readmissions, and what is the average cost per readmitted encounter? |
| E10 | Show me high-cost encounters that ended in denied claims — these are double-loss events (clinical risk + revenue loss). |

### VP Population Health / Quality — "How do we move our Star Ratings?"
| # | Question |
|---|----------|
| E11 | What is our medication adherence rate (PDC ≥ 0.80) for the CMS Star triple-weighted measures — diabetes, RAS antagonists, and statins — and how far are we from the 4-star cut point? |
| E12 | How many of our chronic-disease members are non-adherent right now? Break down by drug class so we know where to focus the pharmacy team. |
| E13 | Which payers have the worst adherence for their members enrolled with us? This tells us where care management dollars buy the most lift. |
| E14 | Show me the overlap of non-adherent + high readmission risk + high SDOH risk — our "triple-threat" population. How many members and what is their estimated annual cost? |
| E15 | Which providers have the best and worst adherence in their patient panels? Are best-practice prescribers worth scaling? |

### COO / VP Care Management — "Where do social factors blind us?"
| # | Question |
|---|----------|
| E16 | How many of our members live in high social vulnerability zip codes (SVI risk_tier = High) and what percentage of them are also high readmission risk? |
| E17 | For members in food-desert zip codes, what is the readmission rate and adherence rate vs. the rest of the population? Show the gap. |
| E18 | Which providers serve the highest concentration of socially vulnerable patients? These are the providers we need to support with care-coordination resources. |
| E19 | Are payers covering chronic medications adequately for our high-SDOH members, or are copays driving non-adherence? Show patient out-of-pocket by payer for chronic drug classes. |
| E20 | If we launched an SDOH outreach program targeting the top SVI zip codes, how many members would be in scope? |

### Chief Strategy Officer / Network VP — "How healthy is our payer-provider network?"
| # | Question |
|---|----------|
| E21 | Which payers have the lowest collection rate (paid-to-billed ratio) and which contracts should we renegotiate first? |
| E22 | Show me the provider-payer prescription network — which providers prescribe most heavily to each payer, and where are payer formularies driving cost shift to patients? |
| E23 | Which 5 payers represent the largest share of our denied-claim dollars? |
| E24 | Compare per-encounter cost across our top providers by specialty — who are the high-value vs. high-cost outliers? |
| E25 | Which payers cover our highest-cost specialty medications and what is the patient out-of-pocket burden? |

### CIO / Chief Data Officer — "Can we actually run the business off this?"
| # | Question |
|---|----------|
| E26 | Are the claims, encounter, and prescription pipelines current? When was the last record ingested into Bronze, Silver, and Gold? |
| E27 | How many records did we process in the last 24 hours and how does that compare to baseline? |
| E28 | Are there any data-quality red flags right now — missing keys, late-arriving facts, broken referential integrity? |
| E29 | Show me the same answer two ways — once as SQL aggregation (HealthcareHLSAgent) and once as graph traversal (Ontology Agent) — to prove the numbers reconcile. |

> **Real-world framing:** these are the same questions executives ask in steering committee. The agents won't always answer them perfectly on the first try (especially Graph queries needing warm-up — see tip below) but they will answer the underlying drill-down questions reliably, which is how you build the executive narrative live in the room.

---

## Fabric Data Agent (HealthcareHLSAgent)

Open **HealthcareHLSAgent** in your Fabric workspace. The Data Agent queries the Gold lakehouse star schema directly. Copy-paste any of these to get started:

### Claim Denials & Revenue Cycle
| # | Question |
|---|----------|
| 1 | What is the overall claim denial rate and how does it break down by payer? |
| 2 | What are the top 5 denial reasons by claim count and total billed amount? |
| 3 | Which providers have the highest denial rates? Show the top 10 with their specialties. |
| 4 | Show me all denied claims over $50,000 — include the patient, payer, denial reason, and billed amount. |
| 5 | What is the total revenue at risk from claims flagged as high denial risk that haven’t been denied yet? |
| 6 | Show me claims with high denial risk that are still pending. |
| 7 | How many claims are in each denial risk category? |

### Readmissions & Patient Risk
| # | Question |
|---|----------|
| 8 | What is the 30-day readmission rate and how does it trend by month? |
| 9 | List the top 10 patients with the highest readmission risk scores with their age and insurance type. |
| 10 | What is the readmission rate by encounter type? |
| 11 | Which facilities have the highest readmission rates? |
| 12 | Show me the average length of stay for high-risk vs low-risk readmission patients. |
| 13 | How many encounters are in each readmission risk category? |

### Medication Adherence
| # | Question |
|---|----------|
| 14 | Show me members who are non-adherent to their medications with their drug class and gap days. |
| 15 | Show me medication adherence rates by drug class and therapeutic area. |
| 16 | Which patients with chronic conditions are non-adherent to their medications? |
| 17 | How many patients are adherent vs non-adherent? |
| 18 | Show me prescription costs broken down by drug class and therapeutic area. |

### Social Determinants of Health (SDOH)
| # | Question |
|---|----------|
| 19 | Show me members living in high social vulnerability zip codes with their risk factors. |
| 20 | How does SDOH risk tier affect readmission risk? |
| 21 | Which payers have the highest readmission rates? |
| 22 | Show me readmitted patients with their social risk and adherence data. |

### Encounters & Providers
| # | Question |
|---|----------|
| 23 | What is the average length of stay by encounter type? |
| 24 | What are the top diagnoses by volume? |
| 25 | Which chronic conditions are most prevalent? |
| 26 | Which patients have the highest prescription costs? |
| 27 | How many patients are in each age group? |

### Payer & Prescription Cost Analysis
| # | Question |
|---|----------|
| 28 | Compare prescription costs by payer — which payer pays the most and which shifts the most cost to patients? |
| 29 | What is the average patient copay by payer and drug class? |
| 30 | Which payers bear the highest cost burden for chronic medications? |
| 31 | Show me which providers prescribe the most to each payer — the provider-payer prescription network. |
| 32 | Which payers have the lowest collection rate (paid vs billed)? Show the reimbursement gap. |

### Cross-Domain Analytics
| # | Question |
|---|----------|
| 33 | Show me patients who were readmitted AND are non-adherent to their medications with their risk scores and drug class. |
| 34 | Show me denied claims with their primary diagnosis. |
| 35 | Which encounters are linked to denied claims? |
| 36 | For patients in high-poverty zip codes, how does denial rate compare to the overall population? |
| 37 | Show me high-risk readmission patients who are also medication non-adherent — what payers cover them and what's the cost? |

---

## Graph Agent (Healthcare Ontology Agent)

Open **Healthcare Ontology Agent** in your Fabric workspace. This agent navigates the Healthcare_Demo_Ontology_HLS graph — tracing relationships between providers, payers, claims, encounters, and patients. Unlike HealthcareHLSAgent (which does SQL aggregations), the Graph Agent excels at **entity profiles**, **relationship traversal**, and **network exploration**.

> **When to use which agent:**
> - **HealthcareHLSAgent** → "How many?", "What rate?", "Top 10", "Monthly trend" (aggregations)
> - **Healthcare Ontology Agent** → "Show me this provider's network", "Which payers denied these claims?", "Trace this claim end-to-end" (traversals)

> **Performance tip:** For "full profile" questions (e.g., Q1 below), the agent runs **separate queries** per relationship type (encounters, claims, prescriptions) rather than one giant query. This avoids Cartesian products that would create millions of intermediate rows and hang. Always specify a provider_id, name, or specialty to help the agent filter early.

> **Warm-up tip (IMPORTANT for demos):** The Graph Agent runs on top of an OpenAI assistant thread. The first 1–2 calls after publishing spin up that thread and load the GQL examples into context. Cold-starting straight into a complex aggregation can fail with `submit_tool_outputs failed` or "An error occurred". **Always warm up first** with two simple list queries before the headline question:
>
> 1. `List 5 providers`
> 2. `Show me 5 patients`
> 3. `Which patients have the most non-adherent drug classes?` (or any aggregation)
> 4. Drill in on a specific patient from step 3.

### Provider Operations & Network
| # | Question |
|---|----------|
| 1 | Show me a full provider profile — patients treated, encounters, claims submitted, and prescriptions written. (You can specify a provider_id, name, or specialty — or let the agent pick one.) |
| 2 | Which providers have submitted claims that were denied? Show each provider with their denied claims, denial reasons, payers, and patients. |
| 3 | Pick a provider and show me their patient panel — encounters, diagnoses, and outcomes for their patients. |
| 4 | Show me prescribing patterns for providers in a given specialty — what medications, which patients, and which payers cover the prescriptions? (Try Cardiology, Internal Medicine, etc.) |
| 5 | Which providers treat patients with high readmission risk? Show the provider, their high-risk patients, and conditions. |
| 6 | Which providers serve patients in socially vulnerable communities? Show the SDOH profile alongside provider details. |

### Payer Portfolio & Coverage
| # | Question |
|---|----------|
| 7 | Show me all claims for a specific payer — which providers submitted them, which patients, and what were the outcomes? (Name any payer or let the agent list them.) |
| 8 | Which payers have the most denied claims? For each payer show the denials with provider, patient, encounter, and denial reason. |
| 9 | What prescriptions does each payer cover? Show the medication, prescribing provider, and patient. |
| 10 | Show me the provider network that bills a specific payer — which providers submit claims and for what encounter types? |
| 11 | Which payers cover patients in high SDOH risk areas? Show the payer, the patients, and their social risk factors. |

### Claims & Financial Investigation
| # | Question |
|---|----------|
| 12 | Trace a claim end-to-end — from patient through encounter, provider, and payer. (Provide a claim_id or say "pick a denied claim".) |
| 13 | Show me all denied claims with the complete story — which provider submitted, to which payer, for which patient, and why denied. |
| 14 | What claims are linked to a specific encounter? Show the financial picture including payer and provider. |
| 15 | Show me denied claims for high-risk readmission patients — which providers and payers are involved. |
| 16 | Show me high-cost encounters — who was the provider, what diagnoses, what payer, and was the claim denied. |

### Provider-Payer Network Analysis
| # | Question |
|---|----------|
| 17 | How are providers and payers connected through claims? Show me the provider-payer claims network. |
| 18 | Which chronic medications are prescribed across the provider network and which payers reimburse them. |
| 19 | Show me the relationship between claim denials and patient diagnoses — what conditions are associated with denied claims. |
| 20 | Trace prescription costs by payer — which payers bear the highest prescription burden and for what drug classes. |

### Clinical Operations & Interventions
| # | Question |
|---|----------|
| 21 | Show me patients with poor medication adherence — who are their providers and which payers cover them? These are intervention opportunities. |
| 22 | Show me which payers cover prescriptions for each drug class — break down total cost, payer-paid, and patient copay. |
| 23 | Find our most vulnerable population — patients in high SDOH risk areas who are non-adherent with high readmission risk. Show providers and payers involved. |
| 24 | What is the overall denial rate by payer or the average cost per provider across the network? |

### Prescription-Payer Relationship Traversal
| # | Question |
|---|----------|
| 25 | For a specific patient, trace all their prescriptions — which provider prescribed each, which payer covers it, and what's the patient copay? |
| 26 | Which payers cover the highest-cost specialty medications? Show the drug, prescribing provider, and payer for the top 20 most expensive prescriptions. |
| 27 | Show me the full prescription lifecycle for a chronic medication — from prescribing provider to payer reimbursement to patient out-of-pocket cost. |
| 28 | Which providers prescribe the most non-generic medications and which payers are absorbing that cost? |
| 29 | For patients in food desert zip codes, trace their prescriptions — are payers covering their chronic medications adequately or are copays high? |

> **Note:** Question 24 involves aggregation — the graph agent will explain that rates/rankings require HealthcareHLSAgent and offer to explore specific payers or providers instead. This demonstrates the agent's ability to redirect to the right tool.

---

## RTI Data Agent Questions

These questions work with the RTI scoring tables after running the Real-Time Intelligence notebooks:

| # | Question |
|---|----------|
| 31 | Which providers have the highest fraud scores? Show the top 10 with their fraud flags and total flagged claim amounts. |
| 32 | How many claims are in each fraud risk tier (CRITICAL, HIGH, MEDIUM, LOW)? |
| 33 | Which facilities have the most open care gap alerts? Show the breakdown by HEDIS measure. |
| 34 | How many patients have CRITICAL care gap alerts? Which measures are most overdue? |
| 35 | Which members are on an accelerating cost trajectory? Show their 30-day spend, ED visits, and readmission status. |
| 36 | What is the total flagged claim amount for claims with geographic anomaly fraud patterns? |
| 37 | Show me the overlap between high-cost members and those with open care gaps. |

---

## Operations Agent Questions (Planned)

These questions are planned for the Operations Agent (Use Case 4 — architecture stub):

| # | Question |
|---|----------|
| 38 | What are today's top 10 priorities across all alert types? |
| 39 | Which facilities have the most CRITICAL alerts right now? |
| 40 | Is the claims pipeline running on time? When was the last event ingested? |
| 41 | Show me patients who triggered both fraud and high-cost alerts simultaneously. |
| 42 | How many CRITICAL alerts are unresolved from the last 24 hours? |
| 43 | What is the average time between event ingestion and alert generation? |

---

## Azure AI Foundry Agent Questions

If you've set up the optional **Foundry Orchestrator Agent** (see [FOUNDRY_IQ_SETUP_GUIDE.md](FOUNDRY_IQ_SETUP_GUIDE.md)), it combines the Data Agent's live numbers with 21 clinical knowledge documents and web search. It provides richer, cited responses with clinical context.

### Clinical Decision Support
| # | Question |
|---|----------|
| 1 | Which patients are at highest risk for readmission and what clinical interventions does the readmission prevention protocol recommend for each risk tier? |
| 2 | For our diabetic patients who are non-adherent to Metformin, what does the ADA stepwise therapy guideline recommend as next steps? |
| 3 | What CHF patients have a readmission risk score above 0.7 and what does the GDMT protocol recommend for their discharge planning? |
| 4 | Show me COPD patients who were readmitted within 30 days. What does the GOLD staging guideline say about their exacerbation management? |

### Denial Management with Policy Context
| # | Question |
|---|----------|
| 5 | What are our top denial reasons and what does the Root Cause Analysis Framework say about corrective actions for each? |
| 6 | For claims denied due to missing prior authorization, what are the PA requirements by payer and what is the appeal success rate? |
| 7 | Show me the highest-value denied claims from BlueCross. What does the Appeal Process Guide recommend for Level 1 appeals? |
| 8 | What does the Clean Claim Checklist say we should verify before submitting emergency encounter claims? |

### Quality Measures & Star Ratings
| # | Question |
|---|----------|
| 9 | What is our current medication adherence rate for the CMS Star triple-weighted measures (diabetes, RAS, statins) and how far are we from the 4-star cut point? |
| 10 | Based on our HEDIS measures data, which quality gaps should we prioritize to improve our Star Rating? |
| 11 | What is our estimated HRRP penalty exposure based on current readmission rates for CHF and COPD? Compare to the CMS penalty threshold. |

### Population Health & SDOH
| # | Question |
|---|----------|
| 12 | For patients in food desert zip codes with high SVI, what SDOH-informed readmission prevention interventions does the protocol recommend? |
| 13 | Which patient populations should we target for care management outreach based on combined readmission risk, adherence gaps, and social vulnerability? |
| 14 | What does the clinical documentation standard require for E&M coding on high-complexity inpatient encounters, and are our providers meeting it? |

### Formulary & Prescription Management
| # | Question |
|---|----------|
| 15 | Which patients are on non-formulary medications? What does the therapeutic interchange policy recommend as alternatives? |
| 16 | What are the step therapy requirements for our diabetic patients on GLP-1 agonists? Which payers require step therapy failure documentation? |
| 17 | Show me specialty drug authorization requirements for Insulin Glargine by payer. Which patients might need reauthorization soon? |

### Provider Network & Contracts
| # | Question |
|---|----------|
| 18 | Which payers have the lowest reimbursement rates for our highest-volume CPT codes? What does the contract guide say about negotiation priorities? |
| 19 | Do we have any network adequacy gaps based on CMS time/distance standards? Which specialties are underserved? |
| 20 | Compare our collection rates against contracted rates by payer. Where are we seeing the most revenue leakage? |
