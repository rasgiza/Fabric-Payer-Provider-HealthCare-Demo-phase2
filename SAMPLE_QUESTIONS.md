# Sample Questions — Healthcare Data Agents

## Fabric Data Agent (HealthcareHLSAgent)

Open **HealthcareHLSAgent** in your Fabric workspace. The Data Agent queries the Gold lakehouse star schema directly. Copy-paste any of these to get started:

### Claim Denials & Revenue Cycle
| # | Question |
|---|----------|
| 1 | What is the overall claim denial rate and how does it break down by payer? |
| 2 | What are the top 5 denial reasons by claim count and total billed amount? |
| 3 | Which providers have the highest denial rates? Show the top 10 with their specialties. |
| 4 | Show me all denied claims over $50,000 — include the patient, payer, denial reason, and billed amount. |
| 5 | What is the total revenue at risk from claims flagged as high denial risk that haven't been denied yet? |
| 6 | Compare the collection rate (paid / billed) across all payers. Which payer reimburses the least? |
| 7 | How many claims are pending vs paid vs denied this month? Show the month-to-date trend. |

### Readmissions & Patient Risk
| # | Question |
|---|----------|
| 8 | What is the 30-day readmission rate? How does it trend by month over the past year? |
| 9 | List the top 10 patients with the highest readmission risk scores. Include their age, insurance type, and number of prior encounters. |
| 10 | What is the readmission rate by encounter type (inpatient, outpatient, emergency)? |
| 11 | Which facilities have the highest readmission rates? |
| 12 | Show me the average length of stay for high-risk vs medium-risk vs low-risk readmission patients. |
| 13 | How many patients are in each readmission risk category (high, medium, low)? |

### Medication Adherence
| # | Question |
|---|----------|
| 14 | How many patients are non-adherent (PDC < 0.8) for diabetes medications? |
| 15 | What is the average PDC score by drug class? Which therapeutic area has the worst adherence? |
| 16 | List patients who are on chronic medications and have PDC scores below 0.5 — include their drug class and gap days. |
| 17 | What is the adherent vs non-adherent member count for statin therapy? |
| 18 | Show the total medication cost by therapeutic area. Which drug class costs the most? |

### Social Determinants of Health (SDOH)
| # | Question |
|---|----------|
| 19 | How many patients live in zip codes with a social vulnerability index above 0.75? |
| 20 | What is the average readmission risk score for patients in high SDOH risk tier vs low risk tier? |
| 21 | Show me zip codes flagged as food deserts — how many patients are in each? |
| 22 | What is the denial rate for patients in high-poverty zip codes vs low-poverty zip codes? |

### Encounters & Providers
| # | Question |
|---|----------|
| 23 | What is the average length of stay by encounter type? |
| 24 | Which providers have the highest total charges? Show the top 10 with specialty and department. |
| 25 | What are the top 10 most frequent ICD diagnoses across all encounters? |
| 26 | How many encounters occurred this year by month? Show the trend. |
| 27 | Which patients have had the most encounters in the past 12 months? |

### Cross-Domain Analytics
| # | Question |
|---|----------|
| 28 | Show me patients who were readmitted AND are non-adherent to their medications. Include their risk scores and drug class. |
| 29 | For patients in high SDOH risk zip codes, what is their average denial rate compared to the overall population? |
| 30 | Which chronic conditions have the highest readmission rates? Show the top 5 ICD categories with readmission counts. |

---

## Graph Agent (HealthcareGraphAgent)

Open **HealthcareGraphAgent** in your Fabric workspace. This agent navigates the Healthcare_Demo_Ontology_HLS graph — tracing entity relationships, following care pathways, and telling patient stories. Unlike HealthcareHLSAgent (which does SQL aggregations), the Graph Agent excels at **entity lookups** and **relationship traversal**.

> **When to use which agent:**
> - **HealthcareHLSAgent** → "How many?", "What rate?", "Top 10", "Monthly trend" (aggregations)
> - **HealthcareGraphAgent** → "What happened to this patient?", "Who treated them?", "How are these entities connected?" (traversals)

### Patient Care Stories
| # | Question |
|---|----------|
| 1 | Tell me the full story of patient PAT0000001 — encounters, diagnoses, medications, claims, providers, everything. |
| 2 | Show me the full care pathway for patient PAT0000005 — from their first encounter through all claims, prescriptions, and diagnoses. |
| 3 | Give me the full clinical picture of patient PAT0000040 — vitals, medications, adherence, diagnoses, and encounters. |
| 4 | A patient was readmitted within days of discharge. Walk me through what happened — the index admission, discharge, readmission, diagnoses, claims, and who was involved. |

### Clinical Context & Vitals
| # | Question |
|---|----------|
| 5 | What were the vitals for patient PAT0000012? Were any abnormal? |
| 6 | What diagnoses does patient PAT0000010 have? Are any of them chronic? |
| 7 | I have a patient named Elizabeth Brown, age 62. What medications is she on, who prescribed them, and is she adherent? |
| 8 | Is patient PAT0000025 adherent to their medications? Show me their adherence by drug class. |

### Provider & Encounter Relationships
| # | Question |
|---|----------|
| 9 | Who is the provider that treated patient PAT0000050 and what other patients have they treated? |
| 10 | Which patients are connected to Dr. Smith? Show me all their encounters and outcomes. |
| 11 | Show me everything connected to encounter ENC0005678 — I want the complete picture. |
| 12 | What prescriptions originated from inpatient encounters for patient PAT0000008? |

### Claims, Payers & Financial Traversal
| # | Question |
|---|----------|
| 13 | Which payer covers the claims for patient PAT0000015 and were any denied? |
| 14 | What claims are linked to encounter ENC0001234 and what's the financial picture? |
| 15 | Trace the complete journey of claim CLM0009999 from patient to payer. |
| 16 | Show me denied claims and the full story behind each — who submitted, what patient, what encounter, what payer. |

### Risk & Intervention Targets
| # | Question |
|---|----------|
| 17 | Show me patients with high readmission risk and their full clinical context. |
| 18 | Find patients with high readmission risk who live in high SDOH risk areas — these are our most vulnerable. |
| 19 | Show me patients who are non-adherent to chronic medications and also have high readmission risk — these are our biggest intervention targets. |
| 20 | Which payers are associated with denied claims for high-risk readmission patients? |

### SDOH & Community Health
| # | Question |
|---|----------|
| 21 | What is the SDOH profile for patient PAT0000030 and how might it affect their care? |
| 22 | I want to understand the relationship between a patient's diagnoses and their claims — do chronic conditions lead to more denials? |

### Medication Prescribing Patterns
| # | Question |
|---|----------|
| 23 | Which providers prescribe chronic medications and to which patients? |
| 24 | Tell me about the sickest patients — who has the most encounters, most diagnoses, and highest costs? |

> **Note:** Question 24 involves aggregation — the graph agent will explain that counts/rankings require HealthcareHLSAgent and offer to show individual patient stories instead. This demonstrates the agent's ability to redirect to the right tool.

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
