import json, pathlib

f = pathlib.Path(r'data_agents/Healthcare Ontology Agent.DataAgent/Files/Config/published/stage_config.json')
data = json.loads(f.read_text(encoding='utf-8'))
txt = data['aiInstructions']

# 1. Replace hardcoded 'Smith' lookup with list-first pattern
old_lookup = "Lookup by name:\n  MATCH (p:Provider) WHERE p.display_name CONTAINS 'Smith' RETURN p LIMIT 5"
new_lookup = (
    "List providers (for 'show me providers' / 'which doctors'):\n"
    "  MATCH (p:Provider) RETURN p.display_name, p.specialty, p.department LIMIT 20\n"
    "  Present as a numbered list. If the user names a provider, use CONTAINS to filter:\n"
    "  MATCH (p:Provider) WHERE p.display_name CONTAINS '<user_input>' RETURN p LIMIT 5"
)
txt = txt.replace(old_lookup, new_lookup)

# 2. Replace hardcoded 'Aetna' with list-first pattern
old_agg = "Bounded aggregation (scoped to a specific payer):\n  MATCH (c:Claim)-[:ClaimHasPayer]->(pay:Payer) WHERE pay.payer_name = 'Aetna' RETURN c.denial_flag, COUNT(c) AS claim_count LIMIT 20"
new_agg = (
    "List payers (for 'show me payers' / 'which insurers'):\n"
    "  MATCH (pay:Payer) RETURN pay.payer_name, pay.payer_type LIMIT 20\n"
    "  Present as a numbered list. Once the user picks a payer, drill down:\n"
    "\n"
    "Bounded aggregation (scoped to a user-selected payer):\n"
    "  MATCH (c:Claim)-[:ClaimHasPayer]->(pay:Payer) WHERE pay.payer_name = '<user_selected_payer>' RETURN c.denial_flag, COUNT(c) AS claim_count LIMIT 20"
)
txt = txt.replace(old_agg, new_agg)

# 3. Replace hardcoded 'Elizabeth Brown' with placeholder
txt = txt.replace(
    "WHERE pat.first_name = 'Elizabeth' AND pat.last_name = 'Brown'",
    "WHERE pat.first_name = '<first_name>' AND pat.last_name = '<last_name>'"
)

# 4. Add INTERACTION PATTERN section
interaction = (
    "INTERACTION PATTERN -- list first, user picks, then drill down:\n"
    "When the user asks a broad question (e.g., 'show me providers', 'which patients', 'what payers'), ALWAYS:\n"
    "1. Run a list query first (LIMIT 20) showing key identifying fields (name, specialty, age, etc.).\n"
    "2. Present as a numbered list so the user can pick by name or number.\n"
    "3. Once the user picks, run the detail/drill-down query for that specific entity.\n"
    "NEVER assume a specific entity -- always let the user choose from the list.\n"
    "\n"
)
txt = txt.replace('AGGREGATION GUIDELINES:', interaction + 'AGGREGATION GUIDELINES:')

# Write published
data['aiInstructions'] = txt
f.write_text(json.dumps(data, indent=2), encoding='utf-8')

# Write draft
draft = pathlib.Path(r'data_agents/Healthcare Ontology Agent.DataAgent/Files/Config/draft/stage_config.json')
dd = json.loads(draft.read_text(encoding='utf-8'))
dd['aiInstructions'] = txt
draft.write_text(json.dumps(dd, indent=2), encoding='utf-8')

# Verify
checks = [
    ('<user_input>' in txt, 'provider lookup uses placeholder'),
    ('<user_selected_payer>' in txt, 'payer uses placeholder'),
    ('<first_name>' in txt, 'patient adherence uses placeholder'),
    ('INTERACTION PATTERN' in txt, 'interaction pattern added'),
    ('List providers' in txt, 'list providers example'),
    ('List payers' in txt, 'list payers example'),
    ('List patients' in txt, 'list patients example'),
    ('Smith' not in txt, 'no hardcoded Smith'),
    ('Aetna' not in txt, 'no hardcoded Aetna'),
    ('Elizabeth' not in txt, 'no hardcoded Elizabeth'),
]
for ok, label in checks:
    status = 'PASS' if ok else 'FAIL'
    print(status + ': ' + label)
