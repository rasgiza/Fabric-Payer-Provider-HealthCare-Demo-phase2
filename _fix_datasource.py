import json, pathlib

for variant in ['published', 'draft']:
    f = pathlib.Path(f'data_agents/Healthcare Ontology Agent.DataAgent/Files/Config/{variant}/graph-Healthcare_Demo_Graph/datasource.json')
    data = json.loads(f.read_text(encoding='utf-8'))
    
    # Fix dataSourceInstructions
    dsi = data['dataSourceInstructions']
    
    # 1. Update entity/relationship count
    dsi = dsi.replace('11 entity types, 17 relationships', '12 entity types, 18 relationships')
    
    # 2. Add Vitals entity type after MedicationAdherence
    if 'Vitals' not in dsi:
        dsi = dsi.replace(
            "MedicationAdherence — patient medication adherence (PK: patient_key + medication_key)",
            "MedicationAdherence — patient medication adherence (PK: patient_key + medication_key)\n  Vitals — patient vital signs: avg_heart_rate, avg_bp_systolic, avg_spo2, avg_temperature, risk_flag (PK: patient_key)"
        )
    
    # 3. Add Vitals relationship after MedicationAdherence relationships
    if 'vitalsTakenFor' not in dsi:
        dsi = dsi.replace(
            "MedicationAdherence → Medication (adherenceMedication, medication_key) — adherence for which medication",
            "MedicationAdherence → Medication (adherenceMedication, medication_key) — adherence for which medication\n  Vitals → Patient (vitalsTakenFor, patient_key) — vital signs for which patient"
        )
    
    # 4. Remove hardcoded 'Dr. Smith' from WHEN TO USE
    dsi = dsi.replace(
        "What medications does Dr. Smith prescribe?",
        "What medications does [provider] prescribe?"
    )
    
    data['dataSourceInstructions'] = dsi
    
    # Fix userDescription
    ud = data['userDescription']
    ud = ud.replace('11 entity types and 17 relationships', '12 entity types and 18 relationships')
    data['userDescription'] = ud
    
    # Add Vitals element if missing
    has_vitals = any(e.get('display_name') == 'Vitals' for e in data.get('elements', []))
    if not has_vitals:
        data['elements'].append({
            "id": "e0000012-0012-0012-0012-000000000012",
            "is_selected": True,
            "display_name": "Vitals",
            "type": "graph.nodeType",
            "description": "Patient vital signs with avg heart rate, BP systolic, SpO2, temperature, and risk flag",
            "children": []
        })
    
    # Add vitalsTakenFor edge if missing
    has_vtf = any(e.get('display_name') == 'vitalsTakenFor' for e in data.get('elements', []))
    if not has_vtf:
        data['elements'].append({
            "id": "r0000018-0018-0018-0018-000000000018",
            "is_selected": True,
            "display_name": "vitalsTakenFor",
            "type": "graph.edgeType",
            "description": "Vitals taken for which patient",
            "children": []
        })
    
    f.write_text(json.dumps(data, indent=2), encoding='utf-8')
    
    # Verify
    final = json.loads(f.read_text(encoding='utf-8'))
    dsi2 = final['dataSourceInstructions']
    ud2 = final['userDescription']
    checks = [
        ('12 entity types, 18 relationships' in dsi2, '12 entities in dataSourceInstructions'),
        ('12 entity types and 18 relationships' in ud2, '12 entities in userDescription'),
        ('Vitals' in dsi2, 'Vitals in dataSourceInstructions'),
        ('vitalsTakenFor' in dsi2, 'vitalsTakenFor in dataSourceInstructions'),
        ('Dr. Smith' not in dsi2, 'no hardcoded Dr. Smith'),
        any(e.get('display_name') == 'Vitals' for e in final['elements']),
        any(e.get('display_name') == 'vitalsTakenFor' for e in final['elements']),
    ]
    labels = [
        '12 entities in dataSourceInstructions',
        '12 entities in userDescription',
        'Vitals in dataSourceInstructions',
        'vitalsTakenFor in dataSourceInstructions',
        'no hardcoded Dr. Smith',
        'Vitals element exists',
        'vitalsTakenFor edge exists',
    ]
    for i, c in enumerate(checks):
        ok = c[0] if isinstance(c, tuple) else c
        label = c[1] if isinstance(c, tuple) else labels[i]
        status = 'PASS' if ok else 'FAIL'
        print(f'{variant} - {status}: {label}')
