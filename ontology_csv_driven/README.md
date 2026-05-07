# ontology_csv_driven

CSV-driven ontology authoring layer for the Microsoft Fabric
Healthcare demo. Decouples ontology design from the Power BI semantic
model so business stakeholders own five CSVs (entities, properties,
relationships, entity_bindings, relationship_bindings) and a builder
turns them into a deployable Fabric ontology.

Layout:

| Folder | Purpose |
|---|---|
| `csv/healthcare_seed/` | Five CSVs extracted from `Healthcare_Demo_Ontology_HLS` (Phase 2 seed) |
| `csv/example_generated/` | Five CSVs produced by the Copilot AI-assist prompt (Phase 5) |
| `scripts/` | `extract_ontology_to_csv.py`, `build_ontology_from_csv.py`, `export_to_csv.py` |
| `notebooks/` | `csv_ontology_setup.ipynb` (Fabric REST builder) |
| `prompts/` | Copilot prompt + per-agent instruction files |
| `docs/` | `csv_schema.md` — schema spec authored in Phase 1 |

Builder writes the deployable ontology to
`ontology/Healthcare_Demo_Ontology_CSV/` at repo root; the launcher's
Cell 9b deploys it from there alongside the existing
`Healthcare_Demo_Ontology_HLS`.
