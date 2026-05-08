# CSV Schema (Fabric Ontology v1, May 2026)

Five CSVs define the ontology. Business stakeholders own them. A
builder script (`scripts/build_ontology_from_csv.py`) reads them and
emits the deployable Fabric ontology folder.

## Locked vocabulary

| Item | Allowed values | Source |
|---|---|---|
| `value_type` | `String`, `BigInt`, `Double`, `DateTime`, `Boolean` | Fabric Ontology v1 — no other values are accepted by the API |
| `cardinality` | `OneToOne`, `OneToMany`, `ManyToOne`, `ManyToMany` | Internal convention; builder maps to Fabric link semantics |

## Naming rules

- `entity_name`, `property_name`, `relationship_name` use **PascalCase**.
- Business CSVs (`entities.csv`, `properties.csv`, `relationships.csv`)
  **never** contain physical table prefixes like `fact_` or `dim_`.
  Physical table names live ONLY in `*_bindings.csv`.
- `business_description` is plain English, not Markdown. **Per-entity
  ≤25 words; per-relationship ≤20 words.** Flows into the Data Agent's
  `datasource.json` (`dataSourceInstructions`, `userDescription`),
  NOT into Fabric ontology JSON (v1 has no description field).

## Entities

`csv/.../entities.csv`

| Column | Type | Notes |
|---|---|---|
| `entity_name` | str | PascalCase, unique. Example: `Patient` |
| `key_property` | str | Property used as primary key. Example: `patient_id` |
| `display_property` | str | Property used as the human-readable label. Example: `full_name` |
| `business_description` | str | ≤25 words. Plain English. |
| `domain_group` | str | Free-form bucket. Example: `Clinical`, `RevenueCycle`, `Operations` |

Example:

```csv
entity_name,key_property,display_property,business_description,domain_group
Patient,patient_id,full_name,A person receiving care.,Clinical
Encounter,encounter_id,encounter_id,A single clinical visit.,Clinical
Claim,claim_id,claim_id,A billed instance of services rendered.,RevenueCycle
```

## Properties

`csv/.../properties.csv`

| Column | Type | Notes |
|---|---|---|
| `entity_name` | str | Must exist in `entities.csv` |
| `property_name` | str | snake_case OR PascalCase. Stable. |
| `value_type` | str | One of the locked vocabulary values |
| `business_description` | str | ≤20 words. Plain English. |
| `unit` | str | Free text or empty. Examples: `USD`, `days`, `count`, `pct`, `mmHg` |

Example:

```csv
entity_name,property_name,value_type,business_description,unit
Patient,patient_id,String,Internal patient identifier.,
Patient,date_of_birth,DateTime,Patient date of birth.,
Encounter,los_days,Double,Length of stay in days.,days
Claim,billed_amount,Double,Amount billed to the payer.,USD
```

## Relationships

`csv/.../relationships.csv`

| Column | Type | Notes |
|---|---|---|
| `relationship_name` | str | PascalCase. Convention: `<Verb>For<Anchor>` or `<Has><Target>` |
| `source_entity` | str | Must exist in `entities.csv` |
| `target_entity` | str | Must exist in `entities.csv` |
| `business_description` | str | ≤20 words |
| `cardinality` | str | One of `OneToOne`, `OneToMany`, `ManyToOne`, `ManyToMany` |

Example:

```csv
relationship_name,source_entity,target_entity,business_description,cardinality
PatientHasEncounter,Patient,Encounter,Patient experienced a clinical visit.,OneToMany
EncounterGeneratesClaim,Encounter,Claim,Encounter produced one or more billable claims.,OneToMany
```

## Entity bindings

`csv/.../entity_bindings.csv`

| Column | Type | Notes |
|---|---|---|
| `entity_name` | str | Must exist in `entities.csv` |
| `source_table` | str | Physical Delta table name. May contain `dim_`/`fact_`/`agg_` prefixes |
| `property_name` | str | Must exist in `properties.csv` for this entity |
| `source_column` | str | Physical column in `source_table` |

The key column does not need its own row — the builder derives the
key binding from `entities.csv.key_property` and the matching row
here. Every property listed in `properties.csv` for an entity SHOULD
have a binding row; missing rows produce a builder warning.

Example:

```csv
entity_name,source_table,property_name,source_column
Patient,dim_patient,patient_id,patient_key
Patient,dim_patient,full_name,patient_full_name
Encounter,fact_encounter,encounter_id,encounter_key
Encounter,fact_encounter,los_days,length_of_stay_days
```

## Relationship bindings

`csv/.../relationship_bindings.csv`

Single-column FK joins only in v1. Multi-column joins are out of scope.

| Column | Type | Notes |
|---|---|---|
| `relationship_name` | str | Must exist in `relationships.csv` |
| `source_table` | str | Physical table for `source_entity` |
| `source_join_column` | str | Column on `source_table` that holds the FK |
| `target_join_column` | str | Column on the target entity's bound table |

The target table is derived from the target entity's binding rows.
Builder cross-checks that `target_join_column` matches the target
entity's `key_property` binding.

Example:

```csv
relationship_name,source_table,source_join_column,target_join_column
PatientHasEncounter,fact_encounter,patient_key,patient_key
EncounterGeneratesClaim,fact_claim,encounter_key,encounter_key
```

## Validation rules (enforced by builder)

1. Every `entity_name` in `properties.csv`, `relationships.csv`,
   `entity_bindings.csv`, and `relationship_bindings.csv` exists in
   `entities.csv`.
2. Every `property_name` in `entity_bindings.csv` exists in
   `properties.csv` for the same entity.
3. Every `value_type` in `properties.csv` is in the locked vocabulary.
4. No duplicate `(entity_name, property_name)` rows.
5. No duplicate `entity_name` or `relationship_name` rows.
6. Every entity has exactly one `key_property` row in
   `entity_bindings.csv`.
7. `business_description` length: entity ≤25 words, relationship and
   property ≤20 words. Builder warns and truncates.
8. Business CSVs must not contain `fact_` / `dim_` / `agg_` substrings
   in `entity_name`, `property_name`, or `relationship_name`.

Validation runs as the first cell of `csv_ontology_setup.ipynb` and
also as a CI check via `scripts/build_ontology_from_csv.py --validate`.
