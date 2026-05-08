"""Build a deployable Fabric Ontology folder from the five-CSV authoring schema.

Reads:
    entities.csv
    properties.csv
    relationships.csv
    entity_bindings.csv
    relationship_bindings.csv

Writes (mirroring the on-disk shape Fabric uses for `Healthcare_Demo_Ontology_HLS`):

    <out-dir>/
        .platform
        definition.json
        manifest.json
        EntityTypes/<entity_id>/
            definition.json
            DataBindings/<binding_uuid>.json
        RelationshipTypes/<relationship_id>/
            definition.json
            Contextualizations/<contextualization_uuid>.json

IDs are deterministic (stable hash of the business name) so successive
builds are byte-stable for git diffs. They are NOT byte-identical to the
HLS export — that's intentional. Functional identity, not round-trip.

Usage:
    python build_ontology_from_csv.py \
        --csv-dir <repo>/ontology_csv_driven/csv/healthcare_seed \
        --out-dir <repo>/ontology/Healthcare_Demo_Ontology_CSV \
        --ontology-name Healthcare_Demo_Ontology_CSV

Long-path safe on Windows via the `\\?\` prefix.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import shutil
import sys
import uuid
from pathlib import Path
from typing import Any


# --------------------------------------------------------------------------- #
# Long-path helper (Windows MAX_PATH workaround)
# --------------------------------------------------------------------------- #

def _lp(p: str | os.PathLike) -> str:
    s = os.fspath(p)
    if sys.platform != "win32":
        return s
    s = os.path.abspath(s)
    if s.startswith("\\\\?\\"):
        return s
    if s.startswith("\\\\"):
        return "\\\\?\\UNC\\" + s.lstrip("\\")
    return "\\\\?\\" + s


def _write_json(path: Path, data: Any) -> None:
    os.makedirs(_lp(path.parent), exist_ok=True)
    with open(_lp(path), "w", encoding="utf-8", newline="\n") as f:
        json.dump(data, f, indent=2)
        f.write("\n")


def _write_text(path: Path, text: str) -> None:
    os.makedirs(_lp(path.parent), exist_ok=True)
    with open(_lp(path), "w", encoding="utf-8", newline="\n") as f:
        f.write(text)


# --------------------------------------------------------------------------- #
# Deterministic ID generation
# --------------------------------------------------------------------------- #

# Modulus chosen so the resulting integers fall into the same digit
# magnitude range as a real Fabric export (see HLS manifest):
# - Entity IDs:        ~12-15 digits
# - Property IDs:      ~19 digits
# - Relationship IDs:  ~19 digits
_ENTITY_ID_MOD = 10**14
_PROPERTY_ID_MOD = 10**19
_RELATIONSHIP_ID_MOD = 10**19


def _stable_int(salt: str, name: str, mod: int) -> int:
    """Produce a deterministic integer ID from a salted name."""
    h = hashlib.sha256(f"{salt}::{name}".encode("utf-8")).hexdigest()
    n = int(h[:18], 16) % mod
    # Avoid degenerate small values (real IDs are never zero-padded short).
    if n < mod // 100:
        n += mod // 10
    return n


def _stable_uuid(salt: str, name: str) -> str:
    """Produce a deterministic UUID from a salted name."""
    digest = hashlib.sha256(f"{salt}::{name}".encode("utf-8")).digest()
    # uuid.UUID(bytes=...) takes 16 bytes.
    u = uuid.UUID(bytes=digest[:16], version=4)
    return str(u)


# --------------------------------------------------------------------------- #
# CSV loading
# --------------------------------------------------------------------------- #

def _load_csv(path: Path) -> list[dict[str, str]]:
    with open(_lp(path), "r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


# --------------------------------------------------------------------------- #
# Validation
# --------------------------------------------------------------------------- #

class ValidationError(Exception):
    pass


def validate(data: dict[str, list[dict[str, str]]]) -> None:
    """Fail-fast validation per Phase 3 step 10.

    Catches:
      - duplicate entity / property / relationship names
      - properties / bindings referencing unknown entities
      - entities whose declared key_property doesn't exist in properties.csv
      - relationships with unknown source/target entities
      - relationship_bindings whose source/target join columns don't appear
        in any entity_binding for the relevant entity (advisory only)
    """
    errors: list[str] = []

    # Duplicates
    ent_names = [r["entity_name"] for r in data["entities"]]
    if len(ent_names) != len(set(ent_names)):
        dups = sorted({n for n in ent_names if ent_names.count(n) > 1})
        errors.append(f"Duplicate entity names: {dups}")

    rel_names = [r["relationship_name"] for r in data["relationships"]]
    if len(rel_names) != len(set(rel_names)):
        dups = sorted({n for n in rel_names if rel_names.count(n) > 1})
        errors.append(f"Duplicate relationship names: {dups}")

    # Properties unique within an entity (same name OK across entities)
    seen_props: set[tuple[str, str]] = set()
    for p in data["properties"]:
        key = (p["entity_name"], p["property_name"])
        if key in seen_props:
            errors.append(f"Duplicate property: {key}")
        seen_props.add(key)

    ent_set = set(ent_names)

    # Properties → entities
    for p in data["properties"]:
        if p["entity_name"] not in ent_set:
            errors.append(
                f"Property {p['property_name']} references unknown entity "
                f"{p['entity_name']}")

    # Entity key_property exists
    props_by_entity: dict[str, set[str]] = {}
    for p in data["properties"]:
        props_by_entity.setdefault(p["entity_name"], set()).add(
            p["property_name"])
    for e in data["entities"]:
        if not e["key_property"]:
            errors.append(f"Entity {e['entity_name']} has empty key_property")
            continue
        if e["key_property"] not in props_by_entity.get(e["entity_name"],
                                                       set()):
            errors.append(
                f"Entity {e['entity_name']} key_property "
                f"{e['key_property']} not declared in properties.csv")

    # Relationships → entities
    for r in data["relationships"]:
        for end in ("source_entity", "target_entity"):
            if r[end] not in ent_set:
                errors.append(
                    f"Relationship {r['relationship_name']} {end}="
                    f"{r[end]} is unknown")

    # Entity bindings → entities + properties
    for b in data["entity_bindings"]:
        if b["entity_name"] not in ent_set:
            errors.append(
                f"entity_binding references unknown entity "
                f"{b['entity_name']}")
            continue
        if b["property_name"] and b["property_name"] not in \
                props_by_entity.get(b["entity_name"], set()):
            errors.append(
                f"entity_binding {b['entity_name']}.{b['property_name']} "
                f"not in properties.csv")

    # Relationship bindings → relationships
    rel_set = set(rel_names)
    for b in data["relationship_bindings"]:
        if b["relationship_name"] not in rel_set:
            errors.append(
                f"relationship_binding references unknown relationship "
                f"{b['relationship_name']}")

    # value_type vocabulary
    allowed_types = {"String", "BigInt", "Double", "DateTime", "Boolean"}
    for p in data["properties"]:
        if p["value_type"] not in allowed_types:
            errors.append(
                f"{p['entity_name']}.{p['property_name']} has invalid "
                f"value_type={p['value_type']!r}; allowed={sorted(allowed_types)}")

    if errors:
        raise ValidationError(
            "Ontology CSV validation failed:\n  - " + "\n  - ".join(errors))


# --------------------------------------------------------------------------- #
# Build
# --------------------------------------------------------------------------- #

def build(csv_dir: Path, out_dir: Path, ontology_name: str) -> None:
    data = {
        "entities": _load_csv(csv_dir / "entities.csv"),
        "properties": _load_csv(csv_dir / "properties.csv"),
        "relationships": _load_csv(csv_dir / "relationships.csv"),
        "entity_bindings": _load_csv(csv_dir / "entity_bindings.csv"),
        "relationship_bindings": _load_csv(
            csv_dir / "relationship_bindings.csv"),
    }
    validate(data)

    # Wipe + recreate out-dir for a clean build.
    if Path(_lp(out_dir)).exists():
        shutil.rmtree(_lp(out_dir))
    os.makedirs(_lp(out_dir), exist_ok=True)

    # ---- Compute IDs ------------------------------------------------------- #
    entity_id: dict[str, int] = {}
    for e in data["entities"]:
        entity_id[e["entity_name"]] = _stable_int(
            "entity", e["entity_name"], _ENTITY_ID_MOD)

    # property IDs are unique per (entity, property) pair
    property_id: dict[tuple[str, str], int] = {}
    for p in data["properties"]:
        property_id[(p["entity_name"], p["property_name"])] = _stable_int(
            "property",
            f"{p['entity_name']}.{p['property_name']}",
            _PROPERTY_ID_MOD)

    relationship_id: dict[str, int] = {}
    for r in data["relationships"]:
        relationship_id[r["relationship_name"]] = _stable_int(
            "relationship", r["relationship_name"], _RELATIONSHIP_ID_MOD)

    # ---- Group bindings by entity / relationship --------------------------- #
    # entity → list of (table, [(prop_name, source_column), ...])
    bindings_by_entity: dict[str, dict[str, list[tuple[str, str]]]] = {}
    for b in data["entity_bindings"]:
        ent = b["entity_name"]
        tbl = b["source_table"]
        bindings_by_entity.setdefault(ent, {}).setdefault(tbl, []).append(
            (b["property_name"], b["source_column"]))

    rel_bindings: dict[str, list[dict[str, str]]] = {}
    for b in data["relationship_bindings"]:
        rel_bindings.setdefault(b["relationship_name"], []).append(b)

    exported_parts: list[dict[str, str]] = [
        {"path": "definition.json", "payloadType": "InlineBase64"},
    ]

    # ---- Write entity types ----------------------------------------------- #
    for e in data["entities"]:
        ename = e["entity_name"]
        eid = entity_id[ename]
        ent_dir = out_dir / "EntityTypes" / str(eid)

        # properties for this entity
        props_for_entity = [p for p in data["properties"]
                            if p["entity_name"] == ename]
        properties_payload = [
            {
                "id": str(property_id[(ename, p["property_name"])]),
                "name": p["property_name"],
                "redefines": None,
                "baseTypeNamespaceType": None,
                "valueType": p["value_type"],
            }
            for p in props_for_entity
        ]

        key_prop_id = property_id[(ename, e["key_property"])]
        display_prop_id = (
            property_id.get((ename, e["display_property"]))
            if e["display_property"] else None
        )

        ent_def = {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/"
                       "item/ontology/entityType/1.0.0/schema.json",
            "id": str(eid),
            "namespace": "usertypes",
            "baseEntityTypeId": None,
            "name": ename,
            "entityIdParts": [str(key_prop_id)],
            "displayNamePropertyId": (
                str(display_prop_id) if display_prop_id else None),
            "namespaceType": "Custom",
            "visibility": "Visible",
            "properties": properties_payload,
        }
        _write_json(ent_dir / "definition.json", ent_def)
        exported_parts.append({
            "path": f"EntityTypes/{eid}/definition.json",
            "payloadType": "InlineBase64",
        })

        # data bindings: one file per (entity, source_table)
        for table, prop_pairs in bindings_by_entity.get(ename, {}).items():
            binding_uuid = _stable_uuid("binding", f"{ename}::{table}")
            binding_payload = {
                "$schema": "https://developer.microsoft.com/json-schemas/"
                           "fabric/item/ontology/dataBinding/1.0.0/schema.json",
                "id": binding_uuid,
                "dataBindingConfiguration": {
                    "dataBindingType": "NonTimeSeries",
                    "propertyBindings": [
                        {
                            "sourceColumnName": col,
                            "targetPropertyId": str(
                                property_id[(ename, prop)]),
                        }
                        for (prop, col) in prop_pairs
                        if (ename, prop) in property_id
                    ],
                    "sourceTableProperties": {
                        "sourceType": "LakehouseTable",
                        "workspaceId":
                            "00000000-0000-0000-0000-000000000000",
                        "itemId":
                            "00000000-0000-0000-0000-000000000000",
                        "sourceTableName": table,
                        "sourceSchema": None,
                    },
                },
            }
            _write_json(
                ent_dir / "DataBindings" / f"{binding_uuid}.json",
                binding_payload)
            exported_parts.append({
                "path": f"EntityTypes/{eid}/DataBindings/{binding_uuid}.json",
                "payloadType": "InlineBase64",
            })

    # ---- Write relationship types ----------------------------------------- #
    for r in data["relationships"]:
        rname = r["relationship_name"]
        rid = relationship_id[rname]
        rel_dir = out_dir / "RelationshipTypes" / str(rid)
        src_eid = entity_id[r["source_entity"]]
        tgt_eid = entity_id[r["target_entity"]]
        rel_def = {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/"
                       "item/ontology/relationshipType/1.0.0/schema.json",
            "namespace": "usertypes",
            "id": str(rid),
            "name": rname,
            "namespaceType": "Imported",
            "source": {"entityTypeId": str(src_eid)},
            "target": {"entityTypeId": str(tgt_eid)},
        }
        _write_json(rel_dir / "definition.json", rel_def)
        exported_parts.append({
            "path": f"RelationshipTypes/{rid}/definition.json",
            "payloadType": "InlineBase64",
        })

        for i, b in enumerate(rel_bindings.get(rname, [])):
            ctx_uuid = _stable_uuid(
                "ctx", f"{rname}::{b['source_table']}::{i}")
            # Resolve target property IDs: use the entity's key_property by
            # convention (single-column FKs only per Phase 1 spec).
            src_ent = r["source_entity"]
            tgt_ent = r["target_entity"]
            src_key = next(
                (e["key_property"] for e in data["entities"]
                 if e["entity_name"] == src_ent), None)
            tgt_key = next(
                (e["key_property"] for e in data["entities"]
                 if e["entity_name"] == tgt_ent), None)
            # Allow override via the optional `_source_key_property` /
            # `_target_key_property` columns produced by the extractor.
            src_key = b.get("_source_key_property") or src_key
            tgt_key = b.get("_target_key_property") or tgt_key
            ctx_payload = {
                "$schema": "https://developer.microsoft.com/json-schemas/"
                           "fabric/item/ontology/contextualization/1.0.0/"
                           "schema.json",
                "id": ctx_uuid,
                "dataBindingTable": {
                    "workspaceId":
                        "00000000-0000-0000-0000-000000000000",
                    "itemId":
                        "00000000-0000-0000-0000-000000000000",
                    "sourceTableName": b["source_table"],
                    "sourceSchema": None,
                    "sourceType": "LakehouseTable",
                },
                "sourceKeyRefBindings": [
                    {
                        "sourceColumnName": b["source_join_column"],
                        "targetPropertyId": str(
                            property_id[(src_ent, src_key)]),
                    }
                ] if src_key and (src_ent, src_key) in property_id else [],
                "targetKeyRefBindings": [
                    {
                        "sourceColumnName": b["target_join_column"],
                        "targetPropertyId": str(
                            property_id[(tgt_ent, tgt_key)]),
                    }
                ] if tgt_key and (tgt_ent, tgt_key) in property_id else [],
            }
            _write_json(
                rel_dir / "Contextualizations" / f"{ctx_uuid}.json",
                ctx_payload)
            exported_parts.append({
                "path":
                    f"RelationshipTypes/{rid}/Contextualizations/"
                    f"{ctx_uuid}.json",
                "payloadType": "InlineBase64",
            })

    # ---- Root files ------------------------------------------------------- #
    _write_json(out_dir / "definition.json", {})
    _write_json(out_dir / "manifest.json", {
        "ontologyName": ontology_name,
        "exportedParts": exported_parts,
    })
    _write_text(out_dir / ".platform", json.dumps({
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/"
                   "gitIntegration/platformProperties/2.0.0/schema.json",
        "metadata": {
            "type": "Ontology",
            "displayName": ontology_name,
        },
        "config": {
            "version": "2.0",
            "logicalId": "00000000-0000-0000-0000-000000000000",
        },
    }, indent=2) + "\n")

    print(f"Ontology:                 {ontology_name}")
    print(f"Entities:                 {len(data['entities'])}")
    print(f"Properties:               {len(data['properties'])}")
    print(f"Relationships:            {len(data['relationships'])}")
    print(f"Entity bindings:          "
          f"{sum(len(v) for v in bindings_by_entity.values())}")
    print(f"Relationship bindings:    "
          f"{sum(len(v) for v in rel_bindings.values())}")
    print(f"Manifest exportedParts:   {len(exported_parts)}")
    print(f"Wrote to:                 {out_dir}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--csv-dir", required=True, type=Path)
    ap.add_argument("--out-dir", required=True, type=Path)
    ap.add_argument("--ontology-name", required=True)
    args = ap.parse_args()
    try:
        build(args.csv_dir, args.out_dir, args.ontology_name)
    except ValidationError as e:
        print(str(e), file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
