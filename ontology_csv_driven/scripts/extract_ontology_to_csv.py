"""Extract a Fabric Ontology export folder to the five-CSV authoring schema.

Reads the on-disk export of a Fabric Ontology (the same shape the launcher
commits under `ontology/<name>/`) and emits five CSVs to the seed folder:

    entities.csv
    properties.csv
    relationships.csv
    entity_bindings.csv
    relationship_bindings.csv

Naming rules enforced (see docs/csv_schema.md):
- Business CSVs (entities/properties/relationships) hold ontology names only;
  no `fact_` / `dim_` table prefixes leak in.
- Physical table + column names live exclusively in *_bindings.csv.

Long-path safe on Windows (uses the `\\?\` prefix) because the canonical
phase2 working folder lives under a deep Downloads path.

Usage:
    python extract_ontology_to_csv.py \
        --ontology-dir <repo>/ontology/Healthcare_Demo_Ontology_HLS \
        --out-dir      <repo>/ontology_csv_driven/csv/healthcare_seed
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from pathlib import Path
from typing import Any


# --------------------------------------------------------------------------- #
# Long-path helpers (Windows MAX_PATH workaround)
# --------------------------------------------------------------------------- #

def _lp(p: str | os.PathLike) -> str:
    """Return path with `\\?\` prefix on Windows for long-path support."""
    s = os.fspath(p)
    if sys.platform != "win32":
        return s
    s = os.path.abspath(s)
    if s.startswith("\\\\?\\"):
        return s
    if s.startswith("\\\\"):
        return "\\\\?\\UNC\\" + s.lstrip("\\")
    return "\\\\?\\" + s


def _read_json(path: Path) -> dict[str, Any]:
    # Some Fabric exports include a UTF-8 BOM; utf-8-sig handles both cases.
    with open(_lp(path), "r", encoding="utf-8-sig") as f:
        return json.load(f)


def _walk_files(root: Path, suffix: str = ".json"):
    """Yield Path objects for files under root with the given suffix.

    Uses os.walk on the long-path-prefixed root so deep trees don't blow up.
    """
    long_root = _lp(root)
    for dirpath, _dirnames, filenames in os.walk(long_root):
        for fn in filenames:
            if fn.endswith(suffix):
                # Strip the \\?\ prefix back off so callers see normal paths.
                full = os.path.join(dirpath, fn)
                if full.startswith("\\\\?\\UNC\\"):
                    full = "\\\\" + full[len("\\\\?\\UNC\\"):]
                elif full.startswith("\\\\?\\"):
                    full = full[len("\\\\?\\"):]
                yield Path(full)


# --------------------------------------------------------------------------- #
# Naming helpers
# --------------------------------------------------------------------------- #

_TABLE_PREFIXES = ("fact_", "dim_", "agg_", "bridge_", "xref_")


def _entity_name_from_table(table: str) -> str:
    """`fact_encounter` -> `Encounter`; preserve already-clean names as-is."""
    base = table
    for pref in _TABLE_PREFIXES:
        if base.startswith(pref):
            base = base[len(pref):]
            break
    parts = base.split("_")
    return "".join(p[:1].upper() + p[1:] for p in parts if p)


def _clean_property_name(name: str) -> str:
    """Strip physical-table prefixes from a property name.

    The HLS export occasionally embeds the source-table prefix in the
    property name itself (e.g. `fact_diagnosis_key`). The business CSVs
    must stay clean of physical naming; the raw column name still lives
    in `entity_bindings.csv.source_column`.
    """
    for pref in _TABLE_PREFIXES:
        if name.startswith(pref):
            return name[len(pref):]
    return name


# --------------------------------------------------------------------------- #
# Extraction
# --------------------------------------------------------------------------- #

def _extract_entity(folder: Path) -> dict[str, Any]:
    """Parse one EntityTypes/<id>/ folder into a normalized dict."""
    defn = _read_json(folder / "definition.json")
    bindings_dir = folder / "DataBindings"
    bindings: list[dict[str, Any]] = []
    if bindings_dir.exists():
        for bf in _walk_files(bindings_dir):
            bindings.append(_read_json(bf))

    # Property id -> name lookup from the definition.
    prop_by_id: dict[str, dict[str, Any]] = {
        p["id"]: p for p in defn.get("properties", [])
    }
    key_prop_id = (defn.get("entityIdParts") or [None])[0]
    display_prop_id = defn.get("displayNamePropertyId")

    return {
        "id": defn["id"],
        "ontology_name": defn["name"],  # already PascalCase in HLS
        "key_property_id": key_prop_id,
        "display_property_id": display_prop_id,
        "properties": prop_by_id,
        "bindings": bindings,
    }


def _extract_relationship(folder: Path) -> dict[str, Any]:
    defn = _read_json(folder / "definition.json")
    ctx_dir = folder / "Contextualizations"
    contextualizations: list[dict[str, Any]] = []
    if ctx_dir.exists():
        for cf in _walk_files(ctx_dir):
            contextualizations.append(_read_json(cf))

    return {
        "id": defn["id"],
        "name": defn["name"],
        "source_entity_id": defn["source"]["entityTypeId"],
        "target_entity_id": defn["target"]["entityTypeId"],
        "contextualizations": contextualizations,
    }


def extract(ontology_dir: Path, out_dir: Path) -> None:
    if not Path(_lp(ontology_dir)).exists():
        raise SystemExit(f"Ontology dir not found: {ontology_dir}")
    out_dir.mkdir(parents=True, exist_ok=True)

    # ---- entities + properties + entity bindings --------------------------- #
    entities_root = ontology_dir / "EntityTypes"
    entity_folders = [
        Path(p) for p in
        (
            os.path.join(_lp(entities_root), name)
            for name in os.listdir(_lp(entities_root))
        )
        if os.path.isdir(_lp(p))
    ]
    # Re-walk via _walk_files-style logic to keep paths sane.
    entity_folders = []
    for name in os.listdir(_lp(entities_root)):
        cand = entities_root / name
        if os.path.isdir(_lp(cand)):
            entity_folders.append(cand)

    entities = [_extract_entity(f) for f in entity_folders]
    by_id = {e["id"]: e for e in entities}

    entity_rows = []
    property_rows = []
    entity_binding_rows = []

    for e in entities:
        prop_by_id = e["properties"]
        key_prop = prop_by_id.get(e["key_property_id"], {})
        disp_prop = prop_by_id.get(e["display_property_id"], {})
        entity_rows.append({
            "entity_name": e["ontology_name"],
            "key_property": _clean_property_name(key_prop.get("name", "")),
            "display_property": _clean_property_name(
                disp_prop.get("name", "")),
            "business_description": "",  # filled in Phase 2 step 6
            "domain_group": "",
        })
        for prop in prop_by_id.values():
            property_rows.append({
                "entity_name": e["ontology_name"],
                "property_name": _clean_property_name(prop["name"]),
                "value_type": prop["valueType"],
                "business_description": "",
                "unit": "",
            })
        for binding in e["bindings"]:
            cfg = binding.get("dataBindingConfiguration", {})
            src = cfg.get("sourceTableProperties", {})
            table = src.get("sourceTableName", "")
            for pb in cfg.get("propertyBindings", []):
                target_prop = prop_by_id.get(pb["targetPropertyId"], {})
                entity_binding_rows.append({
                    "entity_name": e["ontology_name"],
                    "source_table": table,
                    "property_name": _clean_property_name(
                        target_prop.get("name", "")),
                    "source_column": pb["sourceColumnName"],
                })

    # ---- relationships + relationship bindings ----------------------------- #
    rels_root = ontology_dir / "RelationshipTypes"
    rel_folders = []
    if Path(_lp(rels_root)).exists():
        for name in os.listdir(_lp(rels_root)):
            cand = rels_root / name
            if os.path.isdir(_lp(cand)):
                rel_folders.append(cand)

    relationship_rows = []
    relationship_binding_rows = []

    for rf in rel_folders:
        rel = _extract_relationship(rf)
        src = by_id.get(rel["source_entity_id"])
        tgt = by_id.get(rel["target_entity_id"])
        if not src or not tgt:
            continue  # skip orphan rel

        relationship_rows.append({
            "relationship_name": rel["name"],
            "source_entity": src["ontology_name"],
            "target_entity": tgt["ontology_name"],
            "business_description": "",
            "cardinality": "",  # not in v1 export; left blank for human review
        })

        # Each contextualization is one source/target join binding.
        src_props = src["properties"]
        tgt_props = tgt["properties"]
        for ctx in rel["contextualizations"]:
            tbl = ctx.get("dataBindingTable", {}).get("sourceTableName", "")
            sk = (ctx.get("sourceKeyRefBindings") or [{}])[0]
            tk = (ctx.get("targetKeyRefBindings") or [{}])[0]
            sk_prop = src_props.get(sk.get("targetPropertyId"), {})
            tk_prop = tgt_props.get(tk.get("targetPropertyId"), {})
            relationship_binding_rows.append({
                "relationship_name": rel["name"],
                "source_table": tbl,
                "source_join_column": sk.get("sourceColumnName", ""),
                "target_join_column": tk.get("sourceColumnName", ""),
                "_source_key_property": _clean_property_name(
                    sk_prop.get("name", "")),
                "_target_key_property": _clean_property_name(
                    tk_prop.get("name", "")),
            })

    # ---- Write CSVs -------------------------------------------------------- #
    _write_csv(out_dir / "entities.csv",
               ["entity_name", "key_property", "display_property",
                "business_description", "domain_group"],
               entity_rows)
    _write_csv(out_dir / "properties.csv",
               ["entity_name", "property_name", "value_type",
                "business_description", "unit"],
               property_rows)
    _write_csv(out_dir / "relationships.csv",
               ["relationship_name", "source_entity", "target_entity",
                "business_description", "cardinality"],
               relationship_rows)
    _write_csv(out_dir / "entity_bindings.csv",
               ["entity_name", "source_table", "property_name",
                "source_column"],
               entity_binding_rows)
    _write_csv(out_dir / "relationship_bindings.csv",
               ["relationship_name", "source_table", "source_join_column",
                "target_join_column", "_source_key_property",
                "_target_key_property"],
               relationship_binding_rows)

    print(f"Entities:                {len(entity_rows)}")
    print(f"Properties:              {len(property_rows)}")
    print(f"Relationships:           {len(relationship_rows)}")
    print(f"Entity bindings:         {len(entity_binding_rows)}")
    print(f"Relationship bindings:   {len(relationship_binding_rows)}")
    print(f"Wrote five CSVs to:      {out_dir}")


def _write_csv(path: Path, fieldnames: list[str],
               rows: list[dict[str, Any]]) -> None:
    with open(_lp(path), "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--ontology-dir", required=True, type=Path)
    ap.add_argument("--out-dir", required=True, type=Path)
    args = ap.parse_args()
    extract(args.ontology_dir, args.out_dir)


if __name__ == "__main__":
    main()
