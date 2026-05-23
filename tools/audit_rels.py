"""audit_rels.py — parse relationships.tmdl for both SMs and flag:

1. Duplicate / ambiguous role-playing date paths (multiple FKs from same fact
   to dim_date — expected if marked isActive: false on all-but-one).
2. Bidirectional cross-filter (`crossFilteringBehavior: bothDirections`)
   which BPA flags as a performance/ambiguity risk on fact->dim edges.
3. Many-to-many cardinality (`cardinality: ` not many-to-one / one-to-many).
4. Active relationships from same fact to same dim (true ambiguity).
5. Active relationships fanning into dim_date from multiple FKs on the same
   fact (only one should be active).

Outputs a human-readable report to stdout.  Read-only.
"""

from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path

REL_BLOCK = re.compile(
    r"^relationship\s+(?P<id>[\w-]+)\s*\n(?P<body>(?:\t.*\n?)+)",
    re.MULTILINE,
)
PROP_RE = re.compile(r"^\t(?P<key>\w+):\s*(?P<val>.+)$", re.MULTILINE)
SHORT_PROP_RE = re.compile(r"^\t(?P<key>\w+)\s*$", re.MULTILINE)


def parse_rels(path: Path) -> list[dict]:
    text = path.read_text(encoding="utf-8")
    rels: list[dict] = []
    for m in REL_BLOCK.finditer(text):
        body = m.group("body")
        rel = {"id": m.group("id"), "isActive": True, "crossFilter": "automatic", "cardinality": "manyToOne"}
        for pm in PROP_RE.finditer(body):
            rel[pm.group("key")] = pm.group("val").strip()
        # handle shorthand booleans (single-line property w/o value)
        for sm in SHORT_PROP_RE.finditer(body):
            rel.setdefault(sm.group("key"), True)
        if rel.get("isActive") in ("false", False):
            rel["isActive"] = False
        if rel.get("isActive") in ("true", True):
            rel["isActive"] = True
        rels.append(rel)
    return rels


def audit(sm_label: str, rels_path: Path) -> None:
    print(f"\n=== {sm_label} ({rels_path.relative_to(rels_path.parent.parent.parent.parent)}) ===")
    rels = parse_rels(rels_path)
    print(f"  parsed {len(rels)} relationships")

    # Counter of from/to table edges grouped by (from_table, to_table, isActive)
    from_to_active: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for r in rels:
        f = r.get("fromColumn", "?.?").split(".")[0]
        t = r.get("toColumn", "?.?").split(".")[0]
        from_to_active[(f, t)].append(r)

    issues = 0

    # (1) Multiple active edges between the same pair of tables
    for (f, t), group in from_to_active.items():
        active = [r for r in group if r["isActive"]]
        if len(active) > 1:
            issues += 1
            print(f"  [AMBIGUOUS] {f} -> {t}: {len(active)} ACTIVE relationships")
            for r in active:
                print(f"      - {r['fromColumn']} -> {r['toColumn']}")

    # (2) Bidirectional cross-filter on fact->dim edges (BPA performance flag)
    for r in rels:
        cf = str(r.get("crossFilteringBehavior", "")).lower()
        if cf == "bothdirections":
            f = r.get("fromColumn", "?.?").split(".")[0]
            t = r.get("toColumn", "?.?").split(".")[0]
            issues += 1
            print(f"  [BIDIRECTIONAL] {f} -> {t} ({r['fromColumn']} -> {r['toColumn']})")

    # (3) Non many-to-one cardinality
    for r in rels:
        card = str(r.get("cardinality", "manyToOne"))
        if card not in ("manyToOne", "oneToMany", "oneToOne"):
            issues += 1
            print(f"  [CARDINALITY] {card}: {r['fromColumn']} -> {r['toColumn']}")

    # (4) Inactive relationships — list them so we know they're intentional role-play
    inactive = [r for r in rels if not r["isActive"]]
    if inactive:
        print(f"  [INFO] {len(inactive)} inactive relationships (role-playing — verify these are intentional):")
        for r in inactive:
            print(f"      - {r['fromColumn']} -> {r['toColumn']}")

    # (5) Same fact -> dim_date via multiple FKs — only one active expected
    fact_to_date: dict[str, list[dict]] = defaultdict(list)
    for r in rels:
        f = r.get("fromColumn", "?.?").split(".")[0]
        t = r.get("toColumn", "?.?").split(".")[0]
        if t == "dim_date":
            fact_to_date[f].append(r)
    for f, group in fact_to_date.items():
        active = [r for r in group if r["isActive"]]
        if len(group) > 1 and len(active) != 1:
            issues += 1
            print(f"  [DATE-ROLE] {f} has {len(group)} edges to dim_date but {len(active)} are active (expected exactly 1)")

    if issues == 0:
        print("  ✓ no BPA-relationship violations detected")


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    for label, p in [
        ("PayerAnalytics", root / "workspace" / "PayerAnalytics.SemanticModel" / "definition" / "relationships.tmdl"),
        ("ProviderAnalytics", root / "workspace" / "ProviderAnalytics.SemanticModel" / "definition" / "relationships.tmdl"),
    ]:
        if not p.exists():
            print(f"[skip] {p}")
            continue
        audit(label, p)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
