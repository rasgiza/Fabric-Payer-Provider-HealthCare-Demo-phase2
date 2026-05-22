"""apply_sm_polish.py — hide surrogate keys (C) and assign display folders to
measures (F) across PayerAnalytics + ProviderAnalytics semantic models.

Both transforms are idempotent. Run repeatedly safely.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# (C) Hide surrogate keys
# ---------------------------------------------------------------------------
HIDE_PATTERNS = [
    re.compile(r".*_key$"),         # surrogate join keys
    re.compile(r".*_date_key$"),
    re.compile(r"^date_key$"),
]

COLUMN_DECL = re.compile(r"^(?P<indent>\s*)column\s+(?P<name>[A-Za-z_][\w]*)\s*$")


def should_hide(name: str) -> bool:
    n = name.lower()
    return any(p.match(n) for p in HIDE_PATTERNS)


def hide_keys(text: str) -> tuple[str, int]:
    lines = text.splitlines(keepends=False)
    out: list[str] = []
    i = 0
    fixes = 0
    while i < len(lines):
        line = lines[i]
        m = COLUMN_DECL.match(line)
        if not m or not should_hide(m.group("name")):
            out.append(line)
            i += 1
            continue
        # Find extent of block (until blank line followed by sibling decl,
        # OR next column/measure/table at same-or-lower indent).
        indent = m.group("indent")
        out.append(line)
        i += 1
        block_start_in_out = len(out)
        while i < len(lines):
            nxt = lines[i]
            if re.match(rf"^{re.escape(indent)}(column|measure|table)\s", nxt):
                break
            if re.match(r"^\S", nxt):
                break
            out.append(nxt)
            i += 1
        block_end_in_out = len(out)
        block = out[block_start_in_out:block_end_in_out]
        if any(re.match(r"^\s*isHidden\s*:", b) for b in block):
            continue  # already hidden
        # Insert `isHidden: true` right after the column declaration line.
        inner_indent = indent + "\t"
        out.insert(block_start_in_out, f"{inner_indent}isHidden: true")
        fixes += 1
    return "\n".join(out) + ("\n" if text.endswith("\n") else ""), fixes


# ---------------------------------------------------------------------------
# (F) Display folders for measures
# ---------------------------------------------------------------------------
# Ordered category rules.  First match wins.  Patterns are case-insensitive
# regex tested against the measure name.
DISPLAY_FOLDERS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"^denial|denied|appeal", re.I),                      "Denials & Appeals"),
    (re.compile(r"premium|mlr|capitation", re.I),                     "Premiums & MLR"),
    (re.compile(r"^(total billed|total paid|at risk|paid by|net collection|gross collection|avg charges|avg cost|total charges|total cost|margin|expected reimbursement)", re.I), "Financials"),
    (re.compile(r"hedis|star rating|pdc|adherence|quality", re.I),    "Quality"),
    (re.compile(r"raf|risk score|readmission|risk category|risk band", re.I), "Risk"),
    (re.compile(r"^(avg|days|los|length of stay|er visits|inpatient|outpatient|utilization)", re.I), "Utilization"),
    (re.compile(r"^(total|count|distinct count|number of|active)", re.I), "Volume"),
]

MEASURE_DECL = re.compile(r"^(?P<indent>\s*)measure\s+(?P<name>'[^']+'|[A-Za-z_][\w]*)\s*=")


def categorize(name: str) -> str | None:
    for rx, folder in DISPLAY_FOLDERS:
        if rx.search(name):
            return folder
    return None


def add_display_folders(text: str) -> tuple[str, int]:
    lines = text.splitlines(keepends=False)
    out: list[str] = []
    fixes = 0
    i = 0
    while i < len(lines):
        line = lines[i]
        m = MEASURE_DECL.match(line)
        if not m:
            out.append(line)
            i += 1
            continue
        name = m.group("name").strip("'")
        indent = m.group("indent")
        folder = categorize(name)
        # Collect measure block extent.
        out.append(line)
        i += 1
        block_start = len(out)
        while i < len(lines):
            nxt = lines[i]
            if re.match(rf"^{re.escape(indent)}(measure|column)\s", nxt):
                break
            if re.match(r"^\S", nxt):
                break
            out.append(nxt)
            i += 1
        block_end = len(out)
        if folder is None:
            continue
        block = out[block_start:block_end]
        if any(re.match(r"^\s*displayFolder\s*:", b) for b in block):
            continue
        # Insert after lineageTag if present (to keep ordering consistent with
        # Power BI Desktop emission); otherwise insert right after declaration.
        inner_indent = indent + "\t"
        insert_at_offset = 0
        for idx, b in enumerate(block):
            if re.match(r"^\s*lineageTag\s*:", b):
                insert_at_offset = idx + 1
                break
            if re.match(r"^\s*(formatString|description)\s*:", b):
                insert_at_offset = max(insert_at_offset, idx + 1)
        out.insert(block_start + insert_at_offset, f"{inner_indent}displayFolder: {folder}")
        fixes += 1
    return "\n".join(out) + ("\n" if text.endswith("\n") else ""), fixes


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------
def process_file(path: Path) -> tuple[int, int]:
    text = path.read_text(encoding="utf-8")
    text2, h = hide_keys(text)
    text3, f = add_display_folders(text2)
    if h or f:
        path.write_text(text3, encoding="utf-8")
    return h, f


def main(argv: list[str]) -> int:
    if argv:
        roots = [Path(p) for p in argv]
    else:
        here = Path(__file__).resolve().parent.parent
        roots = [
            here / "workspace" / "PayerAnalytics.SemanticModel" / "definition" / "tables",
            here / "workspace" / "ProviderAnalytics.SemanticModel" / "definition" / "tables",
        ]
    total_h = total_f = 0
    for root in roots:
        if not root.exists():
            print(f"[skip] {root}")
            continue
        for tmdl in sorted(root.glob("*.tmdl")):
            h, f = process_file(tmdl)
            total_h += h
            total_f += f
            mark = "+" if (h or f) else "="
            print(f"{mark} {tmdl.relative_to(root.parent.parent.parent)}: hidden={h} folders={f}")
    print(f"\nDone. hidden columns: {total_h}   displayFolder inserts: {total_f}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
