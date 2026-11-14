# ruff: noqa: E501  (the SVG strings are easier to read on one line)
"""Draw the data lineage as an SVG (docs/images/lineage.svg) from the dbt manifest.

The dbt part of the graph is read from `dbt/target/manifest.json`, so it follows the models
instead of drifting from them. Create or refresh the manifest first, then draw:

    dbt parse --project-dir dbt --profiles-dir dbt
    python scripts/draw_lineage.py

The landing and bronze step before the silver tables, and the dashboard, are not dbt nodes,
so they are added here.
"""

from __future__ import annotations

import json
from html import escape
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "dbt" / "target" / "manifest.json"
OUT = ROOT / "docs" / "images" / "lineage.svg"

FONT = "Helvetica, Arial, sans-serif"
INK, MUTED, LINE = "#1f2937", "#6b7280", "#9ca3af"
NODE_W, NODE_H, COLUMN_GAP, MARGIN = 170, 46, 50, 25
TOP, ROW_GAP = 110, 16

# column -> (title, colour, background)
LAYERS = {
    0: ("Landing and bronze", "#b7791f", "#fdf3e3"),
    1: ("Silver tables and seeds", "#64748b", "#f1f5f9"),
    2: ("Staging (dbt views)", "#1d4ed8", "#eff6ff"),
    3: ("Dimensions and event ledger", "#a16207", "#fef9e7"),
    4: ("Valuation (ephemeral)", "#7c3aed", "#f5f3ff"),
    5: ("Daily snapshot", "#a16207", "#fef9e7"),
    6: ("Reports", "#047857", "#ecfdf5"),
    7: ("Serve", "#047857", "#ecfdf5"),
}


def layer_of(name: str, kind: str) -> int:
    if kind in ("source", "seed"):
        return 1
    if name.startswith("stg_"):
        return 2
    if name.startswith("int_"):
        return 4
    if name == "fct_daily_portfolio":
        return 5
    if name.startswith("rpt_"):
        return 6
    return 3


def load() -> tuple[dict[str, dict], list[tuple[str, str]]]:
    manifest = json.loads(MANIFEST.read_text())
    kinds = ("model", "seed", "source")
    raw = {k: v for k, v in {**manifest["nodes"], **manifest["sources"]}.items()}
    nodes = {}
    for key, node in raw.items():
        if node["resource_type"] not in kinds:
            continue
        kind = node["resource_type"]
        detail = node.get("config", {}).get("materialized") or kind
        if kind == "source":
            detail = "silver table"
        nodes[key] = {"name": node["name"], "detail": detail, "layer": layer_of(node["name"], kind)}
    edges = [
        (parent, child)
        for child in nodes
        for parent in manifest["parent_map"].get(child, [])
        if parent in nodes
    ]
    return nodes, edges


def add_outside_dbt(nodes: dict, edges: list) -> None:
    extra = {
        "bronze": ("Landing files > bronze", "Auto Loader, rows as text", 0),
        "dashboard": ("AI/BI dashboard", "fund selector", 7),
    }
    for key, (name, detail, layer) in extra.items():
        nodes[key] = {"name": name, "detail": detail, "layer": layer}
    for key, node in list(nodes.items()):
        if node["detail"] == "silver table":
            edges.append(("bronze", key))
    by_name = {n["name"]: k for k, n in nodes.items()}
    for served in ("rpt_fund_daily_summary", "fct_daily_portfolio", "fct_installment_events",
                   "rpt_ingestion_quality"):  # fmt: skip
        edges.append((by_name[served], "dashboard"))


def order_rows(nodes: dict, edges: list) -> dict[str, float]:
    """Vertical centre of every node. Columns are sorted by the mean position of their parents."""
    parents: dict[str, list[str]] = {k: [] for k in nodes}
    for parent, child in edges:
        parents[child].append(parent)
    rows: dict[str, float] = {}
    for layer in sorted(LAYERS):
        keys = sorted(
            (k for k, n in nodes.items() if n["layer"] == layer), key=lambda k: nodes[k]["name"]
        )
        if layer > 0:
            keys.sort(
                key=lambda k: (
                    sum(rows.get(p, 0) for p in parents[k]) / max(len(parents[k]), 1),
                    nodes[k]["name"],
                )
            )
        for i, key in enumerate(keys):
            rows[key] = i
    return rows


def main() -> None:
    nodes, edges = load()
    add_outside_dbt(nodes, edges)
    rows = order_rows(nodes, edges)
    tallest = max(sum(1 for n in nodes.values() if n["layer"] == layer) for layer in LAYERS)
    height = TOP + tallest * (NODE_H + ROW_GAP) + 70
    width = MARGIN * 2 + len(LAYERS) * NODE_W + (len(LAYERS) - 1) * COLUMN_GAP

    def x_of(key):
        return MARGIN + nodes[key]["layer"] * (NODE_W + COLUMN_GAP)

    def y_of(key):  # centre the column on the tallest one
        count = sum(1 for n in nodes.values() if n["layer"] == nodes[key]["layer"])
        offset = (tallest - count) * (NODE_H + ROW_GAP) / 2
        return TOP + offset + rows[key] * (NODE_H + ROW_GAP)

    out = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        f'<defs><marker id="head" markerWidth="8" markerHeight="8" refX="6" refY="4" orient="auto">'
        f'<path d="M0 0 L8 4 L0 8 z" fill="{LINE}"/></marker></defs>',
        f'<rect width="{width}" height="{height}" fill="#ffffff"/>',
        f'<text x="{MARGIN}" y="40" font-family="{FONT}" font-size="26" font-weight="bold" fill="{INK}">Data lineage</text>',
        f'<text x="{MARGIN}" y="66" font-family="{FONT}" font-size="14" fill="{MUTED}">From the administrator\'s file to the dashboard. '
        f"The dbt part is drawn from the project's manifest.</text>",
    ]
    for layer, (title, colour, background) in LAYERS.items():
        x = MARGIN + layer * (NODE_W + COLUMN_GAP)
        out.append(
            f'<rect x="{x - 8}" y="82" width="{NODE_W + 16}" height="{height - 100}" rx="12" fill="{background}" opacity="0.55"/>'
        )
        out.append(
            f'<text x="{x + NODE_W / 2}" y="104" font-family="{FONT}" font-size="12" font-weight="bold" fill="{colour}" text-anchor="middle">{escape(title)}</text>'
        )
    for parent, child in edges:
        x1, y1 = x_of(parent) + NODE_W, y_of(parent) + NODE_H / 2
        x2, y2 = x_of(child), y_of(child) + NODE_H / 2
        bend = max((x2 - x1) / 2, 20)
        out.append(
            f'<path d="M{x1} {y1} C{x1 + bend} {y1} {x2 - bend} {y2} {x2} {y2}" fill="none" stroke="{LINE}" '
            f'stroke-width="1.6" opacity="0.85" marker-end="url(#head)"/>'
        )
    for key, node in nodes.items():
        colour = LAYERS[node["layer"]][1]
        dash = ' stroke-dasharray="5 4"' if node["detail"] == "ephemeral" else ""
        x, y = x_of(key), y_of(key)
        out.append(
            f'<rect x="{x}" y="{y}" width="{NODE_W}" height="{NODE_H}" rx="9" fill="#ffffff" stroke="{colour}" stroke-width="1.8"{dash}/>'
        )
        out.append(
            f'<text x="{x + 10}" y="{y + 20}" font-family="{FONT}" font-size="12.5" font-weight="bold" fill="{INK}">{escape(node["name"])}</text>'
        )
        out.append(
            f'<text x="{x + 10}" y="{y + 36}" font-family="{FONT}" font-size="11" fill="{MUTED}">{escape(node["detail"])}</text>'
        )
    out.append(
        f'<text x="{MARGIN}" y="{height - 22}" font-family="{FONT}" font-size="12" fill="{MUTED}">'
        f"Dashed boxes are ephemeral: dbt compiles them into the daily snapshot instead of storing them.</text>"
    )
    out.append("</svg>")
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text("\n".join(out) + "\n", encoding="utf-8")
    print(f"wrote {OUT} ({len(nodes)} nodes, {len(edges)} edges)")


if __name__ == "__main__":
    main()
