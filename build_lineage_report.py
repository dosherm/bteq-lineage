#!/usr/bin/env python3
"""
build_lineage_report.py

Reads output_lineage_graph.json and output_semantic_parse.json and produces
a self-contained HTML report with a plain-English narrative for every final
target column (leaf nodes that appear as targets but never as sources).
"""

import argparse
import json
import os
from collections import defaultdict
from typing import Dict, List, Optional


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _split_col(qualified: Optional[str]):
    """Split 'TABLE.COLUMN' -> (table, column). Returns ('', col) for bare names."""
    if not qualified:
        return "", ""
    parts = qualified.split(".", 1)
    return (parts[0], parts[1]) if len(parts) == 2 else ("", parts[0])


def _ordinal(n: int) -> str:
    suffixes = {1: "st", 2: "nd", 3: "rd"}
    return f"{n}{suffixes.get(n if n < 20 else n % 10, 'th')}"


def _classification_badge(cls: str) -> str:
    colours = {
        "Direct assignment": "#2196F3",
        "Derived (function)": "#9C27B0",
        "Derived (expression)": "#FF9800",
        "Constant": "#607D8B",
        "Conditional": "#E91E63",
    }
    colour = colours.get(cls, "#78909C")
    return f'<span class="badge" style="background:{colour}">{cls or "unknown"}</span>'


def _esc(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


# ---------------------------------------------------------------------------
# Narrative builder
# ---------------------------------------------------------------------------

def _narrative_for_column(
    col_lineage: dict,
    unresolved_edges: List[dict],
) -> str:
    """Return an HTML <div> block containing the narrative for one column."""
    target = col_lineage["target_column"]
    tbl, col_name = _split_col(target)
    chains = col_lineage.get("transformation_chains", [])
    direct_sources = col_lineage.get("direct_sources", [])
    ultimate_sources = col_lineage.get("ultimate_base_sources", [])

    parts = []

    if chains:
        # Deduplicate chains by visible content only (ignore metadata fields like source script)
        seen_keys = set()
        unique_chains = []
        for chain in chains:
            path_key = tuple(
                (h.get("hop"), h.get("resolved_expression"), h.get("target_column"),
                 h.get("classification"), h.get("statement_type"))
                for h in chain.get("path", [])
            )
            key = (chain.get("ultimate_source"), chain.get("hops"), path_key)
            if key not in seen_keys:
                seen_keys.add(key)
                unique_chains.append(chain)
        chains = unique_chains

        # One paragraph per chain
        for chain in chains:
            ult_src = chain["ultimate_source"]
            hops = chain["hops"]
            path = chain["path"]

            ult_tbl, ult_col = _split_col(ult_src)
            hop_word = "step" if hops == 1 else f"{hops}-step transformation"

            lines = [
                f"<strong>{_esc(col_name)}</strong> originates from "
                f"<code>{_esc(ult_src)}</code> through a {hop_word}."
            ]

            for hop in path:
                h_num = hop["hop"]
                h_src = hop.get("source_column") or hop.get("resolved_expression", "")
                h_tgt = hop["target_column"]
                h_stype = hop["statement_type"]
                h_expr = hop.get("resolved_expression", "")
                h_cls = hop["classification"]

                badge = _classification_badge(h_cls)
                # Show the immediate source column so steps logically connect.
                # If the resolved expression differs (e.g. a function or CASE), show it too.
                expr_part = f"<code>{_esc(h_src)}</code>"
                if h_expr and h_expr != h_src:
                    expr_part += f" <em style='color:#888;font-size:11px;'>expr: {_esc(h_expr)}</em>"
                lines.append(
                    f"&nbsp;&nbsp;&nbsp;&nbsp;<em>Step {h_num}</em> ({h_stype}): "
                    f"{expr_part} {badge} "
                    f"→ <code>{_esc(h_tgt)}</code>"
                )

            parts.append("<p>" + "<br>".join(lines) + "</p>")

    elif unresolved_edges:
        # No resolved chain — explain why
        notes = sorted({e.get("resolution_note", "source could not be resolved") for e in unresolved_edges})
        exprs = sorted({e.get("resolved_expression", "") for e in unresolved_edges if e.get("resolved_expression")})
        stypes = sorted({e["statement_type"] for e in unresolved_edges})

        note_text = "; ".join(_esc(n) for n in notes)
        expr_text = ", ".join(f"<code>{_esc(x)}</code>" for x in exprs)
        stype_text = ", ".join(stypes)

        parts.append(
            f"<p><strong>{_esc(col_name)}</strong> could not be fully traced to an ultimate source. "
            f"Written via {stype_text} using expression {expr_text or '<em>(unknown)</em>'}. "
            f"Reason: <em>{note_text}</em>.</p>"
        )
    else:
        parts.append(
            f"<p><strong>{_esc(col_name)}</strong> has no resolved lineage information.</p>"
        )

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# HTML generation
# ---------------------------------------------------------------------------

CSS = """
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
       font-size: 14px; color: #212121; background: #f5f5f5; padding: 24px; }
h1 { font-size: 22px; font-weight: 700; margin-bottom: 4px; }
.subtitle { color: #757575; font-size: 13px; margin-bottom: 24px; }
.stats { display: flex; gap: 16px; margin-bottom: 28px; flex-wrap: wrap; }
.stat-card { background: white; border-radius: 8px; padding: 14px 20px;
             box-shadow: 0 1px 3px rgba(0,0,0,.12); min-width: 140px; }
.stat-card .num { font-size: 28px; font-weight: 700; color: #1565C0; }
.stat-card .lbl { font-size: 11px; color: #757575; text-transform: uppercase; letter-spacing: .5px; }
.table-section { margin-bottom: 32px; }
.table-header { background: #1565C0; color: white; padding: 10px 16px;
                border-radius: 8px 8px 0 0; font-size: 15px; font-weight: 600; }
.col-card { background: white; border-left: 4px solid #1565C0;
            padding: 14px 18px; margin-bottom: 1px; }
.col-card:last-child { border-radius: 0 0 8px 8px; }
.col-name { font-weight: 700; font-size: 13px; color: #1565C0; margin-bottom: 8px;
            font-family: 'SFMono-Regular', Consolas, monospace; }
.col-card p { line-height: 1.65; color: #424242; margin-bottom: 6px; }
.col-card p:last-child { margin-bottom: 0; }
code { background: #F3F4F6; border-radius: 3px; padding: 1px 5px;
       font-family: 'SFMono-Regular', Consolas, monospace; font-size: 12px; color: #C62828; }
.badge { color: white; font-size: 11px; padding: 2px 7px;
         border-radius: 10px; font-weight: 600; white-space: nowrap; }
em { color: #555; }
.legend { background: white; border-radius: 8px; padding: 14px 18px;
          box-shadow: 0 1px 3px rgba(0,0,0,.12); margin-bottom: 24px; }
.legend h3 { font-size: 13px; font-weight: 600; margin-bottom: 10px; color: #555; text-transform: uppercase; letter-spacing: .5px; }
.legend-items { display: flex; gap: 12px; flex-wrap: wrap; }
.unresolved-border { border-left-color: #E53935; }
.no-info-border { border-left-color: #9E9E9E; }
"""

LEGEND_ITEMS = [
    ("Direct assignment", "#2196F3"),
    ("Derived (function)", "#9C27B0"),
    ("Derived (expression)", "#FF9800"),
    ("Constant", "#607D8B"),
    ("Conditional", "#E91E63"),
]


def build_html(
    lineage_data: dict,
    parse_data: dict,
    output_path: str,
) -> None:
    # Identify final target columns (appear as target, never as source)
    all_sources = set()
    all_targets = set()
    for e in lineage_data["edges"]:
        if e.get("source_column"):
            all_sources.add(e["source_column"])
        if e.get("target_column"):
            all_targets.add(e["target_column"])

    final_targets = all_targets - all_sources

    # Identify columns that are purely constant (all edges are Constant, no source columns).
    # These are positional literal-value INSERTs (e.g. INSERT INTO tbl('a','b',1))
    # and carry no meaningful lineage — exclude them from the report.
    constant_only_cols: set = set()
    edges_by_target: Dict[str, List[dict]] = defaultdict(list)
    for e in lineage_data["edges"]:
        if e.get("target_column"):
            edges_by_target[e["target_column"]].append(e)
    for col, col_edges in edges_by_target.items():
        if col in final_targets and all(e.get("classification") == "Constant" and not e.get("source_column") for e in col_edges):
            constant_only_cols.add(col)

    # Build unresolved edge index keyed by target_column
    unresolved_by_target: Dict[str, List[dict]] = defaultdict(list)
    for e in lineage_data["edges"]:
        if not e.get("source_column") and e.get("target_column") in final_targets and e.get("target_column") not in constant_only_cols:
            unresolved_by_target[e["target_column"]].append(e)

    # Filter column_lineage to final targets only, excluding constant-only columns
    final_lineage = [
        cl for cl in lineage_data["column_lineage"]
        if cl["target_column"] in final_targets and cl["target_column"] not in constant_only_cols
    ]
    final_lineage.sort(key=lambda x: x["target_column"])

    # Group by table
    by_table: Dict[str, List[dict]] = defaultdict(list)
    for cl in final_lineage:
        tbl, _ = _split_col(cl["target_column"])
        by_table[tbl].append(cl)

    # Count stats
    n_resolved = sum(1 for cl in final_lineage if cl["transformation_chains"])
    n_unresolved = sum(1 for cl in final_lineage if not cl["transformation_chains"])

    # Build HTML
    sections = []
    for table in sorted(by_table.keys()):
        cols = by_table[table]
        cards = []
        for cl in cols:
            col = cl["target_column"]
            _, col_name = _split_col(col)
            unresolved_edges = unresolved_by_target.get(col, [])

            has_chain = bool(cl["transformation_chains"])
            border_class = "" if has_chain else ("unresolved-border" if unresolved_edges else "no-info-border")

            narrative = _narrative_for_column(cl, unresolved_edges)
            cards.append(
                f'<div class="col-card {border_class}">'
                f'<div class="col-name">{_esc(col_name)}</div>'
                f'{narrative}'
                f'</div>'
            )

        sections.append(
            f'<div class="table-section">'
            f'<div class="table-header">{_esc(table)}</div>'
            + "\n".join(cards) +
            f'</div>'
        )

    legend_html = "".join(
        f'<span class="badge" style="background:{c}">{_esc(n)}</span>'
        for n, c in LEGEND_ITEMS
    )

    source_file = os.path.basename(output_path)
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Column Lineage Report</title>
<style>{CSS}</style>
</head>
<body>
<h1>Column Lineage Report</h1>
<p class="subtitle">Final target columns — data flow from ultimate sources to leaf destinations</p>

<div class="stats">
  <div class="stat-card"><div class="num">{len(final_lineage)}</div><div class="lbl">Final Target Columns</div></div>
  <div class="stat-card"><div class="num">{len(by_table)}</div><div class="lbl">Target Tables</div></div>
  <div class="stat-card"><div class="num">{n_resolved}</div><div class="lbl">Fully Traced</div></div>
  <div class="stat-card"><div class="num">{n_unresolved}</div><div class="lbl">Unresolved Source</div></div>
  <div class="stat-card"><div class="num">{lineage_data['summary']['total_chains']}</div><div class="lbl">Total Chains</div></div>
  <div class="stat-card"><div class="num">{lineage_data['summary']['max_chain_hops']}</div><div class="lbl">Max Hops</div></div>
</div>

<div class="legend">
  <h3>Classification legend</h3>
  <div class="legend-items">{legend_html}
    <span style="font-size:12px;color:#555;align-self:center;">
      Blue left border = resolved &nbsp;|&nbsp;
      Red left border = unresolved source &nbsp;|&nbsp;
      Grey left border = no lineage data
    </span>
  </div>
</div>

{"".join(sections)}
</body>
</html>"""

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"HTML  -> {output_path}")
    print(f"        {len(final_lineage)} columns across {len(by_table)} tables "
          f"({n_resolved} resolved, {n_unresolved} unresolved)")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Build HTML lineage report from lineage graph JSON.")
    ap.add_argument("--lineage", required=True, help="Path to output_lineage_graph.json")
    ap.add_argument("--parse", required=True, help="Path to output_semantic_parse.json")
    ap.add_argument("--out", required=True, help="Output HTML path")
    args = ap.parse_args()

    with open(args.lineage, encoding="utf-8") as f:
        lineage_data = json.load(f)
    with open(args.parse, encoding="utf-8") as f:
        parse_data = json.load(f)

    build_html(lineage_data, parse_data, args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
