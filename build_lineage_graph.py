#!/usr/bin/env python3
"""
build_lineage_graph.py

Reads output_semantic_parse.json and builds a column-level lineage graph.

Output contains:
  edges        - every direct source_column -> target_column relationship, with
                 statement context, resolved_expression, and classification
  chains       - full end-to-end paths from ultimate base sources to final targets,
                 showing resolved_expression and classification at every hop
  column_lineage - per target column: its full set of ultimate base sources
                   (transitive closure) and the transformation chain
"""

import argparse
import csv
import json
import os
from collections import defaultdict
from typing import Dict, List, Optional, Set


# ---------------------------------------------------------------------------
# Edge extraction
# ---------------------------------------------------------------------------

def extract_edges(data: dict) -> List[dict]:
    """
    Walk every column_semantics entry in the parse output and emit one edge
    per (source_column, target_column) pair.

    For constants or unresolved columns source_column is None.
    """
    edges = []
    for script in data.get("scripts", []):
        script_name = script.get("script_name", "")
        for stmt in script.get("statements", []):
            sem = stmt.get("semantic_parse")
            if not sem:
                continue

            order = stmt["order"]
            stype = stmt["statement_type"]
            create_kind = sem.get("create_kind", "")
            stmt_label = f"{stype}/{create_kind}" if create_kind else stype

            for col in sem.get("column_semantics", []):
                target_col = col.get("target_column")
                if not target_col:
                    continue
                sources = col.get("base_sources") or []
                resolved_expr = col.get("resolved_expression", "")
                classification = col.get("classification", "")
                merge_clause = col.get("merge_clause")
                resolution_note = col.get("resolution_note")

                edge_base = {
                    "target_column": target_col,
                    "statement_order": order,
                    "statement_type": stmt_label,
                    "resolved_expression": resolved_expr,
                    "classification": classification,
                    "script_name": script_name,
                }
                if merge_clause:
                    edge_base["merge_clause"] = merge_clause

                if sources:
                    for src in sources:
                        edges.append({**edge_base, "source_column": src})
                else:
                    edge_base["source_column"] = None
                    if resolution_note:
                        edge_base["resolution_note"] = resolution_note
                    edges.append(edge_base)

    return edges


# ---------------------------------------------------------------------------
# Chain building (DFS from root sources to leaf targets)
# ---------------------------------------------------------------------------

def build_chains(edges: List[dict]) -> List[dict]:
    """
    Build full end-to-end transformation chains.

    A chain starts at a column with no incoming edges (ultimate base source)
    and follows forward through all statements until a column with no outgoing
    edges (final target).

    Each hop in the chain records the expression and classification so you can
    trace exactly how the value was transformed at each step.
    """
    # Deduplicate edges by (source, target, resolved_expression) before building
    # the forward map. The same script logic often appears across multiple files
    # (e.g. script 2.sh / 3.sh / 4.sh), which would otherwise cause combinatorial
    # explosion in chain counts.
    seen_edge_keys: Set[tuple] = set()
    unique_edges: List[dict] = []
    for edge in edges:
        src = edge.get("source_column")
        tgt = edge.get("target_column")
        if not (src and tgt):
            continue
        key = (src, tgt, edge.get("resolved_expression", ""), edge.get("statement_order", ""))
        if key not in seen_edge_keys:
            seen_edge_keys.add(key)
            unique_edges.append(edge)

    # Forward map: source_col -> list of outgoing edges
    forward: Dict[str, List[dict]] = defaultdict(list)
    all_targets: Set[str] = set()
    all_sources: Set[str] = set()

    for edge in unique_edges:
        src = edge.get("source_column")
        tgt = edge.get("target_column")
        forward[src].append(edge)
        all_targets.add(tgt)
        all_sources.add(src)

    # Root columns: columns that appear as a source but never as a target
    root_cols = all_sources - all_targets

    chains: List[dict] = []
    # Track (ultimate_source, ultimate_target) pairs already recorded.
    # We only keep the first (shortest) path found for each pair — this prevents
    # combinatorial explosion when many intermediate nodes fan out.
    seen_pairs: Set[tuple] = set()

    def dfs(col: str, path: List[dict], visited: Set[str]):
        next_edges = forward.get(col, [])
        if not next_edges:
            if path:
                ult_src = path[0]["source_column"]
                ult_tgt = path[-1]["target_column"]
                pair = (ult_src, ult_tgt)
                if pair in seen_pairs:
                    return
                seen_pairs.add(pair)
                chains.append({
                    "ultimate_source": ult_src,
                    "ultimate_target": ult_tgt,
                    "hops": len(path),
                    "path": [
                        {
                            "hop": i + 1,
                            "source_column": e["source_column"],
                            "target_column": e["target_column"],
                            "statement_order": e["statement_order"],
                            "statement_type": e["statement_type"],
                            "resolved_expression": e["resolved_expression"],
                            "classification": e["classification"],
                            "script_name": e.get("script_name", ""),
                        }
                        for i, e in enumerate(path)
                    ],
                })
            return
        for edge in next_edges:
            tgt = edge["target_column"]
            if tgt in visited:
                continue
            visited.add(tgt)
            dfs(tgt, path + [edge], visited)
            visited.discard(tgt)

    for root in sorted(root_cols):
        dfs(root, [], {root})

    return sorted(chains, key=lambda c: (c["ultimate_source"], c["ultimate_target"]))


# ---------------------------------------------------------------------------
# Column lineage (transitive closure per target column)
# ---------------------------------------------------------------------------

def build_column_lineage(edges: List[dict], chains: List[dict]) -> List[dict]:
    """
    For every target column that appears anywhere in the lineage, emit:
      - its direct sources (one hop back)
      - its ultimate base sources (fully resolved back to raw tables)
      - every chain that ends at this column
    """
    # Group edges by target
    by_target: Dict[str, List[dict]] = defaultdict(list)
    for e in edges:
        if e.get("source_column"):
            by_target[e["target_column"]].append(e)

    # Group chains by ultimate target
    chains_by_target: Dict[str, List[dict]] = defaultdict(list)
    for c in chains:
        chains_by_target[c["ultimate_target"]].append(c)

    all_targets = sorted({e["target_column"] for e in edges if e.get("target_column")})

    lineage = []
    for col in all_targets:
        direct_sources = sorted({e["source_column"] for e in by_target.get(col, []) if e.get("source_column")})
        ultimate_sources = sorted({c["ultimate_source"] for c in chains_by_target.get(col, []) if c.get("ultimate_source")})
        chain_paths = chains_by_target.get(col, [])

        lineage.append({
            "target_column": col,
            "direct_sources": direct_sources,
            "ultimate_base_sources": ultimate_sources,
            "transformation_chains": [
                {
                    "ultimate_source": c["ultimate_source"],
                    "hops": c["hops"],
                    "path": c["path"],
                }
                for c in chain_paths
            ],
        })

    return lineage


# ---------------------------------------------------------------------------
# CSV writers
# ---------------------------------------------------------------------------

def _split_col(qualified: Optional[str]):
    """Split 'TABLE.COLUMN' into (table, column). Returns ('', '') for None."""
    if not qualified:
        return "", ""
    parts = qualified.split(".", 1)
    return (parts[0], parts[1]) if len(parts) == 2 else (parts[0], "")


def write_edges_csv(edges: List[dict], path: str) -> None:
    """
    One row per direct source->target edge.

    Columns:
      script_name, statement_order, statement_type,
      source_table, source_column_name, source_column,
      target_table, target_column_name, target_column,
      resolved_expression, classification, merge_clause, resolution_note
    """
    fieldnames = [
        "script_name", "statement_order", "statement_type",
        "source_table", "source_column_name", "source_column",
        "target_table", "target_column_name", "target_column",
        "resolved_expression", "classification", "merge_clause", "resolution_note",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for e in edges:
            src_tbl, src_col = _split_col(e.get("source_column"))
            tgt_tbl, tgt_col = _split_col(e.get("target_column"))
            w.writerow({
                "script_name": e.get("script_name", ""),
                "statement_order": e.get("statement_order", ""),
                "statement_type": e.get("statement_type", ""),
                "source_table": src_tbl,
                "source_column_name": src_col,
                "source_column": e.get("source_column") or "",
                "target_table": tgt_tbl,
                "target_column_name": tgt_col,
                "target_column": e.get("target_column") or "",
                "resolved_expression": e.get("resolved_expression", ""),
                "classification": e.get("classification", ""),
                "merge_clause": e.get("merge_clause", ""),
                "resolution_note": e.get("resolution_note", ""),
            })


def write_chains_csv(chains: List[dict], path: str) -> None:
    """
    One row per hop in every chain, with ultimate source/target context on every row
    so the full path can be reconstructed by filtering on (ultimate_source, ultimate_target).

    Columns:
      ultimate_source, ultimate_target, total_hops,
      hop_number,
      hop_source_table, hop_source_column_name, hop_source_column,
      hop_target_table, hop_target_column_name, hop_target_column,
      statement_order, statement_type,
      resolved_expression, classification
    """
    fieldnames = [
        "ultimate_source", "ultimate_target", "total_hops",
        "hop_number",
        "hop_source_table", "hop_source_column_name", "hop_source_column",
        "hop_target_table", "hop_target_column_name", "hop_target_column",
        "statement_order", "statement_type",
        "resolved_expression", "classification",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for chain in chains:
            ult_src = chain["ultimate_source"]
            ult_tgt = chain["ultimate_target"]
            total = chain["hops"]
            for hop in chain["path"]:
                src_tbl, src_col = _split_col(hop.get("source_column"))
                tgt_tbl, tgt_col = _split_col(hop.get("target_column"))
                w.writerow({
                    "ultimate_source": ult_src,
                    "ultimate_target": ult_tgt,
                    "total_hops": total,
                    "hop_number": hop["hop"],
                    "hop_source_table": src_tbl,
                    "hop_source_column_name": src_col,
                    "hop_source_column": hop.get("source_column", ""),
                    "hop_target_table": tgt_tbl,
                    "hop_target_column_name": tgt_col,
                    "hop_target_column": hop.get("target_column", ""),
                    "statement_order": hop.get("statement_order", ""),
                    "statement_type": hop.get("statement_type", ""),
                    "resolved_expression": hop.get("resolved_expression", ""),
                    "classification": hop.get("classification", ""),
                })


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Build column-level lineage graph from semantic parse output.")
    ap.add_argument("--input", required=True, help="Path to output_semantic_parse.json")
    ap.add_argument("--out", required=True, help="Output lineage JSON path")
    ap.add_argument("--out-csv", default=None,
                    help="Base path for CSV output. Writes <base>_edges.csv and <base>_chains.csv")
    args = ap.parse_args()

    with open(args.input, encoding="utf-8") as f:
        data = json.load(f)

    edges = extract_edges(data)
    chains = build_chains(edges)
    col_lineage = build_column_lineage(edges, chains)

    resolved = [e for e in edges if e.get("source_column")]
    unresolved = [e for e in edges if not e.get("source_column")]

    result = {
        "summary": {
            "total_edges": len(edges),
            "resolved_edges": len(resolved),
            "unresolved_edges": len(unresolved),
            "total_chains": len(chains),
            "max_chain_hops": max((c["hops"] for c in chains), default=0),
            "unique_target_columns": len(col_lineage),
        },
        "edges": edges,
        "chains": chains,
        "column_lineage": col_lineage,
    }

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)
    print(f"JSON  -> {args.out}")

    if args.out_csv:
        edges_path = f"{args.out_csv}_edges.csv"
        chains_path = f"{args.out_csv}_chains.csv"
        write_edges_csv(edges, edges_path)
        write_chains_csv(chains, chains_path)
        print(f"CSV   -> {edges_path}  ({len(edges)} rows)")
        print(f"CSV   -> {chains_path}  ({sum(c['hops'] for c in chains)} rows)")

    print(f"\nEdges  : {len(edges)} total ({len(resolved)} resolved, {len(unresolved)} unresolved/constant)")
    print(f"Chains : {len(chains)} (max {result['summary']['max_chain_hops']} hops)")
    print(f"Columns: {len(col_lineage)} unique target columns tracked")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
