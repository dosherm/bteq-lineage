#!/usr/bin/env python3
"""
run_bteq_corpus_semantic_parse_v2.py

Redesigned for visibility and restartability:
  - Processes one file at a time
  - Prints progress after each file: [N/total] filename | stmts, INS, CRE, UPD | elapsed | running totals
  - Checkpoints to a .jsonl file after each file — crash-safe, restartable
  - On restart, skips already-completed files and resumes from where it left off
  - Prints final summary with total wall time

Usage:
  python run_bteq_corpus_semantic_parse_v2.py \\
    --dir BTEQ \\
    --prefix "" \\
    --dict-csv EDWard_Attribute_Schema_Table_Column.csv \\
    --out output_semantic_parse_all.json \\
    --checkpoint output_semantic_parse_checkpoint.jsonl

  Add --reset to discard any existing checkpoint and start fresh.
"""

import argparse
import datetime as _dt
import json
import logging
import multiprocessing
import os
import re
import sys
import time
import zipfile
from typing import Dict, List, Optional, Tuple

# Default limits
DEFAULT_MAX_STMT_CHARS = 10_000   # statements larger than this are skipped as too large
DEFAULT_STMT_TIMEOUT   = 10       # seconds before a parse call is abandoned

from sql_semantic_parser import (
    parse_insert_semantics,
    parse_create_semantics,
    parse_update_semantics,
    parse_merge_semantics,
    dq_evaluate,
)


# -----------------------------
# Dictionary loading
# -----------------------------
def load_schema_table_column_csv(csv_path: str) -> Dict[str, Dict[str, str]]:
    import csv

    idx: Dict[str, Dict[str, str]] = {}
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        headers = {h.strip().lower(): h for h in (reader.fieldnames or [])}
        if "schema" not in headers or "table" not in headers or "column" not in headers:
            raise ValueError(
                f"CSV must have headers Schema, Table, Column (got: {reader.fieldnames})"
            )
        for row in reader:
            schema = (row[headers["schema"]] or "").strip()
            table = (row[headers["table"]] or "").strip()
            col = (row[headers["column"]] or "").strip()
            if not schema or not table or not col:
                continue
            key = col.upper()
            fq = f"{schema}.{table}"
            idx.setdefault(key, {})[fq] = fq

    return idx


# -----------------------------
# File listing
# -----------------------------
def list_scripts(args) -> List[str]:
    """Return sorted list of absolute file paths to process."""
    if args.corpus:
        raise NotImplementedError("--corpus not supported in v2; extract first and use --dir")
    elif args.single:
        return [os.path.abspath(args.single)]
    else:
        dir_path = args.dir
        ext = ".sh"
        paths = []
        for fname in sorted(os.listdir(dir_path)):
            if not fname.lower().endswith(ext):
                continue
            fpath = os.path.join(dir_path, fname)
            paths.append(os.path.abspath(fpath))
        return paths


# -----------------------------
# SQL extraction
# -----------------------------
_SQL_STMT_SPLIT_RE = re.compile(r";\s*(?:\n|$)", re.MULTILINE)
# Strip shell variable database prefixes: $DBNAME.TABLE -> TABLE
_SHELL_DB_PREFIX_RE = re.compile(r'\$\{?[A-Za-z_]\w*\}?\.')


def extract_sql_statements(script_text: str) -> List[Tuple[int, str]]:
    lines = []
    for ln in script_text.splitlines():
        s = ln.strip()
        if not s:
            continue
        if s.startswith("."):
            continue
        lines.append(ln)

    joined = "\n".join(lines).strip()
    # Remove shell variable database prefixes so $DBNAME.CLM becomes CLM
    joined = _SHELL_DB_PREFIX_RE.sub("", joined)
    if not joined:
        return []

    parts = [p.strip() for p in _SQL_STMT_SPLIT_RE.split(joined)]
    parts = [p for p in parts if p]

    out: List[Tuple[int, str]] = []
    for i, p in enumerate(parts, start=1):
        out.append((i, p + ";"))
    return out


_BLOCK_COMMENT_RE = re.compile(r"/\*.*?\*/", re.DOTALL)
_LINE_COMMENT_RE = re.compile(r"--[^\n]*")


def _strip_leading_comments(stmt: str) -> str:
    s = stmt
    while True:
        stripped = s.lstrip()
        if stripped.startswith("/*"):
            end = stripped.find("*/")
            if end == -1:
                break
            s = stripped[end + 2:]
        elif stripped.startswith("--"):
            end = stripped.find("\n")
            s = stripped[end + 1:] if end != -1 else ""
        else:
            break
    return s.lstrip()


def classify_statement(stmt: str) -> str:
    s = _strip_leading_comments(stmt).upper()
    if s.startswith("INSERT"):
        return "INSERT"
    if s.startswith("UPDATE"):
        return "UPDATE"
    if s.startswith("DELETE"):
        return "DELETE"
    if s.startswith("MERGE"):
        return "MERGE"
    if s.startswith("CREATE"):
        return "CREATE"
    if s.startswith("SELECT"):
        return "SELECT"
    return "OTHER"


# -----------------------------
# Table-name helpers (used by both positional and simplified parsers)
# -----------------------------
_WS = r'[\s\n\r]+'
_ID = r'[A-Za-z0-9_$.]+'

_INSERT_TARGET_RE  = re.compile(r'INSERT\s+INTO\s+(' + _ID + r')', re.I)
_UPDATE_TARGET_RE  = re.compile(r'UPDATE\s+(' + _ID + r')', re.I)
_FROM_TABLE_RE     = re.compile(r'(?:FROM|JOIN)\s+(' + _ID + r')', re.I)
_SET_FROM_RE       = re.compile(r'FROM\s+(' + _ID + r')', re.I)   # UPDATE ... FROM

# SQL keywords to exclude when extracting column references from expressions
_SQL_KEYWORDS = {
    "CASE","WHEN","THEN","ELSE","END","AND","OR","NOT","IN","IS","NULL","AS",
    "SELECT","FROM","WHERE","JOIN","LEFT","RIGHT","INNER","OUTER","ON","COALESCE",
    "TRIM","CAST","SUBSTR","SUBSTRING","UPPER","LOWER","LENGTH","CHAR","VARCHAR",
    "DATE","INTEGER","DECIMAL","FLOAT","BETWEEN","LIKE","EXISTS","DISTINCT",
    "GROUP","BY","HAVING","ORDER","QUALIFY","OVER","PARTITION","ROW_NUMBER",
    "RANK","SUM","COUNT","MAX","MIN","AVG","EXTRACT","YEAR","MONTH","DAY",
    "INTERVAL","TIMESTAMP","FORMAT","NAMED","TITLE","COMPRESS","INSERT","INTO",
    "VALUES","UPDATE","SET","DELETE","MERGE","USING","MATCHED","TARGET","SOURCE",
    "TRUE","FALSE","ZEROIFNULL","NULLIFZERO","INDEX","REPLACE","CHAR_LENGTH",
}


def _split_top_level_commas(text: str) -> List[str]:
    """Split by commas at depth 0 (not inside parentheses)."""
    parts: List[str] = []
    depth = 0
    current: List[str] = []
    for ch in text:
        if ch == "(":
            depth += 1
            current.append(ch)
        elif ch == ")":
            depth -= 1
            current.append(ch)
        elif ch == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(ch)
    if current:
        parts.append("".join(current).strip())
    return [p for p in parts if p]


def _extract_col_refs_from_expr(expr: str) -> List[str]:
    """Pull TABLE.COLUMN or COLUMN identifiers from an expression, skipping keywords."""
    expr_clean = re.sub(r"'[^']*'", " ", expr)
    tokens = re.findall(r'\b([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)?)\b', expr_clean)
    seen: set = set()
    result: List[str] = []
    for tok in tokens:
        parts = tok.split(".")
        if all(p.upper() in _SQL_KEYWORDS for p in parts):
            continue
        if tok.replace(".", "").isdigit():
            continue
        up = tok.upper()
        if up not in seen:
            seen.add(up)
            result.append(up)
    return result


def _parse_insert_positional(stmt: str) -> Optional[Dict]:
    """
    Positional parser for INSERT INTO target (col_list) SELECT expr_list FROM ...
    Matches INSERT column N to SELECT expression N to produce column-level lineage.
    Returns None if the pattern is not recognised.
    """
    ins_m = re.search(r'INSERT\s+(?:INTO\s+)?(\S+)\s*\(', stmt, re.IGNORECASE)
    if not ins_m:
        return None
    target_table = ins_m.group(1)

    # Extract INSERT column list (inside the first top-level parens after table name)
    start = stmt.index("(", ins_m.start())
    depth = 0
    i = start
    while i < len(stmt):
        if stmt[i] == "(": depth += 1
        elif stmt[i] == ")":
            depth -= 1
            if depth == 0:
                break
        i += 1
    col_list_raw = stmt[start + 1:i]
    after_cols = stmt[i + 1:]

    # Find SELECT after the closing paren
    sel_m = re.search(r'\bSELECT\b', after_cols, re.IGNORECASE)
    if not sel_m:
        return None
    sel_body = after_cols[sel_m.end():]

    # Find FROM at depth 0 to delimit the SELECT expression list
    depth = 0
    from_pos = None
    j = 0
    while j < len(sel_body):
        if sel_body[j] == "(":
            depth += 1
        elif sel_body[j] == ")":
            depth -= 1
        elif depth == 0:
            chunk = sel_body[j:j + 4].upper()
            if chunk == "FROM":
                before_ok = j == 0 or not sel_body[j - 1].isalnum()
                after_ok = j + 4 >= len(sel_body) or not sel_body[j + 4].isalnum()
                if before_ok and after_ok:
                    from_pos = j
                    break
        j += 1

    select_exprs_raw = sel_body[:from_pos] if from_pos is not None else sel_body

    # Strip block comments from column list
    col_list_clean = re.sub(r'/\*.*?\*/', '', col_list_raw, flags=re.DOTALL)
    col_names = _split_top_level_commas(col_list_clean)
    col_names = [c.strip() for c in col_names if c.strip()]

    select_exprs = _split_top_level_commas(select_exprs_raw)

    if not col_names or not select_exprs:
        return None
    if len(col_names) != len(select_exprs):
        # Mismatch — still emit what we can, up to the shorter list
        pass

    column_semantics = []
    for idx, col in enumerate(col_names):
        if idx >= len(select_exprs):
            break
        expr = select_exprs[idx].strip()
        # Strip trailing alias (AS name)
        expr_clean = re.sub(r'\bAS\s+\w+\s*$', '', expr, flags=re.IGNORECASE).strip()
        refs = _extract_col_refs_from_expr(expr_clean)
        # Format refs as TABLE.COLUMN where possible
        base_sources = [r for r in refs if "." in r]
        if not base_sources:
            base_sources = [r for r in refs if r.upper() not in _SQL_KEYWORDS]

        column_semantics.append({
            "target_column": f"{target_table}.{col}",
            "base_sources": base_sources,
            "resolved_expression": expr_clean[:300],
            "classification": "DIRECT" if len(base_sources) == 1 else ("DERIVED" if base_sources else "CONSTANT"),
        })

    if not column_semantics:
        return None

    source_tables = list(dict.fromkeys(
        t for t in _FROM_TABLE_RE.findall(stmt)
        if t.upper() != target_table.upper()
    ))

    return {
        "parse_mode": "positional",
        "target_table": target_table,
        "source_tables": source_tables,
        "column_semantics": column_semantics,
        "notes": [f"Positional parser: {len(column_semantics)} columns mapped"],
    }


def _parse_insert_simplified(stmt: str) -> Dict:
    """Last-resort fallback: table-level lineage only, no column mapping."""
    m = _INSERT_TARGET_RE.search(stmt)
    target = m.group(1) if m else "UNKNOWN"
    sources = list(dict.fromkeys(
        t for t in _FROM_TABLE_RE.findall(stmt)
        if t.upper() != target.upper()
    ))
    return {
        "parse_mode": "simplified",
        "target_table": target,
        "source_tables": sources,
        "column_mappings": [],
        "notes": ["Column-level mapping unavailable: positional parser could not parse structure"],
    }


def _parse_update_simplified(stmt: str) -> Dict:
    m = _UPDATE_TARGET_RE.search(stmt)
    target = m.group(1) if m else "UNKNOWN"
    sources = list(dict.fromkeys(
        t for t in _FROM_TABLE_RE.findall(stmt)
        if t.upper() != target.upper()
    ))
    return {
        "parse_mode": "simplified",
        "target_table": target,
        "source_tables": sources,
        "column_mappings": [],
        "notes": ["Column-level mapping unavailable: statement too large or timed out for full parser"],
    }


# -----------------------------
# Per-script processing
# -----------------------------
def _mp_worker(result_queue, fn_name, stmt, column_dictionary):
    """Worker function run in a child process for timeout-safe parsing."""
    from sql_semantic_parser import (
        parse_insert_semantics, parse_create_semantics,
        parse_update_semantics, parse_merge_semantics,
    )
    fn = {
        "parse_insert_semantics": parse_insert_semantics,
        "parse_create_semantics": parse_create_semantics,
        "parse_update_semantics": parse_update_semantics,
        "parse_merge_semantics":  parse_merge_semantics,
    }[fn_name]
    try:
        result_queue.put(("ok", fn(stmt, column_dictionary=column_dictionary)))
    except Exception as e:
        result_queue.put(("err", f"{type(e).__name__}: {e}"))


# Pattern that causes sqlglot catastrophic slowdown:
# chained || concatenation with CASE WHEN expressions (3+ occurrences is a reliable signal)
_CONCAT_CHAIN_RE = re.compile(r'\|\|.{0,200}\|\|.{0,200}\|\|', re.DOTALL)


def _will_timeout(stmt: str) -> bool:
    """Quick pre-scan to detect patterns known to make sqlglot hang."""
    return bool(_CONCAT_CHAIN_RE.search(stmt))


def _parse_with_timeout(fn, stmt, column_dictionary, timeout_secs):
    """Run fn in a child process; SIGKILL and return error if it exceeds timeout_secs."""
    q = multiprocessing.Queue()
    p = multiprocessing.Process(target=_mp_worker, args=(q, fn.__name__, stmt, column_dictionary))
    p.start()
    p.join(timeout=timeout_secs)
    if p.is_alive():
        p.kill()        # SIGKILL — cannot be deferred by C extensions unlike SIGTERM
        p.join(timeout=3)
        return None, f"ParseTimeout: exceeded {timeout_secs}s"
    if not q.empty():
        status, val = q.get()
        return (val, None) if status == "ok" else (None, val)
    return None, "ParseError: worker exited without result"


def process_script(
    script_name: str,
    full_script_text: str,
    column_dictionary: Optional[Dict[str, Dict[str, str]]] = None,
    max_stmt_chars: int = DEFAULT_MAX_STMT_CHARS,
    stmt_timeout: int = DEFAULT_STMT_TIMEOUT,
) -> Optional[Dict]:
    sql_stmts = extract_sql_statements(full_script_text)
    if not sql_stmts:
        return None

    sql_nodes: List[Dict] = []
    insert_ok = insert_fail = 0
    create_ok = create_fail = 0
    update_ok = update_fail = 0
    merge_ok = merge_fail = 0
    insert_total = create_total = update_total = merge_total = 0

    block_id = 1

    for order, stmt in sql_stmts:
        clean_stmt = _strip_leading_comments(stmt)
        stype = classify_statement(clean_stmt)
        node: Dict = {
            "order": order,
            "block": block_id,
            "statement_type": stype,
            "statement_text": stmt,
        }

        # Decide parse path: full parser or simplified fallback
        too_large = stype in ("INSERT", "CREATE", "UPDATE", "MERGE") and len(clean_stmt) > max_stmt_chars

        if stype == "INSERT":
            insert_total += 1
            if too_large or _will_timeout(clean_stmt):
                # Skip full sqlglot parser — use positional parser instead
                pos = _parse_insert_positional(clean_stmt)
                if pos:
                    node["semantic_parse"] = pos
                    node["dq"] = None
                    reason = "too large" if too_large else "concat-chain pattern detected"
                    node["parse_note"] = f"PositionalParse: {reason}"
                else:
                    node["semantic_parse"] = _parse_insert_simplified(clean_stmt)
                    node["dq"] = None
                    node["parse_note"] = f"SimplifiedParse: positional parser failed"
                insert_ok += 1
            else:
                sem, err = _parse_with_timeout(parse_insert_semantics, clean_stmt, column_dictionary, stmt_timeout)
                if err:
                    # Full parser failed — try positional before simplified
                    pos = _parse_insert_positional(clean_stmt)
                    if pos:
                        node["semantic_parse"] = pos
                        node["dq"] = None
                        node["parse_note"] = f"PositionalParse: full parser failed ({err})"
                    else:
                        node["semantic_parse"] = _parse_insert_simplified(clean_stmt)
                        node["dq"] = None
                        node["parse_note"] = f"SimplifiedParse: full parser failed ({err})"
                    insert_ok += 1
                else:
                    node["semantic_parse"] = sem
                    node["dq"] = dq_evaluate(sem)
                    insert_ok += 1

        elif stype == "CREATE":
            create_total += 1
            if too_large:
                node["semantic_parse"] = None
                node["dq"] = None
                node["parse_error"] = f"StatementTooLarge: {len(clean_stmt):,} chars exceeds limit of {max_stmt_chars:,}"
                create_fail += 1
            else:
                sem, err = _parse_with_timeout(parse_create_semantics, clean_stmt, column_dictionary, stmt_timeout)
                if err:
                    node["semantic_parse"] = None
                    node["dq"] = None
                    node["parse_error"] = err
                    create_fail += 1
                else:
                    node["semantic_parse"] = sem
                    node["dq"] = dq_evaluate(sem)
                    create_ok += 1

        elif stype == "UPDATE":
            update_total += 1
            if too_large or _will_timeout(clean_stmt):
                node["semantic_parse"] = _parse_update_simplified(clean_stmt)
                node["dq"] = None
                node["parse_note"] = f"SimplifiedParse: {'too large' if too_large else 'concat-chain pattern detected'}"
                update_ok += 1
            else:
                sem, err = _parse_with_timeout(parse_update_semantics, clean_stmt, column_dictionary, stmt_timeout)
                if err:
                    node["semantic_parse"] = _parse_update_simplified(clean_stmt)
                    node["dq"] = None
                    node["parse_note"] = f"SimplifiedParse: full parser failed ({err})"
                    update_ok += 1
                else:
                    node["semantic_parse"] = sem
                    node["dq"] = dq_evaluate(sem)
                    update_ok += 1

        elif stype == "MERGE":
            merge_total += 1
            if too_large:
                node["semantic_parse"] = None
                node["dq"] = None
                node["parse_error"] = f"StatementTooLarge: {len(clean_stmt):,} chars exceeds limit of {max_stmt_chars:,}"
                merge_fail += 1
            else:
                sem, err = _parse_with_timeout(parse_merge_semantics, clean_stmt, column_dictionary, stmt_timeout)
                if err:
                    node["semantic_parse"] = None
                    node["dq"] = None
                    node["parse_error"] = err
                    merge_fail += 1
                else:
                    node["semantic_parse"] = sem
                    node["dq"] = dq_evaluate(sem)
                    merge_ok += 1

        else:
            node["semantic_parse"] = None
            node["dq"] = None
            node["notes"] = [f"Not parsed: statement type '{stype}' is not currently supported."]

        sql_nodes.append(node)

    return {
        "script_name": script_name,
        "sql_statement_count": len(sql_nodes),
        "insert_statement_count": insert_total,
        "insert_parsed_ok": insert_ok,
        "insert_parse_failures": insert_fail,
        "create_statement_count": create_total,
        "create_parsed_ok": create_ok,
        "create_parse_failures": create_fail,
        "update_statement_count": update_total,
        "update_parsed_ok": update_ok,
        "update_parse_failures": update_fail,
        "merge_statement_count": merge_total,
        "merge_parsed_ok": merge_ok,
        "merge_parse_failures": merge_fail,
        "statements": sql_nodes,
    }


# -----------------------------
# Checkpoint helpers
# -----------------------------
def load_checkpoint(path: str) -> Dict[str, Dict]:
    """Read existing checkpoint. Returns dict of script_name -> result."""
    if not os.path.exists(path):
        return {}
    done = {}
    with open(path, "r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                done[rec["script_name"]] = rec
            except Exception as e:
                print(f"  WARNING: checkpoint line {lineno} unreadable ({e}), skipping", file=sys.stderr)
    return done


def append_checkpoint(path: str, result: Optional[Dict], script_name: str) -> None:
    """Append one script result to the checkpoint file."""
    rec = result if result is not None else {"script_name": script_name, "_skipped": True}
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec) + "\n")


# -----------------------------
# Output compilation
# -----------------------------
def compile_output(
    checkpoint: Dict[str, Dict],
    input_descriptor: Dict,
    args,
    wall_seconds: float,
) -> Dict:
    out_scripts = []
    sql_total = inserts_ok = inserts_fail = creates_ok = creates_fail = 0
    updates_ok = updates_fail = merges_ok = merges_fail = 0
    inserts_total = creates_total = updates_total = merges_total = 0
    dq_missing = 0
    skipped = 0

    for rec in checkpoint.values():
        if rec.get("_skipped"):
            skipped += 1
            continue
        out_scripts.append(rec)
        sql_total += rec["sql_statement_count"]
        inserts_total += rec["insert_statement_count"]
        inserts_ok += rec["insert_parsed_ok"]
        inserts_fail += rec["insert_parse_failures"]
        creates_total += rec["create_statement_count"]
        creates_ok += rec["create_parsed_ok"]
        creates_fail += rec["create_parse_failures"]
        updates_total += rec["update_statement_count"]
        updates_ok += rec["update_parsed_ok"]
        updates_fail += rec["update_parse_failures"]
        merges_total += rec["merge_statement_count"]
        merges_ok += rec["merge_parsed_ok"]
        merges_fail += rec["merge_parse_failures"]
        for st in rec.get("statements", []):
            dq = st.get("dq")
            if isinstance(dq, dict):
                dq_missing += len(dq.get("missing_base_sources") or [])

    # Sort output scripts by name for deterministic output
    out_scripts.sort(key=lambda r: r["script_name"])

    return {
        "generated_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "wall_seconds": round(wall_seconds, 1),
        "input": input_descriptor,
        "filters": {"script_name_prefix": args.prefix or ""},
        "dictionary": {
            "enabled": bool(args.dict_csv),
            "source": os.path.abspath(args.dict_csv) if args.dict_csv else None,
        },
        "summary": {
            "scripts_emitted": len(out_scripts),
            "scripts_skipped_no_sql": skipped,
            "sql_statements_emitted": sql_total,
            "insert_statements_emitted": inserts_total,
            "insert_parsed_ok": inserts_ok,
            "insert_parse_failures": inserts_fail,
            "create_statements_emitted": creates_total,
            "create_parsed_ok": creates_ok,
            "create_parse_failures": creates_fail,
            "update_statements_emitted": updates_total,
            "update_parsed_ok": updates_ok,
            "update_parse_failures": updates_fail,
            "merge_statements_emitted": merges_total,
            "merge_parsed_ok": merges_ok,
            "merge_parse_failures": merges_fail,
            "dq_missing_base_sources_total": dq_missing,
        },
        "scripts": out_scripts,
    }


# -----------------------------
# Logging setup
# -----------------------------
def setup_logging(log_path: str) -> logging.Logger:
    """Set up a logger that writes to both the terminal (INFO) and a log file (DEBUG)."""
    logger = logging.getLogger("lineage")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    # File handler — full detail
    fh = logging.FileHandler(log_path, mode="a", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-7s  %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
    logger.addHandler(fh)

    # Console handler — info and above only (progress output is handled separately via print)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.WARNING)   # only warnings/errors to terminal; progress uses print()
    ch.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    logger.addHandler(ch)

    return logger


# -----------------------------
# Progress bar / milestone
# -----------------------------
_MILESTONE_PCT = 10   # print a visual milestone every N percent

def _progress_bar(done: int, total: int, width: int = 40) -> str:
    """Return an ASCII progress bar string, e.g.  [████████░░░░░░░░░░░░]  40%"""
    pct = done / total if total else 0
    filled = int(width * pct)
    bar = "█" * filled + "░" * (width - filled)
    return f"[{bar}] {pct*100:5.1f}%  ({done}/{total} files)"


def _milestone_banner(done: int, total: int, rt: dict) -> None:
    """Print a prominent milestone line at each 10% boundary."""
    pct = int((done / total) * 100) if total else 0
    bar = _progress_bar(done, total)
    print(f"\n{'─'*110}", flush=True)
    print(f"  ▶  PROGRESS  {bar}", flush=True)
    print(
        f"     Totals so far: {rt['stmts']} stmts | "
        f"INS {rt['ins_ok']}ok/{rt['ins_fail']}fail | "
        f"CRE {rt['cre_ok']}ok/{rt['cre_fail']}fail | "
        f"UPD {rt['upd_ok']}ok/{rt['upd_fail']}fail",
        flush=True,
    )
    print(f"{'─'*110}\n", flush=True)


# -----------------------------
# CLI
# -----------------------------
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--corpus", help="Path to bteq_script_corpus.json.zip (not supported)")
    ap.add_argument("--single", help="Path to a single .sh/.sql file")
    ap.add_argument("--dir", help="Directory of .sh files")
    ap.add_argument("--prefix", default="ACES_CLM", help="script_name prefix filter ('' to disable)")
    ap.add_argument("--out", required=True, help="Output JSON path")
    ap.add_argument("--dict-csv", default=None, help="CSV with Schema,Table,Column columns")
    ap.add_argument(
        "--max-stmt-chars",
        type=int,
        default=DEFAULT_MAX_STMT_CHARS,
        help=f"Skip statements larger than this many characters (default: {DEFAULT_MAX_STMT_CHARS:,})",
    )
    ap.add_argument(
        "--stmt-timeout",
        type=int,
        default=DEFAULT_STMT_TIMEOUT,
        help=f"Seconds before abandoning a single statement parse (default: {DEFAULT_STMT_TIMEOUT})",
    )
    ap.add_argument(
        "--checkpoint",
        default=None,
        help="JSONL checkpoint file for crash-safe resume (default: <out>.checkpoint.jsonl)",
    )
    ap.add_argument(
        "--reset",
        action="store_true",
        help="Discard existing checkpoint and start fresh",
    )
    ap.add_argument(
        "--files",
        default=None,
        help="Comma-separated list of script filenames to (re)parse selectively, "
             "e.g. --files CLM_BTEQ_EDW_CLM_LOAD.sh,OTHER_SCRIPT.sh  "
             "Removes those entries from the checkpoint and reprocesses only them, "
             "leaving all other previously parsed scripts intact.",
    )
    ap.add_argument(
        "--log",
        default=None,
        help="Log file path (default: <out>.log)",
    )
    args = ap.parse_args()

    inputs = [x for x in [args.corpus, args.single, args.dir] if x]
    if len(inputs) != 1:
        ap.error("Provide exactly one of --corpus, --single, or --dir")

    checkpoint_path = args.checkpoint or (args.out.replace(".json", "") + ".checkpoint.jsonl")
    log_path = args.log or (args.out.replace(".json", "") + ".log")

    # Set up logging
    logger = setup_logging(log_path)
    run_start_ts = _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info("=" * 80)
    logger.info(f"Run started at {run_start_ts}")
    logger.info(f"Output: {args.out}")
    logger.info(f"Checkpoint: {checkpoint_path}")
    logger.info(f"Log: {log_path}")
    print(f"Log file: {log_path}", flush=True)

    # Load column dictionary
    column_dictionary = None
    if args.dict_csv:
        print(f"Loading column dictionary from {args.dict_csv} ...", flush=True)
        t0 = time.time()
        column_dictionary = load_schema_table_column_csv(args.dict_csv)
        print(f"  {len(column_dictionary):,} column entries loaded in {time.time()-t0:.1f}s", flush=True)

    # List files
    all_paths = list_scripts(args)
    prefix = args.prefix or ""
    if prefix:
        all_paths = [p for p in all_paths if os.path.basename(p).startswith(prefix)]

    total = len(all_paths)
    logger.info(f"Scripts to process: {total}")
    print(f"\n{total} scripts to process  |  checkpoint: {checkpoint_path}", flush=True)
    print(_progress_bar(0, total), flush=True)

    # Load or reset checkpoint
    if args.reset and os.path.exists(checkpoint_path):
        os.remove(checkpoint_path)
        logger.info("Checkpoint reset (--reset flag)")
        print("Checkpoint reset.", flush=True)

    checkpoint = load_checkpoint(checkpoint_path)

    # Selective re-run: evict named files from checkpoint so they get reprocessed,
    # then restrict all_paths to only those files.
    if args.files:
        target_names = {f.strip() for f in args.files.split(",") if f.strip()}
        evicted = [n for n in target_names if n in checkpoint]
        for name in evicted:
            del checkpoint[name]
        if evicted:
            # Rewrite checkpoint file without the evicted entries
            with open(checkpoint_path, "w", encoding="utf-8") as f:
                for rec in checkpoint.values():
                    f.write(json.dumps(rec) + "\n")
            logger.info(f"Selective re-run: evicted {evicted} from checkpoint")
            print(f"Selective re-run: removed {evicted} from checkpoint, will reparse.", flush=True)
        # Restrict the file list to only the named files
        all_paths = [p for p in all_paths if os.path.basename(p) in target_names]
        not_found = target_names - {os.path.basename(p) for p in all_paths}
        if not_found:
            print(f"WARNING: these files were not found in the directory: {sorted(not_found)}", flush=True)
        total = len(all_paths)
        print(f"Selective mode: processing {total} file(s): {[os.path.basename(p) for p in all_paths]}", flush=True)

    already_done = sum(1 for r in checkpoint.values() if not r.get("_skipped"))
    remaining = total - sum(1 for p in all_paths if os.path.basename(p) in checkpoint)
    if already_done and not args.files:
        logger.info(f"Resuming: {already_done} already done, {remaining} remaining")
        print(f"Resuming: {already_done} scripts already in checkpoint, {remaining} remaining.\n", flush=True)

    # Determine input descriptor
    if args.dir:
        input_descriptor = {"type": "directory", "path": os.path.abspath(args.dir), "file_count": total}
    else:
        input_descriptor = {"type": "single_file", "path": os.path.abspath(args.single)}

    # Running totals
    rt_stmts = rt_ins_ok = rt_ins_fail = rt_cre_ok = rt_cre_fail = 0
    rt_upd_ok = rt_upd_fail = rt_mrg_ok = rt_mrg_fail = 0
    last_milestone_pct = -1   # track which 10% milestones have been printed

    # Seed running totals from checkpoint
    for rec in checkpoint.values():
        if rec.get("_skipped"):
            continue
        rt_stmts += rec["sql_statement_count"]
        rt_ins_ok += rec["insert_parsed_ok"]
        rt_ins_fail += rec["insert_parse_failures"]
        rt_cre_ok += rec["create_parsed_ok"]
        rt_cre_fail += rec["create_parse_failures"]
        rt_upd_ok += rec["update_parsed_ok"]
        rt_upd_fail += rec["update_parse_failures"]
        rt_mrg_ok += rec["merge_parsed_ok"]
        rt_mrg_fail += rec["merge_parse_failures"]

    wall_start = time.time()
    processed_this_run = 0

    print(f"{'#':>5}  {'Script':<60}  {'Stmts':>5}  {'INS':>4}  {'CRE':>3}  {'UPD':>3}  {'MRG':>3}  {'Fail':>4}  {'Sec':>5}", flush=True)
    print("-" * 110, flush=True)

    for i, fpath in enumerate(all_paths, 1):
        script_name = os.path.basename(fpath)

        if script_name in checkpoint:
            # Already done — skip silently (already counted in running totals above)
            continue

        t0 = time.time()
        with open(fpath, "r", encoding="utf-8", errors="replace") as f:
            txt = f.read()

        result = process_script(
            script_name, txt,
            column_dictionary=column_dictionary,
            max_stmt_chars=args.max_stmt_chars,
            stmt_timeout=args.stmt_timeout,
        )
        elapsed = time.time() - t0

        append_checkpoint(checkpoint_path, result, script_name)
        checkpoint[script_name] = result if result is not None else {"script_name": script_name, "_skipped": True}
        processed_this_run += 1

        if result is None:
            print(f"{i:>5}  {script_name:<60}  {'(no SQL)':>5}  {'':>4}  {'':>3}  {'':>3}  {'':>3}  {'':>4}  {elapsed:>5.1f}", flush=True)
            logger.info(f"[{i}/{total}] {script_name}  (no SQL)  {elapsed:.1f}s")
        else:
            # Update running totals
            rt_stmts += result["sql_statement_count"]
            rt_ins_ok += result["insert_parsed_ok"]
            rt_ins_fail += result["insert_parse_failures"]
            rt_cre_ok += result["create_parsed_ok"]
            rt_cre_fail += result["create_parse_failures"]
            rt_upd_ok += result["update_parsed_ok"]
            rt_upd_fail += result["update_parse_failures"]
            rt_mrg_ok += result["merge_parsed_ok"]
            rt_mrg_fail += result["merge_parse_failures"]

            total_fail = (result["insert_parse_failures"] + result["create_parse_failures"]
                          + result["update_parse_failures"] + result["merge_parse_failures"])

            print(
                f"{i:>5}  {script_name:<60}  "
                f"{result['sql_statement_count']:>5}  "
                f"{result['insert_statement_count']:>4}  "
                f"{result['create_statement_count']:>3}  "
                f"{result['update_statement_count']:>3}  "
                f"{result['merge_statement_count']:>3}  "
                f"{total_fail:>4}  "
                f"{elapsed:>5.1f}",
                flush=True,
            )
            logger.info(
                f"[{i}/{total}] {script_name}  "
                f"stmts={result['sql_statement_count']}  "
                f"INS={result['insert_statement_count']}  "
                f"CRE={result['create_statement_count']}  "
                f"UPD={result['update_statement_count']}  "
                f"fail={total_fail}  elapsed={elapsed:.1f}s"
            )
            if total_fail:
                logger.warning(f"  {script_name}: {total_fail} parse failure(s)")

        # Check for 10% milestone
        all_names = {os.path.basename(p) for p in all_paths}
        done_count = sum(1 for name, r in checkpoint.items() if name in all_names and not r.get("_skipped"))
        current_pct = int((done_count / total) * 100) if total else 100
        milestone = (current_pct // _MILESTONE_PCT) * _MILESTONE_PCT
        if milestone > last_milestone_pct and milestone > 0:
            last_milestone_pct = milestone
            rt = dict(stmts=rt_stmts, ins_ok=rt_ins_ok, ins_fail=rt_ins_fail,
                      cre_ok=rt_cre_ok, cre_fail=rt_cre_fail,
                      upd_ok=rt_upd_ok, upd_fail=rt_upd_fail)
            _milestone_banner(done_count, total, rt)
            logger.info(f"Milestone {milestone}%: {done_count}/{total} files done, {rt_stmts} stmts")

    wall_elapsed = time.time() - wall_start

    # Final summary
    print("\n" + "=" * 110, flush=True)
    print(_progress_bar(total, total), flush=True)
    all_names = {os.path.basename(p) for p in all_paths}
    done_count = sum(1 for name, r in checkpoint.items() if name in all_names and not r.get("_skipped"))
    skip_count = sum(1 for name, r in checkpoint.items() if name in all_names and r.get("_skipped"))
    total_fail = rt_ins_fail + rt_cre_fail + rt_upd_fail + rt_mrg_fail
    summary_line = (
        f"DONE  {done_count} scripts | {rt_stmts} stmts | "
        f"INS {rt_ins_ok}ok/{rt_ins_fail}fail | "
        f"CRE {rt_cre_ok}ok/{rt_cre_fail}fail | "
        f"UPD {rt_upd_ok}ok/{rt_upd_fail}fail | "
        f"MRG {rt_mrg_ok}ok/{rt_mrg_fail}fail | "
        f"{skip_count} skipped (no SQL) | "
        f"wall time {wall_elapsed:.1f}s"
    )
    print(summary_line, flush=True)
    logger.info(summary_line)
    logger.info(f"Run completed at {_dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Write final JSON output
    result_doc = compile_output(checkpoint, input_descriptor, args, wall_elapsed)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(result_doc, f, indent=2)
    print(f"Output written to {args.out}", flush=True)
    print(f"Log written to   {log_path}", flush=True)
    logger.info(f"JSON output written to {args.out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
