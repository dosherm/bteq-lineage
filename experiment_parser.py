#!/usr/bin/env python3
"""
experiment_parser.py

Compares two approaches for parsing large INSERT INTO ... SELECT ... statements:
  A) sqlglot  (current approach, times out on large statements)
  B) Positional parser (new approach: match INSERT col list to SELECT expr list)

Usage:
  .venv/bin/python experiment_parser.py CLM_BTEQ_EDW_CLM_LOAD.sh
"""

import re
import sys
import time
import multiprocessing
from pathlib import Path


# ---------------------------------------------------------------------------
# Step 1: extract SQL from the .sh file (same logic as our main parser)
# ---------------------------------------------------------------------------

_BTEQ_RE = re.compile(r'bteq\s*<<\s*["\']?EOF["\']?\s*\n(.*?)\nEOF', re.DOTALL | re.IGNORECASE)
_COMMENT_RE = re.compile(r'/\*.*?\*/', re.DOTALL)
_LINE_COMMENT_RE = re.compile(r'--[^\n]*')
_SHELL_DB_PREFIX_RE = re.compile(r'\$\{?[A-Za-z_]\w*\}?\.')

def extract_sql_from_sh(path: str) -> list[str]:
    """Extract individual SQL statements from a BTEQ .sh file."""
    text = Path(path).read_text(encoding="utf-8", errors="replace")

    # Strip shell variable database prefixes: $DBNAME.TABLE -> TABLE
    text = _SHELL_DB_PREFIX_RE.sub("", text)

    sql_blocks = []
    for m in _BTEQ_RE.finditer(text):
        sql_blocks.append(m.group(1))

    if not sql_blocks:
        # fallback: everything after bteq <<EOF
        idx = text.lower().find("bteq <<")
        if idx >= 0:
            sql_blocks = [text[idx:]]

    statements = []
    for block in sql_blocks:
        block = _COMMENT_RE.sub(" ", block)
        block = _LINE_COMMENT_RE.sub(" ", block)
        for stmt in block.split(";"):
            stmt = stmt.strip()
            if stmt:
                statements.append(stmt)

    return statements


def find_large_inserts(statements: list[str], min_chars: int = 5000) -> list[tuple[int, str]]:
    """
    Return (index, statement) for INSERT statements above min_chars.
    BTEQ .IF ERRORCODE directives often get prepended to statements — strip them.
    """
    results = []
    for i, s in enumerate(statements):
        # Strip BTEQ control directives (.IF, .LABEL, .GOTO etc.) that prefix the real SQL
        clean = re.sub(r'^\s*\.[^\n]+\n', '', s, flags=re.MULTILINE).strip()
        upper = clean.lstrip().upper()
        if "INSERT" in upper and len(clean) >= min_chars:
            results.append((i, clean))
    return results


# ---------------------------------------------------------------------------
# Approach A: sqlglot
# ---------------------------------------------------------------------------

def _sqlglot_worker(sql: str, result_queue):
    try:
        import sqlglot
        import sqlglot.expressions as exp
        tree = sqlglot.parse_one(sql, dialect="teradata")
        mappings = []
        if tree:
            for col in tree.find_all(exp.Column):
                mappings.append(str(col))
        result_queue.put(("ok", mappings))
    except Exception as e:
        result_queue.put(("error", str(e)))


def run_sqlglot(sql: str, timeout: int = 15) -> dict:
    q = multiprocessing.Queue()
    p = multiprocessing.Process(target=_sqlglot_worker, args=(sql, q))
    t0 = time.time()
    p.start()
    p.join(timeout)
    elapsed = time.time() - t0

    if p.is_alive():
        p.kill()
        p.join()
        return {"status": "timeout", "elapsed": elapsed, "mappings": []}

    if not q.empty():
        status, data = q.get()
        return {"status": status, "elapsed": elapsed, "mappings": data if status == "ok" else [], "error": data if status == "error" else None}

    return {"status": "no_result", "elapsed": elapsed, "mappings": []}


# ---------------------------------------------------------------------------
# Approach B: positional parser
# ---------------------------------------------------------------------------

def _split_top_level(text: str, delimiter: str = ",") -> list[str]:
    """
    Split text by delimiter, but only at depth 0 (not inside parens).
    Handles nested parentheses correctly.
    """
    parts = []
    depth = 0
    current = []
    i = 0
    while i < len(text):
        ch = text[i]
        if ch == "(":
            depth += 1
            current.append(ch)
        elif ch == ")":
            depth -= 1
            current.append(ch)
        elif ch == delimiter and depth == 0:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(ch)
        i += 1
    if current:
        parts.append("".join(current).strip())
    return [p for p in parts if p]


def _extract_column_refs(expr: str) -> list[str]:
    """
    Extract source column references from an expression.
    Returns TABLE.COLUMN or just COLUMN identifiers.
    Skips literals, keywords, and shell variable placeholders.
    """
    # Remove string literals
    expr_clean = re.sub(r"'[^']*'", " ", expr)
    # Remove __VAR_xxx__ shell variable placeholders
    expr_clean = re.sub(r"__VAR_\w+__", " ", expr_clean)

    # Find TABLE.COLUMN or standalone COLUMN identifiers
    tokens = re.findall(r'\b([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)?)\b', expr_clean)

    # Filter out SQL keywords and function names
    KEYWORDS = {
        "CASE", "WHEN", "THEN", "ELSE", "END", "AND", "OR", "NOT", "IN",
        "IS", "NULL", "AS", "SELECT", "FROM", "WHERE", "JOIN", "LEFT",
        "RIGHT", "INNER", "OUTER", "ON", "COALESCE", "TRIM", "CAST",
        "SUBSTR", "SUBSTRING", "UPPER", "LOWER", "LENGTH", "CHAR",
        "VARCHAR", "DATE", "INTEGER", "DECIMAL", "FLOAT", "BETWEEN",
        "LIKE", "EXISTS", "DISTINCT", "GROUP", "BY", "HAVING", "ORDER",
        "QUALIFY", "OVER", "PARTITION", "ROW_NUMBER", "RANK", "SUM",
        "COUNT", "MAX", "MIN", "AVG", "EXTRACT", "YEAR", "MONTH", "DAY",
        "INTERVAL", "TIMESTAMP", "FORMAT", "NAMED", "TITLE", "COMPRESS",
        "INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE", "MERGE",
        "USING", "MATCHED", "TARGET", "SOURCE", "TRUE", "FALSE",
    }

    refs = []
    for tok in tokens:
        parts = tok.split(".")
        # Skip pure keywords
        if all(p.upper() in KEYWORDS for p in parts):
            continue
        # Skip pure numbers
        if tok.replace(".", "").isdigit():
            continue
        refs.append(tok.upper())

    # Deduplicate preserving order
    seen = set()
    result = []
    for r in refs:
        if r not in seen:
            seen.add(r)
            result.append(r)
    return result


def _parse_insert_select(sql: str, target_table: str) -> list[dict]:
    """
    Parse INSERT INTO target (col_list) SELECT expr_list FROM ...
    by matching column positions.

    Returns list of {target_column, source_refs, expression}
    """
    upper = sql.upper()

    # Find INSERT INTO ... ( col_list )
    ins_m = re.search(r'INSERT\s+INTO\s+(\S+)\s*\(', sql, re.IGNORECASE)
    if not ins_m:
        return []

    # Find the col_list (everything inside the outermost parens after INSERT INTO tbl)
    start = ins_m.end() - 1  # position of opening (
    depth = 0
    i = start
    while i < len(sql):
        if sql[i] == "(":
            depth += 1
        elif sql[i] == ")":
            depth -= 1
            if depth == 0:
                break
        i += 1
    col_list_raw = sql[start + 1:i]

    # Find SELECT ... FROM (the SELECT immediately after the closing paren of col_list)
    after_cols = sql[i + 1:]
    sel_m = re.search(r'\bSELECT\b', after_cols, re.IGNORECASE)
    if not sel_m:
        return []

    sel_start = sel_m.end()
    # Find FROM at depth 0
    sel_body = after_cols[sel_start:]

    # Walk to find FROM at depth 0
    depth = 0
    from_pos = None
    j = 0
    while j < len(sel_body):
        if sel_body[j] == "(":
            depth += 1
        elif sel_body[j] == ")":
            depth -= 1
        elif depth == 0:
            chunk = sel_body[j:j+4].upper()
            if chunk == "FROM":
                # Make sure it's a word boundary
                before_ok = j == 0 or not sel_body[j-1].isalnum()
                after_ok = j + 4 >= len(sel_body) or not sel_body[j+4].isalnum()
                if before_ok and after_ok:
                    from_pos = j
                    break
        j += 1

    select_exprs_raw = sel_body[:from_pos] if from_pos else sel_body

    # Extract FROM tables (simple: find identifiers after FROM/JOIN)
    from_raw = sel_body[from_pos:] if from_pos else ""
    source_tables = re.findall(
        r'(?:FROM|JOIN)\s+([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)?)',
        from_raw, re.IGNORECASE
    )

    # Split both lists by top-level comma
    col_names = _split_top_level(col_list_raw)
    select_exprs = _split_top_level(select_exprs_raw)

    # Strip comments from column names
    col_names = [re.sub(r'/\*.*?\*/', '', c, flags=re.DOTALL).strip() for c in col_names]
    col_names = [c for c in col_names if c]

    if len(col_names) != len(select_exprs):
        # Return what we have with a mismatch note
        print(f"  [WARN] col_list={len(col_names)} vs select_exprs={len(select_exprs)} — mismatch")

    mappings = []
    for idx, col in enumerate(col_names):
        expr = select_exprs[idx] if idx < len(select_exprs) else ""
        # Strip alias (AS name at end)
        expr_clean = re.sub(r'\bAS\s+\w+\s*$', '', expr, flags=re.IGNORECASE).strip()
        refs = _extract_column_refs(expr_clean)
        mappings.append({
            "target_column": f"{target_table}.{col.strip()}",
            "expression": expr.strip()[:120],  # truncate for display
            "source_refs": refs,
        })

    return mappings


def run_positional(sql: str, target_table: str) -> dict:
    t0 = time.time()
    try:
        mappings = _parse_insert_select(sql, target_table)
        elapsed = time.time() - t0
        return {"status": "ok", "elapsed": elapsed, "mappings": mappings}
    except Exception as e:
        elapsed = time.time() - t0
        return {"status": "error", "elapsed": elapsed, "mappings": [], "error": str(e)}


# ---------------------------------------------------------------------------
# Main experiment
# ---------------------------------------------------------------------------

def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "CLM_BTEQ_EDW_CLM_LOAD.sh"

    print(f"\n{'='*70}")
    print(f"  EXPERIMENT: sqlglot vs positional parser")
    print(f"  File: {path}")
    print(f"{'='*70}\n")

    print("Extracting SQL statements...")
    statements = extract_sql_from_sh(path)
    print(f"  Found {len(statements)} statements total\n")

    large = find_large_inserts(statements, min_chars=5000)
    print(f"  Found {len(large)} large INSERT statements (>=5000 chars)")
    for i, (idx, s) in enumerate(large):
        print(f"    [{i+1}] stmt #{idx}  {len(s):,} chars  ->  {s[:60].strip()!r}...")
    print()

    if not large:
        print("No large INSERTs found.")
        return

    # Experiment on first large INSERT
    idx, sql = large[0]
    # Detect target table
    tbl_m = re.search(r'INSERT\s+INTO\s+(\S+)', sql, re.IGNORECASE)
    target_table = tbl_m.group(1) if tbl_m else "UNKNOWN"

    print(f"{'─'*70}")
    print(f"  Experimenting on stmt #{idx}: INSERT INTO {target_table}")
    print(f"  Statement size: {len(sql):,} characters")
    print(f"{'─'*70}\n")

    # --- Approach A: sqlglot ---
    print("▶  Approach A: sqlglot (15s timeout)")
    print("   Running...", flush=True)
    result_a = run_sqlglot(sql, timeout=15)
    if result_a["status"] == "timeout":
        print(f"   ✗ TIMED OUT after {result_a['elapsed']:.1f}s")
        print(f"   → sqlglot cannot handle this statement size")
    elif result_a["status"] == "ok":
        print(f"   ✓ Completed in {result_a['elapsed']:.2f}s")
        print(f"   → Found {len(result_a['mappings'])} column references")
        for ref in result_a["mappings"][:10]:
            print(f"       {ref}")
        if len(result_a["mappings"]) > 10:
            print(f"       ... and {len(result_a['mappings'])-10} more")
    else:
        print(f"   ✗ Error in {result_a['elapsed']:.2f}s: {result_a.get('error','')}")
    print()

    # --- Approach B: positional ---
    print("▶  Approach B: positional parser (no timeout needed)")
    result_b = run_positional(sql, target_table)
    if result_b["status"] == "ok":
        m = result_b["mappings"]
        print(f"   ✓ Completed in {result_b['elapsed']:.4f}s")
        print(f"   → Mapped {len(m)} target columns\n")
        print(f"   {'TARGET COLUMN':<50}  {'EXPRESSION (truncated)':<60}  SOURCE REFS")
        print(f"   {'─'*50}  {'─'*60}  {'─'*30}")
        for row in m[:30]:
            refs = ", ".join(row["source_refs"][:3])
            print(f"   {row['target_column']:<50}  {row['expression']:<60}  {refs}")
        if len(m) > 30:
            print(f"   ... and {len(m)-30} more rows")
    else:
        print(f"   ✗ Error in {result_b['elapsed']:.2f}s: {result_b.get('error','')}")
    print()

    # --- Comparison ---
    print(f"{'─'*70}")
    print("  COMPARISON SUMMARY")
    print(f"{'─'*70}")
    print(f"  sqlglot    : {result_a['status']:10s}  {result_a['elapsed']:.2f}s")
    print(f"  positional : {result_b['status']:10s}  {result_b['elapsed']:.4f}s")
    if result_b["status"] == "ok":
        print(f"  Columns mapped by positional : {len(result_b['mappings'])}")
        cols_with_refs = [r for r in result_b["mappings"] if r["source_refs"]]
        print(f"  Columns with source refs     : {len(cols_with_refs)}")
        cols_without = [r for r in result_b["mappings"] if not r["source_refs"]]
        print(f"  Columns with no refs (consts): {len(cols_without)}")
    print()

    # Run all large inserts with positional to show coverage
    if len(large) > 1:
        print(f"{'─'*70}")
        print(f"  POSITIONAL PARSER ON ALL {len(large)} LARGE INSERTs")
        print(f"{'─'*70}")
        total_cols = 0
        for i, (idx2, sql2) in enumerate(large):
            tbl_m2 = re.search(r'INSERT\s+INTO\s+(\S+)', sql2, re.IGNORECASE)
            tbl2 = tbl_m2.group(1) if tbl_m2 else "UNKNOWN"
            r = run_positional(sql2, tbl2)
            cols = len(r["mappings"])
            total_cols += cols
            print(f"  [{i+1}] stmt #{idx2}  INSERT INTO {tbl2:<40}  {len(sql2):>7,} chars  "
                  f"→ {cols} cols  {r['elapsed']:.4f}s  {r['status']}")
        print(f"\n  Total columns mapped: {total_cols}")
    print()


if __name__ == "__main__":
    main()
