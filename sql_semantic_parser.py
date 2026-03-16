#!/usr/bin/env python3
"""
sql_semantic_parser.py

Purpose
- Parse Teradata SQL INSERT ... SELECT statements and derive column-level lineage:
  target_table.target_column -> list of base_table.base_column sources

Key behaviors
- Uses sqlglot to parse and walk the AST (NOT regex lineage).
- Tracks FROM/JOIN base tables and aliases using a simple scope model.
- Expands simple SELECT aliases.
- Collects base sources by scanning expressions for Column nodes.

Update in this version
- Adds optional `column_dictionary` (COLUMN -> {SCHEMA.TABLE,...}) to bind unqualified
  columns (e.g., `CLAIM_ID_NBR`) to the correct base table when multiple base tables exist.
- Resolves alias-qualified columns (e.g., `RD23_PROF_LINE.CLAIM_ID_NBR`) to the physical base
  table using the existing alias map.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

import sqlglot
from sqlglot import exp


# -----------------------------
# BTEQ preprocessing
# -----------------------------

_BTEQ_DOT_CMD_RE = re.compile(r"(?im)^\s*\.(SET|LOGON|LOGOFF|QUIT|IF|GOTO|LABEL|RUN)\b.*?$")
_BTEQ_COMMENT_RE = re.compile(r"(?m)^\s*\*.*?$")
_BTEQ_BLOCK_COMMENT_RE = re.compile(r"(?s)/\*.*?\*/")


def preprocess_bteq_sql(text: str) -> str:
    """
    Remove common BTEQ directives and comments while leaving SQL text.
    """
    s = text
    s = _BTEQ_BLOCK_COMMENT_RE.sub(" ", s)
    s = _BTEQ_DOT_CMD_RE.sub(" ", s)
    s = _BTEQ_COMMENT_RE.sub(" ", s)
    # Normalize whitespace a bit
    s = re.sub(r"[ \t]+", " ", s)
    return s.strip()


# -----------------------------
# Scope model
# -----------------------------

@dataclass
class Scope:
    # base tables visible in this scope (physical names)
    base_tables: Set[str]
    # alias -> either physical base table string OR a derived Scope
    alias_to_source: Dict[str, Any]
    # alias -> expression for derived scopes (subqueries / CTEs) if available
    alias_to_expr: Dict[str, exp.Expression]

    def resolve_alias(self, alias: str) -> Optional[str]:
        src = self.alias_to_source.get(alias)
        if isinstance(src, str):
            return src
        if isinstance(src, Scope):
            # If a derived scope maps to a single base table, return it; else unknown.
            if len(src.base_tables) == 1:
                return next(iter(src.base_tables))
        return None


def _table_name_from_table_exp(t: exp.Table) -> str:
    # Build a qualified name from parts to avoid including the alias in t.sql().
    parts = [p for p in [t.catalog, t.db, t.name] if p]
    return ".".join(parts).upper() if parts else t.name.upper()


def _extract_from_tables(from_exp: exp.Expression) -> List[Tuple[str, Optional[str]]]:
    """
    Extract (table_name, alias) from FROM/JOIN nodes.
    Handles Table and Subquery sources.
    """
    out: List[Tuple[str, Optional[str]]] = []

    # FROM is usually an exp.From containing one or more expressions
    for source in from_exp.find_all(exp.Table):
        # exp.Table might be inside joins too; that's fine
        tbl_name = _table_name_from_table_exp(source)
        alias = None
        if source.alias:
            alias = source.alias
        out.append((tbl_name, alias))

    return out


def build_scope_from_query(query: exp.Expression) -> Scope:
    """
    Build a minimal scope from a SELECT / UNION / subquery by collecting:
    - physical base tables
    - alias -> physical mapping
    - alias -> derived scope mapping for subqueries
    """
    base_tables: Set[str] = set()
    alias_to_source: Dict[str, Any] = {}
    alias_to_expr: Dict[str, exp.Expression] = {}

    # Collect physical tables
    for t in query.find_all(exp.Table):
        tbl_name = _table_name_from_table_exp(t)
        base_tables.add(tbl_name)
        if t.alias:
            alias_to_source[t.alias.upper()] = tbl_name

    # Collect subqueries in FROM/JOIN with alias
    for sub in query.find_all(exp.Subquery):
        if sub.alias:
            derived = build_scope_from_query(sub.this)
            alias_to_source[sub.alias.upper()] = derived
            alias_to_expr[sub.alias.upper()] = sub.this

    # Collect CTEs
    with_exp = query.args.get("with")
    if isinstance(with_exp, exp.With):
        for cte in with_exp.expressions or []:
            if isinstance(cte, exp.CTE):
                name = cte.alias_or_name
                if name:
                    derived = build_scope_from_query(cte.this)
                    alias_to_source[name.upper()] = derived
                    alias_to_expr[name.upper()] = cte.this

    return Scope(base_tables=base_tables, alias_to_source=alias_to_source, alias_to_expr=alias_to_expr)


def flatten_aliases_filtered(scope: Scope) -> Dict[str, str]:
    """
    Flatten alias->physical for aliases resolvable to a single physical table.
    (Drops ambiguous derived scopes.)
    """
    out: Dict[str, str] = {}

    def walk(sc: Scope):
        for a, src in sc.alias_to_source.items():
            if isinstance(src, str):
                out[a.upper()] = src
            elif isinstance(src, Scope):
                if len(src.base_tables) == 1:
                    out[a.upper()] = next(iter(src.base_tables))
                walk(src)

    walk(scope)
    return out


def _norm_ident(s: str) -> str:
    """Normalize identifiers for dictionary matching (case-insensitive, strip quotes)."""
    if s is None:
        return ""
    s = str(s).strip()
    if (s.startswith('"') and s.endswith('"')) or (s.startswith('`') and s.endswith('`')):
        s = s[1:-1]
    return s.upper()


def _table_only(table_name: str) -> str:
    """Return only the table segment of a potentially qualified name."""
    t = (table_name or "").strip()
    if not t:
        return ""
    parts = [p for p in re.split(r"[.]", t) if p]
    return _norm_ident(parts[-1]) if parts else _norm_ident(t)


def bind_unqualified_columns(
    expression: exp.Expression,
    scope: Scope,
    column_dictionary: Optional[Dict[str, Set[str]]] = None,
) -> exp.Expression:
    """Qualify unqualified Column nodes in-place when possible.

    Strategy (in order):
      1) If exactly 1 base table exists in scope, bind to it.
      2) Else, use column_dictionary (COLUMN -> {SCHEMA.TABLE,...}) to pick a single base table.
         Matching is done by table-only name (schema differences ignored).

    If binding is ambiguous, leave unqualified.
    """

    base_tables = sorted(scope.base_tables)
    if not base_tables:
        return expression

    table_only_to_scope_tables: Dict[str, List[str]] = {}
    for bt in base_tables:
        table_only_to_scope_tables.setdefault(_table_only(bt), []).append(bt)

    def choose_base_table(col_name: str) -> Optional[str]:
        if len(base_tables) == 1:
            return base_tables[0]
        if not column_dictionary:
            return None

        col_key = _norm_ident(col_name)
        candidates = column_dictionary.get(col_key)
        if not candidates:
            return None

        matched_scope_tables: Set[str] = set()
        for cand in candidates:
            cand_table = _table_only(cand)
            for scope_bt in table_only_to_scope_tables.get(cand_table, []):
                matched_scope_tables.add(scope_bt)

        if len(matched_scope_tables) == 1:
            return next(iter(matched_scope_tables))
        return None

    # alias_to_source can include derived scopes; treat any alias that resolves to exactly 1 base table.
    alias_to_base: Dict[str, str] = {}
    for a, src in scope.alias_to_source.items():
        if isinstance(src, str):
            alias_to_base[a.upper()] = src
        else:
            if len(src.base_tables) == 1:
                alias_to_base[a.upper()] = next(iter(src.base_tables))

    for col in expression.find_all(exp.Column):
        if col.table:
            continue
        chosen = choose_base_table(col.name)
        if not chosen:
            continue

        chosen_norm = _norm_ident(chosen)
        preferred_ref = None
        for a, bt in alias_to_base.items():
            if _norm_ident(bt) == chosen_norm:
                preferred_ref = a
                break
        col.set("table", preferred_ref or chosen)

    return expression


# -----------------------------
# Expansion / expression handling
# -----------------------------

def projection_alias(proj: exp.Expression) -> Optional[str]:
    if isinstance(proj, exp.Alias):
        return proj.alias
    return None


def projection_expression(proj: exp.Expression) -> exp.Expression:
    if isinstance(proj, exp.Alias):
        return proj.this
    return proj


def build_projection_map(select: exp.Select) -> Dict[str, exp.Expression]:
    """
    alias -> expression (raw)
    """
    m: Dict[str, exp.Expression] = {}
    for p in (select.expressions or []):
        a = projection_alias(p)
        if a:
            m[a.upper()] = projection_expression(p)
    return m


def expand_expression(expr: exp.Expression, scope: Scope, max_depth: int = 4) -> exp.Expression:
    """
    Replace references to SELECT aliases with their underlying expressions (simple cases).
    Only expands for columns like `ALIAS_COL` where that name exists as a projection alias.
    """
    if max_depth <= 0:
        return expr

    # Find nearest SELECT ancestor scope expression if available
    # (We only support expanding within the top-level SELECT of the INSERT query.)
    # Caller passes the correct root scope; we derive projection map from first Select.
    top_select = next(scope.alias_to_expr.get("__TOP_SELECT__", None) for _ in [0] if "__TOP_SELECT__" in scope.alias_to_expr)
    proj_map: Dict[str, exp.Expression] = {}
    if isinstance(top_select, exp.Select):
        proj_map = build_projection_map(top_select)

    def _repl(node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.Column) and not node.table:
            # bare column could actually be a projection alias
            key = node.name.upper()
            if key in proj_map:
                return proj_map[key].copy()
        return node

    out = expr.transform(_repl)
    # recurse slightly to expand nested aliases
    if out is not expr:
        return expand_expression(out, scope, max_depth=max_depth - 1)
    return out


# -----------------------------
# Source collection + classification
# -----------------------------

def column_full_table_name(col: exp.Column) -> Optional[str]:
    if col.table:
        return col.table
    return None


def collect_base_sources(
    expr: exp.Expression,
    scope: Scope,
    alias_map: Optional[Dict[str, str]] = None,
) -> List[str]:
    """Collect physical column sources referenced in an expression.

    `sqlglot` will emit columns as either:
      - TABLE.COLUMN (physical table name), or
      - ALIAS.COLUMN (join alias)

    We normalize to the physical base table using `alias_map`.
    """
    sources: Set[str] = set()
    alias_map = {k.upper(): v for k, v in (alias_map or {}).items()}
    physical_tables = {t.upper() for t in (scope.base_tables or set())}

    for col in expr.find_all(exp.Column):
        tbl = column_full_table_name(col)
        if not tbl:
            continue
        tbl_u = tbl.upper()
        if tbl_u in alias_map:
            tbl_u = alias_map[tbl_u].upper()
        if tbl_u in physical_tables:
            sources.add(f"{tbl_u}.{col.name.upper()}")
    return sorted(sources)


def classify(expr: exp.Expression) -> str:
    """
    Basic classification for target column mapping.
    """
    if isinstance(expr, exp.Column):
        return "Direct assignment"
    # Functions, arithmetic, etc -> derived
    if any(True for _ in expr.find_all(exp.Func)):
        return "Derived (function)"
    if isinstance(expr, (exp.Add, exp.Sub, exp.Mul, exp.Div)):
        return "Derived (arithmetic)"
    if isinstance(expr, exp.Case):
        return "Derived (case)"
    if isinstance(expr, exp.Literal):
        return "Constant"
    return "Derived"


# -----------------------------
# Resolution note helper
# -----------------------------

def _build_resolution_note(
    expr: exp.Expression,
    base_tables: Set[str],
    column_dictionary: Optional[Dict[str, Set[str]]],
) -> str:
    """Explain why base_sources could not be resolved for an expression."""
    unqualified = [col for col in expr.find_all(exp.Column) if not col.table]
    qualified_no_match = [col for col in expr.find_all(exp.Column) if col.table]

    reasons = []
    seen = set()
    for col in unqualified:
        col_key = _norm_ident(col.name)
        msg: str
        if not column_dictionary:
            msg = f"Column '{col.name}' is unqualified and no external dictionary was provided"
        elif col_key not in column_dictionary:
            msg = f"Column '{col.name}' not found in external dictionary lookup"
        else:
            # In dict but didn't match any in-scope table
            cand_tables = {_table_only(k) for k in column_dictionary[col_key]}
            scope_tables = {_table_only(t) for t in base_tables}
            overlap = cand_tables & scope_tables
            if not overlap:
                msg = (
                    f"Column '{col.name}' exists in external dictionary "
                    f"but none of its tables ({', '.join(sorted(cand_tables))}) "
                    f"match the in-scope tables ({', '.join(sorted(scope_tables))})"
                )
            else:
                msg = (
                    f"Column '{col.name}' is ambiguous across multiple source tables: "
                    + ", ".join(sorted(overlap))
                )
        if msg not in seen:
            reasons.append(msg)
            seen.add(msg)

    if not reasons and qualified_no_match:
        reasons.append("Qualified source column references a table not present in scope")

    return "; ".join(reasons) if reasons else "Unable to resolve source expression"


# -----------------------------
# INSERT parsing
# -----------------------------

def _strip_insert_table_parens(target_table_sql: str) -> str:
    """
    If sqlglot renders the target as 'TABLE (COL1, COL2, ...)', strip the paren portion.
    Keep only the identifier segment.
    """
    s = (target_table_sql or "").strip().upper()
    s = re.sub(r"\s*\(.*$", "", s).strip()
    return s


def _extract_insert_target_columns_from_sql(sql: str) -> List[str]:
    """
    Safe fallback when sqlglot doesn't populate parsed.args['columns'] for Teradata INSERT.
    Extract columns from: INSERT INTO <table> ( col1, col2, ... ) SELECT ...
    This only inspects the INSERT header, not the SELECT.
    """
    if not sql:
        return []

    s = sql.strip()
    m = re.search(r"(?is)\bINSERT\s+INTO\s+([^\s(]+)\s*\(", s)
    if not m:
        return []

    start = m.end() - 1  # at '('
    depth = 0
    end = None
    for i in range(start, len(s)):
        ch = s[i]
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                end = i
                break
    if end is None:
        return []

    inside = s[start + 1:end]
    cols = []
    for part in inside.split(","):
        c = part.strip()
        if not c:
            continue
        c = c.strip().strip('"').strip("`")
        cols.append(c.upper())
    return cols


def parse_insert_semantics(
    sql: str,
    dialect: str = "teradata",
    column_dictionary: Optional[Dict[str, Set[str]]] = None,
) -> Dict:
    parsed = sqlglot.parse_one(sql, read=dialect)
    if not isinstance(parsed, exp.Insert):
        raise ValueError("Statement is not an INSERT")

    # Target
    tgt_table_expr = parsed.this
    target_table = _strip_insert_table_parens(tgt_table_expr.sql(dialect=dialect))

    target_cols_exp = parsed.args.get("columns") or []
    target_columns = [c.name.upper() for c in target_cols_exp]
    if not target_columns:
        target_columns = _extract_insert_target_columns_from_sql(sql)

    # Source query
    query = parsed.expression
    if query is None:
        # Teradata allows INSERT INTO table(val1, val2, ...) without the VALUES keyword.
        # sqlglot stores the literal values inside parsed.this.expressions when this happens.
        inline_values = (
            parsed.this.expressions
            if isinstance(parsed.this, exp.Schema)
            else []
        )
        if inline_values:
            # Resolve target table from the inner Table node of the Schema
            inner_tbl = parsed.this.this
            tgt_table_name = (
                ".".join(filter(None, [inner_tbl.catalog, inner_tbl.db, inner_tbl.name]))
                if inner_tbl
                else target_table.split("(")[0].strip()
            )
            col_semantics = []
            for i, val_expr in enumerate(inline_values):
                # target_columns here would contain the literal values extracted by the
                # regex fallback, not real column names — use positional names instead.
                col_name = f"COL_{i+1}"
                col_semantics.append({
                    "target_column": f"{tgt_table_name}.{col_name}",
                    "resolved_expression": val_expr.sql(dialect=dialect),
                    "base_sources": [],
                    "classification": "Constant",
                })
            return {
                "statement_type": "INSERT",
                "target": {
                    "table": tgt_table_name,
                    "columns": [e["target_column"].split(".", 1)[-1] for e in col_semantics],
                },
                "source_scope": {"base_tables": [], "aliases": {}},
                "column_semantics": col_semantics,
            }
        raise ValueError("INSERT missing source query")

    # Build scope for query
    root_scope = build_scope_from_query(query)
    # attach top select for alias expansion
    top_select = query.find(exp.Select)
    if top_select:
        root_scope.alias_to_expr["__TOP_SELECT__"] = top_select

    # Normalize alias keys to uppercase for matching against sqlglot's emitted identifiers.
    alias_map = {k.upper(): v for k, v in flatten_aliases_filtered(root_scope).items()}

    # Get top select projections (INSERT ... SELECT)
    if not top_select:
        raise ValueError("INSERT source is not a SELECT")

    projections = list(top_select.expressions or [])
    if target_columns and len(target_columns) != len(projections):
        # Some Teradata INSERTs omit column list; we require it for deterministic lineage
        # but allow mismatch to proceed with min length.
        pass

    n = min(len(target_columns), len(projections)) if target_columns else len(projections)
    if not target_columns:
        # Derive from projection aliases/column names; fall back to positional names
        for proj in projections:
            alias = projection_alias(proj)
            if alias:
                target_columns.append(alias.upper())
            else:
                expr = projection_expression(proj)
                if isinstance(expr, exp.Column):
                    target_columns.append(expr.name.upper())
                else:
                    target_columns.append(f"COL_{len(target_columns)+1}")

    column_semantics = []
    for i in range(n):
        proj = projections[i]
        raw_expr = projection_expression(proj)
        expanded = expand_expression(raw_expr, root_scope)
        bind_unqualified_columns(expanded, root_scope, column_dictionary)
        resolved_sql = expanded.sql(dialect=dialect).strip()
        base_sources = collect_base_sources(expanded, root_scope, alias_map)
        entry: Dict = {
            "target_column": f"{target_table}.{target_columns[i]}",
            "resolved_expression": resolved_sql,
            "base_sources": base_sources,
            "classification": classify(expanded),
        }
        if not base_sources and entry["classification"] != "Constant":
            entry["resolution_note"] = _build_resolution_note(
                expanded, root_scope.base_tables, column_dictionary
            )
        column_semantics.append(entry)

    return {
        "statement_type": "INSERT",
        "target": {
            "table": target_table,
            "columns": target_columns[:n],
        },
        "source_scope": {
            "base_tables": sorted(root_scope.base_tables),
            "aliases": alias_map,
        },
        "column_semantics": column_semantics,
    }


# -----------------------------
# CREATE parsing (DDL and CTAS)
# -----------------------------

def parse_create_semantics(
    sql: str,
    dialect: str = "teradata",
    column_dictionary: Optional[Dict[str, Set[str]]] = None,
) -> Dict:
    parsed = sqlglot.parse_one(sql, read=dialect)
    if not isinstance(parsed, exp.Create):
        raise ValueError("Statement is not a CREATE")

    # Resolve target table name
    tgt = parsed.this  # exp.Schema for DDL, exp.Table for CTAS
    if isinstance(tgt, exp.Schema):
        tbl_node = tgt.this
        target_table = _table_name_from_table_exp(tbl_node) if isinstance(tbl_node, exp.Table) else tbl_node.name.upper()
    elif isinstance(tgt, exp.Table):
        target_table = _table_name_from_table_exp(tgt)
    else:
        target_table = tgt.sql(dialect=dialect).upper()

    # Source query (present for CTAS, absent for DDL)
    query = parsed.expression

    # --- DDL-only (no AS SELECT) ---
    if query is None:
        col_defs = []
        schema_exprs = tgt.expressions if isinstance(tgt, exp.Schema) else []
        for col_def in schema_exprs:
            if isinstance(col_def, exp.ColumnDef):
                col_name = col_def.name.upper()
                kind = col_def.args.get("kind")
                col_type = kind.sql(dialect=dialect).upper() if kind else "UNKNOWN"
                col_defs.append({"column": col_name, "data_type": col_type})
        return {
            "statement_type": "CREATE",
            "create_kind": "DDL",
            "target": {
                "table": target_table,
                "column_definitions": col_defs,
            },
            "column_semantics": [],
        }

    # --- CTAS (CREATE TABLE ... AS SELECT) ---
    root_scope = build_scope_from_query(query)
    top_select = query.find(exp.Select)
    if top_select:
        root_scope.alias_to_expr["__TOP_SELECT__"] = top_select

    alias_map = {k.upper(): v for k, v in flatten_aliases_filtered(root_scope).items()}

    if not top_select:
        raise ValueError("CREATE TABLE AS source is not a SELECT")

    projections = list(top_select.expressions or [])

    # CTAS has no explicit column list; derive from projections
    target_columns: List[str] = []
    for proj in projections:
        alias = projection_alias(proj)
        if alias:
            target_columns.append(alias.upper())
        else:
            expr_node = projection_expression(proj)
            if isinstance(expr_node, exp.Column):
                target_columns.append(expr_node.name.upper())
            else:
                target_columns.append(f"COL_{len(target_columns) + 1}")

    column_semantics = []
    for i, proj in enumerate(projections):
        raw_expr = projection_expression(proj)
        expanded = expand_expression(raw_expr, root_scope)
        bind_unqualified_columns(expanded, root_scope, column_dictionary)
        resolved_sql = expanded.sql(dialect=dialect).strip()
        base_sources = collect_base_sources(expanded, root_scope, alias_map)
        entry: Dict = {
            "target_column": f"{target_table}.{target_columns[i]}",
            "resolved_expression": resolved_sql,
            "base_sources": base_sources,
            "classification": classify(expanded),
        }
        if not base_sources and entry["classification"] != "Constant":
            entry["resolution_note"] = _build_resolution_note(
                expanded, root_scope.base_tables, column_dictionary
            )
        column_semantics.append(entry)

    return {
        "statement_type": "CREATE",
        "create_kind": "CTAS",
        "target": {
            "table": target_table,
            "columns": target_columns,
        },
        "source_scope": {
            "base_tables": sorted(root_scope.base_tables),
            "aliases": alias_map,
        },
        "column_semantics": column_semantics,
    }


# -----------------------------
# UPDATE parsing
# -----------------------------

def _build_scope_from_update(parsed: exp.Update) -> Tuple[str, Scope, Dict[str, str]]:
    """Extract target table, scope, and alias map from a parsed UPDATE."""
    tgt = parsed.this
    target_table = _table_name_from_table_exp(tgt) if isinstance(tgt, exp.Table) else tgt.name.upper()

    source_tables: Set[str] = set()
    alias_to_source: Dict[str, Any] = {}
    from_exp = parsed.args.get("from_")
    if from_exp:
        for t in from_exp.find_all(exp.Table):
            tbl_name = _table_name_from_table_exp(t)
            source_tables.add(tbl_name)
            if t.alias:
                alias_to_source[t.alias.upper()] = tbl_name

    all_tables = source_tables | {target_table}
    scope = Scope(base_tables=all_tables, alias_to_source=alias_to_source, alias_to_expr={})
    alias_map = {k.upper(): v for k, v in flatten_aliases_filtered(scope).items()}
    return target_table, scope, alias_map


def parse_update_semantics(
    sql: str,
    dialect: str = "teradata",
    column_dictionary: Optional[Dict[str, Set[str]]] = None,
) -> Dict:
    parsed = sqlglot.parse_one(sql, read=dialect)
    if not isinstance(parsed, exp.Update):
        raise ValueError("Statement is not an UPDATE")

    target_table, scope, alias_map = _build_scope_from_update(parsed)
    source_tables = scope.base_tables - {target_table}

    column_semantics = []
    for assignment in (parsed.expressions or []):
        if not isinstance(assignment, exp.EQ):
            continue
        target_col_expr = assignment.left
        source_expr = assignment.right

        target_col = (
            target_col_expr.name.upper()
            if isinstance(target_col_expr, exp.Column)
            else target_col_expr.sql(dialect=dialect).upper()
        )

        bind_unqualified_columns(source_expr, scope, column_dictionary)
        resolved_sql = source_expr.sql(dialect=dialect).strip()
        base_sources = collect_base_sources(source_expr, scope, alias_map)
        entry: Dict = {
            "target_column": f"{target_table}.{target_col}",
            "resolved_expression": resolved_sql,
            "base_sources": base_sources,
            "classification": classify(source_expr),
        }
        if not base_sources and entry["classification"] != "Constant":
            entry["resolution_note"] = _build_resolution_note(
                source_expr, scope.base_tables, column_dictionary
            )
        column_semantics.append(entry)

    return {
        "statement_type": "UPDATE",
        "target": {
            "table": target_table,
        },
        "source_scope": {
            "base_tables": sorted(source_tables),
            "aliases": alias_map,
        },
        "column_semantics": column_semantics,
    }


# -----------------------------
# MERGE parsing
# -----------------------------

def _parse_merge_when_clause(
    when: exp.When,
    target_table: str,
    scope: Scope,
    alias_map: Dict[str, str],
    column_dictionary: Optional[Dict[str, Set[str]]],
    dialect: str,
) -> List[Dict]:
    """Extract column semantics from a single WHEN clause."""
    entries: List[Dict] = []
    matched = when.args.get("matched")
    clause_type = "WHEN MATCHED UPDATE" if matched else "WHEN NOT MATCHED INSERT"
    then = when.args.get("then")

    if isinstance(then, exp.Update):
        for assignment in (then.expressions or []):
            if not isinstance(assignment, exp.EQ):
                continue
            lhs, rhs = assignment.left, assignment.right
            target_col = lhs.name.upper() if isinstance(lhs, exp.Column) else lhs.sql(dialect=dialect).upper()
            bind_unqualified_columns(rhs, scope, column_dictionary)
            resolved_sql = rhs.sql(dialect=dialect).strip()
            base_sources = collect_base_sources(rhs, scope, alias_map)
            entry: Dict = {
                "target_column": f"{target_table}.{target_col}",
                "resolved_expression": resolved_sql,
                "base_sources": base_sources,
                "classification": classify(rhs),
                "merge_clause": clause_type,
            }
            if not base_sources and entry["classification"] != "Constant":
                entry["resolution_note"] = _build_resolution_note(rhs, scope.base_tables, column_dictionary)
            entries.append(entry)

    elif isinstance(then, exp.Insert):
        # Column list is in then.this (Tuple of Column nodes)
        col_list_node = then.this
        insert_cols: List[str] = []
        if isinstance(col_list_node, exp.Tuple):
            insert_cols = [c.name.upper() for c in col_list_node.expressions if isinstance(c, exp.Column)]
        elif isinstance(col_list_node, exp.Schema):
            insert_cols = [c.name.upper() for c in col_list_node.expressions if isinstance(c, (exp.Column, exp.Identifier))]

        # Values are in then.expression (Tuple of value expressions)
        val_node = then.expression
        values: List[exp.Expression] = []
        if isinstance(val_node, exp.Tuple):
            values = val_node.expressions
        elif isinstance(val_node, exp.Values):
            first = val_node.expressions[0] if val_node.expressions else None
            if isinstance(first, exp.Tuple):
                values = first.expressions

        for i, val in enumerate(values):
            target_col = insert_cols[i] if i < len(insert_cols) else f"COL_{i + 1}"
            bind_unqualified_columns(val, scope, column_dictionary)
            resolved_sql = val.sql(dialect=dialect).strip()
            base_sources = collect_base_sources(val, scope, alias_map)
            entry = {
                "target_column": f"{target_table}.{target_col}",
                "resolved_expression": resolved_sql,
                "base_sources": base_sources,
                "classification": classify(val),
                "merge_clause": clause_type,
            }
            if not base_sources and entry["classification"] != "Constant":
                entry["resolution_note"] = _build_resolution_note(val, scope.base_tables, column_dictionary)
            entries.append(entry)

    return entries


def parse_merge_semantics(
    sql: str,
    dialect: str = "teradata",
    column_dictionary: Optional[Dict[str, Set[str]]] = None,
) -> Dict:
    parsed = sqlglot.parse_one(sql, read=dialect)
    if not isinstance(parsed, exp.Merge):
        raise ValueError("Statement is not a MERGE")

    # Target table
    tgt = parsed.this
    target_table = _table_name_from_table_exp(tgt) if isinstance(tgt, exp.Table) else tgt.name.upper()

    # Source (USING clause)
    using = parsed.args.get("using")
    source_tables: Set[str] = set()
    alias_to_source: Dict[str, Any] = {}
    if using:
        for t in using.find_all(exp.Table):
            tbl_name = _table_name_from_table_exp(t)
            source_tables.add(tbl_name)
            if t.alias:
                alias_to_source[t.alias.upper()] = tbl_name
        if isinstance(using, exp.Subquery) and using.alias:
            derived = build_scope_from_query(using.this)
            alias_to_source[using.alias.upper()] = derived

    all_tables = source_tables | {target_table}
    scope = Scope(base_tables=all_tables, alias_to_source=alias_to_source, alias_to_expr={})
    alias_map = {k.upper(): v for k, v in flatten_aliases_filtered(scope).items()}

    column_semantics: List[Dict] = []
    for when in (parsed.args.get("whens") or []):
        if isinstance(when, exp.When):
            column_semantics.extend(
                _parse_merge_when_clause(when, target_table, scope, alias_map, column_dictionary, dialect)
            )

    return {
        "statement_type": "MERGE",
        "target": {
            "table": target_table,
        },
        "source_scope": {
            "base_tables": sorted(source_tables),
            "aliases": alias_map,
        },
        "column_semantics": column_semantics,
    }


# -----------------------------
# DQ evaluation (simple)
# -----------------------------

def dq_evaluate(semantic: Dict) -> Dict:
    """
    DQ checks:
    - missing_base_sources for non-constant mappings, with explanation of why resolution failed
    """
    missing = []
    for m in semantic.get("column_semantics") or []:
        cls = m.get("classification")
        if cls == "Constant":
            continue
        if not (m.get("base_sources") or []):
            missing.append({
                "target_column": m.get("target_column"),
                "classification": cls,
                "resolved_expression": m.get("resolved_expression"),
                "resolution_note": m.get(
                    "resolution_note",
                    "Source column(s) could not be resolved to a known table",
                ),
            })
    notes = []
    if missing:
        dict_miss = [m for m in missing if "not found in external dictionary" in (m.get("resolution_note") or "")]
        ambig = [m for m in missing if "ambiguous" in (m.get("resolution_note") or "")]
        no_dict = [m for m in missing if "no external dictionary" in (m.get("resolution_note") or "")]
        other = [m for m in missing if m not in dict_miss and m not in ambig and m not in no_dict]
        if dict_miss:
            notes.append(
                f"{len(dict_miss)} column(s) not found in external dictionary lookup "
                f"— add them to the CSV to enable lineage resolution."
            )
        if ambig:
            notes.append(
                f"{len(ambig)} column(s) are ambiguous across multiple source tables "
                f"— qualify them in SQL or refine the dictionary."
            )
        if no_dict:
            notes.append(
                f"{len(no_dict)} column(s) are unqualified and no external dictionary was provided."
            )
        if other:
            notes.append(
                f"{len(other)} column(s) could not be resolved — see resolution_note for details."
            )
    return {
        "missing_base_sources": missing,
        "unresolved_derived_alias_refs": [],
        "notes": notes,
    }