# BTEQ SQL Lineage Tool

Reads Teradata BTEQ shell scripts and produces an HTML report showing how data flows column-by-column between tables.

---

## First-time setup

Run this once to create the Python environment:

```bash
python3 -m venv .venv
.venv/bin/pip install sqlglot
```

---

## How to run

```bash
./run_lineage.sh BTEQ
```

- `BTEQ` is the folder containing your `.sh` files — replace it with whatever folder you want to process
- The script runs all three processing steps automatically (parse → graph → report)
- When it finishes, open the HTML report in your browser:

```
output/BTEQ/report.html
```

---

## Options

**Start fresh** (ignore any previous partial run):
```bash
./run_lineage.sh BTEQ --reset
```

**Save output to a specific folder instead of the default:**
```bash
./run_lineage.sh BTEQ output/march_run
```

---

## What it produces

All output files land in `output/BTEQ/` (or whatever folder name you passed in):

| File | What it is |
|---|---|
| `report.html` | Open this — the full lineage report |
| `parse.json` | Raw parse results for every script |
| `graph.json` | Lineage graph data |
| `parse.log` | Detailed log with per-file timing and any errors |
| `parse.checkpoint.jsonl` | Progress checkpoint — lets you resume a crashed run |

---

## Resuming a crashed run

If the process is interrupted, just run the same command again without `--reset`:

```bash
./run_lineage.sh BTEQ
```

It will skip files that already finished and pick up where it left off.

---

## What you see while it runs

The terminal prints a row for each file as it completes, and a progress banner every 10%:

```
[1/109]  ACES_CLM_bteq_CSA_CLM_LINE_STG_load.sh    12 stmts  4.2s
[2/109]  ...

──────────────────────────────────────────────
  ▶  PROGRESS  [████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░]  10.0%  (11/109 files)
──────────────────────────────────────────────
```

A log file (`output/BTEQ/parse.log`) records timestamps and errors for every file.
