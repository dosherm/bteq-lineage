# BTEQ SQL Lineage Tool

Extracts column-level SQL lineage from Teradata BTEQ shell scripts and produces an HTML report.

## What it does

1. **Parses** all `.sh` files in a folder — extracts INSERT, CREATE, UPDATE, and MERGE statements
2. **Builds** a lineage graph — traces how data flows column-by-column from source tables to target tables
3. **Reports** — generates a self-contained HTML report showing every transformation chain

## Requirements

- Python 3.9+
- `sqlglot` library

Install dependencies:
```bash
python3 -m venv .venv
.venv/bin/pip install sqlglot
```

## Quick start

Run the full pipeline against a folder of `.sh` files:

```bash
./run_lineage.sh <folder>
```

**Examples:**
```bash
# Basic run (resumes from checkpoint if it exists)
./run_lineage.sh BTEQ

# Custom output location
./run_lineage.sh BTEQ output/march_run

# Start fresh, discard any previous checkpoint
./run_lineage.sh BTEQ --reset
```

Outputs land in `output/<foldername>/` by default:

| File | Description |
|---|---|
| `parse.json` | Raw parse results for all scripts |
| `graph.json` | Lineage graph with edges and chains |
| `report.html` | Self-contained HTML report — open in any browser |
| `parse.checkpoint.jsonl` | Crash-safe checkpoint (resume without `--reset`) |
| `parse.log` | Full run log with per-file timing and errors |

## Running individual pipeline steps

The `run_lineage.sh` wrapper calls three scripts in sequence. You can also run them individually:

### Step 1 — Parse

```bash
.venv/bin/python run_bteq_corpus_semantic_parse_v2.py \
  --dir BTEQ \
  --prefix "" \
  --dict-csv EDWard_Attribute_Schema_Table_Column.csv \
  --out output/parse.json \
  --reset
```

Key options:

| Option | Default | Description |
|---|---|---|
| `--dir` | — | Folder containing `.sh` files |
| `--prefix` | `ACES_CLM` | Filter files by name prefix (`""` to process all) |
| `--dict-csv` | — | Column dictionary CSV (Schema, Table, Column headers) |
| `--out` | — | Output JSON path |
| `--reset` | off | Discard checkpoint and reprocess everything |
| `--max-stmt-chars` | 10000 | Skip statements larger than this (uses simplified fallback) |
| `--stmt-timeout` | 10 | Seconds before abandoning a single statement |
| `--log` | `<out>.log` | Log file path |

### Step 2 — Build lineage graph

```bash
.venv/bin/python build_lineage_graph.py \
  --input output/parse.json \
  --out output/graph.json
```

### Step 3 — Build HTML report

```bash
.venv/bin/python build_lineage_report.py \
  --lineage output/graph.json \
  --parse output/parse.json \
  --out output/report.html
```

## Progress and logging

While parsing, the terminal shows:
- A per-file row with statement counts and elapsed time
- A visual progress bar at every **10% milestone**
- A full progress bar on completion

A log file (`parse.log`) is written alongside the output JSON with:
- Timestamp for every file processed
- Warnings for any parse failures
- Milestone summaries
- Final run summary

## Column dictionary

The optional `--dict-csv` file maps column names to their canonical table. It must have three columns:

```
Schema,Table,Column
EDW,CLM_STG,CLM_KEY
EDW,CLM_STG,ADJDCTN_DT
...
```

This improves source resolution for unqualified column references in complex SQL.

## Duplicate file handling

The tool processes each `.sh` file once. If your folder contains numbered duplicate files (e.g. `script 2.sh`, `script 3.sh`), deduplicate them first using content-based comparison (MD5 hash) — do not rely on filenames alone.

## Crash recovery

The parser checkpoints after every file. If a run is interrupted:

```bash
# Resume from where it left off (omit --reset)
./run_lineage.sh BTEQ
```

The checkpoint file (`parse.checkpoint.jsonl`) records each completed file as a JSON line. On resume, already-completed files are skipped instantly.
