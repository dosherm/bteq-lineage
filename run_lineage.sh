#!/usr/bin/env bash
# run_lineage.sh — parse all .sh files in a folder and produce lineage outputs
#
# Usage:
#   ./run_lineage.sh <folder> [output_prefix] [--reset]
#
# Examples:
#   ./run_lineage.sh BTEQ
#   ./run_lineage.sh BTEQ output/my_run --reset
#
# Outputs (all in output_prefix directory):
#   <prefix>/parse.json       — raw parse results
#   <prefix>/graph.json       — lineage graph
#   <prefix>/report.html      — HTML report
#   <prefix>/parse.checkpoint.jsonl  — crash-safe checkpoint (resume without --reset)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV="$SCRIPT_DIR/.venv/bin/python"
DICT_CSV="$SCRIPT_DIR/EDWard_Attribute_Schema_Table_Column.csv"

# --- Args ---
if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <folder> [output_prefix] [--reset]"
    echo "  folder         directory containing .sh files"
    echo "  output_prefix  output directory (default: output/<folder_name>)"
    echo "  --reset        discard checkpoint and reprocess from scratch"
    exit 1
fi

INPUT_DIR="$(cd "$1" && pwd)"
FOLDER_NAME="$(basename "$INPUT_DIR")"

# Default output prefix based on folder name
OUTPUT_PREFIX="${2:-$SCRIPT_DIR/output/$FOLDER_NAME}"
if [[ "${2:-}" == "--reset" ]]; then
    OUTPUT_PREFIX="$SCRIPT_DIR/output/$FOLDER_NAME"
fi

RESET_FLAG=""
for arg in "$@"; do
    [[ "$arg" == "--reset" ]] && RESET_FLAG="--reset"
done

# --- Setup ---
mkdir -p "$OUTPUT_PREFIX"

PARSE_JSON="$OUTPUT_PREFIX/parse.json"
GRAPH_JSON="$OUTPUT_PREFIX/graph.json"
REPORT_HTML="$OUTPUT_PREFIX/report.html"
CHECKPOINT="$OUTPUT_PREFIX/parse.checkpoint.jsonl"

SH_COUNT=$(find "$INPUT_DIR" -maxdepth 1 -name "*.sh" | wc -l | tr -d ' ')

echo "============================================================"
echo "  Lineage Pipeline"
echo "  Input  : $INPUT_DIR  ($SH_COUNT .sh files)"
echo "  Output : $OUTPUT_PREFIX"
echo "============================================================"
echo ""

# --- Step 1: Parse ---
echo "[1/3] Parsing SQL statements..."
START=$(date +%s)

"$VENV" "$SCRIPT_DIR/run_bteq_corpus_semantic_parse_v2.py" \
    --dir "$INPUT_DIR" \
    --prefix "" \
    --dict-csv "$DICT_CSV" \
    --out "$PARSE_JSON" \
    --checkpoint "$CHECKPOINT" \
    $RESET_FLAG

END=$(date +%s)
echo ""
echo "  Parse complete in $((END - START))s → $PARSE_JSON"
echo ""

# --- Step 2: Build lineage graph ---
echo "[2/3] Building lineage graph..."
START=$(date +%s)

"$VENV" "$SCRIPT_DIR/build_lineage_graph.py" \
    --input "$PARSE_JSON" \
    --out "$GRAPH_JSON"

END=$(date +%s)
echo "  Graph complete in $((END - START))s → $GRAPH_JSON"
echo ""

# --- Step 3: Build HTML report ---
echo "[3/3] Building HTML report..."
START=$(date +%s)

"$VENV" "$SCRIPT_DIR/build_lineage_report.py" \
    --lineage "$GRAPH_JSON" \
    --parse "$PARSE_JSON" \
    --out "$REPORT_HTML"

END=$(date +%s)
echo "  Report complete in $((END - START))s → $REPORT_HTML"
echo ""
echo "============================================================"
echo "  Done.  Open: $REPORT_HTML"
echo "============================================================"
