#!/usr/bin/env bash
#
# run_noauth_pipeline.sh — Full VoC pipeline without any API credentials
#
# Scrapes Reddit → adapts format → imports to DB → ready for AI analysis
#
# Usage:
#   ./scripts/run_noauth_pipeline.sh                  # all tiers, default settings
#   ./scripts/run_noauth_pipeline.sh --tier 1_pain_language   # single tier
#   ./scripts/run_noauth_pipeline.sh --quick           # fast run (no comments, 10 items)
#
set -euo pipefail

cd "$(dirname "$0")/.."

echo "=============================================="
echo "  Zen Books VoC Pipeline (No-Auth Mode)"
echo "  $(date)"
echo "=============================================="

# Parse args
TIER="all"
MAX_ITEMS=25
TIME_FILTER="month"
EXTRA_FLAGS=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --tier)
            [[ $# -lt 2 ]] && { echo "Error: --tier requires a value"; exit 1; }
            TIER="$2"; shift 2;;
        --max-items)
            [[ $# -lt 2 ]] && { echo "Error: --max-items requires a value"; exit 1; }
            MAX_ITEMS="$2"; shift 2;;
        --time)
            [[ $# -lt 2 ]] && { echo "Error: --time requires a value"; exit 1; }
            TIME_FILTER="$2"; shift 2;;
        --quick)     MAX_ITEMS=10; EXTRA_FLAGS="--no-comments"; shift;;
        *)           echo "Unknown arg: $1"; exit 1;;
    esac
done

echo ""
echo "Step 1/3: Scraping Reddit (public JSON, no auth needed)"
echo "----------------------------------------------"
python scripts/noauth_scrape.py \
    --tier "$TIER" \
    --max-items "$MAX_ITEMS" \
    --time-filter "$TIME_FILTER" \
    $EXTRA_FLAGS

echo ""
echo "Step 2/3: Adapting to pipeline format"
echo "----------------------------------------------"
python scripts/apify_adapter.py

echo ""
echo "Step 3/3: Importing into SQLite database"
echo "----------------------------------------------"
python scripts/apify_import.py

echo ""
echo "=============================================="
echo "  PIPELINE COMPLETE"
echo ""
echo "  Posts are now in the database."
echo "  To run AI analysis (needs OPENAI_API_KEY or ANTHROPIC_API_KEY):"
echo "    python main.py"
echo ""
echo "  Or export raw data:"
echo "    cat data/apify_adapted/summary.json"
echo "=============================================="
