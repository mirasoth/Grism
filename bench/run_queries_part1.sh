#!/bin/bash
#
# Run Grism queries from grism_queries_part1.py
#
# This script validates that all 200 queries can successfully generate
# logical plans. It captures the plan for each query and reports success/failure.
#
# Usage:
#   ./run_queries_part1.sh              # Run all queries
#   ./run_queries_part1.sh --query 4    # Run only query_004
#   ./run_queries_part1.sh --verbose    # Show logical plans
#   ./run_queries_part1.sh --help       # Show help
#

set -e

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Change to project root
cd "$PROJECT_ROOT"

# Run the Python script with all arguments
python -m bench.run_queries_part1_runner "$@"
