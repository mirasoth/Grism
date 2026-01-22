#!/usr/bin/env python3
"""
E2E Logical Plan Benchmark for Grism Python API.

This script runs all 200 queries from grism_queries_part1.py and validates
that each query can successfully construct a logical plan.

Usage:
    python bench/run_logical_benchmark.py [--verbose] [--query QUERY_NUM]

Options:
    --verbose       Print detailed output for each query
    --query NUM     Run only a specific query (e.g., --query 1 for query_001)
"""

import argparse
import json
import os
import re
import sys
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any

# Add the project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import grism as gr


class FrameCapture:
    """
    A wrapper that captures the NodeFrame before .collect() is called.
    
    This allows us to intercept the query chain and extract the logical plan
    without actually executing the query.
    """
    
    def __init__(self, frame: Any, plan_captured: list[str]):
        self._frame = frame
        self._plan_captured = plan_captured
    
    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._frame, name)

        if name == "collect":
            # Intercept collect() - capture the plan and return empty result
            def capture_and_return(*args, **kwargs):
                try:
                    plan = self._frame.explain("logical")
                    self._plan_captured.append(plan)
                except Exception as e:
                    self._plan_captured.append(f"ERROR: {e}")
                return []  # Return empty result
            return capture_and_return

        if name == "union":
            # Special method that needs unwrapping of arguments
            def special_method(*args, **kwargs):
                # Unwrap FrameCapture arguments
                unwrapped_args = []
                for arg in args:
                    if isinstance(arg, FrameCapture):
                        unwrapped_args.append(arg._frame)
                    else:
                        unwrapped_args.append(arg)

                result = attr(*unwrapped_args, **kwargs)
                # Check if result is a frame type
                if hasattr(result, 'explain'):
                    return FrameCapture(result, self._plan_captured)
                return result
            return special_method

        if callable(attr):
            # Wrap methods that return frames to continue capturing
            def wrapper(*args, **kwargs):
                result = attr(*args, **kwargs)
                # Check if result is a frame type
                if hasattr(result, 'explain'):
                    return FrameCapture(result, self._plan_captured)
                return result
            return wrapper

        return attr


class HypergraphCapture:
    """
    A wrapper around Hypergraph that captures logical plans instead of executing queries.
    """
    
    def __init__(self):
        self._hg = gr.Hypergraph.connect("grism://local")
        self._captured_plans: list[str] = []
    
    def get_captured_plan(self) -> str | None:
        """Get the most recently captured plan."""
        return self._captured_plans[-1] if self._captured_plans else None
    
    def clear_captured(self):
        """Clear captured plans."""
        self._captured_plans.clear()
    
    def nodes(self, label: str | None = None) -> FrameCapture:
        frame = self._hg.nodes(label)
        return FrameCapture(frame, self._captured_plans)
    
    def edges(self, label: str | None = None) -> FrameCapture:
        frame = self._hg.edges(label)
        return FrameCapture(frame, self._captured_plans)
    
    def hyperedges(self, label: str | None = None) -> FrameCapture:
        frame = self._hg.hyperedges(label)
        return FrameCapture(frame, self._captured_plans)
    
    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._hg, name)

        if name in ["shortest_path", "all_paths"]:
            # Global functions that need unwrapping of arguments
            def global_function(*args, **kwargs):
                # Unwrap FrameCapture arguments
                unwrapped_args = []
                for arg in args:
                    if isinstance(arg, FrameCapture):
                        unwrapped_args.append(arg._frame)
                    else:
                        unwrapped_args.append(arg)

                return attr(*unwrapped_args, **kwargs)
            return global_function

        return attr


def get_query_functions() -> dict[str, callable]:
    """
    Import and return all query functions from grism_queries_part1.py.
    
    Returns:
        Dict mapping query names to query functions.
    """
    # Import the queries module
    import importlib.util
    
    queries_path = PROJECT_ROOT / "bench" / "grism_queries_part1.py"
    spec = importlib.util.spec_from_file_location("grism_queries_part1", queries_path)
    queries_module = importlib.util.module_from_spec(spec)
    
    # We need to execute in a modified environment where missing symbols don't cause errors
    # The file imports symbols that need to exist
    spec.loader.exec_module(queries_module)
    
    # Extract all query_NNN functions
    query_functions = {}
    for i in range(1, 201):
        query_name = f"query_{i:03d}"
        if hasattr(queries_module, query_name):
            query_functions[query_name] = getattr(queries_module, query_name)
    
    return query_functions


def validate_logical_plan(query_id: str, plan: str) -> tuple[bool, str]:
    """
    Validate a logical plan for correctness.

    Args:
        query_id: The query identifier (e.g., "query_003")
        plan: The logical plan string

    Returns:
        Tuple of (is_valid, validation_message)
    """
    try:
        # Basic structure validation
        if not plan or plan.strip() == "":
            return False, "Empty plan"

        # Check for common structural issues
        if "ERROR:" in plan:
            return False, "Plan contains errors"

        # Query-specific validations (can be expanded)
        if query_id == "query_003":
            # Should have a Project operator
            if "Project" not in plan:
                return False, "Missing Project operator"

        if query_id == "query_005":
            # Should have a Filter with STARTS_WITH
            if "Filter" not in plan or "STARTS_WITH" not in plan:
                return False, "Missing Filter with STARTS_WITH"

        # General validation - check for proper operator nesting
        lines = plan.split("\n")
        indent_levels = []
        for line in lines:
            if line.strip():
                # Count leading spaces to determine indent level
                indent = len(line) - len(line.lstrip())
                indent_levels.append(indent)

        # Check that indentation is consistent (proper tree structure)
        if len(indent_levels) > 1:
            for i in range(1, len(indent_levels)):
                # Each line should be indented more than its parent
                # or at the same level (siblings)
                if indent_levels[i] < indent_levels[i-1] and i < len(indent_levels) - 1:
                    # Check if we're properly closing a subtree
                    if indent_levels[i+1] > indent_levels[i]:
                        return False, "Improper plan nesting"

        return True, "Valid"

    except Exception as e:
        return False, f"Validation error: {e}"


def run_query_with_capture(query_func: callable, hg: HypergraphCapture) -> tuple[bool, str | None, str | None, bool]:
    """
    Run a query function and capture the logical plan.

    Args:
        query_func: The query function to run
        hg: HypergraphCapture instance

    Returns:
        Tuple of (success, plan, error_message, is_valid)
    """
    hg.clear_captured()

    try:
        # Run the query - this will capture the plan when .collect() is called
        query_func(hg)

        plan = hg.get_captured_plan()
        if plan and plan.startswith("ERROR:"):
            return False, None, plan, False

        # If we have a plan, validate it
        is_valid = True
        if plan:
            query_name = getattr(query_func, '__name__', 'unknown')
            is_valid, _ = validate_logical_plan(query_name, plan)

        return True, plan, None, is_valid

    except Exception as e:
        error_msg = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
        return False, None, error_msg, False


def run_benchmark(
    query_filter: int | None = None,
    verbose: bool = False
) -> dict[str, Any]:
    """
    Run the benchmark on all (or selected) queries.

    Args:
        query_filter: If set, only run this specific query number
        verbose: Print detailed output

    Returns:
        Benchmark results dict
    """
    results = {
        "timestamp": datetime.now().isoformat(),
        "total": 0,
        "passed": 0,
        "failed": 0,
        "valid": 0,
        "invalid": 0,
        "queries": {}
    }
    
    print("=" * 70)
    print("Grism E2E Logical Plan Benchmark")
    print("=" * 70)
    print()
    
    # Get query functions
    print("Loading query functions...")
    try:
        query_functions = get_query_functions()
        print(f"Loaded {len(query_functions)} query functions")
    except Exception as e:
        print(f"ERROR: Failed to load query functions: {e}")
        traceback.print_exc()
        return results
    
    print()
    
    # Create capture hypergraph
    hg = HypergraphCapture()
    
    # Filter queries if requested
    if query_filter is not None:
        query_name = f"query_{query_filter:03d}"
        if query_name in query_functions:
            query_functions = {query_name: query_functions[query_name]}
        else:
            print(f"ERROR: Query {query_name} not found")
            return results
    
    # Run each query
    results["total"] = len(query_functions)
    
    for query_name, query_func in sorted(query_functions.items()):
        success, plan, error, is_valid = run_query_with_capture(query_func, hg)

        query_result = {
            "status": "pass" if success else "fail",
            "valid": is_valid,
        }

        if success:
            results["passed"] += 1
            query_result["plan"] = plan
            if is_valid:
                results["valid"] += 1
                status_char = "✓"
            else:
                results["invalid"] += 1
                status_char = "~"  # Tilde for passed but invalid
        else:
            results["failed"] += 1
            query_result["error"] = error
            status_char = "✗"

        results["queries"][query_name] = query_result
        
        # Print progress
        if verbose:
            print(f"{status_char} {query_name}")
            if success and plan:
                # Print first few lines of plan
                plan_preview = "\n".join(plan.split("\n")[:5])
                print(f"   Plan preview:\n{plan_preview}")
            elif error:
                # Print first line of error
                error_preview = error.split("\n")[0][:100]
                print(f"   Error: {error_preview}")
        else:
            # Simple progress indicator
            print(status_char, end="", flush=True)
            if int(query_name.split("_")[1]) % 50 == 0:
                print(f" [{query_name}]")
    
    if not verbose:
        print()  # Newline after progress dots
    
    print()
    print("=" * 70)
    print(f"Results: {results['passed']}/{results['total']} passed, "
          f"{results['failed']} failed")
    if results['passed'] > 0:
        print(f"Validation: {results['valid']}/{results['passed']} plans valid, "
              f"{results['invalid']} invalid")
    print("=" * 70)
    
    return results


def save_results(results: dict[str, Any], output_dir: Path):
    """
    Save benchmark results to files.
    
    Args:
        results: Benchmark results
        output_dir: Directory to save results
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Save JSON results
    json_path = output_dir / "logical_plans.json"
    with open(json_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"Saved logical plans to: {json_path}")
    
    # Save text report
    report_path = output_dir / "benchmark_report.txt"
    with open(report_path, "w") as f:
        f.write("Grism E2E Logical Plan Benchmark Report\n")
        f.write("=" * 50 + "\n\n")
        f.write(f"Timestamp: {results['timestamp']}\n")
        f.write(f"Total queries: {results['total']}\n")
        f.write(f"Passed: {results['passed']}\n")
        f.write(f"Failed: {results['failed']}\n")
        f.write(f"Pass rate: {results['passed']/results['total']*100:.1f}%\n")
        if results['passed'] > 0:
            f.write(f"Valid plans: {results['valid']}/{results['passed']} "
                   f"({results['valid']/results['passed']*100:.1f}%)\n")
            f.write(f"Invalid plans: {results['invalid']}\n")
        f.write("\n")
        
        # List failed queries
        failed = [name for name, data in results["queries"].items() 
                  if data["status"] == "fail"]
        if failed:
            f.write("Failed Queries:\n")
            f.write("-" * 30 + "\n")
            for name in sorted(failed):
                error = results["queries"][name].get("error", "Unknown error")
                error_first_line = error.split("\n")[0][:80]
                f.write(f"  {name}: {error_first_line}\n")
            f.write("\n")
        
        # List passed queries with plan summaries
        f.write("Passed Queries:\n")
        f.write("-" * 30 + "\n")
        for name in sorted(results["queries"].keys()):
            data = results["queries"][name]
            if data["status"] == "pass":
                plan = data.get("plan", "No plan")
                # Extract first meaningful line of plan
                plan_lines = [l for l in plan.split("\n") if l.strip()]
                plan_summary = plan_lines[1] if len(plan_lines) > 1 else plan_lines[0] if plan_lines else "Empty"
                f.write(f"  {name}: {plan_summary[:60]}\n")

    print(f"Saved report to: {report_path}")

    # Save validation report
    validation_path = output_dir / "validation_report.txt"
    with open(validation_path, "w") as f:
        f.write("Logical Plan Validation Report\n")
        f.write("=" * 50 + "\n\n")
        f.write(f"Total queries with plans: {results['passed']}\n")
        f.write(f"Total queries with valid plans: {results['valid']}\n")
        f.write(f"Total queries with invalid plans: {results['invalid']}\n")
        f.write(f"Validation failures: {results['invalid']}\n\n")

        # List invalid plans
        invalid = [name for name, data in results["queries"].items()
                  if data.get("status") == "pass" and not data.get("valid", True)]

        if invalid:
            f.write("Invalid Plans:\n")
            f.write("-" * 30 + "\n")
            for name in sorted(invalid):
                f.write(f"  {name}: Plan failed validation\n")
            f.write("\n")
        else:
            f.write("Invalid Plans:\n")
            f.write("-" * 30 + "\n")
            f.write("None\n\n")

        # Validation details for passed queries
        f.write("Validation Details:\n")
        f.write("-" * 30 + "\n")
        for name in sorted(results["queries"].keys()):
            data = results["queries"][name]
            if data["status"] == "pass":
                plan = data.get("plan", "No plan")
                # Extract first meaningful line of plan
                plan_lines = [l for l in plan.split("\n") if l.strip()]
                plan_summary = plan_lines[1] if len(plan_lines) > 1 else plan_lines[0] if plan_lines else "Empty"
                valid_status = "OK" if data.get("valid", True) else "INVALID"
                f.write(f"{name}: {plan_summary[:60]} - {valid_status}\n")

    print(f"Saved validation report to: {validation_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Run E2E logical plan benchmark for Grism Python API"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Print detailed output for each query"
    )
    parser.add_argument(
        "--query", "-q",
        type=int,
        help="Run only a specific query number (e.g., -q 1 for query_001)"
    )
    parser.add_argument(
        "--output", "-o",
        type=str,
        default=str(PROJECT_ROOT / "bench" / "output"),
        help="Output directory for results"
    )
    
    args = parser.parse_args()
    
    # Run benchmark
    results = run_benchmark(
        query_filter=args.query,
        verbose=args.verbose
    )
    
    # Save results
    output_dir = Path(args.output)
    save_results(results, output_dir)
    
    # Exit with appropriate code
    if results["failed"] > 0:
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
