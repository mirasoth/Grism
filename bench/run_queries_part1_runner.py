#!/usr/bin/env python3
"""
Runner for Grism queries from grism_queries_part1.py.

This script tests that all 200 queries can successfully generate logical plans.
It does NOT execute the queries (which would require actual data), but validates
that the query construction and plan generation work correctly.

Usage:
    python -m bench.run_queries_part1_runner              # Run all queries
    python -m bench.run_queries_part1_runner --query 4    # Run only query_004
    python -m bench.run_queries_part1_runner --verbose    # Show logical plans
    python -m bench.run_queries_part1_runner --range 1-10 # Run queries 1-10
"""

import argparse
import sys
import traceback
from typing import Any, Callable

import grism as gr


def is_frame_like(obj: Any) -> bool:
    """Check if an object is a frame-like type that should be wrapped."""
    type_name = type(obj).__name__
    return type_name in ('NodeFrame', 'EdgeFrame', 'HyperedgeFrame', 'GroupedFrame')


class GroupedFrameCapture:
    """
    A wrapper for GroupedFrame that continues the capture chain.
    
    GroupedFrame doesn't have explain(), but its agg() method returns a Frame.
    """
    
    def __init__(self, frame: Any, plan_captured: list[str]):
        self._frame = frame
        self._plan_captured = plan_captured
    
    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._frame, name)
        
        if callable(attr):
            def wrapper(*args, **kwargs):
                result = attr(*args, **kwargs)
                if hasattr(result, 'explain'):
                    return FrameCapture(result, self._plan_captured)
                elif is_frame_like(result):
                    return GroupedFrameCapture(result, self._plan_captured)
                return result
            return wrapper
        
        return attr


class FrameCapture:
    """
    A wrapper that captures the frame before .collect() is called.
    
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

        if name == "first":
            # Intercept first() - capture the plan and return None
            def capture_first(*args, **kwargs):
                try:
                    plan = self._frame.explain("logical")
                    self._plan_captured.append(plan)
                except Exception as e:
                    self._plan_captured.append(f"ERROR: {e}")
                return None
            return capture_first

        if name == "count":
            # Intercept count() - capture the plan and return 0
            def capture_count(*args, **kwargs):
                try:
                    plan = self._frame.explain("logical")
                    self._plan_captured.append(plan)
                except Exception as e:
                    self._plan_captured.append(f"ERROR: {e}")
                return 0
            return capture_count

        if name == "exists":
            # Intercept exists() - capture the plan and return False
            def capture_exists(*args, **kwargs):
                try:
                    plan = self._frame.explain("logical")
                    self._plan_captured.append(plan)
                except Exception as e:
                    self._plan_captured.append(f"ERROR: {e}")
                return False
            return capture_exists

        if name == "union":
            # Special method that needs unwrapping of arguments
            def special_method(*args, **kwargs):
                unwrapped_args = []
                for arg in args:
                    if isinstance(arg, FrameCapture):
                        unwrapped_args.append(arg._frame)
                    else:
                        unwrapped_args.append(arg)
                result = attr(*unwrapped_args, **kwargs)
                if hasattr(result, 'explain'):
                    return FrameCapture(result, self._plan_captured)
                elif is_frame_like(result):
                    return GroupedFrameCapture(result, self._plan_captured)
                return result
            return special_method

        if callable(attr):
            # Wrap methods that return frames to continue capturing
            def wrapper(*args, **kwargs):
                result = attr(*args, **kwargs)
                if hasattr(result, 'explain'):
                    return FrameCapture(result, self._plan_captured)
                elif is_frame_like(result):
                    return GroupedFrameCapture(result, self._plan_captured)
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
            def global_function(*args, **kwargs):
                unwrapped_args = []
                for arg in args:
                    if isinstance(arg, FrameCapture):
                        unwrapped_args.append(arg._frame)
                    else:
                        unwrapped_args.append(arg)
                result = attr(*unwrapped_args, **kwargs)
                if hasattr(result, 'explain'):
                    return FrameCapture(result, self._captured_plans)
                return result
            return global_function

        return attr


# Global reference to current capture list (set during query execution)
_current_capture_list: list[str] | None = None


def get_query_functions() -> dict[str, Callable]:
    """
    Import and return all query functions from grism_queries_part1.py.
    
    Also patches the module-level shortest_path and all_paths functions
    to return wrapped frames.
        
    Returns:
        Dict mapping query names to query functions.
    """
    from bench import grism_queries_part1 as queries_module
    import grism as gr
    
    # Wrap module-level path functions to return FrameCapture
    original_shortest_path = gr.shortest_path
    original_all_paths = gr.all_paths
    
    def wrapped_shortest_path(*args, **kwargs):
        global _current_capture_list
        # Unwrap any FrameCapture arguments
        unwrapped_args = []
        for arg in args:
            if isinstance(arg, (FrameCapture, GroupedFrameCapture)):
                unwrapped_args.append(arg._frame)
            else:
                unwrapped_args.append(arg)
        result = original_shortest_path(*unwrapped_args, **kwargs)
        if hasattr(result, 'explain') and _current_capture_list is not None:
            return FrameCapture(result, _current_capture_list)
        return result
    
    def wrapped_all_paths(*args, **kwargs):
        global _current_capture_list
        # Unwrap any FrameCapture arguments
        unwrapped_args = []
        for arg in args:
            if isinstance(arg, (FrameCapture, GroupedFrameCapture)):
                unwrapped_args.append(arg._frame)
            else:
                unwrapped_args.append(arg)
        result = original_all_paths(*unwrapped_args, **kwargs)
        if hasattr(result, 'explain') and _current_capture_list is not None:
            return FrameCapture(result, _current_capture_list)
        return result
    
    # Patch the module
    queries_module.shortest_path = wrapped_shortest_path
    queries_module.all_paths = wrapped_all_paths
    
    query_functions = {}
    for i in range(1, 201):
        query_name = f"query_{i:03d}"
        if hasattr(queries_module, query_name):
            query_functions[query_name] = getattr(queries_module, query_name)
    
    return query_functions


def run_query_with_capture(query_func: Callable, hg: HypergraphCapture) -> tuple[bool, str]:
    """
    Run a query function and capture its logical plan.
    
    Args:
        query_func: The query function to run
        hg: HypergraphCapture instance
        
    Returns:
        Tuple of (success, plan_or_error)
    """
    global _current_capture_list
    
    hg.clear_captured()
    # Set the global capture list for module-level functions like shortest_path
    _current_capture_list = hg._captured_plans
    
    try:
        query_func(hg)
        plan = hg.get_captured_plan()
        if plan and not plan.startswith("ERROR:"):
            return True, plan
        elif plan:
            return False, plan
        else:
            return False, "No plan captured"
    except Exception as e:
        return False, f"Exception: {e}\n{traceback.format_exc()}"
    finally:
        _current_capture_list = None


def main():
    parser = argparse.ArgumentParser(
        description="Run Grism queries and validate logical plan generation"
    )
    parser.add_argument(
        "--query", "-q", type=int,
        help="Run only a specific query (e.g., --query 4 for query_004)"
    )
    parser.add_argument(
        "--range", "-r", type=str,
        help="Run a range of queries (e.g., --range 1-10)"
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Show logical plans for each query"
    )
    parser.add_argument(
        "--show-errors", "-e", action="store_true",
        help="Show detailed error messages"
    )
    
    args = parser.parse_args()
    
    # Get all query functions
    query_functions = get_query_functions()
    print(f"Found {len(query_functions)} query functions")
    
    # Determine which queries to run
    if args.query:
        query_name = f"query_{args.query:03d}"
        if query_name not in query_functions:
            print(f"Error: {query_name} not found")
            sys.exit(1)
        queries_to_run = {query_name: query_functions[query_name]}
    elif args.range:
        start, end = map(int, args.range.split("-"))
        queries_to_run = {
            f"query_{i:03d}": query_functions[f"query_{i:03d}"]
            for i in range(start, end + 1)
            if f"query_{i:03d}" in query_functions
        }
    else:
        queries_to_run = query_functions
    
    print(f"Running {len(queries_to_run)} queries...\n")
    
    # Create capture hypergraph
    hg = HypergraphCapture()
    
    # Run queries
    passed = []
    failed = []
    
    for query_name, query_func in sorted(queries_to_run.items()):
        success, result = run_query_with_capture(query_func, hg)
        
        if success:
            passed.append(query_name)
            status = "PASS"
        else:
            failed.append((query_name, result))
            status = "FAIL"
        
        if args.verbose:
            print(f"\n{'='*60}")
            print(f"{query_name}: {status}")
            print(f"{'='*60}")
            if success:
                print(result)
            elif args.show_errors:
                print(f"Error: {result}")
        else:
            # Simple progress output
            print(f"  {query_name}: {status}")
    
    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(f"Total:  {len(queries_to_run)}")
    print(f"Passed: {len(passed)}")
    print(f"Failed: {len(failed)}")
    
    if failed:
        print(f"\nFailed queries:")
        for query_name, error in failed:
            if args.show_errors:
                print(f"  {query_name}:")
                for line in error.split('\n')[:5]:  # First 5 lines
                    print(f"    {line}")
            else:
                # Just show first line of error
                first_line = error.split('\n')[0][:80]
                print(f"  {query_name}: {first_line}")
    
    # Exit with error code if any failed
    sys.exit(0 if not failed else 1)


if __name__ == "__main__":
    main()
