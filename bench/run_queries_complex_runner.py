#!/usr/bin/env python3
"""
Runner for Grism complex queries from grism_queries_complex.py.

This script tests that all complex queries can successfully generate logical plans.
It does NOT execute the queries (which would require actual data), but validates
that the query construction and plan generation work correctly.

Usage:
    python -m bench.run_queries_complex_runner              # Run all queries
    python -m bench.run_queries_complex_runner --query 1    # Run only complex_query_001
    python -m bench.run_queries_complex_runner --verbose    # Show logical plans
    python -m bench.run_queries_complex_runner --all        # Include alternative implementations
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


# Helper functions to reduce duplication
def _unwrap_args(args: tuple) -> list[Any]:
    """Unwrap FrameCapture arguments to their underlying frames."""
    return [arg._frame if isinstance(arg, (FrameCapture, GroupedFrameCapture)) else arg
            for arg in args]


def _wrap_result(result: Any, plan_captured: list[str]) -> Any:
    """Wrap result in appropriate capture type based on its properties."""
    if hasattr(result, 'explain'):
        return FrameCapture(result, plan_captured)
    elif is_frame_like(result):
        return GroupedFrameCapture(result, plan_captured)
    return result


def _create_method_interceptor(frame: Any, plan_captured: list[str], method_name: str, return_value: Any):
    """Create a generic method interceptor that captures the plan."""
    def interceptor(*args, **kwargs):
        try:
            plan = frame.explain("logical")
            plan_captured.append(plan)
        except Exception as e:
            plan_captured.append(f"ERROR: {e}")
        return return_value
    return interceptor


def _create_callable_wrapper(attr: Any, plan_captured: list[str]) -> Any:
    """Create a wrapper for callable attributes that continues the capture chain."""
    def wrapper(*args, **kwargs):
        unwrapped_args = _unwrap_args(args)
        result = attr(*unwrapped_args, **kwargs)
        return _wrap_result(result, plan_captured)
    return wrapper


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
            return _create_callable_wrapper(attr, self._plan_captured)

        return attr


class FrameCapture:
    """
    A wrapper that captures the frame before .collect() is called.

    This allows us to intercept the query chain and extract the logical plan
    without actually executing the query.
    """

    # Define methods that need special interception with their return values
    _INTERCEPTED_METHODS = {
        "collect": [],
        "first": None,
        "count": 0,
        "exists": False,
    }

    def __init__(self, frame: Any, plan_captured: list[str]):
        self._frame = frame
        self._plan_captured = plan_captured

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._frame, name)

        # Handle intercepted methods (collect, first, count, exists)
        if name in self._INTERCEPTED_METHODS:
            return _create_method_interceptor(
                self._frame, self._plan_captured, name, self._INTERCEPTED_METHODS[name]
            )

        # Handle special methods that need argument unwrapping
        if name in ("union", "intersect", "except_"):
            def special_method(*args, **kwargs):
                unwrapped_args = _unwrap_args(args)
                result = attr(*unwrapped_args, **kwargs)
                return _wrap_result(result, self._plan_captured)
            return special_method

        # Wrap regular callable attributes
        if callable(attr):
            return _create_callable_wrapper(attr, self._plan_captured)

        return attr


class HypergraphCapture:
    """
    A wrapper around Hypergraph that captures logical plans instead of executing queries.
    """

    # Methods that return frames and should be wrapped
    _FRAME_METHODS = ("nodes", "edges", "hyperedges")

    def __init__(self):
        self._hg = gr.Hypergraph.connect("grism://local")
        self._captured_plans: list[str] = []

    def get_captured_plan(self) -> str | None:
        """Get the most recently captured plan."""
        return self._captured_plans[-1] if self._captured_plans else None

    def clear_captured(self):
        """Clear captured plans."""
        self._captured_plans.clear()

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._hg, name)

        # Handle frame-returning methods
        if name in self._FRAME_METHODS:
            def frame_method(*args, **kwargs):
                result = attr(*args, **kwargs)
                return FrameCapture(result, self._captured_plans)
            return frame_method

        return attr


# Global reference to current capture list (set during query execution)
_current_capture_list: list[str] | None = None


def _create_path_wrapper(original_func: Callable) -> Callable:
    """Create a wrapper for path functions that handles argument unwrapping and result wrapping."""
    def wrapped_path(*args, **kwargs):
        global _current_capture_list
        unwrapped_args = _unwrap_args(args)
        result = original_func(*unwrapped_args, **kwargs)
        if hasattr(result, 'explain') and _current_capture_list is not None:
            return FrameCapture(result, _current_capture_list)
        return result
    return wrapped_path


def _create_exists_wrapper(original_func: Callable) -> Callable:
    """Create a wrapper for exists() that handles FrameCapture arguments."""
    def wrapped_exists(subquery, *args, **kwargs):
        # Unwrap FrameCapture if necessary
        if isinstance(subquery, FrameCapture):
            subquery = subquery._frame
        return original_func(subquery, *args, **kwargs)
    return wrapped_exists


def _create_all_any_wrapper(original_func: Callable) -> Callable:
    """Create a wrapper for all_() and any_() that handles arguments properly."""
    def wrapped_func(*args, **kwargs):
        # These functions take expressions, not frames, so just pass through
        return original_func(*args, **kwargs)
    return wrapped_func


def get_query_functions(include_alternatives: bool = False) -> dict[str, Callable]:
    """
    Import and return all query functions from grism_queries_complex.py.

    Also patches the module-level functions to work with capture mechanism.

    Args:
        include_alternatives: If True, include alternative implementations (_alt suffix)

    Returns:
        Dict mapping query names to query functions.
    """
    from bench import grism_queries_complex as queries_module
    import grism as gr

    # Wrap module-level path functions to return FrameCapture
    queries_module.shortest_path = _create_path_wrapper(gr.shortest_path)
    queries_module.all_paths = _create_path_wrapper(gr.all_paths)
    
    # Wrap exists to handle FrameCapture arguments
    queries_module.exists = _create_exists_wrapper(gr.exists)
    
    # Wrap all_ and any_ 
    queries_module.all_ = _create_all_any_wrapper(gr.all_)
    queries_module.any_ = _create_all_any_wrapper(gr.any_)

    # Collect all query functions
    query_functions = {}
    
    # Main complex queries (001-005)
    for i in range(1, 6):
        query_name = f"complex_query_{i:03d}"
        if hasattr(queries_module, query_name):
            query_functions[query_name] = getattr(queries_module, query_name)
    
    # Bonus query
    if hasattr(queries_module, "complex_query_bonus"):
        query_functions["complex_query_bonus"] = getattr(queries_module, "complex_query_bonus")
    
    # Alternative implementations (if requested)
    if include_alternatives:
        for i in range(1, 6):
            alt_name = f"complex_query_{i:03d}_alt"
            if hasattr(queries_module, alt_name):
                query_functions[alt_name] = getattr(queries_module, alt_name)

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
        description="Run Grism complex queries and validate logical plan generation"
    )
    parser.add_argument(
        "--query", "-q", type=int,
        help="Run only a specific query (e.g., --query 1 for complex_query_001)"
    )
    parser.add_argument(
        "--name", "-n", type=str,
        help="Run a query by full name (e.g., --name complex_query_003_alt)"
    )
    parser.add_argument(
        "--all", "-a", action="store_true",
        help="Include alternative implementations (_alt suffix)"
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
    query_functions = get_query_functions(include_alternatives=args.all)
    print(f"Found {len(query_functions)} query functions")
    
    # Determine which queries to run
    if args.query:
        query_name = f"complex_query_{args.query:03d}"
        if query_name not in query_functions:
            print(f"Error: {query_name} not found")
            sys.exit(1)
        queries_to_run = {query_name: query_functions[query_name]}
    elif args.name:
        if args.name not in query_functions:
            # Try to find a matching query
            matches = [k for k in query_functions if args.name in k]
            if len(matches) == 1:
                args.name = matches[0]
            elif matches:
                print(f"Error: Ambiguous query name. Matches: {matches}")
                sys.exit(1)
            else:
                print(f"Error: {args.name} not found")
                print(f"Available queries: {list(query_functions.keys())}")
                sys.exit(1)
        queries_to_run = {args.name: query_functions[args.name]}
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
