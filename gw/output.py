"""JSON output helpers for the gw CLI."""

import json
import sys


def success(result):
    """Print a successful result as JSON to stdout."""
    if isinstance(result, str):
        # Try to parse as JSON first
        try:
            parsed = json.loads(result)
            json.dump(parsed, sys.stdout, indent=2, default=str)
        except (json.JSONDecodeError, ValueError):
            json.dump({"output": result}, sys.stdout, indent=2, default=str)
    elif isinstance(result, (dict, list)):
        json.dump(result, sys.stdout, indent=2, default=str)
    else:
        json.dump({"output": str(result)}, sys.stdout, indent=2, default=str)
    print()  # trailing newline


def error(message, exit_code=1):
    """Print an error as JSON to stderr and exit."""
    json.dump({"error": str(message)}, sys.stderr, indent=2, default=str)
    print(file=sys.stderr)
    sys.exit(exit_code)
