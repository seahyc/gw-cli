"""Google Workspace CLI entry point.

Usage: python -m gw <service> <action> [args]
"""

import argparse
import json
import os
from pathlib import Path
import shutil
import sys

from gw.cli import gmail, drive, docs, sheets, calendar, forms, slides, comments
from gw import throttle


def _subparser_choices(parser):
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            return action.choices
    return {}


def _load_env_file(path):
    if not path.exists():
        return {}

    values = {}
    for line in path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            values[key] = value
    return values


def _gws_env():
    env = os.environ.copy()

    # Keep the local gw OAuth env usable for gws without requiring users to
    # duplicate the same secret under both tools' variable names.
    project_env = _load_env_file(Path(__file__).resolve().parents[1] / ".env")
    for key, value in project_env.items():
        env.setdefault(key, value)

    if env.get("GOOGLE_OAUTH_CLIENT_ID"):
        env.setdefault("GOOGLE_WORKSPACE_CLI_CLIENT_ID", env["GOOGLE_OAUTH_CLIENT_ID"])
    if env.get("GOOGLE_OAUTH_CLIENT_SECRET"):
        env.setdefault(
            "GOOGLE_WORKSPACE_CLI_CLIENT_SECRET",
            env["GOOGLE_OAUTH_CLIENT_SECRET"],
        )
    return env


def _exec_gws(argv):
    gws = shutil.which("gws")
    if not gws:
        print(
            json.dumps(
                {
                    "success": False,
                    "error": "gws executable not found on PATH",
                    "hint": "Install @googleworkspace/cli and ensure gws is on PATH.",
                }
            ),
            file=sys.stderr,
        )
        sys.exit(127)

    os.execvpe(gws, [gws, *argv], _gws_env())


def _should_delegate_to_gws(argv, root_choices):
    if not argv:
        return False

    service = argv[0]
    if service in {"api", "gws"}:
        return True
    if service.startswith("-"):
        return False
    if service not in root_choices:
        return True

    service_choices = _subparser_choices(root_choices[service])
    if not service_choices or len(argv) < 2:
        return False

    action = argv[1]
    return not action.startswith("-") and action not in service_choices


def main():
    # Install process-wide rate-limit / 429 retry wrapper around all
    # googleapiclient HttpRequest.execute() calls before any service client
    # is built. Idempotent.
    throttle.install()
    parser = argparse.ArgumentParser(
        prog="gw",
        description="Google Workspace CLI - interact with Google Workspace from the command line",
    )
    subparsers = parser.add_subparsers(dest="service", help="Google Workspace service")

    # Register auth commands directly
    auth_parser = subparsers.add_parser("auth", help="Authentication management")
    auth_sub = auth_parser.add_subparsers(dest="action", required=True)

    login_parser = auth_sub.add_parser("login", help="Authenticate with Google")
    login_parser.add_argument(
        "--manual",
        "--no-browser",
        dest="manual",
        action="store_true",
        help="Headless login: print the consent URL and paste the redirect back "
        "(no browser needed on this machine)",
    )
    auth_sub.add_parser("status", help="Show authentication status")
    auth_sub.add_parser("logout", help="Remove stored credentials")
    subparsers.add_parser(
        "api",
        help="Raw Google API passthrough: gw api <service> <resource> ...",
    )

    # Register all service CLIs
    gmail.register(subparsers)
    drive.register(subparsers)
    docs.register(subparsers)
    sheets.register(subparsers)
    calendar.register(subparsers)
    forms.register(subparsers)
    slides.register(subparsers)
    comments.register(subparsers)

    argv = sys.argv[1:]
    root_choices = _subparser_choices(parser)
    if _should_delegate_to_gws(argv, root_choices):
        _exec_gws(argv[1:] if argv and argv[0] in {"api", "gws"} else argv)

    args = parser.parse_args(argv)

    if not args.service:
        parser.print_help()
        sys.exit(1)

    # Handle auth commands
    if args.service == "auth":
        from gw.auth import auth_login, auth_login_manual, auth_status, auth_logout
        from gw.output import success

        if args.action == "login":
            if getattr(args, "manual", False):
                success(auth_login_manual())
            else:
                success(auth_login())
        elif args.action == "status":
            success(auth_status())
        elif args.action == "logout":
            success(auth_logout())
        return

    # Dispatch to service CLI handlers
    handlers = {
        "gmail": gmail,
        "drive": drive,
        "docs": docs,
        "sheets": sheets,
        "calendar": calendar,
        "forms": forms,
        "slides": slides,
        "comments": comments,
    }

    module = handlers.get(args.service)
    if module and hasattr(module, "handle"):
        module.handle(args)
    elif hasattr(args, "func"):
        args.func(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
