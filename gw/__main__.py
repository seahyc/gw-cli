"""Google Workspace CLI entry point.

Usage: python -m gw <service> <action> [args]
"""

import argparse
import json
import sys

from gw.cli import gmail, drive, docs, sheets, calendar, forms, slides, comments


def main():
    parser = argparse.ArgumentParser(
        prog="gw",
        description="Google Workspace CLI - interact with Google Workspace from the command line",
    )
    subparsers = parser.add_subparsers(dest="service", help="Google Workspace service")

    # Register auth commands directly
    auth_parser = subparsers.add_parser("auth", help="Authentication management")
    auth_sub = auth_parser.add_subparsers(dest="action", required=True)

    auth_sub.add_parser("login", help="Authenticate with Google")
    auth_sub.add_parser("status", help="Show authentication status")
    auth_sub.add_parser("logout", help="Remove stored credentials")

    # Register all service CLIs
    gmail.register(subparsers)
    drive.register(subparsers)
    docs.register(subparsers)
    sheets.register(subparsers)
    calendar.register(subparsers)
    forms.register(subparsers)
    slides.register(subparsers)
    comments.register(subparsers)

    args = parser.parse_args()

    if not args.service:
        parser.print_help()
        sys.exit(1)

    # Handle auth commands
    if args.service == "auth":
        from gw.auth import auth_login, auth_status, auth_logout
        from gw.output import success

        if args.action == "login":
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
