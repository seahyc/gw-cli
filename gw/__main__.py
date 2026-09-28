"""Google Workspace CLI entry point.

Usage: python -m gw <service> <action> [args]
"""

import argparse
import json
import os
from pathlib import Path
import shutil
import sys

from gw.cli import gmail, drive, docs, sheets, calendar, forms, slides, comments, api
from gw import throttle
from gw.cli import docs_collab


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

    # `gw api ...` now uses gw's own Keychain credentials natively (see
    # gw/cli/api.py) and no longer execs gws. This function only remains for
    # the explicit `gw gws ...` escape hatch to the external gws binary,
    # which keeps its own separate credential store and can hit invalid_grant
    # even when gw itself is authenticated. Pass gw's current access token
    # through in case that path is configured to accept one directly.
    try:
        from gw.auth import get_credentials

        credentials, _ = get_credentials()
        if credentials and credentials.token:
            env.setdefault("GOOGLE_OAUTH_ACCESS_TOKEN", credentials.token)
            env.setdefault("GWS_ACCESS_TOKEN", credentials.token)
    except Exception:
        pass  # best-effort; gws falls back to its own auth if this fails

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
    if service == "gws":
        return True
    if service == "api":
        # 'api' is now a native gw command (see gw/cli/api.py): it uses gw's
        # own Keychain credentials instead of gws' separate credential store.
        return False
    if service.startswith("-"):
        return False
    if service not in root_choices:
        return True

    service_choices = _subparser_choices(root_choices[service])
    if not service_choices or len(argv) < 2:
        return False

    action = argv[1]
    return not action.startswith("-") and action not in service_choices


def _extract_account_flag(argv):
    """Pull a global `--account X` / `--account=X` out of argv (any position).

    Returns (account_or_None, remaining_argv). The flag is global so it works
    both as `gw --account work docs read ...` and `gw docs read ... --account work`.
    """
    account = None
    rest = []
    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg == "--":
            rest.extend(argv[i:])
            break
        if arg == "--account":
            if i + 1 >= len(argv):
                raise SystemExit("gw: --account requires a value (email or alias)")
            account = argv[i + 1]
            i += 2
            continue
        if arg.startswith("--account="):
            account = arg.split("=", 1)[1]
            i += 1
            continue
        rest.append(arg)
        i += 1
    return account, rest


def main():
    # Install process-wide rate-limit / 429 retry wrapper around all
    # googleapiclient HttpRequest.execute() calls before any service client
    # is built. Idempotent.
    throttle.install()
    parser = argparse.ArgumentParser(
        prog="gw",
        description="Google Workspace CLI - interact with Google Workspace from the command line",
    )
    parser.add_argument(
        "--account",
        help="Google account (email or alias from `gw auth list`) to use for this "
        "command; defaults to `gw auth use` choice or the first logged-in account. "
        "Also accepted after the subcommand.",
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
    login_parser.add_argument(
        "--account",
        help="Email to log in as, or an alias to attach to whichever account signs in",
    )
    auth_sub.add_parser("status", help="Show authentication status")
    auth_sub.add_parser("list", help="List logged-in accounts, aliases and the default")
    use_parser = auth_sub.add_parser("use", help="Set the default account")
    use_parser.add_argument("account_name", metavar="account", help="Email or alias")
    logout_parser = auth_sub.add_parser(
        "logout", help="Remove stored credentials (all accounts unless --account)"
    )
    logout_parser.add_argument("--account", help="Only log out this email or alias")

    # Register all service CLIs
    gmail.register(subparsers)
    drive.register(subparsers)
    docs.register(subparsers)
    sheets.register(subparsers)
    calendar.register(subparsers)
    forms.register(subparsers)
    slides.register(subparsers)
    comments.register(subparsers)
    api.register(subparsers)
    docs_collab.register(subparsers)  # after docs + comments: extends both

    account, argv = _extract_account_flag(sys.argv[1:])
    if account:
        from gw.auth import set_active_account

        set_active_account(account)
    root_choices = _subparser_choices(parser)
    if _should_delegate_to_gws(argv, root_choices):
        _exec_gws(argv[1:] if argv and argv[0] in {"api", "gws"} else argv)

    args = parser.parse_args(argv)

    if not args.service:
        parser.print_help()
        sys.exit(1)

    # Handle auth commands
    if args.service == "auth":
        from gw.auth import (
            AccountError, auth_list, auth_login, auth_login_manual, auth_logout,
            auth_status, auth_use,
        )
        from gw.output import error, success

        try:
            if args.action == "login":
                if getattr(args, "manual", False):
                    success(auth_login_manual(account))
                else:
                    success(auth_login(account))
            elif args.action == "status":
                success(auth_status())
            elif args.action == "list":
                success(auth_list())
            elif args.action == "use":
                success(auth_use(args.account_name))
            elif args.action == "logout":
                success(auth_logout(account))
        except AccountError as e:
            error(str(e))
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
