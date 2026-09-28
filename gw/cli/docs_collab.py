"""CLI for Docs collaboration awareness.

Adds to the existing command groups without editing their modules:

    gw docs suggestions <doc>
    gw docs snapshot <doc>
    gw docs diff <doc> [--since latest|<timestamp>|<revisionId>]
    gw docs changes <doc>
    gw docs export <doc> --format md|text|email-html [--suggestions ...] [--out PATH]
    gw comments list <file> [--open] [--since ISO] [--author NAME] [--chrono]

Hooked in with a single ``docs_collab.register(subparsers)`` call placed after
the ``docs`` and ``comments`` groups are registered.
"""

import argparse
from pathlib import Path

from gw.auth import get_service, get_services
from gw.output import success, error
from gw.services import docs_collab as svc


def _sub_action(parser):
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            return action
    raise RuntimeError("parser has no subcommands")


def _run(fn):
    def wrapper(args):
        try:
            success(fn(args))
        except Exception as e:  # noqa: BLE001 - CLI boundary
            error(str(e))
    return wrapper


@_run
def cmd_suggestions(args):
    return svc.list_suggestions(get_service("docs"), args.file_id)


@_run
def cmd_snapshot(args):
    docs_service, drive_service = get_services("docs", "drive")
    return svc.snapshot(docs_service, args.file_id, drive_service=drive_service)


@_run
def cmd_diff(args):
    docs_service, drive_service = get_services("docs", "drive")
    return svc.diff_doc(docs_service, drive_service, args.file_id, since=args.since)


@_run
def cmd_changes(args):
    docs_service, drive_service = get_services("docs", "drive")
    return svc.doc_changes(docs_service, drive_service, args.file_id)


@_run
def cmd_export(args):
    result = svc.export_doc(
        get_service("docs"), args.file_id, args.format, suggestions=args.suggestions,
    )
    if args.out:
        out = Path(args.out).expanduser()
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(result["content"], encoding="utf-8")
        content = result.pop("content")
        result.update({"out": str(out), "bytes": len(content.encode("utf-8"))})
    return result


def cmd_comments_list(args):
    try:
        drive_service, docs_service = get_services("drive", "docs")
        success(svc.list_comments(
            drive_service, docs_service, args.file_id,
            open_only=args.open, since=args.since, author=args.author, chrono=args.chrono,
        ))
    except Exception as e:  # noqa: BLE001 - CLI boundary
        error(str(e))


def register(subparsers):
    """Attach collaboration subcommands to the existing docs/comments groups."""
    docs_sub = _sub_action(subparsers.choices["docs"])

    p = docs_sub.add_parser("suggestions", help="List pending suggestions grouped by id")
    p.add_argument("file_id", help="Document ID or URL")
    p.set_defaults(func=cmd_suggestions)

    p = docs_sub.add_parser(
        "snapshot", help="Save text, structure, comments and suggestions to ~/.cache/gw/docs",
    )
    p.add_argument("file_id", help="Document ID or URL")
    p.set_defaults(func=cmd_snapshot)

    p = docs_sub.add_parser(
        "diff", help="Diff the doc (text, comments, suggestions) against a snapshot",
    )
    p.add_argument("file_id", help="Document ID or URL")
    p.add_argument(
        "--since", default="latest",
        help="'latest' (default), a snapshot timestamp (or unique prefix), "
        "or the revisionId recorded in a snapshot",
    )
    p.set_defaults(func=cmd_diff)

    p = docs_sub.add_parser(
        "changes", help="Who modified the doc, revisions, and edits by others since the latest snapshot",
    )
    p.add_argument("file_id", help="Document ID or URL")
    p.set_defaults(func=cmd_changes)

    p = docs_sub.add_parser("export", help="Export as markdown, plain text or email-safe HTML")
    p.add_argument("file_id", help="Document ID or URL")
    p.add_argument("--format", required=True, choices=["md", "text", "email-html"])
    p.add_argument(
        "--suggestions", default="rejected", choices=["accepted", "inline", "rejected"],
        help="How to treat pending suggestions (default: rejected, i.e. the doc "
        "without pending suggestions)",
    )
    p.add_argument("--out", help="Write the content to this path instead of the JSON payload")
    p.set_defaults(func=cmd_export)

    # Upgrade `gw comments list` in place.
    comments_parser = subparsers.choices.get("comments")
    if comments_parser is not None:
        p_list = _sub_action(comments_parser).choices["list"]
        p_list.add_argument("--open", action="store_true", help="Only unresolved comments")
        p_list.add_argument(
            "--since", help="Only threads with a comment or reply created at/after this ISO time",
        )
        p_list.add_argument("--author", help="Comment author name or email (substring, case-insensitive)")
        p_list.add_argument("--chrono", action="store_true", help="Oldest first (default newest first)")
        from gw.cli import comments as comments_cli
        comments_cli.cmd_list = cmd_comments_list
