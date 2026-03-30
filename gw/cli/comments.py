"""CLI commands for Google Workspace comments (Docs, Sheets, Slides via Drive API)."""

from gw.auth import get_service
from gw.output import success, error
from gw.services import comments


def register(subparsers):
    parser = subparsers.add_parser("comments", help="Comment operations on Docs/Sheets/Slides")
    sub = parser.add_subparsers(dest="action", required=True)

    # comments list
    p_list = sub.add_parser("list", help="List all comments on a file")
    p_list.add_argument("file_id")

    # comments create
    p_create = sub.add_parser("create", help="Create a comment")
    p_create.add_argument("file_id")
    p_create.add_argument("--content", required=True)
    p_create.add_argument("--quoted-text", default="")
    p_create.add_argument("--service", choices=["docs"], default=None, help="Use docs service for text anchoring")

    # comments reply
    p_reply = sub.add_parser("reply", help="Reply to a comment")
    p_reply.add_argument("file_id")
    p_reply.add_argument("--comment-id", required=True)
    p_reply.add_argument("--content", required=True)

    # comments resolve
    p_resolve = sub.add_parser("resolve", help="Resolve a comment")
    p_resolve.add_argument("file_id")
    p_resolve.add_argument("--comment-id", required=True)

    # comments edit
    p_edit = sub.add_parser("edit", help="Edit a comment")
    p_edit.add_argument("file_id")
    p_edit.add_argument("--comment-id", required=True)
    p_edit.add_argument("--content", required=True)

    # comments delete
    p_delete = sub.add_parser("delete", help="Delete a comment")
    p_delete.add_argument("file_id")
    p_delete.add_argument("--comment-id", required=True)

    # comments edit-reply
    p_edit_reply = sub.add_parser("edit-reply", help="Edit a reply")
    p_edit_reply.add_argument("file_id")
    p_edit_reply.add_argument("--comment-id", required=True)
    p_edit_reply.add_argument("--reply-id", required=True)
    p_edit_reply.add_argument("--content", required=True)

    # comments delete-reply
    p_delete_reply = sub.add_parser("delete-reply", help="Delete a reply")
    p_delete_reply.add_argument("file_id")
    p_delete_reply.add_argument("--comment-id", required=True)
    p_delete_reply.add_argument("--reply-id", required=True)


def handle(args):
    action = args.action
    if action == "list":
        cmd_list(args)
    elif action == "create":
        cmd_create(args)
    elif action == "reply":
        cmd_reply(args)
    elif action == "resolve":
        cmd_resolve(args)
    elif action == "edit":
        cmd_edit(args)
    elif action == "delete":
        cmd_delete(args)
    elif action == "edit-reply":
        cmd_edit_reply(args)
    elif action == "delete-reply":
        cmd_delete_reply(args)


def cmd_list(args):
    try:
        service = get_service("drive")
        result = comments.read_comments(service, args.file_id)
        success(result)
    except Exception as e:
        error(str(e))


def cmd_create(args):
    try:
        service = get_service("drive")
        docs_service = get_service("docs") if args.service == "docs" else None
        result = comments.create_comment(
            service,
            args.file_id,
            args.content,
            quoted_text=args.quoted_text or "",
            docs_service=docs_service,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_reply(args):
    try:
        service = get_service("drive")
        result = comments.reply_to_comment(
            service, args.file_id, args.comment_id, args.content
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_resolve(args):
    try:
        service = get_service("drive")
        result = comments.resolve_comment(service, args.file_id, args.comment_id)
        success(result)
    except Exception as e:
        error(str(e))


def cmd_edit(args):
    try:
        service = get_service("drive")
        result = comments.edit_comment(
            service, args.file_id, args.comment_id, args.content
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_delete(args):
    try:
        service = get_service("drive")
        result = comments.delete_comment(service, args.file_id, args.comment_id)
        success(result)
    except Exception as e:
        error(str(e))


def cmd_edit_reply(args):
    try:
        service = get_service("drive")
        result = comments.edit_reply(
            service, args.file_id, args.comment_id, args.reply_id, args.content
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_delete_reply(args):
    try:
        service = get_service("drive")
        result = comments.delete_reply(
            service, args.file_id, args.comment_id, args.reply_id
        )
        success(result)
    except Exception as e:
        error(str(e))
