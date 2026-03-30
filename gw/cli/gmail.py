"""
Gmail CLI commands for the gw CLI.

Provides argparse-based commands that map to gmail service functions.
"""

import json

from gw.auth import get_service
from gw.output import success, error
from gw.services import gmail


def register(subparsers):
    """Register the gmail subparser and all its commands."""
    gmail_parser = subparsers.add_parser("gmail", help="Gmail operations")
    gmail_sub = gmail_parser.add_subparsers(dest="gmail_command")

    # --- search ---
    p = gmail_sub.add_parser("search", help="Search Gmail messages")
    p.add_argument("query", help="Gmail search query")
    p.add_argument("--max-results", type=int, default=10, help="Max results (default: 10)")
    p.add_argument("--page-token", default=None, help="Pagination token")
    p.set_defaults(func=cmd_search)

    # --- read ---
    p = gmail_sub.add_parser("read", help="Read a single Gmail message")
    p.add_argument("message_id", help="Gmail message ID")
    p.add_argument("--format", choices=["full", "metadata", "minimal"], default="full", help="Message format")
    p.set_defaults(func=cmd_read)

    # --- read-thread ---
    p = gmail_sub.add_parser("read-thread", help="Read a Gmail thread")
    p.add_argument("thread_id", help="Gmail thread ID")
    p.add_argument("--format", choices=["full", "metadata", "minimal"], default="full", help="Message format")
    p.set_defaults(func=cmd_read_thread)

    # --- batch-read ---
    p = gmail_sub.add_parser("batch-read", help="Read multiple Gmail messages")
    p.add_argument("ids", nargs="+", help="Gmail message IDs")
    p.add_argument("--format", choices=["full", "metadata", "minimal"], default="full", help="Message format")
    p.set_defaults(func=cmd_batch_read)

    # --- batch-read-threads ---
    p = gmail_sub.add_parser("batch-read-threads", help="Read multiple Gmail threads")
    p.add_argument("ids", nargs="+", help="Gmail thread IDs")
    p.add_argument("--format", choices=["full", "metadata", "minimal"], default="full", help="Message format")
    p.set_defaults(func=cmd_batch_read_threads)

    # --- read-attachment ---
    p = gmail_sub.add_parser("read-attachment", help="Download a Gmail attachment")
    p.add_argument("message_id", help="Gmail message ID")
    p.add_argument("attachment_id", help="Attachment ID")
    p.add_argument("--save-to", default=None, help="File path to save attachment")
    p.set_defaults(func=cmd_read_attachment)

    # --- draft ---
    p = gmail_sub.add_parser("draft", help="Create a Gmail draft")
    p.add_argument("--to", default=None, help="Recipient email")
    p.add_argument("--subject", required=True, help="Email subject")
    p.add_argument("--body", required=True, help="Email body")
    p.add_argument("--cc", default=None, help="CC email")
    p.add_argument("--bcc", default=None, help="BCC email")
    p.add_argument("--html", action="store_true", help="Send as HTML")
    p.add_argument("--attach", action="append", default=None, help="Attachment file path (repeatable)")
    p.add_argument("--in-reply-to", default=None, help="Message-ID to reply to")
    p.add_argument("--thread-id", default=None, help="Thread ID for replies")
    p.set_defaults(func=cmd_draft)

    # --- send ---
    p = gmail_sub.add_parser("send", help="Send a Gmail message")
    p.add_argument("--to", required=True, help="Recipient email")
    p.add_argument("--subject", required=True, help="Email subject")
    p.add_argument("--body", required=True, help="Email body")
    p.add_argument("--cc", default=None, help="CC email")
    p.add_argument("--bcc", default=None, help="BCC email")
    p.add_argument("--html", action="store_true", help="Send as HTML")
    p.add_argument("--attach", action="append", default=None, help="Attachment file path (repeatable)")
    p.set_defaults(func=cmd_send)

    # --- labels ---
    p = gmail_sub.add_parser("labels", help="List Gmail labels")
    p.set_defaults(func=cmd_labels)

    # --- label-manage ---
    p = gmail_sub.add_parser("label-manage", help="Create, update, or delete a label")
    p.add_argument("--action", required=True, choices=["create", "update", "delete"], help="Action to perform")
    p.add_argument("--name", default=None, help="Label name")
    p.add_argument("--label-id", default=None, help="Label ID (for update/delete)")
    p.add_argument("--visibility", choices=["show", "hide"], default="show", help="Label visibility")
    p.set_defaults(func=cmd_label_manage)

    # --- label-modify ---
    p = gmail_sub.add_parser("label-modify", help="Add/remove labels from messages")
    p.add_argument("message_ids", nargs="+", help="Message IDs to modify")
    p.add_argument("--add", action="append", default=None, help="Label ID to add (repeatable)")
    p.add_argument("--remove", action="append", default=None, help="Label ID to remove (repeatable)")
    p.set_defaults(func=cmd_label_modify)

    # --- filters ---
    p = gmail_sub.add_parser("filters", help="List Gmail filters")
    p.set_defaults(func=cmd_filters)

    # --- create-filter ---
    p = gmail_sub.add_parser("create-filter", help="Create a Gmail filter")
    p.add_argument("--criteria", required=True, help="Filter criteria as JSON string")
    p.add_argument("--actions", required=True, help="Filter actions as JSON string")
    p.set_defaults(func=cmd_create_filter)

    # --- delete-filter ---
    p = gmail_sub.add_parser("delete-filter", help="Delete a Gmail filter")
    p.add_argument("filter_id", help="Filter ID to delete")
    p.set_defaults(func=cmd_delete_filter)


# ---------------------------------------------------------------------------
# Command handlers
# ---------------------------------------------------------------------------


def cmd_search(args):
    try:
        service = get_service("gmail")
        result = gmail.search_messages(
            service, args.query,
            max_results=args.max_results,
            page_token=args.page_token,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_read(args):
    try:
        service = get_service("gmail")
        result = gmail.get_message_content(service, args.message_id, format=args.format)
        success(result)
    except Exception as e:
        error(str(e))


def cmd_read_thread(args):
    try:
        service = get_service("gmail")
        result = gmail.get_thread_content(service, args.thread_id)
        success(result)
    except Exception as e:
        error(str(e))


def cmd_batch_read(args):
    try:
        service = get_service("gmail")
        result = gmail.get_messages_content_batch(service, args.ids, format=args.format)
        success(result)
    except Exception as e:
        error(str(e))


def cmd_batch_read_threads(args):
    try:
        service = get_service("gmail")
        result = gmail.get_threads_content_batch(service, args.ids)
        success(result)
    except Exception as e:
        error(str(e))


def cmd_read_attachment(args):
    try:
        service = get_service("gmail")
        result = gmail.get_attachment_content(
            service, args.message_id, args.attachment_id, save_to=args.save_to,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_draft(args):
    try:
        service = get_service("gmail")
        result = gmail.draft_message(
            service,
            subject=args.subject,
            body=args.body,
            to=args.to,
            cc=args.cc,
            bcc=args.bcc,
            body_format="html" if args.html else "plain",
            thread_id=args.thread_id,
            in_reply_to=args.in_reply_to,
            attachment_paths=args.attach,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_send(args):
    try:
        service = get_service("gmail")
        result = gmail.send_message(
            service,
            to=args.to,
            subject=args.subject,
            body=args.body,
            cc=args.cc,
            bcc=args.bcc,
            body_format="html" if args.html else "plain",
            attachment_paths=args.attach,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_labels(args):
    try:
        service = get_service("gmail")
        result = gmail.list_labels(service)
        success(result)
    except Exception as e:
        error(str(e))


def cmd_label_manage(args):
    try:
        service = get_service("gmail")
        label_list_vis = "labelShow" if args.visibility == "show" else "labelHide"
        msg_list_vis = args.visibility
        result = gmail.manage_label(
            service,
            action=args.action,
            name=args.name,
            label_id=args.label_id,
            label_list_visibility=label_list_vis,
            message_list_visibility=msg_list_vis,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_label_modify(args):
    try:
        service = get_service("gmail")
        result = gmail.modify_message_labels(
            service,
            message_ids=args.message_ids,
            add_label_ids=args.add,
            remove_label_ids=args.remove,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_filters(args):
    try:
        service = get_service("gmail")
        result = gmail.list_filters(service)
        success(result)
    except Exception as e:
        error(str(e))


def cmd_create_filter(args):
    try:
        service = get_service("gmail")
        criteria = json.loads(args.criteria)
        actions = json.loads(args.actions)
        result = gmail.create_filter(service, criteria, actions)
        success(result)
    except json.JSONDecodeError as e:
        error(f"Invalid JSON: {e}")
    except Exception as e:
        error(str(e))


def cmd_delete_filter(args):
    try:
        service = get_service("gmail")
        result = gmail.delete_filter(service, args.filter_id)
        success(result)
    except Exception as e:
        error(str(e))
