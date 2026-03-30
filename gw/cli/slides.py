"""CLI commands for Google Slides."""

import json

from gw.auth import get_service
from gw.output import success, error
from gw.services import slides


def register(subparsers):
    parser = subparsers.add_parser("slides", help="Google Slides operations")
    sub = parser.add_subparsers(dest="action", required=True)

    # slides create
    p_create = sub.add_parser("create", help="Create a new presentation")
    p_create.add_argument("--title", default="Untitled Presentation")

    # slides read
    p_read = sub.add_parser("read", help="Read a presentation's details")
    p_read.add_argument("file_id")

    # slides page
    p_page = sub.add_parser("page", help="Get details about a specific slide")
    p_page.add_argument("file_id")
    p_page.add_argument("page_id")

    # slides thumbnail
    p_thumb = sub.add_parser("thumbnail", help="Generate a slide thumbnail URL")
    p_thumb.add_argument("file_id")
    p_thumb.add_argument("page_id")
    p_thumb.add_argument("--size", choices=["SMALL", "MEDIUM", "LARGE"], default="MEDIUM")

    # slides batch-update
    p_batch = sub.add_parser("batch-update", help="Apply batch updates to a presentation")
    p_batch.add_argument("file_id")
    p_batch.add_argument("--requests", required=True, help="JSON array of Slides API request objects")


def handle(args):
    action = args.action
    if action == "create":
        cmd_create(args)
    elif action == "read":
        cmd_read(args)
    elif action == "page":
        cmd_page(args)
    elif action == "thumbnail":
        cmd_thumbnail(args)
    elif action == "batch-update":
        cmd_batch_update(args)


def cmd_create(args):
    try:
        service = get_service("slides")
        result = slides.create_presentation(service, title=args.title)
        success(result)
    except Exception as e:
        error(str(e))


def cmd_read(args):
    try:
        service = get_service("slides")
        result = slides.get_presentation(service, file_id=args.file_id)
        success(result)
    except Exception as e:
        error(str(e))


def cmd_page(args):
    try:
        service = get_service("slides")
        result = slides.get_page(
            service, file_id=args.file_id, page_object_id=args.page_id
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_thumbnail(args):
    try:
        service = get_service("slides")
        result = slides.get_page_thumbnail(
            service,
            file_id=args.file_id,
            page_object_id=args.page_id,
            thumbnail_size=args.size,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_batch_update(args):
    try:
        requests_data = json.loads(args.requests)
        service = get_service("slides")
        result = slides.batch_update_presentation(
            service, file_id=args.file_id, requests=requests_data
        )
        success(result)
    except json.JSONDecodeError as e:
        error(f"Invalid JSON for --requests: {e}")
    except Exception as e:
        error(str(e))
