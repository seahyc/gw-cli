"""
CLI sub-commands for Google Drive.

Registers the ``drive`` subparser and all its sub-commands.
"""

from __future__ import annotations

import argparse

from gw.auth import get_service
from gw.output import success, error
from gw.services import drive


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------

def cmd_search(args):
    try:
        service = get_service("drive")
        result = drive.search_files(
            service,
            args.query,
            page_size=args.max_results,
            drive_id=getattr(args, "drive_id", None),
            corpora=getattr(args, "corpora", None),
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_list(args):
    try:
        service = get_service("drive")
        result = drive.list_items(
            service,
            folder_id=args.folder_id,
            page_size=args.max_results,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_read(args):
    try:
        service = get_service("drive")
        result = drive.get_file_content(service, file_id=args.file_id)
        success(result)
    except Exception as e:
        error(str(e))


def cmd_download_url(args):
    try:
        service = get_service("drive")
        result = drive.get_file_download_url(
            service,
            file_id=args.file_id,
            export_format=getattr(args, "export_format", None),
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_check_public(args):
    try:
        service = get_service("drive")
        result = drive.check_public_access(service, file_name=args.file_name)
        success(result)
    except Exception as e:
        error(str(e))


def cmd_create(args):
    try:
        service = get_service("drive")
        if args.type == "folder":
            result = drive.create_folder(
                service,
                name=args.name,
                parent_folder_id=getattr(args, "parent", None),
            )
        else:
            # file
            content = None
            if args.content:
                with open(args.content) as f:
                    content = f.read()
            result = drive.create_file(
                service,
                file_name=args.name,
                content=content,
                folder_id=getattr(args, "parent", None) or "root",
                mime_type=getattr(args, "mime_type", None) or "text/plain",
            )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_upload(args):
    try:
        service = get_service("drive")
        result = drive.upload_file(
            service,
            local_path=args.local_path,
            name=getattr(args, "name", None),
            parent_id=getattr(args, "parent", None),
            mime_type=getattr(args, "mime_type", None),
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_copy(args):
    try:
        service = get_service("drive")
        result = drive.copy_file(
            service,
            file_id=args.file_id,
            new_name=getattr(args, "name", None),
            parent_folder_id=getattr(args, "parent", None),
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_move(args):
    try:
        service = get_service("drive")
        result = drive.move_file(
            service,
            file_id=args.file_id,
            destination_folder_id=args.to,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_export(args):
    try:
        service = get_service("drive")
        result = drive.export_file(
            service,
            file_id=args.file_id,
            mime_type=args.mime_type,
            save_to_drive=getattr(args, "save_to_drive", False),
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_trash(args):
    try:
        service = get_service("drive")
        result = drive.trash_file(
            service,
            file_id=args.file_id,
            untrash=getattr(args, "untrash", False),
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_delete(args):
    try:
        service = get_service("drive")
        result = drive.delete_file(service, file_id=args.file_id)
        success(result)
    except Exception as e:
        error(str(e))


def cmd_permissions(args):
    try:
        service = get_service("drive")
        result = drive.get_file_permissions(service, file_id=args.file_id)
        success(result)
    except Exception as e:
        error(str(e))


def cmd_share(args):
    try:
        service = get_service("drive")
        result = drive.share_file(
            service,
            file_id=args.file_id,
            email=args.email,
            role=args.role,
            share_type=getattr(args, "type", "user"),
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_revisions(args):
    try:
        service = get_service("drive")
        result = drive.list_revisions(
            service,
            file_id=args.file_id,
            max_results=getattr(args, "max_results", 20),
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_shareable_link(args):
    try:
        service = get_service("drive")
        result = drive.get_shareable_link(service, file_id=args.file_id)
        success(result)
    except Exception as e:
        error(str(e))


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------

def register(subparsers: argparse._SubParsersAction) -> None:
    """Register the ``drive`` command and all its sub-commands."""
    drive_parser = subparsers.add_parser("drive", help="Google Drive operations")
    drive_sub = drive_parser.add_subparsers(dest="drive_command", required=True)

    # -- search ---------------------------------------------------------------
    p = drive_sub.add_parser("search", help="Search for files/folders")
    p.add_argument("query", help="Search query (free text or Drive query syntax)")
    p.add_argument("--max-results", type=int, default=10)
    p.add_argument("--drive-id", default=None, help="Shared drive ID")
    p.add_argument("--corpora", default=None, help="Corpora: user, drive, allDrives")
    p.set_defaults(func=cmd_search)

    # -- list -----------------------------------------------------------------
    p = drive_sub.add_parser("list", help="List files in a folder")
    p.add_argument("--folder-id", default="root", help="Folder ID (default: root)")
    p.add_argument("--max-results", type=int, default=100)
    p.set_defaults(func=cmd_list)

    # -- read -----------------------------------------------------------------
    p = drive_sub.add_parser("read", help="Read file content")
    p.add_argument("file_id", help="File ID")
    p.set_defaults(func=cmd_read)

    # -- download-url ---------------------------------------------------------
    p = drive_sub.add_parser("download-url", help="Get download URL for a file")
    p.add_argument("file_id", help="File ID")
    p.add_argument("--export-format", default=None, help="Export format: pdf, docx, xlsx, csv, pptx")
    p.set_defaults(func=cmd_download_url)

    # -- check-public ---------------------------------------------------------
    p = drive_sub.add_parser("check-public", help="Check if a file has public link sharing")
    p.add_argument("file_name", help="File name to search for")
    p.set_defaults(func=cmd_check_public)

    # -- create ---------------------------------------------------------------
    p = drive_sub.add_parser("create", help="Create a file or folder")
    p.add_argument("--name", required=True, help="Name of the file or folder")
    p.add_argument("--type", required=True, choices=["file", "folder"], help="Create a file or folder")
    p.add_argument("--parent", default=None, help="Parent folder ID")
    p.add_argument("--content", default=None, help="Path to local file for content (files only)")
    p.add_argument("--mime-type", default=None, help="MIME type (files only)")
    p.set_defaults(func=cmd_create)

    # -- upload ---------------------------------------------------------------
    p = drive_sub.add_parser("upload", help="Upload a local file")
    p.add_argument("local_path", help="Path to local file")
    p.add_argument("--name", default=None, help="Override file name")
    p.add_argument("--parent", default=None, help="Parent folder ID")
    p.add_argument("--mime-type", default=None, help="MIME type override")
    p.set_defaults(func=cmd_upload)

    # -- copy -----------------------------------------------------------------
    p = drive_sub.add_parser("copy", help="Copy a file")
    p.add_argument("file_id", help="File ID to copy")
    p.add_argument("--name", default=None, help="Name for the copy")
    p.add_argument("--parent", default=None, help="Parent folder ID for the copy")
    p.set_defaults(func=cmd_copy)

    # -- move -----------------------------------------------------------------
    p = drive_sub.add_parser("move", help="Move a file to a different folder")
    p.add_argument("file_id", help="File ID to move")
    p.add_argument("--to", required=True, help="Destination folder ID")
    p.set_defaults(func=cmd_move)

    # -- export ---------------------------------------------------------------
    p = drive_sub.add_parser("export", help="Export a Google Workspace file")
    p.add_argument("file_id", help="File ID to export")
    p.add_argument("--mime-type", required=True, help="Target format: pdf, txt, csv, docx, xlsx, pptx, html, rtf, epub")
    p.add_argument("--save-to-drive", action="store_true", help="Save exported file back to Drive")
    p.set_defaults(func=cmd_export)

    # -- trash ----------------------------------------------------------------
    p = drive_sub.add_parser("trash", help="Trash or untrash a file")
    p.add_argument("file_id", help="File ID")
    p.add_argument("--untrash", action="store_true", help="Restore from trash instead")
    p.set_defaults(func=cmd_trash)

    # -- delete ---------------------------------------------------------------
    p = drive_sub.add_parser("delete", help="Permanently delete a file")
    p.add_argument("file_id", help="File ID to delete")
    p.set_defaults(func=cmd_delete)

    # -- permissions ----------------------------------------------------------
    p = drive_sub.add_parser("permissions", help="View file permissions and sharing info")
    p.add_argument("file_id", help="File ID")
    p.set_defaults(func=cmd_permissions)

    # -- share ----------------------------------------------------------------
    p = drive_sub.add_parser("share", help="Share a file with a user/group")
    p.add_argument("file_id", help="File ID to share")
    p.add_argument("--email", required=True, help="Email address to share with")
    p.add_argument("--role", default="reader", choices=["reader", "writer", "commenter"], help="Permission role")
    p.add_argument("--type", default="user", choices=["user", "group", "domain", "anyone"], help="Share type")
    p.set_defaults(func=cmd_share)

    # -- revisions ------------------------------------------------------------
    p = drive_sub.add_parser("revisions", help="List file revisions")
    p.add_argument("file_id", help="File ID")
    p.add_argument("--max-results", type=int, default=20)
    p.set_defaults(func=cmd_revisions)

    # -- shareable-link -------------------------------------------------------
    p = drive_sub.add_parser("shareable-link", help="Get shareable link for a file")
    p.add_argument("file_id", help="File ID")
    p.set_defaults(func=cmd_shareable_link)
