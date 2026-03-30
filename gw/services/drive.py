"""
Google Drive service layer.

Synchronous wrappers around the Google Drive API, ported from the MCP
tool definitions in ``gdrive/drive_tools.py``.
"""

import io
import logging
import mimetypes
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload, MediaIoBaseUpload

from gw.services._helpers.drive_helpers import (
    DRIVE_QUERY_PATTERNS,
    build_drive_list_params,
    check_public_link_permission,
    format_permission_info,
    get_drive_image_url,
    resolve_drive_item,
    resolve_folder_id,
    validate_share_role,
    validate_share_type,
)

logger = logging.getLogger(__name__)

DOWNLOAD_CHUNK_SIZE_BYTES = 256 * 1024  # 256 KB
UPLOAD_CHUNK_SIZE_BYTES = 5 * 1024 * 1024  # 5 MB (Google recommended minimum)


# ---------------------------------------------------------------------------
# Search / Read
# ---------------------------------------------------------------------------

def search_files(
    service,
    query: str = "",
    page_size: int = 10,
    drive_id: Optional[str] = None,
    include_items_from_all_drives: bool = True,
    corpora: Optional[str] = None,
) -> str:
    """Search for files/folders in Google Drive."""
    logger.info(f"[search_files] Query: '{query}'")

    is_structured_query = any(pattern.search(query) for pattern in DRIVE_QUERY_PATTERNS)

    if is_structured_query:
        final_query = query
    else:
        escaped_query = query.replace("'", "\\'")
        final_query = f"fullText contains '{escaped_query}'"

    list_params = build_drive_list_params(
        query=final_query,
        page_size=page_size,
        drive_id=drive_id,
        include_items_from_all_drives=include_items_from_all_drives,
        corpora=corpora,
    )

    results = service.files().list(**list_params).execute()
    files = results.get("files", [])
    if not files:
        return f"No files found for '{query}'."

    parts = [f"Found {len(files)} files matching '{query}':"]
    for item in files:
        size_str = f", Size: {item.get('size', 'N/A')}" if "size" in item else ""
        parts.append(
            f'- Name: "{item["name"]}" (ID: {item["id"]}, Type: {item["mimeType"]}'
            f'{size_str}, Modified: {item.get("modifiedTime", "N/A")}) '
            f'Link: {item.get("webViewLink", "#")}'
        )
    return "\n".join(parts)


def get_file_content(
    service,
    file_id: str = "",
) -> str:
    """Retrieve file content by ID."""
    logger.info(f"[get_file_content] File ID: '{file_id}'")

    resolved_file_id, file_metadata = resolve_drive_item(
        service, file_id, extra_fields="name, webViewLink",
    )
    file_id = resolved_file_id
    mime_type = file_metadata.get("mimeType", "")
    file_name = file_metadata.get("name", "Unknown File")

    export_mime_type = {
        "application/vnd.google-apps.document": "text/plain",
        "application/vnd.google-apps.spreadsheet": "text/csv",
        "application/vnd.google-apps.presentation": "text/plain",
    }.get(mime_type)

    request_obj = (
        service.files().export_media(fileId=file_id, mimeType=export_mime_type)
        if export_mime_type
        else service.files().get_media(fileId=file_id)
    )

    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request_obj)
    done = False
    while not done:
        status, done = downloader.next_chunk()

    file_content_bytes = fh.getvalue()

    office_mime_types = {
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    }

    if mime_type in office_mime_types:
        try:
            body_text = file_content_bytes.decode("utf-8")
        except UnicodeDecodeError:
            body_text = (
                f"[Binary or unsupported text encoding for mimeType '{mime_type}' - "
                f"{len(file_content_bytes)} bytes]"
            )
    else:
        try:
            body_text = file_content_bytes.decode("utf-8")
        except UnicodeDecodeError:
            body_text = (
                f"[Binary or unsupported text encoding for mimeType '{mime_type}' - "
                f"{len(file_content_bytes)} bytes]"
            )

    header = (
        f'File: "{file_name}" (ID: {file_id}, Type: {mime_type})\n'
        f"Link: {file_metadata.get('webViewLink', '#')}\n\n--- CONTENT ---\n"
    )
    return header + body_text


def get_file_download_url(
    service,
    file_id: str = "",
    export_format: Optional[str] = None,
) -> str:
    """Get a download URL / export a Drive file to bytes."""
    logger.info(f"[get_file_download_url] File ID: '{file_id}', Export format: {export_format}")

    resolved_file_id, file_metadata = resolve_drive_item(
        service, file_id, extra_fields="name, webViewLink, mimeType",
    )
    file_id = resolved_file_id
    mime_type = file_metadata.get("mimeType", "")
    file_name = file_metadata.get("name", "Unknown File")

    export_mime_type = None
    output_filename = file_name
    output_mime_type = mime_type

    if mime_type == "application/vnd.google-apps.document":
        if export_format == "docx":
            export_mime_type = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            output_mime_type = export_mime_type
            if not output_filename.endswith(".docx"):
                output_filename = f"{Path(output_filename).stem}.docx"
        else:
            export_mime_type = "application/pdf"
            output_mime_type = export_mime_type
            if not output_filename.endswith(".pdf"):
                output_filename = f"{Path(output_filename).stem}.pdf"
    elif mime_type == "application/vnd.google-apps.spreadsheet":
        if export_format == "csv":
            export_mime_type = "text/csv"
            output_mime_type = export_mime_type
            if not output_filename.endswith(".csv"):
                output_filename = f"{Path(output_filename).stem}.csv"
        else:
            export_mime_type = (
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            output_mime_type = export_mime_type
            if not output_filename.endswith(".xlsx"):
                output_filename = f"{Path(output_filename).stem}.xlsx"
    elif mime_type == "application/vnd.google-apps.presentation":
        if export_format == "pptx":
            export_mime_type = "application/vnd.openxmlformats-officedocument.presentationml.presentation"
            output_mime_type = export_mime_type
            if not output_filename.endswith(".pptx"):
                output_filename = f"{Path(output_filename).stem}.pptx"
        else:
            export_mime_type = "application/pdf"
            output_mime_type = export_mime_type
            if not output_filename.endswith(".pdf"):
                output_filename = f"{Path(output_filename).stem}.pdf"

    request_obj = (
        service.files().export_media(fileId=file_id, mimeType=export_mime_type)
        if export_mime_type
        else service.files().get_media(fileId=file_id)
    )

    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request_obj)
    done = False
    while not done:
        status, done = downloader.next_chunk()

    file_content_bytes = fh.getvalue()
    size_bytes = len(file_content_bytes)
    size_kb = size_bytes / 1024 if size_bytes else 0

    result_lines = [
        "File downloaded successfully!",
        f"File: {file_name}",
        f"File ID: {file_id}",
        f"Size: {size_kb:.1f} KB ({size_bytes} bytes)",
        f"MIME Type: {output_mime_type}",
    ]

    if export_mime_type:
        result_lines.append(f"\nNote: Google native file exported to {output_mime_type} format.")

    return "\n".join(result_lines)


def list_items(
    service,
    folder_id: str = "root",
    page_size: int = 100,
    drive_id: Optional[str] = None,
    include_items_from_all_drives: bool = True,
    corpora: Optional[str] = None,
) -> str:
    """List files/folders in a Drive folder."""
    logger.info(f"[list_items] Folder ID: '{folder_id}'")

    resolved_folder_id = resolve_folder_id(service, folder_id)
    final_query = f"'{resolved_folder_id}' in parents and trashed=false"

    list_params = build_drive_list_params(
        query=final_query,
        page_size=page_size,
        drive_id=drive_id,
        include_items_from_all_drives=include_items_from_all_drives,
        corpora=corpora,
    )

    results = service.files().list(**list_params).execute()
    files = results.get("files", [])
    if not files:
        return f"No items found in folder '{folder_id}'."

    parts = [f"Found {len(files)} items in folder '{folder_id}':"]
    for item in files:
        size_str = f", Size: {item.get('size', 'N/A')}" if "size" in item else ""
        parts.append(
            f'- Name: "{item["name"]}" (ID: {item["id"]}, Type: {item["mimeType"]}'
            f'{size_str}, Modified: {item.get("modifiedTime", "N/A")}) '
            f'Link: {item.get("webViewLink", "#")}'
        )
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Create / Upload
# ---------------------------------------------------------------------------

def create_file(
    service,
    file_name: str = "",
    content: Optional[str] = None,
    folder_id: str = "root",
    mime_type: str = "text/plain",
) -> str:
    """Create a file in Drive from text content."""
    logger.info(f"[create_file] Name: {file_name}, Folder: {folder_id}")

    if not content:
        raise Exception("You must provide 'content'.")

    resolved_folder_id = resolve_folder_id(service, folder_id)

    file_metadata: Dict[str, Any] = {
        "name": file_name,
        "parents": [resolved_folder_id],
        "mimeType": mime_type,
    }

    file_data = content.encode("utf-8")
    media = MediaIoBaseUpload(
        io.BytesIO(file_data), mimetype=mime_type, resumable=True,
    )

    created_file = (
        service.files()
        .create(
            body=file_metadata,
            media_body=media,
            fields="id, name, webViewLink",
            supportsAllDrives=True,
        )
        .execute()
    )

    link = created_file.get("webViewLink", "No link available")
    new_id = created_file.get("id", "N/A")
    logger.info(f"Successfully created file. Link: {link}")
    return f"File created. ID: {new_id}\nLink: {link}"


def create_folder(
    service,
    name: str = "",
    parent_folder_id: Optional[str] = None,
) -> str:
    """Create a new folder in Drive."""
    logger.info(f"[create_folder] Name: '{name}'")

    folder_metadata: Dict[str, Any] = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
    }

    if parent_folder_id:
        resolved_parent = resolve_folder_id(service, parent_folder_id)
        folder_metadata["parents"] = [resolved_parent]

    created_folder = (
        service.files()
        .create(
            body=folder_metadata,
            fields="id, name, webViewLink",
            supportsAllDrives=True,
        )
        .execute()
    )

    folder_id = created_folder.get("id", "N/A")
    link = created_folder.get("webViewLink", "#")
    logger.info(f"[create_folder] Created folder '{name}' (ID: {folder_id}).")
    return (
        f"Successfully created folder '{name}'.\n"
        f"Folder ID: {folder_id}\n"
        f"Link: {link}"
    )


def upload_file(
    service,
    local_path: str,
    name: Optional[str] = None,
    parent_id: Optional[str] = None,
    mime_type: Optional[str] = None,
) -> str:
    """Upload a local file to Google Drive."""
    path = Path(local_path)
    if not path.exists():
        raise FileNotFoundError(f"Local file not found: {local_path}")

    upload_name = name or path.name
    if not mime_type:
        mime_type, _ = mimetypes.guess_type(str(path))
        mime_type = mime_type or "application/octet-stream"

    file_metadata: Dict[str, Any] = {"name": upload_name}
    if parent_id:
        resolved_parent = resolve_folder_id(service, parent_id)
        file_metadata["parents"] = [resolved_parent]

    media = MediaFileUpload(
        str(path),
        mimetype=mime_type,
        resumable=True,
        chunksize=UPLOAD_CHUNK_SIZE_BYTES,
    )

    created_file = (
        service.files()
        .create(
            body=file_metadata,
            media_body=media,
            fields="id, name, webViewLink",
            supportsAllDrives=True,
        )
        .execute()
    )

    new_id = created_file.get("id", "N/A")
    link = created_file.get("webViewLink", "#")
    size_bytes = os.path.getsize(local_path)
    size_kb = size_bytes / 1024

    logger.info(f"[upload_file] Uploaded '{upload_name}' ({size_kb:.1f} KB) as {new_id}.")
    return (
        f"Uploaded '{upload_name}' ({size_kb:.1f} KB).\n"
        f"File ID: {new_id}\n"
        f"Link: {link}"
    )


# ---------------------------------------------------------------------------
# Permissions / Sharing
# ---------------------------------------------------------------------------

def get_file_permissions(
    service,
    file_id: str = "",
) -> str:
    """Get detailed file metadata including sharing permissions and URLs."""
    logger.info(f"[get_file_permissions] Checking file {file_id}")

    resolved_file_id, _ = resolve_drive_item(service, file_id)
    file_id = resolved_file_id

    file_metadata = (
        service.files()
        .get(
            fileId=file_id,
            fields="id, name, mimeType, size, modifiedTime, owners, "
            "permissions(id, type, role, emailAddress, domain, expirationTime, permissionDetails), "
            "webViewLink, webContentLink, shared, sharingUser, viewersCanCopyContent",
            supportsAllDrives=True,
        )
        .execute()
    )

    output_parts = [
        f"File: {file_metadata.get('name', 'Unknown')}",
        f"ID: {file_id}",
        f"Type: {file_metadata.get('mimeType', 'Unknown')}",
        f"Size: {file_metadata.get('size', 'N/A')} bytes",
        f"Modified: {file_metadata.get('modifiedTime', 'N/A')}",
        "",
        "Sharing Status:",
        f"  Shared: {file_metadata.get('shared', False)}",
    ]

    sharing_user = file_metadata.get("sharingUser")
    if sharing_user:
        output_parts.append(
            f"  Shared by: {sharing_user.get('displayName', 'Unknown')} "
            f"({sharing_user.get('emailAddress', 'Unknown')})"
        )

    permissions = file_metadata.get("permissions", [])
    if permissions:
        output_parts.append(f"  Number of permissions: {len(permissions)}")
        output_parts.append("  Permissions:")
        for perm in permissions:
            output_parts.append(f"    - {format_permission_info(perm)}")
    else:
        output_parts.append("  No additional permissions (private file)")

    output_parts.extend([
        "",
        "URLs:",
        f"  View Link: {file_metadata.get('webViewLink', 'N/A')}",
    ])

    web_content_link = file_metadata.get("webContentLink")
    if web_content_link:
        output_parts.append(f"  Direct Download Link: {web_content_link}")

    has_public_link = check_public_link_permission(permissions)
    if has_public_link:
        output_parts.extend([
            "",
            "This file is shared with 'Anyone with the link' - it can be inserted into Google Docs",
        ])
    else:
        output_parts.extend([
            "",
            "This file is NOT shared with 'Anyone with the link'",
            "   To fix: Right-click the file in Google Drive -> Share -> Anyone with the link -> Viewer",
        ])

    return "\n".join(output_parts)


def check_public_access(
    service,
    file_name: str = "",
) -> str:
    """Search for a file by name and check if it has public link sharing enabled."""
    logger.info(f"[check_public_access] Searching for {file_name}")

    escaped_name = file_name.replace("'", "\\'")
    query = f"name = '{escaped_name}'"

    list_params = {
        "q": query,
        "pageSize": 10,
        "fields": "files(id, name, mimeType, webViewLink)",
        "supportsAllDrives": True,
        "includeItemsFromAllDrives": True,
    }

    results = service.files().list(**list_params).execute()

    files = results.get("files", [])
    if not files:
        return f"No file found with name '{file_name}'"

    if len(files) > 1:
        output_parts = [f"Found {len(files)} files with name '{file_name}':"]
        for f in files:
            output_parts.append(f"  - {f['name']} (ID: {f['id']})")
        output_parts.append("\nChecking the first file...")
        output_parts.append("")
    else:
        output_parts = []

    file_id = files[0]["id"]
    resolved_file_id, _ = resolve_drive_item(service, file_id)
    file_id = resolved_file_id

    file_metadata = (
        service.files()
        .get(
            fileId=file_id,
            fields="id, name, mimeType, permissions, webViewLink, webContentLink, shared",
            supportsAllDrives=True,
        )
        .execute()
    )

    permissions = file_metadata.get("permissions", [])
    has_public_link = check_public_link_permission(permissions)

    output_parts.extend([
        f"File: {file_metadata['name']}",
        f"ID: {file_id}",
        f"Type: {file_metadata['mimeType']}",
        f"Shared: {file_metadata.get('shared', False)}",
        "",
    ])

    if has_public_link:
        output_parts.extend([
            "PUBLIC ACCESS ENABLED - This file can be inserted into Google Docs",
            f"Use with insert_doc_image_url: {get_drive_image_url(file_id)}",
        ])
    else:
        output_parts.extend([
            "NO PUBLIC ACCESS - Cannot insert into Google Docs",
            "Fix: Drive -> Share -> 'Anyone with the link' -> 'Viewer'",
        ])

    return "\n".join(output_parts)


def share_file(
    service,
    file_id: str,
    email: str,
    role: str = "reader",
    share_type: str = "user",
) -> str:
    """Create a permission on a Drive file to share it with a user/group."""
    validate_share_role(role)
    validate_share_type(share_type)

    resolved_file_id, file_metadata = resolve_drive_item(
        service, file_id, extra_fields="name",
    )
    file_id = resolved_file_id
    file_name = file_metadata.get("name", "Unknown File")

    permission_body: Dict[str, Any] = {
        "type": share_type,
        "role": role,
    }
    if share_type in ("user", "group"):
        permission_body["emailAddress"] = email
    elif share_type == "domain":
        permission_body["domain"] = email

    created_perm = (
        service.permissions()
        .create(
            fileId=file_id,
            body=permission_body,
            fields="id, type, role, emailAddress, domain",
            supportsAllDrives=True,
        )
        .execute()
    )

    perm_id = created_perm.get("id", "N/A")
    logger.info(f"[share_file] Shared '{file_name}' with {email} as {role}.")
    return (
        f"Shared '{file_name}' with {email} as {role}.\n"
        f"Permission ID: {perm_id}"
    )


def get_shareable_link(
    service,
    file_id: str = "",
) -> str:
    """Get shareable link and current permissions for a Drive file or folder."""
    logger.info(f"[get_shareable_link] File ID: '{file_id}'")

    resolved_file_id, _ = resolve_drive_item(service, file_id)
    file_id = resolved_file_id

    file_metadata = (
        service.files()
        .get(
            fileId=file_id,
            fields="id, name, mimeType, webViewLink, webContentLink, shared, "
            "permissions(id, type, role, emailAddress, domain, expirationTime)",
            supportsAllDrives=True,
        )
        .execute()
    )

    output_parts = [
        f"File: {file_metadata.get('name', 'Unknown')}",
        f"ID: {file_id}",
        f"Type: {file_metadata.get('mimeType', 'Unknown')}",
        f"Shared: {file_metadata.get('shared', False)}",
        "",
        "Links:",
        f"  View: {file_metadata.get('webViewLink', 'N/A')}",
    ]

    web_content_link = file_metadata.get("webContentLink")
    if web_content_link:
        output_parts.append(f"  Download: {web_content_link}")

    permissions = file_metadata.get("permissions", [])
    if permissions:
        output_parts.append("")
        output_parts.append("Current permissions:")
        for perm in permissions:
            output_parts.append(f"  - {format_permission_info(perm)}")

    return "\n".join(output_parts)


# ---------------------------------------------------------------------------
# Copy / Move / Trash / Delete
# ---------------------------------------------------------------------------

def copy_file(
    service,
    file_id: str = "",
    new_name: Optional[str] = None,
    parent_folder_id: Optional[str] = None,
) -> str:
    """Copy a Drive file."""
    logger.info(f"[copy_file] File ID: '{file_id}'")

    resolved_file_id, file_metadata = resolve_drive_item(
        service, file_id, extra_fields="name",
    )
    file_id = resolved_file_id
    original_name = file_metadata.get("name", "Unknown File")

    copy_body: Dict[str, Any] = {}
    if new_name:
        copy_body["name"] = new_name
    if parent_folder_id:
        resolved_parent = resolve_folder_id(service, parent_folder_id)
        copy_body["parents"] = [resolved_parent]

    copied_file = (
        service.files()
        .copy(
            fileId=file_id,
            body=copy_body,
            fields="id, name, webViewLink",
            supportsAllDrives=True,
        )
        .execute()
    )

    copy_name = copied_file.get("name", new_name or f"Copy of {original_name}")
    copy_id = copied_file.get("id", "N/A")
    link = copied_file.get("webViewLink", "#")

    logger.info(f"[copy_file] Successfully copied file. New ID: {copy_id}")
    return (
        f"Successfully copied '{original_name}' as '{copy_name}'\n"
        f"New File ID: {copy_id}\n"
        f"Link: {link}"
    )


def move_file(
    service,
    file_id: str = "",
    destination_folder_id: str = "",
) -> str:
    """Move a Drive file to a different folder."""
    logger.info(f"[move_file] File ID: '{file_id}', Destination: '{destination_folder_id}'")

    resolved_file_id, file_metadata = resolve_drive_item(
        service, file_id, extra_fields="name, parents",
    )
    file_id = resolved_file_id
    file_name = file_metadata.get("name", "Unknown File")
    current_parents = file_metadata.get("parents", [])

    resolved_destination = resolve_folder_id(service, destination_folder_id)
    previous_parents = ",".join(current_parents) if current_parents else ""

    (
        service.files()
        .update(
            fileId=file_id,
            addParents=resolved_destination,
            removeParents=previous_parents,
            fields="id, name, parents, webViewLink",
            supportsAllDrives=True,
        )
        .execute()
    )

    logger.info(f"[move_file] Moved '{file_name}' to folder '{resolved_destination}'.")
    return f"Moved '{file_name}' to folder '{destination_folder_id}'."


def trash_file(
    service,
    file_id: str = "",
    untrash: bool = False,
) -> str:
    """Move a file to trash, or restore it (untrash=True)."""
    action = "untrash" if untrash else "trash"
    logger.info(f"[trash_file] File ID: '{file_id}', Action: {action}")

    resolved_file_id, file_metadata = resolve_drive_item(
        service, file_id, extra_fields="name",
    )
    file_id = resolved_file_id
    file_name = file_metadata.get("name", "Unknown File")

    (
        service.files()
        .update(
            fileId=file_id,
            body={"trashed": not untrash},
            fields="id, name, trashed, webViewLink",
            supportsAllDrives=True,
        )
        .execute()
    )

    if untrash:
        return f"Restored '{file_name}' from trash."
    else:
        return f"Moved '{file_name}' to trash. Restore with --untrash."


def delete_file(
    service,
    file_id: str = "",
) -> str:
    """PERMANENTLY delete a Drive file (irreversible)."""
    logger.info(f"[delete_file] File ID: '{file_id}'")

    resolved_file_id, file_metadata = resolve_drive_item(
        service, file_id, extra_fields="name",
    )
    file_id = resolved_file_id
    file_name = file_metadata.get("name", "Unknown File")

    (
        service.files()
        .delete(fileId=file_id, supportsAllDrives=True)
        .execute()
    )

    logger.info(f"[delete_file] Permanently deleted '{file_name}' ({file_id}).")
    return f"Permanently deleted '{file_name}'."


# ---------------------------------------------------------------------------
# Revisions
# ---------------------------------------------------------------------------

def list_revisions(
    service,
    file_id: str = "",
    max_results: int = 20,
) -> str:
    """List revision history of a Drive file."""
    logger.info(f"[list_revisions] File ID: '{file_id}'")

    resolved_file_id, file_metadata = resolve_drive_item(
        service, file_id, extra_fields="name",
    )
    file_id = resolved_file_id
    file_name = file_metadata.get("name", "Unknown File")

    results = (
        service.revisions()
        .list(
            fileId=file_id,
            fields="revisions(id,modifiedTime,lastModifyingUser,size)",
            pageSize=max_results,
        )
        .execute()
    )

    revisions = results.get("revisions", [])
    if not revisions:
        return f"No revisions found for '{file_name}'."

    output_parts = [f"Found {len(revisions)} revision(s) for '{file_name}':"]
    for rev in revisions:
        rev_id = rev.get("id", "N/A")
        modified_time = rev.get("modifiedTime", "N/A")
        size = rev.get("size", "N/A")
        user_info = rev.get("lastModifyingUser", {})
        author = user_info.get("displayName", user_info.get("emailAddress", "Unknown"))

        size_str = f", Size: {size} bytes" if size != "N/A" else ""
        output_parts.append(
            f"- Revision {rev_id}: {modified_time} by {author}{size_str}"
        )

    return "\n".join(output_parts)


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------

def export_file(
    service,
    file_id: str = "",
    mime_type: str = "",
    save_to_drive: bool = False,
    save_name: Optional[str] = None,
) -> str:
    """Export a Google Workspace file to a specified format."""
    logger.info(f"[export_file] File ID: '{file_id}', Target MIME: '{mime_type}'")

    mime_shortcuts = {
        "pdf": "application/pdf",
        "txt": "text/plain",
        "csv": "text/csv",
        "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        "html": "text/html",
        "rtf": "application/rtf",
        "epub": "application/epub+zip",
    }
    mime_type = mime_shortcuts.get(mime_type.lower(), mime_type)

    resolved_file_id, file_metadata = resolve_drive_item(
        service, file_id, extra_fields="name, parents",
    )
    file_id = resolved_file_id
    file_name = file_metadata.get("name", "Unknown File")

    exported_content = (
        service.files()
        .export(fileId=file_id, mimeType=mime_type)
        .execute()
    )

    if isinstance(exported_content, str):
        exported_content = exported_content.encode("utf-8")

    size_bytes = len(exported_content)
    size_kb = size_bytes / 1024

    if save_to_drive:
        if not save_name:
            ext_map = {
                "application/pdf": ".pdf",
                "text/plain": ".txt",
                "text/html": ".html",
                "text/csv": ".csv",
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
                "application/vnd.openxmlformats-officedocument.presentationml.presentation": ".pptx",
            }
            ext = ext_map.get(mime_type, "")
            save_name = f"{Path(file_name).stem}{ext}" if ext else f"{file_name}_export"

        parents = file_metadata.get("parents", [])
        upload_metadata: Dict[str, Any] = {
            "name": save_name,
            "mimeType": mime_type,
        }
        if parents:
            upload_metadata["parents"] = parents

        media = MediaIoBaseUpload(
            io.BytesIO(exported_content),
            mimetype=mime_type,
            resumable=True,
        )

        created_file = (
            service.files()
            .create(
                body=upload_metadata,
                media_body=media,
                fields="id, name, webViewLink",
                supportsAllDrives=True,
            )
            .execute()
        )

        new_id = created_file.get("id", "N/A")
        link = created_file.get("webViewLink", "#")
        logger.info(f"[export_file] Saved export as '{save_name}' (ID: {new_id}).")
        return (
            f"Exported '{file_name}' to {mime_type} and saved to Drive.\n"
            f"Saved File: '{save_name}' (ID: {new_id})\n"
            f"Size: {size_kb:.1f} KB ({size_bytes} bytes)\n"
            f"Link: {link}"
        )
    else:
        logger.info(f"[export_file] Exported '{file_name}' - {size_kb:.1f} KB ({mime_type}).")
        return (
            f"Exported '{file_name}' to {mime_type}.\n"
            f"Size: {size_kb:.1f} KB ({size_bytes} bytes)\n"
            f"Content type: {mime_type}\n"
            f"Note: Content was exported but not saved. Use --save-to-drive to save to Drive."
        )
