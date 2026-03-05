"""
Google Drive MCP Tools

This module provides MCP tools for interacting with Google Drive API.
"""

import asyncio
import logging
import io
import httpx
import base64

from typing import Optional, List, Dict, Any
from tempfile import NamedTemporaryFile
from urllib.parse import urlparse
from pathlib import Path

from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

from auth.service_decorator import require_google_service
from auth.oauth_config import is_stateless_mode
from core.attachment_storage import get_attachment_storage, get_attachment_url
from core.utils import extract_office_xml_text, handle_http_errors
from core.server import server
from gdrive.drive_helpers import (
    DRIVE_QUERY_PATTERNS,
    build_drive_list_params,
    check_public_link_permission,
    format_permission_info,
    get_drive_image_url,
    resolve_drive_item,
    resolve_folder_id,
    validate_expiration_time,
    validate_share_role,
    validate_share_type,
)

logger = logging.getLogger(__name__)

DOWNLOAD_CHUNK_SIZE_BYTES = 256 * 1024  # 256 KB
UPLOAD_CHUNK_SIZE_BYTES = 5 * 1024 * 1024  # 5 MB (Google recommended minimum)


@server.tool()
@handle_http_errors("search_drive_files", is_read_only=True, service_type="drive")
@require_google_service("drive", "drive_read")
async def search_drive_files(
    service,
    user_google_email: str = "",
    query: str = "",
    page_size: int = 10,
    drive_id: Optional[str] = None,
    include_items_from_all_drives: bool = True,
    corpora: Optional[str] = None,
) -> str:
    """Search for files/folders in Google Drive. Supports Drive query operators or free text. Set drive_id for shared drives; prefer corpora='user' or 'drive' over 'allDrives'."""
    logger.info(
        f"[search_drive_files] Invoked. Email: '{user_google_email}', Query: '{query}'"
    )

    # Check if the query looks like a structured Drive query or free text
    # Look for Drive API operators and structured query patterns
    is_structured_query = any(pattern.search(query) for pattern in DRIVE_QUERY_PATTERNS)

    if is_structured_query:
        final_query = query
        logger.info(
            f"[search_drive_files] Using structured query as-is: '{final_query}'"
        )
    else:
        # For free text queries, wrap in fullText contains
        escaped_query = query.replace("'", "\\'")
        final_query = f"fullText contains '{escaped_query}'"
        logger.info(
            f"[search_drive_files] Reformatting free text query '{query}' to '{final_query}'"
        )

    list_params = build_drive_list_params(
        query=final_query,
        page_size=page_size,
        drive_id=drive_id,
        include_items_from_all_drives=include_items_from_all_drives,
        corpora=corpora,
    )

    results = await asyncio.to_thread(service.files().list(**list_params).execute)
    files = results.get("files", [])
    if not files:
        return f"No files found for '{query}'."

    formatted_files_text_parts = [
        f"Found {len(files)} files matching '{query}':"
    ]
    for item in files:
        size_str = f", Size: {item.get('size', 'N/A')}" if "size" in item else ""
        formatted_files_text_parts.append(
            f'- Name: "{item["name"]}" (ID: {item["id"]}, Type: {item["mimeType"]}{size_str}, Modified: {item.get("modifiedTime", "N/A")}) Link: {item.get("webViewLink", "#")}'
        )
    text_output = "\n".join(formatted_files_text_parts)
    return text_output


@server.tool()
@handle_http_errors("get_drive_file_content", is_read_only=True, service_type="drive")
@require_google_service("drive", "drive_read")
async def get_drive_file_content(
    service,
    user_google_email: str = "",
    file_id: str = "",
) -> str:
    """Retrieve file content by ID. Native Google files exported as text/CSV; Office files parsed; others downloaded as UTF-8 or noted as binary."""
    logger.info(f"[get_drive_file_content] Invoked. File ID: '{file_id}'")

    resolved_file_id, file_metadata = await resolve_drive_item(
        service,
        file_id,
        extra_fields="name, webViewLink",
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
    loop = asyncio.get_event_loop()
    done = False
    while not done:
        status, done = await loop.run_in_executor(None, downloader.next_chunk)

    file_content_bytes = fh.getvalue()

    # Attempt Office XML extraction only for actual Office XML files
    office_mime_types = {
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    }

    if mime_type in office_mime_types:
        office_text = extract_office_xml_text(file_content_bytes, mime_type)
        if office_text:
            body_text = office_text
        else:
            # Fallback: try UTF-8; otherwise flag binary
            try:
                body_text = file_content_bytes.decode("utf-8")
            except UnicodeDecodeError:
                body_text = (
                    f"[Binary or unsupported text encoding for mimeType '{mime_type}' - "
                    f"{len(file_content_bytes)} bytes]"
                )
    else:
        # For non-Office files (including Google native files), try UTF-8 decode directly
        try:
            body_text = file_content_bytes.decode("utf-8")
        except UnicodeDecodeError:
            body_text = (
                f"[Binary or unsupported text encoding for mimeType '{mime_type}' - "
                f"{len(file_content_bytes)} bytes]"
            )

    # Assemble response
    header = (
        f'File: "{file_name}" (ID: {file_id}, Type: {mime_type})\n'
        f"Link: {file_metadata.get('webViewLink', '#')}\n\n--- CONTENT ---\n"
    )
    return header + body_text


@server.tool()
@handle_http_errors(
    "get_drive_file_download_url", is_read_only=True, service_type="drive"
)
@require_google_service("drive", "drive_read")
async def get_drive_file_download_url(
    service,
    user_google_email: str = "",
    file_id: str = "",
    export_format: Optional[str] = None,
) -> str:
    """Get a download URL for a Drive file. Native files auto-export (Docs/Slides->PDF, Sheets->XLSX). Override with export_format: pdf, docx, xlsx, csv, pptx."""
    logger.info(
        f"[get_drive_file_download_url] Invoked. File ID: '{file_id}', Export format: {export_format}"
    )

    # Resolve shortcuts and get file metadata
    resolved_file_id, file_metadata = await resolve_drive_item(
        service,
        file_id,
        extra_fields="name, webViewLink, mimeType",
    )
    file_id = resolved_file_id
    mime_type = file_metadata.get("mimeType", "")
    file_name = file_metadata.get("name", "Unknown File")

    # Determine export format for Google native files
    export_mime_type = None
    output_filename = file_name
    output_mime_type = mime_type

    if mime_type == "application/vnd.google-apps.document":
        # Google Docs
        if export_format == "docx":
            export_mime_type = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            output_mime_type = export_mime_type
            if not output_filename.endswith(".docx"):
                output_filename = f"{Path(output_filename).stem}.docx"
        else:
            # Default to PDF
            export_mime_type = "application/pdf"
            output_mime_type = export_mime_type
            if not output_filename.endswith(".pdf"):
                output_filename = f"{Path(output_filename).stem}.pdf"

    elif mime_type == "application/vnd.google-apps.spreadsheet":
        # Google Sheets
        if export_format == "csv":
            export_mime_type = "text/csv"
            output_mime_type = export_mime_type
            if not output_filename.endswith(".csv"):
                output_filename = f"{Path(output_filename).stem}.csv"
        else:
            # Default to XLSX
            export_mime_type = (
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            output_mime_type = export_mime_type
            if not output_filename.endswith(".xlsx"):
                output_filename = f"{Path(output_filename).stem}.xlsx"

    elif mime_type == "application/vnd.google-apps.presentation":
        # Google Slides
        if export_format == "pptx":
            export_mime_type = "application/vnd.openxmlformats-officedocument.presentationml.presentation"
            output_mime_type = export_mime_type
            if not output_filename.endswith(".pptx"):
                output_filename = f"{Path(output_filename).stem}.pptx"
        else:
            # Default to PDF
            export_mime_type = "application/pdf"
            output_mime_type = export_mime_type
            if not output_filename.endswith(".pdf"):
                output_filename = f"{Path(output_filename).stem}.pdf"

    # Download the file
    request_obj = (
        service.files().export_media(fileId=file_id, mimeType=export_mime_type)
        if export_mime_type
        else service.files().get_media(fileId=file_id)
    )

    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request_obj)
    loop = asyncio.get_event_loop()
    done = False
    while not done:
        status, done = await loop.run_in_executor(None, downloader.next_chunk)

    file_content_bytes = fh.getvalue()
    size_bytes = len(file_content_bytes)
    size_kb = size_bytes / 1024 if size_bytes else 0

    # Check if we're in stateless mode (can't save files)
    if is_stateless_mode():
        result_lines = [
            "File downloaded successfully!",
            f"File: {file_name}",
            f"File ID: {file_id}",
            f"Size: {size_kb:.1f} KB ({size_bytes} bytes)",
            f"MIME Type: {output_mime_type}",
            "\n⚠️ Stateless mode: File storage disabled.",
            "\nBase64-encoded content (first 100 characters shown):",
            f"{base64.b64encode(file_content_bytes[:100]).decode('utf-8')}...",
        ]
        logger.info(
            f"[get_drive_file_download_url] Successfully downloaded {size_kb:.1f} KB file (stateless mode)"
        )
        return "\n".join(result_lines)

    # Save file and generate URL
    try:
        storage = get_attachment_storage()

        # Encode bytes to base64 (as expected by AttachmentStorage)
        base64_data = base64.urlsafe_b64encode(file_content_bytes).decode("utf-8")

        # Save attachment
        saved_file_id = storage.save_attachment(
            base64_data=base64_data,
            filename=output_filename,
            mime_type=output_mime_type,
        )

        # Generate URL
        download_url = get_attachment_url(saved_file_id)

        result_lines = [
            "File downloaded successfully!",
            f"File: {file_name}",
            f"File ID: {file_id}",
            f"Size: {size_kb:.1f} KB ({size_bytes} bytes)",
            f"MIME Type: {output_mime_type}",
            f"\n📎 Download URL: {download_url}",
            "\nThe file has been saved and is available at the URL above.",
            "The file will expire after 1 hour.",
        ]

        if export_mime_type:
            result_lines.append(
                f"\nNote: Google native file exported to {output_mime_type} format."
            )

        logger.info(
            f"[get_drive_file_download_url] Successfully saved {size_kb:.1f} KB file as {saved_file_id}"
        )
        return "\n".join(result_lines)

    except Exception as e:
        logger.error(f"[get_drive_file_download_url] Failed to save file: {e}")
        return (
            f"Error: Failed to save file for download.\n"
            f"File was downloaded successfully ({size_kb:.1f} KB) but could not be saved.\n\n"
            f"Error details: {str(e)}"
        )


@server.tool()
@handle_http_errors("list_drive_items", is_read_only=True, service_type="drive")
@require_google_service("drive", "drive_read")
async def list_drive_items(
    service,
    user_google_email: str = "",
    folder_id: str = "root",
    page_size: int = 100,
    drive_id: Optional[str] = None,
    include_items_from_all_drives: bool = True,
    corpora: Optional[str] = None,
) -> str:
    """List files/folders in a Drive folder. Set drive_id for shared drives. folder_id defaults to 'root'."""
    logger.info(
        f"[list_drive_items] Invoked. Email: '{user_google_email}', Folder ID: '{folder_id}'"
    )

    resolved_folder_id = await resolve_folder_id(service, folder_id)
    final_query = f"'{resolved_folder_id}' in parents and trashed=false"

    list_params = build_drive_list_params(
        query=final_query,
        page_size=page_size,
        drive_id=drive_id,
        include_items_from_all_drives=include_items_from_all_drives,
        corpora=corpora,
    )

    results = await asyncio.to_thread(service.files().list(**list_params).execute)
    files = results.get("files", [])
    if not files:
        return f"No items found in folder '{folder_id}'."

    formatted_items_text_parts = [
        f"Found {len(files)} items in folder '{folder_id}':"
    ]
    for item in files:
        size_str = f", Size: {item.get('size', 'N/A')}" if "size" in item else ""
        formatted_items_text_parts.append(
            f'- Name: "{item["name"]}" (ID: {item["id"]}, Type: {item["mimeType"]}{size_str}, Modified: {item.get("modifiedTime", "N/A")}) Link: {item.get("webViewLink", "#")}'
        )
    text_output = "\n".join(formatted_items_text_parts)
    return text_output


@server.tool()
@handle_http_errors("create_drive_file", service_type="drive")
@require_google_service("drive", "drive_file")
async def create_drive_file(
    service,
    user_google_email: str = "",
    file_name: str = "",
    content: Optional[str] = None,
    folder_id: str = "root",
    mime_type: str = "text/plain",
    fileUrl: Optional[str] = None,
) -> str:
    """Create a file in Drive from content or fileUrl (file://, http://, https://). For shared drives, set folder_id to a folder within the drive."""
    logger.info(
        f"[create_drive_file] Invoked. Email: '{user_google_email}', File Name: {file_name}, Folder ID: {folder_id}, fileUrl: {fileUrl}"
    )

    if not content and not fileUrl:
        raise Exception("You must provide either 'content' or 'fileUrl'.")

    file_data = None
    resolved_folder_id = await resolve_folder_id(service, folder_id)

    file_metadata = {
        "name": file_name,
        "parents": [resolved_folder_id],
        "mimeType": mime_type,
    }

    # Prefer fileUrl if both are provided
    if fileUrl:
        logger.info(f"[create_drive_file] Fetching file from URL: {fileUrl}")

        # Check if this is a file:// URL
        parsed_url = urlparse(fileUrl)
        # CW-MODIFIED: file:// URLs disabled to prevent arbitrary local file read
        # via prompt injection attacks. Users can upload files via Google Drive UI.
        if parsed_url.scheme == "file":
            raise Exception(
                "file:// URLs are disabled for security reasons. "
                "Upload files via Google Drive web UI or use an HTTP(S) URL."
            )
        # Handle HTTP/HTTPS URLs
        elif parsed_url.scheme in ("http", "https"):
            # when running in stateless mode, deployment may not have access to local file system
            if is_stateless_mode():
                async with httpx.AsyncClient(follow_redirects=True) as client:
                    resp = await client.get(fileUrl)
                    if resp.status_code != 200:
                        raise Exception(
                            f"Failed to fetch file from URL: {fileUrl} (status {resp.status_code})"
                        )
                    file_data = await resp.aread()
                    # Try to get MIME type from Content-Type header
                    content_type = resp.headers.get("Content-Type")
                    if content_type and content_type != "application/octet-stream":
                        mime_type = content_type
                        file_metadata["mimeType"] = content_type
                        logger.info(
                            f"[create_drive_file] Using MIME type from Content-Type header: {content_type}"
                        )

                media = MediaIoBaseUpload(
                    io.BytesIO(file_data),
                    mimetype=mime_type,
                    resumable=True,
                    chunksize=UPLOAD_CHUNK_SIZE_BYTES,
                )

                created_file = await asyncio.to_thread(
                    service.files()
                    .create(
                        body=file_metadata,
                        media_body=media,
                        fields="id, name, webViewLink",
                        supportsAllDrives=True,
                    )
                    .execute
                )
            else:
                # Use NamedTemporaryFile to stream download and upload
                with NamedTemporaryFile() as temp_file:
                    total_bytes = 0
                    # follow redirects
                    async with httpx.AsyncClient(follow_redirects=True) as client:
                        async with client.stream("GET", fileUrl) as resp:
                            if resp.status_code != 200:
                                raise Exception(
                                    f"Failed to fetch file from URL: {fileUrl} (status {resp.status_code})"
                                )

                            # Stream download in chunks
                            async for chunk in resp.aiter_bytes(
                                chunk_size=DOWNLOAD_CHUNK_SIZE_BYTES
                            ):
                                await asyncio.to_thread(temp_file.write, chunk)
                                total_bytes += len(chunk)

                            logger.info(
                                f"[create_drive_file] Downloaded {total_bytes} bytes from URL before upload."
                            )

                            # Try to get MIME type from Content-Type header
                            content_type = resp.headers.get("Content-Type")
                            if (
                                content_type
                                and content_type != "application/octet-stream"
                            ):
                                mime_type = content_type
                                file_metadata["mimeType"] = mime_type
                                logger.info(
                                    f"[create_drive_file] Using MIME type from Content-Type header: {mime_type}"
                                )

                    # Reset file pointer to beginning for upload
                    temp_file.seek(0)

                    # Upload with chunking
                    media = MediaIoBaseUpload(
                        temp_file,
                        mimetype=mime_type,
                        resumable=True,
                        chunksize=UPLOAD_CHUNK_SIZE_BYTES,
                    )

                    logger.info(
                        "[create_drive_file] Starting upload to Google Drive..."
                    )
                    created_file = await asyncio.to_thread(
                        service.files()
                        .create(
                            body=file_metadata,
                            media_body=media,
                            fields="id, name, webViewLink",
                            supportsAllDrives=True,
                        )
                        .execute
                    )
        else:
            if not parsed_url.scheme:
                raise Exception(
                    "fileUrl is missing a URL scheme. Use file://, http://, or https://."
                )
            raise Exception(
                f"Unsupported URL scheme '{parsed_url.scheme}'. Only file://, http://, and https:// are supported."
            )
    elif content:
        file_data = content.encode("utf-8")
        media = io.BytesIO(file_data)

        created_file = await asyncio.to_thread(
            service.files()
            .create(
                body=file_metadata,
                media_body=MediaIoBaseUpload(media, mimetype=mime_type, resumable=True),
                fields="id, name, webViewLink",
                supportsAllDrives=True,
            )
            .execute
        )

    link = created_file.get("webViewLink", "No link available")
    new_id = created_file.get("id", "N/A")
    logger.info(f"Successfully created file. Link: {link}")
    return f"File created. ID: {new_id}\nLink: {link}"


@server.tool()
@handle_http_errors(
    "get_drive_file_permissions", is_read_only=True, service_type="drive"
)
@require_google_service("drive", "drive_read")
async def get_drive_file_permissions(
    service,
    user_google_email: str = "",
    file_id: str = "",
) -> str:
    """Get detailed file metadata including sharing permissions and URLs."""
    logger.info(
        f"[get_drive_file_permissions] Checking file {file_id}"
    )

    resolved_file_id, _ = await resolve_drive_item(service, file_id)
    file_id = resolved_file_id

    try:
        # Get comprehensive file metadata including permissions with details
        file_metadata = await asyncio.to_thread(
            service.files()
            .get(
                fileId=file_id,
                fields="id, name, mimeType, size, modifiedTime, owners, "
                "permissions(id, type, role, emailAddress, domain, expirationTime, permissionDetails), "
                "webViewLink, webContentLink, shared, sharingUser, viewersCanCopyContent",
                supportsAllDrives=True,
            )
            .execute
        )

        # Format the response
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

        # Add sharing user if available
        sharing_user = file_metadata.get("sharingUser")
        if sharing_user:
            output_parts.append(
                f"  Shared by: {sharing_user.get('displayName', 'Unknown')} ({sharing_user.get('emailAddress', 'Unknown')})"
            )

        # Process permissions
        permissions = file_metadata.get("permissions", [])
        if permissions:
            output_parts.append(f"  Number of permissions: {len(permissions)}")
            output_parts.append("  Permissions:")
            for perm in permissions:
                output_parts.append(f"    - {format_permission_info(perm)}")
        else:
            output_parts.append("  No additional permissions (private file)")

        # Add URLs
        output_parts.extend(
            [
                "",
                "URLs:",
                f"  View Link: {file_metadata.get('webViewLink', 'N/A')}",
            ]
        )

        # webContentLink is only available for files that can be downloaded
        web_content_link = file_metadata.get("webContentLink")
        if web_content_link:
            output_parts.append(f"  Direct Download Link: {web_content_link}")

        has_public_link = check_public_link_permission(permissions)

        if has_public_link:
            output_parts.extend(
                [
                    "",
                    "✅ This file is shared with 'Anyone with the link' - it can be inserted into Google Docs",
                ]
            )
        else:
            output_parts.extend(
                [
                    "",
                    "❌ This file is NOT shared with 'Anyone with the link' - it cannot be inserted into Google Docs",
                    "   To fix: Right-click the file in Google Drive → Share → Anyone with the link → Viewer",
                ]
            )

        return "\n".join(output_parts)

    except Exception as e:
        logger.error(f"Error getting file permissions: {e}")
        return f"Error getting file permissions: {e}"


@server.tool()
@handle_http_errors(
    "check_drive_file_public_access", is_read_only=True, service_type="drive"
)
@require_google_service("drive", "drive_read")
async def check_drive_file_public_access(
    service,
    user_google_email: str = "",
    file_name: str = "",
) -> str:
    """Search for a file by name and check if it has public link sharing enabled."""
    logger.info(f"[check_drive_file_public_access] Searching for {file_name}")

    # Search for the file
    escaped_name = file_name.replace("'", "\\'")
    query = f"name = '{escaped_name}'"

    list_params = {
        "q": query,
        "pageSize": 10,
        "fields": "files(id, name, mimeType, webViewLink)",
        "supportsAllDrives": True,
        "includeItemsFromAllDrives": True,
    }

    results = await asyncio.to_thread(service.files().list(**list_params).execute)

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

    # Check permissions for the first file
    file_id = files[0]["id"]
    resolved_file_id, _ = await resolve_drive_item(service, file_id)
    file_id = resolved_file_id

    # Get detailed permissions
    file_metadata = await asyncio.to_thread(
        service.files()
        .get(
            fileId=file_id,
            fields="id, name, mimeType, permissions, webViewLink, webContentLink, shared",
            supportsAllDrives=True,
        )
        .execute
    )

    permissions = file_metadata.get("permissions", [])

    has_public_link = check_public_link_permission(permissions)

    output_parts.extend(
        [
            f"File: {file_metadata['name']}",
            f"ID: {file_id}",
            f"Type: {file_metadata['mimeType']}",
            f"Shared: {file_metadata.get('shared', False)}",
            "",
        ]
    )

    if has_public_link:
        output_parts.extend(
            [
                "✅ PUBLIC ACCESS ENABLED - This file can be inserted into Google Docs",
                f"Use with insert_doc_image_url: {get_drive_image_url(file_id)}",
            ]
        )
    else:
        output_parts.extend(
            [
                "❌ NO PUBLIC ACCESS - Cannot insert into Google Docs",
                "Fix: Drive → Share → 'Anyone with the link' → 'Viewer'",
            ]
        )

    return "\n".join(output_parts)


@server.tool()
@handle_http_errors("update_drive_file", is_read_only=False, service_type="drive")
@require_google_service("drive", "drive_file")
async def update_drive_file(
    service,
    user_google_email: str = "",
    file_id: str = "",
    name: Optional[str] = None,
    description: Optional[str] = None,
    mime_type: Optional[str] = None,
    add_parents: Optional[str] = None,
    remove_parents: Optional[str] = None,
    starred: Optional[bool] = None,
    trashed: Optional[bool] = None,
    writers_can_share: Optional[bool] = None,
    copy_requires_writer_permission: Optional[bool] = None,
    properties: Optional[dict] = None,
) -> str:
    """Update metadata/properties of a Drive file. Use add_parents/remove_parents (comma-separated IDs) to move between folders."""
    logger.info(f"[update_drive_file] Updating file {file_id}")

    current_file_fields = (
        "name, description, mimeType, parents, starred, trashed, webViewLink, "
        "writersCanShare, copyRequiresWriterPermission, properties"
    )
    resolved_file_id, current_file = await resolve_drive_item(
        service,
        file_id,
        extra_fields=current_file_fields,
    )
    file_id = resolved_file_id

    # Build the update body with only specified fields
    update_body = {}
    if name is not None:
        update_body["name"] = name
    if description is not None:
        update_body["description"] = description
    if mime_type is not None:
        update_body["mimeType"] = mime_type
    if starred is not None:
        update_body["starred"] = starred
    if trashed is not None:
        update_body["trashed"] = trashed
    if writers_can_share is not None:
        update_body["writersCanShare"] = writers_can_share
    if copy_requires_writer_permission is not None:
        update_body["copyRequiresWriterPermission"] = copy_requires_writer_permission
    if properties is not None:
        update_body["properties"] = properties

    async def _resolve_parent_arguments(parent_arg: Optional[str]) -> Optional[str]:
        if not parent_arg:
            return None
        parent_ids = [part.strip() for part in parent_arg.split(",") if part.strip()]
        if not parent_ids:
            return None

        resolved_ids = []
        for parent in parent_ids:
            resolved_parent = await resolve_folder_id(service, parent)
            resolved_ids.append(resolved_parent)
        return ",".join(resolved_ids)

    resolved_add_parents = await _resolve_parent_arguments(add_parents)
    resolved_remove_parents = await _resolve_parent_arguments(remove_parents)

    # Build query parameters for parent changes
    query_params = {
        "fileId": file_id,
        "supportsAllDrives": True,
        "fields": "id, name, description, mimeType, parents, starred, trashed, webViewLink, writersCanShare, copyRequiresWriterPermission, properties",
    }

    if resolved_add_parents:
        query_params["addParents"] = resolved_add_parents
    if resolved_remove_parents:
        query_params["removeParents"] = resolved_remove_parents

    # Only include body if there are updates
    if update_body:
        query_params["body"] = update_body

    # Perform the update
    updated_file = await asyncio.to_thread(
        service.files().update(**query_params).execute
    )

    # Build response message
    output_parts = [
        f"Updated file: {updated_file.get('name', current_file['name'])}"
    ]

    # Report what changed
    changes = []
    if name is not None and name != current_file.get("name"):
        changes.append(f"   • Name: '{current_file.get('name')}' → '{name}'")
    if description is not None:
        old_desc_value = current_file.get("description")
        new_desc_value = description
        should_report_change = (old_desc_value or "") != (new_desc_value or "")
        if should_report_change:
            old_desc_display = (
                old_desc_value if old_desc_value not in (None, "") else "(empty)"
            )
            new_desc_display = (
                new_desc_value if new_desc_value not in (None, "") else "(empty)"
            )
            changes.append(f"   • Description: {old_desc_display} → {new_desc_display}")
    if add_parents:
        changes.append(f"   • Added to folder(s): {add_parents}")
    if remove_parents:
        changes.append(f"   • Removed from folder(s): {remove_parents}")
    current_starred = current_file.get("starred")
    if starred is not None and starred != current_starred:
        star_status = "starred" if starred else "unstarred"
        changes.append(f"   • File {star_status}")
    current_trashed = current_file.get("trashed")
    if trashed is not None and trashed != current_trashed:
        trash_status = "moved to trash" if trashed else "restored from trash"
        changes.append(f"   • File {trash_status}")
    current_writers_can_share = current_file.get("writersCanShare")
    if writers_can_share is not None and writers_can_share != current_writers_can_share:
        share_status = "can" if writers_can_share else "cannot"
        changes.append(f"   • Writers {share_status} share the file")
    current_copy_requires_writer_permission = current_file.get(
        "copyRequiresWriterPermission"
    )
    if (
        copy_requires_writer_permission is not None
        and copy_requires_writer_permission != current_copy_requires_writer_permission
    ):
        copy_status = (
            "requires" if copy_requires_writer_permission else "doesn't require"
        )
        changes.append(f"   • Copying {copy_status} writer permission")
    if properties:
        changes.append(f"   • Updated custom properties: {properties}")

    if changes:
        output_parts.append("")
        output_parts.append("Changes applied:")
        output_parts.extend(changes)
    else:
        output_parts.append("   (No changes were made)")

    return "\n".join(output_parts)


@server.tool()
@handle_http_errors("get_drive_shareable_link", is_read_only=True, service_type="drive")
@require_google_service("drive", "drive_read")
async def get_drive_shareable_link(
    service,
    user_google_email: str = "",
    file_id: str = "",
) -> str:
    """Get shareable link and current permissions for a Drive file or folder."""
    logger.info(
        f"[get_drive_shareable_link] Invoked. Email: '{user_google_email}', File ID: '{file_id}'"
    )

    resolved_file_id, _ = await resolve_drive_item(service, file_id)
    file_id = resolved_file_id

    file_metadata = await asyncio.to_thread(
        service.files()
        .get(
            fileId=file_id,
            fields="id, name, mimeType, webViewLink, webContentLink, shared, "
            "permissions(id, type, role, emailAddress, domain, expirationTime)",
            supportsAllDrives=True,
        )
        .execute
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


# =============================================================================
# REMOVED FOR SECURITY: share_drive_file, batch_share_drive_file,
#                       update_drive_permission, remove_drive_permission,
#                       transfer_drive_ownership
#
# These tools were removed to prevent data exfiltration via file sharing.
# An attacker using prompt injection could otherwise share sensitive files
# with external email addresses.
#
# Read-only permission tools (get_drive_file_permissions, get_drive_shareable_link,
# check_drive_file_public_access) are retained for visibility.
# =============================================================================


@server.tool()
@handle_http_errors("copy_drive_file", service_type="drive")
@require_google_service("drive", "drive_file")
async def copy_drive_file(
    service,
    user_google_email: str = "",
    file_id: str = "",
    new_name: Optional[str] = None,
    parent_folder_id: Optional[str] = None,
) -> str:
    """Copy a Drive file. Optionally set new_name and parent_folder_id."""
    logger.info(
        f"[copy_drive_file] Invoked. Email: '{user_google_email}', File ID: '{file_id}'"
    )

    resolved_file_id, file_metadata = await resolve_drive_item(
        service, file_id, extra_fields="name"
    )
    file_id = resolved_file_id
    original_name = file_metadata.get("name", "Unknown File")

    copy_body: Dict[str, Any] = {}
    if new_name:
        copy_body["name"] = new_name
    if parent_folder_id:
        resolved_parent = await resolve_folder_id(service, parent_folder_id)
        copy_body["parents"] = [resolved_parent]

    copied_file = await asyncio.to_thread(
        service.files()
        .copy(
            fileId=file_id,
            body=copy_body,
            fields="id, name, webViewLink",
            supportsAllDrives=True,
        )
        .execute
    )

    copy_name = copied_file.get("name", new_name or f"Copy of {original_name}")
    copy_id = copied_file.get("id", "N/A")
    link = copied_file.get("webViewLink", "#")

    logger.info(f"[copy_drive_file] Successfully copied file. New ID: {copy_id}")
    return (
        f"Successfully copied '{original_name}' as '{copy_name}'\n"
        f"New File ID: {copy_id}\n"
        f"Link: {link}"
    )


@server.tool()
@handle_http_errors("trash_drive_file", service_type="drive")
@require_google_service("drive", "drive_file")
async def trash_drive_file(
    service,
    user_google_email: str = "",
    file_id: str = "",
    untrash: bool = False,
) -> str:
    """Move a file to trash, or restore it (untrash=True)."""
    action = "untrash" if untrash else "trash"
    logger.info(
        f"[trash_drive_file] Invoked. Email: '{user_google_email}', File ID: '{file_id}', Action: {action}"
    )

    resolved_file_id, file_metadata = await resolve_drive_item(
        service, file_id, extra_fields="name"
    )
    file_id = resolved_file_id
    file_name = file_metadata.get("name", "Unknown File")

    updated_file = await asyncio.to_thread(
        service.files()
        .update(
            fileId=file_id,
            body={"trashed": not untrash},
            fields="id, name, trashed, webViewLink",
            supportsAllDrives=True,
        )
        .execute
    )

    if untrash:
        logger.info(f"[trash_drive_file] Restored '{file_name}' from trash.")
        return f"Restored '{file_name}' from trash."
    else:
        logger.info(f"[trash_drive_file] Moved '{file_name}' to trash.")
        return f"Moved '{file_name}' to trash. Restore with untrash=True."


@server.tool()
@handle_http_errors("delete_drive_file", service_type="drive")
@require_google_service("drive", "drive_file")
async def delete_drive_file(
    service,
    user_google_email: str = "",
    file_id: str = "",
) -> str:
    """PERMANENTLY delete a Drive file (irreversible). Use trash_drive_file for recoverable deletion."""
    logger.info(
        f"[delete_drive_file] Invoked. Email: '{user_google_email}', File ID: '{file_id}'"
    )

    resolved_file_id, file_metadata = await resolve_drive_item(
        service, file_id, extra_fields="name"
    )
    file_id = resolved_file_id
    file_name = file_metadata.get("name", "Unknown File")

    await asyncio.to_thread(
        service.files()
        .delete(fileId=file_id, supportsAllDrives=True)
        .execute
    )

    logger.info(f"[delete_drive_file] Permanently deleted '{file_name}' ({file_id}).")
    return f"Permanently deleted '{file_name}'."


@server.tool()
@handle_http_errors("list_revisions", is_read_only=True, service_type="drive")
@require_google_service("drive", "drive_read")
async def list_revisions(
    service,
    user_google_email: str = "",
    file_id: str = "",
    max_results: int = 20,
) -> str:
    """List revision history of a Drive file."""
    logger.info(
        f"[list_revisions] Invoked. Email: '{user_google_email}', File ID: '{file_id}'"
    )

    resolved_file_id, file_metadata = await resolve_drive_item(
        service, file_id, extra_fields="name"
    )
    file_id = resolved_file_id
    file_name = file_metadata.get("name", "Unknown File")

    results = await asyncio.to_thread(
        service.revisions()
        .list(
            fileId=file_id,
            fields="revisions(id,modifiedTime,lastModifyingUser,size)",
            pageSize=max_results,
        )
        .execute
    )

    revisions = results.get("revisions", [])
    if not revisions:
        return f"No revisions found for '{file_name}'."

    output_parts = [
        f"Found {len(revisions)} revision(s) for '{file_name}':"
    ]
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


@server.tool()
@handle_http_errors("get_revision", is_read_only=True, service_type="drive")
@require_google_service("drive", "drive_read")
async def get_revision(
    service,
    user_google_email: str = "",
    file_id: str = "",
    revision_id: str = "",
) -> str:
    """Get detailed info about a specific revision of a Drive file."""
    logger.info(
        f"[get_revision] Invoked. Email: '{user_google_email}', File ID: '{file_id}', Revision ID: '{revision_id}'"
    )

    resolved_file_id, file_metadata = await resolve_drive_item(
        service, file_id, extra_fields="name"
    )
    file_id = resolved_file_id
    file_name = file_metadata.get("name", "Unknown File")

    revision = await asyncio.to_thread(
        service.revisions()
        .get(fileId=file_id, revisionId=revision_id, fields="*")
        .execute
    )

    output_parts = [
        f"Revision details for '{file_name}':",
        f"  Revision ID: {revision.get('id', 'N/A')}",
        f"  Modified Time: {revision.get('modifiedTime', 'N/A')}",
        f"  MIME Type: {revision.get('mimeType', 'N/A')}",
        f"  Size: {revision.get('size', 'N/A')} bytes",
    ]

    user_info = revision.get("lastModifyingUser", {})
    if user_info:
        output_parts.append(
            f"  Last Modified By: {user_info.get('displayName', 'Unknown')} "
            f"({user_info.get('emailAddress', 'Unknown')})"
        )

    output_parts.extend(
        [
            f"  Keep Forever: {revision.get('keepForever', False)}",
            f"  Published: {revision.get('published', False)}",
        ]
    )

    export_links = revision.get("exportLinks", {})
    if export_links:
        output_parts.append("  Export Links:")
        for fmt, link in export_links.items():
            output_parts.append(f"    - {fmt}: {link}")

    logger.info(f"[get_revision] Retrieved revision {revision_id} for file {file_id}.")
    return "\n".join(output_parts)


@server.tool()
@handle_http_errors("export_drive_file", service_type="drive")
@require_google_service("drive", "drive_file")
async def export_drive_file(
    service,
    user_google_email: str = "",
    file_id: str = "",
    mime_type: str = "",
    save_to_drive: bool = False,
    save_name: Optional[str] = None,
) -> str:
    """Export a Google Workspace file to a format (pdf, txt, csv, docx, xlsx, pptx). Set save_to_drive=True to save the result back to Drive."""
    logger.info(
        f"[export_drive_file] Invoked. Email: '{user_google_email}', File ID: '{file_id}', Target MIME: '{mime_type}'"
    )

    resolved_file_id, file_metadata = await resolve_drive_item(
        service, file_id, extra_fields="name, parents"
    )
    file_id = resolved_file_id
    file_name = file_metadata.get("name", "Unknown File")

    exported_content = await asyncio.to_thread(
        service.files()
        .export(fileId=file_id, mimeType=mime_type)
        .execute
    )

    # exported_content is bytes
    if isinstance(exported_content, str):
        exported_content = exported_content.encode("utf-8")

    size_bytes = len(exported_content)
    size_kb = size_bytes / 1024

    if save_to_drive:
        # Determine file name for the saved export
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

        created_file = await asyncio.to_thread(
            service.files()
            .create(
                body=upload_metadata,
                media_body=media,
                fields="id, name, webViewLink",
                supportsAllDrives=True,
            )
            .execute
        )

        new_id = created_file.get("id", "N/A")
        link = created_file.get("webViewLink", "#")
        logger.info(f"[export_drive_file] Saved export as '{save_name}' (ID: {new_id}).")
        return (
            f"Exported '{file_name}' to {mime_type} and saved to Drive.\n"
            f"Saved File: '{save_name}' (ID: {new_id})\n"
            f"Size: {size_kb:.1f} KB ({size_bytes} bytes)\n"
            f"Link: {link}"
        )
    else:
        logger.info(
            f"[export_drive_file] Exported '{file_name}' - {size_kb:.1f} KB ({mime_type})."
        )
        return (
            f"Exported '{file_name}' to {mime_type}.\n"
            f"Size: {size_kb:.1f} KB ({size_bytes} bytes)\n"
            f"Content type: {mime_type}\n"
            f"Note: Content was exported but not saved. Set save_to_drive=True to save to Drive."
        )


@server.tool()
@handle_http_errors("create_drive_folder", service_type="drive")
@require_google_service("drive", "drive_file")
async def create_drive_folder(
    service,
    user_google_email: str = "",
    name: str = "",
    parent_folder_id: Optional[str] = None,
) -> str:
    """Create a new folder in Drive. Set parent_folder_id to nest it; defaults to root."""
    logger.info(
        f"[create_drive_folder] Invoked. Email: '{user_google_email}', Name: '{name}'"
    )

    folder_metadata: Dict[str, Any] = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
    }

    if parent_folder_id:
        resolved_parent = await resolve_folder_id(service, parent_folder_id)
        folder_metadata["parents"] = [resolved_parent]

    created_folder = await asyncio.to_thread(
        service.files()
        .create(
            body=folder_metadata,
            fields="id, name, webViewLink",
            supportsAllDrives=True,
        )
        .execute
    )

    folder_id = created_folder.get("id", "N/A")
    link = created_folder.get("webViewLink", "#")
    logger.info(f"[create_drive_folder] Created folder '{name}' (ID: {folder_id}).")
    return (
        f"Successfully created folder '{name}'.\n"
        f"Folder ID: {folder_id}\n"
        f"Link: {link}"
    )


@server.tool()
@handle_http_errors("move_drive_file", service_type="drive")
@require_google_service("drive", "drive_file")
async def move_drive_file(
    service,
    user_google_email: str = "",
    file_id: str = "",
    destination_folder_id: str = "",
) -> str:
    """Move a Drive file to a different folder."""
    logger.info(
        f"[move_drive_file] Invoked. Email: '{user_google_email}', File ID: '{file_id}', Destination: '{destination_folder_id}'"
    )

    resolved_file_id, file_metadata = await resolve_drive_item(
        service, file_id, extra_fields="name, parents"
    )
    file_id = resolved_file_id
    file_name = file_metadata.get("name", "Unknown File")
    current_parents = file_metadata.get("parents", [])

    resolved_destination = await resolve_folder_id(service, destination_folder_id)

    previous_parents = ",".join(current_parents) if current_parents else ""

    updated_file = await asyncio.to_thread(
        service.files()
        .update(
            fileId=file_id,
            addParents=resolved_destination,
            removeParents=previous_parents,
            fields="id, name, parents, webViewLink",
            supportsAllDrives=True,
        )
        .execute
    )

    logger.info(
        f"[move_drive_file] Moved '{file_name}' to folder '{resolved_destination}'."
    )
    return f"Moved '{file_name}' to folder '{destination_folder_id}'."
