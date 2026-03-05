"""
Google Docs MCP Tools

This module provides MCP tools for interacting with Google Docs API and managing Google Docs via Drive.
"""

import logging
import asyncio
import io
from typing import List, Dict, Any

from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

# Auth & server utilities
from auth.service_decorator import require_google_service, require_multiple_services
from core.utils import extract_office_xml_text, handle_http_errors
from core.server import server
from core.comments import create_comment_tools

# Import helper functions for document operations
from gdocs.docs_helpers import (
    create_insert_text_request,
    create_delete_range_request,
    create_format_text_request,
    create_find_replace_request,
    create_insert_table_request,
    create_insert_page_break_request,
    create_insert_image_request,
    create_bullet_list_request,
)

# Import document structure and table utilities
from gdocs.docs_structure import (
    parse_document_structure,
    find_tables,
    analyze_document_complexity,
)
from gdocs.docs_tables import extract_table_as_data

# Import operation managers for complex business logic
from gdocs.managers import (
    TableOperationManager,
    HeaderFooterManager,
    ValidationManager,
    BatchOperationManager,
)
import json

logger = logging.getLogger(__name__)


@server.tool()
@handle_http_errors("search_docs", is_read_only=True, service_type="docs")
@require_google_service("drive", "drive_read")
async def search_docs(
    service: Any,
    user_google_email: str = "",
    query: str = "",
    page_size: int = 10,
) -> str:
    """Search for Google Docs by name. Returns matching doc names, IDs, and links."""
    logger.info(f"[search_docs] Email={user_google_email}, Query='{query}'")

    escaped_query = query.replace("'", "\\'")

    response = await asyncio.to_thread(
        service.files()
        .list(
            q=f"name contains '{escaped_query}' and mimeType='application/vnd.google-apps.document' and trashed=false",
            pageSize=page_size,
            fields="files(id, name, createdTime, modifiedTime, webViewLink)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        .execute
    )
    files = response.get("files", [])
    if not files:
        return f"No Google Docs found matching '{query}'."

    output = [f"Found {len(files)} Google Docs matching '{query}':"]
    for f in files:
        output.append(
            f"- {f['name']} (ID: {f['id']}) Modified: {f.get('modifiedTime')} Link: {f.get('webViewLink')}"
        )
    return "\n".join(output)


@server.tool()
@handle_http_errors("get_doc_content", is_read_only=True, service_type="docs")
@require_multiple_services(
    [
        {
            "service_type": "drive",
            "scopes": "drive_read",
            "param_name": "drive_service",
        },
        {"service_type": "docs", "scopes": "docs_read", "param_name": "docs_service"},
    ]
)
async def get_doc_content(
    drive_service: Any,
    docs_service: Any,
    user_google_email: str = "",
    file_id: str = "",
) -> str:
    """Retrieve content of a Google Doc or Drive file (.docx). Returns text with metadata header."""
    logger.info(
        f"[get_doc_content] Invoked. Document/File ID: '{file_id}' for user '{user_google_email}'"
    )

    # Step 2: Get file metadata from Drive
    file_metadata = await asyncio.to_thread(
        drive_service.files()
        .get(
            fileId=file_id,
            fields="id, name, mimeType, webViewLink",
            supportsAllDrives=True,
        )
        .execute
    )
    mime_type = file_metadata.get("mimeType", "")
    file_name = file_metadata.get("name", "Unknown File")
    web_view_link = file_metadata.get("webViewLink", "#")

    logger.info(
        f"[get_doc_content] File '{file_name}' (ID: {file_id}) has mimeType: '{mime_type}'"
    )

    body_text = ""  # Initialize body_text

    # Step 3: Process based on mimeType
    if mime_type == "application/vnd.google-apps.document":
        logger.info("[get_doc_content] Processing as native Google Doc.")
        doc_data = await asyncio.to_thread(
            docs_service.documents()
            .get(documentId=file_id, includeTabsContent=True)
            .execute
        )
        # Tab header format constant
        TAB_HEADER_FORMAT = "\n--- TAB: {tab_name} ---\n"

        def extract_text_from_elements(elements, tab_name=None, depth=0):
            """Extract text from document elements (paragraphs, tables, etc.)"""
            # Prevent infinite recursion by limiting depth
            if depth > 5:
                return ""
            text_lines = []
            if tab_name:
                text_lines.append(TAB_HEADER_FORMAT.format(tab_name=tab_name))

            for element in elements:
                if "paragraph" in element:
                    paragraph = element.get("paragraph", {})
                    para_elements = paragraph.get("elements", [])
                    current_line_text = ""
                    for pe in para_elements:
                        text_run = pe.get("textRun", {})
                        if text_run and "content" in text_run:
                            current_line_text += text_run["content"]
                    if current_line_text.strip():
                        text_lines.append(current_line_text)
                elif "table" in element:
                    # Handle table content
                    table = element.get("table", {})
                    table_rows = table.get("tableRows", [])
                    for row in table_rows:
                        row_cells = row.get("tableCells", [])
                        for cell in row_cells:
                            cell_content = cell.get("content", [])
                            cell_text = extract_text_from_elements(
                                cell_content, depth=depth + 1
                            )
                            if cell_text.strip():
                                text_lines.append(cell_text)
            return "".join(text_lines)

        def process_tab_hierarchy(tab, level=0):
            """Process a tab and its nested child tabs recursively"""
            tab_text = ""

            if "documentTab" in tab:
                props = tab.get("tabProperties", {})
                tab_title = props.get("title", "Untitled Tab")
                tab_id = props.get("tabId", "Unknown ID")
                # Add indentation for nested tabs to show hierarchy
                if level > 0:
                    tab_title = "    " * level + f"{tab_title} ( ID: {tab_id})"
                tab_body = tab.get("documentTab", {}).get("body", {}).get("content", [])
                tab_text += extract_text_from_elements(tab_body, tab_title)

            # Process child tabs (nested tabs)
            child_tabs = tab.get("childTabs", [])
            for child_tab in child_tabs:
                tab_text += process_tab_hierarchy(child_tab, level + 1)

            return tab_text

        processed_text_lines = []

        # Process main document body
        body_elements = doc_data.get("body", {}).get("content", [])
        main_content = extract_text_from_elements(body_elements)
        if main_content.strip():
            processed_text_lines.append(main_content)

        # Process all tabs
        tabs = doc_data.get("tabs", [])
        for tab in tabs:
            tab_content = process_tab_hierarchy(tab)
            if tab_content.strip():
                processed_text_lines.append(tab_content)

        body_text = "".join(processed_text_lines)
    else:
        logger.info(
            f"[get_doc_content] Processing as Drive file (e.g., .docx, other). MimeType: {mime_type}"
        )

        export_mime_type_map = {
            # Example: "application/vnd.google-apps.spreadsheet"z: "text/csv",
            # Native GSuite types that are not Docs would go here if this function
            # was intended to export them. For .docx, direct download is used.
        }
        effective_export_mime = export_mime_type_map.get(mime_type)

        request_obj = (
            drive_service.files().export_media(
                fileId=file_id,
                mimeType=effective_export_mime,
                supportsAllDrives=True,
            )
            if effective_export_mime
            else drive_service.files().get_media(
                fileId=file_id, supportsAllDrives=True
            )
        )

        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request_obj)
        loop = asyncio.get_event_loop()
        done = False
        while not done:
            status, done = await loop.run_in_executor(None, downloader.next_chunk)

        file_content_bytes = fh.getvalue()

        office_text = extract_office_xml_text(file_content_bytes, mime_type)
        if office_text:
            body_text = office_text
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
        f"Link: {web_view_link}\n\n--- CONTENT ---\n"
    )
    return header + body_text


@server.tool()
@handle_http_errors("list_docs_in_folder", is_read_only=True, service_type="docs")
@require_google_service("drive", "drive_read")
async def list_docs_in_folder(
    service: Any, user_google_email: str = "", folder_id: str = "root", page_size: int = 100
) -> str:
    """List Google Docs in a Drive folder. Returns doc names, IDs, and links."""
    logger.info(
        f"[list_docs_in_folder] Invoked. Email: '{user_google_email}', Folder ID: '{folder_id}'"
    )

    rsp = await asyncio.to_thread(
        service.files()
        .list(
            q=f"'{folder_id}' in parents and mimeType='application/vnd.google-apps.document' and trashed=false",
            pageSize=page_size,
            fields="files(id, name, modifiedTime, webViewLink)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        .execute
    )
    items = rsp.get("files", [])
    if not items:
        return f"No Google Docs found in folder '{folder_id}'."
    out = [f"Found {len(items)} Docs in folder '{folder_id}':"]
    for f in items:
        out.append(
            f"- {f['name']} (ID: {f['id']}) Modified: {f.get('modifiedTime')} Link: {f.get('webViewLink')}"
        )
    return "\n".join(out)


@server.tool()
@handle_http_errors("create_doc", service_type="docs")
@require_google_service("docs", "docs_write")
async def create_doc(
    service: Any,
    user_google_email: str = "",
    title: str = "",
    content: str = "",
) -> str:
    """Create a new Google Doc, optionally with initial content. Returns new doc ID and link."""
    logger.info(f"[create_doc] Title='{title}'")

    doc = await asyncio.to_thread(
        service.documents().create(body={"title": title}).execute
    )
    doc_id = doc.get("documentId")
    if content:
        requests = [{"insertText": {"location": {"index": 1}, "text": content}}]
        await asyncio.to_thread(
            service.documents()
            .batchUpdate(documentId=doc_id, body={"requests": requests})
            .execute
        )
    link = f"https://docs.google.com/document/d/{doc_id}/edit"
    msg = f"Created Google Doc '{title}' (ID: {doc_id}). Link: {link}"
    logger.info(f"[create_doc] Created '{title}' (ID: {doc_id})")
    return msg


@server.tool()
@handle_http_errors("modify_doc_text", service_type="docs")
@require_google_service("docs", "docs_write")
async def modify_doc_text(
    service: Any,
    user_google_email: str = "",
    file_id: str = "",
    start_index: int = 0,
    end_index: int = None,
    text: str = None,
    bold: bool = None,
    italic: bool = None,
    underline: bool = None,
    font_size: int = None,
    font_family: str = None,
    text_color: str = None,
    background_color: str = None,
    strikethrough: bool = None,
    superscript: bool = None,
    subscript: bool = None,
    link_url: str = None,
) -> str:
    """Insert/replace text and apply character formatting at a position in a Google Doc."""
    all_formatting = [bold, italic, underline, font_size, font_family, text_color,
                       background_color, strikethrough, superscript, subscript, link_url]
    logger.info(
        f"[modify_doc_text] Doc={file_id}, start={start_index}, end={end_index}, text={text is not None}, "
        f"formatting={any(p is not None for p in all_formatting)}"
    )

    # Input validation
    validator = ValidationManager()

    is_valid, error_msg = validator.validate_document_id(file_id)
    if not is_valid:
        return f"Error: {error_msg}"

    has_formatting = any(
        [
            bold is not None,
            italic is not None,
            underline is not None,
            font_size,
            font_family,
            text_color,
            background_color,
            strikethrough is not None,
            superscript is not None,
            subscript is not None,
            link_url,
        ]
    )

    # Validate that we have something to do
    if text is None and not has_formatting:
        return "Error: Must provide either 'text' to insert/replace, or formatting parameters (bold, italic, underline, strikethrough, superscript, subscript, link_url, font_size, font_family, text_color, background_color)."

    # Validate text formatting params if provided
    if has_formatting:
        is_valid, error_msg = validator.validate_text_formatting_params(
            bold,
            italic,
            underline,
            font_size,
            font_family,
            text_color,
            background_color,
            strikethrough,
            superscript,
            subscript,
            link_url,
        )
        if not is_valid:
            return f"Error: {error_msg}"

        # For formatting, we need end_index
        if end_index is None:
            return "Error: 'end_index' is required when applying formatting."

        is_valid, error_msg = validator.validate_index_range(start_index, end_index)
        if not is_valid:
            return f"Error: {error_msg}"

    requests = []
    operations = []

    # Handle text insertion/replacement
    if text is not None:
        if end_index is not None and end_index > start_index:
            # Text replacement
            if start_index == 0:
                # Special case: Cannot delete at index 0 (first section break)
                # Instead, we insert new text at index 1 and then delete the old text
                requests.append(create_insert_text_request(1, text))
                adjusted_end = end_index + len(text)
                requests.append(
                    create_delete_range_request(1 + len(text), adjusted_end)
                )
                operations.append(
                    f"Replaced text from index {start_index} to {end_index}"
                )
            else:
                # Normal replacement: delete old text, then insert new text
                requests.extend(
                    [
                        create_delete_range_request(start_index, end_index),
                        create_insert_text_request(start_index, text),
                    ]
                )
                operations.append(
                    f"Replaced text from index {start_index} to {end_index}"
                )
        else:
            # Text insertion
            actual_index = 1 if start_index == 0 else start_index
            requests.append(create_insert_text_request(actual_index, text))
            operations.append(f"Inserted text at index {start_index}")

    # Handle formatting
    if has_formatting:
        # Adjust range for formatting based on text operations
        format_start = start_index
        format_end = end_index

        if text is not None:
            if end_index is not None and end_index > start_index:
                # Text was replaced - format the new text
                format_end = start_index + len(text)
            else:
                # Text was inserted - format the inserted text
                actual_index = 1 if start_index == 0 else start_index
                format_start = actual_index
                format_end = actual_index + len(text)

        # Handle special case for formatting at index 0
        if format_start == 0:
            format_start = 1
        if format_end is not None and format_end <= format_start:
            format_end = format_start + 1

        requests.append(
            create_format_text_request(
                format_start,
                format_end,
                bold,
                italic,
                underline,
                font_size,
                font_family,
                text_color,
                background_color,
                strikethrough,
                superscript,
                subscript,
                link_url,
            )
        )

        format_details = []
        if bold is not None:
            format_details.append(f"bold={bold}")
        if italic is not None:
            format_details.append(f"italic={italic}")
        if underline is not None:
            format_details.append(f"underline={underline}")
        if strikethrough is not None:
            format_details.append(f"strikethrough={strikethrough}")
        if superscript is not None:
            format_details.append(f"superscript={superscript}")
        if subscript is not None:
            format_details.append(f"subscript={subscript}")
        if link_url:
            format_details.append(f"link_url={link_url}")
        if font_size:
            format_details.append(f"font_size={font_size}")
        if font_family:
            format_details.append(f"font_family={font_family}")
        if text_color:
            format_details.append(f"text_color={text_color}")
        if background_color:
            format_details.append(f"background_color={background_color}")

        operations.append(
            f"Applied formatting ({', '.join(format_details)}) to range {format_start}-{format_end}"
        )

    await asyncio.to_thread(
        service.documents()
        .batchUpdate(documentId=file_id, body={"requests": requests})
        .execute
    )

    operation_summary = "; ".join(operations)
    text_info = f" Text length: {len(text)} characters." if text else ""
    return f"{operation_summary}.{text_info}"


@server.tool()
@handle_http_errors("find_and_replace_doc", service_type="docs")
@require_google_service("docs", "docs_write")
async def find_and_replace_doc(
    service: Any,
    user_google_email: str = "",
    file_id: str = "",
    find_text: str = "",
    replace_text: str = "",
    match_case: bool = False,
) -> str:
    """Find and replace all occurrences of text in a Google Doc."""
    logger.info(
        f"[find_and_replace_doc] Doc={file_id}, find='{find_text}', replace='{replace_text}'"
    )

    requests = [create_find_replace_request(find_text, replace_text, match_case)]

    result = await asyncio.to_thread(
        service.documents()
        .batchUpdate(documentId=file_id, body={"requests": requests})
        .execute
    )

    # Extract number of replacements from response
    replacements = 0
    if "replies" in result and result["replies"]:
        reply = result["replies"][0]
        if "replaceAllText" in reply:
            replacements = reply["replaceAllText"].get("occurrencesChanged", 0)

    return f"Replaced {replacements} occurrence(s) of '{find_text}' with '{replace_text}'."


@server.tool()
@handle_http_errors("insert_table", service_type="docs")
@require_google_service("docs", "docs_write")
async def insert_table(
    service: Any,
    user_google_email: str = "",
    file_id: str = "",
    index: int = 0,
    rows: int = 0,
    columns: int = 0,
) -> str:
    """Insert a table at the given index in a Google Doc."""
    logger.info(f"[insert_table] Doc={file_id}, index={index}, rows={rows}, columns={columns}")

    if not rows or not columns:
        return "Error: 'rows' and 'columns' are required."

    if index == 0:
        index = 1

    requests = [create_insert_table_request(index, rows, columns)]

    await asyncio.to_thread(
        service.documents()
        .batchUpdate(documentId=file_id, body={"requests": requests})
        .execute
    )

    return f"Inserted table ({rows}x{columns}) at index {index}."


@server.tool()
@handle_http_errors("insert_list", service_type="docs")
@require_google_service("docs", "docs_write")
async def insert_list(
    service: Any,
    user_google_email: str = "",
    file_id: str = "",
    index: int = 0,
    list_type: str = "UNORDERED",
    text: str = "List item",
) -> str:
    """Insert a bullet or numbered list. list_type: UNORDERED or ORDERED."""
    logger.info(f"[insert_list] Doc={file_id}, index={index}, list_type={list_type}")

    if index == 0:
        index = 1

    requests = [
        create_insert_text_request(index, text + "\n"),
        create_bullet_list_request(index, index + len(text), list_type),
    ]

    await asyncio.to_thread(
        service.documents()
        .batchUpdate(documentId=file_id, body={"requests": requests})
        .execute
    )

    return f"Inserted {list_type.lower()} list at index {index}."


@server.tool()
@handle_http_errors("insert_page_break", service_type="docs")
@require_google_service("docs", "docs_write")
async def insert_page_break(
    service: Any,
    user_google_email: str = "",
    file_id: str = "",
    index: int = 0,
) -> str:
    """Insert a page break at the given index in a Google Doc."""
    logger.info(f"[insert_page_break] Doc={file_id}, index={index}")

    if index == 0:
        index = 1

    requests = [create_insert_page_break_request(index)]

    await asyncio.to_thread(
        service.documents()
        .batchUpdate(documentId=file_id, body={"requests": requests})
        .execute
    )

    return f"Inserted page break at index {index}."


@server.tool()
@handle_http_errors("insert_doc_image", service_type="docs")
@require_multiple_services(
    [
        {"service_type": "docs", "scopes": "docs_write", "param_name": "docs_service"},
        {
            "service_type": "drive",
            "scopes": "drive_read",
            "param_name": "drive_service",
        },
    ]
)
async def insert_doc_image(
    docs_service: Any,
    drive_service: Any,
    user_google_email: str = "",
    file_id: str = "",
    image_source: str = "",
    index: int = 0,
    width: int = 0,
    height: int = 0,
) -> str:
    """Insert an image into a Google Doc from a Drive file ID or public URL."""
    logger.info(
        f"[insert_doc_image] Doc={file_id}, source={image_source}, index={index}"
    )

    # Handle the special case where we can't insert at the first section break
    # If index is 0, bump it to 1 to avoid the section break
    if index == 0:
        logger.debug("Adjusting index from 0 to 1 to avoid first section break")
        index = 1

    # Determine if source is a Drive file ID or URL
    is_drive_file = not (
        image_source.startswith("http://") or image_source.startswith("https://")
    )

    if is_drive_file:
        # Verify Drive file exists and get metadata
        try:
            file_metadata = await asyncio.to_thread(
                drive_service.files()
                .get(
                    fileId=image_source,
                    fields="id, name, mimeType",
                    supportsAllDrives=True,
                )
                .execute
            )
            mime_type = file_metadata.get("mimeType", "")
            if not mime_type.startswith("image/"):
                return f"Error: File {image_source} is not an image (MIME type: {mime_type})."

            image_uri = f"https://drive.google.com/uc?id={image_source}"
            source_description = f"Drive file {file_metadata.get('name', image_source)}"
        except Exception as e:
            return f"Error: Could not access Drive file {image_source}: {str(e)}"
    else:
        image_uri = image_source
        source_description = "URL image"

    # Use helper to create image request
    requests = [create_insert_image_request(index, image_uri, width, height)]

    await asyncio.to_thread(
        docs_service.documents()
        .batchUpdate(documentId=file_id, body={"requests": requests})
        .execute
    )

    size_info = ""
    if width or height:
        size_info = f" (size: {width or 'auto'}x{height or 'auto'} points)"

    return f"Inserted {source_description}{size_info} at index {index}."


@server.tool()
@handle_http_errors("update_doc_headers_footers", service_type="docs")
@require_google_service("docs", "docs_write")
async def update_doc_headers_footers(
    service: Any,
    user_google_email: str = "",
    file_id: str = "",
    section_type: str = "",
    content: str = "",
    header_footer_type: str = "DEFAULT",
) -> str:
    """Update headers or footers in a Google Doc. section_type: "header" or "footer". header_footer_type: "DEFAULT", "FIRST_PAGE_ONLY", or "EVEN_PAGE"."""
    logger.info(f"[update_doc_headers_footers] Doc={file_id}, type={section_type}")

    # Input validation
    validator = ValidationManager()

    is_valid, error_msg = validator.validate_document_id(file_id)
    if not is_valid:
        return f"Error: {error_msg}"

    is_valid, error_msg = validator.validate_header_footer_params(
        section_type, header_footer_type
    )
    if not is_valid:
        return f"Error: {error_msg}"

    is_valid, error_msg = validator.validate_text_content(content)
    if not is_valid:
        return f"Error: {error_msg}"

    # Use HeaderFooterManager to handle the complex logic
    header_footer_manager = HeaderFooterManager(service)

    success, message = await header_footer_manager.update_header_footer_content(
        file_id, section_type, content, header_footer_type
    )

    if success:
        return message
    else:
        return f"Error: {message}"


@server.tool()
@handle_http_errors("batch_update_doc", service_type="docs")
@require_google_service("docs", "docs_write")
async def batch_update_doc(
    service: Any,
    user_google_email: str = "",
    file_id: str = "",
    operations: List[Dict[str, Any]] = None,
) -> str:
    """Execute multiple document operations in a single atomic batch. Each op needs a 'type' key (insert_text, delete_text, replace_text, format_text, insert_table, insert_page_break, find_replace, update_paragraph_style, insert_section_break)."""
    logger.debug(f"[batch_update_doc] Doc={file_id}, operations={len(operations)}")

    # Input validation
    validator = ValidationManager()

    is_valid, error_msg = validator.validate_document_id(file_id)
    if not is_valid:
        return f"Error: {error_msg}"

    is_valid, error_msg = validator.validate_batch_operations(operations)
    if not is_valid:
        return f"Error: {error_msg}"

    # Use BatchOperationManager to handle the complex logic
    batch_manager = BatchOperationManager(service)

    success, message, metadata = await batch_manager.execute_batch_operations(
        file_id, operations
    )

    if success:
        replies_count = metadata.get("replies_count", 0)
        return f"{message}. API replies: {replies_count}."
    else:
        return f"Error: {message}"


@server.tool()
@handle_http_errors("inspect_doc_structure", is_read_only=True, service_type="docs")
@require_google_service("docs", "docs_read")
async def inspect_doc_structure(
    service: Any,
    user_google_email: str = "",
    file_id: str = "",
    detailed: bool = False,
) -> str:
    """Analyze document structure to find safe insertion indices and table positions. Use total_length for safe insertion. Call before table operations."""
    logger.debug(f"[inspect_doc_structure] Doc={file_id}, detailed={detailed}")

    # Get the document
    doc = await asyncio.to_thread(
        service.documents().get(documentId=file_id).execute
    )

    if detailed:
        # Return full parsed structure
        structure = parse_document_structure(doc)

        # Simplify for JSON serialization
        result = {
            "title": structure["title"],
            "total_length": structure["total_length"],
            "statistics": {
                "elements": len(structure["body"]),
                "tables": len(structure["tables"]),
                "paragraphs": sum(
                    1 for e in structure["body"] if e.get("type") == "paragraph"
                ),
                "has_headers": bool(structure["headers"]),
                "has_footers": bool(structure["footers"]),
            },
            "elements": [],
        }

        # Add element summaries
        for element in structure["body"]:
            elem_summary = {
                "type": element["type"],
                "start_index": element["start_index"],
                "end_index": element["end_index"],
            }

            if element["type"] == "table":
                elem_summary["rows"] = element["rows"]
                elem_summary["columns"] = element["columns"]
                elem_summary["cell_count"] = len(element.get("cells", []))
            elif element["type"] == "paragraph":
                elem_summary["text_preview"] = element.get("text", "")[:100]

            result["elements"].append(elem_summary)

        # Add table details
        if structure["tables"]:
            result["tables"] = []
            for i, table in enumerate(structure["tables"]):
                table_data = extract_table_as_data(table)
                result["tables"].append(
                    {
                        "index": i,
                        "position": {
                            "start": table["start_index"],
                            "end": table["end_index"],
                        },
                        "dimensions": {
                            "rows": table["rows"],
                            "columns": table["columns"],
                        },
                        "preview": table_data[:3] if table_data else [],  # First 3 rows
                    }
                )

    else:
        # Return basic analysis
        result = analyze_document_complexity(doc)

        # Add table information
        tables = find_tables(doc)
        if tables:
            result["table_details"] = []
            for i, table in enumerate(tables):
                result["table_details"].append(
                    {
                        "index": i,
                        "rows": table["rows"],
                        "columns": table["columns"],
                        "start_index": table["start_index"],
                        "end_index": table["end_index"],
                    }
                )

    return json.dumps(result, indent=2)


@server.tool()
@handle_http_errors("create_table_with_data", service_type="docs")
@require_google_service("docs", "docs_write")
async def create_table_with_data(
    service: Any,
    user_google_email: str = "",
    file_id: str = "",
    table_data: List[List[str]] = None,
    index: int = 0,
    bold_headers: bool = True,
) -> str:
    """Create and populate a table in one operation. Get index from inspect_doc_structure total_length. table_data: 2D list of strings, all rows same width, use "" for empty cells."""
    logger.debug(f"[create_table_with_data] Doc={file_id}, index={index}")

    # Input validation
    validator = ValidationManager()

    is_valid, error_msg = validator.validate_document_id(file_id)
    if not is_valid:
        return f"ERROR: {error_msg}"

    is_valid, error_msg = validator.validate_table_data(table_data)
    if not is_valid:
        return f"ERROR: {error_msg}"

    is_valid, error_msg = validator.validate_index(index, "Index")
    if not is_valid:
        return f"ERROR: {error_msg}"

    # Use TableOperationManager to handle the complex logic
    table_manager = TableOperationManager(service)

    # Try to create the table, and if it fails due to index being at document end, retry with index-1
    success, message, metadata = await table_manager.create_and_populate_table(
        file_id, table_data, index, bold_headers
    )

    # If it failed due to index being at or beyond document end, retry with adjusted index
    if not success and "must be less than the end index" in message:
        logger.debug(
            f"Index {index} is at document boundary, retrying with index {index - 1}"
        )
        success, message, metadata = await table_manager.create_and_populate_table(
            file_id, table_data, index - 1, bold_headers
        )

    if success:
        rows = metadata.get("rows", 0)
        columns = metadata.get("columns", 0)
        return f"SUCCESS: {message}. Table: {rows}x{columns}."
    else:
        return f"ERROR: {message}"


@server.tool()
@handle_http_errors("debug_table_structure", is_read_only=True, service_type="docs")
@require_google_service("docs", "docs_read")
async def debug_table_structure(
    service: Any,
    user_google_email: str = "",
    file_id: str = "",
    table_index: int = 0,
) -> str:
    """Inspect a table's dimensions, cell positions, content, and insertion indices. Use after table creation or when debugging table issues."""
    logger.debug(
        f"[debug_table_structure] Doc={file_id}, table_index={table_index}"
    )

    # Get the document
    doc = await asyncio.to_thread(
        service.documents().get(documentId=file_id).execute
    )

    # Find tables
    tables = find_tables(doc)
    if table_index >= len(tables):
        return f"Error: Table index {table_index} not found. Document has {len(tables)} table(s)."

    table_info = tables[table_index]

    # Extract detailed cell information
    debug_info = {
        "table_index": table_index,
        "dimensions": f"{table_info['rows']}x{table_info['columns']}",
        "table_range": f"[{table_info['start_index']}-{table_info['end_index']}]",
        "cells": [],
    }

    for row_idx, row in enumerate(table_info["cells"]):
        row_info = []
        for col_idx, cell in enumerate(row):
            cell_debug = {
                "position": f"({row_idx},{col_idx})",
                "range": f"[{cell['start_index']}-{cell['end_index']}]",
                "insertion_index": cell.get("insertion_index", "N/A"),
                "current_content": repr(cell.get("content", "")),
                "content_elements_count": len(cell.get("content_elements", [])),
            }
            row_info.append(cell_debug)
        debug_info["cells"].append(row_info)

    return json.dumps(debug_info, indent=2)


@server.tool()
@handle_http_errors("export_doc_to_pdf", service_type="drive")
@require_google_service("drive", "drive_file")
async def export_doc_to_pdf(
    service: Any,
    user_google_email: str = "",
    file_id: str = "",
    pdf_filename: str = None,
    folder_id: str = None,
) -> str:
    """Export a Google Doc to PDF and save to Drive. Returns new PDF file ID and link."""
    logger.info(
        f"[export_doc_to_pdf] Doc={file_id}, pdf_filename={pdf_filename}, folder_id={folder_id}"
    )

    # Get file metadata first to validate it's a Google Doc
    try:
        file_metadata = await asyncio.to_thread(
            service.files()
            .get(
                fileId=file_id,
                fields="id, name, mimeType, webViewLink",
                supportsAllDrives=True,
            )
            .execute
        )
    except Exception as e:
        return f"Error: Could not access document {file_id}: {str(e)}"

    mime_type = file_metadata.get("mimeType", "")
    original_name = file_metadata.get("name", "Unknown Document")
    web_view_link = file_metadata.get("webViewLink", "#")

    # Verify it's a Google Doc
    if mime_type != "application/vnd.google-apps.document":
        return f"Error: File '{original_name}' is not a Google Doc (MIME type: {mime_type}). Only native Google Docs can be exported to PDF."

    logger.info(f"[export_doc_to_pdf] Exporting '{original_name}' to PDF")

    # Export the document as PDF
    try:
        request_obj = service.files().export_media(
            fileId=file_id, mimeType="application/pdf"
        )

        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request_obj)

        done = False
        while not done:
            _, done = await asyncio.to_thread(downloader.next_chunk)

        pdf_content = fh.getvalue()
        pdf_size = len(pdf_content)

    except Exception as e:
        return f"Error: Failed to export document to PDF: {str(e)}"

    # Determine PDF filename
    if not pdf_filename:
        pdf_filename = f"{original_name}_PDF.pdf"
    elif not pdf_filename.endswith(".pdf"):
        pdf_filename += ".pdf"

    # Upload PDF to Drive
    try:
        # Reuse the existing BytesIO object by resetting to the beginning
        fh.seek(0)
        # Create media upload object
        media = MediaIoBaseUpload(fh, mimetype="application/pdf", resumable=True)

        # Prepare file metadata for upload
        file_metadata = {"name": pdf_filename, "mimeType": "application/pdf"}

        # Add parent folder if specified
        if folder_id:
            file_metadata["parents"] = [folder_id]

        # Upload the file
        uploaded_file = await asyncio.to_thread(
            service.files()
            .create(
                body=file_metadata,
                media_body=media,
                fields="id, name, webViewLink, parents",
                supportsAllDrives=True,
            )
            .execute
        )

        pdf_file_id = uploaded_file.get("id")
        pdf_web_link = uploaded_file.get("webViewLink", "#")
        pdf_parents = uploaded_file.get("parents", [])

        logger.info(
            f"[export_doc_to_pdf] Successfully uploaded PDF to Drive: {pdf_file_id}"
        )

        return f"Exported to PDF '{pdf_filename}' (ID: {pdf_file_id}, {pdf_size:,} bytes). Link: {pdf_web_link}"

    except Exception as e:
        return f"Error: Failed to upload PDF to Drive: {str(e)}. PDF was generated successfully ({pdf_size:,} bytes) but could not be saved to Drive."


@server.tool()
@handle_http_errors("update_paragraph_style", service_type="docs")
@require_google_service("docs", "docs_write")
async def update_paragraph_style(
    service: Any,
    user_google_email: str = "",
    file_id: str = "",
    start_index: int = 0,
    end_index: int = 0,
    heading_type: str = None,
    alignment: str = None,
    line_spacing: float = None,
    space_above: float = None,
    space_below: float = None,
    indent_first_line: float = None,
    indent_start: float = None,
    indent_end: float = None,
) -> str:
    """Apply paragraph-level formatting to a range. heading_type: NORMAL_TEXT, HEADING_1-6, TITLE, SUBTITLE. alignment: START, CENTER, END, JUSTIFIED."""
    logger.info(
        f"[update_paragraph_style] Doc={file_id}, range={start_index}-{end_index}"
    )

    validator = ValidationManager()
    is_valid, error_msg = validator.validate_document_id(file_id)
    if not is_valid:
        return f"Error: {error_msg}"

    is_valid, error_msg = validator.validate_index_range(start_index, end_index)
    if not is_valid:
        return f"Error: {error_msg}"

    paragraph_style = {}
    fields = []

    if heading_type is not None:
        paragraph_style["namedStyleType"] = heading_type
        fields.append("namedStyleType")
    if alignment is not None:
        paragraph_style["alignment"] = alignment
        fields.append("alignment")
    if line_spacing is not None:
        paragraph_style["lineSpacing"] = line_spacing
        fields.append("lineSpacing")
    if space_above is not None:
        paragraph_style["spaceAbove"] = {"magnitude": space_above, "unit": "PT"}
        fields.append("spaceAbove")
    if space_below is not None:
        paragraph_style["spaceBelow"] = {"magnitude": space_below, "unit": "PT"}
        fields.append("spaceBelow")
    if indent_first_line is not None:
        paragraph_style["indentFirstLine"] = {"magnitude": indent_first_line, "unit": "PT"}
        fields.append("indentFirstLine")
    if indent_start is not None:
        paragraph_style["indentStart"] = {"magnitude": indent_start, "unit": "PT"}
        fields.append("indentStart")
    if indent_end is not None:
        paragraph_style["indentEnd"] = {"magnitude": indent_end, "unit": "PT"}
        fields.append("indentEnd")

    if not fields:
        return "Error: Must provide at least one paragraph style parameter."

    request = {
        "updateParagraphStyle": {
            "range": {"startIndex": start_index, "endIndex": end_index},
            "paragraphStyle": paragraph_style,
            "fields": ",".join(fields),
        }
    }

    await asyncio.to_thread(
        service.documents()
        .batchUpdate(documentId=file_id, body={"requests": [request]})
        .execute
    )

    style_details = ", ".join(f"{f}={paragraph_style.get(f, paragraph_style.get(f))}" for f in fields)
    return f"Applied paragraph style ({style_details}) to range {start_index}-{end_index}."


@server.tool()
@handle_http_errors("update_document_style", service_type="docs")
@require_google_service("docs", "docs_write")
async def update_document_style(
    service: Any,
    user_google_email: str = "",
    file_id: str = "",
    margin_top: float = None,
    margin_bottom: float = None,
    margin_left: float = None,
    margin_right: float = None,
    page_width: float = None,
    page_height: float = None,
    default_font_family: str = None,
    default_font_size: float = None,
) -> str:
    """Set page-level defaults (margins, page size, default font) for a Google Doc. Dimensions in points (US Letter: 612x792)."""
    logger.info(f"[update_document_style] Doc={file_id}")

    validator = ValidationManager()
    is_valid, error_msg = validator.validate_document_id(file_id)
    if not is_valid:
        return f"Error: {error_msg}"

    requests = []

    # Build document style request (margins and page size)
    doc_style = {}
    doc_fields = []

    if margin_top is not None:
        doc_style["marginTop"] = {"magnitude": margin_top, "unit": "PT"}
        doc_fields.append("marginTop")
    if margin_bottom is not None:
        doc_style["marginBottom"] = {"magnitude": margin_bottom, "unit": "PT"}
        doc_fields.append("marginBottom")
    if margin_left is not None:
        doc_style["marginLeft"] = {"magnitude": margin_left, "unit": "PT"}
        doc_fields.append("marginLeft")
    if margin_right is not None:
        doc_style["marginRight"] = {"magnitude": margin_right, "unit": "PT"}
        doc_fields.append("marginRight")
    if page_width is not None:
        doc_style.setdefault("pageSize", {})["width"] = {"magnitude": page_width, "unit": "PT"}
        doc_fields.append("pageSize.width")
    if page_height is not None:
        doc_style.setdefault("pageSize", {})["height"] = {"magnitude": page_height, "unit": "PT"}
        doc_fields.append("pageSize.height")

    if doc_fields:
        requests.append({
            "updateDocumentStyle": {
                "documentStyle": doc_style,
                "fields": ",".join(doc_fields),
            }
        })

    # Handle default font by fetching doc length and applying updateTextStyle to full body
    if default_font_family is not None or default_font_size is not None:
        # Get the document to find the body content length
        doc = await asyncio.to_thread(
            service.documents().get(documentId=file_id).execute
        )
        body_content = doc.get("body", {}).get("content", [])
        if body_content:
            end_index = body_content[-1].get("endIndex", 1)
        else:
            end_index = 1

        text_style = {}
        style_fields = []
        if default_font_family is not None:
            text_style["weightedFontFamily"] = {"fontFamily": default_font_family}
            style_fields.append("weightedFontFamily")
        if default_font_size is not None:
            text_style["fontSize"] = {"magnitude": default_font_size, "unit": "PT"}
            style_fields.append("fontSize")

        requests.append({
            "updateTextStyle": {
                "textStyle": text_style,
                "range": {
                    "startIndex": 1,
                    "endIndex": end_index - 1,
                },
                "fields": ",".join(style_fields),
            }
        })

    if not requests:
        return "Error: Must provide at least one document style parameter."

    await asyncio.to_thread(
        service.documents()
        .batchUpdate(documentId=file_id, body={"requests": requests})
        .execute
    )

    changes = []
    if margin_top is not None:
        changes.append(f"margin_top={margin_top}pt")
    if margin_bottom is not None:
        changes.append(f"margin_bottom={margin_bottom}pt")
    if margin_left is not None:
        changes.append(f"margin_left={margin_left}pt")
    if margin_right is not None:
        changes.append(f"margin_right={margin_right}pt")
    if page_width is not None:
        changes.append(f"page_width={page_width}pt")
    if page_height is not None:
        changes.append(f"page_height={page_height}pt")
    if default_font_family is not None:
        changes.append(f"font_family={default_font_family}")
    if default_font_size is not None:
        changes.append(f"font_size={default_font_size}pt")
    return f"Updated document style ({', '.join(changes)})."


@server.tool()
@handle_http_errors("manage_named_range", service_type="docs")
@require_google_service("docs", "docs_write")
async def manage_named_range(
    service: Any,
    user_google_email: str = "",
    file_id: str = "",
    action: str = "",
    name: str = "",
    start_index: int = None,
    end_index: int = None,
    named_range_id: str = None,
    replacement_text: str = None,
) -> str:
    """Create, delete, or replace content in named ranges. action: "create", "delete", or "replace_content"."""
    logger.info(
        f"[manage_named_range] Doc={file_id}, action={action}, name={name}"
    )

    validator = ValidationManager()
    is_valid, error_msg = validator.validate_document_id(file_id)
    if not is_valid:
        return f"Error: {error_msg}"

    if action not in ("create", "delete", "replace_content"):
        return "Error: action must be one of 'create', 'delete', or 'replace_content'."

    requests = []

    if action == "create":
        if start_index is None or end_index is None:
            return "Error: 'start_index' and 'end_index' are required for creating a named range."
        requests.append({
            "createNamedRange": {
                "name": name,
                "range": {"startIndex": start_index, "endIndex": end_index},
            }
        })
    elif action == "delete":
        if named_range_id:
            requests.append({
                "deleteNamedRange": {"namedRangeId": named_range_id}
            })
        else:
            requests.append({
                "deleteNamedRange": {"name": name}
            })
    elif action == "replace_content":
        if replacement_text is None:
            return "Error: 'replacement_text' is required for replace_content action."
        requests.append({
            "replaceNamedRangeContent": {
                "namedRangeName": name,
                "text": replacement_text,
            }
        })

    result = await asyncio.to_thread(
        service.documents()
        .batchUpdate(documentId=file_id, body={"requests": requests})
        .execute
    )

    if action == "create":
        # Extract the created named range ID from the response
        range_id = ""
        if "replies" in result and result["replies"]:
            reply = result["replies"][0]
            if "createNamedRange" in reply:
                range_id = reply["createNamedRange"].get("namedRangeId", "")
        return f"Created named range '{name}' (ID: {range_id}) at {start_index}-{end_index}."
    elif action == "delete":
        return f"Deleted named range '{name}'."
    else:
        return f"Replaced content of named range '{name}'."


@server.tool()
@handle_http_errors("manage_table_structure", service_type="docs")
@require_google_service("docs", "docs_write")
async def manage_table_structure(
    service: Any,
    user_google_email: str = "",
    file_id: str = "",
    action: str = "",
    table_start_index: int = 0,
    row_index: int = None,
    column_index: int = None,
    insert_below: bool = True,
    insert_right: bool = True,
    start_row: int = None,
    end_row: int = None,
    start_column: int = None,
    end_column: int = None,
) -> str:
    """Modify table structure: insert/delete rows/columns, merge/unmerge cells. action: insert_row, insert_column, delete_row, delete_column, merge_cells, unmerge_cells."""
    logger.info(
        f"[manage_table_structure] Doc={file_id}, action={action}, table_start={table_start_index}"
    )

    validator = ValidationManager()
    is_valid, error_msg = validator.validate_document_id(file_id)
    if not is_valid:
        return f"Error: {error_msg}"

    valid_actions = ("insert_row", "insert_column", "delete_row", "delete_column", "merge_cells", "unmerge_cells")
    if action not in valid_actions:
        return f"Error: action must be one of {', '.join(valid_actions)}."

    requests = []

    if action == "insert_row":
        if row_index is None:
            return "Error: 'row_index' is required for insert_row action."
        requests.append({
            "insertTableRow": {
                "tableCellLocation": {
                    "tableStartLocation": {"index": table_start_index},
                    "rowIndex": row_index,
                    "columnIndex": 0,
                },
                "insertBelow": insert_below,
            }
        })
    elif action == "insert_column":
        if column_index is None:
            return "Error: 'column_index' is required for insert_column action."
        requests.append({
            "insertTableColumn": {
                "tableCellLocation": {
                    "tableStartLocation": {"index": table_start_index},
                    "rowIndex": 0,
                    "columnIndex": column_index,
                },
                "insertRight": insert_right,
            }
        })
    elif action == "delete_row":
        if row_index is None:
            return "Error: 'row_index' is required for delete_row action."
        requests.append({
            "deleteTableRow": {
                "tableCellLocation": {
                    "tableStartLocation": {"index": table_start_index},
                    "rowIndex": row_index,
                    "columnIndex": 0,
                },
            }
        })
    elif action == "delete_column":
        if column_index is None:
            return "Error: 'column_index' is required for delete_column action."
        requests.append({
            "deleteTableColumn": {
                "tableCellLocation": {
                    "tableStartLocation": {"index": table_start_index},
                    "rowIndex": 0,
                    "columnIndex": column_index,
                },
            }
        })
    elif action in ("merge_cells", "unmerge_cells"):
        if any(v is None for v in [start_row, end_row, start_column, end_column]):
            return "Error: 'start_row', 'end_row', 'start_column', and 'end_column' are all required for merge/unmerge operations."
        table_range = {
            "tableCellLocation": {
                "tableStartLocation": {"index": table_start_index},
                "rowIndex": start_row,
                "columnIndex": start_column,
            },
            "rowSpan": end_row - start_row,
            "columnSpan": end_column - start_column,
        }
        if action == "merge_cells":
            requests.append({"mergeTableCells": {"tableRange": table_range}})
        else:
            requests.append({"unmergeTableCells": {"tableRange": table_range}})

    await asyncio.to_thread(
        service.documents()
        .batchUpdate(documentId=file_id, body={"requests": requests})
        .execute
    )

    return f"Performed '{action}' on table at index {table_start_index}."


@server.tool()
@handle_http_errors("insert_section_break", service_type="docs")
@require_google_service("docs", "docs_write")
async def insert_section_break(
    service: Any,
    user_google_email: str = "",
    file_id: str = "",
    index: int = 0,
    section_type: str = "NEXT_PAGE",
) -> str:
    """Insert a section break. section_type: "CONTINUOUS" or "NEXT_PAGE"."""
    logger.info(
        f"[insert_section_break] Doc={file_id}, index={index}, type={section_type}"
    )

    validator = ValidationManager()
    is_valid, error_msg = validator.validate_document_id(file_id)
    if not is_valid:
        return f"Error: {error_msg}"

    if section_type not in ("CONTINUOUS", "NEXT_PAGE"):
        return "Error: section_type must be 'CONTINUOUS' or 'NEXT_PAGE'."

    if index == 0:
        index = 1

    request = {
        "insertSectionBreak": {
            "location": {"index": index},
            "sectionType": section_type,
        }
    }

    await asyncio.to_thread(
        service.documents()
        .batchUpdate(documentId=file_id, body={"requests": [request]})
        .execute
    )

    return f"Inserted {section_type} section break at index {index}."


@server.tool()
@handle_http_errors("insert_footnote", service_type="docs")
@require_google_service("docs", "docs_write")
async def insert_footnote(
    service: Any,
    user_google_email: str = "",
    file_id: str = "",
    index: int = 0,
    footnote_text: str = None,
) -> str:
    """Insert a footnote reference at the given index, optionally with body text."""
    logger.info(
        f"[insert_footnote] Doc={file_id}, index={index}, has_text={footnote_text is not None}"
    )

    validator = ValidationManager()
    is_valid, error_msg = validator.validate_document_id(file_id)
    if not is_valid:
        return f"Error: {error_msg}"

    if index == 0:
        index = 1

    # Step 1: Create the footnote
    create_request = {
        "createFootnote": {
            "location": {"index": index},
        }
    }

    result = await asyncio.to_thread(
        service.documents()
        .batchUpdate(documentId=file_id, body={"requests": [create_request]})
        .execute
    )

    footnote_id = ""
    if "replies" in result and result["replies"]:
        reply = result["replies"][0]
        if "createFootnote" in reply:
            footnote_id = reply["createFootnote"].get("footnoteId", "")

    # Step 2: If footnote_text is provided, insert text into the footnote body
    if footnote_text and footnote_id:
        # Get the document to find the footnote content element index
        doc = await asyncio.to_thread(
            service.documents().get(documentId=file_id).execute
        )

        footnotes = doc.get("footnotes", {})
        footnote = footnotes.get(footnote_id, {})
        footnote_content = footnote.get("content", [])

        if footnote_content:
            # Find the first paragraph's start index in the footnote
            first_element = footnote_content[0]
            if "paragraph" in first_element:
                # Insert at the start of the footnote paragraph
                footnote_start = first_element.get("startIndex", 0)
                insert_request = {
                    "insertText": {
                        "location": {
                            "segmentId": footnote_id,
                            "index": footnote_start,
                        },
                        "text": footnote_text,
                    }
                }

                await asyncio.to_thread(
                    service.documents()
                    .batchUpdate(documentId=file_id, body={"requests": [insert_request]})
                    .execute
                )

    return f"Inserted footnote at index {index} (footnote ID: {footnote_id})."


@server.tool()
@handle_http_errors("delete_positioned_object", service_type="docs")
@require_google_service("docs", "docs_write")
async def delete_positioned_object(
    service: Any,
    user_google_email: str = "",
    file_id: str = "",
    object_id: str = "",
) -> str:
    """Delete a positioned object (e.g., image) from a Google Doc by its object ID."""
    logger.info(
        f"[delete_positioned_object] Doc={file_id}, object_id={object_id}"
    )

    validator = ValidationManager()
    is_valid, error_msg = validator.validate_document_id(file_id)
    if not is_valid:
        return f"Error: {error_msg}"

    if not object_id:
        return "Error: 'object_id' is required."

    request = {
        "deletePositionedObject": {
            "objectId": object_id,
        }
    }

    await asyncio.to_thread(
        service.documents()
        .batchUpdate(documentId=file_id, body={"requests": [request]})
        .execute
    )

    return f"Deleted positioned object '{object_id}'."


# Create comment management tools for documents
_comment_tools = create_comment_tools("document", "file_id")  # file_id_param used in generated tool signatures

# Extract and register the functions
read_doc_comments = _comment_tools["read_comments"]
create_doc_comment = _comment_tools["create_comment"]
reply_to_comment = _comment_tools["reply_to_comment"]
resolve_comment = _comment_tools["resolve_comment"]
edit_doc_comment = _comment_tools["edit_comment"]
delete_doc_comment = _comment_tools["delete_comment"]
edit_doc_comment_reply = _comment_tools["edit_reply"]
delete_doc_comment_reply = _comment_tools["delete_reply"]
