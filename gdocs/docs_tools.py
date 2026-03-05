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
    user_google_email: str,
    query: str,
    page_size: int = 10,
) -> str:
    """
    Searches for Google Docs by name using Drive API (mimeType filter).

    Returns:
        str: A formatted list of Google Docs matching the search query.
    """
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
    user_google_email: str,
    document_id: str,
) -> str:
    """
    Retrieves content of a Google Doc or a Drive file (like .docx) identified by document_id.
    - Native Google Docs: Fetches content via Docs API.
    - Office files (.docx, etc.) stored in Drive: Downloads via Drive API and extracts text.

    Returns:
        str: The document content with metadata header.
    """
    logger.info(
        f"[get_doc_content] Invoked. Document/File ID: '{document_id}' for user '{user_google_email}'"
    )

    # Step 2: Get file metadata from Drive
    file_metadata = await asyncio.to_thread(
        drive_service.files()
        .get(
            fileId=document_id,
            fields="id, name, mimeType, webViewLink",
            supportsAllDrives=True,
        )
        .execute
    )
    mime_type = file_metadata.get("mimeType", "")
    file_name = file_metadata.get("name", "Unknown File")
    web_view_link = file_metadata.get("webViewLink", "#")

    logger.info(
        f"[get_doc_content] File '{file_name}' (ID: {document_id}) has mimeType: '{mime_type}'"
    )

    body_text = ""  # Initialize body_text

    # Step 3: Process based on mimeType
    if mime_type == "application/vnd.google-apps.document":
        logger.info("[get_doc_content] Processing as native Google Doc.")
        doc_data = await asyncio.to_thread(
            docs_service.documents()
            .get(documentId=document_id, includeTabsContent=True)
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
                fileId=document_id,
                mimeType=effective_export_mime,
                supportsAllDrives=True,
            )
            if effective_export_mime
            else drive_service.files().get_media(
                fileId=document_id, supportsAllDrives=True
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
        f'File: "{file_name}" (ID: {document_id}, Type: {mime_type})\n'
        f"Link: {web_view_link}\n\n--- CONTENT ---\n"
    )
    return header + body_text


@server.tool()
@handle_http_errors("list_docs_in_folder", is_read_only=True, service_type="docs")
@require_google_service("drive", "drive_read")
async def list_docs_in_folder(
    service: Any, user_google_email: str, folder_id: str = "root", page_size: int = 100
) -> str:
    """
    Lists Google Docs within a specific Drive folder.

    Returns:
        str: A formatted list of Google Docs in the specified folder.
    """
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
    user_google_email: str,
    title: str,
    content: str = "",
) -> str:
    """
    Creates a new Google Doc and optionally inserts initial content.

    Returns:
        str: Confirmation message with document ID and link.
    """
    logger.info(f"[create_doc] Invoked. Email: '{user_google_email}', Title='{title}'")

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
    msg = f"Created Google Doc '{title}' (ID: {doc_id}) for {user_google_email}. Link: {link}"
    logger.info(
        f"Successfully created Google Doc '{title}' (ID: {doc_id}) for {user_google_email}. Link: {link}"
    )
    return msg


@server.tool()
@handle_http_errors("modify_doc_text", service_type="docs")
@require_google_service("docs", "docs_write")
async def modify_doc_text(
    service: Any,
    user_google_email: str,
    document_id: str,
    start_index: int,
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
    """
    Modifies text in a Google Doc - can insert/replace text and/or apply formatting in a single operation.

    Args:
        user_google_email: User's Google email address
        document_id: ID of the document to update
        start_index: Start position for operation (0-based)
        end_index: End position for text replacement/formatting (if not provided with text, text is inserted)
        text: New text to insert or replace with (optional - can format existing text without changing it)
        bold: Whether to make text bold (True/False/None to leave unchanged)
        italic: Whether to make text italic (True/False/None to leave unchanged)
        underline: Whether to underline text (True/False/None to leave unchanged)
        font_size: Font size in points
        font_family: Font family name (e.g., "Arial", "Times New Roman")
        text_color: Foreground text color (#RRGGBB)
        background_color: Background/highlight color (#RRGGBB)
        strikethrough: Whether to apply strikethrough (True/False/None to leave unchanged)
        superscript: Whether to make text superscript (True/False/None to leave unchanged)
        subscript: Whether to make text subscript (True/False/None to leave unchanged)
        link_url: URL to link the text to (string or None to leave unchanged)

    Returns:
        str: Confirmation message with operation details
    """
    all_formatting = [bold, italic, underline, font_size, font_family, text_color,
                       background_color, strikethrough, superscript, subscript, link_url]
    logger.info(
        f"[modify_doc_text] Doc={document_id}, start={start_index}, end={end_index}, text={text is not None}, "
        f"formatting={any(p is not None for p in all_formatting)}"
    )

    # Input validation
    validator = ValidationManager()

    is_valid, error_msg = validator.validate_document_id(document_id)
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
        .batchUpdate(documentId=document_id, body={"requests": requests})
        .execute
    )

    link = f"https://docs.google.com/document/d/{document_id}/edit"
    operation_summary = "; ".join(operations)
    text_info = f" Text length: {len(text)} characters." if text else ""
    return f"{operation_summary} in document {document_id}.{text_info} Link: {link}"


@server.tool()
@handle_http_errors("find_and_replace_doc", service_type="docs")
@require_google_service("docs", "docs_write")
async def find_and_replace_doc(
    service: Any,
    user_google_email: str,
    document_id: str,
    find_text: str,
    replace_text: str,
    match_case: bool = False,
) -> str:
    """
    Finds and replaces text throughout a Google Doc.

    Args:
        user_google_email: User's Google email address
        document_id: ID of the document to update
        find_text: Text to search for
        replace_text: Text to replace with
        match_case: Whether to match case exactly

    Returns:
        str: Confirmation message with replacement count
    """
    logger.info(
        f"[find_and_replace_doc] Doc={document_id}, find='{find_text}', replace='{replace_text}'"
    )

    requests = [create_find_replace_request(find_text, replace_text, match_case)]

    result = await asyncio.to_thread(
        service.documents()
        .batchUpdate(documentId=document_id, body={"requests": requests})
        .execute
    )

    # Extract number of replacements from response
    replacements = 0
    if "replies" in result and result["replies"]:
        reply = result["replies"][0]
        if "replaceAllText" in reply:
            replacements = reply["replaceAllText"].get("occurrencesChanged", 0)

    link = f"https://docs.google.com/document/d/{document_id}/edit"
    return f"Replaced {replacements} occurrence(s) of '{find_text}' with '{replace_text}' in document {document_id}. Link: {link}"


@server.tool()
@handle_http_errors("insert_doc_elements", service_type="docs")
@require_google_service("docs", "docs_write")
async def insert_doc_elements(
    service: Any,
    user_google_email: str,
    document_id: str,
    element_type: str,
    index: int,
    rows: int = None,
    columns: int = None,
    list_type: str = None,
    text: str = None,
) -> str:
    """
    Inserts structural elements like tables, lists, or page breaks into a Google Doc.

    Args:
        user_google_email: User's Google email address
        document_id: ID of the document to update
        element_type: Type of element to insert ("table", "list", "page_break")
        index: Position to insert element (0-based)
        rows: Number of rows for table (required for table)
        columns: Number of columns for table (required for table)
        list_type: Type of list ("UNORDERED", "ORDERED") (required for list)
        text: Initial text content for list items

    Returns:
        str: Confirmation message with insertion details
    """
    logger.info(
        f"[insert_doc_elements] Doc={document_id}, type={element_type}, index={index}"
    )

    # Handle the special case where we can't insert at the first section break
    # If index is 0, bump it to 1 to avoid the section break
    if index == 0:
        logger.debug("Adjusting index from 0 to 1 to avoid first section break")
        index = 1

    requests = []

    if element_type == "table":
        if not rows or not columns:
            return "Error: 'rows' and 'columns' parameters are required for table insertion."

        requests.append(create_insert_table_request(index, rows, columns))
        description = f"table ({rows}x{columns})"

    elif element_type == "list":
        if not list_type:
            return "Error: 'list_type' parameter is required for list insertion ('UNORDERED' or 'ORDERED')."

        if not text:
            text = "List item"

        # Insert text first, then create list
        requests.extend(
            [
                create_insert_text_request(index, text + "\n"),
                create_bullet_list_request(index, index + len(text), list_type),
            ]
        )
        description = f"{list_type.lower()} list"

    elif element_type == "page_break":
        requests.append(create_insert_page_break_request(index))
        description = "page break"

    else:
        return f"Error: Unsupported element type '{element_type}'. Supported types: 'table', 'list', 'page_break'."

    await asyncio.to_thread(
        service.documents()
        .batchUpdate(documentId=document_id, body={"requests": requests})
        .execute
    )

    link = f"https://docs.google.com/document/d/{document_id}/edit"
    return f"Inserted {description} at index {index} in document {document_id}. Link: {link}"


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
    user_google_email: str,
    document_id: str,
    image_source: str,
    index: int,
    width: int = 0,
    height: int = 0,
) -> str:
    """
    Inserts an image into a Google Doc from Drive or a URL.

    Args:
        user_google_email: User's Google email address
        document_id: ID of the document to update
        image_source: Drive file ID or public image URL
        index: Position to insert image (0-based)
        width: Image width in points (optional)
        height: Image height in points (optional)

    Returns:
        str: Confirmation message with insertion details
    """
    logger.info(
        f"[insert_doc_image] Doc={document_id}, source={image_source}, index={index}"
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
        .batchUpdate(documentId=document_id, body={"requests": requests})
        .execute
    )

    size_info = ""
    if width or height:
        size_info = f" (size: {width or 'auto'}x{height or 'auto'} points)"

    link = f"https://docs.google.com/document/d/{document_id}/edit"
    return f"Inserted {source_description}{size_info} at index {index} in document {document_id}. Link: {link}"


@server.tool()
@handle_http_errors("update_doc_headers_footers", service_type="docs")
@require_google_service("docs", "docs_write")
async def update_doc_headers_footers(
    service: Any,
    user_google_email: str,
    document_id: str,
    section_type: str,
    content: str,
    header_footer_type: str = "DEFAULT",
) -> str:
    """
    Updates headers or footers in a Google Doc.

    Args:
        user_google_email: User's Google email address
        document_id: ID of the document to update
        section_type: Type of section to update ("header" or "footer")
        content: Text content for the header/footer
        header_footer_type: Type of header/footer ("DEFAULT", "FIRST_PAGE_ONLY", "EVEN_PAGE")

    Returns:
        str: Confirmation message with update details
    """
    logger.info(f"[update_doc_headers_footers] Doc={document_id}, type={section_type}")

    # Input validation
    validator = ValidationManager()

    is_valid, error_msg = validator.validate_document_id(document_id)
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
        document_id, section_type, content, header_footer_type
    )

    if success:
        link = f"https://docs.google.com/document/d/{document_id}/edit"
        return f"{message}. Link: {link}"
    else:
        return f"Error: {message}"


@server.tool()
@handle_http_errors("batch_update_doc", service_type="docs")
@require_google_service("docs", "docs_write")
async def batch_update_doc(
    service: Any,
    user_google_email: str,
    document_id: str,
    operations: List[Dict[str, Any]],
) -> str:
    """
    Executes multiple document operations in a single atomic batch update.

    Args:
        user_google_email: User's Google email address
        document_id: ID of the document to update
        operations: List of operation dictionaries. Each operation should contain:
                   - type: Operation type ('insert_text', 'delete_text', 'replace_text', 'format_text', 'insert_table', 'insert_page_break')
                   - Additional parameters specific to each operation type

    Example operations:
        [
            {"type": "insert_text", "index": 1, "text": "Hello World"},
            {"type": "format_text", "start_index": 1, "end_index": 12, "bold": true},
            {"type": "insert_table", "index": 20, "rows": 2, "columns": 3}
        ]

    Returns:
        str: Confirmation message with batch operation results
    """
    logger.debug(f"[batch_update_doc] Doc={document_id}, operations={len(operations)}")

    # Input validation
    validator = ValidationManager()

    is_valid, error_msg = validator.validate_document_id(document_id)
    if not is_valid:
        return f"Error: {error_msg}"

    is_valid, error_msg = validator.validate_batch_operations(operations)
    if not is_valid:
        return f"Error: {error_msg}"

    # Use BatchOperationManager to handle the complex logic
    batch_manager = BatchOperationManager(service)

    success, message, metadata = await batch_manager.execute_batch_operations(
        document_id, operations
    )

    if success:
        link = f"https://docs.google.com/document/d/{document_id}/edit"
        replies_count = metadata.get("replies_count", 0)
        return f"{message} on document {document_id}. API replies: {replies_count}. Link: {link}"
    else:
        return f"Error: {message}"


@server.tool()
@handle_http_errors("inspect_doc_structure", is_read_only=True, service_type="docs")
@require_google_service("docs", "docs_read")
async def inspect_doc_structure(
    service: Any,
    user_google_email: str,
    document_id: str,
    detailed: bool = False,
) -> str:
    """
    Essential tool for finding safe insertion points and understanding document structure.

    USE THIS FOR:
    - Finding the correct index for table insertion
    - Understanding document layout before making changes
    - Locating existing tables and their positions
    - Getting document statistics and complexity info

    CRITICAL FOR TABLE OPERATIONS:
    ALWAYS call this BEFORE creating tables to get a safe insertion index.

    WHAT THE OUTPUT SHOWS:
    - total_elements: Number of document elements
    - total_length: Maximum safe index for insertion
    - tables: Number of existing tables
    - table_details: Position and dimensions of each table

    WORKFLOW:
    Step 1: Call this function
    Step 2: Note the "total_length" value
    Step 3: Use an index < total_length for table insertion
    Step 4: Create your table

    Args:
        user_google_email: User's Google email address
        document_id: ID of the document to inspect
        detailed: Whether to return detailed structure information

    Returns:
        str: JSON string containing document structure and safe insertion indices
    """
    logger.debug(f"[inspect_doc_structure] Doc={document_id}, detailed={detailed}")

    # Get the document
    doc = await asyncio.to_thread(
        service.documents().get(documentId=document_id).execute
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

    link = f"https://docs.google.com/document/d/{document_id}/edit"
    return f"Document structure analysis for {document_id}:\n\n{json.dumps(result, indent=2)}\n\nLink: {link}"


@server.tool()
@handle_http_errors("create_table_with_data", service_type="docs")
@require_google_service("docs", "docs_write")
async def create_table_with_data(
    service: Any,
    user_google_email: str,
    document_id: str,
    table_data: List[List[str]],
    index: int,
    bold_headers: bool = True,
) -> str:
    """
    Creates a table and populates it with data in one reliable operation.

    CRITICAL: YOU MUST CALL inspect_doc_structure FIRST TO GET THE INDEX!

    MANDATORY WORKFLOW - DO THESE STEPS IN ORDER:

    Step 1: ALWAYS call inspect_doc_structure first
    Step 2: Use the 'total_length' value from inspect_doc_structure as your index
    Step 3: Format data as 2D list: [["col1", "col2"], ["row1col1", "row1col2"]]
    Step 4: Call this function with the correct index and data

    EXAMPLE DATA FORMAT:
    table_data = [
        ["Header1", "Header2", "Header3"],    # Row 0 - headers
        ["Data1", "Data2", "Data3"],          # Row 1 - first data row
        ["Data4", "Data5", "Data6"]           # Row 2 - second data row
    ]

    CRITICAL INDEX REQUIREMENTS:
    - NEVER use index values like 1, 2, 10 without calling inspect_doc_structure first
    - ALWAYS get index from inspect_doc_structure 'total_length' field
    - Index must be a valid insertion point in the document

    DATA FORMAT REQUIREMENTS:
    - Must be 2D list of strings only
    - Each inner list = one table row
    - All rows MUST have same number of columns
    - Use empty strings "" for empty cells, never None
    - Use debug_table_structure after creation to verify results

    Args:
        user_google_email: User's Google email address
        document_id: ID of the document to update
        table_data: 2D list of strings - EXACT format: [["col1", "col2"], ["row1col1", "row1col2"]]
        index: Document position (MANDATORY: get from inspect_doc_structure 'total_length')
        bold_headers: Whether to make first row bold (default: true)

    Returns:
        str: Confirmation with table details and link
    """
    logger.debug(f"[create_table_with_data] Doc={document_id}, index={index}")

    # Input validation
    validator = ValidationManager()

    is_valid, error_msg = validator.validate_document_id(document_id)
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
        document_id, table_data, index, bold_headers
    )

    # If it failed due to index being at or beyond document end, retry with adjusted index
    if not success and "must be less than the end index" in message:
        logger.debug(
            f"Index {index} is at document boundary, retrying with index {index - 1}"
        )
        success, message, metadata = await table_manager.create_and_populate_table(
            document_id, table_data, index - 1, bold_headers
        )

    if success:
        link = f"https://docs.google.com/document/d/{document_id}/edit"
        rows = metadata.get("rows", 0)
        columns = metadata.get("columns", 0)

        return (
            f"SUCCESS: {message}. Table: {rows}x{columns}, Index: {index}. Link: {link}"
        )
    else:
        return f"ERROR: {message}"


@server.tool()
@handle_http_errors("debug_table_structure", is_read_only=True, service_type="docs")
@require_google_service("docs", "docs_read")
async def debug_table_structure(
    service: Any,
    user_google_email: str,
    document_id: str,
    table_index: int = 0,
) -> str:
    """
    ESSENTIAL DEBUGGING TOOL - Use this whenever tables don't work as expected.

    USE THIS IMMEDIATELY WHEN:
    - Table population put data in wrong cells
    - You get "table not found" errors
    - Data appears concatenated in first cell
    - Need to understand existing table structure
    - Planning to use populate_existing_table

    WHAT THIS SHOWS YOU:
    - Exact table dimensions (rows × columns)
    - Each cell's position coordinates (row,col)
    - Current content in each cell
    - Insertion indices for each cell
    - Table boundaries and ranges

    HOW TO READ THE OUTPUT:
    - "dimensions": "2x3" = 2 rows, 3 columns
    - "position": "(0,0)" = first row, first column
    - "current_content": What's actually in each cell right now
    - "insertion_index": Where new text would be inserted in that cell

    WORKFLOW INTEGRATION:
    1. After creating table → Use this to verify structure
    2. Before populating → Use this to plan your data format
    3. After population fails → Use this to see what went wrong
    4. When debugging → Compare your data array to actual table structure

    Args:
        user_google_email: User's Google email address
        document_id: ID of the document to inspect
        table_index: Which table to debug (0 = first table, 1 = second table, etc.)

    Returns:
        str: Detailed JSON structure showing table layout, cell positions, and current content
    """
    logger.debug(
        f"[debug_table_structure] Doc={document_id}, table_index={table_index}"
    )

    # Get the document
    doc = await asyncio.to_thread(
        service.documents().get(documentId=document_id).execute
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

    link = f"https://docs.google.com/document/d/{document_id}/edit"
    return f"Table structure debug for table {table_index}:\n\n{json.dumps(debug_info, indent=2)}\n\nLink: {link}"


@server.tool()
@handle_http_errors("export_doc_to_pdf", service_type="drive")
@require_google_service("drive", "drive_file")
async def export_doc_to_pdf(
    service: Any,
    user_google_email: str,
    document_id: str,
    pdf_filename: str = None,
    folder_id: str = None,
) -> str:
    """
    Exports a Google Doc to PDF format and saves it to Google Drive.

    Args:
        user_google_email: User's Google email address
        document_id: ID of the Google Doc to export
        pdf_filename: Name for the PDF file (optional - if not provided, uses original name + "_PDF")
        folder_id: Drive folder ID to save PDF in (optional - if not provided, saves in root)

    Returns:
        str: Confirmation message with PDF file details and links
    """
    logger.info(
        f"[export_doc_to_pdf] Email={user_google_email}, Doc={document_id}, pdf_filename={pdf_filename}, folder_id={folder_id}"
    )

    # Get file metadata first to validate it's a Google Doc
    try:
        file_metadata = await asyncio.to_thread(
            service.files()
            .get(
                fileId=document_id,
                fields="id, name, mimeType, webViewLink",
                supportsAllDrives=True,
            )
            .execute
        )
    except Exception as e:
        return f"Error: Could not access document {document_id}: {str(e)}"

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
            fileId=document_id, mimeType="application/pdf"
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

        folder_info = ""
        if folder_id:
            folder_info = f" in folder {folder_id}"
        elif pdf_parents:
            folder_info = f" in folder {pdf_parents[0]}"

        return f"Successfully exported '{original_name}' to PDF and saved to Drive as '{pdf_filename}' (ID: {pdf_file_id}, {pdf_size:,} bytes){folder_info}. PDF: {pdf_web_link} | Original: {web_view_link}"

    except Exception as e:
        return f"Error: Failed to upload PDF to Drive: {str(e)}. PDF was generated successfully ({pdf_size:,} bytes) but could not be saved to Drive."


@server.tool()
@handle_http_errors("update_paragraph_style", service_type="docs")
@require_google_service("docs", "docs_write")
async def update_paragraph_style(
    service: Any,
    user_google_email: str,
    document_id: str,
    start_index: int,
    end_index: int,
    heading_type: str = None,
    alignment: str = None,
    line_spacing: float = None,
    space_above: float = None,
    space_below: float = None,
    indent_first_line: float = None,
    indent_start: float = None,
    indent_end: float = None,
) -> str:
    """
    Applies paragraph-level formatting to a range in a Google Doc.

    Args:
        user_google_email: User's Google email address
        document_id: ID of the document to update
        start_index: Start position of the paragraph range
        end_index: End position of the paragraph range
        heading_type: Paragraph style type - one of "NORMAL_TEXT", "HEADING_1" through "HEADING_6", "TITLE", "SUBTITLE"
        alignment: Text alignment - one of "START", "CENTER", "END", "JUSTIFIED"
        line_spacing: Line spacing as percentage (e.g., 115 for 1.15x spacing)
        space_above: Space above paragraph in points
        space_below: Space below paragraph in points
        indent_first_line: First line indent in points
        indent_start: Left indent in points (for LTR text)
        indent_end: Right indent in points (for LTR text)

    Returns:
        str: Confirmation message with operation details
    """
    logger.info(
        f"[update_paragraph_style] Doc={document_id}, range={start_index}-{end_index}"
    )

    validator = ValidationManager()
    is_valid, error_msg = validator.validate_document_id(document_id)
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
        .batchUpdate(documentId=document_id, body={"requests": [request]})
        .execute
    )

    link = f"https://docs.google.com/document/d/{document_id}/edit"
    style_details = ", ".join(f"{f}={paragraph_style.get(f, paragraph_style.get(f))}" for f in fields)
    return f"Applied paragraph style ({style_details}) to range {start_index}-{end_index} in document {document_id}. Link: {link}"


@server.tool()
@handle_http_errors("update_document_style", service_type="docs")
@require_google_service("docs", "docs_write")
async def update_document_style(
    service: Any,
    user_google_email: str,
    document_id: str,
    margin_top: float = None,
    margin_bottom: float = None,
    margin_left: float = None,
    margin_right: float = None,
    page_width: float = None,
    page_height: float = None,
    default_font_family: str = None,
    default_font_size: float = None,
) -> str:
    """
    Sets page-level document style defaults for a Google Doc.

    Args:
        user_google_email: User's Google email address
        document_id: ID of the document to update
        margin_top: Top margin in points
        margin_bottom: Bottom margin in points
        margin_left: Left margin in points
        margin_right: Right margin in points
        page_width: Page width in points (US Letter = 612)
        page_height: Page height in points (US Letter = 792)
        default_font_family: Default font family name
        default_font_size: Default font size in points

    Returns:
        str: Confirmation message with operation details
    """
    logger.info(f"[update_document_style] Doc={document_id}")

    validator = ValidationManager()
    is_valid, error_msg = validator.validate_document_id(document_id)
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

    # Handle default font via named style update on NORMAL_TEXT
    if default_font_family is not None or default_font_size is not None:
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
                "fields": ",".join(style_fields),
            }
        })

    if not requests:
        return "Error: Must provide at least one document style parameter."

    await asyncio.to_thread(
        service.documents()
        .batchUpdate(documentId=document_id, body={"requests": requests})
        .execute
    )

    link = f"https://docs.google.com/document/d/{document_id}/edit"
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
    return f"Updated document style ({', '.join(changes)}) for document {document_id}. Link: {link}"


@server.tool()
@handle_http_errors("manage_named_range", service_type="docs")
@require_google_service("docs", "docs_write")
async def manage_named_range(
    service: Any,
    user_google_email: str,
    document_id: str,
    action: str,
    name: str,
    start_index: int = None,
    end_index: int = None,
    named_range_id: str = None,
    replacement_text: str = None,
) -> str:
    """
    Creates, deletes, or replaces content in named ranges in a Google Doc.

    Args:
        user_google_email: User's Google email address
        document_id: ID of the document to update
        action: Action to perform - "create", "delete", or "replace_content"
        name: Name of the named range
        start_index: Start position for creating a named range
        end_index: End position for creating a named range
        named_range_id: ID of the named range (for delete action)
        replacement_text: Text to replace the named range content with (for replace_content action)

    Returns:
        str: Confirmation message with operation details
    """
    logger.info(
        f"[manage_named_range] Doc={document_id}, action={action}, name={name}"
    )

    validator = ValidationManager()
    is_valid, error_msg = validator.validate_document_id(document_id)
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
        .batchUpdate(documentId=document_id, body={"requests": requests})
        .execute
    )

    link = f"https://docs.google.com/document/d/{document_id}/edit"

    if action == "create":
        # Extract the created named range ID from the response
        range_id = ""
        if "replies" in result and result["replies"]:
            reply = result["replies"][0]
            if "createNamedRange" in reply:
                range_id = reply["createNamedRange"].get("namedRangeId", "")
        return f"Created named range '{name}' (ID: {range_id}) at {start_index}-{end_index} in document {document_id}. Link: {link}"
    elif action == "delete":
        return f"Deleted named range '{name}' from document {document_id}. Link: {link}"
    else:
        return f"Replaced content of named range '{name}' with '{replacement_text[:50]}{'...' if len(replacement_text) > 50 else ''}' in document {document_id}. Link: {link}"


@server.tool()
@handle_http_errors("manage_table_structure", service_type="docs")
@require_google_service("docs", "docs_write")
async def manage_table_structure(
    service: Any,
    user_google_email: str,
    document_id: str,
    action: str,
    table_start_index: int,
    row_index: int = None,
    column_index: int = None,
    insert_below: bool = True,
    insert_right: bool = True,
    start_row: int = None,
    end_row: int = None,
    start_column: int = None,
    end_column: int = None,
) -> str:
    """
    Modifies table structure in a Google Doc - insert/delete rows/columns, merge/unmerge cells.

    Args:
        user_google_email: User's Google email address
        document_id: ID of the document to update
        action: Action to perform - "insert_row", "insert_column", "delete_row", "delete_column", "merge_cells", "unmerge_cells"
        table_start_index: The start index of the table in the document
        row_index: Row index for insert/delete row operations
        column_index: Column index for insert/delete column operations
        insert_below: Whether to insert row below the specified index (default: True)
        insert_right: Whether to insert column to the right of the specified index (default: True)
        start_row: Start row for merge/unmerge operations
        end_row: End row for merge/unmerge operations (exclusive)
        start_column: Start column for merge/unmerge operations
        end_column: End column for merge/unmerge operations (exclusive)

    Returns:
        str: Confirmation message with operation details
    """
    logger.info(
        f"[manage_table_structure] Doc={document_id}, action={action}, table_start={table_start_index}"
    )

    validator = ValidationManager()
    is_valid, error_msg = validator.validate_document_id(document_id)
    if not is_valid:
        return f"Error: {error_msg}"

    valid_actions = ("insert_row", "insert_column", "delete_row", "delete_column", "merge_cells", "unmerge_cells")
    if action not in valid_actions:
        return f"Error: action must be one of {', '.join(valid_actions)}."

    table_location = {"tableStartLocation": {"index": table_start_index}}
    requests = []

    if action == "insert_row":
        if row_index is None:
            return "Error: 'row_index' is required for insert_row action."
        requests.append({
            "insertTableRow": {
                **table_location,
                "cellLocation": {
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
                "cellLocation": {
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
        .batchUpdate(documentId=document_id, body={"requests": requests})
        .execute
    )

    link = f"https://docs.google.com/document/d/{document_id}/edit"
    return f"Successfully performed '{action}' on table at index {table_start_index} in document {document_id}. Link: {link}"


@server.tool()
@handle_http_errors("insert_section_break", service_type="docs")
@require_google_service("docs", "docs_write")
async def insert_section_break(
    service: Any,
    user_google_email: str,
    document_id: str,
    index: int,
    section_type: str = "NEXT_PAGE",
) -> str:
    """
    Inserts a section break into a Google Doc.

    Args:
        user_google_email: User's Google email address
        document_id: ID of the document to update
        index: Position to insert the section break
        section_type: Type of section break - "CONTINUOUS" or "NEXT_PAGE" (default: "NEXT_PAGE")

    Returns:
        str: Confirmation message with operation details
    """
    logger.info(
        f"[insert_section_break] Doc={document_id}, index={index}, type={section_type}"
    )

    validator = ValidationManager()
    is_valid, error_msg = validator.validate_document_id(document_id)
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
        .batchUpdate(documentId=document_id, body={"requests": [request]})
        .execute
    )

    link = f"https://docs.google.com/document/d/{document_id}/edit"
    return f"Inserted {section_type} section break at index {index} in document {document_id}. Link: {link}"


@server.tool()
@handle_http_errors("insert_footnote", service_type="docs")
@require_google_service("docs", "docs_write")
async def insert_footnote(
    service: Any,
    user_google_email: str,
    document_id: str,
    index: int,
    footnote_text: str = None,
) -> str:
    """
    Inserts a footnote reference at the specified position in a Google Doc, optionally with text.

    Args:
        user_google_email: User's Google email address
        document_id: ID of the document to update
        index: Position to insert the footnote reference
        footnote_text: Optional text to add to the footnote body

    Returns:
        str: Confirmation message with operation details
    """
    logger.info(
        f"[insert_footnote] Doc={document_id}, index={index}, has_text={footnote_text is not None}"
    )

    validator = ValidationManager()
    is_valid, error_msg = validator.validate_document_id(document_id)
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
        .batchUpdate(documentId=document_id, body={"requests": [create_request]})
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
            service.documents().get(documentId=document_id).execute
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
                    .batchUpdate(documentId=document_id, body={"requests": [insert_request]})
                    .execute
                )

    link = f"https://docs.google.com/document/d/{document_id}/edit"
    text_info = f" with text '{footnote_text[:50]}{'...' if len(footnote_text) > 50 else ''}'" if footnote_text else ""
    return f"Inserted footnote{text_info} at index {index} (footnote ID: {footnote_id}) in document {document_id}. Link: {link}"


@server.tool()
@handle_http_errors("delete_positioned_object", service_type="docs")
@require_google_service("docs", "docs_write")
async def delete_positioned_object(
    service: Any,
    user_google_email: str,
    document_id: str,
    object_id: str,
) -> str:
    """
    Deletes a positioned object (such as an image) from a Google Doc.

    Args:
        user_google_email: User's Google email address
        document_id: ID of the document to update
        object_id: The ID of the positioned object to delete

    Returns:
        str: Confirmation message with operation details
    """
    logger.info(
        f"[delete_positioned_object] Doc={document_id}, object_id={object_id}"
    )

    validator = ValidationManager()
    is_valid, error_msg = validator.validate_document_id(document_id)
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
        .batchUpdate(documentId=document_id, body={"requests": [request]})
        .execute
    )

    link = f"https://docs.google.com/document/d/{document_id}/edit"
    return f"Deleted positioned object '{object_id}' from document {document_id}. Link: {link}"


# Create comment management tools for documents
_comment_tools = create_comment_tools("document", "document_id")

# Extract and register the functions
read_doc_comments = _comment_tools["read_comments"]
create_doc_comment = _comment_tools["create_comment"]
reply_to_comment = _comment_tools["reply_to_comment"]
resolve_comment = _comment_tools["resolve_comment"]
edit_doc_comment = _comment_tools["edit_comment"]
delete_doc_comment = _comment_tools["delete_comment"]
edit_doc_comment_reply = _comment_tools["edit_reply"]
delete_doc_comment_reply = _comment_tools["delete_reply"]
