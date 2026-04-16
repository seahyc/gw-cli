"""
Google Docs service layer.

Pure synchronous functions for interacting with the Google Docs and Drive APIs.
Each function accepts pre-authenticated service client(s) and returns a string result.
"""

import io
import json
import logging
from typing import List, Dict, Any

from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

from gw.services._helpers.docs_helpers import (
    create_insert_text_request,
    create_delete_range_request,
    create_format_text_request,
    create_find_replace_request,
    create_insert_table_request,
    create_insert_page_break_request,
    create_insert_image_request,
    create_bullet_list_request,
)
from gw.services._helpers.docs_structure import (
    parse_document_structure,
    find_tables,
    analyze_document_complexity,
)
from gw.services._helpers.docs_tables import extract_table_as_data
from gw.services._helpers.docs_managers import (
    TableOperationManager,
    HeaderFooterManager,
    ValidationManager,
    BatchOperationManager,
)
from gw.services._helpers.docs_markdown import (
    parse_markdown,
    build_heading_block,
    build_paragraph_block,
    build_list_block,
    build_quote_block,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Tab helpers
# ---------------------------------------------------------------------------

# Request-type -> list of key paths (relative to the request body) where a
# Location or Range object lives. We inject tabId into each of these so that
# write operations target a specific tab instead of the default tab.
_TAB_TARGETS = {
    "insertText": ["location"],
    "deleteContentRange": ["range"],
    "updateTextStyle": ["range"],
    "updateParagraphStyle": ["range"],
    "insertTable": ["location"],
    "insertTableRow": ["tableCellLocation.tableStartLocation"],
    "insertTableColumn": ["tableCellLocation.tableStartLocation"],
    "deleteTableRow": ["tableCellLocation.tableStartLocation"],
    "deleteTableColumn": ["tableCellLocation.tableStartLocation"],
    "mergeTableCells": ["tableRange.tableCellLocation.tableStartLocation"],
    "unmergeTableCells": ["tableRange.tableCellLocation.tableStartLocation"],
    "insertPageBreak": ["location"],
    "insertSectionBreak": ["location"],
    "insertInlineImage": ["location"],
    "createFootnote": ["location"],
    "createParagraphBullets": ["range"],
    "deleteParagraphBullets": ["range"],
    "createNamedRange": ["range"],
    "deletePositionedObject": [],  # objectId-based, no location
    "updateTableCellStyle": ["tableRange.tableCellLocation.tableStartLocation"],
    "updateTableColumnProperties": ["tableStartLocation"],
    "updateTableRowStyle": ["tableStartLocation"],
    "pinTableHeaderRows": ["tableStartLocation"],
}


def _set_by_path(container, path, key, value):
    """Navigate dotted path inside container (creating dicts as needed) and set key=value."""
    node = container
    if path:
        for part in path.split("."):
            if part not in node or not isinstance(node[part], dict):
                node[part] = {}
            node = node[part]
    node[key] = value


def _apply_tab_id_to_request(request, tab_id):
    """Inject tabId into a single Docs API request (mutates in place)."""
    if not tab_id:
        return request
    for op_name, op_body in request.items():
        if not isinstance(op_body, dict):
            continue
        # replaceAllText uses tabsCriteria to scope to specific tabs
        if op_name == "replaceAllText":
            op_body["tabsCriteria"] = {"tabIds": [tab_id]}
            continue
        targets = _TAB_TARGETS.get(op_name)
        if targets is None:
            # Default: if the body has a 'location' or 'range' dict, tag it
            if isinstance(op_body.get("location"), dict):
                op_body["location"]["tabId"] = tab_id
            if isinstance(op_body.get("range"), dict):
                op_body["range"]["tabId"] = tab_id
            continue
        for target_path in targets:
            _set_by_path(op_body, target_path, "tabId", tab_id)
    return request


def _apply_tab_id(requests, tab_id):
    """Inject tabId into all requests in a list (mutates and returns the list)."""
    if not tab_id:
        return requests
    for req in requests:
        _apply_tab_id_to_request(req, tab_id)
    return requests


# ---------------------------------------------------------------------------
# Tabs: list / create
# ---------------------------------------------------------------------------

def list_tabs(service, file_id: str = "") -> str:
    """List all tabs (including nested child tabs) in a Google Doc. Returns JSON array."""
    logger.info(f"[list_tabs] Doc={file_id}")

    doc = (
        service.documents()
        .get(documentId=file_id, includeTabsContent=True)
        .execute()
    )

    def walk(tabs, parent_id=None, depth=0):
        out = []
        for tab in tabs or []:
            props = tab.get("tabProperties", {}) or {}
            tab_id = props.get("tabId", "")
            out.append({
                "tabId": tab_id,
                "title": props.get("title", ""),
                "index": props.get("index", 0),
                "parentTabId": parent_id,
                "depth": depth,
            })
            out.extend(walk(tab.get("childTabs", []), parent_id=tab_id, depth=depth + 1))
        return out

    tabs = walk(doc.get("tabs", []))
    return json.dumps(tabs, indent=2)


def create_tab(
    service,
    file_id: str = "",
    title: str = "",
    index: int = None,
    parent_tab_id: str = None,
) -> str:
    """Create a new tab in a Google Doc. Returns the new tab's ID as JSON."""
    logger.info(
        f"[create_tab] Doc={file_id}, title='{title}', index={index}, parent={parent_tab_id}"
    )

    if not title:
        return "Error: 'title' is required."

    tab_props = {"title": title}
    if index is not None:
        tab_props["index"] = index
    if parent_tab_id:
        tab_props["parentTabId"] = parent_tab_id

    request = {"addDocumentTab": {"tabProperties": tab_props}}

    result = (
        service.documents()
        .batchUpdate(documentId=file_id, body={"requests": [request]})
        .execute()
    )

    new_tab_id = ""
    if "replies" in result and result["replies"]:
        reply = result["replies"][0]
        # The reply key mirrors the request type; tabId lives inside tabProperties
        if "addDocumentTab" in reply:
            tab_props_reply = reply["addDocumentTab"].get("tabProperties", {})
            new_tab_id = tab_props_reply.get("tabId", "")

    return json.dumps(
        {
            "tabId": new_tab_id,
            "title": title,
            "index": index,
            "parentTabId": parent_tab_id,
        },
        indent=2,
    )


def delete_tab(service, file_id: str = "", tab_id: str = "") -> str:
    """Delete a tab from a Google Doc. Returns JSON confirmation."""
    logger.info(f"[delete_tab] Doc={file_id}, tab_id={tab_id}")

    if not tab_id:
        return "Error: 'tab_id' is required."

    # Pre-flight: verify the tab exists and ensure we're not deleting the last one.
    doc = (
        service.documents()
        .get(documentId=file_id, includeTabsContent=True)
        .execute()
    )

    def collect(tabs):
        out = []
        for tab in tabs or []:
            props = tab.get("tabProperties", {}) or {}
            out.append(props.get("tabId", ""))
            out.extend(collect(tab.get("childTabs", [])))
        return out

    all_tab_ids = collect(doc.get("tabs", []))

    if tab_id not in all_tab_ids:
        return f"Error: tab '{tab_id}' not found in document {file_id}."

    # Only block deletion when tab_id is the sole top-level tab AND there are
    # no child tabs elsewhere. Google Docs requires at least one tab in the doc.
    if len(all_tab_ids) <= 1:
        return (
            f"Error: cannot delete the only tab in document {file_id}. "
            "Google Docs requires at least one tab."
        )

    request = {"deleteTab": {"tabId": tab_id}}

    service.documents().batchUpdate(
        documentId=file_id, body={"requests": [request]}
    ).execute()

    return json.dumps({"deleted": True, "tabId": tab_id}, indent=2)


# ---------------------------------------------------------------------------
# Search / List
# ---------------------------------------------------------------------------

def search_docs(service, query: str = "", page_size: int = 10) -> str:
    """Search for Google Docs by name. Returns matching doc names, IDs, and links."""
    logger.info(f"[search_docs] Query='{query}'")

    escaped_query = query.replace("'", "\\'")

    response = (
        service.files()
        .list(
            q=f"name contains '{escaped_query}' and mimeType='application/vnd.google-apps.document' and trashed=false",
            pageSize=page_size,
            fields="files(id, name, createdTime, modifiedTime, webViewLink)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        .execute()
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


def list_docs_in_folder(service, folder_id: str = "root", page_size: int = 100) -> str:
    """List Google Docs in a Drive folder. Returns doc names, IDs, and links."""
    logger.info(f"[list_docs_in_folder] Folder ID: '{folder_id}'")

    rsp = (
        service.files()
        .list(
            q=f"'{folder_id}' in parents and mimeType='application/vnd.google-apps.document' and trashed=false",
            pageSize=page_size,
            fields="files(id, name, modifiedTime, webViewLink)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        .execute()
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


# ---------------------------------------------------------------------------
# Read / Inspect
# ---------------------------------------------------------------------------

def get_doc_content(drive_service, docs_service, file_id: str = "", tab_id: str = None) -> str:
    """Retrieve content of a Google Doc or Drive file (.docx). Returns text with metadata header.

    If tab_id is provided, only that tab's content is returned.
    """
    logger.info(f"[get_doc_content] Document/File ID: '{file_id}', tab_id={tab_id}")

    # Get file metadata from Drive
    file_metadata = (
        drive_service.files()
        .get(
            fileId=file_id,
            fields="id, name, mimeType, webViewLink",
            supportsAllDrives=True,
        )
        .execute()
    )
    mime_type = file_metadata.get("mimeType", "")
    file_name = file_metadata.get("name", "Unknown File")
    web_view_link = file_metadata.get("webViewLink", "#")

    logger.info(f"[get_doc_content] File '{file_name}' (ID: {file_id}) has mimeType: '{mime_type}'")

    body_text = ""

    if mime_type == "application/vnd.google-apps.document":
        logger.info("[get_doc_content] Processing as native Google Doc.")
        doc_data = (
            docs_service.documents()
            .get(documentId=file_id, includeTabsContent=True)
            .execute()
        )
        TAB_HEADER_FORMAT = "\n--- TAB: {tab_name} ---\n"

        def extract_text_from_elements(elements, tab_name=None, depth=0):
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

        def process_tab_hierarchy(tab, level=0, target_tab_id=None):
            tab_text = ""
            props = tab.get("tabProperties", {})
            this_tab_id = props.get("tabId", "Unknown ID")
            # When target_tab_id is set, only emit content for that tab (still
            # recurse into children so the target can be a nested tab).
            if "documentTab" in tab and (target_tab_id is None or this_tab_id == target_tab_id):
                tab_title = props.get("title", "Untitled Tab")
                if level > 0:
                    tab_title = "    " * level + f"{tab_title} ( ID: {this_tab_id})"
                tab_body = tab.get("documentTab", {}).get("body", {}).get("content", [])
                tab_text += extract_text_from_elements(tab_body, tab_title)

            child_tabs = tab.get("childTabs", [])
            for child_tab in child_tabs:
                tab_text += process_tab_hierarchy(child_tab, level + 1, target_tab_id)

            return tab_text

        processed_text_lines = []

        if tab_id:
            # Only emit the requested tab's content.
            tabs = doc_data.get("tabs", [])
            for tab in tabs:
                tab_content = process_tab_hierarchy(tab, target_tab_id=tab_id)
                if tab_content.strip():
                    processed_text_lines.append(tab_content)
        else:
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

        export_mime_type_map = {}
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
        done = False
        while not done:
            status, done = downloader.next_chunk()

        file_content_bytes = fh.getvalue()

        # Office XML text extraction (not supported in CLI mode)
        office_text = None

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


def inspect_doc_structure(service, file_id: str = "", detailed: bool = False) -> str:
    """Analyze document structure to find safe insertion indices and table positions."""
    logger.debug(f"[inspect_doc_structure] Doc={file_id}, detailed={detailed}")

    doc = (
        service.documents()
        .get(documentId=file_id)
        .execute()
    )

    if detailed:
        structure = parse_document_structure(doc)

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
                        "preview": table_data[:3] if table_data else [],
                    }
                )
    else:
        result = analyze_document_complexity(doc)

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


# ---------------------------------------------------------------------------
# Create
# ---------------------------------------------------------------------------

def create_doc(service, title: str = "", content: str = "") -> str:
    """Create a new Google Doc, optionally with initial content. Returns new doc ID and link."""
    logger.info(f"[create_doc] Title='{title}'")

    doc = (
        service.documents()
        .create(body={"title": title})
        .execute()
    )
    doc_id = doc.get("documentId")
    if content:
        requests = [{"insertText": {"location": {"index": 1}, "text": content}}]
        (
            service.documents()
            .batchUpdate(documentId=doc_id, body={"requests": requests})
            .execute()
        )
    link = f"https://docs.google.com/document/d/{doc_id}/edit"
    msg = f"Created Google Doc '{title}' (ID: {doc_id}). Link: {link}"
    logger.info(f"[create_doc] Created '{title}' (ID: {doc_id})")
    return msg


# ---------------------------------------------------------------------------
# Edit / Modify
# ---------------------------------------------------------------------------

def modify_doc_text(
    service,
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
    tab_id: str = None,
) -> str:
    """Insert/replace text and apply character formatting at a position in a Google Doc."""
    all_formatting = [bold, italic, underline, font_size, font_family, text_color,
                       background_color, strikethrough, superscript, subscript, link_url]
    logger.info(
        f"[modify_doc_text] Doc={file_id}, start={start_index}, end={end_index}, text={text is not None}, "
        f"formatting={any(p is not None for p in all_formatting)}"
    )

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

    if text is None and not has_formatting:
        return "Error: Must provide either 'text' to insert/replace, or formatting parameters (bold, italic, underline, strikethrough, superscript, subscript, link_url, font_size, font_family, text_color, background_color)."

    if has_formatting:
        is_valid, error_msg = validator.validate_text_formatting_params(
            bold, italic, underline, font_size, font_family, text_color,
            background_color, strikethrough, superscript, subscript, link_url,
        )
        if not is_valid:
            return f"Error: {error_msg}"

        if end_index is None:
            return "Error: 'end_index' is required when applying formatting."

        is_valid, error_msg = validator.validate_index_range(start_index, end_index)
        if not is_valid:
            return f"Error: {error_msg}"

    requests = []
    operations = []

    if text is not None:
        if end_index is not None and end_index > start_index:
            if start_index == 0:
                requests.append(create_insert_text_request(1, text))
                adjusted_end = end_index + len(text)
                requests.append(
                    create_delete_range_request(1 + len(text), adjusted_end)
                )
                operations.append(
                    f"Replaced text from index {start_index} to {end_index}"
                )
            else:
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
            actual_index = 1 if start_index == 0 else start_index
            requests.append(create_insert_text_request(actual_index, text))
            operations.append(f"Inserted text at index {start_index}")

    if has_formatting:
        format_start = start_index
        format_end = end_index

        if text is not None:
            if end_index is not None and end_index > start_index:
                format_end = start_index + len(text)
            else:
                actual_index = 1 if start_index == 0 else start_index
                format_start = actual_index
                format_end = actual_index + len(text)

        if format_start == 0:
            format_start = 1
        if format_end is not None and format_end <= format_start:
            format_end = format_start + 1

        requests.append(
            create_format_text_request(
                format_start, format_end,
                bold, italic, underline, font_size, font_family,
                text_color, background_color, strikethrough,
                superscript, subscript, link_url,
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

    _apply_tab_id(requests, tab_id)
    (
        service.documents()
        .batchUpdate(documentId=file_id, body={"requests": requests})
        .execute()
    )

    operation_summary = "; ".join(operations)
    text_info = f" Text length: {len(text)} characters." if text else ""
    return f"{operation_summary}.{text_info}"


def find_and_replace_doc(
    service,
    file_id: str = "",
    find_text: str = "",
    replace_text: str = "",
    match_case: bool = False,
    tab_id: str = None,
) -> str:
    """Find and replace all occurrences of text in a Google Doc."""
    logger.info(
        f"[find_and_replace_doc] Doc={file_id}, find='{find_text}', replace='{replace_text}', tab_id={tab_id}"
    )

    requests = [create_find_replace_request(find_text, replace_text, match_case)]
    _apply_tab_id(requests, tab_id)

    result = (
        service.documents()
        .batchUpdate(documentId=file_id, body={"requests": requests})
        .execute()
    )

    replacements = 0
    if "replies" in result and result["replies"]:
        reply = result["replies"][0]
        if "replaceAllText" in reply:
            replacements = reply["replaceAllText"].get("occurrencesChanged", 0)

    return f"Replaced {replacements} occurrence(s) of '{find_text}' with '{replace_text}'."


# ---------------------------------------------------------------------------
# Insert operations
# ---------------------------------------------------------------------------

def insert_table(
    service,
    file_id: str = "",
    index: int = 0,
    rows: int = 0,
    columns: int = 0,
    tab_id: str = None,
) -> str:
    """Insert a table at the given index in a Google Doc."""
    logger.info(f"[insert_table] Doc={file_id}, index={index}, rows={rows}, columns={columns}, tab_id={tab_id}")

    if not rows or not columns:
        return "Error: 'rows' and 'columns' are required."

    if index == 0:
        index = 1

    requests = [create_insert_table_request(index, rows, columns)]
    _apply_tab_id(requests, tab_id)

    (
        service.documents()
        .batchUpdate(documentId=file_id, body={"requests": requests})
        .execute()
    )

    return f"Inserted table ({rows}x{columns}) at index {index}."


def insert_list(
    service,
    file_id: str = "",
    index: int = 0,
    list_type: str = "UNORDERED",
    text: str = "List item",
    tab_id: str = None,
) -> str:
    """Insert a bullet or numbered list. list_type: UNORDERED or ORDERED."""
    logger.info(f"[insert_list] Doc={file_id}, index={index}, list_type={list_type}, tab_id={tab_id}")

    if index == 0:
        index = 1

    requests = [
        create_insert_text_request(index, text + "\n"),
        create_bullet_list_request(index, index + len(text), list_type),
    ]
    _apply_tab_id(requests, tab_id)

    (
        service.documents()
        .batchUpdate(documentId=file_id, body={"requests": requests})
        .execute()
    )

    return f"Inserted {list_type.lower()} list at index {index}."


def insert_page_break(service, file_id: str = "", index: int = 0, tab_id: str = None) -> str:
    """Insert a page break at the given index in a Google Doc."""
    logger.info(f"[insert_page_break] Doc={file_id}, index={index}, tab_id={tab_id}")

    if index == 0:
        index = 1

    requests = [create_insert_page_break_request(index)]
    _apply_tab_id(requests, tab_id)

    (
        service.documents()
        .batchUpdate(documentId=file_id, body={"requests": requests})
        .execute()
    )

    return f"Inserted page break at index {index}."


def insert_doc_image(
    docs_service,
    drive_service,
    file_id: str = "",
    image_source: str = "",
    index: int = 0,
    width: int = 0,
    height: int = 0,
    tab_id: str = None,
) -> str:
    """Insert an image into a Google Doc from a Drive file ID or public URL."""
    logger.info(
        f"[insert_doc_image] Doc={file_id}, source={image_source}, index={index}"
    )

    if index == 0:
        logger.debug("Adjusting index from 0 to 1 to avoid first section break")
        index = 1

    is_drive_file = not (
        image_source.startswith("http://") or image_source.startswith("https://")
    )

    if is_drive_file:
        try:
            file_metadata = (
                drive_service.files()
                .get(
                    fileId=image_source,
                    fields="id, name, mimeType",
                    supportsAllDrives=True,
                )
                .execute()
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

    requests = [create_insert_image_request(index, image_uri, width, height)]
    _apply_tab_id(requests, tab_id)

    (
        docs_service.documents()
        .batchUpdate(documentId=file_id, body={"requests": requests})
        .execute()
    )

    size_info = ""
    if width or height:
        size_info = f" (size: {width or 'auto'}x{height or 'auto'} points)"

    return f"Inserted {source_description}{size_info} at index {index}."


def insert_section_break(
    service,
    file_id: str = "",
    index: int = 0,
    section_type: str = "NEXT_PAGE",
    tab_id: str = None,
) -> str:
    """Insert a section break. section_type: CONTINUOUS or NEXT_PAGE."""
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
    _apply_tab_id_to_request(request, tab_id)

    (
        service.documents()
        .batchUpdate(documentId=file_id, body={"requests": [request]})
        .execute()
    )

    return f"Inserted {section_type} section break at index {index}."


def insert_footnote(
    service,
    file_id: str = "",
    index: int = 0,
    footnote_text: str = None,
    tab_id: str = None,
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

    create_request = {
        "createFootnote": {
            "location": {"index": index},
        }
    }
    _apply_tab_id_to_request(create_request, tab_id)

    result = (
        service.documents()
        .batchUpdate(documentId=file_id, body={"requests": [create_request]})
        .execute()
    )

    footnote_id = ""
    if "replies" in result and result["replies"]:
        reply = result["replies"][0]
        if "createFootnote" in reply:
            footnote_id = reply["createFootnote"].get("footnoteId", "")

    if footnote_text and footnote_id:
        doc = (
            service.documents()
            .get(documentId=file_id)
            .execute()
        )

        footnotes = doc.get("footnotes", {})
        footnote = footnotes.get(footnote_id, {})
        footnote_content = footnote.get("content", [])

        if footnote_content:
            first_element = footnote_content[0]
            if "paragraph" in first_element:
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

                (
                    service.documents()
                    .batchUpdate(documentId=file_id, body={"requests": [insert_request]})
                    .execute()
                )

    return f"Inserted footnote at index {index} (footnote ID: {footnote_id})."


# ---------------------------------------------------------------------------
# Table operations
# ---------------------------------------------------------------------------

def create_table_with_data(
    service,
    file_id: str = "",
    table_data: List[List[str]] = None,
    index: int = 0,
    bold_headers: bool = True,
    tab_id: str = None,
) -> str:
    """Create and populate a table in one operation."""
    logger.debug(f"[create_table_with_data] Doc={file_id}, index={index}")

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

    table_manager = TableOperationManager(service)

    success, message, metadata = table_manager.create_and_populate_table(
        file_id, table_data, index, bold_headers, tab_id=tab_id
    )

    if not success and "must be less than the end index" in message:
        logger.debug(
            f"Index {index} is at document boundary, retrying with index {index - 1}"
        )
        success, message, metadata = table_manager.create_and_populate_table(
            file_id, table_data, index - 1, bold_headers, tab_id=tab_id
        )

    if success:
        rows = metadata.get("rows", 0)
        columns = metadata.get("columns", 0)
        return f"SUCCESS: {message}. Table: {rows}x{columns}."
    else:
        return f"ERROR: {message}"


def debug_table_structure(
    service,
    file_id: str = "",
    table_index: int = 0,
) -> str:
    """Inspect a table's dimensions, cell positions, content, and insertion indices."""
    logger.debug(
        f"[debug_table_structure] Doc={file_id}, table_index={table_index}"
    )

    doc = (
        service.documents()
        .get(documentId=file_id)
        .execute()
    )

    tables = find_tables(doc)
    if table_index >= len(tables):
        return f"Error: Table index {table_index} not found. Document has {len(tables)} table(s)."

    table_info = tables[table_index]

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


def manage_table_structure(
    service,
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
    """Modify table structure: insert/delete rows/columns, merge/unmerge cells."""
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

    (
        service.documents()
        .batchUpdate(documentId=file_id, body={"requests": requests})
        .execute()
    )

    return f"Performed '{action}' on table at index {table_start_index}."


# ---------------------------------------------------------------------------
# Styles
# ---------------------------------------------------------------------------

def update_paragraph_style(
    service,
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
    """Apply paragraph-level formatting to a range."""
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

    (
        service.documents()
        .batchUpdate(documentId=file_id, body={"requests": [request]})
        .execute()
    )

    style_details = ", ".join(f"{f}={paragraph_style.get(f, paragraph_style.get(f))}" for f in fields)
    return f"Applied paragraph style ({style_details}) to range {start_index}-{end_index}."


def update_document_style(
    service,
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
    """Set page-level defaults (margins, page size, default font) for a Google Doc."""
    logger.info(f"[update_document_style] Doc={file_id}")

    validator = ValidationManager()
    is_valid, error_msg = validator.validate_document_id(file_id)
    if not is_valid:
        return f"Error: {error_msg}"

    requests = []

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

    if default_font_family is not None or default_font_size is not None:
        doc = (
            service.documents()
            .get(documentId=file_id)
            .execute()
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

    (
        service.documents()
        .batchUpdate(documentId=file_id, body={"requests": requests})
        .execute()
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


# ---------------------------------------------------------------------------
# Named ranges
# ---------------------------------------------------------------------------

def manage_named_range(
    service,
    file_id: str = "",
    action: str = "",
    name: str = "",
    start_index: int = None,
    end_index: int = None,
    named_range_id: str = None,
    replacement_text: str = None,
) -> str:
    """Create, delete, or replace content in named ranges."""
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

    result = (
        service.documents()
        .batchUpdate(documentId=file_id, body={"requests": requests})
        .execute()
    )

    if action == "create":
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


# ---------------------------------------------------------------------------
# Headers / Footers
# ---------------------------------------------------------------------------

def update_doc_headers_footers(
    service,
    file_id: str = "",
    section_type: str = "",
    content: str = "",
    header_footer_type: str = "DEFAULT",
) -> str:
    """Update headers or footers in a Google Doc."""
    logger.info(f"[update_doc_headers_footers] Doc={file_id}, type={section_type}")

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

    header_footer_manager = HeaderFooterManager(service)

    ok, message = header_footer_manager.update_header_footer_content(
        file_id, section_type, content, header_footer_type
    )

    if ok:
        return message
    else:
        return f"Error: {message}"


# ---------------------------------------------------------------------------
# Batch update
# ---------------------------------------------------------------------------

def batch_update_doc(
    service,
    file_id: str = "",
    operations: List[Dict[str, Any]] = None,
    tab_id: str = None,
) -> str:
    """Execute multiple document operations in a single atomic batch."""
    logger.debug(f"[batch_update_doc] Doc={file_id}, operations={len(operations or [])}, tab_id={tab_id}")

    validator = ValidationManager()

    is_valid, error_msg = validator.validate_document_id(file_id)
    if not is_valid:
        return f"Error: {error_msg}"

    is_valid, error_msg = validator.validate_batch_operations(operations)
    if not is_valid:
        return f"Error: {error_msg}"

    batch_manager = BatchOperationManager(service)

    ok, message, metadata = batch_manager.execute_batch_operations(
        file_id, operations, tab_id=tab_id
    )

    if ok:
        replies_count = metadata.get("replies_count", 0)
        return f"{message}. API replies: {replies_count}."
    else:
        return f"Error: {message}"


# ---------------------------------------------------------------------------
# Delete
# ---------------------------------------------------------------------------

def delete_positioned_object(
    service,
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

    (
        service.documents()
        .batchUpdate(documentId=file_id, body={"requests": [request]})
        .execute()
    )

    return f"Deleted positioned object '{object_id}'."


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------

def export_doc_to_pdf(
    service,
    file_id: str = "",
    pdf_filename: str = None,
    folder_id: str = None,
) -> str:
    """Export a Google Doc to PDF and save to Drive. Returns new PDF file ID and link."""
    logger.info(
        f"[export_doc_to_pdf] Doc={file_id}, pdf_filename={pdf_filename}, folder_id={folder_id}"
    )

    try:
        file_metadata = (
            service.files()
            .get(
                fileId=file_id,
                fields="id, name, mimeType, webViewLink",
                supportsAllDrives=True,
            )
            .execute()
        )
    except Exception as e:
        return f"Error: Could not access document {file_id}: {str(e)}"

    mime_type = file_metadata.get("mimeType", "")
    original_name = file_metadata.get("name", "Unknown Document")

    if mime_type != "application/vnd.google-apps.document":
        return f"Error: File '{original_name}' is not a Google Doc (MIME type: {mime_type}). Only native Google Docs can be exported to PDF."

    logger.info(f"[export_doc_to_pdf] Exporting '{original_name}' to PDF")

    try:
        request_obj = service.files().export_media(
            fileId=file_id, mimeType="application/pdf"
        )

        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request_obj)

        done = False
        while not done:
            _, done = downloader.next_chunk()

        pdf_content = fh.getvalue()
        pdf_size = len(pdf_content)

    except Exception as e:
        return f"Error: Failed to export document to PDF: {str(e)}"

    if not pdf_filename:
        pdf_filename = f"{original_name}_PDF.pdf"
    elif not pdf_filename.endswith(".pdf"):
        pdf_filename += ".pdf"

    try:
        fh.seek(0)
        media = MediaIoBaseUpload(fh, mimetype="application/pdf", resumable=True)

        upload_metadata = {"name": pdf_filename, "mimeType": "application/pdf"}

        if folder_id:
            upload_metadata["parents"] = [folder_id]

        uploaded_file = (
            service.files()
            .create(
                body=upload_metadata,
                media_body=media,
                fields="id, name, webViewLink, parents",
                supportsAllDrives=True,
            )
            .execute()
        )

        pdf_file_id = uploaded_file.get("id")
        pdf_web_link = uploaded_file.get("webViewLink", "#")

        logger.info(
            f"[export_doc_to_pdf] Successfully uploaded PDF to Drive: {pdf_file_id}"
        )

        return f"Exported to PDF '{pdf_filename}' (ID: {pdf_file_id}, {pdf_size:,} bytes). Link: {pdf_web_link}"

    except Exception as e:
        return f"Error: Failed to upload PDF to Drive: {str(e)}. PDF was generated successfully ({pdf_size:,} bytes) but could not be saved to Drive."


# ---------------------------------------------------------------------------
# Markdown -> native Google Docs formatting
# ---------------------------------------------------------------------------

def _find_target_tab(doc, tab_id):
    """Return the tab matching tab_id or None. Recurses into childTabs."""
    def walk(tabs):
        for t in tabs or []:
            if t.get("tabProperties", {}).get("tabId") == tab_id:
                return t
            found = walk(t.get("childTabs", []))
            if found is not None:
                return found
        return None

    return walk(doc.get("tabs", []))


def _get_body_content(doc, tab_id):
    """Return the body content array for the target tab.

    Note: a document fetched with includeTabsContent=True may NOT populate
    the top-level `body` field -- all content sits under `tabs[...]`. When
    tab_id is None we fall back to the first top-level tab's body.
    """
    if tab_id:
        tab = _find_target_tab(doc, tab_id)
        if tab is None:
            raise ValueError(f"tab_id '{tab_id}' not found in document")
        return tab.get("documentTab", {}).get("body", {}).get("content", [])
    # tab_id not set: prefer the document-level body if populated, otherwise
    # fall back to the first tab's body (happens when includeTabsContent=True).
    body_content = doc.get("body", {}).get("content", [])
    if body_content:
        return body_content
    tabs = doc.get("tabs", [])
    if tabs:
        return tabs[0].get("documentTab", {}).get("body", {}).get("content", [])
    return []


def _get_body_end_index(doc, tab_id):
    """Return the last endIndex of the body for the given tab (or default body).

    Used to know the full extent of existing content before we clear/insert.
    """
    content = _get_body_content(doc, tab_id)
    if not content:
        return 1
    return content[-1].get("endIndex", 1)


def insert_markdown(
    service,
    file_id: str = "",
    markdown_text: str = "",
    tab_id: str = None,
    start_index: int = 1,
    replace: bool = False,
) -> str:
    """Insert markdown content into a Google Doc as native formatting.

    Parameters
    ----------
    service : Google Docs service client
    file_id : document ID
    markdown_text : the markdown source
    tab_id : optional tab ID (default: main doc body)
    start_index : index to start inserting at (ignored if replace=True; then 1)
    replace : if True, clear all existing content in the target first

    Returns a JSON summary string.

    Supported markdown subset:
      - Headings (# .. ######) -> HEADING_1..HEADING_6
      - Paragraphs
      - Bullet lists (- item)
      - Blockquotes (> text) rendered as italic + 36pt indent
      - Tables (GitHub-flavored pipe syntax) with bold header row
      - Inline **bold**
      - Inline `code` (rendered in Roboto Mono)
      - Horizontal rules (---) are skipped (heading spacing is sufficient)

    Limitations: no nested lists, no ordered lists, no images/links,
    no fenced code blocks, no raw HTML.
    """
    logger.info(
        f"[insert_markdown] Doc={file_id}, tab_id={tab_id}, "
        f"start_index={start_index}, replace={replace}, "
        f"md_len={len(markdown_text or '')}"
    )

    if not file_id:
        return "Error: 'file_id' is required."
    if markdown_text is None:
        markdown_text = ""

    blocks = parse_markdown(markdown_text)
    counts = {
        "blocks": len(blocks),
        "headings": sum(1 for b in blocks if b["type"] == "heading"),
        "paragraphs": sum(1 for b in blocks if b["type"] == "paragraph"),
        "lists": sum(1 for b in blocks if b["type"] == "list"),
        "quotes": sum(1 for b in blocks if b["type"] == "quote"),
        "tables": sum(1 for b in blocks if b["type"] == "table"),
        "hr_skipped": sum(1 for b in blocks if b["type"] == "hr"),
    }

    # Step 1 (optional): clear existing content in the target.
    if replace:
        doc = (
            service.documents()
            .get(documentId=file_id, includeTabsContent=True)
            .execute()
        )
        last_end = _get_body_end_index(doc, tab_id)
        # The body always ends with a final trailing newline that cannot be
        # deleted. Deletable range is [1, last_end - 1).
        if last_end > 2:
            rng = {"startIndex": 1, "endIndex": last_end - 1}
            if tab_id:
                rng["tabId"] = tab_id
            clear_reqs = [
                {"deleteContentRange": {"range": rng}},
                {
                    "updateParagraphStyle": {
                        "range": (
                            {"startIndex": 1, "endIndex": 2, "tabId": tab_id}
                            if tab_id
                            else {"startIndex": 1, "endIndex": 2}
                        ),
                        "paragraphStyle": {"namedStyleType": "NORMAL_TEXT"},
                        "fields": "namedStyleType",
                    }
                },
            ]
            service.documents().batchUpdate(
                documentId=file_id, body={"requests": clear_reqs}
            ).execute()
        cursor = 1
    else:
        cursor = max(1, int(start_index or 1))

    # Step 2: walk blocks. Accumulate text-based requests; flush them whenever
    # we hit a table (tables need an insertTable call followed by a doc re-read
    # to learn cell indices before filling cells).
    CHUNK = 200
    pending: List[Dict[str, Any]] = []

    def flush():
        nonlocal pending
        if not pending:
            return
        for i in range(0, len(pending), CHUNK):
            batch = pending[i:i + CHUNK]
            service.documents().batchUpdate(
                documentId=file_id, body={"requests": batch}
            ).execute()
        pending = []

    for block in blocks:
        btype = block["type"]
        if btype == "heading":
            reqs, cursor = build_heading_block(
                block["runs"], block["level"], cursor, tab_id
            )
            pending.extend(reqs)
        elif btype == "paragraph":
            reqs, cursor = build_paragraph_block(block["runs"], cursor, tab_id)
            pending.extend(reqs)
        elif btype == "list":
            reqs, cursor = build_list_block(block["items_runs"], cursor, tab_id)
            pending.extend(reqs)
        elif btype == "quote":
            reqs, cursor = build_quote_block(block["runs"], cursor, tab_id)
            pending.extend(reqs)
        elif btype == "hr":
            # Skip horizontal rules -- heading/paragraph separation is enough.
            continue
        elif btype == "table":
            # Flush text-based requests first so cursor positions stay valid.
            flush()
            rows = block["rows"]
            cells_runs = block["cells_runs"]
            n_rows = len(rows)
            n_cols = max(len(r) for r in rows) if rows else 0
            if n_rows == 0 or n_cols == 0:
                continue
            # Normalize row widths
            rows_norm = [r + [""] * (n_cols - len(r)) for r in rows]
            cells_runs_norm = [
                cr + [[]] * (n_cols - len(cr)) for cr in cells_runs
            ]

            # Insert empty table at cursor.
            insert_table_loc = {"index": cursor}
            if tab_id:
                insert_table_loc["tabId"] = tab_id
            service.documents().batchUpdate(
                documentId=file_id,
                body={"requests": [{"insertTable": {
                    "location": insert_table_loc,
                    "rows": n_rows,
                    "columns": n_cols,
                }}]},
            ).execute()

            # Re-read doc to find the new table and its cell indices.
            doc2 = (
                service.documents()
                .get(documentId=file_id, includeTabsContent=True)
                .execute()
            )
            body_content = _get_body_content(doc2, tab_id)
            target_table = None
            for el in body_content:
                if "table" in el and el.get("startIndex", -1) >= cursor:
                    target_table = el
                    break
            if target_table is None:
                raise RuntimeError("Inserted table not found in document body")

            # Build cell-fill requests in reverse order so earlier indices
            # remain valid as we insert later content.
            fill_reqs: List[Dict[str, Any]] = []
            for r_idx in range(n_rows - 1, -1, -1):
                row_el = target_table["table"]["tableRows"][r_idx]
                for c_idx in range(n_cols - 1, -1, -1):
                    cell = row_el["tableCells"][c_idx]
                    cell_start = cell["startIndex"]
                    insert_idx = cell_start + 1
                    runs = cells_runs_norm[r_idx][c_idx]
                    plain = "".join(t for t, _ in runs)
                    if not plain:
                        continue
                    loc = {"index": insert_idx}
                    if tab_id:
                        loc["tabId"] = tab_id
                    fill_reqs.append({
                        "insertText": {"location": loc, "text": plain}
                    })
                    # Inline bold/code styling inside the cell
                    off = insert_idx
                    for run_text, attrs in runs:
                        rl = len(run_text)
                        if attrs.get("bold"):
                            rng = {"startIndex": off, "endIndex": off + rl}
                            if tab_id:
                                rng["tabId"] = tab_id
                            fill_reqs.append({"updateTextStyle": {
                                "range": rng,
                                "textStyle": {"bold": True},
                                "fields": "bold",
                            }})
                        if attrs.get("code"):
                            rng = {"startIndex": off, "endIndex": off + rl}
                            if tab_id:
                                rng["tabId"] = tab_id
                            fill_reqs.append({"updateTextStyle": {
                                "range": rng,
                                "textStyle": {
                                    "weightedFontFamily": {"fontFamily": "Roboto Mono"}
                                },
                                "fields": "weightedFontFamily",
                            }})
                        off += rl

            for i in range(0, len(fill_reqs), CHUNK):
                service.documents().batchUpdate(
                    documentId=file_id,
                    body={"requests": fill_reqs[i:i + CHUNK]},
                ).execute()

            # Bold the header row. Re-read to get fresh cell positions.
            doc3 = (
                service.documents()
                .get(documentId=file_id, includeTabsContent=True)
                .execute()
            )
            body_content3 = _get_body_content(doc3, tab_id)
            target_table3 = None
            for el in body_content3:
                if "table" in el and el.get("startIndex", -1) >= cursor:
                    target_table3 = el
                    break
            if target_table3 is not None:
                header_row = target_table3["table"]["tableRows"][0]
                hdr_reqs: List[Dict[str, Any]] = []
                for c_idx in range(n_cols):
                    cell = header_row["tableCells"][c_idx]
                    cs = cell["startIndex"]
                    ce = cell["endIndex"]
                    if ce - cs > 1:
                        rng = {"startIndex": cs + 1, "endIndex": ce - 1}
                        if tab_id:
                            rng["tabId"] = tab_id
                        hdr_reqs.append({"updateTextStyle": {
                            "range": rng,
                            "textStyle": {"bold": True},
                            "fields": "bold",
                        }})
                if hdr_reqs:
                    service.documents().batchUpdate(
                        documentId=file_id, body={"requests": hdr_reqs}
                    ).execute()
                # After a table, continue inserting at the start of the
                # paragraph immediately following the table.
                cursor = target_table3["endIndex"]

    flush()

    return json.dumps({
        "inserted": True,
        "file_id": file_id,
        "tab_id": tab_id,
        "replaced": replace,
        "start_index": start_index if not replace else 1,
        **counts,
    }, indent=2)


# ---------------------------------------------------------------------------
# Table column-width inspection & adjustment
# ---------------------------------------------------------------------------

# Rough per-character width at common body font sizes (in points).
# Roboto/Arial are both approximately the same here. These numbers are
# lower-bound-friendly averages that include spaces and moderate letter mix.
_CHAR_WIDTH_BY_SIZE = {
    9: 4.6,
    10: 5.2,
    10.5: 5.4,
    11: 5.7,
    12: 6.2,
    13: 6.7,
    14: 7.2,
}


def _char_width_pt(font_size_pt: float) -> float:
    """Approximate average character width in points for a given font size."""
    # Clamp to the table; interpolate linearly for values in between keys.
    sizes = sorted(_CHAR_WIDTH_BY_SIZE.keys())
    if font_size_pt <= sizes[0]:
        return _CHAR_WIDTH_BY_SIZE[sizes[0]]
    if font_size_pt >= sizes[-1]:
        # Extrapolate by ratio for large sizes.
        return _CHAR_WIDTH_BY_SIZE[sizes[-1]] * (font_size_pt / sizes[-1])
    for i in range(len(sizes) - 1):
        low, high = sizes[i], sizes[i + 1]
        if low <= font_size_pt <= high:
            frac = (font_size_pt - low) / (high - low)
            return _CHAR_WIDTH_BY_SIZE[low] + frac * (
                _CHAR_WIDTH_BY_SIZE[high] - _CHAR_WIDTH_BY_SIZE[low]
            )
    return _CHAR_WIDTH_BY_SIZE[11]


def _fetch_doc_with_tabs(service, file_id):
    """Fetch a document with tabs content (needed for per-tab documentStyle)."""
    return (
        service.documents()
        .get(documentId=file_id, includeTabsContent=True)
        .execute()
    )


def _document_style_for_tab(doc, tab_id):
    """Return the documentStyle dict for the given tab_id, falling back to the
    first top-level tab if tab_id is None, and finally to the doc-level style.
    """
    if tab_id:
        tab = _find_target_tab(doc, tab_id)
        if tab is not None:
            ds = tab.get("documentTab", {}).get("documentStyle")
            if ds:
                return ds
    # Fall back to first top-level tab's style when it's present.
    tabs = doc.get("tabs", []) or []
    if tabs:
        ds = tabs[0].get("documentTab", {}).get("documentStyle")
        if ds:
            return ds
    return doc.get("documentStyle", {}) or {}


def _inner_page_width_pt(doc_style):
    """Usable inner page width (page width minus left/right margins), in pt.

    Returns None if the document doesn't expose pageSize/margins (common in
    new docs that rely on template defaults).
    """
    page = doc_style.get("pageSize") or {}
    width_info = page.get("width") or {}
    width_pt = width_info.get("magnitude")
    if width_pt is None:
        return None
    left = (doc_style.get("marginLeft") or {}).get("magnitude", 0)
    right = (doc_style.get("marginRight") or {}).get("magnitude", 0)
    return float(width_pt) - float(left) - float(right)


def _collect_tables_in_body(body_content):
    """Return a list of table elements from a body content array, in order."""
    return [el for el in body_content if "table" in el]


def _collect_tab_tables(doc, tab_id):
    """Return (tables, doc_style, tab_title) for the requested tab, or for the
    default body / first tab when tab_id is None.
    """
    if tab_id:
        tab = _find_target_tab(doc, tab_id)
        if tab is None:
            raise ValueError(f"tab_id '{tab_id}' not found in document")
        props = tab.get("tabProperties", {}) or {}
        doc_tab = tab.get("documentTab", {}) or {}
        body_content = doc_tab.get("body", {}).get("content", []) or []
        doc_style = doc_tab.get("documentStyle", {}) or {}
        return (
            _collect_tables_in_body(body_content),
            doc_style,
            props.get("title", ""),
        )

    # No tab_id: prefer top-level body when populated, else first top-level tab.
    body_content = (doc.get("body", {}) or {}).get("content", []) or []
    if body_content:
        return (
            _collect_tables_in_body(body_content),
            doc.get("documentStyle", {}) or {},
            "",
        )
    tabs = doc.get("tabs", []) or []
    if tabs:
        first = tabs[0]
        props = first.get("tabProperties", {}) or {}
        doc_tab = first.get("documentTab", {}) or {}
        body_content = doc_tab.get("body", {}).get("content", []) or []
        doc_style = doc_tab.get("documentStyle", {}) or {}
        return (
            _collect_tables_in_body(body_content),
            doc_style,
            props.get("title", ""),
        )
    return [], {}, ""


def _cell_text(cell):
    """Extract plain text from a table cell (joined paragraphs, no trailing newline)."""
    text_parts = []
    for element in cell.get("content", []) or []:
        if "paragraph" in element:
            for pe in element["paragraph"].get("elements", []) or []:
                tr = pe.get("textRun")
                if tr and "content" in tr:
                    text_parts.append(tr["content"])
    text = "".join(text_parts)
    # Drop the trailing newline Google Docs tacks onto cells.
    return text.rstrip("\n")


def _column_widths_for_table(table_el, inner_page_width_pt):
    """Return a list of per-column dicts: {width_pt, width_type, is_explicit}.

    For EVENLY_DISTRIBUTED columns, we derive an effective width by dividing
    the inner page width by the column count (best-effort visual estimate).
    """
    tbl = table_el["table"]
    col_props = tbl.get("tableStyle", {}).get("tableColumnProperties", []) or []
    n_cols = len(tbl.get("tableRows", [{}])[0].get("tableCells", [])) if tbl.get("tableRows") else 0

    out = []
    # When inner page width is unknown, fall back to a conventional 468pt
    # (Letter size with 1-inch margins) so wrap estimates still have a
    # usable reference.
    effective_page = inner_page_width_pt if inner_page_width_pt else 468.0
    even_width = effective_page / n_cols if n_cols else 0

    for i in range(n_cols):
        cp = col_props[i] if i < len(col_props) else {}
        wt = cp.get("widthType", "EVENLY_DISTRIBUTED")
        width_obj = cp.get("width") or {}
        explicit = (wt == "FIXED_WIDTH") and ("magnitude" in width_obj)
        if explicit:
            width_pt = float(width_obj.get("magnitude", 0))
        else:
            width_pt = float(even_width)
        out.append({
            "width_pt": round(width_pt, 2),
            "width_type": wt,
            "is_explicit": explicit,
        })
    return out


def _truncate(text, limit):
    text = text.replace("\n", " ").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "…"


def list_doc_tables(service, file_id: str = "", tab_id: str = None) -> str:
    """List tables in a Google Doc (or a specific tab) with width layout info."""
    logger.info(f"[list_doc_tables] Doc={file_id}, tab_id={tab_id}")
    if not file_id:
        return "Error: 'file_id' is required."

    doc = _fetch_doc_with_tabs(service, file_id)
    tables, doc_style, tab_title = _collect_tab_tables(doc, tab_id)

    inner_w = _inner_page_width_pt(doc_style)

    table_entries = []
    for idx, el in enumerate(tables):
        tbl = el["table"]
        rows = tbl.get("tableRows", []) or []
        n_rows = len(rows)
        n_cols = len(rows[0].get("tableCells", [])) if rows else 0

        col_widths = _column_widths_for_table(el, inner_w)
        total_width = round(sum(c["width_pt"] for c in col_widths), 2)

        # First-row preview (plain text per cell, truncated)
        first_row_preview = []
        if rows:
            for cell in rows[0].get("tableCells", []) or []:
                first_row_preview.append(_truncate(_cell_text(cell), 30))

        # Longest cell per column (chars). Skip header row so these numbers
        # reflect body content, which is usually where wrapping decisions
        # matter most. Fall back to all rows if only 1 row exists.
        longest_per_col = [0] * n_cols
        longest_sample_per_col = [""] * n_cols
        scan_rows = rows[1:] if n_rows > 1 else rows
        for row in scan_rows:
            for c_idx, cell in enumerate(row.get("tableCells", []) or []):
                text = _cell_text(cell)
                if len(text) > longest_per_col[c_idx]:
                    longest_per_col[c_idx] = len(text)
                    longest_sample_per_col[c_idx] = _truncate(text, 40)

        table_entries.append({
            "index": idx,
            "start_index": el.get("startIndex"),
            "end_index": el.get("endIndex"),
            "rows": n_rows,
            "columns": n_cols,
            "columns_detail": [
                {
                    "col": i,
                    "width_pt": col_widths[i]["width_pt"],
                    "width_type": col_widths[i]["width_type"],
                    "is_explicit": col_widths[i]["is_explicit"],
                    "longest_cell_chars": longest_per_col[i] if i < len(longest_per_col) else 0,
                    "longest_cell_sample": longest_sample_per_col[i] if i < len(longest_sample_per_col) else "",
                }
                for i in range(n_cols)
            ],
            "total_width_pt": total_width,
            "first_row_preview": first_row_preview,
        })

    result = {
        "file_id": file_id,
        "tab_id": tab_id,
        "tab_title": tab_title,
        "inner_page_width_pt": round(inner_w, 2) if inner_w else None,
        "table_count": len(tables),
        "tables": table_entries,
    }
    return json.dumps(result, indent=2)


def set_table_column_widths(
    service,
    file_id: str = "",
    table_index: int = 0,
    widths: List[float] = None,
    unit: str = "PT",
    tab_id: str = None,
) -> str:
    """Set fixed-width columns on a specific table by position.

    Args:
        file_id: document ID
        table_index: 0-based position of the table within the tab/body
        widths: list of widths (one per column), in the given unit
        unit: default PT; passed through to the API as-is
        tab_id: target tab (optional)
    """
    logger.info(
        f"[set_table_column_widths] Doc={file_id}, table_index={table_index}, "
        f"widths={widths}, unit={unit}, tab_id={tab_id}"
    )
    if not file_id:
        return "Error: 'file_id' is required."
    if not widths:
        return "Error: 'widths' (list of per-column widths) is required."

    doc = _fetch_doc_with_tabs(service, file_id)
    tables, _doc_style, _tab_title = _collect_tab_tables(doc, tab_id)
    if table_index < 0 or table_index >= len(tables):
        return (
            f"Error: table_index {table_index} out of range "
            f"(tab has {len(tables)} table(s))."
        )

    table_el = tables[table_index]
    tbl = table_el["table"]
    rows = tbl.get("tableRows", []) or []
    n_cols = len(rows[0].get("tableCells", [])) if rows else 0

    if len(widths) != n_cols:
        return (
            f"Error: got {len(widths)} widths but table has {n_cols} column(s)."
        )

    start_index = table_el.get("startIndex")
    if start_index is None:
        return "Error: could not determine tableStartLocation index."

    requests = []
    for col_idx, width_val in enumerate(widths):
        req = {
            "updateTableColumnProperties": {
                "tableStartLocation": {"index": start_index},
                "columnIndices": [col_idx],
                "tableColumnProperties": {
                    "widthType": "FIXED_WIDTH",
                    "width": {"magnitude": float(width_val), "unit": unit},
                },
                "fields": "widthType,width",
            }
        }
        requests.append(req)

    _apply_tab_id(requests, tab_id)

    service.documents().batchUpdate(
        documentId=file_id, body={"requests": requests}
    ).execute()

    return json.dumps({
        "updated": True,
        "file_id": file_id,
        "tab_id": tab_id,
        "table_index": table_index,
        "table_start_index": start_index,
        "columns": n_cols,
        "widths": [float(w) for w in widths],
        "unit": unit,
    }, indent=2)


def table_wrap_estimate(
    service,
    file_id: str = "",
    table_index: int = 0,
    widths: List[float] = None,
    font_size: float = 11.0,
    tab_id: str = None,
) -> str:
    """Predict how many lines each cell of a table will wrap to, given current
    or proposed column widths.

    Returns a JSON report with per-row max-lines, per-column detail, and a
    summary total_wrap_penalty (sum of lines above 1 across all cells).
    """
    logger.info(
        f"[table_wrap_estimate] Doc={file_id}, table_index={table_index}, "
        f"widths={widths}, font_size={font_size}, tab_id={tab_id}"
    )
    if not file_id:
        return "Error: 'file_id' is required."

    doc = _fetch_doc_with_tabs(service, file_id)
    tables, doc_style, _tab_title = _collect_tab_tables(doc, tab_id)
    if table_index < 0 or table_index >= len(tables):
        return (
            f"Error: table_index {table_index} out of range "
            f"(tab has {len(tables)} table(s))."
        )

    table_el = tables[table_index]
    tbl = table_el["table"]
    rows = tbl.get("tableRows", []) or []
    n_cols = len(rows[0].get("tableCells", [])) if rows else 0

    inner_w = _inner_page_width_pt(doc_style)
    current_widths = _column_widths_for_table(table_el, inner_w)

    if widths is None:
        active_widths = [c["width_pt"] for c in current_widths]
        source = "current"
    else:
        if len(widths) != n_cols:
            return (
                f"Error: got {len(widths)} widths but table has "
                f"{n_cols} column(s)."
            )
        active_widths = [float(w) for w in widths]
        source = "proposed"

    char_w = _char_width_pt(font_size)

    import math

    def wrap_lines(text, col_width_pt):
        # Number of lines required to fit `text` in a column of the given width.
        # Split on explicit newlines; each segment wraps independently.
        if col_width_pt <= 0:
            return max(1, len(text) or 1)
        # Chars-per-line rounded down (conservative). Guard against zero-width.
        chars_per_line = max(1, int(col_width_pt // char_w))
        lines = 0
        segments = text.split("\n") if text else [""]
        for seg in segments:
            if not seg:
                lines += 1
                continue
            lines += math.ceil(len(seg) / chars_per_line)
        return max(1, lines)

    per_row = []
    total_penalty = 0
    heavy_rows = 0
    col_max_lines = [0] * n_cols

    for r_idx, row in enumerate(rows):
        cells = row.get("tableCells", []) or []
        per_col_lines = []
        per_col_chars = []
        max_lines = 0
        for c_idx in range(n_cols):
            text = _cell_text(cells[c_idx]) if c_idx < len(cells) else ""
            lines = wrap_lines(text, active_widths[c_idx])
            per_col_lines.append(lines)
            per_col_chars.append(len(text))
            if lines > max_lines:
                max_lines = lines
            if lines > col_max_lines[c_idx]:
                col_max_lines[c_idx] = lines
            total_penalty += max(0, lines - 1)
        if max_lines >= 4:
            heavy_rows += 1
        per_row.append({
            "row_index": r_idx,
            "max_lines_in_row": max_lines,
            "per_col_lines": per_col_lines,
            "per_col_chars": per_col_chars,
            "wraps_heavily": max_lines >= 4,
        })

    # Convenience auto-rebalance: allocate widths proportional to longest cell
    # per column (skipping header row if present). Only included as a hint.
    scan_rows = rows[1:] if len(rows) > 1 else rows
    longest_per_col = [1] * n_cols
    for row in scan_rows:
        for c_idx, cell in enumerate(row.get("tableCells", []) or []):
            lc = len(_cell_text(cell))
            if lc > longest_per_col[c_idx]:
                longest_per_col[c_idx] = lc
    total_chars = sum(longest_per_col) or 1
    page_budget = inner_w if inner_w else 468.0
    # Enforce a minimum column width so tiny-char columns don't go to 0.
    min_col_pt = 24.0
    raw_allocs = [page_budget * (lc / total_chars) for lc in longest_per_col]
    # Post-process: clamp minimums, then re-scale the rest to fit the budget.
    floored = [max(min_col_pt, w) for w in raw_allocs]
    scale = page_budget / sum(floored) if sum(floored) else 1
    suggested_widths = [round(w * scale, 1) for w in floored]

    report = {
        "file_id": file_id,
        "tab_id": tab_id,
        "table_index": table_index,
        "table_start_index": table_el.get("startIndex"),
        "rows": len(rows),
        "columns": n_cols,
        "font_size_pt": font_size,
        "char_width_pt": round(char_w, 3),
        "widths_source": source,
        "widths_pt": [round(w, 2) for w in active_widths],
        "inner_page_width_pt": round(inner_w, 2) if inner_w else None,
        "total_widths_pt": round(sum(active_widths), 2),
        "per_row": per_row,
        "summary": {
            "total_wrap_penalty": total_penalty,
            "heavy_rows": heavy_rows,
            "max_lines_per_column": col_max_lines,
            "recommended_widths_pt": suggested_widths,
        },
    }
    return json.dumps(report, indent=2)
