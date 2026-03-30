"""CLI subcommands for Google Docs."""

import json

from gw.auth import get_service, get_services
from gw.output import success, error
from gw.services import docs


# ---------------------------------------------------------------------------
# Command handlers
# ---------------------------------------------------------------------------

def cmd_read(args):
    try:
        drive_service, docs_service = get_services("drive", "docs")
        result = docs.get_doc_content(drive_service, docs_service, args.file_id)
        success(result)
    except Exception as e:
        error(str(e))


def cmd_inspect(args):
    try:
        service = get_service("docs")
        result = docs.inspect_doc_structure(service, args.file_id, detailed=args.detailed)
        success(result)
    except Exception as e:
        error(str(e))


def cmd_edit(args):
    try:
        service = get_service("docs")
        result = docs.find_and_replace_doc(
            service, args.file_id, args.find, args.replace,
            match_case=args.match_case,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_insert_text(args):
    try:
        service = get_service("docs")
        result = docs.modify_doc_text(service, args.file_id, start_index=args.index, text=args.text)
        success(result)
    except Exception as e:
        error(str(e))


def cmd_insert_table(args):
    try:
        service = get_service("docs")
        result = docs.insert_table(service, args.file_id, index=args.index, rows=args.rows, columns=args.cols)
        success(result)
    except Exception as e:
        error(str(e))


def cmd_create_table(args):
    try:
        service = get_service("docs")
        table_data = json.loads(args.data)
        result = docs.create_table_with_data(
            service, args.file_id, table_data=table_data,
            index=args.index, bold_headers=args.bold_headers,
        )
        success(result)
    except json.JSONDecodeError as e:
        error(f"Invalid JSON for --data: {e}")
    except Exception as e:
        error(str(e))


def cmd_insert_image(args):
    try:
        docs_service, drive_service = get_services("docs", "drive")
        result = docs.insert_doc_image(
            docs_service, drive_service, args.file_id,
            image_source=args.url, index=args.index,
            width=args.width, height=args.height,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_insert_list(args):
    try:
        service = get_service("docs")
        items = json.loads(args.items)
        list_type = "ORDERED" if args.ordered else "UNORDERED"
        text = "\n".join(items) if isinstance(items, list) else str(items)
        result = docs.insert_list(
            service, args.file_id, index=args.index,
            list_type=list_type, text=text,
        )
        success(result)
    except json.JSONDecodeError as e:
        error(f"Invalid JSON for --items: {e}")
    except Exception as e:
        error(str(e))


def cmd_insert_page_break(args):
    try:
        service = get_service("docs")
        result = docs.insert_page_break(service, args.file_id, index=args.index)
        success(result)
    except Exception as e:
        error(str(e))


def cmd_insert_section_break(args):
    try:
        service = get_service("docs")
        result = docs.insert_section_break(
            service, args.file_id, index=args.index, section_type=args.type,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_insert_footnote(args):
    try:
        service = get_service("docs")
        result = docs.insert_footnote(service, args.file_id, index=args.index, footnote_text=args.text)
        success(result)
    except Exception as e:
        error(str(e))


def cmd_delete_object(args):
    try:
        service = get_service("docs")
        result = docs.delete_positioned_object(service, args.file_id, object_id=args.object_id)
        success(result)
    except Exception as e:
        error(str(e))


def cmd_update_paragraph_style(args):
    try:
        service = get_service("docs")
        result = docs.update_paragraph_style(
            service, args.file_id,
            start_index=args.start, end_index=args.end,
            heading_type=args.heading, alignment=args.alignment,
            line_spacing=args.line_spacing, space_above=args.space_above,
            space_below=args.space_below, indent_first_line=args.indent_first_line,
            indent_start=args.indent_start, indent_end=args.indent_end,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_update_document_style(args):
    try:
        service = get_service("docs")
        result = docs.update_document_style(
            service, args.file_id,
            margin_top=args.margin_top, margin_bottom=args.margin_bottom,
            margin_left=args.margin_left, margin_right=args.margin_right,
            page_width=args.page_width, page_height=args.page_height,
            default_font_family=args.default_font_family,
            default_font_size=args.default_font_size,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_manage_named_range(args):
    try:
        service = get_service("docs")
        result = docs.manage_named_range(
            service, args.file_id,
            action=args.action, name=args.name,
            start_index=args.start, end_index=args.end,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_manage_table(args):
    try:
        service = get_service("docs")
        result = docs.manage_table_structure(
            service, args.file_id,
            action=args.action, table_start_index=args.table_index,
            row_index=args.row_index, column_index=args.column_index,
            insert_below=args.insert_below, insert_right=args.insert_right,
            start_row=args.start_row, end_row=args.end_row,
            start_column=args.start_column, end_column=args.end_column,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_debug_table(args):
    try:
        service = get_service("docs")
        result = docs.debug_table_structure(service, args.file_id, table_index=args.table_index)
        success(result)
    except Exception as e:
        error(str(e))


def cmd_batch_update(args):
    try:
        service = get_service("docs")
        operations = json.loads(args.requests)
        result = docs.batch_update_doc(service, args.file_id, operations=operations)
        success(result)
    except json.JSONDecodeError as e:
        error(f"Invalid JSON for --requests: {e}")
    except Exception as e:
        error(str(e))


def cmd_header_footer(args):
    try:
        service = get_service("docs")
        result = docs.update_doc_headers_footers(
            service, args.file_id,
            section_type=args.type or "",
            content=args.content or "",
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_export_pdf(args):
    try:
        service = get_service("drive")
        result = docs.export_doc_to_pdf(
            service, args.file_id,
            pdf_filename=args.output, folder_id=args.folder_id,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_create(args):
    try:
        service = get_service("docs")
        result = docs.create_doc(service, title=args.title, content=args.content or "")
        success(result)
    except Exception as e:
        error(str(e))


def cmd_search(args):
    try:
        service = get_service("drive")
        result = docs.search_docs(service, query=args.query, page_size=args.max_results)
        success(result)
    except Exception as e:
        error(str(e))


def cmd_list_in_folder(args):
    try:
        service = get_service("drive")
        result = docs.list_docs_in_folder(
            service, folder_id=args.folder_id, page_size=args.max_results,
        )
        success(result)
    except Exception as e:
        error(str(e))


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------

def register(subparsers):
    """Register the 'docs' command group with its subcommands."""
    docs_parser = subparsers.add_parser("docs", help="Google Docs operations")
    docs_sub = docs_parser.add_subparsers(dest="docs_command", required=True)

    # read
    p = docs_sub.add_parser("read", help="Read a Google Doc's content")
    p.add_argument("file_id", help="Document or file ID")
    p.set_defaults(func=cmd_read)

    # inspect
    p = docs_sub.add_parser("inspect", help="Inspect document structure")
    p.add_argument("file_id", help="Document ID")
    p.add_argument("--detailed", action="store_true", help="Show detailed element breakdown")
    p.set_defaults(func=cmd_inspect)

    # edit (find-and-replace)
    p = docs_sub.add_parser("edit", help="Find and replace text in a doc")
    p.add_argument("file_id", help="Document ID")
    p.add_argument("--find", required=True, help="Text to find")
    p.add_argument("--replace", required=True, help="Replacement text")
    p.add_argument("--match-case", action="store_true", help="Case-sensitive matching")
    p.set_defaults(func=cmd_edit)

    # insert-text
    p = docs_sub.add_parser("insert-text", help="Insert text at an index")
    p.add_argument("file_id", help="Document ID")
    p.add_argument("--text", required=True, help="Text to insert")
    p.add_argument("--index", type=int, default=0, help="Insertion index (default: 0)")
    p.set_defaults(func=cmd_insert_text)

    # insert-table
    p = docs_sub.add_parser("insert-table", help="Insert an empty table")
    p.add_argument("file_id", help="Document ID")
    p.add_argument("--rows", type=int, required=True, help="Number of rows")
    p.add_argument("--cols", type=int, required=True, help="Number of columns")
    p.add_argument("--index", type=int, default=0, help="Insertion index")
    p.set_defaults(func=cmd_insert_table)

    # create-table (with data)
    p = docs_sub.add_parser("create-table", help="Create a table pre-populated with data")
    p.add_argument("file_id", help="Document ID")
    p.add_argument("--data", required=True, help="2D JSON array of cell strings")
    p.add_argument("--index", type=int, default=0, help="Insertion index")
    p.add_argument("--bold-headers", action="store_true", default=True, help="Bold the first row (default: true)")
    p.set_defaults(func=cmd_create_table)

    # insert-image
    p = docs_sub.add_parser("insert-image", help="Insert an image from URL or Drive file ID")
    p.add_argument("file_id", help="Document ID")
    p.add_argument("--url", required=True, help="Image URL or Drive file ID")
    p.add_argument("--index", type=int, default=0, help="Insertion index")
    p.add_argument("--width", type=int, default=0, help="Width in points")
    p.add_argument("--height", type=int, default=0, help="Height in points")
    p.set_defaults(func=cmd_insert_image)

    # insert-list
    p = docs_sub.add_parser("insert-list", help="Insert a bulleted or numbered list")
    p.add_argument("file_id", help="Document ID")
    p.add_argument("--items", required=True, help='JSON array of list items, e.g. \'["a","b","c"]\'')
    p.add_argument("--index", type=int, default=0, help="Insertion index")
    p.add_argument("--ordered", action="store_true", help="Numbered list instead of bullets")
    p.set_defaults(func=cmd_insert_list)

    # insert-page-break
    p = docs_sub.add_parser("insert-page-break", help="Insert a page break")
    p.add_argument("file_id", help="Document ID")
    p.add_argument("--index", type=int, default=0, help="Insertion index")
    p.set_defaults(func=cmd_insert_page_break)

    # insert-section-break
    p = docs_sub.add_parser("insert-section-break", help="Insert a section break")
    p.add_argument("file_id", help="Document ID")
    p.add_argument("--index", type=int, default=0, help="Insertion index")
    p.add_argument("--type", default="NEXT_PAGE", choices=["NEXT_PAGE", "CONTINUOUS"], help="Section break type")
    p.set_defaults(func=cmd_insert_section_break)

    # insert-footnote
    p = docs_sub.add_parser("insert-footnote", help="Insert a footnote")
    p.add_argument("file_id", help="Document ID")
    p.add_argument("--index", type=int, required=True, help="Index for the footnote reference")
    p.add_argument("--text", required=True, help="Footnote body text")
    p.set_defaults(func=cmd_insert_footnote)

    # delete-object
    p = docs_sub.add_parser("delete-object", help="Delete a positioned object by ID")
    p.add_argument("file_id", help="Document ID")
    p.add_argument("--object-id", required=True, help="Object ID to delete")
    p.set_defaults(func=cmd_delete_object)

    # update-paragraph-style
    p = docs_sub.add_parser("update-paragraph-style", help="Apply paragraph formatting to a range")
    p.add_argument("file_id", help="Document ID")
    p.add_argument("--start", type=int, required=True, help="Start index")
    p.add_argument("--end", type=int, required=True, help="End index")
    p.add_argument("--heading", help="Heading type (NORMAL_TEXT, HEADING_1..6, TITLE, SUBTITLE)")
    p.add_argument("--alignment", help="Alignment (START, CENTER, END, JUSTIFIED)")
    p.add_argument("--line-spacing", type=float, help="Line spacing multiplier")
    p.add_argument("--space-above", type=float, help="Space above paragraph (pt)")
    p.add_argument("--space-below", type=float, help="Space below paragraph (pt)")
    p.add_argument("--indent-first-line", type=float, help="First-line indent (pt)")
    p.add_argument("--indent-start", type=float, help="Start indent (pt)")
    p.add_argument("--indent-end", type=float, help="End indent (pt)")
    p.set_defaults(func=cmd_update_paragraph_style)

    # update-document-style
    p = docs_sub.add_parser("update-document-style", help="Set page-level defaults")
    p.add_argument("file_id", help="Document ID")
    p.add_argument("--margin-top", type=float, help="Top margin (pt)")
    p.add_argument("--margin-bottom", type=float, help="Bottom margin (pt)")
    p.add_argument("--margin-left", type=float, help="Left margin (pt)")
    p.add_argument("--margin-right", type=float, help="Right margin (pt)")
    p.add_argument("--page-width", type=float, help="Page width (pt)")
    p.add_argument("--page-height", type=float, help="Page height (pt)")
    p.add_argument("--default-font-family", help="Default font family")
    p.add_argument("--default-font-size", type=float, help="Default font size (pt)")
    p.set_defaults(func=cmd_update_document_style)

    # manage-named-range
    p = docs_sub.add_parser("manage-named-range", help="Create or delete a named range")
    p.add_argument("file_id", help="Document ID")
    p.add_argument("--action", required=True, choices=["create", "delete"], help="Action to perform")
    p.add_argument("--name", required=True, help="Named range name")
    p.add_argument("--start", type=int, help="Start index (for create)")
    p.add_argument("--end", type=int, help="End index (for create)")
    p.set_defaults(func=cmd_manage_named_range)

    # manage-table
    p = docs_sub.add_parser("manage-table", help="Modify table structure (insert/delete rows/columns, merge/unmerge)")
    p.add_argument("file_id", help="Document ID")
    p.add_argument("--action", required=True,
                   choices=["insert_row", "insert_column", "delete_row", "delete_column", "merge_cells", "unmerge_cells"],
                   help="Table operation")
    p.add_argument("--table-index", type=int, required=True, help="Table start index in document")
    p.add_argument("--row-index", type=int, help="Row index for insert/delete row")
    p.add_argument("--column-index", type=int, help="Column index for insert/delete column")
    p.add_argument("--insert-below", action="store_true", default=True, help="Insert row below (default: true)")
    p.add_argument("--insert-right", action="store_true", default=True, help="Insert column to right (default: true)")
    p.add_argument("--start-row", type=int, help="Start row for merge/unmerge")
    p.add_argument("--end-row", type=int, help="End row for merge/unmerge")
    p.add_argument("--start-column", type=int, help="Start column for merge/unmerge")
    p.add_argument("--end-column", type=int, help="End column for merge/unmerge")
    p.set_defaults(func=cmd_manage_table)

    # debug-table
    p = docs_sub.add_parser("debug-table", help="Inspect a table's internal structure")
    p.add_argument("file_id", help="Document ID")
    p.add_argument("--table-index", type=int, required=True, help="Zero-based table index")
    p.set_defaults(func=cmd_debug_table)

    # batch-update
    p = docs_sub.add_parser("batch-update", help="Execute raw batch update operations")
    p.add_argument("file_id", help="Document ID")
    p.add_argument("--requests", required=True, help="JSON array of operation objects")
    p.set_defaults(func=cmd_batch_update)

    # header-footer
    p = docs_sub.add_parser("header-footer", help="Manage headers and footers")
    p.add_argument("file_id", help="Document ID")
    p.add_argument("--action", required=True, choices=["get", "create", "delete"], help="Action")
    p.add_argument("--type", choices=["header", "footer"], help="Header or footer")
    p.add_argument("--content", help="Content text for create action")
    p.set_defaults(func=cmd_header_footer)

    # export-pdf
    p = docs_sub.add_parser("export-pdf", help="Export a Google Doc to PDF on Drive")
    p.add_argument("file_id", help="Document ID")
    p.add_argument("--output", help="PDF filename (default: <docname>_PDF.pdf)")
    p.add_argument("--folder-id", help="Target Drive folder ID")
    p.set_defaults(func=cmd_export_pdf)

    # create
    p = docs_sub.add_parser("create", help="Create a new Google Doc")
    p.add_argument("--title", required=True, help="Document title")
    p.add_argument("--content", help="Initial content text")
    p.set_defaults(func=cmd_create)

    # search
    p = docs_sub.add_parser("search", help="Search for Google Docs by name")
    p.add_argument("query", help="Search query")
    p.add_argument("--max-results", type=int, default=10, help="Maximum results (default: 10)")
    p.set_defaults(func=cmd_search)

    # list-in-folder
    p = docs_sub.add_parser("list-in-folder", help="List Google Docs in a Drive folder")
    p.add_argument("--folder-id", default="root", help="Folder ID (default: root)")
    p.add_argument("--max-results", type=int, default=100, help="Maximum results (default: 100)")
    p.set_defaults(func=cmd_list_in_folder)
