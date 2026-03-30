"""CLI sub-commands for Google Sheets."""

import json

from gw.auth import get_service
from gw.output import success, error
from gw.services import sheets


def _parse_bool(value: str) -> bool:
    """Parse a boolean CLI argument."""
    return value.lower() in ("true", "1", "yes")


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------


def cmd_list(args):
    try:
        service = get_service("drive")
        result = sheets.list_spreadsheets(service, max_results=args.max_results)
        success(result)
    except Exception as e:
        error(str(e))


def cmd_info(args):
    try:
        service = get_service("sheets")
        result = sheets.get_spreadsheet_info(service, args.file_id)
        success(result)
    except Exception as e:
        error(str(e))


def cmd_read(args):
    try:
        service = get_service("sheets")
        result = sheets.read_values(
            service,
            args.file_id,
            range_name=args.range,
            value_render_option=args.render,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_batch_read(args):
    try:
        service = get_service("sheets")
        result = sheets.batch_read_values(
            service,
            args.file_id,
            ranges=args.ranges,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_write(args):
    try:
        service = get_service("sheets")
        values = json.loads(args.values) if args.values else None
        result = sheets.modify_values(
            service,
            args.file_id,
            range_name=args.range,
            values=values,
            value_input_option=args.input_option,
            clear_values=args.clear,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_find_replace(args):
    try:
        service = get_service("sheets")
        result = sheets.find_replace(
            service,
            args.file_id,
            find=args.find,
            replacement=args.replace,
            sheet_id=args.sheet_id,
            match_case=args.match_case,
            match_entire_cell=args.match_entire_cell,
            search_by_regex=args.use_regex,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_read_cells(args):
    try:
        service = get_service("sheets")
        result = sheets.get_sheet_cells(
            service,
            args.file_id,
            range_name=args.range,
            facets=args.facets,
            include_empty=args.include_empty,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_write_cells(args):
    try:
        service = get_service("sheets")
        cells = json.loads(args.cells)
        result = sheets.update_sheet_cells(
            service,
            args.file_id,
            cells=cells,
            mode=args.mode,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_transform(args):
    try:
        service = get_service("sheets")
        operations = json.loads(args.operations)
        result = sheets.transform_sheet_cells(
            service,
            args.file_id,
            range_name=args.range,
            operations=operations,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_format(args):
    try:
        service = get_service("sheets")
        result = sheets.format_range(
            service,
            args.file_id,
            range_name=args.range,
            background_color=args.bg_color,
            text_color=args.text_color,
            bold=args.bold,
            italic=args.italic,
            font_size=args.font_size,
            font_family=args.font_family,
            horizontal_alignment=args.h_align,
            vertical_alignment=args.v_align,
            wrap_strategy=args.wrap,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_borders(args):
    try:
        service = get_service("sheets")
        borders = json.loads(args.borders) if isinstance(args.borders, str) else args.borders
        # borders JSON can be a string like "all" or a comma-separated list
        if isinstance(borders, str):
            border_str = borders
        else:
            border_str = json.dumps(borders)
        result = sheets.update_borders(
            service,
            args.file_id,
            range_name=args.range,
            borders=border_str if isinstance(borders, str) else "all",
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_merge(args):
    try:
        service = get_service("sheets")
        result = sheets.merge_cells(
            service,
            args.file_id,
            range_name=args.range,
            merge_type=args.type,
            unmerge=False,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_unmerge(args):
    try:
        service = get_service("sheets")
        result = sheets.merge_cells(
            service,
            args.file_id,
            range_name=args.range,
            unmerge=True,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_conditional_format(args):
    try:
        service = get_service("sheets")
        action = args.action

        if action == "add":
            condition_values = json.loads(args.condition_values) if args.condition_values else None
            gradient_points = json.loads(args.gradient_points) if args.gradient_points else None
            result = sheets.add_conditional_formatting(
                service,
                args.file_id,
                range_name=args.range,
                condition_type=args.condition_type,
                condition_values=condition_values,
                background_color=args.bg_color,
                text_color=args.text_color,
                rule_index=args.rule_index,
                gradient_points=gradient_points,
            )
        elif action == "update":
            condition_values = json.loads(args.condition_values) if args.condition_values else None
            gradient_points = json.loads(args.gradient_points) if args.gradient_points else None
            result = sheets.update_conditional_formatting(
                service,
                args.file_id,
                rule_index=args.rule_index,
                range_name=args.range,
                condition_type=args.condition_type,
                condition_values=condition_values,
                background_color=args.bg_color,
                text_color=args.text_color,
                sheet_name=args.sheet_name,
                gradient_points=gradient_points,
            )
        elif action == "delete":
            result = sheets.delete_conditional_formatting(
                service,
                args.file_id,
                rule_index=args.rule_index,
                sheet_name=args.sheet_name,
            )
        else:
            error(f"Unknown action: {action}")
            return

        success(result)
    except Exception as e:
        error(str(e))


def cmd_create(args):
    try:
        service = get_service("sheets")
        result = sheets.create_spreadsheet(
            service,
            title=args.title,
            sheet_names=args.sheet_names,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_add_tab(args):
    try:
        service = get_service("sheets")
        result = sheets.create_sheet(
            service,
            args.file_id,
            sheet_name=args.title,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_duplicate_tab(args):
    try:
        service = get_service("sheets")
        result = sheets.duplicate_sheet(
            service,
            args.file_id,
            source_sheet_id=args.tab_id,
            new_name=args.name,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_delete_tab(args):
    try:
        service = get_service("sheets")
        result = sheets.delete_sheet(
            service,
            args.file_id,
            sheet_id=args.tab_id,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_update_tab(args):
    try:
        service = get_service("sheets")
        hidden = None
        if args.hidden is not None:
            hidden = _parse_bool(args.hidden)
        result = sheets.update_sheet_properties(
            service,
            args.file_id,
            sheet_id=args.tab_id,
            new_name=args.title,
            hidden=hidden,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_insert_dimension(args):
    try:
        service = get_service("sheets")
        result = sheets.insert_dimension(
            service,
            args.file_id,
            sheet_id=args.tab_id,
            dimension=args.dimension,
            start_index=args.start,
            end_index=args.end,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_delete_dimension(args):
    try:
        service = get_service("sheets")
        result = sheets.delete_dimension(
            service,
            args.file_id,
            sheet_id=args.tab_id,
            dimension=args.dimension,
            start_index=args.start,
            end_index=args.end,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_resize(args):
    try:
        service = get_service("sheets")
        result = sheets.resize_dimension(
            service,
            args.file_id,
            sheet_id=args.tab_id,
            dimension=args.dimension,
            start_index=args.start,
            end_index=args.end,
            pixel_size=args.size,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_auto_resize(args):
    try:
        service = get_service("sheets")
        result = sheets.auto_resize_dimensions(
            service,
            args.file_id,
            sheet_id=args.tab_id,
            dimension=args.dimension,
            start_index=args.start,
            end_index=args.end,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_freeze(args):
    try:
        service = get_service("sheets")
        result = sheets.freeze_dimensions(
            service,
            args.file_id,
            sheet_id=args.tab_id,
            frozen_rows=args.rows,
            frozen_columns=args.cols,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_sort(args):
    try:
        service = get_service("sheets")
        order = "ASCENDING" if args.ascending else "DESCENDING"
        sort_specs = [{"column_index": args.column, "order": order}]
        result = sheets.sort_range(
            service,
            args.file_id,
            range_name=args.range,
            sort_specs=sort_specs,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_validate(args):
    try:
        service = get_service("sheets")
        rule = json.loads(args.rule)
        result = sheets.set_data_validation(
            service,
            args.file_id,
            range_name=args.range,
            validation_type=rule.get("type", "ONE_OF_LIST"),
            values=rule.get("values"),
            strict=rule.get("strict", True),
            show_dropdown=rule.get("show_dropdown", True),
            clear=rule.get("clear", False),
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_named_range(args):
    try:
        service = get_service("sheets")
        # CLI uses create/update/delete; service uses add/update/delete
        action_map = {"create": "add", "update": "update", "delete": "delete"}
        result = sheets.manage_named_ranges(
            service,
            args.file_id,
            action=action_map[args.action],
            name=args.name,
            range_name=args.range,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_filter_view(args):
    try:
        service = get_service("sheets")
        filter_json = json.loads(args.filter) if args.filter else {}
        # CLI uses create/get/update/delete; service uses add/clear_basic
        action_map = {"create": "add", "delete": "clear_basic",
                      "get": "add", "update": "add"}
        action = action_map.get(args.action, args.action)
        range_name = filter_json.get("range") if filter_json else None
        result = sheets.manage_filter_view(
            service,
            args.file_id,
            action=action,
            range_name=range_name,
        )
        success(result)
    except Exception as e:
        error(str(e))


def cmd_protected_range(args):
    try:
        service = get_service("sheets")
        editors_json = json.loads(args.editors) if args.editors else None
        # CLI uses create/update/delete; service uses add/delete
        action_map = {"create": "add", "update": "add", "delete": "delete"}
        result = sheets.manage_protected_range(
            service,
            args.file_id,
            action=action_map.get(args.action, args.action),
            range_name=args.range,
            protected_range_id=int(editors_json["id"]) if editors_json and "id" in editors_json else None,
        )
        success(result)
    except Exception as e:
        error(str(e))


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


def register(subparsers):
    """Register all ``sheets`` sub-commands on *subparsers*."""
    sheets_parser = subparsers.add_parser("sheets", help="Google Sheets operations")
    sheets_sub = sheets_parser.add_subparsers(dest="sheets_command")

    # list
    p = sheets_sub.add_parser("list", help="List spreadsheets")
    p.add_argument("--max-results", type=int, default=25)
    p.set_defaults(func=cmd_list)

    # info
    p = sheets_sub.add_parser("info", help="Get spreadsheet info")
    p.add_argument("file_id")
    p.set_defaults(func=cmd_info)

    # read
    p = sheets_sub.add_parser("read", help="Read cell values from a range")
    p.add_argument("file_id")
    p.add_argument("--range", default="A1:Z1000")
    p.add_argument("--render", default="FORMATTED_VALUE",
                   choices=["FORMATTED_VALUE", "UNFORMATTED_VALUE", "FORMULA"])
    p.set_defaults(func=cmd_read)

    # batch-read
    p = sheets_sub.add_parser("batch-read", help="Read values from multiple ranges")
    p.add_argument("file_id")
    p.add_argument("--ranges", nargs="+", required=True)
    p.set_defaults(func=cmd_batch_read)

    # write
    p = sheets_sub.add_parser("write", help="Write or clear values in a range")
    p.add_argument("file_id")
    p.add_argument("--range", required=True)
    p.add_argument("--values", default=None, help="JSON array of arrays")
    p.add_argument("--input-option", default="USER_ENTERED",
                   choices=["USER_ENTERED", "RAW"])
    p.add_argument("--clear", action="store_true")
    p.set_defaults(func=cmd_write)

    # find-replace
    p = sheets_sub.add_parser("find-replace", help="Find and replace text")
    p.add_argument("file_id")
    p.add_argument("--find", required=True)
    p.add_argument("--replace", required=True)
    p.add_argument("--sheet-id", type=int, default=None)
    p.add_argument("--match-case", action="store_true")
    p.add_argument("--match-entire-cell", action="store_true")
    p.add_argument("--use-regex", action="store_true")
    p.set_defaults(func=cmd_find_replace)

    # read-cells
    p = sheets_sub.add_parser("read-cells", help="Read structured CellData")
    p.add_argument("file_id")
    p.add_argument("--range", default="A1:Z1000")
    p.add_argument("--facets", default=None, help="JSON list of facet names")
    p.add_argument("--include-empty", action="store_true")
    p.set_defaults(func=cmd_read_cells)

    # write-cells
    p = sheets_sub.add_parser("write-cells", help="Apply CellData patches")
    p.add_argument("file_id")
    p.add_argument("--cells", required=True, help="JSON array of cell patches")
    p.add_argument("--mode", default="patch", choices=["patch", "replace"])
    p.set_defaults(func=cmd_write_cells)

    # transform
    p = sheets_sub.add_parser("transform", help="Apply declarative transformations")
    p.add_argument("file_id")
    p.add_argument("--range", required=True)
    p.add_argument("--operations", required=True, help="JSON array of operations")
    p.set_defaults(func=cmd_transform)

    # format
    p = sheets_sub.add_parser("format", help="Apply formatting to a range")
    p.add_argument("file_id")
    p.add_argument("--range", required=True)
    p.add_argument("--bg-color", default=None, help="Background color (#RRGGBB)")
    p.add_argument("--text-color", default=None, help="Text color (#RRGGBB)")
    p.add_argument("--bold", action="store_true", default=None)
    p.add_argument("--italic", action="store_true", default=None)
    p.add_argument("--font-size", type=int, default=None)
    p.add_argument("--font-family", default=None)
    p.add_argument("--h-align", default=None, choices=["LEFT", "CENTER", "RIGHT"])
    p.add_argument("--v-align", default=None, choices=["TOP", "MIDDLE", "BOTTOM"])
    p.add_argument("--wrap", default=None, choices=["OVERFLOW_CELL", "CLIP", "WRAP"])
    p.set_defaults(func=cmd_format)

    # borders
    p = sheets_sub.add_parser("borders", help="Apply borders to a range")
    p.add_argument("file_id")
    p.add_argument("--range", required=True)
    p.add_argument("--borders", required=True, help="Border spec: all, outer, inner, or JSON")
    p.set_defaults(func=cmd_borders)

    # merge
    p = sheets_sub.add_parser("merge", help="Merge cells in a range")
    p.add_argument("file_id")
    p.add_argument("--range", required=True)
    p.add_argument("--type", default="MERGE_ALL",
                   choices=["MERGE_ALL", "MERGE_COLUMNS", "MERGE_ROWS"])
    p.set_defaults(func=cmd_merge)

    # unmerge
    p = sheets_sub.add_parser("unmerge", help="Unmerge cells in a range")
    p.add_argument("file_id")
    p.add_argument("--range", required=True)
    p.set_defaults(func=cmd_unmerge)

    # conditional-format
    p = sheets_sub.add_parser("conditional-format", help="Manage conditional formatting")
    p.add_argument("file_id")
    p.add_argument("--action", required=True, choices=["add", "update", "delete"])
    p.add_argument("--range", default=None)
    p.add_argument("--condition-type", default=None)
    p.add_argument("--condition-values", default=None, help="JSON list")
    p.add_argument("--bg-color", default=None)
    p.add_argument("--text-color", default=None)
    p.add_argument("--rule-index", type=int, default=None)
    p.add_argument("--gradient-points", default=None, help="JSON list")
    p.add_argument("--sheet-name", default=None)
    p.set_defaults(func=cmd_conditional_format)

    # create
    p = sheets_sub.add_parser("create", help="Create a new spreadsheet")
    p.add_argument("--title", required=True)
    p.add_argument("--sheet-names", nargs="*", default=None)
    p.set_defaults(func=cmd_create)

    # add-tab
    p = sheets_sub.add_parser("add-tab", help="Add a sheet tab")
    p.add_argument("file_id")
    p.add_argument("--title", required=True)
    p.set_defaults(func=cmd_add_tab)

    # duplicate-tab
    p = sheets_sub.add_parser("duplicate-tab", help="Duplicate a sheet tab")
    p.add_argument("file_id")
    p.add_argument("--tab-id", type=int, required=True)
    p.add_argument("--name", default=None)
    p.set_defaults(func=cmd_duplicate_tab)

    # delete-tab
    p = sheets_sub.add_parser("delete-tab", help="Delete a sheet tab")
    p.add_argument("file_id")
    p.add_argument("--tab-id", type=int, required=True)
    p.set_defaults(func=cmd_delete_tab)

    # update-tab
    p = sheets_sub.add_parser("update-tab", help="Update sheet tab properties")
    p.add_argument("file_id")
    p.add_argument("--tab-id", type=int, required=True)
    p.add_argument("--title", default=None)
    p.add_argument("--hidden", default=None, help="true or false")
    p.set_defaults(func=cmd_update_tab)

    # insert-dimension
    p = sheets_sub.add_parser("insert-dimension", help="Insert rows or columns")
    p.add_argument("file_id")
    p.add_argument("--tab-id", type=int, required=True)
    p.add_argument("--dimension", required=True, choices=["ROWS", "COLUMNS"])
    p.add_argument("--start", type=int, required=True)
    p.add_argument("--end", type=int, required=True)
    p.set_defaults(func=cmd_insert_dimension)

    # delete-dimension
    p = sheets_sub.add_parser("delete-dimension", help="Delete rows or columns")
    p.add_argument("file_id")
    p.add_argument("--tab-id", type=int, required=True)
    p.add_argument("--dimension", required=True, choices=["ROWS", "COLUMNS"])
    p.add_argument("--start", type=int, required=True)
    p.add_argument("--end", type=int, required=True)
    p.set_defaults(func=cmd_delete_dimension)

    # resize
    p = sheets_sub.add_parser("resize", help="Resize rows or columns to a pixel size")
    p.add_argument("file_id")
    p.add_argument("--tab-id", type=int, required=True)
    p.add_argument("--dimension", required=True, choices=["ROWS", "COLUMNS"])
    p.add_argument("--start", type=int, required=True)
    p.add_argument("--end", type=int, required=True)
    p.add_argument("--size", type=int, required=True, help="Pixel size")
    p.set_defaults(func=cmd_resize)

    # auto-resize
    p = sheets_sub.add_parser("auto-resize", help="Auto-resize rows or columns")
    p.add_argument("file_id")
    p.add_argument("--tab-id", type=int, required=True)
    p.add_argument("--dimension", required=True, choices=["ROWS", "COLUMNS"])
    p.add_argument("--start", type=int, required=True)
    p.add_argument("--end", type=int, default=None)
    p.set_defaults(func=cmd_auto_resize)

    # freeze
    p = sheets_sub.add_parser("freeze", help="Freeze rows and/or columns")
    p.add_argument("file_id")
    p.add_argument("--tab-id", type=int, required=True)
    p.add_argument("--rows", type=int, default=None)
    p.add_argument("--cols", type=int, default=None)
    p.set_defaults(func=cmd_freeze)

    # sort
    p = sheets_sub.add_parser("sort", help="Sort data in a range")
    p.add_argument("file_id")
    p.add_argument("--range", required=True)
    p.add_argument("--column", type=int, required=True, help="Column index (0-based)")
    p.add_argument("--ascending", action="store_true")
    p.set_defaults(func=cmd_sort)

    # validate
    p = sheets_sub.add_parser("validate", help="Set data validation on a range")
    p.add_argument("file_id")
    p.add_argument("--range", required=True)
    p.add_argument("--rule", required=True, help="JSON validation rule")
    p.set_defaults(func=cmd_validate)

    # named-range
    p = sheets_sub.add_parser("named-range", help="Manage named ranges")
    p.add_argument("file_id")
    p.add_argument("--action", required=True, choices=["create", "update", "delete"])
    p.add_argument("--name", required=True)
    p.add_argument("--range", default=None)
    p.set_defaults(func=cmd_named_range)

    # filter-view
    p = sheets_sub.add_parser("filter-view", help="Manage filter views")
    p.add_argument("file_id")
    p.add_argument("--action", required=True, choices=["create", "get", "update", "delete"])
    p.add_argument("--filter", default=None, help="JSON filter specification")
    p.set_defaults(func=cmd_filter_view)

    # protected-range
    p = sheets_sub.add_parser("protected-range", help="Manage protected ranges")
    p.add_argument("file_id")
    p.add_argument("--action", required=True, choices=["create", "update", "delete"])
    p.add_argument("--range", default=None)
    p.add_argument("--editors", default=None, help="JSON editors specification")
    p.set_defaults(func=cmd_protected_range)

    return sheets_parser
