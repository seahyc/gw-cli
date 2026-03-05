"""
Google Sheets MCP Tools

This module provides MCP tools for interacting with Google Sheets API.
"""

import logging
import asyncio
import json
import copy
from typing import List, Literal, Optional, Union

from auth.service_decorator import require_google_service
from core.server import server
from core.utils import handle_http_errors, UserInputError
from core.comments import create_comment_tools
from gsheets.sheets_helpers import (
    CONDITION_TYPES,
    _a1_range_for_values,
    _build_boolean_rule,
    _build_gradient_rule,
    _fetch_detailed_sheet_errors,
    _fetch_sheets_with_rules,
    _format_conditional_rules_section,
    _format_sheet_error_section,
    _get_sheet_id_by_name,
    _parse_a1_range,
    _parse_condition_values,
    _parse_gradient_points,
    _parse_hex_color,
    _select_sheet,
    _values_contain_sheets_errors,
)

# Configure module logger
logger = logging.getLogger(__name__)


@server.tool()
@handle_http_errors("list_spreadsheets", is_read_only=True, service_type="sheets")
@require_google_service("drive", "drive_read")
async def list_spreadsheets(
    service,
    user_google_email: str = "",
    max_results: int = 25,
) -> str:
    """List spreadsheets the user has access to."""
    logger.info(f"[list_spreadsheets] Invoked. Email: '{user_google_email}'")

    files_response = await asyncio.to_thread(
        service.files()
        .list(
            q="mimeType='application/vnd.google-apps.spreadsheet'",
            pageSize=max_results,
            fields="files(id,name,modifiedTime,webViewLink)",
            orderBy="modifiedTime desc",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        .execute
    )

    files = files_response.get("files", [])
    if not files:
        return "No spreadsheets found."

    spreadsheets_list = [
        f'- "{file["name"]}" (ID: {file["id"]}) | Modified: {file.get("modifiedTime", "Unknown")} | Link: {file.get("webViewLink", "No link")}'
        for file in files
    ]

    text_output = (
        f"Found {len(files)} spreadsheets:\n"
        + "\n".join(spreadsheets_list)
    )

    logger.info(
        f"Successfully listed {len(files)} spreadsheets for {user_google_email}."
    )
    return text_output


@server.tool()
@handle_http_errors("get_spreadsheet_info", is_read_only=True, service_type="sheets")
@require_google_service("sheets", "sheets_read")
async def get_spreadsheet_info(
    service,
    file_id: str,
    user_google_email: str = "",
) -> str:
    """Get spreadsheet metadata including title, locale, and sheet list with conditional formats."""
    logger.info(
        f"[get_spreadsheet_info] Invoked. Email: '{user_google_email}', Spreadsheet ID: {file_id}"
    )

    spreadsheet = await asyncio.to_thread(
        service.spreadsheets()
        .get(
            spreadsheetId=file_id,
            fields="spreadsheetId,properties(title,locale),sheets(properties(title,sheetId,gridProperties(rowCount,columnCount)),conditionalFormats)",
        )
        .execute
    )

    properties = spreadsheet.get("properties", {})
    title = properties.get("title", "Unknown")
    locale = properties.get("locale", "Unknown")
    sheets = spreadsheet.get("sheets", [])

    sheet_titles = {}
    for sheet in sheets:
        sheet_props = sheet.get("properties", {})
        sid = sheet_props.get("sheetId")
        if sid is not None:
            sheet_titles[sid] = sheet_props.get("title", f"Sheet {sid}")

    sheets_info = []
    for sheet in sheets:
        sheet_props = sheet.get("properties", {})
        sheet_name = sheet_props.get("title", "Unknown")
        sheet_id = sheet_props.get("sheetId", "Unknown")
        grid_props = sheet_props.get("gridProperties", {})
        rows = grid_props.get("rowCount", "Unknown")
        cols = grid_props.get("columnCount", "Unknown")
        rules = sheet.get("conditionalFormats", []) or []

        sheets_info.append(
            f'  - "{sheet_name}" (ID: {sheet_id}) | Size: {rows}x{cols} | Conditional formats: {len(rules)}'
        )
        if rules:
            sheets_info.append(
                _format_conditional_rules_section(
                    sheet_name, rules, sheet_titles, indent="    "
                )
            )

    sheets_section = "\n".join(sheets_info) if sheets_info else "  No sheets found"
    text_output = "\n".join(
        [
            f'Spreadsheet: "{title}" (ID: {file_id}) | Locale: {locale}',
            f"Sheets ({len(sheets)}):",
            sheets_section,
        ]
    )

    logger.info(
        f"Successfully retrieved info for spreadsheet {file_id} for {user_google_email}."
    )
    return text_output


@server.tool()
@handle_http_errors("read_sheet_values", is_read_only=True, service_type="sheets")
@require_google_service("sheets", "sheets_read")
async def read_sheet_values(
    service,
    file_id: str,
    range_name: str = "A1:Z1000",
    value_render_option: Literal[
        "FORMATTED_VALUE", "UNFORMATTED_VALUE", "FORMULA"
    ] = "FORMATTED_VALUE",
    user_google_email: str = "",
) -> str:
    """Read cell values from a range. value_render_option: FORMATTED_VALUE (default), UNFORMATTED_VALUE, or FORMULA."""
    logger.info(
        f"[read_sheet_values] Invoked. Email: '{user_google_email}', Spreadsheet: {file_id}, Range: {range_name}, ValueRenderOption: {value_render_option}"
    )

    result = await asyncio.to_thread(
        service.spreadsheets()
        .values()
        .get(
            spreadsheetId=file_id,
            range=range_name,
            valueRenderOption=value_render_option,
        )
        .execute
    )

    values = result.get("values", [])
    if not values:
        return f"No data found in range '{range_name}'."

    detailed_errors_section = ""
    if _values_contain_sheets_errors(values):
        resolved_range = result.get("range", range_name)
        detailed_range = _a1_range_for_values(resolved_range, values) or resolved_range
        try:
            errors = await _fetch_detailed_sheet_errors(
                service, file_id, detailed_range
            )
            detailed_errors_section = _format_sheet_error_section(
                errors=errors, range_label=detailed_range
            )
        except Exception as exc:
            logger.warning(
                "[read_sheet_values] Failed fetching detailed error messages for range '%s': %s",
                detailed_range,
                exc,
            )

    # Format as TSV-like output, stripping trailing empty cells
    formatted_rows = []
    for row in values:
        # Strip trailing empty cells
        stripped = list(row)
        while stripped and stripped[-1] == "":
            stripped.pop()
        formatted_rows.append("\t".join(str(cell) for cell in stripped))

    text_output = (
        "\n".join(formatted_rows[:50])
        + (f"\n... and {len(values) - 50} more rows" if len(values) > 50 else "")
    )

    logger.info(f"Successfully read {len(values)} rows for {user_google_email}.")
    return text_output + detailed_errors_section


@server.tool()
@handle_http_errors("modify_sheet_values", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def modify_sheet_values(
    service,
    file_id: str,
    range_name: str,
    values: Optional[Union[str, List[List[str]]]] = None,
    value_input_option: str = "USER_ENTERED",
    clear_values: bool = False,
    user_google_email: str = "",
) -> str:
    """Write, update, or clear values in a range. Set clear_values=True to clear. value_input_option: USER_ENTERED (default) or RAW."""
    operation = "clear" if clear_values else "write"
    logger.info(
        f"[modify_sheet_values] Invoked. Operation: {operation}, Email: '{user_google_email}', Spreadsheet: {file_id}, Range: {range_name}"
    )

    # Parse values if it's a JSON string (MCP passes parameters as JSON strings)
    if values is not None and isinstance(values, str):
        try:
            parsed_values = json.loads(values)
            if not isinstance(parsed_values, list):
                raise ValueError(
                    f"Values must be a list, got {type(parsed_values).__name__}"
                )
            # Validate it's a list of lists
            for i, row in enumerate(parsed_values):
                if not isinstance(row, list):
                    raise ValueError(
                        f"Row {i} must be a list, got {type(row).__name__}"
                    )
            values = parsed_values
            logger.info(
                f"[modify_sheet_values] Parsed JSON string to Python list with {len(values)} rows"
            )
        except json.JSONDecodeError as e:
            raise UserInputError(f"Invalid JSON format for values: {e}")
        except ValueError as e:
            raise UserInputError(f"Invalid values structure: {e}")

    if not clear_values and not values:
        raise UserInputError(
            "Either 'values' must be provided or 'clear_values' must be True."
        )

    if clear_values:
        result = await asyncio.to_thread(
            service.spreadsheets()
            .values()
            .clear(spreadsheetId=file_id, range=range_name)
            .execute
        )

        cleared_range = result.get("clearedRange", range_name)
        text_output = f"Cleared {cleared_range}."
        logger.info(
            f"Successfully cleared range '{cleared_range}' for {user_google_email}."
        )
    else:
        body = {"values": values}

        result = await asyncio.to_thread(
            service.spreadsheets()
            .values()
            .update(
                spreadsheetId=file_id,
                range=range_name,
                valueInputOption=value_input_option,
                # NOTE: This increases response payload/shape by including `updatedData`, but lets
                # us detect Sheets error tokens (e.g. "#VALUE!", "#REF!") without an extra read.
                includeValuesInResponse=True,
                responseValueRenderOption="FORMATTED_VALUE",
                body=body,
            )
            .execute
        )

        updated_cells = result.get("updatedCells", 0)
        updated_rows = result.get("updatedRows", 0)
        updated_columns = result.get("updatedColumns", 0)

        detailed_errors_section = ""
        updated_data = result.get("updatedData") or {}
        updated_values = updated_data.get("values", []) or []
        if updated_values and _values_contain_sheets_errors(updated_values):
            updated_range = result.get("updatedRange", range_name)
            detailed_range = (
                _a1_range_for_values(updated_range, updated_values) or updated_range
            )
            try:
                errors = await _fetch_detailed_sheet_errors(
                    service, file_id, detailed_range
                )
                detailed_errors_section = _format_sheet_error_section(
                    errors=errors, range_label=detailed_range
                )
            except Exception as exc:
                logger.warning(
                    "[modify_sheet_values] Failed fetching detailed error messages for range '%s': %s",
                    detailed_range,
                    exc,
                )

        text_output = f"Updated {updated_cells} cells in {range_name}."
        text_output += detailed_errors_section
        logger.info(
            f"Successfully updated {updated_cells} cells for {user_google_email}."
        )

    return text_output


@server.tool()
@handle_http_errors("format_sheet_range", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def format_sheet_range(
    service,
    file_id: str,
    range_name: str,
    background_color: Optional[str] = None,
    text_color: Optional[str] = None,
    number_format_type: Optional[str] = None,
    number_format_pattern: Optional[str] = None,
    bold: Optional[bool] = None,
    italic: Optional[bool] = None,
    underline: Optional[bool] = None,
    strikethrough: Optional[bool] = None,
    font_size: Optional[int] = None,
    font_family: Optional[str] = None,
    horizontal_alignment: Optional[str] = None,
    vertical_alignment: Optional[str] = None,
    wrap_strategy: Optional[str] = None,
    user_google_email: str = "",
) -> str:
    """Apply formatting to a range. Colors as #RRGGBB. number_format_type: NUMBER, CURRENCY, DATE, TIME, DATE_TIME, PERCENT, TEXT, SCIENTIFIC. horizontal_alignment: LEFT, CENTER, RIGHT. wrap_strategy: OVERFLOW_CELL, CLIP, WRAP."""
    logger.info(
        "[format_sheet_range] Invoked. Email: '%s', Spreadsheet: %s, Range: %s",
        user_google_email,
        file_id,
        range_name,
    )

    if not any([
        background_color, text_color, number_format_type,
        bold is not None, italic is not None, underline is not None,
        strikethrough is not None, font_size is not None, font_family,
        horizontal_alignment, vertical_alignment, wrap_strategy,
    ]):
        raise UserInputError(
            "Provide at least one formatting option (color, number format, text style, alignment, or wrap strategy)."
        )

    bg_color_parsed = _parse_hex_color(background_color)
    text_color_parsed = _parse_hex_color(text_color)

    number_format = None
    if number_format_type:
        allowed_number_formats = {
            "NUMBER",
            "NUMBER_WITH_GROUPING",
            "CURRENCY",
            "PERCENT",
            "SCIENTIFIC",
            "DATE",
            "TIME",
            "DATE_TIME",
            "TEXT",
        }
        normalized_type = number_format_type.upper()
        if normalized_type not in allowed_number_formats:
            raise UserInputError(
                f"number_format_type must be one of {sorted(allowed_number_formats)}."
            )
        number_format = {"type": normalized_type}
        if number_format_pattern:
            number_format["pattern"] = number_format_pattern

    metadata = await asyncio.to_thread(
        service.spreadsheets()
        .get(
            spreadsheetId=file_id,
            fields="sheets(properties(sheetId,title))",
        )
        .execute
    )
    sheets = metadata.get("sheets", [])
    grid_range = _parse_a1_range(range_name, sheets)

    user_entered_format = {}
    fields = []
    if bg_color_parsed:
        user_entered_format["backgroundColor"] = bg_color_parsed
        fields.append("userEnteredFormat.backgroundColor")

    text_format = {}
    if text_color_parsed:
        text_format["foregroundColor"] = text_color_parsed
        fields.append("userEnteredFormat.textFormat.foregroundColor")
    if bold is not None:
        text_format["bold"] = bold
        fields.append("userEnteredFormat.textFormat.bold")
    if italic is not None:
        text_format["italic"] = italic
        fields.append("userEnteredFormat.textFormat.italic")
    if underline is not None:
        text_format["underline"] = underline
        fields.append("userEnteredFormat.textFormat.underline")
    if strikethrough is not None:
        text_format["strikethrough"] = strikethrough
        fields.append("userEnteredFormat.textFormat.strikethrough")
    if font_size is not None:
        text_format["fontSize"] = font_size
        fields.append("userEnteredFormat.textFormat.fontSize")
    if font_family is not None:
        text_format["fontFamily"] = font_family
        fields.append("userEnteredFormat.textFormat.fontFamily")
    if text_format:
        user_entered_format["textFormat"] = text_format

    if number_format:
        user_entered_format["numberFormat"] = number_format
        fields.append("userEnteredFormat.numberFormat")

    if horizontal_alignment:
        user_entered_format["horizontalAlignment"] = horizontal_alignment
        fields.append("userEnteredFormat.horizontalAlignment")
    if vertical_alignment:
        user_entered_format["verticalAlignment"] = vertical_alignment
        fields.append("userEnteredFormat.verticalAlignment")
    if wrap_strategy:
        user_entered_format["wrapStrategy"] = wrap_strategy
        fields.append("userEnteredFormat.wrapStrategy")

    if not user_entered_format:
        raise UserInputError(
            "No formatting applied. Verify provided colors, number format, or text style options."
        )

    request_body = {
        "requests": [
            {
                "repeatCell": {
                    "range": grid_range,
                    "cell": {"userEnteredFormat": user_entered_format},
                    "fields": ",".join(fields),
                }
            }
        ]
    }

    await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(spreadsheetId=file_id, body=request_body)
        .execute
    )

    applied_parts = []
    if bg_color_parsed:
        applied_parts.append(f"background {background_color}")
    if text_color_parsed:
        applied_parts.append(f"text color {text_color}")
    if number_format:
        nf_desc = number_format["type"]
        if number_format_pattern:
            nf_desc += f" (pattern: {number_format_pattern})"
        applied_parts.append(f"format {nf_desc}")
    if bold is not None:
        applied_parts.append(f"bold={bold}")
    if italic is not None:
        applied_parts.append(f"italic={italic}")
    if underline is not None:
        applied_parts.append(f"underline={underline}")
    if strikethrough is not None:
        applied_parts.append(f"strikethrough={strikethrough}")
    if font_size is not None:
        applied_parts.append(f"fontSize={font_size}")
    if font_family is not None:
        applied_parts.append(f"fontFamily={font_family}")
    if horizontal_alignment:
        applied_parts.append(f"hAlign={horizontal_alignment}")
    if vertical_alignment:
        applied_parts.append(f"vAlign={vertical_alignment}")
    if wrap_strategy:
        applied_parts.append(f"wrap={wrap_strategy}")

    summary = ", ".join(applied_parts)
    return f"Applied formatting to {range_name}: {summary}."


@server.tool()
@handle_http_errors("add_conditional_formatting", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def add_conditional_formatting(
    service,
    file_id: str,
    range_name: str,
    condition_type: str,
    condition_values: Optional[Union[str, List[Union[str, int, float]]]] = None,
    background_color: Optional[str] = None,
    text_color: Optional[str] = None,
    rule_index: Optional[int] = None,
    gradient_points: Optional[Union[str, List[dict]]] = None,
    user_google_email: str = "",
) -> str:
    """Add a conditional formatting rule. If gradient_points provided, creates a gradient rule (ignoring boolean params). condition_type: NUMBER_GREATER, TEXT_CONTAINS, CUSTOM_FORMULA, etc."""
    logger.info(
        "[add_conditional_formatting] Invoked. Email: '%s', Spreadsheet: %s, Range: %s, Type: %s, Values: %s",
        user_google_email,
        file_id,
        range_name,
        condition_type,
        condition_values,
    )

    if rule_index is not None and (not isinstance(rule_index, int) or rule_index < 0):
        raise UserInputError("rule_index must be a non-negative integer when provided.")

    condition_values_list = _parse_condition_values(condition_values)
    gradient_points_list = _parse_gradient_points(gradient_points)

    sheets, sheet_titles = await _fetch_sheets_with_rules(service, file_id)
    grid_range = _parse_a1_range(range_name, sheets)

    target_sheet = None
    for sheet in sheets:
        if sheet.get("properties", {}).get("sheetId") == grid_range.get("sheetId"):
            target_sheet = sheet
            break
    if target_sheet is None:
        raise UserInputError(
            "Target sheet not found while adding conditional formatting."
        )

    current_rules = target_sheet.get("conditionalFormats", []) or []

    insert_at = rule_index if rule_index is not None else len(current_rules)
    if insert_at > len(current_rules):
        raise UserInputError(
            f"rule_index {insert_at} is out of range for sheet '{target_sheet.get('properties', {}).get('title', 'Unknown')}' "
            f"(current count: {len(current_rules)})."
        )

    if gradient_points_list:
        new_rule = _build_gradient_rule([grid_range], gradient_points_list)
        rule_desc = "gradient"
        values_desc = ""
        applied_parts = [f"gradient points {len(gradient_points_list)}"]
    else:
        rule, cond_type_normalized = _build_boolean_rule(
            [grid_range],
            condition_type,
            condition_values_list,
            background_color,
            text_color,
        )
        new_rule = rule
        rule_desc = cond_type_normalized
        values_desc = ""
        if condition_values_list:
            values_desc = f" with values {condition_values_list}"
        applied_parts = []
        if background_color:
            applied_parts.append(f"background {background_color}")
        if text_color:
            applied_parts.append(f"text {text_color}")

    new_rules_state = copy.deepcopy(current_rules)
    new_rules_state.insert(insert_at, new_rule)

    add_rule_request = {"rule": new_rule}
    if rule_index is not None:
        add_rule_request["index"] = rule_index

    request_body = {"requests": [{"addConditionalFormatRule": add_rule_request}]}

    await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(spreadsheetId=file_id, body=request_body)
        .execute
    )

    format_desc = ", ".join(applied_parts) if applied_parts else "format applied"

    sheet_title = target_sheet.get("properties", {}).get("title", "Unknown")
    state_text = _format_conditional_rules_section(
        sheet_title, new_rules_state, sheet_titles, indent=""
    )

    return "\n".join(
        [
            f"Added conditional format on {range_name}: {rule_desc}{values_desc}; {format_desc}.",
            state_text,
        ]
    )


@server.tool()
@handle_http_errors("update_conditional_formatting", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def update_conditional_formatting(
    service,
    file_id: str,
    rule_index: int,
    range_name: Optional[str] = None,
    condition_type: Optional[str] = None,
    condition_values: Optional[Union[str, List[Union[str, int, float]]]] = None,
    background_color: Optional[str] = None,
    text_color: Optional[str] = None,
    sheet_name: Optional[str] = None,
    gradient_points: Optional[Union[str, List[dict]]] = None,
    user_google_email: str = "",
) -> str:
    """Update an existing conditional formatting rule by index. Omitted params preserve existing values."""
    logger.info(
        "[update_conditional_formatting] Invoked. Email: '%s', Spreadsheet: %s, Range: %s, Rule Index: %s",
        user_google_email,
        file_id,
        range_name,
        rule_index,
    )

    if not isinstance(rule_index, int) or rule_index < 0:
        raise UserInputError("rule_index must be a non-negative integer.")

    condition_values_list = _parse_condition_values(condition_values)
    gradient_points_list = _parse_gradient_points(gradient_points)

    sheets, sheet_titles = await _fetch_sheets_with_rules(service, file_id)

    target_sheet = None
    grid_range = None
    if range_name:
        grid_range = _parse_a1_range(range_name, sheets)
        for sheet in sheets:
            if sheet.get("properties", {}).get("sheetId") == grid_range.get("sheetId"):
                target_sheet = sheet
                break
    else:
        target_sheet = _select_sheet(sheets, sheet_name)

    if target_sheet is None:
        raise UserInputError(
            "Target sheet not found while updating conditional formatting."
        )

    sheet_props = target_sheet.get("properties", {})
    sheet_id = sheet_props.get("sheetId")
    sheet_title = sheet_props.get("title", f"Sheet {sheet_id}")

    rules = target_sheet.get("conditionalFormats", []) or []
    if rule_index >= len(rules):
        raise UserInputError(
            f"rule_index {rule_index} is out of range for sheet '{sheet_title}' (current count: {len(rules)})."
        )

    existing_rule = rules[rule_index]
    ranges_to_use = existing_rule.get("ranges", [])
    if range_name:
        ranges_to_use = [grid_range]
    if not ranges_to_use:
        ranges_to_use = [{"sheetId": sheet_id}]

    new_rule = None
    rule_desc = ""
    values_desc = ""
    format_desc = ""

    if gradient_points_list is not None:
        new_rule = _build_gradient_rule(ranges_to_use, gradient_points_list)
        rule_desc = "gradient"
        format_desc = f"gradient points {len(gradient_points_list)}"
    elif "gradientRule" in existing_rule:
        if any([background_color, text_color, condition_type, condition_values_list]):
            raise UserInputError(
                "Existing rule is a gradient rule. Provide gradient_points to update it, or omit formatting/condition parameters to keep it unchanged."
            )
        new_rule = {
            "ranges": ranges_to_use,
            "gradientRule": existing_rule.get("gradientRule", {}),
        }
        rule_desc = "gradient"
        format_desc = "gradient (unchanged)"
    else:
        existing_boolean = existing_rule.get("booleanRule", {})
        existing_condition = existing_boolean.get("condition", {})
        existing_format = copy.deepcopy(existing_boolean.get("format", {}))

        cond_type = (condition_type or existing_condition.get("type", "")).upper()
        if not cond_type:
            raise UserInputError("condition_type is required for boolean rules.")
        if cond_type not in CONDITION_TYPES:
            raise UserInputError(
                f"condition_type must be one of {sorted(CONDITION_TYPES)}."
            )

        if condition_values_list is not None:
            cond_values = [
                {"userEnteredValue": str(val)} for val in condition_values_list
            ]
        else:
            cond_values = existing_condition.get("values")

        new_format = copy.deepcopy(existing_format) if existing_format else {}
        if background_color is not None:
            bg_color_parsed = _parse_hex_color(background_color)
            if bg_color_parsed:
                new_format["backgroundColor"] = bg_color_parsed
            elif "backgroundColor" in new_format:
                del new_format["backgroundColor"]
        if text_color is not None:
            text_color_parsed = _parse_hex_color(text_color)
            text_format = copy.deepcopy(new_format.get("textFormat", {}))
            if text_color_parsed:
                text_format["foregroundColor"] = text_color_parsed
            elif "foregroundColor" in text_format:
                del text_format["foregroundColor"]
            if text_format:
                new_format["textFormat"] = text_format
            elif "textFormat" in new_format:
                del new_format["textFormat"]

        if not new_format:
            raise UserInputError("At least one format option must remain on the rule.")

        new_rule = {
            "ranges": ranges_to_use,
            "booleanRule": {
                "condition": {"type": cond_type},
                "format": new_format,
            },
        }
        if cond_values:
            new_rule["booleanRule"]["condition"]["values"] = cond_values

        rule_desc = cond_type
        if condition_values_list:
            values_desc = f" with values {condition_values_list}"
        format_parts = []
        if "backgroundColor" in new_format:
            format_parts.append("background updated")
        if "textFormat" in new_format and new_format["textFormat"].get(
            "foregroundColor"
        ):
            format_parts.append("text color updated")
        format_desc = ", ".join(format_parts) if format_parts else "format preserved"

    new_rules_state = copy.deepcopy(rules)
    new_rules_state[rule_index] = new_rule

    request_body = {
        "requests": [
            {
                "updateConditionalFormatRule": {
                    "index": rule_index,
                    "sheetId": sheet_id,
                    "rule": new_rule,
                }
            }
        ]
    }

    await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(spreadsheetId=file_id, body=request_body)
        .execute
    )

    state_text = _format_conditional_rules_section(
        sheet_title, new_rules_state, sheet_titles, indent=""
    )

    return "\n".join(
        [
            f"Updated conditional format [{rule_index}] on '{sheet_title}': {rule_desc}{values_desc}; {format_desc}.",
            state_text,
        ]
    )


@server.tool()
@handle_http_errors("delete_conditional_formatting", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def delete_conditional_formatting(
    service,
    file_id: str,
    rule_index: int,
    sheet_name: Optional[str] = None,
    user_google_email: str = "",
) -> str:
    """Delete a conditional formatting rule by index."""
    logger.info(
        "[delete_conditional_formatting] Invoked. Email: '%s', Spreadsheet: %s, Sheet: %s, Rule Index: %s",
        user_google_email,
        file_id,
        sheet_name,
        rule_index,
    )

    if not isinstance(rule_index, int) or rule_index < 0:
        raise UserInputError("rule_index must be a non-negative integer.")

    sheets, sheet_titles = await _fetch_sheets_with_rules(service, file_id)
    target_sheet = _select_sheet(sheets, sheet_name)

    sheet_props = target_sheet.get("properties", {})
    sheet_id = sheet_props.get("sheetId")
    target_sheet_name = sheet_props.get("title", f"Sheet {sheet_id}")
    rules = target_sheet.get("conditionalFormats", []) or []
    if rule_index >= len(rules):
        raise UserInputError(
            f"rule_index {rule_index} is out of range for sheet '{target_sheet_name}' (current count: {len(rules)})."
        )

    new_rules_state = copy.deepcopy(rules)
    del new_rules_state[rule_index]

    request_body = {
        "requests": [
            {
                "deleteConditionalFormatRule": {
                    "index": rule_index,
                    "sheetId": sheet_id,
                }
            }
        ]
    }

    await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(spreadsheetId=file_id, body=request_body)
        .execute
    )

    state_text = _format_conditional_rules_section(
        target_sheet_name, new_rules_state, sheet_titles, indent=""
    )

    return "\n".join(
        [
            f"Deleted conditional format [{rule_index}] on '{target_sheet_name}'.",
            state_text,
        ]
    )


@server.tool()
@handle_http_errors("create_spreadsheet", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def create_spreadsheet(
    service,
    title: str,
    sheet_names: Optional[List[str]] = None,
    user_google_email: str = "",
) -> str:
    """Create a new spreadsheet with optional sheet names."""
    logger.info(
        f"[create_spreadsheet] Invoked. Email: '{user_google_email}', Title: {title}"
    )

    spreadsheet_body = {"properties": {"title": title}}

    if sheet_names:
        spreadsheet_body["sheets"] = [
            {"properties": {"title": sheet_name}} for sheet_name in sheet_names
        ]

    spreadsheet = await asyncio.to_thread(
        service.spreadsheets()
        .create(
            body=spreadsheet_body,
            fields="spreadsheetId,spreadsheetUrl,properties(title,locale)",
        )
        .execute
    )

    properties = spreadsheet.get("properties", {})
    file_id = spreadsheet.get("spreadsheetId")
    spreadsheet_url = spreadsheet.get("spreadsheetUrl")
    locale = properties.get("locale", "Unknown")

    text_output = (
        f"Created spreadsheet '{title}'. "
        f"ID: {file_id} | URL: {spreadsheet_url} | Locale: {locale}"
    )

    logger.info(
        f"Successfully created spreadsheet for {user_google_email}. ID: {file_id}"
    )
    return text_output


@server.tool()
@handle_http_errors("create_sheet", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def create_sheet(
    service,
    file_id: str,
    sheet_name: str,
    user_google_email: str = "",
) -> str:
    """Create a new sheet tab within a spreadsheet."""
    logger.info(
        f"[create_sheet] Invoked. Email: '{user_google_email}', Spreadsheet: {file_id}, Sheet: {sheet_name}"
    )

    request_body = {"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]}

    response = await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(spreadsheetId=file_id, body=request_body)
        .execute
    )

    sheet_id = response["replies"][0]["addSheet"]["properties"]["sheetId"]

    text_output = f"Created sheet '{sheet_name}' (ID: {sheet_id})."

    logger.info(
        f"Successfully created sheet for {user_google_email}. Sheet ID: {sheet_id}"
    )
    return text_output


@server.tool()
@handle_http_errors("update_borders", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def update_borders(
    service,
    file_id: str,
    range_name: str,
    border_style: str = "SOLID",
    border_color: str = "#000000",
    borders: str = "all",
    user_google_email: str = "",
) -> str:
    """Apply borders to a range. border_style: DOTTED, DASHED, SOLID, SOLID_MEDIUM, SOLID_THICK, DOUBLE. borders: all, outer, inner, or comma-separated (top,bottom,left,right,innerHorizontal,innerVertical)."""
    logger.info(
        "[update_borders] Invoked. Email: '%s', Spreadsheet: %s, Range: %s",
        user_google_email,
        file_id,
        range_name,
    )

    allowed_styles = {"DOTTED", "DASHED", "SOLID", "SOLID_MEDIUM", "SOLID_THICK", "DOUBLE"}
    if border_style.upper() not in allowed_styles:
        raise UserInputError(f"border_style must be one of {sorted(allowed_styles)}.")

    color_parsed = _parse_hex_color(border_color)
    if not color_parsed:
        raise UserInputError("border_color must be a valid hex color (e.g., '#000000').")

    border_spec = {"style": border_style.upper(), "color": color_parsed}

    if borders == "all":
        border_keys = {"top", "bottom", "left", "right", "innerHorizontal", "innerVertical"}
    elif borders == "outer":
        border_keys = {"top", "bottom", "left", "right"}
    elif borders == "inner":
        border_keys = {"innerHorizontal", "innerVertical"}
    else:
        border_keys = {b.strip() for b in borders.split(",")}
        valid_keys = {"top", "bottom", "left", "right", "innerHorizontal", "innerVertical"}
        invalid = border_keys - valid_keys
        if invalid:
            raise UserInputError(f"Invalid border positions: {invalid}. Valid: {sorted(valid_keys)}.")

    metadata = await asyncio.to_thread(
        service.spreadsheets()
        .get(
            spreadsheetId=file_id,
            fields="sheets(properties(sheetId,title))",
        )
        .execute
    )
    sheets = metadata.get("sheets", [])
    grid_range = _parse_a1_range(range_name, sheets)

    update_borders_req = {"range": grid_range}
    for key in border_keys:
        update_borders_req[key] = border_spec

    request_body = {"requests": [{"updateBorders": update_borders_req}]}

    await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(spreadsheetId=file_id, body=request_body)
        .execute
    )

    return f"Applied {border_style} borders ({borders}) to {range_name}."


@server.tool()
@handle_http_errors("merge_cells", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def merge_cells(
    service,
    file_id: str,
    range_name: str,
    merge_type: str = "MERGE_ALL",
    unmerge: bool = False,
    user_google_email: str = "",
) -> str:
    """Merge or unmerge cells. merge_type: MERGE_ALL, MERGE_COLUMNS, MERGE_ROWS. Set unmerge=True to unmerge."""
    logger.info(
        "[merge_cells] Invoked. Email: '%s', Spreadsheet: %s, Range: %s, Unmerge: %s",
        user_google_email,
        file_id,
        range_name,
        unmerge,
    )

    allowed_merge_types = {"MERGE_ALL", "MERGE_COLUMNS", "MERGE_ROWS"}
    if merge_type.upper() not in allowed_merge_types:
        raise UserInputError(f"merge_type must be one of {sorted(allowed_merge_types)}.")

    metadata = await asyncio.to_thread(
        service.spreadsheets()
        .get(
            spreadsheetId=file_id,
            fields="sheets(properties(sheetId,title))",
        )
        .execute
    )
    sheets = metadata.get("sheets", [])
    grid_range = _parse_a1_range(range_name, sheets)

    if unmerge:
        request = {"unmergeCells": {"range": grid_range}}
        action = "Unmerged"
    else:
        request = {"mergeCells": {"range": grid_range, "mergeType": merge_type.upper()}}
        action = f"Merged ({merge_type})"

    request_body = {"requests": [request]}

    await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(spreadsheetId=file_id, body=request_body)
        .execute
    )

    return f"{action} cells in {range_name}."


@server.tool()
@handle_http_errors("insert_dimension", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def insert_dimension(
    service,
    file_id: str,
    sheet_name: str,
    dimension: str,
    start_index: int,
    count: int = 1,
    inherit_from_before: bool = True,
    user_google_email: str = "",
) -> str:
    """Insert rows or columns. dimension: ROWS or COLUMNS."""
    logger.info(
        "[insert_dimension] Invoked. Email: '%s', Spreadsheet: %s, Sheet: %s, Dim: %s, Start: %d, Count: %d",
        user_google_email,
        file_id,
        sheet_name,
        dimension,
        start_index,
        count,
    )

    if dimension.upper() not in ("ROWS", "COLUMNS"):
        raise UserInputError("dimension must be 'ROWS' or 'COLUMNS'.")

    sheet_id = await _get_sheet_id_by_name(service, file_id, sheet_name)

    request_body = {
        "requests": [
            {
                "insertDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": dimension.upper(),
                        "startIndex": start_index,
                        "endIndex": start_index + count,
                    },
                    "inheritFromBefore": inherit_from_before,
                }
            }
        ]
    }

    await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(spreadsheetId=file_id, body=request_body)
        .execute
    )

    return f"Inserted {count} {dimension.lower()} at index {start_index} in '{sheet_name}'."


@server.tool()
@handle_http_errors("delete_dimension", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def delete_dimension(
    service,
    file_id: str,
    sheet_name: str,
    dimension: str,
    start_index: int,
    end_index: int,
    user_google_email: str = "",
) -> str:
    """Delete rows or columns. dimension: ROWS or COLUMNS. end_index is exclusive."""
    logger.info(
        "[delete_dimension] Invoked. Email: '%s', Spreadsheet: %s, Sheet: %s, Dim: %s, Start: %d, End: %d",
        user_google_email,
        file_id,
        sheet_name,
        dimension,
        start_index,
        end_index,
    )

    if dimension.upper() not in ("ROWS", "COLUMNS"):
        raise UserInputError("dimension must be 'ROWS' or 'COLUMNS'.")
    if start_index >= end_index:
        raise UserInputError("start_index must be less than end_index.")

    sheet_id = await _get_sheet_id_by_name(service, file_id, sheet_name)

    request_body = {
        "requests": [
            {
                "deleteDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": dimension.upper(),
                        "startIndex": start_index,
                        "endIndex": end_index,
                    }
                }
            }
        ]
    }

    await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(spreadsheetId=file_id, body=request_body)
        .execute
    )

    deleted_count = end_index - start_index
    return f"Deleted {deleted_count} {dimension.lower()} (index {start_index} to {end_index}) in '{sheet_name}'."


@server.tool()
@handle_http_errors("sort_range", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def sort_range(
    service,
    file_id: str,
    range_name: str,
    sort_specs: Union[str, List[dict]],
    user_google_email: str = "",
) -> str:
    """Sort data in a range. sort_specs: list of {column_index: int, order: ASCENDING|DESCENDING}."""
    logger.info(
        "[sort_range] Invoked. Email: '%s', Spreadsheet: %s, Range: %s",
        user_google_email,
        file_id,
        range_name,
    )

    if isinstance(sort_specs, str):
        try:
            sort_specs = json.loads(sort_specs)
        except json.JSONDecodeError as e:
            raise UserInputError(f"sort_specs must be valid JSON: {e}")

    if not isinstance(sort_specs, list) or not sort_specs:
        raise UserInputError("sort_specs must be a non-empty list of sort specifications.")

    metadata = await asyncio.to_thread(
        service.spreadsheets()
        .get(
            spreadsheetId=file_id,
            fields="sheets(properties(sheetId,title))",
        )
        .execute
    )
    sheets = metadata.get("sheets", [])
    grid_range = _parse_a1_range(range_name, sheets)

    api_sort_specs = []
    for spec in sort_specs:
        if not isinstance(spec, dict):
            raise UserInputError("Each sort_spec must be a dict with 'column_index' and 'order'.")
        col_idx = spec.get("column_index")
        order = spec.get("order", "ASCENDING").upper()
        if col_idx is None:
            raise UserInputError("Each sort_spec must have 'column_index'.")
        if order not in ("ASCENDING", "DESCENDING"):
            raise UserInputError("sort order must be 'ASCENDING' or 'DESCENDING'.")
        api_sort_specs.append({
            "dimensionIndex": col_idx,
            "sortOrder": order,
        })

    request_body = {
        "requests": [
            {
                "sortRange": {
                    "range": grid_range,
                    "sortSpecs": api_sort_specs,
                }
            }
        ]
    }

    await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(spreadsheetId=file_id, body=request_body)
        .execute
    )

    specs_desc = ", ".join(
        f"col {s.get('column_index')} {s.get('order', 'ASCENDING')}" for s in sort_specs
    )
    return f"Sorted {range_name} by: {specs_desc}."


@server.tool()
@handle_http_errors("set_data_validation", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def set_data_validation(
    service,
    file_id: str,
    range_name: str,
    validation_type: str = "ONE_OF_LIST",
    values: Optional[Union[str, List]] = None,
    strict: bool = True,
    show_dropdown: bool = True,
    clear: bool = False,
    user_google_email: str = "",
) -> str:
    """Set or clear data validation. validation_type: ONE_OF_LIST, NUMBER_BETWEEN, CUSTOM_FORMULA, etc. Set clear=True to remove."""
    logger.info(
        "[set_data_validation] Invoked. Email: '%s', Spreadsheet: %s, Range: %s, Clear: %s",
        user_google_email,
        file_id,
        range_name,
        clear,
    )

    metadata = await asyncio.to_thread(
        service.spreadsheets()
        .get(
            spreadsheetId=file_id,
            fields="sheets(properties(sheetId,title))",
        )
        .execute
    )
    sheets = metadata.get("sheets", [])
    grid_range = _parse_a1_range(range_name, sheets)

    if clear:
        request_body = {
            "requests": [
                {
                    "setDataValidation": {
                        "range": grid_range,
                    }
                }
            ]
        }

        await asyncio.to_thread(
            service.spreadsheets()
            .batchUpdate(spreadsheetId=file_id, body=request_body)
            .execute
        )

        return f"Cleared data validation from {range_name}."

    if isinstance(values, str):
        try:
            values = json.loads(values)
        except json.JSONDecodeError as e:
            raise UserInputError(f"values must be valid JSON: {e}")

    if values is not None and not isinstance(values, list):
        values = [values]

    condition_values = []
    if values:
        condition_values = [{"userEnteredValue": str(v)} for v in values]

    rule = {
        "condition": {
            "type": validation_type.upper(),
        },
        "strict": strict,
        "showCustomUi": show_dropdown,
    }
    if condition_values:
        rule["condition"]["values"] = condition_values

    request_body = {
        "requests": [
            {
                "setDataValidation": {
                    "range": grid_range,
                    "rule": rule,
                }
            }
        ]
    }

    await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(spreadsheetId=file_id, body=request_body)
        .execute
    )

    values_desc = f" with values {values}" if values else ""
    return f"Set {validation_type} validation{values_desc} on {range_name}."


@server.tool()
@handle_http_errors("delete_sheet", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def delete_sheet(
    service,
    file_id: str,
    sheet_name: str,
    user_google_email: str = "",
) -> str:
    """Delete a sheet tab from a spreadsheet."""
    logger.info(
        "[delete_sheet] Invoked. Email: '%s', Spreadsheet: %s, Sheet: %s",
        user_google_email,
        file_id,
        sheet_name,
    )

    sheet_id = await _get_sheet_id_by_name(service, file_id, sheet_name)

    request_body = {
        "requests": [{"deleteSheet": {"sheetId": sheet_id}}]
    }

    await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(spreadsheetId=file_id, body=request_body)
        .execute
    )

    return f"Deleted sheet '{sheet_name}' (ID: {sheet_id})."


@server.tool()
@handle_http_errors("batch_read_values", is_read_only=True, service_type="sheets")
@require_google_service("sheets", "sheets_read")
async def batch_read_values(
    service,
    file_id: str,
    ranges: Union[str, List[str]],
    value_render_option: str = "FORMATTED_VALUE",
    user_google_email: str = "",
) -> str:
    """Read values from multiple ranges in one request. value_render_option: FORMATTED_VALUE (default), UNFORMATTED_VALUE, or FORMULA."""
    logger.info(
        "[batch_read_values] Invoked. Email: '%s', Spreadsheet: %s, Ranges: %s",
        user_google_email,
        file_id,
        ranges,
    )

    if isinstance(ranges, str):
        try:
            ranges = json.loads(ranges)
        except json.JSONDecodeError as e:
            raise UserInputError(f"ranges must be a list or valid JSON list: {e}")

    if not isinstance(ranges, list) or not ranges:
        raise UserInputError("ranges must be a non-empty list of A1-notation ranges.")

    result = await asyncio.to_thread(
        service.spreadsheets()
        .values()
        .batchGet(
            spreadsheetId=file_id,
            ranges=ranges,
            valueRenderOption=value_render_option,
        )
        .execute
    )

    value_ranges = result.get("valueRanges", [])
    output_parts = []
    total_rows = 0

    for vr in value_ranges:
        range_label = vr.get("range", "Unknown")
        values = vr.get("values", [])
        total_rows += len(values)

        if not values:
            output_parts.append(f"\n--- {range_label} ---\nNo data found.")
            continue

        formatted_rows = []
        for row in values:
            stripped = list(row)
            while stripped and stripped[-1] == "":
                stripped.pop()
            formatted_rows.append("\t".join(str(cell) for cell in stripped))

        output_parts.append(
            f"\n--- {range_label} ({len(values)} rows) ---\n"
            + "\n".join(formatted_rows[:50])
            + (f"\n... and {len(values) - 50} more rows" if len(values) > 50 else "")
        )

    logger.info(f"Successfully batch-read {len(value_ranges)} ranges for {user_google_email}.")
    return "".join(output_parts).lstrip("\n")


@server.tool()
@handle_http_errors("auto_resize_dimensions", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def auto_resize_dimensions(
    service,
    file_id: str,
    sheet_name: str,
    dimension: str = "COLUMNS",
    start_index: int = 0,
    end_index: Optional[int] = None,
    user_google_email: str = "",
) -> str:
    """Auto-resize rows or columns to fit content. dimension: ROWS or COLUMNS."""
    logger.info(
        "[auto_resize_dimensions] Invoked. Email: '%s', Spreadsheet: %s, Sheet: %s, Dim: %s",
        user_google_email,
        file_id,
        sheet_name,
        dimension,
    )

    if dimension.upper() not in ("ROWS", "COLUMNS"):
        raise UserInputError("dimension must be 'ROWS' or 'COLUMNS'.")

    sheet_id = await _get_sheet_id_by_name(service, file_id, sheet_name)

    dimensions_range = {
        "sheetId": sheet_id,
        "dimension": dimension.upper(),
        "startIndex": start_index,
    }
    if end_index is not None:
        dimensions_range["endIndex"] = end_index

    request_body = {
        "requests": [
            {
                "autoResizeDimensions": {
                    "dimensions": dimensions_range,
                }
            }
        ]
    }

    await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(spreadsheetId=file_id, body=request_body)
        .execute
    )

    range_desc = f"from index {start_index}" + (f" to {end_index}" if end_index else " to end")
    return f"Auto-resized {dimension.lower()} {range_desc} in '{sheet_name}'."


@server.tool()
@handle_http_errors("freeze_dimensions", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def freeze_dimensions(
    service,
    file_id: str,
    sheet_name: str,
    frozen_rows: Optional[int] = None,
    frozen_columns: Optional[int] = None,
    user_google_email: str = "",
) -> str:
    """Freeze rows and/or columns. Set to 0 to unfreeze."""
    logger.info(
        "[freeze_dimensions] Invoked. Email: '%s', Spreadsheet: %s, Sheet: %s, Rows: %s, Cols: %s",
        user_google_email,
        file_id,
        sheet_name,
        frozen_rows,
        frozen_columns,
    )

    if frozen_rows is None and frozen_columns is None:
        raise UserInputError("Provide at least one of frozen_rows or frozen_columns.")

    sheet_id = await _get_sheet_id_by_name(service, file_id, sheet_name)

    grid_properties = {}
    field_parts = []
    if frozen_rows is not None:
        grid_properties["frozenRowCount"] = frozen_rows
        field_parts.append("gridProperties.frozenRowCount")
    if frozen_columns is not None:
        grid_properties["frozenColumnCount"] = frozen_columns
        field_parts.append("gridProperties.frozenColumnCount")

    request_body = {
        "requests": [
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": sheet_id,
                        "gridProperties": grid_properties,
                    },
                    "fields": ",".join(field_parts),
                }
            }
        ]
    }

    await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(spreadsheetId=file_id, body=request_body)
        .execute
    )

    parts = []
    if frozen_rows is not None:
        parts.append(f"{frozen_rows} rows")
    if frozen_columns is not None:
        parts.append(f"{frozen_columns} columns")

    return f"Froze {' and '.join(parts)} in '{sheet_name}'."


@server.tool()
@handle_http_errors("duplicate_sheet", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def duplicate_sheet(
    service,
    file_id: str,
    sheet_name: str,
    new_name: str,
    insert_index: Optional[int] = None,
    user_google_email: str = "",
) -> str:
    """Duplicate a sheet tab within a spreadsheet."""
    logger.info(
        "[duplicate_sheet] Invoked. Email: '%s', Spreadsheet: %s, Source: %s, NewName: %s",
        user_google_email,
        file_id,
        sheet_name,
        new_name,
    )

    sheet_id = await _get_sheet_id_by_name(service, file_id, sheet_name)

    dup_request = {
        "sourceSheetId": sheet_id,
        "newSheetName": new_name,
    }
    if insert_index is not None:
        dup_request["insertSheetIndex"] = insert_index

    request_body = {"requests": [{"duplicateSheet": dup_request}]}

    response = await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(spreadsheetId=file_id, body=request_body)
        .execute
    )

    new_sheet_id = response["replies"][0]["duplicateSheet"]["properties"]["sheetId"]

    return f"Duplicated '{sheet_name}' as '{new_name}' (ID: {new_sheet_id})."


@server.tool()
@handle_http_errors("update_sheet_properties", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def update_sheet_properties(
    service,
    file_id: str,
    sheet_name: str,
    new_name: Optional[str] = None,
    tab_color: Optional[str] = None,
    hidden: Optional[bool] = None,
    right_to_left: Optional[bool] = None,
    index: Optional[int] = None,
    user_google_email: str = "",
) -> str:
    """Update sheet tab properties (rename, color, visibility, order)."""
    logger.info(
        "[update_sheet_properties] Invoked. Email: '%s', Spreadsheet: %s, Sheet: %s",
        user_google_email,
        file_id,
        sheet_name,
    )

    if not any([new_name, tab_color, hidden is not None, right_to_left is not None, index is not None]):
        raise UserInputError(
            "Provide at least one property to update (new_name, tab_color, hidden, right_to_left, index)."
        )

    sheet_id = await _get_sheet_id_by_name(service, file_id, sheet_name)

    properties = {"sheetId": sheet_id}
    field_parts = []

    if new_name is not None:
        properties["title"] = new_name
        field_parts.append("title")
    if tab_color is not None:
        color_parsed = _parse_hex_color(tab_color)
        if not color_parsed:
            raise UserInputError("tab_color must be a valid hex color.")
        properties["tabColorStyle"] = {"rgbColor": color_parsed}
        field_parts.append("tabColorStyle")
    if hidden is not None:
        properties["hidden"] = hidden
        field_parts.append("hidden")
    if right_to_left is not None:
        properties["rightToLeft"] = right_to_left
        field_parts.append("rightToLeft")
    if index is not None:
        properties["index"] = index
        field_parts.append("index")

    request_body = {
        "requests": [
            {
                "updateSheetProperties": {
                    "properties": properties,
                    "fields": ",".join(field_parts),
                }
            }
        ]
    }

    await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(spreadsheetId=file_id, body=request_body)
        .execute
    )

    changes = []
    if new_name:
        changes.append(f"renamed to '{new_name}'")
    if tab_color:
        changes.append(f"tab color {tab_color}")
    if hidden is not None:
        changes.append("hidden" if hidden else "visible")
    if right_to_left is not None:
        changes.append(f"rightToLeft={right_to_left}")
    if index is not None:
        changes.append(f"moved to index {index}")

    return f"Updated sheet '{sheet_name}': {', '.join(changes)}."


@server.tool()
@handle_http_errors("find_replace_sheet", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def find_replace_sheet(
    service,
    file_id: str,
    find: str,
    replacement: str,
    sheet_name: Optional[str] = None,
    match_case: bool = False,
    match_entire_cell: bool = False,
    search_by_regex: bool = False,
    user_google_email: str = "",
) -> str:
    """Find and replace text in a spreadsheet."""
    logger.info(
        "[find_replace_sheet] Invoked. Email: '%s', Spreadsheet: %s, Find: '%s', Replace: '%s'",
        user_google_email,
        file_id,
        find,
        replacement,
    )

    find_replace_req = {
        "find": find,
        "replacement": replacement,
        "matchCase": match_case,
        "matchEntireCell": match_entire_cell,
        "searchByRegex": search_by_regex,
    }

    if sheet_name is not None:
        sheet_id = await _get_sheet_id_by_name(service, file_id, sheet_name)
        find_replace_req["sheetId"] = sheet_id
    else:
        find_replace_req["allSheets"] = True

    request_body = {"requests": [{"findReplace": find_replace_req}]}

    response = await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(spreadsheetId=file_id, body=request_body)
        .execute
    )

    reply = response.get("replies", [{}])[0].get("findReplace", {})
    occurrences = reply.get("occurrencesChanged", 0)
    rows_changed = reply.get("rowsChanged", 0)
    sheets_changed = reply.get("sheetsChanged", 0)

    scope = f"sheet '{sheet_name}'" if sheet_name else "all sheets"
    return (
        f"Replaced '{find}' with '{replacement}' in {scope}: "
        f"{occurrences} occurrences in {rows_changed} rows across {sheets_changed} sheets."
    )


@server.tool()
@handle_http_errors("manage_named_ranges", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def manage_named_ranges(
    service,
    file_id: str,
    action: str,
    name: str,
    range_name: Optional[str] = None,
    named_range_id: Optional[str] = None,
    user_google_email: str = "",
) -> str:
    """Add, update, or delete named ranges. action: add, update, delete."""
    logger.info(
        "[manage_named_ranges] Invoked. Email: '%s', Spreadsheet: %s, Action: %s, Name: %s",
        user_google_email,
        file_id,
        action,
        name,
    )

    action_lower = action.lower()
    if action_lower not in ("add", "update", "delete"):
        raise UserInputError("action must be 'add', 'update', or 'delete'.")

    if action_lower == "add":
        if not range_name:
            raise UserInputError("range_name is required for 'add' action.")

        metadata = await asyncio.to_thread(
            service.spreadsheets()
            .get(
                spreadsheetId=file_id,
                fields="sheets(properties(sheetId,title))",
            )
            .execute
        )
        sheets = metadata.get("sheets", [])
        grid_range = _parse_a1_range(range_name, sheets)

        request_body = {
            "requests": [
                {
                    "addNamedRange": {
                        "namedRange": {
                            "name": name,
                            "range": grid_range,
                        }
                    }
                }
            ]
        }

        response = await asyncio.to_thread(
            service.spreadsheets()
            .batchUpdate(spreadsheetId=file_id, body=request_body)
            .execute
        )

        nr_id = response["replies"][0]["addNamedRange"]["namedRange"]["namedRangeId"]
        return f"Added named range '{name}' (ID: {nr_id}) for {range_name}."

    elif action_lower == "update":
        if not named_range_id:
            raise UserInputError("named_range_id is required for 'update' action.")
        if not range_name:
            raise UserInputError("range_name is required for 'update' action.")

        metadata = await asyncio.to_thread(
            service.spreadsheets()
            .get(
                spreadsheetId=file_id,
                fields="sheets(properties(sheetId,title))",
            )
            .execute
        )
        sheets = metadata.get("sheets", [])
        grid_range = _parse_a1_range(range_name, sheets)

        request_body = {
            "requests": [
                {
                    "updateNamedRange": {
                        "namedRange": {
                            "namedRangeId": named_range_id,
                            "name": name,
                            "range": grid_range,
                        },
                        "fields": "name,range",
                    }
                }
            ]
        }

        await asyncio.to_thread(
            service.spreadsheets()
            .batchUpdate(spreadsheetId=file_id, body=request_body)
            .execute
        )

        return f"Updated named range '{name}' (ID: {named_range_id}) to {range_name}."

    else:  # delete
        if not named_range_id:
            raise UserInputError("named_range_id is required for 'delete' action.")

        request_body = {
            "requests": [
                {"deleteNamedRange": {"namedRangeId": named_range_id}}
            ]
        }

        await asyncio.to_thread(
            service.spreadsheets()
            .batchUpdate(spreadsheetId=file_id, body=request_body)
            .execute
        )

        return f"Deleted named range '{name}' (ID: {named_range_id})."


@server.tool()
@handle_http_errors("manage_filter_view", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def manage_filter_view(
    service,
    file_id: str,
    action: str,
    range_name: Optional[str] = None,
    title: Optional[str] = None,
    user_google_email: str = "",
) -> str:
    """Manage basic filters. action: add (set basic filter) or clear_basic (clear it)."""
    logger.info(
        "[manage_filter_view] Invoked. Email: '%s', Spreadsheet: %s, Action: %s",
        user_google_email,
        file_id,
        action,
    )

    action_lower = action.lower()
    if action_lower not in ("add", "clear_basic"):
        raise UserInputError("action must be 'add' or 'clear_basic'.")

    if action_lower == "add":
        if not range_name:
            raise UserInputError("range_name is required for 'add' action.")

        metadata = await asyncio.to_thread(
            service.spreadsheets()
            .get(
                spreadsheetId=file_id,
                fields="sheets(properties(sheetId,title))",
            )
            .execute
        )
        sheets = metadata.get("sheets", [])
        grid_range = _parse_a1_range(range_name, sheets)

        request_body = {
            "requests": [
                {
                    "setBasicFilter": {
                        "filter": {
                            "range": grid_range,
                        }
                    }
                }
            ]
        }

        await asyncio.to_thread(
            service.spreadsheets()
            .batchUpdate(spreadsheetId=file_id, body=request_body)
            .execute
        )

        return f"Set basic filter on {range_name}."

    else:  # clear_basic
        if not range_name:
            raise UserInputError("range_name is required for 'clear_basic' to identify the sheet.")

        metadata = await asyncio.to_thread(
            service.spreadsheets()
            .get(
                spreadsheetId=file_id,
                fields="sheets(properties(sheetId,title))",
            )
            .execute
        )
        sheets = metadata.get("sheets", [])
        grid_range = _parse_a1_range(range_name, sheets)
        sheet_id = grid_range["sheetId"]

        request_body = {
            "requests": [
                {"clearBasicFilter": {"sheetId": sheet_id}}
            ]
        }

        await asyncio.to_thread(
            service.spreadsheets()
            .batchUpdate(spreadsheetId=file_id, body=request_body)
            .execute
        )

        return "Cleared basic filter."


@server.tool()
@handle_http_errors("manage_protected_range", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def manage_protected_range(
    service,
    file_id: str,
    action: str,
    range_name: Optional[str] = None,
    description: Optional[str] = None,
    protected_range_id: Optional[int] = None,
    warning_only: bool = False,
    user_google_email: str = "",
) -> str:
    """Add or delete protected ranges. action: add or delete."""
    logger.info(
        "[manage_protected_range] Invoked. Email: '%s', Spreadsheet: %s, Action: %s",
        user_google_email,
        file_id,
        action,
    )

    action_lower = action.lower()
    if action_lower not in ("add", "delete"):
        raise UserInputError("action must be 'add' or 'delete'.")

    if action_lower == "add":
        if not range_name:
            raise UserInputError("range_name is required for 'add' action.")

        metadata = await asyncio.to_thread(
            service.spreadsheets()
            .get(
                spreadsheetId=file_id,
                fields="sheets(properties(sheetId,title))",
            )
            .execute
        )
        sheets = metadata.get("sheets", [])
        grid_range = _parse_a1_range(range_name, sheets)

        protected_range = {
            "range": grid_range,
            "warningOnly": warning_only,
        }
        if description:
            protected_range["description"] = description

        request_body = {
            "requests": [
                {"addProtectedRange": {"protectedRange": protected_range}}
            ]
        }

        response = await asyncio.to_thread(
            service.spreadsheets()
            .batchUpdate(spreadsheetId=file_id, body=request_body)
            .execute
        )

        pr_id = response["replies"][0]["addProtectedRange"]["protectedRange"]["protectedRangeId"]
        mode = "warning only" if warning_only else "protected"
        return f"Added {mode} range (ID: {pr_id}) on {range_name}."

    else:  # delete
        if protected_range_id is None:
            raise UserInputError("protected_range_id is required for 'delete' action.")

        request_body = {
            "requests": [
                {"deleteProtectedRange": {"protectedRangeId": protected_range_id}}
            ]
        }

        await asyncio.to_thread(
            service.spreadsheets()
            .batchUpdate(spreadsheetId=file_id, body=request_body)
            .execute
        )

        return f"Deleted protected range (ID: {protected_range_id})."


# Create comment management tools for sheets
_comment_tools = create_comment_tools("spreadsheet", "file_id")

# Extract and register the functions
read_sheet_comments = _comment_tools["read_comments"]
create_sheet_comment = _comment_tools["create_comment"]
reply_to_sheet_comment = _comment_tools["reply_to_comment"]
resolve_sheet_comment = _comment_tools["resolve_comment"]
edit_sheet_comment = _comment_tools["edit_comment"]
delete_sheet_comment = _comment_tools["delete_comment"]
edit_sheet_comment_reply = _comment_tools["edit_reply"]
delete_sheet_comment_reply = _comment_tools["delete_reply"]
