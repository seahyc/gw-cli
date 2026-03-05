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
    user_google_email: str,
    max_results: int = 25,
) -> str:
    """
    Lists spreadsheets from Google Drive that the user has access to.

    Args:
        user_google_email (str): The user's Google email address. Required.
        max_results (int): Maximum number of spreadsheets to return. Defaults to 25.

    Returns:
        str: A formatted list of spreadsheet files (name, ID, modified time).
    """
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
        return f"No spreadsheets found for {user_google_email}."

    spreadsheets_list = [
        f'- "{file["name"]}" (ID: {file["id"]}) | Modified: {file.get("modifiedTime", "Unknown")} | Link: {file.get("webViewLink", "No link")}'
        for file in files
    ]

    text_output = (
        f"Successfully listed {len(files)} spreadsheets for {user_google_email}:\n"
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
    user_google_email: str,
    spreadsheet_id: str,
) -> str:
    """
    Gets information about a specific spreadsheet including its sheets.

    Args:
        user_google_email (str): The user's Google email address. Required.
        spreadsheet_id (str): The ID of the spreadsheet to get info for. Required.

    Returns:
        str: Formatted spreadsheet information including title, locale, and sheets list.
    """
    logger.info(
        f"[get_spreadsheet_info] Invoked. Email: '{user_google_email}', Spreadsheet ID: {spreadsheet_id}"
    )

    spreadsheet = await asyncio.to_thread(
        service.spreadsheets()
        .get(
            spreadsheetId=spreadsheet_id,
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
            f'Spreadsheet: "{title}" (ID: {spreadsheet_id}) | Locale: {locale}',
            f"Sheets ({len(sheets)}):",
            sheets_section,
        ]
    )

    logger.info(
        f"Successfully retrieved info for spreadsheet {spreadsheet_id} for {user_google_email}."
    )
    return text_output


@server.tool()
@handle_http_errors("read_sheet_values", is_read_only=True, service_type="sheets")
@require_google_service("sheets", "sheets_read")
async def read_sheet_values(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    range_name: str = "A1:Z1000",
    value_render_option: Literal[
        "FORMATTED_VALUE", "UNFORMATTED_VALUE", "FORMULA"
    ] = "FORMATTED_VALUE",
) -> str:
    """
    Reads values from a specific range in a Google Sheet.

    Args:
        user_google_email (str): The user's Google email address. Required.
        spreadsheet_id (str): The ID of the spreadsheet. Required.
        range_name (str): The range to read (e.g., "Sheet1!A1:D10", "A1:D10"). Defaults to "A1:Z1000".
        value_render_option (str): How values should be rendered in the output.
            "FORMATTED_VALUE" (default) - display values (e.g., "1,450", "$2.50").
            "UNFORMATTED_VALUE" - raw numbers without formatting (e.g., 1450, 2.5).
            "FORMULA" - the underlying formulas (e.g., "=C7*C9 - C8*C9").

    Returns:
        str: The formatted values from the specified range.
    """
    logger.info(
        f"[read_sheet_values] Invoked. Email: '{user_google_email}', Spreadsheet: {spreadsheet_id}, Range: {range_name}, ValueRenderOption: {value_render_option}"
    )

    result = await asyncio.to_thread(
        service.spreadsheets()
        .values()
        .get(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueRenderOption=value_render_option,
        )
        .execute
    )

    values = result.get("values", [])
    if not values:
        return f"No data found in range '{range_name}' for {user_google_email}."

    detailed_errors_section = ""
    if _values_contain_sheets_errors(values):
        resolved_range = result.get("range", range_name)
        detailed_range = _a1_range_for_values(resolved_range, values) or resolved_range
        try:
            errors = await _fetch_detailed_sheet_errors(
                service, spreadsheet_id, detailed_range
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

    # Format the output as a readable table
    formatted_rows = []
    for i, row in enumerate(values, 1):
        # Pad row with empty strings to show structure
        padded_row = row + [""] * max(0, len(values[0]) - len(row)) if values else row
        formatted_rows.append(f"Row {i:2d}: {padded_row}")

    text_output = (
        f"Successfully read {len(values)} rows from range '{range_name}' in spreadsheet {spreadsheet_id} for {user_google_email}:\n"
        + "\n".join(formatted_rows[:50])  # Limit to first 50 rows for readability
        + (f"\n... and {len(values) - 50} more rows" if len(values) > 50 else "")
    )

    logger.info(f"Successfully read {len(values)} rows for {user_google_email}.")
    return text_output + detailed_errors_section


@server.tool()
@handle_http_errors("modify_sheet_values", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def modify_sheet_values(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    range_name: str,
    values: Optional[Union[str, List[List[str]]]] = None,
    value_input_option: str = "USER_ENTERED",
    clear_values: bool = False,
) -> str:
    """
    Modifies values in a specific range of a Google Sheet - can write, update, or clear values.

    Args:
        user_google_email (str): The user's Google email address. Required.
        spreadsheet_id (str): The ID of the spreadsheet. Required.
        range_name (str): The range to modify (e.g., "Sheet1!A1:D10", "A1:D10"). Required.
        values (Optional[Union[str, List[List[str]]]]): 2D array of values to write/update. Can be a JSON string or Python list. Required unless clear_values=True.
        value_input_option (str): How to interpret input values ("RAW" or "USER_ENTERED"). Defaults to "USER_ENTERED".
        clear_values (bool): If True, clears the range instead of writing values. Defaults to False.

    Returns:
        str: Confirmation message of the successful modification operation.
    """
    operation = "clear" if clear_values else "write"
    logger.info(
        f"[modify_sheet_values] Invoked. Operation: {operation}, Email: '{user_google_email}', Spreadsheet: {spreadsheet_id}, Range: {range_name}"
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
            .clear(spreadsheetId=spreadsheet_id, range=range_name)
            .execute
        )

        cleared_range = result.get("clearedRange", range_name)
        text_output = f"Successfully cleared range '{cleared_range}' in spreadsheet {spreadsheet_id} for {user_google_email}."
        logger.info(
            f"Successfully cleared range '{cleared_range}' for {user_google_email}."
        )
    else:
        body = {"values": values}

        result = await asyncio.to_thread(
            service.spreadsheets()
            .values()
            .update(
                spreadsheetId=spreadsheet_id,
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
                    service, spreadsheet_id, detailed_range
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

        text_output = (
            f"Successfully updated range '{range_name}' in spreadsheet {spreadsheet_id} for {user_google_email}. "
            f"Updated: {updated_cells} cells, {updated_rows} rows, {updated_columns} columns."
        )
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
    user_google_email: str,
    spreadsheet_id: str,
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
) -> str:
    """
    Applies formatting to a range: colors, number/date formats, text style, alignment, and wrapping.

    Colors accept hex strings (#RRGGBB). Number formats follow Sheets types
    (e.g., NUMBER, NUMBER_WITH_GROUPING, CURRENCY, DATE, TIME, DATE_TIME,
    PERCENT, TEXT, SCIENTIFIC). If no sheet name is provided, the first sheet
    is used.

    Args:
        user_google_email (str): The user's Google email address. Required.
        spreadsheet_id (str): The ID of the spreadsheet. Required.
        range_name (str): A1-style range (optionally with sheet name). Required.
        background_color (Optional[str]): Hex background color (e.g., "#FFEECC").
        text_color (Optional[str]): Hex text color (e.g., "#000000").
        number_format_type (Optional[str]): Sheets number format type (e.g., "DATE").
        number_format_pattern (Optional[str]): Optional custom pattern for the number format.
        bold (Optional[bool]): Whether to bold the text.
        italic (Optional[bool]): Whether to italicize the text.
        underline (Optional[bool]): Whether to underline the text.
        strikethrough (Optional[bool]): Whether to strikethrough the text.
        font_size (Optional[int]): Font size in points.
        font_family (Optional[str]): Font family name (e.g., "Arial", "Roboto").
        horizontal_alignment (Optional[str]): Horizontal alignment: "LEFT", "CENTER", or "RIGHT".
        vertical_alignment (Optional[str]): Vertical alignment: "TOP", "MIDDLE", or "BOTTOM".
        wrap_strategy (Optional[str]): Text wrapping: "OVERFLOW_CELL", "CLIP", or "WRAP".

    Returns:
        str: Confirmation of the applied formatting.
    """
    logger.info(
        "[format_sheet_range] Invoked. Email: '%s', Spreadsheet: %s, Range: %s",
        user_google_email,
        spreadsheet_id,
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
            spreadsheetId=spreadsheet_id,
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
        .batchUpdate(spreadsheetId=spreadsheet_id, body=request_body)
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
    return (
        f"Applied formatting to range '{range_name}' in spreadsheet {spreadsheet_id} "
        f"for {user_google_email}: {summary}."
    )


@server.tool()
@handle_http_errors("add_conditional_formatting", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def add_conditional_formatting(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    range_name: str,
    condition_type: str,
    condition_values: Optional[Union[str, List[Union[str, int, float]]]] = None,
    background_color: Optional[str] = None,
    text_color: Optional[str] = None,
    rule_index: Optional[int] = None,
    gradient_points: Optional[Union[str, List[dict]]] = None,
) -> str:
    """
    Adds a conditional formatting rule to a range.

    Args:
        user_google_email (str): The user's Google email address. Required.
        spreadsheet_id (str): The ID of the spreadsheet. Required.
        range_name (str): A1-style range (optionally with sheet name). Required.
        condition_type (str): Sheets condition type (e.g., NUMBER_GREATER, TEXT_CONTAINS, DATE_BEFORE, CUSTOM_FORMULA).
        condition_values (Optional[Union[str, List[Union[str, int, float]]]]): Values for the condition; accepts a list or a JSON string representing a list. Depends on condition_type.
        background_color (Optional[str]): Hex background color to apply when condition matches.
        text_color (Optional[str]): Hex text color to apply when condition matches.
        rule_index (Optional[int]): Optional position to insert the rule (0-based) within the sheet's rules.
        gradient_points (Optional[Union[str, List[dict]]]): List (or JSON list) of gradient points for a color scale. If provided, a gradient rule is created and boolean parameters are ignored.

    Returns:
        str: Confirmation of the added rule.
    """
    logger.info(
        "[add_conditional_formatting] Invoked. Email: '%s', Spreadsheet: %s, Range: %s, Type: %s, Values: %s",
        user_google_email,
        spreadsheet_id,
        range_name,
        condition_type,
        condition_values,
    )

    if rule_index is not None and (not isinstance(rule_index, int) or rule_index < 0):
        raise UserInputError("rule_index must be a non-negative integer when provided.")

    condition_values_list = _parse_condition_values(condition_values)
    gradient_points_list = _parse_gradient_points(gradient_points)

    sheets, sheet_titles = await _fetch_sheets_with_rules(service, spreadsheet_id)
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
        .batchUpdate(spreadsheetId=spreadsheet_id, body=request_body)
        .execute
    )

    format_desc = ", ".join(applied_parts) if applied_parts else "format applied"

    sheet_title = target_sheet.get("properties", {}).get("title", "Unknown")
    state_text = _format_conditional_rules_section(
        sheet_title, new_rules_state, sheet_titles, indent=""
    )

    return "\n".join(
        [
            f"Added conditional format on '{range_name}' in spreadsheet {spreadsheet_id} "
            f"for {user_google_email}: {rule_desc}{values_desc}; format: {format_desc}.",
            state_text,
        ]
    )


@server.tool()
@handle_http_errors("update_conditional_formatting", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def update_conditional_formatting(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    rule_index: int,
    range_name: Optional[str] = None,
    condition_type: Optional[str] = None,
    condition_values: Optional[Union[str, List[Union[str, int, float]]]] = None,
    background_color: Optional[str] = None,
    text_color: Optional[str] = None,
    sheet_name: Optional[str] = None,
    gradient_points: Optional[Union[str, List[dict]]] = None,
) -> str:
    """
    Updates an existing conditional formatting rule by index on a sheet.

    Args:
        user_google_email (str): The user's Google email address. Required.
        spreadsheet_id (str): The ID of the spreadsheet. Required.
        range_name (Optional[str]): A1-style range to apply the updated rule (optionally with sheet name). If omitted, existing ranges are preserved.
        rule_index (int): Index of the rule to update (0-based).
        condition_type (Optional[str]): Sheets condition type. If omitted, the existing rule's type is preserved.
        condition_values (Optional[Union[str, List[Union[str, int, float]]]]): Values for the condition.
        background_color (Optional[str]): Hex background color when condition matches.
        text_color (Optional[str]): Hex text color when condition matches.
        sheet_name (Optional[str]): Sheet name to locate the rule when range_name is omitted. Defaults to first sheet.
        gradient_points (Optional[Union[str, List[dict]]]): If provided, updates the rule to a gradient color scale using these points.

    Returns:
        str: Confirmation of the updated rule and the current rule state.
    """
    logger.info(
        "[update_conditional_formatting] Invoked. Email: '%s', Spreadsheet: %s, Range: %s, Rule Index: %s",
        user_google_email,
        spreadsheet_id,
        range_name,
        rule_index,
    )

    if not isinstance(rule_index, int) or rule_index < 0:
        raise UserInputError("rule_index must be a non-negative integer.")

    condition_values_list = _parse_condition_values(condition_values)
    gradient_points_list = _parse_gradient_points(gradient_points)

    sheets, sheet_titles = await _fetch_sheets_with_rules(service, spreadsheet_id)

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
        .batchUpdate(spreadsheetId=spreadsheet_id, body=request_body)
        .execute
    )

    state_text = _format_conditional_rules_section(
        sheet_title, new_rules_state, sheet_titles, indent=""
    )

    return "\n".join(
        [
            f"Updated conditional format at index {rule_index} on sheet '{sheet_title}' in spreadsheet {spreadsheet_id} "
            f"for {user_google_email}: {rule_desc}{values_desc}; format: {format_desc}.",
            state_text,
        ]
    )


@server.tool()
@handle_http_errors("delete_conditional_formatting", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def delete_conditional_formatting(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    rule_index: int,
    sheet_name: Optional[str] = None,
) -> str:
    """
    Deletes an existing conditional formatting rule by index on a sheet.

    Args:
        user_google_email (str): The user's Google email address. Required.
        spreadsheet_id (str): The ID of the spreadsheet. Required.
        rule_index (int): Index of the rule to delete (0-based).
        sheet_name (Optional[str]): Name of the sheet that contains the rule. Defaults to the first sheet if not provided.

    Returns:
        str: Confirmation of the deletion and the current rule state.
    """
    logger.info(
        "[delete_conditional_formatting] Invoked. Email: '%s', Spreadsheet: %s, Sheet: %s, Rule Index: %s",
        user_google_email,
        spreadsheet_id,
        sheet_name,
        rule_index,
    )

    if not isinstance(rule_index, int) or rule_index < 0:
        raise UserInputError("rule_index must be a non-negative integer.")

    sheets, sheet_titles = await _fetch_sheets_with_rules(service, spreadsheet_id)
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
        .batchUpdate(spreadsheetId=spreadsheet_id, body=request_body)
        .execute
    )

    state_text = _format_conditional_rules_section(
        target_sheet_name, new_rules_state, sheet_titles, indent=""
    )

    return "\n".join(
        [
            f"Deleted conditional format at index {rule_index} on sheet '{target_sheet_name}' in spreadsheet {spreadsheet_id} for {user_google_email}.",
            state_text,
        ]
    )


@server.tool()
@handle_http_errors("create_spreadsheet", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def create_spreadsheet(
    service,
    user_google_email: str,
    title: str,
    sheet_names: Optional[List[str]] = None,
) -> str:
    """
    Creates a new Google Spreadsheet.

    Args:
        user_google_email (str): The user's Google email address. Required.
        title (str): The title of the new spreadsheet. Required.
        sheet_names (Optional[List[str]]): List of sheet names to create. If not provided, creates one sheet with default name.

    Returns:
        str: Information about the newly created spreadsheet including ID, URL, and locale.
    """
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
    spreadsheet_id = spreadsheet.get("spreadsheetId")
    spreadsheet_url = spreadsheet.get("spreadsheetUrl")
    locale = properties.get("locale", "Unknown")

    text_output = (
        f"Successfully created spreadsheet '{title}' for {user_google_email}. "
        f"ID: {spreadsheet_id} | URL: {spreadsheet_url} | Locale: {locale}"
    )

    logger.info(
        f"Successfully created spreadsheet for {user_google_email}. ID: {spreadsheet_id}"
    )
    return text_output


@server.tool()
@handle_http_errors("create_sheet", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def create_sheet(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    sheet_name: str,
) -> str:
    """
    Creates a new sheet within an existing spreadsheet.

    Args:
        user_google_email (str): The user's Google email address. Required.
        spreadsheet_id (str): The ID of the spreadsheet. Required.
        sheet_name (str): The name of the new sheet. Required.

    Returns:
        str: Confirmation message of the successful sheet creation.
    """
    logger.info(
        f"[create_sheet] Invoked. Email: '{user_google_email}', Spreadsheet: {spreadsheet_id}, Sheet: {sheet_name}"
    )

    request_body = {"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]}

    response = await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(spreadsheetId=spreadsheet_id, body=request_body)
        .execute
    )

    sheet_id = response["replies"][0]["addSheet"]["properties"]["sheetId"]

    text_output = f"Successfully created sheet '{sheet_name}' (ID: {sheet_id}) in spreadsheet {spreadsheet_id} for {user_google_email}."

    logger.info(
        f"Successfully created sheet for {user_google_email}. Sheet ID: {sheet_id}"
    )
    return text_output


@server.tool()
@handle_http_errors("update_borders", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def update_borders(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    range_name: str,
    border_style: str = "SOLID",
    border_color: str = "#000000",
    borders: str = "all",
) -> str:
    """
    Applies borders to a range of cells.

    Args:
        user_google_email (str): The user's Google email address. Required.
        spreadsheet_id (str): The ID of the spreadsheet. Required.
        range_name (str): A1-style range (optionally with sheet name). Required.
        border_style (str): Border line style. One of DOTTED, DASHED, SOLID, SOLID_MEDIUM, SOLID_THICK, DOUBLE. Defaults to "SOLID".
        border_color (str): Hex color for borders (e.g., "#000000"). Defaults to "#000000".
        borders (str): Which borders to apply. Comma-separated from: top, bottom, left, right, innerHorizontal, innerVertical. Or use "all", "outer", "inner". Defaults to "all".

    Returns:
        str: Confirmation of the applied borders.
    """
    logger.info(
        "[update_borders] Invoked. Email: '%s', Spreadsheet: %s, Range: %s",
        user_google_email,
        spreadsheet_id,
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
            spreadsheetId=spreadsheet_id,
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
        .batchUpdate(spreadsheetId=spreadsheet_id, body=request_body)
        .execute
    )

    return (
        f"Applied {border_style} borders ({borders}) with color {border_color} "
        f"to range '{range_name}' in spreadsheet {spreadsheet_id} for {user_google_email}."
    )


@server.tool()
@handle_http_errors("merge_cells", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def merge_cells(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    range_name: str,
    merge_type: str = "MERGE_ALL",
    unmerge: bool = False,
) -> str:
    """
    Merges or unmerges cells in a range.

    Args:
        user_google_email (str): The user's Google email address. Required.
        spreadsheet_id (str): The ID of the spreadsheet. Required.
        range_name (str): A1-style range of cells to merge/unmerge. Required.
        merge_type (str): Type of merge: MERGE_ALL, MERGE_COLUMNS, or MERGE_ROWS. Defaults to "MERGE_ALL".
        unmerge (bool): If True, unmerge cells instead of merging. Defaults to False.

    Returns:
        str: Confirmation of the merge/unmerge operation.
    """
    logger.info(
        "[merge_cells] Invoked. Email: '%s', Spreadsheet: %s, Range: %s, Unmerge: %s",
        user_google_email,
        spreadsheet_id,
        range_name,
        unmerge,
    )

    allowed_merge_types = {"MERGE_ALL", "MERGE_COLUMNS", "MERGE_ROWS"}
    if merge_type.upper() not in allowed_merge_types:
        raise UserInputError(f"merge_type must be one of {sorted(allowed_merge_types)}.")

    metadata = await asyncio.to_thread(
        service.spreadsheets()
        .get(
            spreadsheetId=spreadsheet_id,
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
        .batchUpdate(spreadsheetId=spreadsheet_id, body=request_body)
        .execute
    )

    return (
        f"{action} cells in range '{range_name}' in spreadsheet {spreadsheet_id} "
        f"for {user_google_email}."
    )


@server.tool()
@handle_http_errors("insert_dimension", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def insert_dimension(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    sheet_name: str,
    dimension: str,
    start_index: int,
    count: int = 1,
    inherit_from_before: bool = True,
) -> str:
    """
    Inserts rows or columns into a sheet.

    Args:
        user_google_email (str): The user's Google email address. Required.
        spreadsheet_id (str): The ID of the spreadsheet. Required.
        sheet_name (str): Name of the sheet tab. Required.
        dimension (str): "ROWS" or "COLUMNS". Required.
        start_index (int): 0-based index where to insert. Required.
        count (int): Number of rows/columns to insert. Defaults to 1.
        inherit_from_before (bool): Inherit formatting from the row/column before. Defaults to True.

    Returns:
        str: Confirmation of the insertion.
    """
    logger.info(
        "[insert_dimension] Invoked. Email: '%s', Spreadsheet: %s, Sheet: %s, Dim: %s, Start: %d, Count: %d",
        user_google_email,
        spreadsheet_id,
        sheet_name,
        dimension,
        start_index,
        count,
    )

    if dimension.upper() not in ("ROWS", "COLUMNS"):
        raise UserInputError("dimension must be 'ROWS' or 'COLUMNS'.")

    sheet_id = await _get_sheet_id_by_name(service, spreadsheet_id, sheet_name)

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
        .batchUpdate(spreadsheetId=spreadsheet_id, body=request_body)
        .execute
    )

    return (
        f"Inserted {count} {dimension.lower()} at index {start_index} in sheet '{sheet_name}' "
        f"of spreadsheet {spreadsheet_id} for {user_google_email}."
    )


@server.tool()
@handle_http_errors("delete_dimension", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def delete_dimension(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    sheet_name: str,
    dimension: str,
    start_index: int,
    end_index: int,
) -> str:
    """
    Deletes rows or columns from a sheet.

    Args:
        user_google_email (str): The user's Google email address. Required.
        spreadsheet_id (str): The ID of the spreadsheet. Required.
        sheet_name (str): Name of the sheet tab. Required.
        dimension (str): "ROWS" or "COLUMNS". Required.
        start_index (int): 0-based start index (inclusive). Required.
        end_index (int): 0-based end index (exclusive). Required.

    Returns:
        str: Confirmation of the deletion.
    """
    logger.info(
        "[delete_dimension] Invoked. Email: '%s', Spreadsheet: %s, Sheet: %s, Dim: %s, Start: %d, End: %d",
        user_google_email,
        spreadsheet_id,
        sheet_name,
        dimension,
        start_index,
        end_index,
    )

    if dimension.upper() not in ("ROWS", "COLUMNS"):
        raise UserInputError("dimension must be 'ROWS' or 'COLUMNS'.")
    if start_index >= end_index:
        raise UserInputError("start_index must be less than end_index.")

    sheet_id = await _get_sheet_id_by_name(service, spreadsheet_id, sheet_name)

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
        .batchUpdate(spreadsheetId=spreadsheet_id, body=request_body)
        .execute
    )

    deleted_count = end_index - start_index
    return (
        f"Deleted {deleted_count} {dimension.lower()} (index {start_index} to {end_index}) "
        f"in sheet '{sheet_name}' of spreadsheet {spreadsheet_id} for {user_google_email}."
    )


@server.tool()
@handle_http_errors("sort_range", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def sort_range(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    range_name: str,
    sort_specs: Union[str, List[dict]],
) -> str:
    """
    Sorts data in a range by one or more columns.

    Args:
        user_google_email (str): The user's Google email address. Required.
        spreadsheet_id (str): The ID of the spreadsheet. Required.
        range_name (str): A1-style range to sort (optionally with sheet name). Required.
        sort_specs (Union[str, List[dict]]): List of sort specifications, each with 'column_index' (0-based within range) and 'order' ("ASCENDING" or "DESCENDING"). Can be a JSON string. Required.

    Returns:
        str: Confirmation of the sort operation.
    """
    logger.info(
        "[sort_range] Invoked. Email: '%s', Spreadsheet: %s, Range: %s",
        user_google_email,
        spreadsheet_id,
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
            spreadsheetId=spreadsheet_id,
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
        .batchUpdate(spreadsheetId=spreadsheet_id, body=request_body)
        .execute
    )

    specs_desc = ", ".join(
        f"col {s.get('column_index')} {s.get('order', 'ASCENDING')}" for s in sort_specs
    )
    return (
        f"Sorted range '{range_name}' in spreadsheet {spreadsheet_id} "
        f"for {user_google_email} by: {specs_desc}."
    )


@server.tool()
@handle_http_errors("set_data_validation", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def set_data_validation(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    range_name: str,
    validation_type: str = "ONE_OF_LIST",
    values: Optional[Union[str, List]] = None,
    strict: bool = True,
    show_dropdown: bool = True,
    clear: bool = False,
) -> str:
    """
    Sets or clears data validation on a range.

    Args:
        user_google_email (str): The user's Google email address. Required.
        spreadsheet_id (str): The ID of the spreadsheet. Required.
        range_name (str): A1-style range (optionally with sheet name). Required.
        validation_type (str): Validation type: ONE_OF_LIST, NUMBER_BETWEEN, NUMBER_GREATER, TEXT_CONTAINS, DATE_BEFORE, CUSTOM_FORMULA, etc. Defaults to "ONE_OF_LIST".
        values (Optional[Union[str, List]]): Validation values (items for ONE_OF_LIST, bounds for NUMBER_BETWEEN, formula for CUSTOM_FORMULA). Can be a JSON string.
        strict (bool): Reject invalid input if True. Defaults to True.
        show_dropdown (bool): Show dropdown for list validation. Defaults to True.
        clear (bool): If True, clear validation from range. Defaults to False.

    Returns:
        str: Confirmation of the validation operation.
    """
    logger.info(
        "[set_data_validation] Invoked. Email: '%s', Spreadsheet: %s, Range: %s, Clear: %s",
        user_google_email,
        spreadsheet_id,
        range_name,
        clear,
    )

    metadata = await asyncio.to_thread(
        service.spreadsheets()
        .get(
            spreadsheetId=spreadsheet_id,
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
            .batchUpdate(spreadsheetId=spreadsheet_id, body=request_body)
            .execute
        )

        return (
            f"Cleared data validation from range '{range_name}' in spreadsheet {spreadsheet_id} "
            f"for {user_google_email}."
        )

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
        .batchUpdate(spreadsheetId=spreadsheet_id, body=request_body)
        .execute
    )

    values_desc = f" with values {values}" if values else ""
    return (
        f"Set {validation_type} data validation{values_desc} on range '{range_name}' "
        f"in spreadsheet {spreadsheet_id} for {user_google_email}."
    )


@server.tool()
@handle_http_errors("delete_sheet", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def delete_sheet(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    sheet_name: str,
) -> str:
    """
    Deletes a sheet tab from a spreadsheet.

    Args:
        user_google_email (str): The user's Google email address. Required.
        spreadsheet_id (str): The ID of the spreadsheet. Required.
        sheet_name (str): Name of the sheet tab to delete. Required.

    Returns:
        str: Confirmation of the deletion.
    """
    logger.info(
        "[delete_sheet] Invoked. Email: '%s', Spreadsheet: %s, Sheet: %s",
        user_google_email,
        spreadsheet_id,
        sheet_name,
    )

    sheet_id = await _get_sheet_id_by_name(service, spreadsheet_id, sheet_name)

    request_body = {
        "requests": [{"deleteSheet": {"sheetId": sheet_id}}]
    }

    await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(spreadsheetId=spreadsheet_id, body=request_body)
        .execute
    )

    return (
        f"Deleted sheet '{sheet_name}' (ID: {sheet_id}) from spreadsheet {spreadsheet_id} "
        f"for {user_google_email}."
    )


@server.tool()
@handle_http_errors("batch_read_values", is_read_only=True, service_type="sheets")
@require_google_service("sheets", "sheets_read")
async def batch_read_values(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    ranges: Union[str, List[str]],
    value_render_option: str = "FORMATTED_VALUE",
) -> str:
    """
    Reads values from multiple ranges in a single request.

    Args:
        user_google_email (str): The user's Google email address. Required.
        spreadsheet_id (str): The ID of the spreadsheet. Required.
        ranges (Union[str, List[str]]): List of A1-notation ranges to read. Can be a JSON string. Required.
        value_render_option (str): How values should be rendered: "FORMATTED_VALUE", "UNFORMATTED_VALUE", or "FORMULA". Defaults to "FORMATTED_VALUE".

    Returns:
        str: Formatted values from all specified ranges.
    """
    logger.info(
        "[batch_read_values] Invoked. Email: '%s', Spreadsheet: %s, Ranges: %s",
        user_google_email,
        spreadsheet_id,
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
            spreadsheetId=spreadsheet_id,
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
        for i, row in enumerate(values, 1):
            formatted_rows.append(f"Row {i:2d}: {row}")

        output_parts.append(
            f"\n--- {range_label} ({len(values)} rows) ---\n"
            + "\n".join(formatted_rows[:50])
            + (f"\n... and {len(values) - 50} more rows" if len(values) > 50 else "")
        )

    header = (
        f"Successfully read {len(value_ranges)} ranges ({total_rows} total rows) "
        f"from spreadsheet {spreadsheet_id} for {user_google_email}:"
    )

    logger.info(f"Successfully batch-read {len(value_ranges)} ranges for {user_google_email}.")
    return header + "".join(output_parts)


@server.tool()
@handle_http_errors("auto_resize_dimensions", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def auto_resize_dimensions(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    sheet_name: str,
    dimension: str = "COLUMNS",
    start_index: int = 0,
    end_index: Optional[int] = None,
) -> str:
    """
    Auto-resizes rows or columns to fit their content.

    Args:
        user_google_email (str): The user's Google email address. Required.
        spreadsheet_id (str): The ID of the spreadsheet. Required.
        sheet_name (str): Name of the sheet tab. Required.
        dimension (str): "ROWS" or "COLUMNS". Defaults to "COLUMNS".
        start_index (int): 0-based start index. Defaults to 0.
        end_index (Optional[int]): 0-based end index (exclusive). If None, resizes all from start_index.

    Returns:
        str: Confirmation of the auto-resize operation.
    """
    logger.info(
        "[auto_resize_dimensions] Invoked. Email: '%s', Spreadsheet: %s, Sheet: %s, Dim: %s",
        user_google_email,
        spreadsheet_id,
        sheet_name,
        dimension,
    )

    if dimension.upper() not in ("ROWS", "COLUMNS"):
        raise UserInputError("dimension must be 'ROWS' or 'COLUMNS'.")

    sheet_id = await _get_sheet_id_by_name(service, spreadsheet_id, sheet_name)

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
        .batchUpdate(spreadsheetId=spreadsheet_id, body=request_body)
        .execute
    )

    range_desc = f"from index {start_index}" + (f" to {end_index}" if end_index else " to end")
    return (
        f"Auto-resized {dimension.lower()} {range_desc} in sheet '{sheet_name}' "
        f"of spreadsheet {spreadsheet_id} for {user_google_email}."
    )


@server.tool()
@handle_http_errors("freeze_dimensions", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def freeze_dimensions(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    sheet_name: str,
    frozen_rows: Optional[int] = None,
    frozen_columns: Optional[int] = None,
) -> str:
    """
    Freezes rows and/or columns in a sheet.

    Args:
        user_google_email (str): The user's Google email address. Required.
        spreadsheet_id (str): The ID of the spreadsheet. Required.
        sheet_name (str): Name of the sheet tab. Required.
        frozen_rows (Optional[int]): Number of rows to freeze from the top. Set to 0 to unfreeze.
        frozen_columns (Optional[int]): Number of columns to freeze from the left. Set to 0 to unfreeze.

    Returns:
        str: Confirmation of the freeze operation.
    """
    logger.info(
        "[freeze_dimensions] Invoked. Email: '%s', Spreadsheet: %s, Sheet: %s, Rows: %s, Cols: %s",
        user_google_email,
        spreadsheet_id,
        sheet_name,
        frozen_rows,
        frozen_columns,
    )

    if frozen_rows is None and frozen_columns is None:
        raise UserInputError("Provide at least one of frozen_rows or frozen_columns.")

    sheet_id = await _get_sheet_id_by_name(service, spreadsheet_id, sheet_name)

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
        .batchUpdate(spreadsheetId=spreadsheet_id, body=request_body)
        .execute
    )

    parts = []
    if frozen_rows is not None:
        parts.append(f"{frozen_rows} rows")
    if frozen_columns is not None:
        parts.append(f"{frozen_columns} columns")

    return (
        f"Froze {' and '.join(parts)} in sheet '{sheet_name}' "
        f"of spreadsheet {spreadsheet_id} for {user_google_email}."
    )


@server.tool()
@handle_http_errors("duplicate_sheet", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def duplicate_sheet(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    sheet_name: str,
    new_name: str,
    insert_index: Optional[int] = None,
) -> str:
    """
    Duplicates a sheet tab within a spreadsheet.

    Args:
        user_google_email (str): The user's Google email address. Required.
        spreadsheet_id (str): The ID of the spreadsheet. Required.
        sheet_name (str): Name of the source sheet to duplicate. Required.
        new_name (str): Name for the new copy. Required.
        insert_index (Optional[int]): Position for the new sheet (0-based). If None, appends at end.

    Returns:
        str: Confirmation of the duplication with the new sheet ID.
    """
    logger.info(
        "[duplicate_sheet] Invoked. Email: '%s', Spreadsheet: %s, Source: %s, NewName: %s",
        user_google_email,
        spreadsheet_id,
        sheet_name,
        new_name,
    )

    sheet_id = await _get_sheet_id_by_name(service, spreadsheet_id, sheet_name)

    dup_request = {
        "sourceSheetId": sheet_id,
        "newSheetName": new_name,
    }
    if insert_index is not None:
        dup_request["insertSheetIndex"] = insert_index

    request_body = {"requests": [{"duplicateSheet": dup_request}]}

    response = await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(spreadsheetId=spreadsheet_id, body=request_body)
        .execute
    )

    new_sheet_id = response["replies"][0]["duplicateSheet"]["properties"]["sheetId"]

    return (
        f"Duplicated sheet '{sheet_name}' as '{new_name}' (ID: {new_sheet_id}) "
        f"in spreadsheet {spreadsheet_id} for {user_google_email}."
    )


@server.tool()
@handle_http_errors("update_sheet_properties", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def update_sheet_properties(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    sheet_name: str,
    new_name: Optional[str] = None,
    tab_color: Optional[str] = None,
    hidden: Optional[bool] = None,
    right_to_left: Optional[bool] = None,
    index: Optional[int] = None,
) -> str:
    """
    Updates properties of a sheet tab (rename, color, visibility, order).

    Args:
        user_google_email (str): The user's Google email address. Required.
        spreadsheet_id (str): The ID of the spreadsheet. Required.
        sheet_name (str): Current name of the sheet tab. Required.
        new_name (Optional[str]): New name for the sheet tab.
        tab_color (Optional[str]): Hex color for the tab (e.g., "#FF0000").
        hidden (Optional[bool]): Whether the sheet should be hidden.
        right_to_left (Optional[bool]): Whether the sheet is right-to-left.
        index (Optional[int]): New position index for the sheet (0-based).

    Returns:
        str: Confirmation of the property updates.
    """
    logger.info(
        "[update_sheet_properties] Invoked. Email: '%s', Spreadsheet: %s, Sheet: %s",
        user_google_email,
        spreadsheet_id,
        sheet_name,
    )

    if not any([new_name, tab_color, hidden is not None, right_to_left is not None, index is not None]):
        raise UserInputError(
            "Provide at least one property to update (new_name, tab_color, hidden, right_to_left, index)."
        )

    sheet_id = await _get_sheet_id_by_name(service, spreadsheet_id, sheet_name)

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
        .batchUpdate(spreadsheetId=spreadsheet_id, body=request_body)
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

    return (
        f"Updated sheet '{sheet_name}' ({', '.join(changes)}) "
        f"in spreadsheet {spreadsheet_id} for {user_google_email}."
    )


@server.tool()
@handle_http_errors("find_replace_sheet", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def find_replace_sheet(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    find: str,
    replacement: str,
    sheet_name: Optional[str] = None,
    match_case: bool = False,
    match_entire_cell: bool = False,
    search_by_regex: bool = False,
) -> str:
    """
    Finds and replaces text in a spreadsheet.

    Args:
        user_google_email (str): The user's Google email address. Required.
        spreadsheet_id (str): The ID of the spreadsheet. Required.
        find (str): The text to search for. Required.
        replacement (str): The replacement text. Required.
        sheet_name (Optional[str]): Limit search to a specific sheet. If None, searches all sheets.
        match_case (bool): Case-sensitive search. Defaults to False.
        match_entire_cell (bool): Match the entire cell content. Defaults to False.
        search_by_regex (bool): Treat the find string as a regex. Defaults to False.

    Returns:
        str: Summary of replacements made.
    """
    logger.info(
        "[find_replace_sheet] Invoked. Email: '%s', Spreadsheet: %s, Find: '%s', Replace: '%s'",
        user_google_email,
        spreadsheet_id,
        find,
        replacement,
    )

    find_replace_req = {
        "find": find,
        "replacement": replacement,
        "matchCase": match_case,
        "matchEntireCell": match_entire_cell,
        "searchByRegex": search_by_regex,
        "allSheets": sheet_name is None,
    }

    if sheet_name is not None:
        sheet_id = await _get_sheet_id_by_name(service, spreadsheet_id, sheet_name)
        find_replace_req["sheetId"] = sheet_id

    request_body = {"requests": [{"findReplace": find_replace_req}]}

    response = await asyncio.to_thread(
        service.spreadsheets()
        .batchUpdate(spreadsheetId=spreadsheet_id, body=request_body)
        .execute
    )

    reply = response.get("replies", [{}])[0].get("findReplace", {})
    occurrences = reply.get("occurrencesChanged", 0)
    rows_changed = reply.get("rowsChanged", 0)
    sheets_changed = reply.get("sheetsChanged", 0)

    scope = f"sheet '{sheet_name}'" if sheet_name else "all sheets"
    return (
        f"Find & replace in {scope} of spreadsheet {spreadsheet_id} for {user_google_email}: "
        f"replaced '{find}' with '{replacement}' - {occurrences} occurrences in {rows_changed} rows across {sheets_changed} sheets."
    )


@server.tool()
@handle_http_errors("manage_named_ranges", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def manage_named_ranges(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    action: str,
    name: str,
    range_name: Optional[str] = None,
    named_range_id: Optional[str] = None,
) -> str:
    """
    Adds, updates, or deletes named ranges in a spreadsheet.

    Args:
        user_google_email (str): The user's Google email address. Required.
        spreadsheet_id (str): The ID of the spreadsheet. Required.
        action (str): Action to perform: "add", "update", or "delete". Required.
        name (str): Name for the named range. Required.
        range_name (Optional[str]): A1-style range (for add/update).
        named_range_id (Optional[str]): ID of the named range (for update/delete).

    Returns:
        str: Confirmation of the operation.
    """
    logger.info(
        "[manage_named_ranges] Invoked. Email: '%s', Spreadsheet: %s, Action: %s, Name: %s",
        user_google_email,
        spreadsheet_id,
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
                spreadsheetId=spreadsheet_id,
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
            .batchUpdate(spreadsheetId=spreadsheet_id, body=request_body)
            .execute
        )

        nr_id = response["replies"][0]["addNamedRange"]["namedRange"]["namedRangeId"]
        return (
            f"Added named range '{name}' (ID: {nr_id}) for range '{range_name}' "
            f"in spreadsheet {spreadsheet_id} for {user_google_email}."
        )

    elif action_lower == "update":
        if not named_range_id:
            raise UserInputError("named_range_id is required for 'update' action.")
        if not range_name:
            raise UserInputError("range_name is required for 'update' action.")

        metadata = await asyncio.to_thread(
            service.spreadsheets()
            .get(
                spreadsheetId=spreadsheet_id,
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
            .batchUpdate(spreadsheetId=spreadsheet_id, body=request_body)
            .execute
        )

        return (
            f"Updated named range '{name}' (ID: {named_range_id}) to range '{range_name}' "
            f"in spreadsheet {spreadsheet_id} for {user_google_email}."
        )

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
            .batchUpdate(spreadsheetId=spreadsheet_id, body=request_body)
            .execute
        )

        return (
            f"Deleted named range '{name}' (ID: {named_range_id}) "
            f"from spreadsheet {spreadsheet_id} for {user_google_email}."
        )


@server.tool()
@handle_http_errors("manage_filter_view", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def manage_filter_view(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    action: str,
    range_name: Optional[str] = None,
    title: Optional[str] = None,
) -> str:
    """
    Manages basic filters and filter views on a sheet.

    Args:
        user_google_email (str): The user's Google email address. Required.
        spreadsheet_id (str): The ID of the spreadsheet. Required.
        action (str): Action to perform: "add" (set basic filter), "clear_basic" (clear basic filter). Required.
        range_name (Optional[str]): A1-style range for the filter (required for "add").
        title (Optional[str]): Title for filter views (used with "add").

    Returns:
        str: Confirmation of the filter operation.
    """
    logger.info(
        "[manage_filter_view] Invoked. Email: '%s', Spreadsheet: %s, Action: %s",
        user_google_email,
        spreadsheet_id,
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
                spreadsheetId=spreadsheet_id,
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
            .batchUpdate(spreadsheetId=spreadsheet_id, body=request_body)
            .execute
        )

        return (
            f"Set basic filter on range '{range_name}' in spreadsheet {spreadsheet_id} "
            f"for {user_google_email}."
        )

    else:  # clear_basic
        if not range_name:
            raise UserInputError("range_name is required for 'clear_basic' to identify the sheet.")

        metadata = await asyncio.to_thread(
            service.spreadsheets()
            .get(
                spreadsheetId=spreadsheet_id,
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
            .batchUpdate(spreadsheetId=spreadsheet_id, body=request_body)
            .execute
        )

        return (
            f"Cleared basic filter from sheet in spreadsheet {spreadsheet_id} "
            f"for {user_google_email}."
        )


@server.tool()
@handle_http_errors("manage_protected_range", service_type="sheets")
@require_google_service("sheets", "sheets_write")
async def manage_protected_range(
    service,
    user_google_email: str,
    spreadsheet_id: str,
    action: str,
    range_name: Optional[str] = None,
    description: Optional[str] = None,
    protected_range_id: Optional[int] = None,
    warning_only: bool = False,
) -> str:
    """
    Adds or deletes protected ranges in a spreadsheet.

    Args:
        user_google_email (str): The user's Google email address. Required.
        spreadsheet_id (str): The ID of the spreadsheet. Required.
        action (str): "add" or "delete". Required.
        range_name (Optional[str]): A1-style range to protect (for "add").
        description (Optional[str]): Description of the protection (for "add").
        protected_range_id (Optional[int]): ID of the protected range (for "delete").
        warning_only (bool): Show warning instead of blocking edits. Defaults to False.

    Returns:
        str: Confirmation of the protection operation.
    """
    logger.info(
        "[manage_protected_range] Invoked. Email: '%s', Spreadsheet: %s, Action: %s",
        user_google_email,
        spreadsheet_id,
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
                spreadsheetId=spreadsheet_id,
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
            .batchUpdate(spreadsheetId=spreadsheet_id, body=request_body)
            .execute
        )

        pr_id = response["replies"][0]["addProtectedRange"]["protectedRange"]["protectedRangeId"]
        mode = "warning only" if warning_only else "protected"
        return (
            f"Added {mode} range (ID: {pr_id}) on '{range_name}' "
            f"in spreadsheet {spreadsheet_id} for {user_google_email}."
        )

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
            .batchUpdate(spreadsheetId=spreadsheet_id, body=request_body)
            .execute
        )

        return (
            f"Deleted protected range (ID: {protected_range_id}) "
            f"from spreadsheet {spreadsheet_id} for {user_google_email}."
        )


# Create comment management tools for sheets
_comment_tools = create_comment_tools("spreadsheet", "spreadsheet_id")

# Extract and register the functions
read_sheet_comments = _comment_tools["read_comments"]
create_sheet_comment = _comment_tools["create_comment"]
reply_to_sheet_comment = _comment_tools["reply_to_comment"]
resolve_sheet_comment = _comment_tools["resolve_comment"]
edit_sheet_comment = _comment_tools["edit_comment"]
delete_sheet_comment = _comment_tools["delete_comment"]
edit_sheet_comment_reply = _comment_tools["edit_reply"]
delete_sheet_comment_reply = _comment_tools["delete_reply"]
