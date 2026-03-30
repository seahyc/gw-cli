"""
Google Sheets service layer.

Synchronous wrappers around the Google Sheets API.  Every public function
accepts an already-authenticated ``service`` (or ``drive_service``) object
built by :func:`gw.auth.get_service`.
"""

import copy
import json
import logging
from typing import Any, Dict, List, Literal, Optional, Union

from gw.services._helpers.sheets_helpers import (
    CONDITION_TYPES,
    _a1_range_for_values,
    _build_boolean_rule,
    _build_gradient_rule,
    _fetch_detailed_sheet_errors,
    _fetch_sheets_with_rules,
    _format_a1_cell,
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

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CELL_FACETS = {
    "value",
    "formatted_value",
    "format",
    "effective_format",
    "text_runs",
    "hyperlinks",
    "notes",
    "validation",
    "chips",
}

MUTABLE_CELL_FIELDS = [
    "userEnteredValue",
    "userEnteredFormat",
    "textFormatRuns",
    "note",
    "dataValidation",
]

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


class UserInputError(Exception):
    """Raised when user-provided input is invalid."""


def _normalize_json_input(value: Any, field_name: str) -> Any:
    """Parse a JSON-encoded argument when needed."""
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError as exc:
            raise UserInputError(f"Invalid JSON for '{field_name}': {exc}") from exc
    return value


def _normalize_facets(facets: Optional[Union[str, List[str]]]) -> List[str]:
    parsed = _normalize_json_input(facets, "facets")
    if parsed is None:
        return ["value", "formatted_value", "format", "text_runs", "notes", "hyperlinks"]
    if not isinstance(parsed, list):
        raise UserInputError("facets must be a list of facet names.")

    normalized: List[str] = []
    for item in parsed:
        if not isinstance(item, str):
            raise UserInputError("Each facet must be a string.")
        facet = item.strip().lower()
        if facet not in CELL_FACETS:
            raise UserInputError(
                f"Unsupported facet '{item}'. Allowed: {sorted(CELL_FACETS)}."
            )
        if facet not in normalized:
            normalized.append(facet)

    if "text_runs" in normalized and "formatted_value" not in normalized:
        normalized.append("formatted_value")
    return normalized


def _cell_fields_for_facets(facets: List[str]) -> str:
    cell_fields: List[str] = []
    if "value" in facets:
        cell_fields.extend(["userEnteredValue", "effectiveValue"])
    if "formatted_value" in facets:
        cell_fields.append("formattedValue")
    if "format" in facets:
        cell_fields.append("userEnteredFormat")
    if "effective_format" in facets:
        cell_fields.append("effectiveFormat")
    if "text_runs" in facets:
        cell_fields.extend(["textFormatRuns", "userEnteredValue", "formattedValue"])
    if "hyperlinks" in facets:
        cell_fields.append("hyperlink")
    if "notes" in facets:
        cell_fields.append("note")
    if "validation" in facets:
        cell_fields.append("dataValidation")
    if "chips" in facets:
        cell_fields.append("chipRuns")

    joined = ",".join(dict.fromkeys(cell_fields))
    return (
        "sheets(properties(title,sheetId),"
        f"data(startRow,startColumn,rowData(values({joined}))))"
    )


def _scalar_from_extended_value(value: Optional[dict]) -> Any:
    if not value:
        return None
    for key in (
        "stringValue",
        "numberValue",
        "boolValue",
        "formulaValue",
        "errorValue",
    ):
        if key in value:
            return value[key]
    return None


def _derive_text_segments(text: str, text_format_runs: List[dict]) -> List[dict]:
    if not text_format_runs:
        return []

    sorted_runs = sorted(
        [run for run in text_format_runs if isinstance(run, dict)],
        key=lambda run: int(run.get("startIndex", 0)),
    )
    segments: List[dict] = []
    for idx, run in enumerate(sorted_runs):
        start = int(run.get("startIndex", 0))
        end = (
            int(sorted_runs[idx + 1].get("startIndex", len(text)))
            if idx + 1 < len(sorted_runs)
            else len(text)
        )
        segments.append(
            {
                "start": start,
                "end": end,
                "text": text[start:end],
                "format": copy.deepcopy(run.get("format", {})),
            }
        )
    return segments


def _cell_has_payload(cell: dict) -> bool:
    for key in (
        "userEnteredValue",
        "effectiveValue",
        "formattedValue",
        "userEnteredFormat",
        "effectiveFormat",
        "textFormatRuns",
        "hyperlink",
        "note",
        "dataValidation",
        "chipRuns",
    ):
        if key in cell and cell.get(key) not in (None, "", [], {}):
            return True
    return False


def _serialize_grid_cell(
    *,
    cell: dict,
    sheet_title: str,
    row_index: int,
    col_index: int,
    facets: List[str],
) -> dict:
    result: Dict[str, Any] = {
        "a1": _format_a1_cell(sheet_title, row_index, col_index),
        "row": row_index + 1,
        "column": col_index + 1,
    }

    if "value" in facets:
        user_value = copy.deepcopy(cell.get("userEnteredValue"))
        effective_value = copy.deepcopy(cell.get("effectiveValue"))
        result["userEnteredValue"] = user_value
        result["effectiveValue"] = effective_value
        result["value"] = _scalar_from_extended_value(user_value or effective_value)
    if "formatted_value" in facets:
        result["formattedValue"] = cell.get("formattedValue")
    if "format" in facets:
        result["userEnteredFormat"] = copy.deepcopy(cell.get("userEnteredFormat"))
    if "effective_format" in facets:
        result["effectiveFormat"] = copy.deepcopy(cell.get("effectiveFormat"))
    if "hyperlinks" in facets and "hyperlink" in cell:
        result["hyperlink"] = cell.get("hyperlink")
    if "notes" in facets and "note" in cell:
        result["note"] = cell.get("note")
    if "validation" in facets and "dataValidation" in cell:
        result["dataValidation"] = copy.deepcopy(cell.get("dataValidation"))
    if "chips" in facets and "chipRuns" in cell:
        result["chipRuns"] = copy.deepcopy(cell.get("chipRuns"))
    if "text_runs" in facets:
        raw_runs = copy.deepcopy(cell.get("textFormatRuns") or [])
        result["textFormatRuns"] = raw_runs
        text_value = (
            ((cell.get("userEnteredValue") or {}).get("stringValue"))
            or cell.get("formattedValue")
            or ""
        )
        result["text"] = text_value
        result["segments"] = _derive_text_segments(text_value, raw_runs)

    return result


def _coerce_extended_value(value: Any) -> dict:
    if isinstance(value, dict):
        known = {
            "stringValue",
            "numberValue",
            "boolValue",
            "formulaValue",
            "errorValue",
        }
        if known.intersection(value.keys()):
            return copy.deepcopy(value)
        raise UserInputError(
            "Value dicts must use Sheets ExtendedValue keys like stringValue or formulaValue."
        )
    if isinstance(value, bool):
        return {"boolValue": value}
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return {"numberValue": value}
    if value is None:
        return {}
    return {"stringValue": str(value)}


def _coerce_text_run(run: dict, text_length: int) -> dict:
    if not isinstance(run, dict):
        raise UserInputError("Each text run must be an object.")

    if "startIndex" in run and "format" in run:
        return {
            "startIndex": int(run["startIndex"]),
            "format": copy.deepcopy(run["format"]),
        }

    start = run.get("from", run.get("start"))
    end = run.get("to", run.get("end"))
    if start is None:
        raise UserInputError("Friendly text runs require 'from' or 'start'.")
    start_int = int(start)
    end_int = int(end) if end is not None else text_length
    if start_int < 0 or end_int < start_int:
        raise UserInputError("Invalid text run bounds.")
    format_obj = copy.deepcopy(run.get("format") or {})
    return {"startIndex": start_int, "endIndex": end_int, "format": format_obj}


def _normalize_text_runs_input(runs: Any, text_length: int) -> List[dict]:
    parsed = _normalize_json_input(runs, "text_runs")
    if parsed is None:
        return []
    if not isinstance(parsed, list):
        raise UserInputError("text_runs must be a list.")

    normalized = [_coerce_text_run(run, text_length) for run in parsed]
    normalized.sort(key=lambda run: int(run.get("startIndex", 0)))
    return [
        {"startIndex": int(run["startIndex"]), "format": copy.deepcopy(run["format"])}
        for run in normalized
    ]


def _friendly_patch_to_cell(patch: dict) -> tuple[dict, List[str]]:
    if not isinstance(patch, dict):
        raise UserInputError("Each cell patch must be an object.")

    cell = copy.deepcopy(patch.get("cell") or {})
    if cell and not isinstance(cell, dict):
        raise UserInputError("'cell' must be an object when provided.")

    text = patch.get("text")
    if text is not None:
        cell["userEnteredValue"] = {"stringValue": str(text)}

    if "value" in patch:
        cell["userEnteredValue"] = _coerce_extended_value(patch["value"])
    if "formula" in patch:
        cell["userEnteredValue"] = {"formulaValue": str(patch["formula"])}
    if "note" in patch:
        cell["note"] = patch["note"]
    if "base_format" in patch:
        cell["userEnteredFormat"] = copy.deepcopy(patch["base_format"])
    if "userEnteredFormat" in patch:
        cell["userEnteredFormat"] = copy.deepcopy(patch["userEnteredFormat"])
    if "dataValidation" in patch:
        cell["dataValidation"] = copy.deepcopy(patch["dataValidation"])

    run_key = "text_runs" if "text_runs" in patch else "runs" if "runs" in patch else None
    if run_key:
        current_text = (
            ((cell.get("userEnteredValue") or {}).get("stringValue"))
            or str(text or "")
        )
        cell["textFormatRuns"] = _normalize_text_runs_input(
            patch.get(run_key), len(current_text)
        )

    if patch.get("clear_text_runs"):
        cell["textFormatRuns"] = []
    if patch.get("clear_note"):
        cell["note"] = None

    fields = [field for field in MUTABLE_CELL_FIELDS if field in cell]
    return cell, fields


def _field_mask_for_patch(fields: List[str], mode: str) -> str:
    if mode == "replace":
        return ",".join(MUTABLE_CELL_FIELDS)
    ordered = [field for field in MUTABLE_CELL_FIELDS if field in fields]
    if not ordered:
        raise UserInputError("Cell patch did not specify any mutable fields.")
    return ",".join(ordered)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def list_spreadsheets(
    service,
    max_results: int = 25,
) -> str:
    """List spreadsheets the user has access to.

    *service* must be a **drive** service (not sheets).
    """
    logger.info("[list_spreadsheets] Invoked.")

    files_response = (
        service.files()
        .list(
            q="mimeType='application/vnd.google-apps.spreadsheet'",
            pageSize=max_results,
            fields="files(id,name,modifiedTime,webViewLink)",
            orderBy="modifiedTime desc",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        .execute()
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

    logger.info(f"Successfully listed {len(files)} spreadsheets.")
    return text_output


def get_spreadsheet_info(
    service,
    file_id: str,
) -> str:
    """Get spreadsheet metadata including title, locale, and sheet list with conditional formats."""
    logger.info(
        "[get_spreadsheet_info] Invoked. Spreadsheet ID: %s", file_id
    )

    spreadsheet = (
        service.spreadsheets()
        .get(
            spreadsheetId=file_id,
            fields="spreadsheetId,properties(title,locale),sheets(properties(title,sheetId,gridProperties(rowCount,columnCount)),conditionalFormats)",
        )
        .execute()
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

    logger.info("Successfully retrieved info for spreadsheet %s.", file_id)
    return text_output


def get_sheet_cells(
    service,
    file_id: str,
    range_name: str = "A1:Z1000",
    facets: Optional[Union[str, List[str]]] = None,
    include_empty: bool = False,
) -> str:
    """Read structured CellData for a range."""
    selected_facets = _normalize_facets(facets)
    logger.info(
        "[get_sheet_cells] Invoked. Spreadsheet: %s, Range: %s, Facets: %s",
        file_id,
        range_name,
        selected_facets,
    )

    response = (
        service.spreadsheets()
        .get(
            spreadsheetId=file_id,
            ranges=[range_name],
            includeGridData=True,
            fields=_cell_fields_for_facets(selected_facets),
        )
        .execute()
    )

    payload = {
        "spreadsheetId": file_id,
        "requestedRange": range_name,
        "facets": selected_facets,
        "sheets": [],
    }

    for sheet in response.get("sheets", []) or []:
        sheet_title = sheet.get("properties", {}).get("title") or "Unknown"
        row_groups: List[List[dict]] = []
        for grid in sheet.get("data", []) or []:
            start_row = int(grid.get("startRow", 0) or 0)
            start_col = int(grid.get("startColumn", 0) or 0)
            for row_offset, row_data in enumerate(grid.get("rowData", []) or []):
                current_row: List[dict] = []
                for col_offset, cell in enumerate(row_data.get("values", []) or []):
                    if not include_empty and not _cell_has_payload(cell):
                        continue
                    current_row.append(
                        _serialize_grid_cell(
                            cell=cell,
                            sheet_title=sheet_title,
                            row_index=start_row + row_offset,
                            col_index=start_col + col_offset,
                            facets=selected_facets,
                        )
                    )
                if current_row or include_empty:
                    row_groups.append(current_row)

        payload["sheets"].append({"title": sheet_title, "rows": row_groups})

    return json.dumps(payload, ensure_ascii=True, indent=2)


def update_sheet_cells(
    service,
    file_id: str,
    cells: Union[str, List[dict]],
    mode: Literal["patch", "replace"] = "patch",
) -> str:
    """Apply CellData patches to cells."""
    parsed_cells = _normalize_json_input(cells, "cells")
    if not isinstance(parsed_cells, list) or not parsed_cells:
        raise UserInputError("cells must be a non-empty list of cell patch objects.")
    if mode not in {"patch", "replace"}:
        raise UserInputError("mode must be either 'patch' or 'replace'.")

    logger.info(
        "[update_sheet_cells] Invoked. Spreadsheet: %s, Mode: %s, Patches: %s",
        file_id,
        mode,
        len(parsed_cells),
    )

    metadata = (
        service.spreadsheets()
        .get(
            spreadsheetId=file_id,
            fields="sheets(properties(sheetId,title))",
        )
        .execute()
    )
    sheets = metadata.get("sheets", []) or []

    requests = []
    touched_cells: List[str] = []
    for patch in parsed_cells:
        if not isinstance(patch, dict):
            raise UserInputError("Each item in cells must be an object.")

        a1 = patch.get("a1") or patch.get("range_name")
        if not isinstance(a1, str) or not a1.strip():
            raise UserInputError("Each cell patch must include a non-empty 'a1'.")

        grid_range = _parse_a1_range(a1, sheets)
        start_row = grid_range.get("startRowIndex")
        start_col = grid_range.get("startColumnIndex")
        end_row = grid_range.get("endRowIndex")
        end_col = grid_range.get("endColumnIndex")
        if None in (start_row, start_col, end_row, end_col):
            raise UserInputError(
                f"Patch target '{a1}' must resolve to a single cell, not an open-ended range."
            )
        if (end_row - start_row) != 1 or (end_col - start_col) != 1:
            raise UserInputError(
                f"Patch target '{a1}' must reference exactly one cell."
            )

        cell_payload, patch_fields = _friendly_patch_to_cell(patch)
        field_mask = _field_mask_for_patch(patch_fields, mode)
        requests.append(
            {
                "updateCells": {
                    "range": grid_range,
                    "rows": [{"values": [cell_payload]}],
                    "fields": field_mask,
                }
            }
        )
        touched_cells.append(a1)

    (
        service.spreadsheets()
        .batchUpdate(spreadsheetId=file_id, body={"requests": requests})
        .execute()
    )

    return (
        f"Updated {len(touched_cells)} cell(s) in patch mode '{mode}': "
        + ", ".join(touched_cells)
    )


def transform_sheet_cells(
    service,
    file_id: str,
    range_name: str,
    operations: Union[str, List[dict]],
) -> str:
    """Read a range, apply declarative text/note transformations, and write affected cells back."""
    parsed_ops = _normalize_json_input(operations, "operations")
    if not isinstance(parsed_ops, list) or not parsed_ops:
        raise UserInputError(
            "operations must be a non-empty list of transformation objects."
        )

    raw_cells_json = get_sheet_cells(
        service=service,
        file_id=file_id,
        range_name=range_name,
        facets=["value", "formatted_value", "format", "text_runs", "notes"],
        include_empty=False,
    )
    snapshot = json.loads(raw_cells_json)

    patches: List[dict] = []
    for sheet in snapshot.get("sheets", []):
        for row in sheet.get("rows", []):
            for cell in row:
                working = {
                    "a1": cell["a1"],
                    "text": cell.get("text")
                    if isinstance(cell.get("text"), str)
                    else (
                        cell.get("value")
                        if isinstance(cell.get("value"), str)
                        else None
                    ),
                    "base_format": copy.deepcopy(cell.get("userEnteredFormat") or {}),
                    "runs": copy.deepcopy(cell.get("textFormatRuns") or []),
                    "note": cell.get("note"),
                }
                changed = False

                for operation in parsed_ops:
                    if not isinstance(operation, dict):
                        raise UserInputError("Each operation must be an object.")
                    op_type = operation.get("type")
                    if op_type == "set_text":
                        working["text"] = str(operation.get("text", ""))
                        changed = True
                    elif op_type == "replace_text":
                        target = operation.get("find")
                        if not isinstance(target, str) or target == "":
                            raise UserInputError(
                                "replace_text requires a non-empty 'find' string."
                            )
                        replacement = str(operation.get("replace", ""))
                        text_value = working.get("text")
                        if isinstance(text_value, str) and target in text_value:
                            working["text"] = text_value.replace(target, replacement)
                            changed = True
                    elif op_type == "clear_runs":
                        if working.get("runs"):
                            working["runs"] = []
                            changed = True
                    elif op_type == "apply_run_format":
                        match_text = operation.get("match")
                        fmt = operation.get("format")
                        text_value = working.get("text")
                        if not isinstance(fmt, dict):
                            raise UserInputError(
                                "apply_run_format requires a format object."
                            )
                        if (
                            isinstance(match_text, str)
                            and match_text
                            and isinstance(text_value, str)
                            and text_value
                        ):
                            start = text_value.find(match_text)
                            if start >= 0:
                                runs = working.get("runs") or []
                                runs.append(
                                    {
                                        "startIndex": start,
                                        "format": copy.deepcopy(fmt),
                                    }
                                )
                                working["runs"] = sorted(
                                    runs,
                                    key=lambda run: int(run.get("startIndex", 0)),
                                )
                                changed = True
                    elif op_type == "set_note":
                        working["note"] = str(operation.get("note", ""))
                        changed = True
                    elif op_type == "clear_note":
                        if working.get("note") is not None:
                            working["note"] = None
                            changed = True
                    else:
                        raise UserInputError(
                            f"Unsupported transform operation '{op_type}'."
                        )

                if changed:
                    patch: Dict[str, Any] = {"a1": working["a1"]}
                    if working.get("text") is not None:
                        patch["text"] = working["text"]
                    patch["base_format"] = working["base_format"]
                    patch["runs"] = working.get("runs") or []
                    patch["note"] = working.get("note")
                    patches.append(patch)

    if not patches:
        return f"No cells changed in {range_name}."

    return update_sheet_cells(
        service=service,
        file_id=file_id,
        cells=patches,
        mode="patch",
    )


def read_values(
    service,
    file_id: str,
    range_name: str = "A1:Z1000",
    value_render_option: Literal[
        "FORMATTED_VALUE", "UNFORMATTED_VALUE", "FORMULA"
    ] = "FORMATTED_VALUE",
) -> str:
    """Read cell values from a range."""
    logger.info(
        "[read_values] Invoked. Spreadsheet: %s, Range: %s, ValueRenderOption: %s",
        file_id,
        range_name,
        value_render_option,
    )

    result = (
        service.spreadsheets()
        .values()
        .get(
            spreadsheetId=file_id,
            range=range_name,
            valueRenderOption=value_render_option,
        )
        .execute()
    )

    values = result.get("values", [])
    if not values:
        return f"No data found in range '{range_name}'."

    detailed_errors_section = ""
    if _values_contain_sheets_errors(values):
        resolved_range = result.get("range", range_name)
        detailed_range = _a1_range_for_values(resolved_range, values) or resolved_range
        try:
            errors = _fetch_detailed_sheet_errors(service, file_id, detailed_range)
            detailed_errors_section = _format_sheet_error_section(
                errors=errors, range_label=detailed_range
            )
        except Exception as exc:
            logger.warning(
                "[read_values] Failed fetching detailed error messages for range '%s': %s",
                detailed_range,
                exc,
            )

    # Format as TSV-like output, stripping trailing empty cells
    formatted_rows = []
    for row in values:
        stripped = list(row)
        while stripped and stripped[-1] == "":
            stripped.pop()
        formatted_rows.append("\t".join(str(cell) for cell in stripped))

    text_output = (
        "\n".join(formatted_rows[:50])
        + (f"\n... and {len(values) - 50} more rows" if len(values) > 50 else "")
    )

    logger.info("Successfully read %d rows.", len(values))
    return text_output + detailed_errors_section


def batch_read_values(
    service,
    file_id: str,
    ranges: Union[str, List[str]],
    value_render_option: str = "FORMATTED_VALUE",
) -> str:
    """Read values from multiple ranges in one request."""
    logger.info(
        "[batch_read_values] Invoked. Spreadsheet: %s, Ranges: %s",
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

    result = (
        service.spreadsheets()
        .values()
        .batchGet(
            spreadsheetId=file_id,
            ranges=ranges,
            valueRenderOption=value_render_option,
        )
        .execute()
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

    logger.info("Successfully batch-read %d ranges.", len(value_ranges))
    return "".join(output_parts).lstrip("\n")


def modify_values(
    service,
    file_id: str,
    range_name: str,
    values: Optional[Union[str, List[List[str]]]] = None,
    value_input_option: str = "USER_ENTERED",
    clear_values: bool = False,
) -> str:
    """Write, update, or clear values in a range."""
    operation = "clear" if clear_values else "write"
    logger.info(
        "[modify_values] Invoked. Operation: %s, Spreadsheet: %s, Range: %s",
        operation,
        file_id,
        range_name,
    )

    # Parse values if it's a JSON string
    if values is not None and isinstance(values, str):
        try:
            parsed_values = json.loads(values)
            if not isinstance(parsed_values, list):
                raise ValueError(
                    f"Values must be a list, got {type(parsed_values).__name__}"
                )
            for i, row in enumerate(parsed_values):
                if not isinstance(row, list):
                    raise ValueError(
                        f"Row {i} must be a list, got {type(row).__name__}"
                    )
            values = parsed_values
            logger.info(
                "[modify_values] Parsed JSON string to Python list with %d rows",
                len(values),
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
        result = (
            service.spreadsheets()
            .values()
            .clear(spreadsheetId=file_id, range=range_name)
            .execute()
        )

        cleared_range = result.get("clearedRange", range_name)
        text_output = f"Cleared {cleared_range}."
        logger.info("Successfully cleared range '%s'.", cleared_range)
    else:
        body = {"values": values}

        result = (
            service.spreadsheets()
            .values()
            .update(
                spreadsheetId=file_id,
                range=range_name,
                valueInputOption=value_input_option,
                includeValuesInResponse=True,
                responseValueRenderOption="FORMATTED_VALUE",
                body=body,
            )
            .execute()
        )

        updated_cells = result.get("updatedCells", 0)

        detailed_errors_section = ""
        updated_data = result.get("updatedData") or {}
        updated_values = updated_data.get("values", []) or []
        if updated_values and _values_contain_sheets_errors(updated_values):
            updated_range = result.get("updatedRange", range_name)
            detailed_range = (
                _a1_range_for_values(updated_range, updated_values) or updated_range
            )
            try:
                errors = _fetch_detailed_sheet_errors(
                    service, file_id, detailed_range
                )
                detailed_errors_section = _format_sheet_error_section(
                    errors=errors, range_label=detailed_range
                )
            except Exception as exc:
                logger.warning(
                    "[modify_values] Failed fetching detailed error messages for range '%s': %s",
                    detailed_range,
                    exc,
                )

        text_output = f"Updated {updated_cells} cells in {range_name}."
        text_output += detailed_errors_section
        logger.info("Successfully updated %d cells.", updated_cells)

    return text_output


def format_range(
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
) -> str:
    """Apply formatting to a range. Colors as #RRGGBB."""
    logger.info(
        "[format_range] Invoked. Spreadsheet: %s, Range: %s",
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

    metadata = (
        service.spreadsheets()
        .get(
            spreadsheetId=file_id,
            fields="sheets(properties(sheetId,title))",
        )
        .execute()
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

    (
        service.spreadsheets()
        .batchUpdate(spreadsheetId=file_id, body=request_body)
        .execute()
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


def add_conditional_formatting(
    service,
    file_id: str,
    range_name: str,
    condition_type: str,
    condition_values: Optional[Union[str, List[Union[str, int, float]]]] = None,
    background_color: Optional[str] = None,
    text_color: Optional[str] = None,
    rule_index: Optional[int] = None,
    gradient_points: Optional[Union[str, List[dict]]] = None,
) -> str:
    """Add a conditional formatting rule."""
    logger.info(
        "[add_conditional_formatting] Invoked. Spreadsheet: %s, Range: %s, Type: %s",
        file_id,
        range_name,
        condition_type,
    )

    if rule_index is not None and (not isinstance(rule_index, int) or rule_index < 0):
        raise UserInputError("rule_index must be a non-negative integer when provided.")

    condition_values_list = _parse_condition_values(condition_values)
    gradient_points_list = _parse_gradient_points(gradient_points)

    sheets, sheet_titles = _fetch_sheets_with_rules(service, file_id)
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

    (
        service.spreadsheets()
        .batchUpdate(spreadsheetId=file_id, body=request_body)
        .execute()
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


def update_conditional_formatting(
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
) -> str:
    """Update an existing conditional formatting rule by index."""
    logger.info(
        "[update_conditional_formatting] Invoked. Spreadsheet: %s, Range: %s, Rule Index: %s",
        file_id,
        range_name,
        rule_index,
    )

    if not isinstance(rule_index, int) or rule_index < 0:
        raise UserInputError("rule_index must be a non-negative integer.")

    condition_values_list = _parse_condition_values(condition_values)
    gradient_points_list = _parse_gradient_points(gradient_points)

    sheets, sheet_titles = _fetch_sheets_with_rules(service, file_id)

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

    (
        service.spreadsheets()
        .batchUpdate(spreadsheetId=file_id, body=request_body)
        .execute()
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


def delete_conditional_formatting(
    service,
    file_id: str,
    rule_index: int,
    sheet_name: Optional[str] = None,
) -> str:
    """Delete a conditional formatting rule by index."""
    logger.info(
        "[delete_conditional_formatting] Invoked. Spreadsheet: %s, Sheet: %s, Rule Index: %s",
        file_id,
        sheet_name,
        rule_index,
    )

    if not isinstance(rule_index, int) or rule_index < 0:
        raise UserInputError("rule_index must be a non-negative integer.")

    sheets, sheet_titles = _fetch_sheets_with_rules(service, file_id)
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

    (
        service.spreadsheets()
        .batchUpdate(spreadsheetId=file_id, body=request_body)
        .execute()
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


def create_spreadsheet(
    service,
    title: str,
    sheet_names: Optional[List[str]] = None,
) -> str:
    """Create a new spreadsheet with optional sheet names."""
    logger.info("[create_spreadsheet] Invoked. Title: %s", title)

    spreadsheet_body = {"properties": {"title": title}}

    if sheet_names:
        spreadsheet_body["sheets"] = [
            {"properties": {"title": sheet_name}} for sheet_name in sheet_names
        ]

    spreadsheet = (
        service.spreadsheets()
        .create(
            body=spreadsheet_body,
            fields="spreadsheetId,spreadsheetUrl,properties(title,locale)",
        )
        .execute()
    )

    properties = spreadsheet.get("properties", {})
    file_id = spreadsheet.get("spreadsheetId")
    spreadsheet_url = spreadsheet.get("spreadsheetUrl")
    locale = properties.get("locale", "Unknown")

    text_output = (
        f"Created spreadsheet '{title}'. "
        f"ID: {file_id} | URL: {spreadsheet_url} | Locale: {locale}"
    )

    logger.info("Successfully created spreadsheet. ID: %s", file_id)
    return text_output


def create_sheet(
    service,
    file_id: str,
    sheet_name: str,
) -> str:
    """Create a new sheet tab within a spreadsheet."""
    logger.info(
        "[create_sheet] Invoked. Spreadsheet: %s, Sheet: %s", file_id, sheet_name
    )

    request_body = {"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]}

    response = (
        service.spreadsheets()
        .batchUpdate(spreadsheetId=file_id, body=request_body)
        .execute()
    )

    sheet_id = response["replies"][0]["addSheet"]["properties"]["sheetId"]

    text_output = f"Created sheet '{sheet_name}' (ID: {sheet_id})."
    logger.info("Successfully created sheet. Sheet ID: %d", sheet_id)
    return text_output


def update_borders(
    service,
    file_id: str,
    range_name: str,
    border_style: str = "SOLID",
    border_color: str = "#000000",
    borders: str = "all",
) -> str:
    """Apply borders to a range."""
    logger.info(
        "[update_borders] Invoked. Spreadsheet: %s, Range: %s", file_id, range_name
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

    metadata = (
        service.spreadsheets()
        .get(
            spreadsheetId=file_id,
            fields="sheets(properties(sheetId,title))",
        )
        .execute()
    )
    sheets = metadata.get("sheets", [])
    grid_range = _parse_a1_range(range_name, sheets)

    update_borders_req = {"range": grid_range}
    for key in border_keys:
        update_borders_req[key] = border_spec

    request_body = {"requests": [{"updateBorders": update_borders_req}]}

    (
        service.spreadsheets()
        .batchUpdate(spreadsheetId=file_id, body=request_body)
        .execute()
    )

    return f"Applied {border_style} borders ({borders}) to {range_name}."


def merge_cells(
    service,
    file_id: str,
    range_name: str,
    merge_type: str = "MERGE_ALL",
    unmerge: bool = False,
) -> str:
    """Merge or unmerge cells."""
    logger.info(
        "[merge_cells] Invoked. Spreadsheet: %s, Range: %s, Unmerge: %s",
        file_id,
        range_name,
        unmerge,
    )

    allowed_merge_types = {"MERGE_ALL", "MERGE_COLUMNS", "MERGE_ROWS"}
    if merge_type.upper() not in allowed_merge_types:
        raise UserInputError(f"merge_type must be one of {sorted(allowed_merge_types)}.")

    metadata = (
        service.spreadsheets()
        .get(
            spreadsheetId=file_id,
            fields="sheets(properties(sheetId,title))",
        )
        .execute()
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

    (
        service.spreadsheets()
        .batchUpdate(spreadsheetId=file_id, body=request_body)
        .execute()
    )

    return f"{action} cells in {range_name}."


def insert_dimension(
    service,
    file_id: str,
    sheet_id: int,
    dimension: str,
    start_index: int,
    end_index: int,
    inherit_from_before: bool = True,
) -> str:
    """Insert rows or columns. dimension: ROWS or COLUMNS."""
    logger.info(
        "[insert_dimension] Invoked. Spreadsheet: %s, Sheet: %d, Dim: %s, Start: %d, End: %d",
        file_id,
        sheet_id,
        dimension,
        start_index,
        end_index,
    )

    if dimension.upper() not in ("ROWS", "COLUMNS"):
        raise UserInputError("dimension must be 'ROWS' or 'COLUMNS'.")

    request_body = {
        "requests": [
            {
                "insertDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": dimension.upper(),
                        "startIndex": start_index,
                        "endIndex": end_index,
                    },
                    "inheritFromBefore": inherit_from_before,
                }
            }
        ]
    }

    (
        service.spreadsheets()
        .batchUpdate(spreadsheetId=file_id, body=request_body)
        .execute()
    )

    count = end_index - start_index
    return f"Inserted {count} {dimension.lower()} at index {start_index} (sheet {sheet_id})."


def delete_dimension(
    service,
    file_id: str,
    sheet_id: int,
    dimension: str,
    start_index: int,
    end_index: int,
) -> str:
    """Delete rows or columns. dimension: ROWS or COLUMNS. end_index is exclusive."""
    logger.info(
        "[delete_dimension] Invoked. Spreadsheet: %s, Sheet: %d, Dim: %s, Start: %d, End: %d",
        file_id,
        sheet_id,
        dimension,
        start_index,
        end_index,
    )

    if dimension.upper() not in ("ROWS", "COLUMNS"):
        raise UserInputError("dimension must be 'ROWS' or 'COLUMNS'.")
    if start_index >= end_index:
        raise UserInputError("start_index must be less than end_index.")

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

    (
        service.spreadsheets()
        .batchUpdate(spreadsheetId=file_id, body=request_body)
        .execute()
    )

    deleted_count = end_index - start_index
    return f"Deleted {deleted_count} {dimension.lower()} (index {start_index} to {end_index}) in sheet {sheet_id}."


def resize_dimension(
    service,
    file_id: str,
    sheet_id: int,
    dimension: str,
    start_index: int,
    end_index: int,
    pixel_size: int,
) -> str:
    """Resize rows or columns to a specific pixel size."""
    logger.info(
        "[resize_dimension] Invoked. Spreadsheet: %s, Sheet: %d, Dim: %s, Start: %d, End: %d, Size: %d",
        file_id,
        sheet_id,
        dimension,
        start_index,
        end_index,
        pixel_size,
    )

    if dimension.upper() not in ("ROWS", "COLUMNS"):
        raise UserInputError("dimension must be 'ROWS' or 'COLUMNS'.")

    request_body = {
        "requests": [
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": dimension.upper(),
                        "startIndex": start_index,
                        "endIndex": end_index,
                    },
                    "properties": {"pixelSize": pixel_size},
                    "fields": "pixelSize",
                }
            }
        ]
    }

    (
        service.spreadsheets()
        .batchUpdate(spreadsheetId=file_id, body=request_body)
        .execute()
    )

    return f"Resized {dimension.lower()} {start_index}-{end_index} to {pixel_size}px in sheet {sheet_id}."


def sort_range(
    service,
    file_id: str,
    range_name: str,
    sort_specs: Union[str, List[dict]],
) -> str:
    """Sort data in a range."""
    logger.info(
        "[sort_range] Invoked. Spreadsheet: %s, Range: %s", file_id, range_name
    )

    if isinstance(sort_specs, str):
        try:
            sort_specs = json.loads(sort_specs)
        except json.JSONDecodeError as e:
            raise UserInputError(f"sort_specs must be valid JSON: {e}")

    if not isinstance(sort_specs, list) or not sort_specs:
        raise UserInputError("sort_specs must be a non-empty list of sort specifications.")

    metadata = (
        service.spreadsheets()
        .get(
            spreadsheetId=file_id,
            fields="sheets(properties(sheetId,title))",
        )
        .execute()
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

    (
        service.spreadsheets()
        .batchUpdate(spreadsheetId=file_id, body=request_body)
        .execute()
    )

    specs_desc = ", ".join(
        f"col {s.get('column_index')} {s.get('order', 'ASCENDING')}" for s in sort_specs
    )
    return f"Sorted {range_name} by: {specs_desc}."


def set_data_validation(
    service,
    file_id: str,
    range_name: str,
    validation_type: str = "ONE_OF_LIST",
    values: Optional[Union[str, List]] = None,
    strict: bool = True,
    show_dropdown: bool = True,
    clear: bool = False,
) -> str:
    """Set or clear data validation."""
    logger.info(
        "[set_data_validation] Invoked. Spreadsheet: %s, Range: %s, Clear: %s",
        file_id,
        range_name,
        clear,
    )

    metadata = (
        service.spreadsheets()
        .get(
            spreadsheetId=file_id,
            fields="sheets(properties(sheetId,title))",
        )
        .execute()
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

        (
            service.spreadsheets()
            .batchUpdate(spreadsheetId=file_id, body=request_body)
            .execute()
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

    (
        service.spreadsheets()
        .batchUpdate(spreadsheetId=file_id, body=request_body)
        .execute()
    )

    values_desc = f" with values {values}" if values else ""
    return f"Set {validation_type} validation{values_desc} on {range_name}."


def delete_sheet(
    service,
    file_id: str,
    sheet_id: int,
) -> str:
    """Delete a sheet tab from a spreadsheet by ID."""
    logger.info(
        "[delete_sheet] Invoked. Spreadsheet: %s, Sheet ID: %d", file_id, sheet_id
    )

    request_body = {
        "requests": [{"deleteSheet": {"sheetId": sheet_id}}]
    }

    (
        service.spreadsheets()
        .batchUpdate(spreadsheetId=file_id, body=request_body)
        .execute()
    )

    return f"Deleted sheet (ID: {sheet_id})."


def auto_resize_dimensions(
    service,
    file_id: str,
    sheet_id: int,
    dimension: str = "COLUMNS",
    start_index: int = 0,
    end_index: Optional[int] = None,
) -> str:
    """Auto-resize rows or columns to fit content."""
    logger.info(
        "[auto_resize_dimensions] Invoked. Spreadsheet: %s, Sheet: %d, Dim: %s",
        file_id,
        sheet_id,
        dimension,
    )

    if dimension.upper() not in ("ROWS", "COLUMNS"):
        raise UserInputError("dimension must be 'ROWS' or 'COLUMNS'.")

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

    (
        service.spreadsheets()
        .batchUpdate(spreadsheetId=file_id, body=request_body)
        .execute()
    )

    range_desc = f"from index {start_index}" + (f" to {end_index}" if end_index else " to end")
    return f"Auto-resized {dimension.lower()} {range_desc} in sheet {sheet_id}."


def freeze_dimensions(
    service,
    file_id: str,
    sheet_id: int,
    frozen_rows: Optional[int] = None,
    frozen_columns: Optional[int] = None,
) -> str:
    """Freeze rows and/or columns. Set to 0 to unfreeze."""
    logger.info(
        "[freeze_dimensions] Invoked. Spreadsheet: %s, Sheet: %d, Rows: %s, Cols: %s",
        file_id,
        sheet_id,
        frozen_rows,
        frozen_columns,
    )

    if frozen_rows is None and frozen_columns is None:
        raise UserInputError("Provide at least one of frozen_rows or frozen_columns.")

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

    (
        service.spreadsheets()
        .batchUpdate(spreadsheetId=file_id, body=request_body)
        .execute()
    )

    parts = []
    if frozen_rows is not None:
        parts.append(f"{frozen_rows} rows")
    if frozen_columns is not None:
        parts.append(f"{frozen_columns} columns")

    return f"Froze {' and '.join(parts)} in sheet {sheet_id}."


def duplicate_sheet(
    service,
    file_id: str,
    source_sheet_id: int,
    new_name: Optional[str] = None,
    insert_index: Optional[int] = None,
) -> str:
    """Duplicate a sheet tab within a spreadsheet."""
    logger.info(
        "[duplicate_sheet] Invoked. Spreadsheet: %s, Source: %d, NewName: %s",
        file_id,
        source_sheet_id,
        new_name,
    )

    dup_request: Dict[str, Any] = {
        "sourceSheetId": source_sheet_id,
    }
    if new_name is not None:
        dup_request["newSheetName"] = new_name
    if insert_index is not None:
        dup_request["insertSheetIndex"] = insert_index

    request_body = {"requests": [{"duplicateSheet": dup_request}]}

    response = (
        service.spreadsheets()
        .batchUpdate(spreadsheetId=file_id, body=request_body)
        .execute()
    )

    new_sheet_id = response["replies"][0]["duplicateSheet"]["properties"]["sheetId"]
    new_sheet_title = response["replies"][0]["duplicateSheet"]["properties"]["title"]

    return f"Duplicated sheet {source_sheet_id} as '{new_sheet_title}' (ID: {new_sheet_id})."


def update_sheet_properties(
    service,
    file_id: str,
    sheet_id: int,
    new_name: Optional[str] = None,
    tab_color: Optional[str] = None,
    hidden: Optional[bool] = None,
    right_to_left: Optional[bool] = None,
    index: Optional[int] = None,
) -> str:
    """Update sheet tab properties (rename, color, visibility, order)."""
    logger.info(
        "[update_sheet_properties] Invoked. Spreadsheet: %s, Sheet: %d", file_id, sheet_id
    )

    if not any([new_name, tab_color, hidden is not None, right_to_left is not None, index is not None]):
        raise UserInputError(
            "Provide at least one property to update (new_name, tab_color, hidden, right_to_left, index)."
        )

    properties: Dict[str, Any] = {"sheetId": sheet_id}
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

    (
        service.spreadsheets()
        .batchUpdate(spreadsheetId=file_id, body=request_body)
        .execute()
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

    return f"Updated sheet {sheet_id}: {', '.join(changes)}."


def find_replace(
    service,
    file_id: str,
    find: str,
    replacement: str,
    sheet_id: Optional[int] = None,
    match_case: bool = False,
    match_entire_cell: bool = False,
    search_by_regex: bool = False,
) -> str:
    """Find and replace text in a spreadsheet."""
    logger.info(
        "[find_replace] Invoked. Spreadsheet: %s, Find: '%s', Replace: '%s'",
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

    if sheet_id is not None:
        find_replace_req["sheetId"] = sheet_id
    else:
        find_replace_req["allSheets"] = True

    request_body = {"requests": [{"findReplace": find_replace_req}]}

    response = (
        service.spreadsheets()
        .batchUpdate(spreadsheetId=file_id, body=request_body)
        .execute()
    )

    reply = response.get("replies", [{}])[0].get("findReplace", {})
    occurrences = reply.get("occurrencesChanged", 0)
    rows_changed = reply.get("rowsChanged", 0)
    sheets_changed = reply.get("sheetsChanged", 0)

    scope = f"sheet {sheet_id}" if sheet_id is not None else "all sheets"
    return (
        f"Replaced '{find}' with '{replacement}' in {scope}: "
        f"{occurrences} occurrences in {rows_changed} rows across {sheets_changed} sheets."
    )


def manage_named_ranges(
    service,
    file_id: str,
    action: str,
    name: str,
    range_name: Optional[str] = None,
    named_range_id: Optional[str] = None,
) -> str:
    """Add, update, or delete named ranges. action: add, update, delete."""
    logger.info(
        "[manage_named_ranges] Invoked. Spreadsheet: %s, Action: %s, Name: %s",
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

        metadata = (
            service.spreadsheets()
            .get(
                spreadsheetId=file_id,
                fields="sheets(properties(sheetId,title))",
            )
            .execute()
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

        response = (
            service.spreadsheets()
            .batchUpdate(spreadsheetId=file_id, body=request_body)
            .execute()
        )

        nr_id = response["replies"][0]["addNamedRange"]["namedRange"]["namedRangeId"]
        return f"Added named range '{name}' (ID: {nr_id}) for {range_name}."

    elif action_lower == "update":
        if not named_range_id:
            raise UserInputError("named_range_id is required for 'update' action.")
        if not range_name:
            raise UserInputError("range_name is required for 'update' action.")

        metadata = (
            service.spreadsheets()
            .get(
                spreadsheetId=file_id,
                fields="sheets(properties(sheetId,title))",
            )
            .execute()
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

        (
            service.spreadsheets()
            .batchUpdate(spreadsheetId=file_id, body=request_body)
            .execute()
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

        (
            service.spreadsheets()
            .batchUpdate(spreadsheetId=file_id, body=request_body)
            .execute()
        )

        return f"Deleted named range '{name}' (ID: {named_range_id})."


def manage_filter_view(
    service,
    file_id: str,
    action: str,
    range_name: Optional[str] = None,
    title: Optional[str] = None,
) -> str:
    """Manage basic filters. action: add (set basic filter) or clear_basic (clear it)."""
    logger.info(
        "[manage_filter_view] Invoked. Spreadsheet: %s, Action: %s", file_id, action
    )

    action_lower = action.lower()
    if action_lower not in ("add", "clear_basic"):
        raise UserInputError("action must be 'add' or 'clear_basic'.")

    if action_lower == "add":
        if not range_name:
            raise UserInputError("range_name is required for 'add' action.")

        metadata = (
            service.spreadsheets()
            .get(
                spreadsheetId=file_id,
                fields="sheets(properties(sheetId,title))",
            )
            .execute()
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

        (
            service.spreadsheets()
            .batchUpdate(spreadsheetId=file_id, body=request_body)
            .execute()
        )

        return f"Set basic filter on {range_name}."

    else:  # clear_basic
        if not range_name:
            raise UserInputError("range_name is required for 'clear_basic' to identify the sheet.")

        metadata = (
            service.spreadsheets()
            .get(
                spreadsheetId=file_id,
                fields="sheets(properties(sheetId,title))",
            )
            .execute()
        )
        sheets = metadata.get("sheets", [])
        grid_range = _parse_a1_range(range_name, sheets)
        sheet_id = grid_range["sheetId"]

        request_body = {
            "requests": [
                {"clearBasicFilter": {"sheetId": sheet_id}}
            ]
        }

        (
            service.spreadsheets()
            .batchUpdate(spreadsheetId=file_id, body=request_body)
            .execute()
        )

        return "Cleared basic filter."


def manage_protected_range(
    service,
    file_id: str,
    action: str,
    range_name: Optional[str] = None,
    description: Optional[str] = None,
    protected_range_id: Optional[int] = None,
    warning_only: bool = False,
) -> str:
    """Add or delete protected ranges. action: add or delete."""
    logger.info(
        "[manage_protected_range] Invoked. Spreadsheet: %s, Action: %s", file_id, action
    )

    action_lower = action.lower()
    if action_lower not in ("add", "delete"):
        raise UserInputError("action must be 'add' or 'delete'.")

    if action_lower == "add":
        if not range_name:
            raise UserInputError("range_name is required for 'add' action.")

        metadata = (
            service.spreadsheets()
            .get(
                spreadsheetId=file_id,
                fields="sheets(properties(sheetId,title))",
            )
            .execute()
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

        response = (
            service.spreadsheets()
            .batchUpdate(spreadsheetId=file_id, body=request_body)
            .execute()
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

        (
            service.spreadsheets()
            .batchUpdate(spreadsheetId=file_id, body=request_body)
            .execute()
        )

        return f"Deleted protected range (ID: {protected_range_id})."
