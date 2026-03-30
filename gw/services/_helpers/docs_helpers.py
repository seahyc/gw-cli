"""
Google Docs Helper Functions

This module provides utility functions for common Google Docs operations
to simplify the implementation of document editing tools.
"""

import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


def _normalize_color(
    color: Optional[str], param_name: str
) -> Optional[Dict[str, float]]:
    """Normalize a hex color string (#RRGGBB) into Docs API rgbColor format."""
    if color is None:
        return None

    if not isinstance(color, str):
        raise ValueError(f"{param_name} must be a hex string like '#RRGGBB'")

    if len(color) != 7 or not color.startswith("#"):
        raise ValueError(f"{param_name} must be a hex string like '#RRGGBB'")

    hex_color = color[1:]
    if any(c not in "0123456789abcdefABCDEF" for c in hex_color):
        raise ValueError(f"{param_name} must be a hex string like '#RRGGBB'")

    r = int(hex_color[0:2], 16) / 255
    g = int(hex_color[2:4], 16) / 255
    b = int(hex_color[4:6], 16) / 255
    return {"red": r, "green": g, "blue": b}


def build_text_style(
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
) -> tuple[Dict[str, Any], list[str]]:
    """Build a (text_style_dict, field_names_list) tuple for the Docs API."""
    text_style = {}
    fields = []

    if bold is not None:
        text_style["bold"] = bold
        fields.append("bold")

    if italic is not None:
        text_style["italic"] = italic
        fields.append("italic")

    if underline is not None:
        text_style["underline"] = underline
        fields.append("underline")

    if strikethrough is not None:
        text_style["strikethrough"] = strikethrough
        fields.append("strikethrough")

    if superscript is not None and superscript:
        text_style["baselineOffset"] = "SUPERSCRIPT"
        fields.append("baselineOffset")
    elif subscript is not None and subscript:
        text_style["baselineOffset"] = "SUBSCRIPT"
        fields.append("baselineOffset")

    if font_size is not None:
        text_style["fontSize"] = {"magnitude": font_size, "unit": "PT"}
        fields.append("fontSize")

    if font_family is not None:
        text_style["weightedFontFamily"] = {"fontFamily": font_family}
        fields.append("weightedFontFamily")

    if text_color is not None:
        rgb = _normalize_color(text_color, "text_color")
        text_style["foregroundColor"] = {"color": {"rgbColor": rgb}}
        fields.append("foregroundColor")

    if background_color is not None:
        rgb = _normalize_color(background_color, "background_color")
        text_style["backgroundColor"] = {"color": {"rgbColor": rgb}}
        fields.append("backgroundColor")

    if link_url is not None:
        text_style["link"] = {"url": link_url}
        fields.append("link")

    return text_style, fields


def create_insert_text_request(index: int, text: str) -> Dict[str, Any]:
    """Create an insertText request for the Docs API."""
    return {"insertText": {"location": {"index": index}, "text": text}}


def create_insert_text_segment_request(
    index: int, text: str, segment_id: str
) -> Dict[str, Any]:
    """Create an insertText request with segmentId for headers/footers."""
    return {
        "insertText": {
            "location": {"segmentId": segment_id, "index": index},
            "text": text,
        }
    }


def create_delete_range_request(start_index: int, end_index: int) -> Dict[str, Any]:
    """Create a deleteContentRange request for the Docs API."""
    return {
        "deleteContentRange": {
            "range": {"startIndex": start_index, "endIndex": end_index}
        }
    }


def create_format_text_request(
    start_index: int,
    end_index: int,
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
) -> Optional[Dict[str, Any]]:
    """Create an updateTextStyle request for the Docs API. Returns None if no styles provided."""
    text_style, fields = build_text_style(
        bold, italic, underline, font_size, font_family, text_color, background_color,
        strikethrough, superscript, subscript, link_url,
    )

    if not text_style:
        return None

    return {
        "updateTextStyle": {
            "range": {"startIndex": start_index, "endIndex": end_index},
            "textStyle": text_style,
            "fields": ",".join(fields),
        }
    }


def create_find_replace_request(
    find_text: str, replace_text: str, match_case: bool = False
) -> Dict[str, Any]:
    """Create a replaceAllText request for the Docs API."""
    return {
        "replaceAllText": {
            "containsText": {"text": find_text, "matchCase": match_case},
            "replaceText": replace_text,
        }
    }


def create_insert_table_request(index: int, rows: int, columns: int) -> Dict[str, Any]:
    """Create an insertTable request for the Docs API."""
    return {
        "insertTable": {"location": {"index": index}, "rows": rows, "columns": columns}
    }


def create_insert_page_break_request(index: int) -> Dict[str, Any]:
    """Create an insertPageBreak request for the Docs API."""
    return {"insertPageBreak": {"location": {"index": index}}}


def create_insert_image_request(
    index: int, image_uri: str, width: int = None, height: int = None
) -> Dict[str, Any]:
    """Create an insertInlineImage request for the Docs API."""
    request = {"insertInlineImage": {"location": {"index": index}, "uri": image_uri}}

    # Add size properties if specified
    object_size = {}
    if width is not None:
        object_size["width"] = {"magnitude": width, "unit": "PT"}
    if height is not None:
        object_size["height"] = {"magnitude": height, "unit": "PT"}

    if object_size:
        request["insertInlineImage"]["objectSize"] = object_size

    return request


def create_bullet_list_request(
    start_index: int, end_index: int, list_type: str = "UNORDERED"
) -> Dict[str, Any]:
    """Create a createParagraphBullets request for the Docs API."""
    bullet_preset = (
        "BULLET_DISC_CIRCLE_SQUARE"
        if list_type == "UNORDERED"
        else "NUMBERED_DECIMAL_ALPHA_ROMAN"
    )

    return {
        "createParagraphBullets": {
            "range": {"startIndex": start_index, "endIndex": end_index},
            "bulletPreset": bullet_preset,
        }
    }


def validate_operation(operation: Dict[str, Any]) -> tuple[bool, str]:
    """Validate a batch operation dictionary. Returns (is_valid, error_message)."""
    op_type = operation.get("type")
    if not op_type:
        return False, "Missing 'type' field"

    # Validate required fields for each operation type
    required_fields = {
        "insert_text": ["index", "text"],
        "delete_text": ["start_index", "end_index"],
        "replace_text": ["start_index", "end_index", "text"],
        "format_text": ["start_index", "end_index"],
        "insert_table": ["index", "rows", "columns"],
        "insert_page_break": ["index"],
        "find_replace": ["find_text", "replace_text"],
        "update_paragraph_style": ["start_index", "end_index"],
        "insert_section_break": ["index"],
    }

    if op_type not in required_fields:
        return False, f"Unsupported operation type: {op_type or 'None'}"

    for field in required_fields[op_type]:
        if field not in operation:
            return False, f"Missing required field: {field}"

    return True, ""
