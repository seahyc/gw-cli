"""
Markdown -> Google Docs conversion helpers.

Parses a simplified subset of Markdown into structured blocks and emits
Google Docs API batchUpdate requests that apply native formatting:

- Headings (# / ## / ### / ...) -> HEADING_1..HEADING_6
- Paragraphs (NORMAL_TEXT)
- Bullet lists (- item) with BULLET_DISC_CIRCLE_SQUARE
- Blockquotes (> text) rendered as italic with 36pt left indent
- Tables (| a | b |\n|---|---|\n| c | d |) with bold header row
- Inline **bold**
- Inline `code` (rendered in Roboto Mono)
- Horizontal rules (---) are intentionally skipped (heading/spacing suffices)

Limitations:
- No nested lists
- No ordered lists
- No images, links, strikethrough, multiple emphasis markers
- No fenced code blocks (only inline `code`)
- No raw HTML
"""

import re
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

InlineRun = Tuple[str, Dict[str, bool]]


def parse_inline(text: str) -> List[InlineRun]:
    """Split a line of text into a list of (text, attrs) runs.

    Supports **bold** and `code`. attrs is a dict with optional keys
    'bold' and 'code' set to True.
    """
    runs: List[InlineRun] = []
    i = 0
    while i < len(text):
        bold = re.search(r"\*\*([^*]+)\*\*", text[i:])
        code = re.search(r"`([^`]+)`", text[i:])
        cands = []
        if bold:
            cands.append((bold.start() + i, bold.end() + i, bold.group(1), "bold"))
        if code:
            cands.append((code.start() + i, code.end() + i, code.group(1), "code"))
        if not cands:
            if i < len(text):
                runs.append((text[i:], {}))
            break
        cands.sort()
        start, end, inner, kind = cands[0]
        if start > i:
            runs.append((text[i:start], {}))
        if kind == "bold":
            runs.append((inner, {"bold": True}))
        else:
            runs.append((inner, {"code": True}))
        i = end
    return [r for r in runs if r[0]]


def parse_markdown(text: str) -> List[Dict[str, Any]]:
    """Parse a markdown string into a list of block dicts.

    Block types:
      {"type": "heading", "level": int, "runs": [...]}
      {"type": "paragraph", "runs": [...]}
      {"type": "list", "items_runs": [[...], ...]}
      {"type": "quote", "runs": [...]}
      {"type": "table", "rows": [[...]], "cells_runs": [[[...], ...], ...]}
      {"type": "hr"}
    """
    lines = text.split("\n")
    blocks: List[Dict[str, Any]] = []
    i = 0
    while i < len(lines):
        raw = lines[i]
        stripped = raw.rstrip()
        if not stripped:
            i += 1
            continue

        m = re.match(r"^(#{1,6})\s+(.*)$", stripped)
        if m:
            blocks.append({
                "type": "heading",
                "level": len(m.group(1)),
                "runs": parse_inline(m.group(2)),
            })
            i += 1
            continue

        if stripped == "---":
            blocks.append({"type": "hr"})
            i += 1
            continue

        # Table: header row followed by separator row of dashes/pipes/colons
        if (
            stripped.startswith("|")
            and i + 1 < len(lines)
            and re.match(r"^\|[\s\-:|]+\|\s*$", lines[i + 1].rstrip())
        ):
            rows = [[c.strip() for c in stripped.strip("|").split("|")]]
            i += 2
            while i < len(lines) and lines[i].rstrip().startswith("|"):
                rows.append([c.strip() for c in lines[i].rstrip().strip("|").split("|")])
                i += 1
            cells_runs = [[parse_inline(c) for c in row] for row in rows]
            blocks.append({"type": "table", "rows": rows, "cells_runs": cells_runs})
            continue

        if stripped.startswith("> "):
            quote_lines = []
            while i < len(lines) and lines[i].rstrip().startswith("> "):
                quote_lines.append(lines[i].rstrip()[2:])
                i += 1
            blocks.append({
                "type": "quote",
                "runs": parse_inline(" ".join(quote_lines)),
            })
            continue

        if stripped.startswith("- "):
            items = []
            while i < len(lines) and lines[i].rstrip().startswith("- "):
                items.append(lines[i].rstrip()[2:])
                i += 1
            items_runs = [parse_inline(it) for it in items]
            blocks.append({"type": "list", "items_runs": items_runs})
            continue

        # Paragraph: gobble consecutive non-special lines
        para = [stripped]
        i += 1
        while i < len(lines):
            nxt = lines[i].rstrip()
            if not nxt:
                break
            if re.match(r"^#{1,6}\s", nxt):
                break
            if nxt == "---":
                break
            if nxt.startswith("|"):
                break
            if nxt.startswith("- "):
                break
            if nxt.startswith("> "):
                break
            para.append(nxt)
            i += 1
        blocks.append({"type": "paragraph", "runs": parse_inline(" ".join(para))})

    return blocks


# ---------------------------------------------------------------------------
# Request building
# ---------------------------------------------------------------------------

HEADING_NAMES = {
    1: "HEADING_1",
    2: "HEADING_2",
    3: "HEADING_3",
    4: "HEADING_4",
    5: "HEADING_5",
    6: "HEADING_6",
}


def _runs_plain_text(runs: List[InlineRun]) -> str:
    return "".join(r[0] for r in runs)


def _loc(index: int, tab_id: Optional[str]) -> Dict[str, Any]:
    loc: Dict[str, Any] = {"index": index}
    if tab_id:
        loc["tabId"] = tab_id
    return loc


def _range(start: int, end: int, tab_id: Optional[str]) -> Dict[str, Any]:
    rng: Dict[str, Any] = {"startIndex": start, "endIndex": end}
    if tab_id:
        rng["tabId"] = tab_id
    return rng


def _style_runs(
    runs: List[InlineRun], start_index: int, tab_id: Optional[str]
) -> List[Dict[str, Any]]:
    """Emit updateTextStyle requests for bold/code runs within a run sequence."""
    reqs: List[Dict[str, Any]] = []
    offset = start_index
    for run_text, attrs in runs:
        rl = len(run_text)
        if attrs.get("bold"):
            reqs.append({
                "updateTextStyle": {
                    "range": _range(offset, offset + rl, tab_id),
                    "textStyle": {"bold": True},
                    "fields": "bold",
                }
            })
        if attrs.get("code"):
            reqs.append({
                "updateTextStyle": {
                    "range": _range(offset, offset + rl, tab_id),
                    "textStyle": {"weightedFontFamily": {"fontFamily": "Roboto Mono"}},
                    "fields": "weightedFontFamily",
                }
            })
        offset += rl
    return reqs


def build_heading_block(
    runs: List[InlineRun], level: int, cursor: int, tab_id: Optional[str]
) -> Tuple[List[Dict[str, Any]], int]:
    text = _runs_plain_text(runs) + "\n"
    text_len = len(text)
    named_style = HEADING_NAMES.get(level, "HEADING_6")
    reqs: List[Dict[str, Any]] = [
        {"insertText": {"location": _loc(cursor, tab_id), "text": text}},
        {
            "updateParagraphStyle": {
                "range": _range(cursor, cursor + text_len, tab_id),
                "paragraphStyle": {"namedStyleType": named_style},
                "fields": "namedStyleType",
            }
        },
    ]
    reqs.extend(_style_runs(runs, cursor, tab_id))
    return reqs, cursor + text_len


def build_paragraph_block(
    runs: List[InlineRun], cursor: int, tab_id: Optional[str]
) -> Tuple[List[Dict[str, Any]], int]:
    text = _runs_plain_text(runs) + "\n"
    text_len = len(text)
    reqs: List[Dict[str, Any]] = [
        {"insertText": {"location": _loc(cursor, tab_id), "text": text}},
        {
            "updateParagraphStyle": {
                "range": _range(cursor, cursor + text_len, tab_id),
                "paragraphStyle": {"namedStyleType": "NORMAL_TEXT"},
                "fields": "namedStyleType",
            }
        },
    ]
    reqs.extend(_style_runs(runs, cursor, tab_id))
    return reqs, cursor + text_len


def build_list_block(
    items_runs: List[List[InlineRun]], cursor: int, tab_id: Optional[str]
) -> Tuple[List[Dict[str, Any]], int]:
    reqs: List[Dict[str, Any]] = []
    list_start = cursor
    offset = cursor
    for runs in items_runs:
        text = _runs_plain_text(runs) + "\n"
        text_len = len(text)
        reqs.append({"insertText": {"location": _loc(offset, tab_id), "text": text}})
        reqs.append({
            "updateParagraphStyle": {
                "range": _range(offset, offset + text_len, tab_id),
                "paragraphStyle": {"namedStyleType": "NORMAL_TEXT"},
                "fields": "namedStyleType",
            }
        })
        reqs.extend(_style_runs(runs, offset, tab_id))
        offset += text_len
    reqs.append({
        "createParagraphBullets": {
            "range": _range(list_start, offset, tab_id),
            "bulletPreset": "BULLET_DISC_CIRCLE_SQUARE",
        }
    })
    return reqs, offset


def build_quote_block(
    runs: List[InlineRun], cursor: int, tab_id: Optional[str]
) -> Tuple[List[Dict[str, Any]], int]:
    text = _runs_plain_text(runs) + "\n"
    text_len = len(text)
    reqs: List[Dict[str, Any]] = [
        {"insertText": {"location": _loc(cursor, tab_id), "text": text}},
        {
            "updateParagraphStyle": {
                "range": _range(cursor, cursor + text_len, tab_id),
                "paragraphStyle": {
                    "namedStyleType": "NORMAL_TEXT",
                    "indentStart": {"magnitude": 36, "unit": "PT"},
                },
                "fields": "namedStyleType,indentStart",
            }
        },
        {
            "updateTextStyle": {
                # Italicize content but not the trailing newline
                "range": _range(cursor, cursor + text_len - 1, tab_id),
                "textStyle": {"italic": True},
                "fields": "italic",
            }
        },
    ]
    # Also honor inline bold/code within the quote
    reqs.extend(_style_runs(runs, cursor, tab_id))
    return reqs, cursor + text_len
