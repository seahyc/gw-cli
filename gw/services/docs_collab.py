"""Collaboration awareness for Google Docs.

Everything here exists so that an agent co-editing a doc with humans can see
what the humans did: pending suggestions, comment threads (and whether their
anchors still exist), snapshots of the doc, diffs against a snapshot, who
touched the file since, and faithful exports (markdown / text / email HTML).

The module is split into:

* pure functions that operate on already-fetched API payloads (block model,
  suggestion grouping, comment normalisation, diffing, renderers); these are
  unit-tested without network access, and
* thin fetch helpers that take authenticated service clients (the callers get
  them from ``gw.auth.get_service``).

``snapshot(service, doc_id)`` is the public hook that write commands can call
before mutating a doc.
"""

from __future__ import annotations

import difflib
import html
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

DOC_URL = "https://docs.google.com/document/d/{doc_id}/edit"
SUGGESTION_AUTHOR_NOTE = (
    "The Google Docs API does not expose who made a suggestion; open the doc "
    "in the browser to see suggestion authors."
)

# suggestionsViewMode values for documents.get
VIEW_MODES = {
    "accepted": "PREVIEW_SUGGESTIONS_ACCEPTED",
    "inline": "SUGGESTIONS_INLINE",
    "rejected": "PREVIEW_WITHOUT_SUGGESTIONS",
}

_DOC_ID_RE = re.compile(r"/document/d/([a-zA-Z0-9_-]+)")


def parse_doc_id(value: str) -> str:
    """Accept a bare document ID or a Docs URL and return the ID."""
    m = _DOC_ID_RE.search(value or "")
    return m.group(1) if m else value


def doc_url(doc_id: str) -> str:
    return DOC_URL.format(doc_id=doc_id)


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _parse_time(value: str | None) -> datetime | None:
    """Parse an RFC 3339 / ISO 8601 timestamp (a bare date is allowed)."""
    if not value:
        return None
    v = value.strip()
    if v.endswith("Z"):
        v = v[:-1] + "+00:00"
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", v):
        v += "T00:00:00+00:00"
    dt = datetime.fromisoformat(v)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


# ---------------------------------------------------------------------------
# Block model (pure)
# ---------------------------------------------------------------------------

def _element_text(pe: dict) -> str:
    if "textRun" in pe:
        return pe["textRun"].get("content", "")
    if "person" in pe:
        props = pe["person"].get("personProperties", {})
        return props.get("name") or props.get("email", "")
    if "richLink" in pe:
        props = pe["richLink"].get("richLinkProperties", {})
        return props.get("title") or props.get("uri", "")
    if "inlineObjectElement" in pe:
        return "[image]"
    if "footnoteReference" in pe:
        return f"[^{pe['footnoteReference'].get('footnoteNumber', '')}]"
    if "horizontalRule" in pe:
        return ""
    return ""


def _run_from_element(pe: dict) -> dict | None:
    text = _element_text(pe)
    if text == "":
        return None
    kind = next(iter(k for k in pe if k not in ("startIndex", "endIndex")), "")
    inner = pe.get(kind, {}) if isinstance(pe.get(kind), dict) else {}
    style = inner.get("textStyle", {})
    link = (style.get("link") or {}).get("url")
    if kind == "richLink":
        link = inner.get("richLinkProperties", {}).get("uri") or link
    return {
        "text": text,
        "bold": bool(style.get("bold")),
        "italic": bool(style.get("italic")),
        "strike": bool(style.get("strikethrough")),
        "link": link,
        "ins": bool(inner.get("suggestedInsertionIds")),
        "del": bool(inner.get("suggestedDeletionIds")),
        "start": pe.get("startIndex"),
        "end": pe.get("endIndex"),
    }


def _list_is_ordered(doc: dict, list_id: str, level: int) -> bool:
    lists = doc.get("lists", {})
    levels = (
        lists.get(list_id, {}).get("listProperties", {}).get("nestingLevels", [])
    )
    if level < len(levels):
        lvl = levels[level]
        if lvl.get("glyphSymbol"):
            return False
        glyph = lvl.get("glyphType")
        return bool(glyph) and glyph not in ("GLYPH_TYPE_UNSPECIFIED", "NONE")
    return False


def _paragraph_block(doc: dict, el: dict) -> dict:
    para = el["paragraph"]
    runs = [r for r in (_run_from_element(pe) for pe in para.get("elements", [])) if r]
    # Strip the paragraph-terminating newline from the last run.
    if runs and runs[-1]["text"].endswith("\n"):
        runs[-1]["text"] = runs[-1]["text"][:-1]
        if runs[-1]["text"] == "":
            runs.pop()
    for r in runs:
        r["text"] = r["text"].replace("\u000b", "\n")
    style = para.get("paragraphStyle", {}).get("namedStyleType", "NORMAL_TEXT")
    level = None
    if style.startswith("HEADING_"):
        level = int(style.split("_")[1])
    elif style == "TITLE":
        level = 1
    elif style == "SUBTITLE":
        level = 2
    bullet = None
    if "bullet" in para:
        list_id = para["bullet"].get("listId", "")
        nest = para["bullet"].get("nestingLevel", 0)
        bullet = {
            "list_id": list_id,
            "level": nest,
            "ordered": _list_is_ordered(doc, list_id, nest),
        }
    return {
        "type": "paragraph",
        "style": style,
        "heading_level": level,
        "bullet": bullet,
        "runs": runs,
        "text": "".join(r["text"] for r in runs),
        "start": el.get("startIndex"),
        "end": el.get("endIndex"),
    }


def _blocks_from_content(doc: dict, content: list) -> list:
    blocks = []
    for el in content:
        if "paragraph" in el:
            blocks.append(_paragraph_block(doc, el))
        elif "table" in el:
            rows = []
            for row in el["table"].get("tableRows", []):
                cells = []
                for cell in row.get("tableCells", []):
                    paras = [
                        b for b in _blocks_from_content(doc, cell.get("content", []))
                        if b["type"] == "paragraph"
                    ]
                    cells.append({
                        "paragraphs": paras,
                        "text": "\n".join(p["text"] for p in paras).strip("\n"),
                    })
                rows.append(cells)
            blocks.append({
                "type": "table",
                "rows": rows,
                "start": el.get("startIndex"),
                "end": el.get("endIndex"),
            })
        # sectionBreak / tableOfContents are skipped on purpose.
    return blocks


def extract_blocks(doc: dict) -> list:
    """Flatten a documents.get payload (body of the first tab) into blocks."""
    return _blocks_from_content(doc, doc.get("body", {}).get("content", []))


def is_bold_only(block: dict) -> bool:
    """A non-heading, non-bullet paragraph whose visible text is entirely bold."""
    if block["type"] != "paragraph" or block["heading_level"] or block["bullet"]:
        return False
    visible = [r for r in block["runs"] if r["text"].strip()]
    return bool(visible) and all(r["bold"] for r in visible)


def is_italic_only(block: dict) -> bool:
    if block["type"] != "paragraph" or block["heading_level"]:
        return False
    visible = [r for r in block["runs"] if r["text"].strip()]
    return bool(visible) and all(r["italic"] for r in visible)


def doc_plain_text(blocks: list) -> str:
    """All visible text, paragraphs and table cells, newline separated."""
    out = []
    for b in blocks:
        if b["type"] == "paragraph":
            out.append(b["text"])
        else:
            for row in b["rows"]:
                for cell in row:
                    out.append(cell["text"])
    return "\n".join(out)


def diff_lines(blocks: list) -> list:
    """One line per non-empty paragraph, one line per table row."""
    lines = []
    for b in blocks:
        if b["type"] == "paragraph":
            text = b["text"].replace("\n", " / ")
            if not text.strip():
                continue
            if b["heading_level"]:
                text = "#" * b["heading_level"] + " " + text
            elif b["bullet"]:
                text = "  " * b["bullet"]["level"] + "- " + text
            lines.append(text)
        else:
            for row in b["rows"]:
                cells = [c["text"].replace("\n", " / ") for c in row]
                lines.append("| " + " | ".join(cells) + " |")
    return lines


def structure_summary(blocks: list) -> dict:
    paragraphs = [b for b in blocks if b["type"] == "paragraph" and b["text"].strip()]
    return {
        "paragraphs": len(paragraphs),
        "headings": [
            {"level": b["heading_level"], "text": b["text"]}
            for b in paragraphs if b["heading_level"]
        ],
        "bold_headings": [b["text"] for b in paragraphs if is_bold_only(b)],
        "bullets": sum(1 for b in paragraphs if b["bullet"]),
        "tables": [
            {
                "rows": len(b["rows"]),
                "cols": max((len(r) for r in b["rows"]), default=0),
                "start": b["start"],
                "header": [c["text"] for c in b["rows"][0]] if b["rows"] else [],
            }
            for b in blocks if b["type"] == "table"
        ],
    }


# ---------------------------------------------------------------------------
# Suggestions (pure)
# ---------------------------------------------------------------------------

def _iter_paragraphs(content: list):
    """Yield (paragraph_element, row_context) for every paragraph incl. tables."""
    for el in content:
        if "paragraph" in el:
            yield el
        elif "table" in el:
            for row in el["table"].get("tableRows", []):
                for cell in row.get("tableCells", []):
                    yield from _iter_paragraphs(cell.get("content", []))
        elif "tableOfContents" in el:
            yield from _iter_paragraphs(el["tableOfContents"].get("content", []))


def _marked_snippet(para: dict, sid: str, width: int = 100) -> str:
    """Paragraph text with this suggestion marked [+ins+] / [-del-], trimmed."""
    parts = []
    first = None
    for pe in para.get("elements", []):
        text = _element_text(pe).replace("\u000b", "\n").rstrip("\n")
        kind = next(iter(k for k in pe if k not in ("startIndex", "endIndex")), "")
        inner = pe.get(kind, {}) if isinstance(pe.get(kind), dict) else {}
        pos = sum(len(p) for p in parts)
        if sid in inner.get("suggestedInsertionIds", []):
            first = pos if first is None else first
            parts.append(f"[+{text}+]")
        elif sid in inner.get("suggestedDeletionIds", []):
            first = pos if first is None else first
            parts.append(f"[-{text}-]")
        else:
            parts.append(text)
    s = "".join(parts)
    if first is None or len(s) <= 2 * width:
        return s if len(s) <= 2 * width else s[: 2 * width] + "..."
    lo = max(0, first - width)
    hi = min(len(s), first + width)
    return ("..." if lo else "") + s[lo:hi] + ("..." if hi < len(s) else "")


def group_suggestions(doc: dict) -> list:
    """Group pending suggestions by suggestion id.

    ``doc`` must be fetched with suggestionsViewMode=SUGGESTIONS_INLINE.
    Returns a list sorted by start index; each item has id, kinds, inserted,
    deleted, start, end, snippet.
    """
    groups: dict[str, dict] = {}

    def g(sid: str) -> dict:
        return groups.setdefault(sid, {
            "id": sid, "kinds": [], "inserted": "", "deleted": "",
            "start": None, "end": None, "snippet": None,
        })

    def touch(item, kind, start, end, para=None):
        if kind not in item["kinds"]:
            item["kinds"].append(kind)
        if start is not None:
            item["start"] = start if item["start"] is None else min(item["start"], start)
        if end is not None:
            item["end"] = end if item["end"] is None else max(item["end"], end)
        if para is not None and item["snippet"] is None:
            item["snippet"] = _marked_snippet(para, item["id"])

    content = doc.get("body", {}).get("content", [])
    for el in _iter_paragraphs(content):
        para = el["paragraph"]
        for sid in para.get("suggestedParagraphStyleChanges", {}) or {}:
            touch(g(sid), "paragraph_style", el.get("startIndex"), el.get("endIndex"), para)
        for sid in (para.get("suggestedBulletChanges") or {}):
            touch(g(sid), "bullet", el.get("startIndex"), el.get("endIndex"), para)
        for pe in para.get("elements", []):
            kind = next(iter(k for k in pe if k not in ("startIndex", "endIndex")), "")
            inner = pe.get(kind, {}) if isinstance(pe.get(kind), dict) else {}
            text = _element_text(pe)
            s, e = pe.get("startIndex"), pe.get("endIndex")
            for sid in inner.get("suggestedInsertionIds", []):
                item = g(sid)
                item["inserted"] += text
                touch(item, "insertion", s, e, para)
            for sid in inner.get("suggestedDeletionIds", []):
                item = g(sid)
                item["deleted"] += text
                touch(item, "deletion", s, e, para)
            for sid in (inner.get("suggestedTextStyleChanges") or {}):
                touch(g(sid), "text_style", s, e, para)

    # Table-structure suggestions (inserted/deleted tables or rows).
    for el in content:
        if "table" not in el:
            continue
        table = el["table"]
        for sid in table.get("suggestedInsertionIds", []):
            touch(g(sid), "table_insertion", el.get("startIndex"), el.get("endIndex"))
        for sid in table.get("suggestedDeletionIds", []):
            touch(g(sid), "table_deletion", el.get("startIndex"), el.get("endIndex"))
        for row in table.get("tableRows", []):
            row_text = " | ".join(
                "".join(
                    _element_text(pe)
                    for p in _iter_paragraphs(c.get("content", []))
                    for pe in p["paragraph"].get("elements", [])
                ).replace("\n", " ").strip()
                for c in row.get("tableCells", [])
            )
            for sid in row.get("suggestedInsertionIds", []):
                touch(g(sid), "row_insertion", row.get("startIndex"), row.get("endIndex"))
                g(sid)["snippet"] = g(sid)["snippet"] or f"[+| {row_text} |+]"
            for sid in row.get("suggestedDeletionIds", []):
                touch(g(sid), "row_deletion", row.get("startIndex"), row.get("endIndex"))
                g(sid)["snippet"] = g(sid)["snippet"] or f"[-| {row_text} |-]"

    return sorted(groups.values(), key=lambda x: (x["start"] is None, x["start"] or 0))


# ---------------------------------------------------------------------------
# Comments (pure)
# ---------------------------------------------------------------------------

def _norm_ws(s: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(s or "")).strip()


def anchor_status(quoted: str | None, doc_text: str | None) -> str:
    """attached / detached / unanchored / unknown (non-Docs file)."""
    if not quoted:
        return "unanchored"
    if doc_text is None:
        return "unknown"
    return "attached" if _norm_ws(quoted) in _norm_ws(doc_text) else "detached"


def _person(p: dict | None) -> str:
    p = p or {}
    return p.get("displayName") or p.get("emailAddress") or "Unknown"


def normalize_comments(
    raw: list,
    doc_text: str | None,
    *,
    file_id: str | None = None,
    open_only: bool = False,
    since: str | None = None,
    author: str | None = None,
    chrono: bool = False,
) -> list:
    """Turn Drive comments.list items into filtered, sorted structured dicts.

    ``since``: keep a comment if it or any of its replies was created at or
    after this time. ``author``: case-insensitive substring match on the
    comment author's display name or email.
    """
    since_dt = _parse_time(since)
    out = []
    for c in raw:
        if c.get("deleted"):
            continue
        if open_only and c.get("resolved"):
            continue
        a = c.get("author") or {}
        if author:
            needle = author.lower()
            hay = f"{a.get('displayName', '')} {a.get('emailAddress', '')}".lower()
            if needle not in hay:
                continue
        replies = [
            {
                "id": r.get("id"),
                "author": _person(r.get("author")),
                "created": r.get("createdTime"),
                "modified": r.get("modifiedTime"),
                "action": r.get("action"),
                "content": r.get("content", ""),
            }
            for r in c.get("replies", []) if not r.get("deleted")
        ]
        if since_dt:
            times = [c.get("createdTime")] + [r["created"] for r in replies]
            if not any(t and _parse_time(t) >= since_dt for t in times):
                continue
        quoted = html.unescape((c.get("quotedFileContent") or {}).get("value", "")) or None
        item = {
            "id": c.get("id"),
            "author": _person(a),
            "created": c.get("createdTime"),
            "modified": c.get("modifiedTime"),
            "resolved": bool(c.get("resolved")),
            "quoted": quoted,
            "anchor_status": anchor_status(quoted, doc_text),
            "content": c.get("content", ""),
            "replies": replies,
        }
        if file_id:
            item["url"] = f"{doc_url(file_id)}?disco={c.get('id')}"
        out.append(item)
    out.sort(key=lambda x: x["created"] or "", reverse=not chrono)
    return out


# ---------------------------------------------------------------------------
# Snapshot diffing (pure)
# ---------------------------------------------------------------------------

def unified_diff(old_lines: list, new_lines: list, old_label="snapshot", new_label="current") -> dict:
    diff = list(difflib.unified_diff(old_lines, new_lines, old_label, new_label, lineterm="", n=1))
    added = sum(1 for d in diff if d.startswith("+") and not d.startswith("+++"))
    removed = sum(1 for d in diff if d.startswith("-") and not d.startswith("---"))
    return {"changed": bool(diff), "added": added, "removed": removed, "diff": "\n".join(diff)}


def diff_comments(old: list, new: list) -> dict:
    old_by_id = {c["id"]: c for c in old}
    new_c, changed, resolved, reopened = [], [], [], []
    for c in new:
        o = old_by_id.get(c["id"])
        if o is None:
            new_c.append(c)
            continue
        if c["resolved"] and not o["resolved"]:
            resolved.append(c)
        elif o["resolved"] and not c["resolved"]:
            reopened.append(c)
        old_reply_ids = {r["id"] for r in o.get("replies", [])}
        new_replies = [r for r in c.get("replies", []) if r["id"] not in old_reply_ids]
        if (
            new_replies
            or c["content"] != o["content"]
            or c["anchor_status"] != o["anchor_status"]
        ):
            changed.append({
                **c,
                "new_replies": new_replies,
                "content_changed": c["content"] != o["content"],
                "anchor_status_before": o["anchor_status"],
            })
    new_ids = {c["id"] for c in new}
    deleted = [o for o in old if o["id"] not in new_ids]
    return {
        "new": new_c, "changed": changed, "resolved": resolved,
        "reopened": reopened, "deleted": deleted,
    }


def diff_suggestions(old: list, new: list) -> dict:
    old_ids = {s["id"] for s in old}
    new_ids = {s["id"] for s in new}
    return {
        "new": [s for s in new if s["id"] not in old_ids],
        "gone": [s for s in old if s["id"] not in new_ids],
        "note": "'gone' suggestions were accepted or rejected (the API does not say which).",
    }


# ---------------------------------------------------------------------------
# Renderers (pure)
# ---------------------------------------------------------------------------

_NUM_RE = re.compile(r"^[\s(+\-−]*[$€£¥₹]?\s*[\d.,]+\s*[%kKmMbBxX]?\)?\s*$")


def _is_numeric(s: str) -> bool:
    s = s.strip()
    return bool(s) and any(ch.isdigit() for ch in s) and bool(_NUM_RE.match(s))


def numeric_columns(rows: list) -> list:
    """Per column: True when every non-empty body cell (rows[1:]) is numeric."""
    ncols = max((len(r) for r in rows), default=0)
    flags = []
    for i in range(ncols):
        vals = [r[i] for r in rows[1:] if i < len(r) and r[i].strip()]
        flags.append(bool(vals) and all(_is_numeric(v) for v in vals))
    return flags


def _merge_runs(runs: list) -> list:
    keys = ("bold", "italic", "strike", "link", "ins", "del")
    merged = []
    for r in runs:
        if merged and all(merged[-1][k] == r[k] for k in keys):
            merged[-1] = {**merged[-1], "text": merged[-1]["text"] + r["text"]}
        else:
            merged.append(dict(r))
    return merged


def _md_escape(text: str) -> str:
    return re.sub(r"([\\`*_\[\]])", r"\\\1", text)


def _md_inline(runs: list, *, suppress_bold=False, in_table=False) -> str:
    out = []
    for r in _merge_runs(runs):
        text = r["text"]
        if not text:
            continue
        lead = text[: len(text) - len(text.lstrip())]
        trail = text[len(text.rstrip()):]
        core = _md_escape(text.strip())
        if core:
            if r["link"]:
                core = f"[{core}]({r['link']})"
            if r["bold"] and not suppress_bold:
                core = f"**{core}**"
            if r["italic"]:
                core = f"*{core}*"
            if r["strike"]:
                core = f"~~{core}~~"
            if r["ins"]:
                core = "{++" + core + "++}"
            if r["del"]:
                core = "{--" + core + "--}"
        out.append(lead + core + trail)
    s = "".join(out)
    if in_table:
        return s.replace("|", "\\|").replace("\n", "<br>")
    return s.replace("\n", "  \n")


def _cell_md(cell: dict) -> str:
    return "<br>".join(_md_inline(p["runs"], in_table=True) for p in cell["paragraphs"])


def _md_table(block: dict) -> list:
    rows = block["rows"]
    if not rows:
        return []
    ncols = max(len(r) for r in rows)
    text_rows = [[c["text"] for c in r] + [""] * (ncols - len(r)) for r in rows]
    md_rows = [[_cell_md(c) for c in r] + [""] * (ncols - len(r)) for r in rows]
    nums = numeric_columns(text_rows)
    lines = ["| " + " | ".join(md_rows[0]) + " |"]
    lines.append("| " + " | ".join("---:" if n else "---" for n in nums) + " |")
    for r in md_rows[1:]:
        lines.append("| " + " | ".join(r) + " |")
    return lines


def render_markdown(blocks: list) -> str:
    out: list = []

    def gap():
        if out and out[-1] != "":
            out.append("")

    prev_bullet = False
    for b in blocks:
        if b["type"] == "table":
            gap()
            out.extend(_md_table(b))
            out.append("")
            prev_bullet = False
            continue
        if not b["text"].strip():
            if prev_bullet:
                out.append("")
            prev_bullet = False
            continue
        if b["bullet"]:
            if not prev_bullet:
                gap()
            marker = "1." if b["bullet"]["ordered"] else "-"
            out.append("  " * b["bullet"]["level"] + f"{marker} " + _md_inline(b["runs"]))
            prev_bullet = True
            continue
        if prev_bullet:
            out.append("")
        prev_bullet = False
        gap()
        if b["heading_level"]:
            out.append("#" * b["heading_level"] + " " + _md_inline(b["runs"], suppress_bold=True))
        elif is_bold_only(b):
            out.append("**" + _md_inline(b["runs"], suppress_bold=True).strip() + "**")
        else:
            out.append(_md_inline(b["runs"]))
        out.append("")
    while out and out[-1] == "":
        out.pop()
    return "\n".join(out) + "\n"


def _text_inline(runs: list) -> str:
    parts = []
    for r in runs:
        t = r["text"]
        if r["ins"]:
            t = f"[+{t}+]"
        elif r["del"]:
            t = f"[-{t}-]"
        parts.append(t)
    return "".join(parts)


def _text_table(rows: list) -> list:
    ncols = max((len(r) for r in rows), default=0)
    text_rows = [
        [" / ".join(p for p in c["text"].split("\n")) for c in r] + [""] * (ncols - len(r))
        for r in rows
    ]
    nums = numeric_columns(text_rows)
    widths = [max(len(r[i]) for r in text_rows) for i in range(ncols)]
    lines = []
    for ri, r in enumerate(text_rows):
        cells = [
            (r[i].rjust(widths[i]) if nums[i] and ri > 0 else r[i].ljust(widths[i]))
            for i in range(ncols)
        ]
        lines.append("  ".join(cells).rstrip())
        if ri == 0 and len(text_rows) > 1:
            lines.append("  ".join("-" * w for w in widths))
    return lines


def render_text(blocks: list) -> str:
    out = []
    for b in blocks:
        if b["type"] == "table":
            if out and out[-1] != "":
                out.append("")
            out.extend(_text_table(b["rows"]))
            out.append("")
            continue
        text = _text_inline(b["runs"])
        if b["bullet"]:
            marker = "1." if b["bullet"]["ordered"] else "-"
            text = "  " * b["bullet"]["level"] + f"{marker} " + text
        out.append(text)
    while out and out[-1] == "":
        out.pop()
    return "\n".join(out) + "\n"


FONT_STACK = (
    "-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',"
    "Arial,sans-serif"
)
_TEXT_COLOR = "#1f2937"
_MUTED = "#6b7280"
_BORDER = "#d1d5db"


# Title Case words that sentence_case() may lowercase. Anything not listed is
# left alone, so proper nouns ("Glints", "Jakarta") keep their capital; an
# unlisted common word staying capitalised is the cheaper mistake.
COMMON_HEADING_WORDS = frozenset("""
a about above across after against all also an and any are as at be before
below between both but by can do does down during each few for from further
has have how if in into is it its more most new next no not of off on once
only or other our out over own per same should so some such than that the
their them then there these they this those through to too under until up
very via was we were what when where which while who why will with within
without you your
action actions analysis approach area areas background budget changes
context cost costs data decision decisions design details goal goals growth
impact issue issues items key launch metrics model notes open options
overview plan plans priorities process progress proposal questions quarter
quarterly recap recommendation recommendations report results review
revenue risk risks roadmap scope status step steps strategy summary target
targets team timeline update updates week weekly work year
""".split())


def sentence_case(text: str, keep=()) -> str:
    """Sentence-case a heading while keeping acronyms, mixed case and proper nouns.

    "QUARTERLY REVENUE UPDATE" -> "Quarterly revenue update"
    "Next Steps For The API"   -> "Next steps for the API"
    "Why Glints Wins In Jakarta" -> "Why Glints wins in Jakarta" only if "wins"
    were listed; unlisted Title Case words (likely names) are kept as written.

    Only Title Case words found in COMMON_HEADING_WORDS are lowercased; words in
    `keep` (case-insensitive) are never changed. An ALL-CAPS heading is
    lowercased wholesale apart from `keep` words, which get Title Case.
    """
    keep_lower = {k.lower() for k in keep or ()}
    words = text.split(" ")
    letters = [ch for ch in text if ch.isalpha()]
    all_caps = bool(letters) and all(ch.isupper() for ch in letters)
    out = []
    for w in words:
        core = w.strip(".,:;!?()[]\"'")
        if not w or core.lower() in keep_lower:
            if all_caps and w:
                w = w[:1] + w[1:].lower()
            out.append(w)
            continue
        if all_caps:
            w = w.lower()
        elif core[:1].isupper() and core[1:].islower() and core.lower() in COMMON_HEADING_WORDS:
            w = w.lower()
        # else: acronym (API), mixed case (iPhone), or likely proper noun -> keep
        out.append(w)
    s = " ".join(out)
    for i, ch in enumerate(s):
        if ch.isalpha():
            return s[:i] + ch.upper() + s[i + 1:]
    return s


def _html_inline(runs: list) -> str:
    out = []
    for r in _merge_runs(runs):
        t = html.escape(r["text"]).replace("\n", "<br>")
        if not t:
            continue
        if r["link"]:
            t = f'<a href="{html.escape(r["link"], quote=True)}" style="color:#1d4ed8;text-decoration:underline">{t}</a>'
        if r["bold"]:
            t = f"<strong>{t}</strong>"
        if r["italic"]:
            t = f"<em>{t}</em>"
        if r["strike"]:
            t = f'<span style="text-decoration:line-through">{t}</span>'
        if r["ins"]:
            t = f'<ins style="color:#047857;text-decoration:underline">{t}</ins>'
        if r["del"]:
            t = f'<del style="color:#b91c1c;text-decoration:line-through">{t}</del>'
        out.append(t)
    return "".join(out)


def _html_heading(text_html: str, level: int) -> str:
    size = {1: 22, 2: 18, 3: 16}.get(level, 15)
    return (
        f'<p style="margin:20px 0 8px 0;font-family:{FONT_STACK};font-size:{size}px;'
        f'line-height:1.3;font-weight:bold;color:{_TEXT_COLOR}">{text_html}</p>'
    )


def _html_table(block: dict) -> str:
    rows = block["rows"]
    if not rows:
        return ""
    ncols = max(len(r) for r in rows)
    nums = numeric_columns([[c["text"] for c in r] for r in rows])
    cell_style = (
        f"border:1px solid {_BORDER};font-family:{FONT_STACK};font-size:14px;"
        f"color:{_TEXT_COLOR};vertical-align:top"
    )
    parts = [
        f'<table width="100%" cellpadding="6" cellspacing="0" border="1" '
        f'style="border-collapse:collapse;border:1px solid {_BORDER};margin:8px 0 16px 0">'
    ]
    for ri, row in enumerate(rows):
        parts.append("<tr>")
        for ci in range(ncols):
            cell = row[ci] if ci < len(row) else {"paragraphs": []}
            inner = "<br>".join(_html_inline(p["runs"]) for p in cell["paragraphs"]) or "&nbsp;"
            align = "right" if nums[ci] and ri > 0 else "left"
            if ri == 0:
                parts.append(
                    f'<th align="{align}" style="{cell_style};background-color:#f3f4f6;'
                    f'font-weight:bold;text-align:{align}">{inner}</th>'
                )
            else:
                parts.append(f'<td align="{align}" style="{cell_style};text-align:{align}">{inner}</td>')
        parts.append("</tr>")
    parts.append("</table>")
    return "".join(parts)


def render_email_html(blocks: list, keep_case=()) -> str:
    """Outlook-safe HTML fragment: inline styles only, no <style> blocks."""
    p_style = (
        f"margin:0 0 12px 0;font-family:{FONT_STACK};font-size:14px;"
        f"line-height:1.5;color:{_TEXT_COLOR}"
    )
    muted = (
        f"margin:0 0 12px 0;font-family:{FONT_STACK};font-size:12px;"
        f"line-height:1.4;color:{_MUTED}"
    )
    body: list = []
    list_stack: list = []  # open list tags

    def close_lists(to_depth=0):
        while len(list_stack) > to_depth:
            body.append(f"</{list_stack.pop()}>")

    for b in blocks:
        if b["type"] == "paragraph" and b["bullet"]:
            depth = b["bullet"]["level"] + 1
            tag = "ol" if b["bullet"]["ordered"] else "ul"
            close_lists(depth)
            if len(list_stack) == depth and list_stack[-1] != tag:
                close_lists(depth - 1)  # list type changed at this level
            while len(list_stack) < depth:
                body.append(
                    f'<{tag} style="margin:0 0 12px 0;padding-left:24px;'
                    f'font-family:{FONT_STACK};font-size:14px;color:{_TEXT_COLOR}">'
                )
                list_stack.append(tag)
            body.append(f'<li style="margin:0 0 4px 0;line-height:1.5">{_html_inline(b["runs"])}</li>')
            continue
        close_lists()
        if b["type"] == "table":
            body.append(_html_table(b))
        elif not b["text"].strip():
            continue
        elif b["heading_level"]:
            runs = _sentence_case_runs([{**r, "bold": False} for r in b["runs"]], keep_case)
            body.append(_html_heading(_html_inline(runs), b["heading_level"]))
        elif is_bold_only(b):
            runs = _sentence_case_runs([{**r, "bold": False} for r in b["runs"]], keep_case)
            body.append(_html_heading(_html_inline(runs), 3))
        elif is_italic_only(b):
            runs = [{**r, "italic": False} for r in b["runs"]]
            body.append(f'<p style="{muted}"><em>{_html_inline(runs)}</em></p>')
        else:
            body.append(f'<p style="{p_style}">{_html_inline(b["runs"])}</p>')
    close_lists()
    inner = "\n".join(body)
    return (
        f'<div style="font-family:{FONT_STACK};font-size:14px;line-height:1.5;'
        f'color:{_TEXT_COLOR};max-width:720px">\n{inner}\n</div>\n'
    )


def _sentence_case_runs(runs: list, keep_case=()) -> list:
    """Sentence-case the heading text; collapses runs when formatting is uniform."""
    text = "".join(r["text"] for r in runs)
    cased = sentence_case(text, keep_case)
    if len(cased) != len(text):
        return runs
    out, pos = [], 0
    for r in runs:
        n = len(r["text"])
        out.append({**r, "text": cased[pos:pos + n]})
        pos += n
    return out


# ---------------------------------------------------------------------------
# Fetch helpers (network)
# ---------------------------------------------------------------------------

def fetch_doc(docs_service, doc_id: str, view: str = "rejected") -> dict:
    mode = VIEW_MODES.get(view, view)
    return docs_service.documents().get(documentId=doc_id, suggestionsViewMode=mode).execute()


def fetch_raw_comments(drive_service, file_id: str) -> list:
    comments, token = [], None
    fields = (
        "nextPageToken,comments(id,content,author(displayName,emailAddress,me),"
        "createdTime,modifiedTime,resolved,deleted,quotedFileContent,"
        "replies(id,content,author(displayName,emailAddress,me),createdTime,"
        "modifiedTime,action,deleted))"
    )
    while True:
        resp = drive_service.comments().list(
            fileId=file_id, fields=fields, pageSize=100, pageToken=token,
        ).execute()
        comments.extend(resp.get("comments", []))
        token = resp.get("nextPageToken")
        if not token:
            return comments


def fetch_file_meta(drive_service, file_id: str) -> dict:
    return drive_service.files().get(
        fileId=file_id,
        fields="id,name,mimeType,modifiedTime,version,webViewLink,"
        "lastModifyingUser(displayName,emailAddress,me)",
        supportsAllDrives=True,
    ).execute()


def fetch_revisions(drive_service, file_id: str) -> list:
    revs, token = [], None
    while True:
        resp = drive_service.revisions().list(
            fileId=file_id, pageSize=1000, pageToken=token,
            fields="nextPageToken,revisions(id,modifiedTime,"
            "lastModifyingUser(displayName,emailAddress,me))",
        ).execute()
        revs.extend(resp.get("revisions", []))
        token = resp.get("nextPageToken")
        if not token:
            return revs


def _drive():
    from gw.auth import get_service
    return get_service("drive")


def list_comments(drive_service, docs_service, file_id: str, **filters) -> dict:
    """Structured comment list with anchor_status for Google Docs files."""
    file_id = parse_doc_id(file_id)
    meta = fetch_file_meta(drive_service, file_id)
    doc_text = None
    is_doc = meta.get("mimeType") == "application/vnd.google-apps.document"
    if is_doc and docs_service is not None:
        doc_text = doc_plain_text(extract_blocks(fetch_doc(docs_service, file_id, "inline")))
    items = normalize_comments(
        fetch_raw_comments(drive_service, file_id), doc_text,
        file_id=file_id if is_doc else None, **filters,
    )
    return {
        "id": file_id,
        "url": doc_url(file_id) if is_doc else meta.get("webViewLink"),
        "title": meta.get("name"),
        "count": len(items),
        "filters": {k: v for k, v in filters.items() if v},
        "comments": items,
    }


def list_suggestions(docs_service, doc_id: str) -> dict:
    doc_id = parse_doc_id(doc_id)
    doc = fetch_doc(docs_service, doc_id, "inline")
    items = group_suggestions(doc)
    return {
        "id": doc_id,
        "url": doc_url(doc_id),
        "title": doc.get("title"),
        "revisionId": doc.get("revisionId"),
        "count": len(items),
        "authors_note": SUGGESTION_AUTHOR_NOTE,
        "suggestions": items,
    }


def export_doc(
    docs_service, doc_id: str, fmt: str, suggestions: str = "rejected", keep_case=(),
) -> dict:
    doc_id = parse_doc_id(doc_id)
    doc = fetch_doc(docs_service, doc_id, suggestions)
    blocks = extract_blocks(doc)
    if fmt == "md":
        content = render_markdown(blocks)
    elif fmt == "text":
        content = render_text(blocks)
    elif fmt == "email-html":
        content = render_email_html(blocks, keep_case)
    else:
        raise ValueError(f"Unknown format: {fmt}")
    return {
        "id": doc_id,
        "url": doc_url(doc_id),
        "title": doc.get("title"),
        "revisionId": doc.get("revisionId"),
        "format": fmt,
        "suggestions": suggestions,
        "content": content,
    }


# ---------------------------------------------------------------------------
# Snapshots
# ---------------------------------------------------------------------------

def cache_root() -> Path:
    base = os.environ.get("GW_CACHE_DIR") or os.path.join(
        os.environ.get("XDG_CACHE_HOME") or os.path.expanduser("~/.cache"), "gw"
    )
    return Path(base) / "docs"


def _snap_dir(doc_id: str) -> Path:
    return cache_root() / doc_id


def _capture(docs_service, drive_service, doc_id: str) -> dict:
    """Current state of a doc in snapshot shape (not written to disk)."""
    doc = fetch_doc(docs_service, doc_id, "rejected")
    inline = fetch_doc(docs_service, doc_id, "inline")
    blocks = extract_blocks(doc)
    meta = fetch_file_meta(drive_service, doc_id)
    inline_text = doc_plain_text(extract_blocks(inline))
    return {
        "id": doc_id,
        "url": doc_url(doc_id),
        "title": doc.get("title"),
        "taken_at": _now_iso(),
        "revisionId": doc.get("revisionId"),
        "modifiedTime": meta.get("modifiedTime"),
        "lastModifyingUser": meta.get("lastModifyingUser"),
        "drive_version": meta.get("version"),
        "text": doc_plain_text(blocks),
        "lines": diff_lines(blocks),
        "structure": structure_summary(blocks),
        "comments": normalize_comments(
            fetch_raw_comments(drive_service, doc_id), inline_text, file_id=doc_id, chrono=True,
        ),
        "suggestions": group_suggestions(inline),
    }


def snapshot(service, doc_id: str, drive_service=None) -> dict:
    """Save the doc's current state under ~/.cache/gw/docs/<doc_id>/.

    ``service`` is a Docs API client. Returns a small summary including the
    snapshot path. Intended to be called by write commands before mutating.
    """
    doc_id = parse_doc_id(doc_id)
    drive_service = drive_service or _drive()
    snap = _capture(service, drive_service, doc_id)
    d = _snap_dir(doc_id)
    d.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = d / f"{stamp}.json"
    n = 1
    while path.exists():
        path = d / f"{stamp}-{n}.json"
        n += 1
    snap["snapshot"] = path.stem
    path.write_text(json.dumps(snap, indent=2, ensure_ascii=False))
    (d / "latest").write_text(path.name + "\n")
    return {
        "id": doc_id,
        "url": snap["url"],
        "title": snap["title"],
        "snapshot": path.stem,
        "path": str(path),
        "revisionId": snap["revisionId"],
        "modifiedTime": snap["modifiedTime"],
        "lastModifyingUser": snap["lastModifyingUser"],
        "structure": snap["structure"],
        "comments": len(snap["comments"]),
        "suggestions": len(snap["suggestions"]),
    }


def list_snapshots(doc_id: str) -> list:
    d = _snap_dir(parse_doc_id(doc_id))
    return sorted(p.stem for p in d.glob("*.json")) if d.exists() else []


def load_snapshot(doc_id: str, ref: str = "latest") -> dict:
    """Load by 'latest', timestamp (or unique prefix), or snapshot revisionId."""
    doc_id = parse_doc_id(doc_id)
    d = _snap_dir(doc_id)
    if not d.exists():
        raise FileNotFoundError(f"No snapshots for {doc_id}. Run: gw docs snapshot {doc_id}")
    if ref in (None, "", "latest"):
        pointer = d / "latest"
        if not pointer.exists():
            raise FileNotFoundError(f"No latest snapshot for {doc_id}")
        return json.loads((d / pointer.read_text().strip()).read_text())
    names = list_snapshots(doc_id)
    exact = d / f"{ref}.json"
    if exact.exists():
        return json.loads(exact.read_text())
    matches = [n for n in names if n.startswith(ref)]
    if len(matches) == 1:
        return json.loads((d / f"{matches[0]}.json").read_text())
    for name in reversed(names):
        data = json.loads((d / f"{name}.json").read_text())
        if data.get("revisionId") == ref:
            return data
    raise FileNotFoundError(
        f"No snapshot matching '{ref}' for {doc_id}. Available: {', '.join(names) or 'none'}"
    )


def diff_doc(docs_service, drive_service, doc_id: str, since: str = "latest") -> dict:
    doc_id = parse_doc_id(doc_id)
    old = load_snapshot(doc_id, since)
    cur = _capture(docs_service, drive_service, doc_id)
    text = unified_diff(old["lines"], cur["lines"], f"snapshot {old.get('snapshot')}", "current")
    return {
        "id": doc_id,
        "url": cur["url"],
        "title": cur["title"],
        "since": {
            "snapshot": old.get("snapshot"),
            "taken_at": old.get("taken_at"),
            "revisionId": old.get("revisionId"),
            "modifiedTime": old.get("modifiedTime"),
        },
        "current": {
            "revisionId": cur["revisionId"],
            "modifiedTime": cur["modifiedTime"],
            "lastModifyingUser": cur["lastModifyingUser"],
        },
        "revision_changed": old.get("revisionId") != cur["revisionId"],
        "text": text,
        "comments": diff_comments(old.get("comments", []), cur["comments"]),
        "suggestions": diff_suggestions(old.get("suggestions", []), cur["suggestions"]),
    }


def summarize_changes(meta: dict, revisions: list, snap: dict | None, me: str | None) -> dict:
    """Pure: decide whether anyone other than ``me`` edited after ``snap``."""
    def is_me(user: dict | None) -> bool:
        user = user or {}
        if user.get("me") is True:
            return True
        return bool(me) and (user.get("emailAddress") or "").lower() == me.lower()

    # Drive's file modifiedTime can lag behind Docs edits by minutes, so the
    # snapshot's own capture time is the baseline and revisions are the signal.
    since = _parse_time(snap.get("taken_at") or snap.get("modifiedTime")) if snap else None
    after = [
        r for r in revisions
        if since is None or (_parse_time(r.get("modifiedTime")) or since) > since
    ]
    others = [r for r in after if not is_me(r.get("lastModifyingUser"))]
    cur_mod = _parse_time(meta.get("modifiedTime"))
    last_by_other = not is_me(meta.get("lastModifyingUser"))
    if since and cur_mod and cur_mod > since and last_by_other and not others:
        others = [{"id": None, "modifiedTime": meta.get("modifiedTime"),
                   "lastModifyingUser": meta.get("lastModifyingUser")}]
    editors = sorted({_person(r.get("lastModifyingUser")) for r in others})
    return {
        "snapshot": snap.get("snapshot") if snap else None,
        "snapshot_taken_at": snap.get("taken_at") if snap else None,
        "modified_since_snapshot": (
            bool(after) or bool(cur_mod and cur_mod > since) if snap else None
        ),
        "latest_revision_time": revisions[-1].get("modifiedTime") if revisions else None,
        "others_edited_since_snapshot": bool(others) if snap else None,
        "other_editors": editors,
        "revisions_since_snapshot": len(after) if snap else None,
    }


def doc_changes(docs_service, drive_service, doc_id: str) -> dict:
    doc_id = parse_doc_id(doc_id)
    meta = fetch_file_meta(drive_service, doc_id)
    revisions = fetch_revisions(drive_service, doc_id)
    me = (drive_service.about().get(fields="user(emailAddress)").execute()
          .get("user", {}).get("emailAddress"))
    try:
        snap = load_snapshot(doc_id, "latest")
    except FileNotFoundError:
        snap = None
    doc = fetch_doc(docs_service, doc_id, "rejected")
    summary = summarize_changes(meta, revisions, snap, me)
    return {
        "id": doc_id,
        "url": doc_url(doc_id),
        "title": meta.get("name"),
        "me": me,
        "modifiedTime": meta.get("modifiedTime"),
        "lastModifyingUser": meta.get("lastModifyingUser"),
        "revisionId": doc.get("revisionId"),
        "revision_changed_since_snapshot": (
            snap.get("revisionId") != doc.get("revisionId") if snap else None
        ),
        **summary,
        "revision_count": len(revisions),
        "revisions": revisions[-20:],
        "note": (
            "Drive revisions are coarse (edits are batched) and the Docs revisionId "
            "is opaque; comments and suggestions do not always create a revision."
        ),
    }
