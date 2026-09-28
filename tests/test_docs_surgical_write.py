"""Unit tests for the pure diff/skip logic behind `docs replace-markdown`'s
default surgical (comment- and suggestion-preserving) mode.

No network/auth: these exercise _paragraph_has_pending_suggestion,
_old_body_items, and the opcode-level pending-suggestion skip in
_replace_markdown_preserving_comments purely against fixture JSON shaped
like a Docs API `documents.get(suggestionsViewMode="SUGGESTIONS_INLINE")`
response.
"""

import difflib

from gw.services.docs import (
    _old_body_items,
    _paragraph_has_pending_suggestion,
)


def _plain_paragraph(text, start=1):
    end = start + len(text) + 1
    return {
        "startIndex": start,
        "endIndex": end,
        "paragraph": {
            "elements": [
                {
                    "startIndex": start,
                    "endIndex": end,
                    "textRun": {"content": text + "\n"},
                }
            ]
        },
    }


def _suggested_paragraph(text, start=1, suggestion_id="s1"):
    end = start + len(text) + 1
    return {
        "startIndex": start,
        "endIndex": end,
        "paragraph": {
            "elements": [
                {
                    "startIndex": start,
                    "endIndex": end,
                    "textRun": {
                        "content": text + "\n",
                        "suggestedInsertionIds": [suggestion_id],
                    },
                }
            ]
        },
    }


def test_paragraph_has_pending_suggestion_true_for_suggested_insertion():
    el = _suggested_paragraph("pending edit")
    assert _paragraph_has_pending_suggestion(el) is True


def test_paragraph_has_pending_suggestion_true_for_suggested_deletion():
    el = _plain_paragraph("deleted text")
    el["paragraph"]["elements"][0]["textRun"]["suggestedDeletionIds"] = ["d1"]
    assert _paragraph_has_pending_suggestion(el) is True


def test_paragraph_has_pending_suggestion_false_for_plain_text():
    el = _plain_paragraph("ordinary text")
    assert _paragraph_has_pending_suggestion(el) is False


def test_old_body_items_tags_has_suggestion_flag():
    body = [
        _plain_paragraph("Heading", start=1),
        _plain_paragraph("Normal paragraph", start=9),
        _suggested_paragraph("Paragraph with pending suggestion", start=27),
    ]
    items = _old_body_items(body)
    assert [it["has_suggestion"] for it in items] == [False, False, True]


def test_diff_opcode_over_suggested_paragraph_is_flagged_for_skip():
    """Mirrors the skip check in _replace_markdown_preserving_comments:
    an opcode touching any old item with has_suggestion=True must be
    identifiable so the caller can leave it untouched and report it.
    """
    old_body = [
        _plain_paragraph("Heading", start=1),
        _suggested_paragraph("Old text with a pending suggestion", start=9),
        _plain_paragraph("Trailing paragraph", start=45),
    ]
    old_items = _old_body_items(old_body)
    old_keys = [it["key"] for it in old_items]
    new_keys = ["Heading", "New text with no pending suggestion", "Trailing paragraph"]

    opcodes = difflib.SequenceMatcher(None, old_keys, new_keys, autojunk=False).get_opcodes()

    def opcode_has_suggestion(i1, i2):
        return any(old_items[k].get("has_suggestion") for k in range(i1, i2))

    flagged = [
        (tag, i1, i2)
        for tag, i1, i2, _j1, _j2 in opcodes
        if tag != "equal" and opcode_has_suggestion(i1, i2)
    ]
    assert flagged == [("replace", 1, 2)]


def test_diff_opcode_without_suggestion_is_not_flagged():
    old_body = [
        _plain_paragraph("Heading", start=1),
        _plain_paragraph("Ordinary paragraph to change", start=9),
    ]
    old_items = _old_body_items(old_body)
    old_keys = [it["key"] for it in old_items]
    new_keys = ["Heading", "Ordinary paragraph, changed"]

    opcodes = difflib.SequenceMatcher(None, old_keys, new_keys, autojunk=False).get_opcodes()

    def opcode_has_suggestion(i1, i2):
        return any(old_items[k].get("has_suggestion") for k in range(i1, i2))

    flagged = [
        (tag, i1, i2)
        for tag, i1, i2, _j1, _j2 in opcodes
        if tag != "equal" and opcode_has_suggestion(i1, i2)
    ]
    assert flagged == []
