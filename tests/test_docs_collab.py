"""Unit tests for the pure functions in gw.services.docs_collab."""

import json
from html.parser import HTMLParser
from pathlib import Path

import pytest

from gw.services import docs_collab as dc

FIXTURES = Path(__file__).parent / "fixtures"


def _run(text, **kw):
    return {"textRun": {"content": text, "textStyle": kw}}


def _para(*runs, style="NORMAL_TEXT", bullet=None):
    p = {"paragraphStyle": {"namedStyleType": style}, "elements": list(runs)}
    if bullet:
        p["bullet"] = {"listId": bullet}
    return {"paragraph": p}


def _cell(text):
    return {"content": [_para(_run(text + "\n"))]}


def _table(rows):
    return {"table": {"tableRows": [{"tableCells": [_cell(c) for c in r]} for r in rows]}}


@pytest.fixture
def doc():
    return {
        "title": "T",
        "lists": {
            "b": {"listProperties": {"nestingLevels": [{"glyphSymbol": "*"}]}},
            "n": {"listProperties": {"nestingLevels": [{"glyphType": "DECIMAL"}]}},
        },
        "body": {"content": [
            _para(_run("Plan\n"), style="HEADING_1"),
            _para(_run("Summary", bold=True), _run("\n", bold=True)),
            _para(_run("We grew "), _run("fast", italic=True), _run(" and "),
                  _run("well", bold=True), _run(".\n")),
            _para(_run("first\n"), bullet="b"),
            _para(_run("second\n"), bullet="b"),
            _para(_run("step one\n"), bullet="n"),
            _para(_run("\n")),
            _table([["Metric", "Q1", "Q2"], ["Revenue", "$1,200", "$1,500"], ["Churn", "3%", "2.5%"]]),
            _para(_run("Source: internal.\n", italic=True)),
        ]},
    }


# -- suggestions -------------------------------------------------------------

def test_group_suggestions_fixture():
    doc = json.loads((FIXTURES / "doc_with_suggestions.json").read_text())
    groups = {g["id"]: g for g in dc.group_suggestions(doc)}
    assert list(groups) == ["suggest.a", "suggest.b", "suggest.c", "suggest.d"]

    a = groups["suggest.a"]
    assert a["kinds"] == ["deletion", "insertion"]
    assert a["deleted"] == "$1M "
    assert a["inserted"] == "$2M "
    assert (a["start"], a["end"]) == (11, 19)
    assert a["snippet"] == "Revenue is [-$1M -][+$2M +]this year."

    b = groups["suggest.b"]
    assert b["kinds"] == ["insertion"] and b["inserted"] == "two more\n"
    assert b["snippet"] == "Hire [+two more+]"

    assert groups["suggest.c"]["kinds"] == ["text_style"]
    d = groups["suggest.d"]
    assert (d["deleted"], d["inserted"]) == ("old\n", "new\n")


def test_group_suggestions_empty(doc):
    assert dc.group_suggestions(doc) == []


# -- comments ----------------------------------------------------------------

def test_anchor_status():
    text = "Revenue grew  fast\nthis year &amp; next"
    assert dc.anchor_status("grew fast", text) == "attached"
    assert dc.anchor_status("fast this year", text) == "attached"
    assert dc.anchor_status("shrank", text) == "detached"
    assert dc.anchor_status("", text) == "unanchored"
    assert dc.anchor_status("x", None) == "unknown"


def test_normalize_comments_filters_and_sort():
    raw = [
        {"id": "c2", "author": {"displayName": "Bob"}, "createdTime": "2026-01-02T00:00:00Z",
         "content": "later", "resolved": True, "quotedFileContent": {"value": "gone text"}},
        {"id": "c1", "author": {"displayName": "Alice", "emailAddress": "a@x.com"},
         "createdTime": "2026-01-01T00:00:00Z", "content": "first",
         "quotedFileContent": {"value": "hello &amp; world"},
         "replies": [{"id": "r1", "author": {"displayName": "Bob"},
                      "createdTime": "2026-01-05T00:00:00Z", "content": "ok"}]},
        {"id": "c3", "deleted": True, "createdTime": "2026-01-03T00:00:00Z"},
    ]
    text = "hello & world"
    out = dc.normalize_comments(raw, text, file_id="D", chrono=True)
    assert [c["id"] for c in out] == ["c1", "c2"]
    assert out[0]["anchor_status"] == "attached"
    assert out[0]["quoted"] == "hello & world"
    assert out[1]["anchor_status"] == "detached"
    assert out[0]["replies"][0]["author"] == "Bob"
    assert out[0]["url"].endswith("/document/d/D/edit?disco=c1")

    assert [c["id"] for c in dc.normalize_comments(raw, text)] == ["c2", "c1"]
    assert [c["id"] for c in dc.normalize_comments(raw, text, open_only=True)] == ["c1"]
    assert [c["id"] for c in dc.normalize_comments(raw, text, author="a@X")] == ["c1"]
    # since matches on reply time as well as comment time
    assert [c["id"] for c in dc.normalize_comments(raw, text, since="2026-01-04")] == ["c1"]
    assert dc.normalize_comments(raw, text, since="2026-02-01T00:00:00Z") == []


# -- diffing -----------------------------------------------------------------

def test_diff_lines_and_unified_diff(doc):
    blocks = dc.extract_blocks(doc)
    lines = dc.diff_lines(blocks)
    assert lines[0] == "# Plan"
    assert "- first" in lines
    assert "| Revenue | $1,200 | $1,500 |" in lines
    assert "" not in lines

    new = [l.replace("$1,500", "$1,700") for l in lines]
    d = dc.unified_diff(lines, new)
    assert d["changed"] and d["added"] == 1 and d["removed"] == 1
    assert "-| Revenue | $1,200 | $1,500 |" in d["diff"]
    assert "+| Revenue | $1,200 | $1,700 |" in d["diff"]
    assert dc.unified_diff(lines, lines)["changed"] is False


def test_diff_comments_and_suggestions():
    old = [
        {"id": "a", "content": "x", "resolved": False, "anchor_status": "attached", "replies": []},
        {"id": "b", "content": "y", "resolved": False, "anchor_status": "attached", "replies": []},
        {"id": "gone", "content": "z", "resolved": False, "anchor_status": "attached", "replies": []},
    ]
    new = [
        {"id": "a", "content": "x", "resolved": True, "anchor_status": "attached", "replies": []},
        {"id": "b", "content": "y", "resolved": False, "anchor_status": "detached",
         "replies": [{"id": "r"}]},
        {"id": "c", "content": "n", "resolved": False, "anchor_status": "attached", "replies": []},
    ]
    d = dc.diff_comments(old, new)
    assert [c["id"] for c in d["new"]] == ["c"]
    assert [c["id"] for c in d["resolved"]] == ["a"]
    assert [c["id"] for c in d["changed"]] == ["b"]
    assert d["changed"][0]["anchor_status_before"] == "attached"
    assert [c["id"] for c in d["deleted"]] == ["gone"]

    s = dc.diff_suggestions([{"id": "s1"}], [{"id": "s1"}, {"id": "s2"}])
    assert [x["id"] for x in s["new"]] == ["s2"] and s["gone"] == []


def test_summarize_changes():
    snap = {"snapshot": "20260101T000000Z", "taken_at": "2026-01-01T00:00:00Z"}
    revs = [
        {"id": "1", "modifiedTime": "2025-12-31T00:00:00Z", "lastModifyingUser": {"displayName": "Co", "me": False}},
        {"id": "2", "modifiedTime": "2026-01-02T00:00:00Z", "lastModifyingUser": {"displayName": "Me", "me": True}},
    ]
    # Drive modifiedTime lagging behind the revisions must not hide edits.
    meta = {"modifiedTime": "2025-12-31T00:00:00Z", "lastModifyingUser": {"me": True}}
    s = dc.summarize_changes(meta, revs, snap, "me@x.com")
    assert s["modified_since_snapshot"] and not s["others_edited_since_snapshot"]
    revs.append({"id": "3", "modifiedTime": "2026-01-03T00:00:00Z",
                 "lastModifyingUser": {"displayName": "Co", "emailAddress": "co@x.com"}})
    s = dc.summarize_changes(meta, revs, snap, "me@x.com")
    assert s["others_edited_since_snapshot"] and s["other_editors"] == ["Co"]
    assert dc.summarize_changes(meta, revs, None, "me@x.com")["others_edited_since_snapshot"] is None


# -- renderers ---------------------------------------------------------------

def test_render_markdown(doc):
    md = dc.render_markdown(dc.extract_blocks(doc))
    assert md == (
        "# Plan\n"
        "\n"
        "**Summary**\n"
        "\n"
        "We grew *fast* and **well**.\n"
        "\n"
        "- first\n"
        "- second\n"
        "1. step one\n"
        "\n"
        "| Metric | Q1 | Q2 |\n"
        "| --- | ---: | ---: |\n"
        "| Revenue | $1,200 | $1,500 |\n"
        "| Churn | 3% | 2.5% |\n"
        "\n"
        "*Source: internal.*\n"
    )


def test_markdown_inline_suggestions_and_escaping():
    doc = {"body": {"content": [_para(
        {"textRun": {"content": "a|b *x* ", "textStyle": {}}},
        {"textRun": {"content": "new", "textStyle": {}, "suggestedInsertionIds": ["s"]}},
        {"textRun": {"content": "\n", "textStyle": {}}},
    )]}}
    assert dc.render_markdown(dc.extract_blocks(doc)) == "a|b \\*x\\* {++new++}\n"


def test_render_text_aligns_tables(doc):
    txt = dc.render_text(dc.extract_blocks(doc))
    assert "Metric   Q1      Q2" in txt
    assert "Revenue  $1,200  $1,500" in txt
    assert "Churn        3%    2.5%" in txt
    assert "- first" in txt


def test_render_email_html(doc):
    out = dc.render_email_html(dc.extract_blocks(doc))
    assert "<style" not in out
    assert 'cellpadding="6"' in out and 'width="100%"' in out
    assert "color:#6b7280" in out  # italic footnote muted
    assert '<td align="right"' in out
    assert "<ul" in out and "<ol" in out

    class P(HTMLParser):
        stack = []
        def handle_starttag(self, tag, attrs):
            if tag not in ("br",):
                self.stack.append(tag)
        def handle_endtag(self, tag):
            assert self.stack.pop() == tag
    p = P()
    p.feed(out)
    p.close()
    assert p.stack == []


def test_sentence_case():
    assert dc.sentence_case("QUARTERLY REVENUE UPDATE") == "Quarterly revenue update"
    assert dc.sentence_case("Next Steps For The API") == "Next steps for the API"
    assert dc.sentence_case("our GitHub plan") == "Our GitHub plan"


def test_numeric_columns():
    rows = [["a", "b", "c"], ["x", "1,000", "(3.5%)"], ["y", "", "-2"]]
    assert dc.numeric_columns(rows) == [False, True, True]


def test_parse_doc_id():
    assert dc.parse_doc_id("https://docs.google.com/document/d/abc_123-X/edit#h") == "abc_123-X"
    assert dc.parse_doc_id("abc") == "abc"


def test_load_snapshot_refs(tmp_path, monkeypatch):
    monkeypatch.setenv("GW_CACHE_DIR", str(tmp_path))
    d = tmp_path / "docs" / "D"
    d.mkdir(parents=True)
    (d / "20260101T000000Z.json").write_text(json.dumps({"snapshot": "20260101T000000Z", "revisionId": "r1"}))
    (d / "20260102T000000Z.json").write_text(json.dumps({"snapshot": "20260102T000000Z", "revisionId": "r2"}))
    (d / "latest").write_text("20260102T000000Z.json\n")
    assert dc.load_snapshot("D")["revisionId"] == "r2"
    assert dc.load_snapshot("D", "20260101")["revisionId"] == "r1"
    assert dc.load_snapshot("D", "r1")["snapshot"] == "20260101T000000Z"
    with pytest.raises(FileNotFoundError):
        dc.load_snapshot("D", "nope")


def test_sentence_case_keeps_proper_nouns():
    # "Glints" is not a common heading word, so it keeps its capital.
    assert dc.sentence_case("Glints Revenue Update") == "Glints revenue update"
    assert dc.sentence_case("Next Steps For Glints In Jakarta") == "Next steps for Glints in Jakarta"


def test_sentence_case_keep_list():
    assert dc.sentence_case("Review Plan", keep=["plan"]) == "Review Plan"
    assert dc.sentence_case("GLINTS REVENUE UPDATE", keep=["glints"]) == "Glints revenue update"
