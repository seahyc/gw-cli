# Docs collaboration awareness

Use these commands whenever a Google Doc is shared with other people. Before
you edit, check what collaborators changed. After you edit, check that your
change did not detach their comments. `<doc>` accepts a document ID or a full
Docs URL. Every command prints JSON with `id` and `url`.

## Before editing a shared doc

```sh
gw docs changes <doc>             # has anyone else edited since my last snapshot?
gw docs diff <doc>                # what changed since the latest snapshot
gw comments list <doc> --open --chrono
gw docs suggestions <doc>
gw docs snapshot <doc>            # new baseline before you write
```

## Commands

| Command | Output |
|---|---|
| `gw docs suggestions <doc>` | Pending suggestions, grouped by suggestion id. Each has `kinds` (insertion, deletion, text_style, paragraph_style, row_insertion, ...), `inserted`, `deleted`, `start`/`end`, and a `snippet` of the paragraph with `[+inserted+]` and `[-deleted-]` markers. The Docs API does not report suggestion authors (`authors_note`). |
| `gw comments list <file> [--open] [--since ISO] [--author NAME] [--chrono]` | Structured threads: `id, author, created, modified, resolved, quoted, anchor_status, content, replies[], url`. The default order is newest first; `--chrono` sorts oldest first. `--since` keeps a thread when the comment or any reply was created at or after the given time. `--author` matches a substring of the comment author's name or email. |
| `gw docs snapshot <doc>` | Writes `~/.cache/gw/docs/<doc_id>/<UTC timestamp>.json` and a `latest` pointer. The snapshot records text lines, a structural summary, `revisionId`, the Drive `modifiedTime` and `lastModifyingUser`, comments, and suggestions. |
| `gw docs diff <doc> [--since latest\|<timestamp>\|<revisionId>]` | Paragraph-level unified diff (`text.diff`, one line per table row), plus comments `new/changed/resolved/reopened/deleted` and suggestions `new/gone`. |
| `gw docs changes <doc>` | Drive `modifiedTime`, `lastModifyingUser`, the last 20 revisions, `others_edited_since_snapshot`, and `other_editors`. |
| `gw docs export <doc> --format md\|text\|email-html [--suggestions accepted\|inline\|rejected] [--out PATH]` | Rendered `content`. With `--out`, the command writes the file and returns `out` and `bytes`. |

`anchor_status` values:

- `attached`: the quoted text still exists in the doc. Whitespace differences are ignored.
- `detached`: the quoted text no longer exists. An edit removed or rewrote it, so tell the comment author.
- `unanchored`: the comment is a whole-document comment.
- `unknown`: the file is not a Google Doc.

## Export details

- `--suggestions` defaults to `rejected`, which shows the doc without pending suggestions. `accepted` previews the doc with every suggestion accepted. `inline` shows both sides: `{++ins++}`/`{--del--}` in md, `[+ins+]`/`[-del-]` in text, and `<ins>`/`<del>` in HTML.
- `md`: Docs headings become `#` headings. A paragraph that is entirely bold becomes `**heading**`. Bullets become `- ` and numbered lists become `1. `. Tables become GitHub tables with numeric columns right-aligned. Bold, italic, strikethrough, and links are preserved.
- `text`: plain text with space-aligned tables. Numeric columns are right-aligned.
- `email-html`: an Outlook-safe fragment. All styles are inline, with no `<style>` block. Tables use `width`, `cellpadding`, and `border` attributes. Text uses the system font stack. Headings and bold-only lines are sentence-cased; acronyms and mixed-case words such as API and GitHub are kept. Italic-only paragraphs, such as footnotes, render as grey muted text.

## Caveats

- Drive `modifiedTime` can lag behind Docs edits by minutes. `changes` therefore compares revisions against the time the snapshot was taken.
- Drive revisions are coarse because edits are batched. A new comment or suggestion does not always create a revision.
- A suggestion that disappears from `diff` (`suggestions.gone`) was accepted or rejected. The API does not say which.

## For write commands

Call `gw.services.docs_collab.snapshot(docs_service, doc_id)` before a
mutation. It returns the snapshot path, and it builds its own Drive client
when you do not pass `drive_service=`.
