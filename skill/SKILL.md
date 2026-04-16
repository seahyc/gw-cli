---
name: google-workspace
description: Use this skill for Google Workspace tasks across Gmail, Drive, Docs, Sheets, Slides, Calendar, and Forms. It covers search, reading, writing, comments, replies, notes, exports, and rich Google Sheets cell formatting including inline text colors, hyperlinks, and per-run styles.
---

# Google Workspace

Use the `gw` CLI tool via the Bash tool for all Google Workspace operations. All commands output JSON to stdout.

Command pattern: `gw <service> <action> [args]`

Services: `auth`, `gmail`, `drive`, `docs`, `sheets`, `calendar`, `forms`, `slides`, `comments`

## Default approach

1. Identify the Workspace service the task requires.
2. Use the appropriate `gw` command for that service.
3. For writes, confirm the target and intended mutation before executing.
4. For any command's full usage: `gw <service> <action> --help`

## Service map

### Auth

```bash
gw auth login      # Opens browser for Google OAuth consent
gw auth status     # Show current auth state (user, scopes, token expiry)
gw auth logout     # Remove stored credentials from Keychain
```

Authentication notes:
- Credentials are stored in macOS Keychain and refreshed automatically.
- If a command returns a 401 or `RefreshError`, run `gw auth login` to re-authenticate.
- `GOOGLE_OAUTH_CLIENT_ID` and `GOOGLE_OAUTH_CLIENT_SECRET` env vars must be set for login.
- After login, subsequent commands work without env vars (credentials embed the client info).

### Gmail

```bash
gw gmail search <query> [--max-results N]
gw gmail read <message_id> [--format full|metadata|minimal]
gw gmail read-thread <thread_id>
gw gmail batch-read <id>...
gw gmail batch-read-threads <id>...
gw gmail read-attachment <message_id> <attachment_id> [--save-to PATH]
gw gmail draft --to ADDR --subject SUBJ --body BODY [--cc CC] [--bcc BCC] [--html] [--attach PATH]... [--in-reply-to ID] [--thread-id ID]
gw gmail send --to ADDR --subject SUBJ --body BODY [--cc CC] [--bcc BCC] [--html] [--attach PATH]...
gw gmail labels
gw gmail label-manage --action create|update|delete --name NAME [--label-id ID]
gw gmail label-modify <message_id>... --add LABEL --remove LABEL
gw gmail filters
gw gmail create-filter --criteria JSON --actions JSON
gw gmail delete-filter <filter_id>
```

Typical workflow:
```bash
# Search then read
gw gmail search "from:boss@company.com subject:review"
gw gmail read <message_id>

# Draft a reply
gw gmail draft --to boss@company.com --subject "Re: Review" --body "..." --in-reply-to <msg_id>

# Send directly
gw gmail send --to team@company.com --subject "Update" --body "..." --attach report.pdf
```

### Drive

```bash
gw drive search <query> [--max-results N] [--drive-id ID] [--corpora CORPORA]
gw drive list [--folder-id ID] [--max-results N]
gw drive read <file_id>
gw drive download-url <file_id> [--export-format FORMAT]
gw drive check-public <file_name>
gw drive create --name NAME --type file|folder [--parent ID] [--content PATH] [--mime-type MIME]
gw drive upload <local_path> [--name NAME] [--parent ID] [--mime-type MIME]
gw drive copy <file_id> [--name NAME] [--parent ID]
gw drive move <file_id> --to FOLDER_ID
gw drive export <file_id> --mime-type TYPE [--save-to-drive]
gw drive trash <file_id> [--untrash]
gw drive delete <file_id>
gw drive permissions <file_id>
gw drive share <file_id> --email EMAIL --role reader|writer|commenter [--type user|group|domain|anyone]
gw drive revisions <file_id>
gw drive shareable-link <file_id>
```

Typical workflow:
```bash
# Search and read
gw drive search "name contains 'report'"
gw drive read <file_id>

# Upload and share
gw drive upload ./report.pdf --name "Q1 Report"
gw drive share <file_id> --email colleague@company.com --role writer
```

### Docs

```bash
gw docs read <file_id> [--tab-id ID]
gw docs list-tabs <file_id>
gw docs create-tab <file_id> --title TITLE [--index N] [--parent-tab-id ID]
gw docs delete-tab <file_id> --tab-id ID
gw docs inspect <file_id> [--detailed]
gw docs search <query> [--max-results N]
gw docs list-in-folder [--folder-id ID] [--max-results N]
gw docs create --title TITLE [--content TEXT]
gw docs edit <file_id> --find TEXT --replace TEXT [--match-case] [--tab-id ID]
gw docs insert-text <file_id> --text TEXT --index N [--tab-id ID]
gw docs insert-table <file_id> --rows N --cols N [--index N] [--tab-id ID]
gw docs create-table <file_id> --data JSON [--index N] [--bold-headers] [--tab-id ID]
gw docs insert-image <file_id> --url URL [--index N] [--width N] [--height N] [--tab-id ID]
gw docs insert-list <file_id> --items JSON [--index N] [--ordered] [--tab-id ID]
gw docs insert-markdown <file_id> --file PATH | --content STRING [--tab-id ID] [--index N] [--replace]
gw docs insert-page-break <file_id> [--index N] [--tab-id ID]
gw docs insert-section-break <file_id> [--index N] [--type NEXT_PAGE|CONTINUOUS] [--tab-id ID]
gw docs insert-footnote <file_id> --index N --text TEXT [--tab-id ID]
gw docs delete-object <file_id> --object-id ID
gw docs update-paragraph-style <file_id> --start N --end N [style flags]
gw docs update-document-style <file_id> [margin/page/font flags]
gw docs manage-named-range <file_id> --action create|delete --name NAME [--start N --end N]
gw docs manage-table <file_id> --action ACTION --table-index N [flags]
gw docs debug-table <file_id> --table-index N
gw docs list-tables <file_id> [--tab-id ID]
gw docs set-table-column-widths <file_id> --table-index N --widths "W1,W2,..." [--unit PT] [--tab-id ID]
gw docs table-wrap-estimate <file_id> --table-index N [--widths "W1,W2,..."] [--font-size PT] [--tab-id ID]
gw docs batch-update <file_id> --requests JSON [--tab-id ID]
gw docs header-footer <file_id> --action get|create|delete [--type header|footer] [--content TEXT]
gw docs export-pdf <file_id> [--output PATH] [--folder-id ID]
```

#### Tabs

Google Docs supports multiple tabs per document. Use `list-tabs` to discover them, `create-tab` to add new ones, and `delete-tab` to remove them. Pass `--tab-id` on read/write commands to scope the operation to a specific tab; without it the commands target the default tab (and `read` returns all tabs concatenated, preserving historical behavior).

```bash
# Discover tabs
gw docs list-tabs <file_id>

# Create a new top-level tab and capture its ID
gw docs create-tab <file_id> --title "Draft notes"

# Create a nested child tab under an existing parent
gw docs create-tab <file_id> --title "Subsection" --parent-tab-id <parent_tab_id>

# Delete a tab by ID (Google Docs requires at least one tab to remain)
gw docs delete-tab <file_id> --tab-id <tab_id>

# Read or write a specific tab
gw docs read <file_id> --tab-id <tab_id>
gw docs insert-text <file_id> --text "Hello" --index 1 --tab-id <tab_id>
```

Typical workflow:
```bash
# Read and inspect structure
gw docs read <file_id>
gw docs inspect <file_id> --detailed

# Find and replace text
gw docs edit <file_id> --find "old text" --replace "new text"

# Insert structured content
gw docs create-table <file_id> --data '[["Name","Status"],["Alice","Done"]]' --bold-headers
```

#### Markdown -> native Docs formatting

`gw docs insert-markdown` converts a markdown file (or string) into native
Google Docs formatting: real headings, bullets, tables, inline bold, and inline
monospace for `` `code` ``. Useful for dropping a long markdown doc into a
Google Doc without the syntax leaking through.

```bash
# Replace the entire contents of a tab with a formatted markdown file
gw docs insert-markdown <file_id> --file ./brief.md --tab-id <tab_id> --replace

# Append markdown to the end of a doc (compute index from `inspect` or read)
gw docs insert-markdown <file_id> --file ./appendix.md --index 1234

# Inline content instead of a file
gw docs insert-markdown <file_id> --content "# Hello\n\nWorld" --replace
```

Supported subset: `#`..`######` headings, paragraphs, `-` bullet lists,
`> blockquotes` (rendered as italic + 36pt left indent), GitHub-style pipe
tables (first row bolded), `**bold**`, `` `code` `` (Roboto Mono). Horizontal
rules (`---`) are intentionally skipped — heading/paragraph spacing is
sufficient. Not supported: nested/ordered lists, images, links, fenced code
blocks, raw HTML.

#### Table column widths: inspect, set, preview wrapping

Three commands make it possible to see, change, and predict how tables lay out
without rebuilding them.

```bash
# Inspect every table in a tab: per-column widths, widthType (FIXED/EVEN),
# total table width vs inner page width, first-row preview, and the longest
# cell in each column (useful to judge whether a column is too narrow).
gw docs list-tables <file_id> --tab-id <tab_id>

# Set fixed widths column-by-column (comma-separated, PT by default).
# Width count must match the table's column count. Uses the underlying
# updateTableColumnProperties request so nothing else in the table is rebuilt.
gw docs set-table-column-widths <file_id> --table-index 1 --tab-id <tab_id> \
    --widths "30,45,105,145,145"

# Predict wrapping. Without --widths: evaluate the table's current widths.
# With --widths: evaluate a proposed layout before committing it. Returns
# per-row max_lines_in_row, per_col_lines, and a summary with
# total_wrap_penalty plus a rough recommended_widths_pt hint.
gw docs table-wrap-estimate <file_id> --table-index 1 --tab-id <tab_id>
gw docs table-wrap-estimate <file_id> --table-index 1 --tab-id <tab_id> \
    --widths "30,45,105,145,145"
```

Notes:
- Widths are treated as `FIXED_WIDTH` at unit `PT` (override with `--unit`).
- Wrap estimation assumes Roboto 11pt by default; pass `--font-size` (e.g. 10)
  to match a smaller body font. The heuristic uses an average glyph width, so
  it's a best-effort predictor, not pixel-perfect.
- The default tab's layout numbers come from `documentStyle` inside that tab
  (Google Docs nests page size and margins under `documentTab.documentStyle`
  when `includeTabsContent=True`).

#### Anchored comments on Docs

To anchor a comment to specific text in a Google Doc, use `gw comments create` with `--quoted-text` and `--service docs`:

```bash
gw comments create DOC_ID --content "This number needs verification" --quoted-text "2.1 million monthly active job seekers" --service docs
```

The comment will be pinned to the first occurrence of that exact text. Tips:
- `--quoted-text` must be an exact substring of the document content
- Keep it short but unique enough to match only the intended location
- Works on Google Docs only (not Sheets or Slides)

### Sheets

Two layers of commands:

**Simple value operations** (for ordinary tabular reads/writes):
```bash
gw sheets read <file_id> --range RANGE [--render FORMATTED_VALUE|UNFORMATTED_VALUE|FORMULA]
gw sheets batch-read <file_id> --ranges RANGE...
gw sheets write <file_id> --range RANGE --values JSON [--input-option USER_ENTERED|RAW] [--clear]
gw sheets find-replace <file_id> --find TEXT --replace TEXT [flags]
```

**Rich cell operations** (for inline formatting, notes, hyperlinks, per-run styles):
```bash
gw sheets read-cells <file_id> --range RANGE [--facets FACETS] [--include-empty]
gw sheets write-cells <file_id> --cells JSON [--mode patch|replace]
gw sheets transform <file_id> --range RANGE --operations JSON
```

**Formatting and structure:**
```bash
gw sheets format <file_id> --range RANGE [formatting flags]
gw sheets borders <file_id> --range RANGE --borders JSON
gw sheets merge/unmerge <file_id> --range RANGE
gw sheets conditional-format <file_id> --action add|update|delete [flags]
```

**Spreadsheet and tab management:**
```bash
gw sheets list [--max-results N]
gw sheets info <file_id>
gw sheets create --title TITLE [--sheet-names NAME...]
gw sheets add-tab/duplicate-tab/delete-tab/update-tab <file_id> [flags]
gw sheets insert-dimension/delete-dimension <file_id> --tab-id ID --dimension ROWS|COLUMNS --start N --end N
gw sheets resize/auto-resize/freeze/sort <file_id> [flags]
gw sheets validate/named-range/filter-view/protected-range <file_id> [flags]
```

Typical workflow:
```bash
# Read values
gw sheets read SPREADSHEET_ID --range "'Sheet 1'!A1:D10"

# Write values
gw sheets write SPREADSHEET_ID --range "'Sheet 1'!A1:B2" --values '[["Name","Age"],["Alice",30]]'

# Rich cell operations
gw sheets read-cells SPREADSHEET_ID --range "'Sheet 1'!B2:B8" --facets "value,format,text_runs,notes"

# Write rich cells
gw sheets write-cells SPREADSHEET_ID --cells '[{"a1":"Sheet1!B2","text":"Important","runs":[{"from":0,"to":9,"format":{"bold":true}}]}]'
```

### Calendar

```bash
gw calendar list
gw calendar events [--calendar-id ID] [--time-min ISO] [--time-max ISO] [--max-results N] [--query TEXT]
gw calendar create-event --summary TEXT --start ISO --end ISO [--calendar-id ID] [--attendees EMAIL...] [--recurrence RRULE...] [--add-meet] [--reminders JSON]
gw calendar modify-event <event_id> [--calendar-id ID] [modification flags]
gw calendar delete-event <event_id> [--calendar-id ID]
```

Typical workflow:
```bash
# Check schedule
gw calendar events --time-min "2026-03-30T00:00:00Z" --time-max "2026-03-31T00:00:00Z"

# Create event with Meet
gw calendar create-event --summary "Team sync" --start "2026-04-01T10:00:00+08:00" --end "2026-04-01T11:00:00+08:00" --attendees alice@co.com bob@co.com --add-meet
```

### Forms

```bash
gw forms create --title TITLE [--description TEXT]
gw forms read <form_id>
gw forms responses <form_id> [--max-results N]
gw forms response <form_id> <response_id>
gw forms publish-settings <form_id> [--template BOOL] [--require-auth BOOL]
```

Typical workflow:
```bash
gw forms read <form_id>
gw forms responses <form_id> --max-results 50
```

### Slides

```bash
gw slides create [--title TITLE]
gw slides read <file_id>
gw slides page <file_id> <page_id>
gw slides thumbnail <file_id> <page_id> [--size SMALL|MEDIUM|LARGE]
gw slides batch-update <file_id> --requests JSON
```

Typical workflow:
```bash
gw slides read <file_id>
gw slides page <file_id> <page_id>
gw slides batch-update <file_id> --requests '[...]'
```

### Comments (works on Docs, Sheets, Slides via Drive API)

```bash
gw comments list <file_id>
gw comments create <file_id> --content TEXT [--quoted-text TEXT] [--service docs]
gw comments reply <file_id> --comment-id ID --content TEXT
gw comments resolve <file_id> --comment-id ID
gw comments edit <file_id> --comment-id ID --content TEXT
gw comments delete <file_id> --comment-id ID
gw comments edit-reply <file_id> --comment-id ID --reply-id ID --content TEXT
gw comments delete-reply <file_id> --comment-id ID --reply-id ID
```

## Rules

- Use `gw` via the Bash tool for all Google Workspace operations.
- All output is JSON -- parse it as needed.
- For any command's full usage: `gw <service> <action> --help`
- Prefer specific commands over batch operations when possible.
- For rich Sheets formatting (inline styles, notes, hyperlinks), use `read-cells`/`write-cells` over `read`/`write`.
- Use simple Sheets value commands (`read`/`write`) for ordinary tabular updates.
- Keep mutations minimal: smallest document range, cell range, or file scope that solves the task.
- For write operations, state the specific target and intended mutation before executing.
