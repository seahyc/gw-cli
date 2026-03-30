# Google Workspace CLI Migration (`gw`)

## Summary

Replace the MCP server with a pure CLI tool `gw`. The skill teaches Claude to shell out via Bash instead of calling MCP tools. All three layers (Google API usage, CLI surface, skill documentation) stay fully in sync.

## Architecture

```
gw gmail search "invoice"
  └─> cli/gmail.py (argparse)
        └─> services/gmail.py (business logic, unchanged from current *_tools.py)
              └─> auth/google_auth.py (OAuth + Keychain, unchanged)
```

### Directory Structure (after migration)

```
google-workspace-mcp/  (rename to google-workspace-cli later if desired)
├── gw/
│   ├── __init__.py
│   ├── __main__.py              # Entry point: argparse root + service subparsers
│   ├── auth/                    # KEPT from current auth/
│   │   ├── google_auth.py       # OAuth flow, token refresh, credential loading
│   │   ├── credential_store.py  # Keychain storage
│   │   ├── scopes.py            # Service-specific OAuth scopes
│   │   ├── oauth_config.py      # OAuth client config
│   │   └── oauth_callback_server.py  # Browser OAuth redirect handler
│   ├── services/                # ADAPTED from current gmail/, gdrive/, etc.
│   │   ├── gmail.py
│   │   ├── drive.py
│   │   ├── docs.py
│   │   ├── sheets.py
│   │   ├── calendar.py
│   │   ├── forms.py
│   │   ├── slides.py
│   │   └── _helpers/
│   │       ├── sheets_helpers.py
│   │       ├── docs_helpers.py
│   │       ├── drive_helpers.py
│   │       └── docs_managers/
│   │           ├── batch_operation_manager.py
│   │           ├── table_operation_manager.py
│   │           ├── header_footer_manager.py
│   │           └── validation_manager.py
│   └── cli/                     # NEW: thin argparse → service function wrappers
│       ├── __init__.py
│       ├── gmail.py
│       ├── drive.py
│       ├── docs.py
│       ├── sheets.py
│       ├── calendar.py
│       ├── forms.py
│       └── slides.py
├── pyproject.toml               # Updated: add [project.scripts] entry
└── tests/                       # Existing tests adapted
```

### What's Deleted

```
core/server.py                   # FastMCP server
core/tool_registry.py            # MCP tool registration
core/tool_tier_loader.py         # MCP tier filtering
core/tool_tiers.yaml             # MCP tier definitions
auth/service_decorator.py        # @require_google_service MCP decorator
auth/oauth21_session_store.py    # MCP session management
main.py                          # MCP entry point
```

### Config Cleanup

Remove from `~/.codex/config.toml`:
```toml
[mcp_servers.google-workspace]     # entire section
```

### Shell Wrapper

`~/.local/bin/gw`:
```bash
#!/bin/bash
exec uv run --directory /Users/yingcong/Code/mcp-servers/google-workspace-mcp python -m gw "$@"
```

## CLI Interface

All commands output JSON to stdout. Errors go to stderr with non-zero exit code.

### Global Flags

```
gw --help
gw <service> --help
gw <service> <action> --help
```

### Gmail (12 commands)

```
gw gmail search <query> [--max-results N]
gw gmail read <message-id> [--format full|metadata|minimal]
gw gmail read-thread <thread-id> [--format full|metadata|minimal]
gw gmail batch-read <id>... [--format full|metadata|minimal]
gw gmail batch-read-threads <id>... [--format full|metadata|minimal]
gw gmail read-attachment <message-id> <attachment-id> [--save-to PATH]
gw gmail draft --to ADDR --subject SUBJ --body BODY [--cc ADDR] [--bcc ADDR] [--html] [--attach PATH]... [--in-reply-to MSG-ID] [--thread-id THREAD-ID]
gw gmail send --to ADDR --subject SUBJ --body BODY [--cc ADDR] [--bcc ADDR] [--html] [--attach PATH]...
gw gmail labels
gw gmail label-manage --action create|update|delete --name NAME [--new-name NAME] [--visibility show|hide]
gw gmail label-modify <message-id>... --add LABEL --remove LABEL
gw gmail batch-label-modify --query QUERY --add LABEL --remove LABEL
gw gmail filters
gw gmail create-filter --criteria JSON --actions JSON
gw gmail delete-filter <filter-id>
```

### Drive (14 commands)

```
gw drive search <query> [--max-results N] [--drive-id ID] [--corpora user|allDrives]
gw drive list [--folder-id ID] [--max-results N] [--drive-id ID]
gw drive read <file-id>
gw drive download-url <file-id>
gw drive check-public <file-id>
gw drive create --name NAME --type file|folder [--parent ID] [--content PATH] [--mime-type MIME]
gw drive upload <local-path> [--name NAME] [--parent ID] [--mime-type MIME]
gw drive copy <file-id> --name NAME [--parent ID]
gw drive move <file-id> --to FOLDER-ID
gw drive export <file-id> --mime-type TYPE [--output PATH]
gw drive trash <file-id>
gw drive permissions <file-id>
gw drive share <file-id> --email ADDR --role reader|writer|commenter [--type user|group|domain]
gw drive revisions <file-id>
```

### Docs (21 commands)

```
gw docs read <file-id>
gw docs inspect <file-id>
gw docs edit <file-id> --find TEXT --replace TEXT [--match-case]
gw docs insert-text <file-id> --text TEXT --index N
gw docs insert-table <file-id> --rows N --cols N [--index N]
gw docs create-table <file-id> --data JSON [--index N]
gw docs insert-image <file-id> --url URL [--index N] [--width N] [--height N]
gw docs insert-list <file-id> --items JSON [--index N] [--ordered]
gw docs insert-page-break <file-id> [--index N]
gw docs insert-section-break <file-id> [--index N] [--type NEXT_PAGE|CONTINUOUS]
gw docs insert-footnote <file-id> --index N --text TEXT
gw docs delete-object <file-id> --object-id ID
gw docs update-paragraph-style <file-id> --range START-END --style JSON
gw docs update-document-style <file-id> --style JSON
gw docs manage-named-range <file-id> --action create|delete --name NAME [--range START-END]
gw docs manage-table <file-id> --action insert-row|insert-col|delete-row|delete-col|merge|unmerge --table-index N [--index N]
gw docs debug-table <file-id> --table-index N
gw docs batch-update <file-id> --requests JSON
gw docs header-footer <file-id> --action get|create|delete [--type header|footer] [--section-id ID] [--content TEXT]
gw docs comment <file-id> --content TEXT [--quoted-text TEXT]
gw docs comments <file-id>
gw docs reply <file-id> --comment-id ID --content TEXT
gw docs export-pdf <file-id> --output PATH
```

Note: Comment tools (`comment`, `comments`, `reply`) are currently listed in `tool_tiers.yaml` but **not implemented** in the codebase. These must be implemented as part of this migration using the Google Drive Comments API (comments on Docs/Sheets/Slides all go through the Drive API).

### Sheets (29 commands)

```
# Simple value operations
gw sheets read <file-id> --range RANGE
gw sheets batch-read <file-id> --ranges RANGE...
gw sheets write <file-id> --range RANGE --values JSON
gw sheets find-replace <file-id> --find TEXT --replace TEXT [--sheet-id ID] [--match-case] [--match-entire-cell] [--use-regex]

# Structured cell operations
gw sheets read-cells <file-id> --range RANGE [--facets value,formatted_value,format,text_runs,notes] [--include-empty]
gw sheets write-cells <file-id> --cells JSON
gw sheets transform <file-id> --range RANGE --transforms JSON

# Formatting
gw sheets format <file-id> --range RANGE --format JSON
gw sheets borders <file-id> --range RANGE --borders JSON
gw sheets merge <file-id> --range RANGE [--type MERGE_ALL|MERGE_COLUMNS|MERGE_ROWS]
gw sheets unmerge <file-id> --range RANGE
gw sheets conditional-format <file-id> --action add|update|delete [--range RANGE] [--rule JSON] [--index N]

# Structure
gw sheets create --title TITLE
gw sheets add-tab <file-id> --title TITLE
gw sheets duplicate-tab <file-id> --tab-id ID [--name NAME]
gw sheets delete-tab <file-id> --tab-id ID
gw sheets update-tab <file-id> --tab-id ID [--title NAME] [--hidden BOOL]
gw sheets insert-dimension <file-id> --tab-id ID --dimension ROWS|COLUMNS --start N --end N
gw sheets delete-dimension <file-id> --tab-id ID --dimension ROWS|COLUMNS --start N --end N
gw sheets resize <file-id> --tab-id ID --dimension ROWS|COLUMNS --start N --end N --size N
gw sheets auto-resize <file-id> --tab-id ID --dimension ROWS|COLUMNS --start N --end N
gw sheets freeze <file-id> --tab-id ID [--rows N] [--cols N]
gw sheets sort <file-id> --range RANGE --column N [--ascending]

# Data management
gw sheets validate <file-id> --range RANGE --rule JSON
gw sheets named-range <file-id> --action create|update|delete --name NAME [--range RANGE]
gw sheets filter-view <file-id> --action create|get|update|delete [--filter JSON]
gw sheets protected-range <file-id> --action create|update|delete [--range RANGE] [--editors JSON]

# Comments (TO BE IMPLEMENTED)
gw sheets comment <file-id> --content TEXT [--anchor RANGE]
gw sheets comments <file-id>
gw sheets reply <file-id> --comment-id ID --content TEXT
```

### Calendar (5 commands)

```
gw calendar list
gw calendar events <calendar-id> [--time-min ISO] [--time-max ISO] [--max-results N] [--query TEXT]
gw calendar create-event <calendar-id> --summary TEXT --start ISO --end ISO [--description TEXT] [--location TEXT] [--attendees ADDR...] [--add-meet] [--reminders JSON] [--recurrence RRULE]
gw calendar modify-event <calendar-id> <event-id> [--summary TEXT] [--start ISO] [--end ISO] [--description TEXT] [--location TEXT] [--attendees ADDR...] [--add-meet] [--reminders JSON]
gw calendar delete-event <calendar-id> <event-id>
```

Note: `--attendees` and `--recurrence` are **not currently implemented** in the service layer. These must be added as part of this migration.

### Forms (5 commands)

```
gw forms create --title TITLE
gw forms read <form-id>
gw forms responses <form-id> [--max-results N]
gw forms response <form-id> <response-id>
gw forms publish-settings <form-id> [--is-template BOOL] [--allow-anonymous BOOL]
```

### Slides (8 commands)

```
gw slides create --title TITLE
gw slides read <presentation-id>
gw slides page <presentation-id> <page-id>
gw slides thumbnail <presentation-id> <page-id> [--output PATH]
gw slides batch-update <presentation-id> --requests JSON

# Comments (TO BE IMPLEMENTED)
gw slides comment <presentation-id> --content TEXT
gw slides comments <presentation-id>
gw slides reply <presentation-id> --comment-id ID --content TEXT
```

## Implementation Sync Matrix

Every row must have all three columns filled. If any column is empty, it's a bug.

### Legend
- **Exists** = currently implemented and working
- **Add** = must be implemented in this migration
- **Port** = move from MCP tool to CLI command (logic exists, needs new wrapper)

| Command | Service Layer | CLI | Skill |
|---------|--------------|-----|-------|
| **Gmail** | | | |
| gmail search | Exists | Port | Port |
| gmail read | Exists | Port | Port |
| gmail read-thread | Exists | Port | Port |
| gmail batch-read | Exists | Port | Port |
| gmail batch-read-threads | Exists | Port | Port |
| gmail read-attachment | Exists | Port | Port |
| gmail draft | Exists | Port | Port |
| gmail send | **Add** | Add | Add |
| gmail labels | Exists | Port | Port |
| gmail label-manage | Exists | Port | Port |
| gmail label-modify | Exists | Port | Port |
| gmail batch-label-modify | Exists | Port | Port |
| gmail filters | Exists | Port | Port |
| gmail create-filter | **Add** | Add | Add |
| gmail delete-filter | **Add** | Add | Add |
| **Drive** | | | |
| drive search | Exists | Port | Port |
| drive list | Exists | Port | Port |
| drive read | Exists | Port | Port |
| drive download-url | Exists | Port | Port |
| drive check-public | Exists | Port | Port |
| drive create | Exists | Port | Port |
| drive upload | **Add** | Add | Add |
| drive copy | Exists | Port | Port |
| drive move | Exists | Port | Port |
| drive export | Exists | Port | Port |
| drive trash | Exists | Port | Port |
| drive permissions | Exists | Port | Port |
| drive share | **Add** | Add | Add |
| drive revisions | Exists | Port | Port |
| **Docs** | | | |
| docs read | Exists | Port | Port |
| docs inspect | Exists | Port | Port |
| docs edit | Exists | Port | Port |
| docs insert-text | Exists | Port | Port |
| docs insert-table | Exists | Port | Port |
| docs create-table | Exists | Port | Port |
| docs insert-image | Exists | Port | Port |
| docs insert-list | Exists | Port | Port |
| docs insert-page-break | Exists | Port | Port |
| docs insert-section-break | Exists | Port | Port |
| docs insert-footnote | Exists | Port | Port |
| docs delete-object | Exists | Port | Port |
| docs update-paragraph-style | Exists | Port | Port |
| docs update-document-style | Exists | Port | Port |
| docs manage-named-range | Exists | Port | Port |
| docs manage-table | Exists | Port | Port |
| docs debug-table | Exists | Port | Port |
| docs batch-update | Exists | Port | Port |
| docs header-footer | Exists | Port | Port |
| docs comment | **Add** | Add | Add |
| docs comments | **Add** | Add | Add |
| docs reply | **Add** | Add | Add |
| docs export-pdf | Exists | Port | Port |
| **Sheets** | | | |
| sheets read | Exists | Port | Port |
| sheets batch-read | Exists | Port | Port |
| sheets write | Exists | Port | Port |
| sheets find-replace | Exists | Port | Port |
| sheets read-cells | Exists | Port | Port |
| sheets write-cells | Exists | Port | Port |
| sheets transform | Exists | Port | Port |
| sheets format | Exists | Port | Port |
| sheets borders | Exists | Port | Port |
| sheets merge | Exists | Port | Port |
| sheets unmerge | Exists | Port | Port |
| sheets conditional-format | Exists | Port | Port |
| sheets create | Exists | Port | Port |
| sheets add-tab | Exists | Port | Port |
| sheets duplicate-tab | Exists | Port | Port |
| sheets delete-tab | Exists | Port | Port |
| sheets update-tab | Exists | Port | Port |
| sheets insert-dimension | Exists | Port | Port |
| sheets delete-dimension | Exists | Port | Port |
| sheets resize | Exists | Port | Port |
| sheets auto-resize | Exists | Port | Port |
| sheets freeze | Exists | Port | Port |
| sheets sort | Exists | Port | Port |
| sheets validate | Exists | Port | Port |
| sheets named-range | Exists | Port | Port |
| sheets filter-view | Exists | Port | Port |
| sheets protected-range | Exists | Port | Port |
| sheets comment | **Add** | Add | Add |
| sheets comments | **Add** | Add | Add |
| sheets reply | **Add** | Add | Add |
| **Calendar** | | | |
| calendar list | Exists | Port | Port |
| calendar events | Exists | Port | Port |
| calendar create-event | Exists (partial) | Port + Add attendees/recurrence | Port |
| calendar modify-event | Exists (partial) | Port + Add attendees | Port |
| calendar delete-event | Exists | Port | Port |
| **Forms** | | | |
| forms create | Exists | Port | Port |
| forms read | Exists | Port | Port |
| forms responses | Exists | Port | Port |
| forms response | Exists | Port | Port |
| forms publish-settings | Exists | Port | Port |
| **Slides** | | | |
| slides create | Exists | Port | Port |
| slides read | Exists | Port | Port |
| slides page | Exists | Port | Port |
| slides thumbnail | Exists | Port | Port |
| slides batch-update | Exists | Port | Port |
| slides comment | **Add** | Add | Add |
| slides comments | **Add** | Add | Add |
| slides reply | **Add** | Add | Add |

**Totals: 97 commands (78 Port, 19 Add)**

## New Features to Implement

These are capabilities that were either security-blocked or never built. All must have service layer + CLI + skill coverage.

1. **`gw gmail send`** — Send emails (was security-blocked). Reuse draft logic + add `.send()`.
2. **`gw gmail create-filter` / `delete-filter`** — Gmail filter CRUD (was security-blocked).
3. **`gw drive upload`** — Upload local files to Drive. Use `MediaFileUpload` from `googleapiclient.http`.
4. **`gw drive share`** — Create/update file permissions. Use Drive Permissions API.
5. **Comments on Docs/Sheets/Slides** (9 commands) — All use the Drive Comments API (`drive.comments()` and `drive.replies()`). A shared `_comments.py` helper handles all three services.
6. **Calendar attendees** — Add `attendees` param to create/modify event.
7. **Calendar recurrence** — Add `recurrence` param (RRULE string) to create event.

## First-Run Authentication

On first use (no stored credentials), `gw` automatically:
1. Opens the system browser to Google OAuth consent screen
2. Starts a local callback server to receive the auth code
3. Exchanges the code for tokens and stores them in macOS Keychain
4. Proceeds with the original command

Subsequent runs use the stored token, refreshing automatically when expired. This is the same flow the MCP server used — no changes needed.

## Service Layer Adaptation

Current tool functions look like:
```python
@require_google_service(["gmail"])
async def search_gmail_messages(gmail: Any, query: str, max_results: int = 25) -> dict:
    results = await asyncio.to_thread(
        gmail.users().messages().list(userId="me", q=query, maxResults=max_results).execute
    )
    return results
```

New service functions:
```python
def search_gmail_messages(gmail, query: str, max_results: int = 25) -> dict:
    return gmail.users().messages().list(
        userId="me", q=query, maxResults=max_results
    ).execute()
```

Changes:
- Remove `@require_google_service` decorator
- Remove `async` / `await asyncio.to_thread` (CLI is synchronous)
- Accept pre-built service client as first arg
- Return dict (CLI layer handles JSON serialization + printing)

## CLI Layer Pattern

Each `cli/<service>.py` follows the same pattern:

```python
import argparse, json, sys
from gw.auth.google_auth import get_authenticated_google_service
from gw.services.gmail import search_gmail_messages

def register(subparsers):
    gmail = subparsers.add_parser("gmail")
    gmail_sub = gmail.add_subparsers(dest="action", required=True)

    search = gmail_sub.add_parser("search")
    search.add_argument("query")
    search.add_argument("--max-results", type=int, default=25)
    search.set_defaults(func=cmd_search)

def cmd_search(args):
    gmail = get_authenticated_google_service("gmail", "v1")
    result = search_gmail_messages(gmail, args.query, args.max_results)
    json.dump(result, sys.stdout, indent=2, default=str)
```

## Skill Rewrite

The new `SKILL.md` documents:
1. How to invoke `gw` (via Bash tool)
2. Every command with its flags (generated from the sync matrix above)
3. Common workflows (search-then-read patterns)
4. Rich cell patterns for Sheets (same as current, but using CLI syntax)
5. No reference to MCP, no fictional tools

Example skill excerpt:
```markdown
### Gmail

Search, read, draft, and send emails.

Typical flow:
1. `gw gmail search "from:someone subject:invoice"`
2. `gw gmail read <message-id>` or `gw gmail read-thread <thread-id>`
3. `gw gmail draft --to addr --subject "Re: ..." --body "..." --in-reply-to <msg-id>`

#### Commands
- `gw gmail search <query>` — search messages, returns IDs and snippets
- `gw gmail read <message-id>` — full message content with headers and body
- `gw gmail send --to ADDR --subject SUBJ --body BODY` — send an email
...
```

## Migration Steps

### Phase 1: Restructure
1. Create `gw/` package with `__init__.py` and `__main__.py`
2. Move `auth/` into `gw/auth/` (keep google_auth.py, credential_store.py, scopes.py, oauth_config.py, oauth_callback_server.py)
3. Move service logic into `gw/services/` (strip async, strip decorator)
4. Move helpers into `gw/services/_helpers/`

### Phase 2: Build CLI Layer
5. Create `gw/cli/` with one module per service
6. Wire argparse in `__main__.py` to register all service subparsers
7. Each command: parse args → get auth service → call service function → print JSON

### Phase 3: Implement New Features
8. `gmail send` — add to services/gmail.py + cli/gmail.py
9. `drive upload` + `drive share` — add to services/drive.py + cli/drive.py
10. Comments (docs/sheets/slides) — shared helper in services/_helpers/comments.py + CLI wrappers
11. Calendar attendees + recurrence — extend services/calendar.py

### Phase 4: Shell Wrapper + Skill
12. Create `~/.local/bin/gw` wrapper script
13. Rewrite SKILL.md with full CLI reference
14. Remove old `scripts/workspace_mcp.py`

### Phase 5: Teardown MCP
15. Delete `main.py`, `core/`, `auth/service_decorator.py`, `auth/oauth21_session_store.py`
16. Remove `[mcp_servers.google-workspace]` from `~/.codex/config.toml`
17. Update `pyproject.toml`

## Testing

- Each service gets a smoke test: `gw <service> --help` exits 0
- Auth test: `gw gmail labels` returns valid JSON
- New features get integration tests against live API (manual, not CI)

## Out of Scope (Future)

- Charts, pivot tables in Sheets
- Form question CRUD
- Slide animations/transitions/speaker notes
- Google Chat, Tasks, Search services
- `--format` flag for human-readable output
- Shell completions
