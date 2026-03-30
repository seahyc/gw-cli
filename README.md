# gw — Google Workspace CLI

A command-line tool for interacting with Google Workspace: Gmail, Drive, Docs, Sheets, Calendar, Forms, Slides.

Originally forked from [taylorwilsdon/google_workspace_mcp](https://github.com/taylorwilsdon/google_workspace_mcp), rebuilt as a pure CLI.

## Install

Requires Python 3.10+ and [uv](https://docs.astral.sh/uv/).

```bash
# Clone and create a shell wrapper
git clone https://github.com/seahyc/gw-cli.git ~/Code/gw-cli

# Create the wrapper script
mkdir -p ~/.local/bin
cat > ~/.local/bin/gw << 'EOF'
#!/bin/bash
exec uv run --directory ~/Code/gw-cli python -m gw "$@"
EOF
chmod +x ~/.local/bin/gw
```

Ensure `~/.local/bin` is in your `PATH`.

## Setup

Set your Google OAuth credentials:

```bash
export GOOGLE_OAUTH_CLIENT_ID="your-client-id"
export GOOGLE_OAUTH_CLIENT_SECRET="your-client-secret"
```

Then authenticate:

```bash
gw auth login
```

This opens a browser for Google OAuth consent and stores credentials in macOS Keychain.

## Usage

```bash
gw <service> <action> [args]
```

All output is JSON.

### Services

| Service | Description |
|---------|-------------|
| `gmail` | Search, read, draft, send emails |
| `drive` | Search, read, upload, share files |
| `docs` | Read, edit, insert content in documents |
| `sheets` | Read, write, format spreadsheets |
| `calendar` | List, create, modify events |
| `forms` | Create forms, read responses |
| `slides` | Create, read, batch-update presentations |
| `comments` | Comments on Docs/Sheets/Slides |
| `auth` | Login, logout, status |

### Examples

```bash
# Search Gmail
gw gmail search "from:boss subject:review"

# Read a Google Sheet
gw sheets read SPREADSHEET_ID --range "'Sheet 1'!A1:D10"

# Upload a file to Drive
gw drive upload ./report.pdf --name "Q1 Report"

# Create a calendar event
gw calendar create-event --summary "Team sync" --start 2026-04-01T10:00:00 --end 2026-04-01T11:00:00

# Get help for any command
gw gmail send --help
```

## License

MIT
