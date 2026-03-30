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

## Google Cloud Project Setup

You need a GCP project with OAuth credentials. One-time setup:

1. Go to [Google Cloud Console](https://console.cloud.google.com/) and create a project (or use an existing one)
2. Enable the APIs you need:
   - [Gmail API](https://console.cloud.google.com/apis/library/gmail.googleapis.com)
   - [Google Drive API](https://console.cloud.google.com/apis/library/drive.googleapis.com)
   - [Google Docs API](https://console.cloud.google.com/apis/library/docs.googleapis.com)
   - [Google Sheets API](https://console.cloud.google.com/apis/library/sheets.googleapis.com)
   - [Google Calendar API](https://console.cloud.google.com/apis/library/calendar-json.googleapis.com)
   - [Google Forms API](https://console.cloud.google.com/apis/library/forms.googleapis.com)
   - [Google Slides API](https://console.cloud.google.com/apis/library/slides.googleapis.com)
3. Go to **APIs & Services > Credentials > Create Credentials > OAuth client ID**
4. Choose **Desktop app** as the application type
5. Copy the **Client ID** and **Client Secret**

## Authentication

Set your OAuth credentials as environment variables:

```bash
export GOOGLE_OAUTH_CLIENT_ID="your-client-id"
export GOOGLE_OAUTH_CLIENT_SECRET="your-client-secret"
```

Then authenticate:

```bash
gw auth login
```

This opens a browser for Google OAuth consent and stores credentials in macOS Keychain. After login, the env vars are no longer needed — credentials are refreshed automatically.

To check status or re-authenticate:

```bash
gw auth status    # Check current auth state
gw auth login     # Re-authenticate (e.g., after token revocation)
gw auth logout    # Remove stored credentials
```

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
