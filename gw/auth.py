"""
Simplified authentication for the gw CLI.

Uses macOS Keychain for credential storage. On first use, opens
the browser for OAuth consent via InstalledAppFlow.
"""

import json
import logging
import os
import sys
import webbrowser

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google.auth.exceptions import RefreshError
from googleapiclient.discovery import build

logger = logging.getLogger(__name__)

# Keychain service name (matches the MCP server for credential reuse)
KEYCHAIN_SERVICE = "hardened-google-workspace-mcp"

# Google API service configs
SERVICE_VERSIONS = {
    "gmail": "v1",
    "drive": "v3",
    "docs": "v1",
    "sheets": "v4",
    "calendar": "v3",
    "forms": "v1",
    "slides": "v1",
    "oauth2": "v2",
}

# All scopes needed for full Google Workspace access
ALL_SCOPES = [
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    # Gmail (including send)
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/gmail.compose",
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/gmail.labels",
    "https://www.googleapis.com/auth/gmail.settings.basic",
    # Drive (including sharing)
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/drive.file",
    # Docs
    "https://www.googleapis.com/auth/documents.readonly",
    "https://www.googleapis.com/auth/documents",
    # Sheets
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/spreadsheets",
    # Calendar
    "https://www.googleapis.com/auth/calendar",
    "https://www.googleapis.com/auth/calendar.readonly",
    "https://www.googleapis.com/auth/calendar.events",
    # Forms
    "https://www.googleapis.com/auth/forms.body",
    "https://www.googleapis.com/auth/forms.body.readonly",
    "https://www.googleapis.com/auth/forms.responses.readonly",
    # Slides
    "https://www.googleapis.com/auth/presentations",
    "https://www.googleapis.com/auth/presentations.readonly",
]


def _get_client_config():
    """Get OAuth client configuration from environment variables."""
    client_id = os.environ.get("GOOGLE_OAUTH_CLIENT_ID")
    client_secret = os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET")

    if not client_id or not client_secret:
        print(
            "Error: GOOGLE_OAUTH_CLIENT_ID and GOOGLE_OAUTH_CLIENT_SECRET must be set.",
            file=sys.stderr,
        )
        print(
            "Set them in your environment or in ~/.config/gw/env",
            file=sys.stderr,
        )
        sys.exit(1)

    return {
        "installed": {
            "client_id": client_id,
            "client_secret": client_secret,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "redirect_uris": ["http://localhost"],
        }
    }


def _get_keychain_store():
    """Get the KeychainCredentialStore for credential persistence."""
    try:
        import keyring

        return keyring
    except ImportError:
        print(
            "Error: keyring package required. Install with: pip install keyring",
            file=sys.stderr,
        )
        sys.exit(1)


def _load_credentials():
    """Load credentials from macOS Keychain."""
    keyring = _get_keychain_store()

    # Get list of registered users
    users_json = keyring.get_password(KEYCHAIN_SERVICE, "__registered_users__")
    if not users_json:
        return None, None

    try:
        users = json.loads(users_json)
    except (json.JSONDecodeError, TypeError):
        return None, None

    if not users:
        return None, None

    # Use first registered user
    user_email = users[0] if isinstance(users, list) else list(users)[0]
    creds_json = keyring.get_password(KEYCHAIN_SERVICE, user_email)
    if not creds_json:
        return None, user_email

    try:
        creds_data = json.loads(creds_json)
    except (json.JSONDecodeError, TypeError):
        return None, user_email

    from datetime import datetime

    expiry = None
    if creds_data.get("expiry"):
        try:
            expiry = datetime.fromisoformat(creds_data["expiry"])
            if expiry.tzinfo is not None:
                expiry = expiry.replace(tzinfo=None)
        except (ValueError, TypeError):
            pass

    credentials = Credentials(
        token=creds_data.get("token"),
        refresh_token=creds_data.get("refresh_token"),
        token_uri=creds_data.get("token_uri"),
        client_id=creds_data.get("client_id"),
        client_secret=creds_data.get("client_secret"),
        scopes=creds_data.get("scopes"),
        expiry=expiry,
    )

    return credentials, user_email


def _save_credentials(user_email, credentials):
    """Save credentials to macOS Keychain."""
    keyring = _get_keychain_store()

    creds_data = {
        "token": credentials.token,
        "refresh_token": credentials.refresh_token,
        "token_uri": credentials.token_uri,
        "client_id": credentials.client_id,
        "client_secret": credentials.client_secret,
        "scopes": list(credentials.scopes) if credentials.scopes else [],
        "expiry": credentials.expiry.isoformat() if credentials.expiry else None,
    }

    keyring.set_password(KEYCHAIN_SERVICE, user_email, json.dumps(creds_data))

    # Update users list
    users_json = keyring.get_password(KEYCHAIN_SERVICE, "__registered_users__")
    try:
        users = set(json.loads(users_json)) if users_json else set()
    except (json.JSONDecodeError, TypeError):
        users = set()
    users.add(user_email)
    keyring.set_password(
        KEYCHAIN_SERVICE, "__registered_users__", json.dumps(sorted(users))
    )


def _run_oauth_flow():
    """Run the OAuth flow to get new credentials."""
    from google_auth_oauthlib.flow import InstalledAppFlow

    client_config = _get_client_config()

    flow = InstalledAppFlow.from_client_config(client_config, scopes=ALL_SCOPES)

    print("Opening browser for Google authentication...", file=sys.stderr)
    # Try ports in sequence to avoid conflicts
    for port in [8080, 8090, 9090, 0]:
        try:
            credentials = flow.run_local_server(port=port, open_browser=True)
            break
        except OSError:
            if port == 0:
                raise
            continue

    # Get user email
    service = build("oauth2", "v2", credentials=credentials)
    user_info = service.userinfo().get().execute()
    user_email = user_info.get("email", "unknown")

    # Save to keychain
    _save_credentials(user_email, credentials)

    print(f"Authenticated as {user_email}", file=sys.stderr)
    return credentials, user_email


def get_credentials():
    """Get valid credentials, running OAuth flow if needed."""
    credentials, user_email = _load_credentials()

    if credentials and credentials.valid:
        return credentials, user_email

    if credentials and credentials.expired and credentials.refresh_token:
        try:
            credentials.refresh(Request())
            if user_email:
                _save_credentials(user_email, credentials)
            return credentials, user_email
        except RefreshError:
            print("Token expired, re-authenticating...", file=sys.stderr)

    return _run_oauth_flow()


def get_service(service_name, version=None):
    """Get an authenticated Google API service client.

    Args:
        service_name: Google API service name (gmail, drive, docs, sheets, etc.)
        version: API version override. Defaults to standard version for the service.

    Returns:
        Authenticated Google API service client.
    """
    if version is None:
        version = SERVICE_VERSIONS.get(service_name)
        if not version:
            raise ValueError(f"Unknown service: {service_name}. Specify version explicitly.")

    credentials, _ = get_credentials()
    return build(service_name, version, credentials=credentials)


def get_services(*service_names):
    """Get multiple authenticated Google API service clients.

    Args:
        *service_names: Variable number of service names.

    Returns:
        Tuple of authenticated service clients in the same order.
    """
    credentials, _ = get_credentials()
    return tuple(
        build(name, SERVICE_VERSIONS[name], credentials=credentials)
        for name in service_names
    )


def auth_status():
    """Print current authentication status."""
    credentials, user_email = _load_credentials()

    if not credentials:
        return {"authenticated": False, "message": "No credentials found. Run: gw auth login"}

    if credentials.valid:
        return {
            "authenticated": True,
            "user": user_email,
            "scopes": list(credentials.scopes) if credentials.scopes else [],
        }

    if credentials.expired and credentials.refresh_token:
        return {
            "authenticated": True,
            "user": user_email,
            "token_expired": True,
            "message": "Token expired but can be refreshed automatically",
        }

    return {
        "authenticated": False,
        "user": user_email,
        "message": "Credentials invalid. Run: gw auth login",
    }


def auth_login():
    """Force re-authentication."""
    credentials, user_email = _run_oauth_flow()
    return {"authenticated": True, "user": user_email}


def auth_logout():
    """Remove stored credentials."""
    keyring = _get_keychain_store()

    users_json = keyring.get_password(KEYCHAIN_SERVICE, "__registered_users__")
    if users_json:
        try:
            users = json.loads(users_json)
            for user in users:
                try:
                    keyring.delete_password(KEYCHAIN_SERVICE, user)
                except Exception:
                    pass
            keyring.delete_password(KEYCHAIN_SERVICE, "__registered_users__")
        except (json.JSONDecodeError, TypeError):
            pass

    return {"message": "Logged out successfully"}
