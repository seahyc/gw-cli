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

# Google may return a superset of the requested scopes when the account has
# previously granted this OAuth client extra scopes (e.g. pubsub, cloud-platform
# from a sibling tool sharing the client_id). oauthlib treats scope!=requested as
# an error by default; relax it so the token exchange succeeds. The extra scopes
# are harmless — the credentials still cover everything gw needs.
os.environ.setdefault("OAUTHLIB_RELAX_TOKEN_SCOPE", "1")
os.environ.setdefault("OAUTHLIB_IGNORE_SCOPE_CHANGE", "1")

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


# Keychain bookkeeping entries (stored under KEYCHAIN_SERVICE alongside the
# per-email credential entries). `__registered_users__` predates multi-account
# support; when no explicit default is stored, the first registered user is
# the default, exactly as before.
_USERS_KEY = "__registered_users__"
_DEFAULT_KEY = "__default_user__"
_ALIASES_KEY = "__account_aliases__"

# Account selected for this process via `gw --account X ...` or GW_ACCOUNT.
_active_account = None


class AccountError(RuntimeError):
    """Raised when a requested account is not registered."""


def set_active_account(account):
    """Select the account (email or alias) used by get_credentials()."""
    global _active_account
    _active_account = account or None


def _read_json_entry(keyring, key, fallback):
    raw = keyring.get_password(KEYCHAIN_SERVICE, key)
    if not raw:
        return fallback
    try:
        value = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return fallback
    return value if isinstance(value, type(fallback)) else fallback


def _registered_users(keyring=None):
    keyring = keyring or _get_keychain_store()
    users = _read_json_entry(keyring, _USERS_KEY, [])
    return list(users)


def _aliases(keyring=None):
    keyring = keyring or _get_keychain_store()
    return _read_json_entry(keyring, _ALIASES_KEY, {})


def _default_user(keyring=None):
    keyring = keyring or _get_keychain_store()
    users = _registered_users(keyring)
    stored = keyring.get_password(KEYCHAIN_SERVICE, _DEFAULT_KEY)
    if stored and stored in users:
        return stored
    return users[0] if users else None


def resolve_account(account, keyring=None):
    """Map an email or alias to a registered email. None -> default account."""
    keyring = keyring or _get_keychain_store()
    users = _registered_users(keyring)
    if not account:
        return _default_user(keyring)
    aliases = _aliases(keyring)
    email = aliases.get(account, account)
    if email in users:
        return email
    lowered = {u.lower(): u for u in users}
    if email.lower() in lowered:
        return lowered[email.lower()]
    raise AccountError(
        f"account '{account}' is not logged in. Run: gw auth login --account {account}"
    )


def _selected_account_name():
    return _active_account or os.environ.get("GW_ACCOUNT") or None


def _credentials_from_json(creds_json):
    try:
        creds_data = json.loads(creds_json)
    except (json.JSONDecodeError, TypeError):
        return None

    from datetime import datetime

    expiry = None
    if creds_data.get("expiry"):
        try:
            expiry = datetime.fromisoformat(creds_data["expiry"])
            if expiry.tzinfo is not None:
                expiry = expiry.replace(tzinfo=None)
        except (ValueError, TypeError):
            pass

    return Credentials(
        token=creds_data.get("token"),
        refresh_token=creds_data.get("refresh_token"),
        token_uri=creds_data.get("token_uri"),
        client_id=creds_data.get("client_id"),
        client_secret=creds_data.get("client_secret"),
        scopes=creds_data.get("scopes"),
        expiry=expiry,
    )


def _load_credentials(account=None):
    """Load credentials from macOS Keychain for `account` (email or alias).

    With no account, uses the process-selected account (--account/GW_ACCOUNT)
    or else the stored default (first registered user if none was chosen).
    """
    keyring = _get_keychain_store()
    user_email = resolve_account(account or _selected_account_name(), keyring)
    if not user_email:
        return None, None

    creds_json = keyring.get_password(KEYCHAIN_SERVICE, user_email)
    if not creds_json:
        return None, user_email
    return _credentials_from_json(creds_json), user_email


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

    # Update users list. Keep the existing order so the first registered
    # user stays the implicit default for installs that never ran `auth use`.
    users = _registered_users(keyring)
    if user_email not in users:
        users.append(user_email)
    keyring.set_password(KEYCHAIN_SERVICE, _USERS_KEY, json.dumps(users))


def _record_login_target(user_email, requested):
    """After a login for `requested` (email or alias), remember the alias and
    make sure a mismatched email is reported instead of silently accepted."""
    if not requested:
        return
    keyring = _get_keychain_store()
    if "@" in requested:
        if requested.lower() != user_email.lower():
            print(
                f"Warning: asked to log in as {requested} but Google returned "
                f"{user_email}; stored credentials under {user_email}.",
                file=sys.stderr,
            )
        return
    aliases = _aliases(keyring)
    aliases[requested] = user_email
    keyring.set_password(KEYCHAIN_SERVICE, _ALIASES_KEY, json.dumps(aliases, sort_keys=True))


def _finalize_credentials(credentials, requested_account=None):
    """Fetch the user's email for freshly-minted credentials and persist them."""
    service = build("oauth2", "v2", credentials=credentials)
    user_info = service.userinfo().get().execute()
    user_email = user_info.get("email", "unknown")

    _save_credentials(user_email, credentials)
    _record_login_target(user_email, requested_account)

    print(f"Authenticated as {user_email}", file=sys.stderr)
    return credentials, user_email


# Loopback redirect used by the manual (headless) flow. Google still fully
# supports http://localhost redirects; only the old OOB (urn:...:oob) flow was
# deprecated. Must be listed as an authorized redirect URI on the OAuth client.
MANUAL_REDIRECT_URI = "http://localhost:8080/"


def _login_hint_kwargs(account):
    """Pre-select the Google account on the consent screen when we know it."""
    if account and "@" in account:
        return {"login_hint": account}
    return {}


def _run_oauth_flow(account=None):
    """Run the OAuth flow to get new credentials."""
    from google_auth_oauthlib.flow import InstalledAppFlow

    client_config = _get_client_config()

    flow = InstalledAppFlow.from_client_config(client_config, scopes=ALL_SCOPES)

    print("Opening browser for Google authentication...", file=sys.stderr)
    # Try ports in sequence to avoid conflicts
    for port in [8080, 8090, 9090, 0]:
        try:
            credentials = flow.run_local_server(
                port=port, open_browser=True, **_login_hint_kwargs(account)
            )
            break
        except OSError:
            if port == 0:
                raise
            continue

    return _finalize_credentials(credentials, requested_account=account)


def build_manual_auth_url(account=None):
    """Build the consent URL for the headless (paste-URL) flow.

    Returns (auth_url, flow). The caller shows auth_url to the human, who
    completes consent on ANY device (phone, laptop) and copies the resulting
    ``http://localhost:8080/?code=...`` URL from the browser's address bar —
    the loopback page failing to load is expected and harmless; the code in the
    URL is valid regardless. No browser is needed on this machine.
    """
    from google_auth_oauthlib.flow import InstalledAppFlow

    client_config = _get_client_config()
    flow = InstalledAppFlow.from_client_config(client_config, scopes=ALL_SCOPES)
    flow.redirect_uri = MANUAL_REDIRECT_URI
    auth_url, _ = flow.authorization_url(
        access_type="offline",
        prompt="consent",  # force a refresh_token even on re-consent
        **_login_hint_kwargs(account),
    )
    return auth_url, flow


def exchange_manual_response(flow, redirect_response, account=None):
    """Exchange the pasted redirect URL (or bare code) for credentials + save.

    ``redirect_response`` may be the full ``http://localhost:8080/?code=...``
    URL copied from the browser, or just the ``code`` value.
    """
    redirect_response = (redirect_response or "").strip()
    if not redirect_response:
        raise ValueError("empty authorization response")

    if redirect_response.startswith("http://") or redirect_response.startswith("https://"):
        flow.fetch_token(authorization_response=redirect_response)
    else:
        # Bare code pasted — exchange it directly.
        flow.fetch_token(code=redirect_response)

    return _finalize_credentials(flow.credentials, requested_account=account)


def get_credentials():
    """Get valid credentials for the selected account, running OAuth if needed."""
    requested = _selected_account_name()
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

    return _run_oauth_flow(account=user_email or requested)


def current_account():
    """Email of the account whose credentials this process uses (no OAuth)."""
    try:
        return resolve_account(_selected_account_name())
    except AccountError:
        return None


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


def _credential_state(credentials):
    if not credentials:
        return "missing"
    if credentials.valid:
        return "valid"
    if credentials.expired and credentials.refresh_token:
        return "refreshable"
    return "invalid"


def auth_status():
    """Print current authentication status."""
    credentials, user_email = _load_credentials()

    if not credentials:
        return {"authenticated": False, "message": "No credentials found. Run: gw auth login"}

    if credentials.valid:
        return {
            "authenticated": True,
            "user": user_email,
            "account": user_email,
            "scopes": list(credentials.scopes) if credentials.scopes else [],
        }

    if credentials.expired and credentials.refresh_token:
        return {
            "authenticated": True,
            "user": user_email,
            "account": user_email,
            "token_expired": True,
            "message": "Token expired but can be refreshed automatically",
        }

    return {
        "authenticated": False,
        "user": user_email,
        "account": user_email,
        "message": "Credentials invalid. Run: gw auth login",
    }


def auth_list():
    """List every stored account with its aliases and token state (no secrets)."""
    keyring = _get_keychain_store()
    users = _registered_users(keyring)
    default = _default_user(keyring)
    aliases = _aliases(keyring)
    selected = None
    try:
        selected = resolve_account(_selected_account_name(), keyring)
    except AccountError:
        pass
    accounts = []
    for email in users:
        raw = keyring.get_password(KEYCHAIN_SERVICE, email)
        creds = _credentials_from_json(raw) if raw else None
        accounts.append({
            "email": email,
            "aliases": sorted(a for a, e in aliases.items() if e == email),
            "default": email == default,
            "selected": email == selected,
            "credential_state": _credential_state(creds),
        })
    return {
        "accounts": accounts,
        "default": default,
        "selected": selected,
        "summary": (
            f"{len(accounts)} account(s); default {default or 'none'}"
            if accounts else "No accounts. Run: gw auth login"
        ),
    }


def auth_use(account):
    """Make `account` (email or alias) the default for future commands."""
    keyring = _get_keychain_store()
    email = resolve_account(account, keyring)
    if not email:
        raise AccountError("no accounts are logged in. Run: gw auth login")
    keyring.set_password(KEYCHAIN_SERVICE, _DEFAULT_KEY, email)
    return {"default": email, "summary": f"Default account is now {email}"}


def auth_login(account=None):
    """Force re-authentication (optionally for a specific account or alias)."""
    credentials, user_email = _run_oauth_flow(account=account or _selected_account_name())
    return {"authenticated": True, "user": user_email, "account": user_email}


def auth_login_manual(account=None):
    """Headless re-authentication: print the URL, read the pasted redirect back.

    Designed for machines with no browser (e.g. the headless VM): nothing here
    calls xdg-open or spins a callback server that the login device must reach.
    Reads the pasted redirect URL / code from stdin.
    """
    account = account or _selected_account_name()
    auth_url, flow = build_manual_auth_url(account)
    print("\nOpen this URL on any device (phone is fine) and approve access:\n", file=sys.stderr)
    print(auth_url, file=sys.stderr)
    print(
        "\nAfter approving, your browser will try to open a http://localhost:8080/?code=... "
        "page that fails to load — that is expected. Copy that full URL from the address "
        "bar and paste it below.\n",
        file=sys.stderr,
    )
    print("Paste redirect URL (or just the code): ", end="", file=sys.stderr, flush=True)
    redirect_response = sys.stdin.readline()
    try:
        credentials, user_email = exchange_manual_response(
            flow, redirect_response, account=account
        )
    except Exception as exc:  # noqa: BLE001 — surface a clean message, not a traceback
        print(
            f"\nCould not exchange the pasted value for tokens: {exc}\n"
            "Make sure you copied the FULL http://localhost:8080/?code=... URL from the "
            "address bar (the code expires quickly — if it's been a while, just re-run).",
            file=sys.stderr,
        )
        sys.exit(1)
    return {"authenticated": True, "user": user_email, "account": user_email}


def auth_logout(account=None):
    """Remove stored credentials: one account when given, otherwise all."""
    keyring = _get_keychain_store()

    if account:
        email = resolve_account(account, keyring)
        try:
            keyring.delete_password(KEYCHAIN_SERVICE, email)
        except Exception:
            pass
        users = [u for u in _registered_users(keyring) if u != email]
        keyring.set_password(KEYCHAIN_SERVICE, _USERS_KEY, json.dumps(users))
        aliases = {a: e for a, e in _aliases(keyring).items() if e != email}
        keyring.set_password(KEYCHAIN_SERVICE, _ALIASES_KEY, json.dumps(aliases, sort_keys=True))
        if keyring.get_password(KEYCHAIN_SERVICE, _DEFAULT_KEY) == email:
            try:
                keyring.delete_password(KEYCHAIN_SERVICE, _DEFAULT_KEY)
            except Exception:
                pass
        return {"message": f"Logged out {email}", "account": email}

    for user in _registered_users(keyring):
        try:
            keyring.delete_password(KEYCHAIN_SERVICE, user)
        except Exception:
            pass
    for key in (_USERS_KEY, _DEFAULT_KEY, _ALIASES_KEY):
        try:
            keyring.delete_password(KEYCHAIN_SERVICE, key)
        except Exception:
            pass

    return {"message": "Logged out successfully"}
