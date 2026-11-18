import json
import os
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import pytest
from google.oauth2.credentials import Credentials
from google.auth.exceptions import RefreshError
from gw import auth


def credentials(expired=False):
    return Credentials(token="fake-token", refresh_token="fake-refresh", token_uri="https://oauth2.googleapis.com/token", client_id="fake-client", client_secret="fake-secret", scopes=auth.FILE_AUTH_SCOPES, expiry=datetime.utcnow() + timedelta(hours=-2 if expired else 2))


def test_file_save_load_private_and_atomic(tmp_path, monkeypatch):
    path = tmp_path / "account.json"
    auth._save_credential_file(path, "test@example.com", credentials())
    assert path.stat().st_mode & 0o777 == 0o600
    monkeypatch.setenv("GW_CREDENTIALS_FILE", str(path))
    with patch.object(auth, "_get_keychain_store", side_effect=AssertionError("no keychain")), patch.object(auth, "_run_oauth_flow", side_effect=AssertionError("no interactive auth")):
        loaded, email = auth.get_credentials()
    assert email == "test@example.com"
    assert loaded.token == "fake-token"
    assert loaded.scopes == auth.FILE_AUTH_SCOPES
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize("kind", ["missing", "invalid", "public", "symlink"])
def test_invalid_file_never_opens_oauth(tmp_path, monkeypatch, kind):
    path = tmp_path / "account.json"
    if kind == "invalid":
        path.write_text("{}")
        path.chmod(0o600)
    elif kind in ("public", "symlink"):
        auth._save_credential_file(path, "test@example.com", credentials())
        if kind == "public":
            path.chmod(0o644)
        else:
            actual = tmp_path / "actual.json"
            path.rename(actual)
            path.symlink_to(actual)
    monkeypatch.setenv("GW_CREDENTIALS_FILE", str(path))
    with patch.object(auth, "_run_oauth_flow", side_effect=AssertionError("no interactive auth")), pytest.raises(auth.CredentialFileError, match="Reauthorize explicitly"):
        auth.get_credentials()


def test_refresh_persists_and_failures_never_launch_auth(tmp_path, monkeypatch):
    path = tmp_path / "account.json"
    auth._save_credential_file(path, "test@example.com", credentials(expired=True))
    monkeypatch.setenv("GW_CREDENTIALS_FILE", str(path))
    def refresh(instance, request):
        instance.token = "renewed-fake"
        instance.expiry = datetime.utcnow() + timedelta(hours=1)
    with patch.object(Credentials, "refresh", refresh):
        auth.get_credentials()
    assert json.loads(path.read_text())["token"] == "renewed-fake"
    auth._save_credential_file(path, "test@example.com", credentials(expired=True))
    with patch.object(Credentials, "refresh", side_effect=RefreshError("secret token must not surface")), patch.object(auth, "_run_oauth_flow", side_effect=AssertionError("no oauth")), pytest.raises(auth.CredentialFileError) as exc:
        auth.get_credentials()
    assert "secret token" not in str(exc.value)
    assert "refresh failed" in str(exc.value)


def test_legacy_login_preserves_call_signature(monkeypatch):
    monkeypatch.delenv("GW_CREDENTIALS_FILE", raising=False)
    with patch.object(auth, "_run_oauth_flow", return_value=(credentials(), "test@example.com")) as flow:
        assert auth.auth_login("test@example.com")["authenticated"]
    flow.assert_called_once_with(account="test@example.com")


def test_dedicated_login_no_keychain_and_reports_scope_superset(tmp_path, capsys):
    path = tmp_path / "account.json"
    creds = credentials()
    creds._granted_scopes = auth.FILE_AUTH_SCOPES + ["extra-scope"]
    service = Mock()
    service.userinfo.return_value.get.return_value.execute.return_value = {"email": "test@example.com"}
    with patch.object(auth, "build", return_value=service), patch.object(auth, "_get_keychain_store", side_effect=AssertionError("no keychain")):
        auth._finalize_credentials(creds, credential_file=path, requested_scopes=auth.FILE_AUTH_SCOPES)
    assert "additional previously authorized scopes: extra-scope" in capsys.readouterr().err
    assert "extra-scope" in json.loads(path.read_text())["scopes"]


def test_missing_requested_scope_does_not_save(tmp_path):
    path = tmp_path / "account.json"
    creds = credentials()
    creds._granted_scopes = ["openid"]
    service = Mock()
    service.userinfo.return_value.get.return_value.execute.return_value = {"email": "test@example.com"}
    with patch.object(auth, "build", return_value=service), pytest.raises(auth.CredentialFileError, match="did not grant"):
        auth._finalize_credentials(creds, credential_file=path, requested_scopes=auth.FILE_AUTH_SCOPES)
    assert not path.exists()


def test_loopback_print_url_narrow_scopes(tmp_path):
    path = tmp_path / "account.json"
    flow = Mock()
    flow.run_local_server.return_value = credentials()
    with patch.object(auth, "_get_client_config", return_value={}), patch("google_auth_oauthlib.flow.InstalledAppFlow.from_client_config", return_value=flow) as factory, patch.object(auth, "_finalize_credentials", return_value=(credentials(), "test@example.com")):
        auth._run_oauth_flow(credential_file=path, open_browser=False)
    assert factory.call_args.kwargs["scopes"] == auth.FILE_AUTH_SCOPES
    assert flow.run_local_server.call_args.kwargs["open_browser"] is False
    assert flow.run_local_server.call_args.kwargs["prompt"] == "consent"


def test_service_account_file_loads_without_oauth(tmp_path, monkeypatch):
    import rsa
    from google.oauth2 import service_account
    pem = rsa.newkeys(1024)[1].save_pkcs1().decode()
    path = tmp_path / "sa.json"
    path.write_text(json.dumps({
        "type": "service_account", "project_id": "p", "private_key_id": "k", "private_key": pem,
        "client_email": "bot@p.iam.gserviceaccount.com", "client_id": "1",
        "token_uri": "https://oauth2.googleapis.com/token"}))
    path.chmod(0o600)
    monkeypatch.setenv("GW_CREDENTIALS_FILE", str(path))

    def fake_refresh(self, request):
        self.token = "sa-token"
        self.expiry = datetime.utcnow() + timedelta(hours=1)

    with patch.object(service_account.Credentials, "refresh", fake_refresh), \
         patch.object(auth, "_get_keychain_store", side_effect=AssertionError("no keychain")), \
         patch.object(auth, "_run_oauth_flow", side_effect=AssertionError("no interactive auth")):
        loaded, email = auth.get_credentials()
    assert email == "bot@p.iam.gserviceaccount.com"
    assert loaded.token == "sa-token"
    assert set(auth.SERVICE_ACCOUNT_SCOPES) <= set(loaded.scopes)
    assert auth.current_account() == "bot@p.iam.gserviceaccount.com"
    assert path.read_text().count("sa-token") == 0


def test_readonly_mode_requests_only_read_scopes(tmp_path, monkeypatch):
    import rsa
    path = tmp_path / "sa.json"
    path.write_text(json.dumps({
        "type": "service_account", "project_id": "p", "private_key_id": "k", "private_key": rsa.newkeys(1024)[1].save_pkcs1().decode(),
        "client_email": "bot@p.iam.gserviceaccount.com", "client_id": "1", "token_uri": "https://oauth2.googleapis.com/token"}))
    path.chmod(0o600)
    monkeypatch.setenv("GW_CREDENTIALS_FILE", str(path))
    monkeypatch.setenv("GW_READONLY", "1")
    loaded, _ = auth._load_credential_file(path)
    assert set(loaded.scopes) == set(auth.READONLY_SCOPES)
    monkeypatch.setenv("GW_READONLY", "0")
    loaded, _ = auth._load_credential_file(path)
    assert set(loaded.scopes) == set(auth.SERVICE_ACCOUNT_SCOPES)
