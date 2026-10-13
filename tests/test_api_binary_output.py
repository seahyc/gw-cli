import json
from types import SimpleNamespace
from unittest.mock import Mock, patch

from gw.cli import api


def run_api(result, output):
    args = SimpleNamespace(version=None, api_service="drive", resource="files", method="export", params='{"fileId":"fake-id","mimeType":"application/pdf"}', body=None, dry_run=False, output=output)
    service = Mock()
    service.files.return_value.export.return_value.execute.return_value = result
    with patch.object(api, "get_credentials", return_value=(Mock(), "test@example.com")), patch("googleapiclient.discovery.build", return_value=service):
        api.cmd_api(args)


def test_binary_output_atomic_and_no_bytes_on_stdout(tmp_path, capsys):
    path = tmp_path / "template.pdf"
    path.write_bytes(b"old")
    run_api(b"%PDF-fake-binary", str(path))
    output = json.loads(capsys.readouterr().out)
    assert output == {"path": str(path), "size_bytes": 16}
    assert path.read_bytes() == b"%PDF-fake-binary"
    assert list(tmp_path.iterdir()) == [path]


def test_json_behavior_unchanged(capsys):
    run_api({"id": "fake-id"}, None)
    assert json.loads(capsys.readouterr().out) == {"id": "fake-id"}
