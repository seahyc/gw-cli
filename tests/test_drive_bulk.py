import json
from types import SimpleNamespace

import pytest

from gw.services.drive_bulk import copy_batch


class Failure(Exception):
    def __init__(self, status):
        self.resp = SimpleNamespace(status=status)


class Service:
    def __init__(self, responses=None, outer=False):
        self.responses = responses or {}
        self.outer = outer
        self.calls = []
        self.sizes = []

    def files(self):
        return self

    def copy(self, **kwargs):
        self.calls.append(kwargs)
        return kwargs

    def new_batch_http_request(self):
        service = self

        class Batch:
            def __init__(self):
                self.requests = []

            def add(self, request, request_id, callback):
                self.requests.append((request, request_id, callback))

            def execute(self):
                service.sizes.append(len(self.requests))
                if service.outer:
                    raise OSError('connection lost')
                for req, rid, callback in self.requests:
                    sequence = service.responses.get(req['fileId'], [])
                    result = sequence.pop(0) if sequence else {
                        'id': 'copy-' + req['fileId'], 'md5Checksum': 'a' * 32,
                        'size': '12', 'parents': req['body']['parents'],
                        'name': req['body']['name'],
                    }
                    callback(rid, None if isinstance(result, Exception) else result,
                             result if isinstance(result, Exception) else None)
        return Batch()


def paths(tmp_path, count=1):
    items = [{'source_id': str(i), 'parent': 'destination', 'name': f'{i}.pdf',
              'expected_md5': 'a' * 32, 'expected_size': 12} for i in range(count)]
    manifest, checkpoint = tmp_path / 'manifest.json', tmp_path / 'checkpoint.jsonl'
    manifest.write_text(json.dumps(items))
    return manifest, checkpoint


def test_batches_and_resume(tmp_path):
    manifest, checkpoint = paths(tmp_path, 201)
    service = Service()
    progress = []
    result = copy_batch(service, manifest, checkpoint, batch_size=100, sleep=lambda _: None,
                        progress=progress.append)
    assert result == {'total': 201, 'skipped': 0, 'copied': 201, 'failed': 0}
    assert service.sizes == [100, 100, 1]
    assert len(progress) == 3
    assert all(call['body']['parents'] == ['destination'] for call in service.calls)
    resumed = Service()
    assert copy_batch(resumed, manifest, checkpoint)['skipped'] == 201
    assert resumed.calls == []


def test_retry_and_permanent_failure(tmp_path):
    manifest, checkpoint = paths(tmp_path, 3)
    service = Service({'0': [Failure(429), Failure(503)], '1': [Failure(403)],
                       '2': [Failure(500)] * 3})
    waits = []
    result = copy_batch(service, manifest, checkpoint, max_retries=2, sleep=waits.append)
    assert result['copied'] == 1 and result['failed'] == 2
    assert waits == [2, 4]
    assert len(checkpoint.read_text().splitlines()) == 3


def test_mismatch_preserves_copy_for_reconciliation(tmp_path):
    manifest, checkpoint = paths(tmp_path)
    service = Service({'0': [{'id': 'created', 'md5Checksum': 'b' * 32, 'size': '12',
                              'parents': ['destination'], 'name': '0.pdf'}]})
    assert copy_batch(service, manifest, checkpoint)['failed'] == 1
    with pytest.raises(ValueError, match='Reconcile'):
        copy_batch(Service(), manifest, checkpoint)


def test_lost_batch_is_not_replayed(tmp_path):
    manifest, checkpoint = paths(tmp_path)
    service = Service(outer=True)
    assert copy_batch(service, manifest, checkpoint)['failed'] == 1
    assert len(service.calls) == 1
    assert json.loads(checkpoint.read_text())['status'] == 'uncertain'
    with pytest.raises(ValueError, match='Reconcile'):
        copy_batch(Service(), manifest, checkpoint)


def test_changed_destination_rejected(tmp_path):
    manifest, checkpoint = paths(tmp_path)
    copy_batch(Service(), manifest, checkpoint)
    items = json.loads(manifest.read_text())
    items[0]['parent'] = 'other'
    manifest.write_text(json.dumps(items))
    with pytest.raises(ValueError, match='changed'):
        copy_batch(Service(), manifest, checkpoint)


def test_invalid_manifest_prevents_calls(tmp_path):
    manifest, checkpoint = paths(tmp_path)
    manifest.write_text('[{"source_id":"a"}]')
    service = Service()
    with pytest.raises(ValueError):
        copy_batch(service, manifest, checkpoint)
    assert not service.calls
    assert not checkpoint.exists()


def test_cli_failure_exits_nonzero(tmp_path, monkeypatch, capsys):
    from gw.cli import drive
    manifest, checkpoint = paths(tmp_path)
    monkeypatch.setattr(drive, 'get_service', lambda _: Service({'0': [Failure(403)]}))
    args = SimpleNamespace(manifest=manifest, checkpoint=checkpoint, batch_size=50,
                           max_retries=0, batch_delay=0)
    with pytest.raises(SystemExit) as exc:
        drive.cmd_copy_batch(args)
    assert exc.value.code == 1
    assert json.loads(capsys.readouterr().out)['failed'] == 1


def test_failed_items_resume(tmp_path):
    manifest, checkpoint = paths(tmp_path, 2)
    copy_batch(Service({'0': [Failure(403)]}), manifest, checkpoint)
    service = Service()
    counts = copy_batch(service, manifest, checkpoint)
    assert counts == {'total': 2, 'skipped': 1, 'copied': 1, 'failed': 0}
    assert [call['fileId'] for call in service.calls] == ['0']


@pytest.mark.parametrize('option', [{'batch_size': 101}, {'max_retries': -1}, {'batch_delay': -1}])
def test_invalid_options_prevent_calls(tmp_path, option):
    manifest, checkpoint = paths(tmp_path)
    service = Service()
    with pytest.raises(ValueError):
        copy_batch(service, manifest, checkpoint, **option)
    assert not service.calls
