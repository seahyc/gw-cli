"""Checkpointed Drive copies, with bounded batches and per-item retries."""

import json
import os
import re
import sys
import time
from pathlib import Path


FIELDS = "id,md5Checksum,size,parents,name"


def _validate(items):
    if not isinstance(items, list):
        raise ValueError("Manifest must be a JSON list")
    seen = set()
    for item in items:
        if not isinstance(item, dict):
            raise ValueError("Manifest items must be objects")
        for key in ("source_id", "parent", "name"):
            if not isinstance(item.get(key), str) or not item[key].strip():
                raise ValueError(f"Each item requires nonempty {key}")
        if not re.fullmatch(r"[a-fA-F0-9]{32}", item.get("expected_md5", "")):
            raise ValueError("Each item requires expected_md5 (32 hex characters)")
        size = item.get("expected_size")
        if isinstance(size, bool) or not isinstance(size, (int, str)) or not str(size).isdigit():
            raise ValueError("Each item requires nonnegative expected_size")
        if item["source_id"] in seen:
            raise ValueError("Duplicate source_id in manifest")
        seen.add(item["source_id"])


def _matches(item, record):
    return (bool(record.get("id"))
            and str(record.get("md5Checksum", "")).lower() == item["expected_md5"].lower()
            and str(record.get("size")) == str(int(item["expected_size"]))
            and record.get("parents") == [item["parent"]]
            and record.get("name") == item["name"])


def copy_batch(service, manifest, checkpoint, batch_size=50, max_retries=4,
               batch_delay=1.0, sleep=time.sleep, progress=None):
    """Copy binary files and return counts; ambiguous outcomes require reconciliation.

    Checkpoints are bound to the requested destination/name/hash/size. A copy
    whose response was lost cannot safely be retried automatically.
    """
    if not 1 <= batch_size <= 100 or not 0 <= max_retries <= 10 or batch_delay < 0:
        raise ValueError("batch_size must be 1..100, max_retries 0..10, delay >= 0")
    manifest_path, checkpoint_path = Path(manifest), Path(checkpoint)
    if manifest_path.resolve() == checkpoint_path.resolve():
        raise ValueError("Checkpoint must differ from manifest")
    items = json.loads(manifest_path.read_text())
    _validate(items)
    previous = {}
    if checkpoint_path.exists():
        for line in checkpoint_path.read_text().splitlines():
            record = json.loads(line)
            previous[record["source_id"]] = record
    pending = []
    skipped = 0
    for item in items:
        record = previous.get(item["source_id"])
        if record:
            if record.get("request") != item:
                raise ValueError(f"Checkpoint request changed for {item['source_id']}")
            if record.get("status") == "success" and _matches(item, record):
                skipped += 1
                continue
            if record.get("id") or record.get("status") == "uncertain":
                raise ValueError(f"Reconcile existing copy before retry: {item['source_id']}")
        pending.append((item, 0))
    counts = {"total": len(items), "skipped": skipped, "copied": 0, "failed": 0}
    if progress is None:
        progress = lambda value: print(json.dumps(value), file=sys.stderr, flush=True)
    with checkpoint_path.open("a") as output:
        def save(item, status, response=None, error=None):
            record = dict(response or {})
            record.update(source_id=item["source_id"], status=status, request=item)
            if error:
                record["error"] = str(error)
            output.write(json.dumps(record) + "\n")
            output.flush()
            os.fsync(output.fileno())
            counts["copied" if status == "success" else "failed"] += 1

        while pending:
            chunk, pending = pending[:batch_size], pending[batch_size:]
            outcomes = {}
            batch = service.new_batch_http_request()
            for index, (item, _) in enumerate(chunk):
                request_id = str(index)
                def callback(rid, response, exception):
                    outcomes[rid] = (response, exception)
                batch.add(service.files().copy(
                    fileId=item["source_id"],
                    body={"parents": [item["parent"]], "name": item["name"]},
                    fields=FIELDS, supportsAllDrives=True,
                ), request_id=request_id, callback=callback)
            outer_error = None
            try:
                batch.execute()
            except Exception as exc:
                outer_error = exc
            retries = []
            for index, (item, attempt) in enumerate(chunk):
                response, exc = outcomes.get(str(index), (None, outer_error))
                if str(index) not in outcomes:
                    save(item, "uncertain", error=exc or "Missing batch response")
                    continue
                if exc is None:
                    if _matches(item, response or {}):
                        save(item, "success", response)
                    else:
                        save(item, "failed", response, "Copy metadata mismatch")
                    continue
                status = getattr(getattr(exc, "resp", None), "status", None)
                if (status == 429 or status is not None and 500 <= status <= 599) and attempt < max_retries:
                    retries.append((item, attempt + 1))
                else:
                    save(item, "failed", error=exc)
            progress(dict(counts, pending=len(pending) + len(retries)))
            if retries:
                sleep(min(32, 2 ** max(attempt for _, attempt in retries)))
                pending = retries + pending
            elif pending and batch_delay:
                sleep(batch_delay)
    return counts
