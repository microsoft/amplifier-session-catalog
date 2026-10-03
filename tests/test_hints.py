import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

import pytest

from amplifier_session_catalog import Catalog
from amplifier_session_catalog.discovery import Discovery, project_slug
from amplifier_session_catalog.hints import HintInbox

pytestmark = pytest.mark.skipif(os.name != "posix", reason="Private hint inbox v1 requires POSIX ownership/mode checks")


def notice(inbox, directory):
    value = {"version": 1, "sessionDirectory": str(directory)}
    path = inbox / (hashlib.sha256(str(directory).encode()).hexdigest() + ".json")
    path.write_text(json.dumps(value))
    return path


def native(tmp_path, identity="one"):
    workspace = tmp_path / "workspace"
    workspace.mkdir(exist_ok=True)
    home = tmp_path / "native"
    directory = home / "projects" / project_slug(workspace) / "sessions" / identity
    directory.mkdir(parents=True)
    (directory / "metadata.json").write_text(json.dumps({"name": identity, "working_dir": str(workspace), "parent_id": None}))
    (directory / "transcript.jsonl").write_bytes(b'preserved transcript\n')
    (directory / "events.jsonl").write_bytes(b'preserved historical events\n')
    return home, directory


def test_hints_use_public_discovery_without_opening_history(tmp_path, monkeypatch):
    home, directory = native(tmp_path)
    catalog = Catalog(tmp_path / "catalog.sqlite")
    discovery = Discovery(catalog, [home])
    inbox = HintInbox(tmp_path / "hints", discovery)
    notice(inbox.directory, directory)
    original = os.open
    def guarded(path, *args, **kwargs):
        assert Path(path).name not in {"transcript.jsonl", "events.jsonl"}
        return original(path, *args, **kwargs)
    monkeypatch.setattr(os, "open", guarded)
    result = inbox.drain()
    assert result["processed"] == 1
    assert catalog.list(connectionId="test")["items"][0]["nativeSessionId"] == "one"
    assert (directory / "transcript.jsonl").read_bytes() == b'preserved transcript\n'
    assert (directory / "events.jsonl").read_bytes() == b'preserved historical events\n'
    inbox.close()


def test_claim_does_not_unlink_a_concurrent_writer_and_crash_claim_recovers(tmp_path):
    directory = tmp_path / "session"
    directory.mkdir()
    class Reader:
        def __init__(self): self.calls = 0
        def hint(self, sessionDirectory):
            self.calls += 1
            if self.calls == 1:
                notice(inbox.directory, sessionDirectory)
    reader = Reader()
    inbox = HintInbox(tmp_path / "hints", reader)
    path = notice(inbox.directory, directory)
    assert inbox.drain(limit=1)["processed"] == 1
    assert path.exists(), "Consuming an old claim must preserve the newly coalesced notice"
    inbox.close()
    claimed = path.with_name(path.stem + ".processing-" + "a" * 32)
    path.rename(claimed)
    recovered = HintInbox(inbox.directory, reader)
    assert recovered.drain(limit=1)["processed"] == 1
    assert reader.calls == 2
    assert not claimed.exists()
    recovered.close()


def test_batches_are_bounded_and_invalid_locations_or_special_files_are_rejected(tmp_path):
    home, directory = native(tmp_path)
    reader = Discovery(Catalog(tmp_path / "catalog.sqlite"), [home])
    inbox = HintInbox(tmp_path / "hints", reader)
    outside = tmp_path / "outside"
    outside.mkdir()
    bad = notice(inbox.directory, outside)
    oversized = inbox.directory / ("b" * 64 + ".json")
    oversized.write_bytes(b"x" * 8193)
    symlink = inbox.directory / ("c" * 64 + ".json")
    symlink.symlink_to(directory / "events.jsonl")
    fifo = inbox.directory / ("d" * 64 + ".json")
    if hasattr(os, "mkfifo"):
        os.mkfifo(fifo)
    total = 0
    for _ in range(10):
        result = inbox.drain(limit=1)
        assert result["examined"] <= 1
        total += result["rejected"]
    assert total == (4 if hasattr(os, "mkfifo") else 3)
    assert not bad.exists() and not symlink.exists()
    assert (directory / "events.jsonl").read_bytes() == b'preserved historical events\n'
    inbox.close()


def test_failed_index_write_keeps_claim_for_later_retry(tmp_path):
    class Reader:
        def __init__(self): self.fail = True
        def hint(self, sessionDirectory):
            if self.fail: raise OSError("index temporarily unavailable")
    reader = Reader()
    inbox = HintInbox(tmp_path / "hints", reader)
    notice(inbox.directory, tmp_path)
    assert inbox.drain(limit=1)["failed"] == 1
    assert len(list(inbox.directory.iterdir())) == 1
    inbox.close()
    reader.fail = False
    assert inbox.drain(limit=1)["processed"] == 1
    inbox.close()


def test_foreign_directory_is_preserved_without_stalling_following_notice(tmp_path):
    class Reader:
        def hint(self, sessionDirectory): pass
    inbox = HintInbox(tmp_path / "hints", Reader())
    foreign = inbox.directory / ("a" * 64 + ".json")
    foreign.mkdir()
    (foreign / "preserved").write_text("foreign data")
    notice(inbox.directory, tmp_path)
    result = inbox.drain()
    assert result["rejected"] == 1 and result["processed"] == 1
    assert (foreign / "preserved").read_text() == "foreign data"
    inbox.close()


def test_serve_consumes_notices_with_scans_disabled(tmp_path):
    home, directory = native(tmp_path)
    inbox = tmp_path / "hints"
    inbox.mkdir(mode=0o700)
    process = subprocess.Popen([sys.executable, "-m", "amplifier_session_catalog", "serve", "--db", str(tmp_path / "catalog.sqlite"), "--home", str(home), "--hint-directory", str(inbox), "--hint-interval", ".02", "--scan-interval", "0", "--workspace-check-interval", "0"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        notice(inbox, directory)
        deadline = time.monotonic() + 5
        while True:
            process.stdin.write(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "list", "params": {"connectionId": "hint-only", "limit": 1}}) + "\n")
            process.stdin.flush()
            result = json.loads(process.stdout.readline())
            if result["result"]["items"]:
                break
            assert time.monotonic() < deadline
            time.sleep(.02)
        assert result["result"]["items"][0]["nativeSessionId"] == "one"
        assert result["result"]["freshness"]["phase"] == "not-started"
    finally:
        process.stdin.close()
        process.wait(timeout=5)
        assert process.returncode == 0, process.stderr.read()
