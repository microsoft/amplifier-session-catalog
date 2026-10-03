"""Bounded, location-only writer notices. Native metadata remains authority."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import stat
import uuid

_NAME = re.compile(r"([a-f0-9]{64})(?:\.json|\.processing-[a-f0-9]{32})\Z")
MAX_HINT_BYTES = 8192


class HintInbox:
    """One private spool, one bounded consumer; claimed notices survive a crash."""

    def __init__(self, directory, discovery):
        if os.name != "posix":
            raise ValueError("Private hint inbox v1 requires POSIX ownership/mode checks")
        path = Path(directory).expanduser()
        if not path.is_absolute() or path.is_symlink():
            raise ValueError("Hint directory must be absolute and not a symlink")
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
        info = path.stat()
        if not stat.S_ISDIR(info.st_mode) or info.st_mode & 0o077:
            raise ValueError("Hint directory must be private")
        if hasattr(os, "getuid") and info.st_uid != os.getuid():
            raise ValueError("Hint directory has a different owner")
        self.directory, self.discovery = path, discovery
        self.entries = None

    def drain(self, limit=64):
        if type(limit) is not int or not 1 <= limit <= 256:
            raise ValueError("Hint batch limit must be 1..256")
        result = {"examined": 0, "processed": 0, "rejected": 0, "failed": 0}
        if self.entries is None:
            self.entries = os.scandir(self.directory)
        for _ in range(limit):
            try:
                entry = next(self.entries)
            except StopIteration:
                self.close()
                break
            result["examined"] += 1
            match = _NAME.fullmatch(entry.name)
            if not match:
                continue
            claim = Path(entry.path)
            try:
                # Leave foreign directories intact, including their contents.
                # They are not notice files and must not be renamed as claims.
                if entry.is_dir(follow_symlinks=False):
                    result["rejected"] += 1
                    continue
                if entry.name.endswith(".json"):
                    claim = self.directory / (match[1] + ".processing-" + uuid.uuid4().hex)
                    os.replace(entry.path, claim)
                if not stat.S_ISREG(claim.lstat().st_mode):
                    raise ValueError("Hint must not be a symlink or special file")
                descriptor = os.open(claim, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0))
                with os.fdopen(descriptor, "rb") as stream:
                    info = os.fstat(stream.fileno())
                    if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_HINT_BYTES:
                        raise ValueError("Hint must be a bounded regular file")
                    raw = stream.read(MAX_HINT_BYTES + 1)
                if len(raw) > MAX_HINT_BYTES:
                    raise ValueError("Hint grew past byte limit")
                value = json.loads(raw)
                if not isinstance(value, dict) or set(value) != {"version", "sessionDirectory"} or type(value["version"]) is not int or value["version"] != 1 or not isinstance(value["sessionDirectory"], str):
                    raise ValueError("Invalid location-only v1 hint")
                if hashlib.sha256(value["sessionDirectory"].encode("utf-8")).hexdigest() != match[1]:
                    raise ValueError("Hint filename does not match its location")
                # Same public contract used by ACP writer notifications. This
                # validates native roots and reads bounded metadata, never events.
                self.discovery.hint(sessionDirectory=value["sessionDirectory"])
                claim.unlink(missing_ok=True)
                result["processed"] += 1
            except FileNotFoundError:
                continue  # Another consumer or coalescing writer won the race.
            except (ValueError, UnicodeDecodeError, RecursionError):
                try:
                    claim.unlink(missing_ok=True)
                except OSError:
                    # A concurrent replacement may now be a directory. Never
                    # remove it recursively or let it starve the entire inbox.
                    pass
                result["rejected"] += 1
            except OSError:
                result["failed"] += 1  # Preserve the claim for a later bounded pass.
            except Exception:
                result["failed"] += 1
        return result

    def close(self):
        if self.entries is not None:
            self.entries.close()
            self.entries = None
