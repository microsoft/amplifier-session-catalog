"""Incremental metadata discovery. No transcript or event contents are read."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import stat
import threading
import time
import uuid

from .catalog import Catalog, directory_status, iso, timestamp

MAX_METADATA = 8 * 1024 * 1024
METADATA = ("metadata.json", "metadata.json.backup", "naming.json", "context-intelligence/metadata.json")
FIELDS = {"name", "title", "description", "bundle", "bundle_name", "parent_id", "parent_session_id",
          "agent_name", "created", "created_at", "started_at", "updated_at", "last_updated",
          "last_event_at", "turn_count", "working_dir", "cwd", "project_dir", "workspace",
          "forked_from_turn", "forked_at", "session_visibility", "session_purpose"}


def project_slug(workspace):
    path = str(Path(workspace).expanduser().resolve()).replace("/", "-").replace("\\", "-").replace(":", "")
    return path if path.startswith("-") else "-" + path


def session_uri(home, project, identity):
    namespace = f"amplifier-native:{Path(home).resolve()}:{project}/{identity}"
    return "ahp-session:/" + str(uuid.uuid5(uuid.NAMESPACE_URL, namespace))


def classify(identity, *sources):
    # Preserve Unified's native lineage rules, including independent CLI forks.
    for metadata in sources:
        keys = [k for k in ("parent_id", "parent_session_id") if k in metadata
                and (metadata[k] is None or isinstance(metadata[k], str))]
        key = next((k for k in keys if metadata[k]), keys[0] if keys else None)
        parent = metadata.get(key) or None
        if metadata.get("_independent_fork") or (type(metadata.get("forked_from_turn")) is int
                and metadata["forked_from_turn"] >= 0 and metadata.get("forked_at")):
            return "root", parent
        if metadata.get("session_visibility") == "internal":
            return "internal", parent
        if key is not None:
            return ("child" if parent else "root"), parent
        if metadata.get("session_visibility") == "chat":
            return "root", None
    return ("child" if "_" in identity else "root"), None


def small_metadata(path):
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    with os.fdopen(fd, "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_METADATA:
            raise ValueError("Native metadata must be a regular file of at most 8 MiB")
        data = stream.read(MAX_METADATA + 1)
        if len(data) > MAX_METADATA:
            raise ValueError("Native metadata grew beyond the limit")
    value = json.loads(data)
    if not isinstance(value, dict):
        raise ValueError("Native metadata must be an object")
    result = {k: v for k, v in value.items() if k in FIELDS and (v is None or isinstance(v, (str, int, float, bool)))}
    config = value.get("config")
    if isinstance(config, dict):
        for key in ("working_dir", "cwd", "project_dir", "workspace"):
            if not result.get(key) and isinstance(config.get(key), str):
                result[key] = config[key]
    fork = value.get("fork")
    if isinstance(fork, dict) and fork.get("source_session_id") and type(fork.get("through_user_turn")) is int:
        result["_independent_fork"] = True
    return result


def file_stamp(path):
    try:
        info = path.stat(follow_symlinks=False)
    except FileNotFoundError:
        return None
    if not stat.S_ISREG(info.st_mode):
        raise ValueError("Native source must be a regular file")
    return [info.st_ino, info.st_mtime_ns, info.st_size]


class Discovery:
    def __init__(self, catalog: Catalog, homes, *, known_workspaces=(), engine_id="amplifier", app_homes=()):
        self.catalog = catalog
        self.homes = tuple(Path(home).expanduser().resolve() for home in homes)
        self.known = {project_slug(path): str(Path(path).expanduser().resolve()) for path in known_workspaces}
        self.engine_id = engine_id
        self.app_homes = tuple(Path(home).expanduser().resolve() for home in app_homes)
        self.stop = threading.Event()
        self._lock = threading.Lock()
        self._thread = None
        self.metadata_reads = 0

    def _validate_directory(self, directory):
        directory = Path(directory).expanduser().absolute()
        for home in self.homes:
            try:
                parts = directory.relative_to(home / "projects").parts
            except ValueError:
                continue
            if (len(parts) == 3 and parts[1] == "sessions" and not parts[0].startswith(".")
                    and not parts[2].startswith(".") and directory.resolve() == directory):
                return home, parts[0], parts[2]
        raise ValueError("Session directory is outside configured native roots or uses a symlink")

    def hint(self, sessionDirectory, *, run=None, force=False):
        """Read current facts after a writer hint; never trust hinted metadata."""
        home, project, identity = self._validate_directory(sessionDirectory)
        directory = Path(sessionDirectory).expanduser().absolute()
        previous = self.catalog.by_storage(directory)
        stamps = [file_stamp(directory / name) for name in METADATA]
        transcript = file_stamp(directory / "transcript.jsonl") or file_stamp(directory / "transcript.jsonl.backup")
        markers = [(root / "sessions" / identity / "lifecycle.json") for root in self.app_homes]
        marker_stamps = [file_stamp(path) for path in markers]
        fingerprint = hashlib.sha256(json.dumps([stamps, transcript, marker_stamps]).encode()).hexdigest()
        if previous and previous["fingerprint"] == fingerprint and not force:
            if run:
                self.catalog.touch_seen(previous["uri"], run)
            return {"uri": previous["uri"], "changed": False}
        native, capture, naming, issues = {}, {}, {}, []
        for i, name in enumerate(METADATA):
            if not stamps[i]:
                continue
            if i == 1 and native:
                continue
            try:
                data = small_metadata(directory / name)
                self.metadata_reads += 1
                if i in (0, 1):
                    native = data
                    if i == 1:
                        issues.append("metadata recovered from backup")
                elif i == 2:
                    naming = data
                else:
                    capture = data
            except (OSError, ValueError, RecursionError) as exc:
                issues.append(f"{name}: {type(exc).__name__}")
        if not native and not capture:
            if previous:
                record = self.catalog.get(previous["uri"])
                record.update(availability="read-only", reason="Native metadata is currently unavailable")
                self.catalog.upsert(record, source="native:" + str(home), seen_run=run)
                return {"uri": previous["uri"], "changed": True, "issues": issues}
            return {"changed": False, "issues": issues or ["No readable native metadata"]}
        merged = {**capture, **native}
        # Native metadata is canonical. Older naming sidecars fill only absent fields.
        for key in ("name", "title", "description"):
            if not merged.get(key) and naming.get(key):
                merged[key] = naming[key]
        workspace = None
        for metadata in (native, capture):
            for key in ("working_dir", "cwd", "project_dir", "workspace"):
                value = metadata.get(key)
                if isinstance(value, str) and value and Path(value).expanduser().is_absolute():
                    path = str(Path(value).expanduser().resolve())
                    if project_slug(path) == project:
                        workspace = path
                        break
            if workspace:
                break
        workspace = workspace or self.known.get(project)
        if not workspace:
            return {"changed": False, "issues": issues + ["Workspace identity unresolved; project slugs are not reversible paths"]}
        previous = previous or self.catalog.by_native(self.engine_id, identity, workspace, directory)
        turns = merged.get("turn_count")
        if not previous and not (transcript and transcript[2]) and not (type(turns) is int and turns > 0):
            return {"changed": False, "issues": issues, "empty": True}
        kind, parent = classify(identity, native, capture)
        activity = transcript[1] / 1e9 if transcript and transcript[2] else timestamp(merged.get("last_event_at"))
        created = next((timestamp(merged[k]) for k in ("created", "created_at", "started_at")
                        if timestamp(merged.get(k))), activity)
        uri = previous["uri"] if previous else session_uri(home, project, identity)
        record = {"uri": uri, "engineId": self.engine_id, "nativeSessionId": identity,
                  "workingDirectory": workspace, "kind": kind, "storagePath": str(directory),
                  "nativeProject": project, "title": merged.get("name") or merged.get("title")
                  or merged.get("agent_name") or f"Conversation {identity[:8]}",
                  "createdAt": iso(created), "modifiedAt": iso(activity or created),
                  "availability": "read-only" if not native else "available"}
        if parent:
            parent_record = self.catalog.by_storage(directory.parent / parent)
            parent_record = parent_record or self.catalog.by_native(self.engine_id, parent, workspace, directory.parent / parent)
            record["parentUri"] = parent_record["uri"] if parent_record else session_uri(home, project, parent)
        for key in ("bundle", "description"):
            if isinstance(merged.get(key), str):
                record[key] = merged[key][:8192]
        if issues:
            record["reason"] = "; ".join(issues)[:8192]
        self.catalog.upsert(record, source="native:" + str(home), fingerprint=fingerprint, seen_run=run)
        for path, stamp in zip(markers, marker_stamps):
            if not stamp: continue
            try:
                if stamp[2] > 65536 or any(part.is_symlink() for part in (path, *path.parents)):
                    raise ValueError("Unsafe native lifecycle marker")
                fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
                with os.fdopen(fd, "rb") as stream: raw = stream.read(65537)
                if len(raw) > 65536: raise ValueError("Oversized lifecycle marker")
                marker = json.loads(raw)
                if marker.get("sessionId") != identity or marker.get("historyCwd") != workspace:
                    raise ValueError("Lifecycle marker identity does not match native history")
                self.catalog.set_visibility(uri=uri, nativeSessionId=identity, workingDirectory=workspace,
                    deleted=marker["deleted"], commandId=marker["commandId"], evidence="configured-native-marker")
            except (OSError, ValueError, KeyError, TypeError):
                issues.append("Native lifecycle marker is unavailable or invalid; prior visibility retained")
        return {"uri": uri, "changed": True, "issues": issues}

    @staticmethod
    def _directories(path):
        with os.scandir(path) as entries:
            for entry in entries:
                if not entry.name.startswith(".") and entry.is_dir(follow_symlinks=False):
                    yield Path(entry.path)

    def scan(self):
        if not self._lock.acquire(blocking=False):
            return self.catalog.progress()
        run = str(uuid.uuid4())
        state = {"phase": "running", "runId": run, "projects": 0, "sessions": 0,
                 "changed": 0, "issues": 0, "metadataReads": 0, "startedAt": iso(time.time())}
        initial_reads = self.metadata_reads
        self.catalog.progress(state)
        try:
            for home in self.homes:
                if self.stop.is_set():
                    break
                try:
                    for project in self._directories(home / "projects"):
                        if self.stop.is_set():
                            break
                        state["projects"] += 1
                        try:
                            for directory in self._directories(project / "sessions"):
                                if self.stop.is_set():
                                    break
                                try:
                                    result = self.hint(str(directory), run=run)
                                    state["changed"] += int(result["changed"])
                                    state["issues"] += len(result.get("issues", []))
                                except (OSError, ValueError, RecursionError):
                                    state["issues"] += 1
                                state["sessions"] += 1
                                if state["sessions"] % 64 == 0:
                                    state["metadataReads"] = self.metadata_reads - initial_reads
                                    self.catalog.progress(state)
                        except (OSError, ValueError):
                            state["issues"] += 1
                except (OSError, ValueError):
                    state["issues"] += 1
            state["phase"] = "interrupted" if self.stop.is_set() else "complete"
        except BaseException:
            state["phase"] = "failed"
            raise
        finally:
            state["metadataReads"] = self.metadata_reads - initial_reads
            state["completedAt"] = iso(time.time())
            self.catalog.progress(state)
            self._lock.release()
        return state

    def start(self):
        if self._thread and self._thread.is_alive():
            return self.catalog.progress()
        self._thread = threading.Thread(target=self.scan, name="catalog-discovery", daemon=True)
        self._thread.start()
        return {"phase": "scheduled"}

    def close(self):
        self.stop.set()
        if self._thread:
            self._thread.join(timeout=10)
