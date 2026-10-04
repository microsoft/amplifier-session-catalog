"""Incremental metadata discovery. No transcript or event contents are read."""
from __future__ import annotations

from collections import OrderedDict
import hashlib
import json
import os
from pathlib import Path
import stat
import threading
import time
import uuid

from .catalog import Catalog, directory_status, iso, timestamp, managed_location, managed_root

class MetadataReadChanged(ValueError):
    """Typed transient descriptor race; public validation remains compatible."""


class RetryableHint(OSError):
    """Private inbox outcome; keep the immutable claim for another bounded pass."""


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


def read_metadata_json(path, maximum=MAX_METADATA, *, admit=None):
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0))
    with os.fdopen(fd, "rb", buffering=0) as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size > maximum:
            raise ValueError("Native metadata must be a bounded regular file")
        if admit is not None:
            admit(Path(path), info)
        # Admit the opened descriptor, not an earlier path stat. Never consume
        # bytes beyond that admission, even if a concurrent writer grows it.
        data = stream.read(info.st_size)
        after = os.fstat(stream.fileno())
        if len(data) != info.st_size or (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
            raise MetadataReadChanged("Native metadata changed during read")
    value = json.loads(data)
    if not isinstance(value, dict):
        raise ValueError("Native metadata must be an object")
    return value


def small_metadata(path, *, read_json=read_metadata_json):
    value = read_json(path)
    result = {k: v for k, v in value.items() if k in FIELDS and (v is None or isinstance(v, (str, int, float, bool)))}
    if 'location' in value:
        try:result['location']=managed_location(value['location'])
        except (ValueError,TypeError,AttributeError):result['_location_invalid']=True
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
        self._family_proofs = OrderedDict()
        self._proof_lock = threading.Lock()

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

    def hint(self, sessionDirectory, *, run=None, force=False, _read_json=None):
        """Read current facts after a writer hint; never trust hinted metadata."""
        return self._hint(sessionDirectory, run=run, force=force, _read_json=_read_json)

    def _hint_for_inbox(self, sessionDirectory):
        # Not registered as RPC. Public hints retain their existing result shape.
        return self._hint(sessionDirectory, retry_transient=True)

    def _hint(self, sessionDirectory, *, run=None, force=False, _read_json=None, retry_transient=False):
        home, project, identity = self._validate_directory(sessionDirectory)
        directory = Path(sessionDirectory).expanduser().absolute()
        previous = self.catalog.by_storage(directory)
        try:
            directory_info = directory.stat(follow_symlinks=False)
        except FileNotFoundError:
            record = self.catalog.get(previous['uri']) if previous else None
            if not record or record.get('storagePath') != str(directory) or record['engineId'] != self.engine_id or record['nativeSessionId'] != identity or project_slug(record['workingDirectory']) != project:
                return {'changed': False, 'issues': ['Native source directory is missing or not exactly indexed']}
            def absent():
                self._validate_directory(directory)
                try:
                    directory.stat(follow_symlinks=False)
                except FileNotFoundError:
                    return True
                return False
            changed = self.catalog._observe_source_presence(record, missing=True, confirm=absent)
            if not changed and retry_transient:raise RetryableHint('Selected source changed during absence observation')
            return {'uri': record['uri'], 'changed': changed, 'issues': [] if changed else ['Native source changed during absence observation']}
        if not stat.S_ISDIR(directory_info.st_mode):
            raise ValueError('Native source directory must be a nonsymlink directory')
        stamps = [file_stamp(directory / name) for name in METADATA]
        transcript = file_stamp(directory / "transcript.jsonl") or file_stamp(directory / "transcript.jsonl.backup")
        markers = [(root / "sessions" / identity / "lifecycle.json") for root in self.app_homes]
        marker_stamps = [file_stamp(path) for path in markers]
        # A derived-index upgrade must rebuild native parent evidence even when
        # canonical file stamps have not changed since the prior scanner version.
        fingerprint = hashlib.sha256(json.dumps(['native-parent-v1',stamps, transcript, marker_stamps]).encode()).hexdigest()
        if previous and previous["fingerprint"] == fingerprint and not force and not previous.get("source_missing"):
            if run:
                self.catalog.touch_seen(previous["uri"], run)
            return {"uri": previous["uri"], "changed": False}
        native, capture, naming, issues = {}, {}, {}, []
        transient_native = False
        def read_json(path, maximum=MAX_METADATA):
            def count(_path, _info):
                self.metadata_reads += 1
            return (_read_json(path, maximum) if _read_json is not None
                    else read_metadata_json(path, maximum, admit=count))
        for i, name in enumerate(METADATA):
            if not stamps[i]:
                continue
            if i == 1 and native:
                continue
            try:
                data = small_metadata(directory / name, read_json=read_json)
                if i in (0, 1):
                    native = data
                    if i == 1:
                        issues.append("metadata recovered from backup")
                elif i == 2:
                    naming = data
                else:
                    capture = data
            except (OSError, ValueError, RecursionError) as exc:
                if i in (0,1) and isinstance(exc,(OSError,MetadataReadChanged)):
                    transient_native = True
                # Keep public diagnostic wording compatible for descriptor races.
                category='ValueError' if isinstance(exc,MetadataReadChanged) else type(exc).__name__
                issues.append(f"{name}: {category}")
        if retry_transient and transient_native and not native:
            # Failed reads are not newer source facts. In particular, an older
            # drainer must not downgrade a successful concurrent projection.
            raise RetryableHint('Selected native metadata temporarily unavailable')
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
        location=native.get('location')
        if location:
            try:managed_root(workspace,location)
            except ValueError:
                location=None
                issues.append('Managed location does not match native workspace; prior projection retained')
        if native.get('_location_invalid'):
            issues.append('Managed location is invalid; prior projection retained')
        kind, parent = classify(identity, native, capture)
        managed_uri=None
        if location and kind=='root':
            if identity!=location['allocationId']:
                location=None
                issues.append('Managed root native identity differs from allocation; prior projection retained')
            else:
                managed_uri='ahp-session:/'+location['allocationId']
                if previous and previous['uri']!=managed_uri:
                    return {'changed':False,'issues':issues+['Managed identity differs from existing mapping; owner repair required']}
                existing=self.catalog.get(managed_uri)
                if existing and (existing['engineId'],existing['nativeSessionId'],existing['workingDirectory'])!=(self.engine_id,identity,workspace):
                    return {'changed':False,'issues':issues+['Managed identity is already bound to another source; owner repair required']}
        turns = merged.get("turn_count")
        if not previous and not location and not (transcript and transcript[2]) and not (type(turns) is int and turns > 0):
            return {"changed": False, "issues": issues, "empty": True}
        activity = transcript[1] / 1e9 if transcript and transcript[2] else timestamp(merged.get("last_event_at"))
        created = next((timestamp(merged[k]) for k in ("created", "created_at", "started_at")
                        if timestamp(merged.get(k))), activity)
        uri = previous["uri"] if previous else managed_uri or session_uri(home, project, identity)
        record = {"uri": uri, "engineId": self.engine_id, "nativeSessionId": identity,
                  "workingDirectory": workspace, "kind": kind, "storagePath": str(directory),
                  "nativeProject": project, "title": merged.get("name") or merged.get("title")
                  or merged.get("agent_name") or f"Conversation {identity[:8]}",
                  "createdAt": iso(created), "modifiedAt": iso(activity or created),
                  "availability": "read-only" if not native else "available"}
        if location:record['location']=location
        if parent:
            parent_record = self.catalog.by_storage(directory.parent / parent)
            parent_record = parent_record or self.catalog.by_native(self.engine_id, parent, workspace, directory.parent / parent)
            record["parentUri"] = parent_record["uri"] if parent_record else session_uri(home, project, parent)
        for key in ("bundle", "description"):
            if isinstance(merged.get(key), str):
                record[key] = merged[key][:8192]
        if issues:
            record["reason"] = "; ".join(issues)[:8192]
        confirmations=[]
        transient_lifecycle=False
        for path, stamp in zip(markers, marker_stamps):
            if not stamp: continue
            try:
                if stamp[2] > 65536 or any(part.is_symlink() for part in (path, *path.parents)):
                    raise ValueError("Unsafe native lifecycle marker")
                marker = read_json(path, 65536)
                if marker.get("sessionId") != identity or marker.get("historyCwd") != workspace:
                    raise ValueError("Lifecycle marker identity does not match native history")
                if type(marker.get("deleted")) is not bool or not isinstance(marker.get("commandId"),str) or not 1 <= len(marker["commandId"]) <= 200:
                    raise ValueError("Invalid native lifecycle marker proof")
                confirmations.append({"deleted":marker["deleted"], "commandId":marker["commandId"], "evidence":"configured-native-marker"})
            except (OSError, ValueError, KeyError, TypeError, RecursionError) as exc:
                if isinstance(exc,(OSError,MetadataReadChanged)):transient_lifecycle=True
                issues.append("Native lifecycle marker is unavailable or invalid; prior visibility retained")
        confirmation=confirmations[0] if confirmations and all(value==confirmations[0] for value in confirmations) else None
        if confirmations and confirmation is None: issues.append("Conflicting native lifecycle markers; prior visibility retained")
        if retry_transient and transient_lifecycle:
            # An older failed drainer has no newer facts to publish, including
            # its earlier native metadata or partial lifecycle confirmations.
            # A successful competitor may already have consumed their claim.
            raise RetryableHint('Selected authoritative metadata temporarily unavailable')
        indexed = self.catalog.upsert(record, source="native:" + str(home), fingerprint=fingerprint, seen_run=run, confirmed_visibility=confirmation,
                            native_parent={'nativeId':parent,'storagePath':str(directory.parent/parent)} if parent else None)
        if native and not issues and previous and previous.get("source_missing"):
            def present():
                self._validate_directory(directory)
                current = directory.stat(follow_symlinks=False)
                return stat.S_ISDIR(current.st_mode) and (current.st_dev, current.st_ino) == (directory_info.st_dev, directory_info.st_ino) and [file_stamp(directory/name) for name in METADATA] == stamps
            restored=self.catalog._observe_source_presence({**record, 'uri': indexed['uri']}, missing=False, confirm=present)
            if not restored and retry_transient:raise RetryableHint('Selected source changed during presence restoration')
        return {"uri": uri, "changed": True, "issues": issues}

    def _family_inventory(self, namespace, budget):
        # Count ALL entries, including irrelevant lock/temp files: no hidden unbounded walk.
        values = []
        with os.scandir(namespace) as iterator:
            for entry in iterator:
                if len(values) >= budget:
                    raise ValueError('namespace-entry-limit')
                mode = entry.stat(follow_symlinks=False).st_mode
                if stat.S_ISLNK(mode):
                    raise ValueError('namespace-symlink')
                stamps = []
                if stat.S_ISDIR(mode) and not entry.name.startswith('.'):
                    paths = [Path(entry.path)/name for name in METADATA + ('transcript.jsonl', 'transcript.jsonl.backup')]
                    paths.extend(root/'sessions'/entry.name/'lifecycle.json' for root in self.app_homes)
                    for path in paths:
                        try:
                            if path.resolve() != path:
                                raise ValueError('source-metadata-symlink')
                            info = path.lstat()
                        except FileNotFoundError:
                            stamps.append(None)
                            continue
                        if not stat.S_ISREG(info.st_mode):
                            raise ValueError('source-special-file')
                        stamps.append((info.st_ino, info.st_size, info.st_mtime_ns, info.st_dev, info.st_ctime_ns))
                values.append((entry.name, stat.S_ISDIR(mode), stamps))
        return sorted(values)

    def verify_family(self, *, parentUri, verificationToken):
        """Passive bounded physical/index recheck; never repair or authorize effects."""
        if not isinstance(parentUri, str) or not parentUri or len(parentUri) > 512:
            raise ValueError('Invalid parentUri')
        if not isinstance(verificationToken, str) or len(verificationToken) > 200:
            raise ValueError('Invalid verificationToken')
        result = {'version': 1, 'scope': 'selected-native-project-namespaces',
                  'parentUri': parentUri, 'valid': False, 'issues': [],
                  'externalWriterCoverage': False, 'nativeOwnershipVerified': False,
                  'transcriptBodiesRead': False}
        with self._proof_lock:
            prior = self._family_proofs.get(verificationToken)
        if not prior or prior['parentUri'] != parentUri or prior['expires'] < time.monotonic():
            result['issues'].append('unknown-expired-or-mismatched-family-proof')
        else:
            result.update(indexRevision=prior['revision'], inventoryHash=prior['inventoryHash'])
            deadline = time.monotonic()+5
            try:
                if self.catalog.descendants(parentUri=parentUri, limit=prior['limit'])['revision'] != prior['revision']:
                    raise ValueError('indexed-family-changed')
                for namespace, inventory in prior['inventories'].items():
                    if time.monotonic() > deadline:
                        raise ValueError('verification-time-limit')
                    if self._family_inventory(namespace, len(inventory)) != inventory:
                        raise ValueError('native-namespace-facts-changed')
                if self.catalog.descendants(parentUri=parentUri, limit=prior['limit'])['revision'] != prior['revision']:
                    raise ValueError('indexed-family-changed')
                result['valid'] = True
            except (OSError, ValueError, RuntimeError) as exc:
                result['issues'].append(str(exc) if isinstance(exc, ValueError) else type(exc).__name__)
        result['completedAt'] = iso(time.time())
        return result

    def reconcile_family(self, *, parentUri, limit=100, maxEntries=512):
        """Bounded selected namespace repair; never a native ownership proof."""
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError('limit must be 1..100')
        if type(maxEntries) is not int or not 1 <= maxEntries <= 512:
            raise ValueError('maxEntries must be 1..512')
        family = self.catalog.descendants(parentUri=parentUri, limit=limit)
        issues, inventories, entries, byte_count = [], {}, 0, 0
        source_reads, source_bytes, admitted_files = 0, 0, 0
        admitted_sources = {}
        deadline = time.monotonic() + 5
        proof = {'version': 1, 'scope': 'selected-native-project-namespaces',
                 'phase': 'refused', 'issues': issues, 'maxEntries': maxEntries,
                 'metadataByteLimit': MAX_METADATA, 'metadataFileLimit': 4*maxEntries, 'externalWriterCoverage': False,
                 'transcriptBodiesRead': False, 'nativeOwnershipVerified': False}
        def finish():
            result = self.catalog.descendants(parentUri=parentUri, limit=limit)
            proof.update(indexRevision=result['revision'], inventoryHash=hashlib.sha256(json.dumps([(str(path), rows) for path, rows in sorted(inventories.items())], sort_keys=True).encode()).hexdigest(), entriesExamined=entries,
                         metadataReads=source_reads, metadataBytesAdmitted=source_bytes,
                         namespaces=[str(path) for path in inventories], completedAt=iso(time.time()))
            if proof['metadataReads'] > proof['metadataFileLimit']:
                issues.append('metadata-file-limit')
            proof['phase'] = 'complete' if not issues and result['completeIndexedCoverage'] else 'refused'
            if proof['phase'] == 'complete':
                token = str(uuid.uuid4())
                with self._proof_lock:
                    self._family_proofs[token] = {'parentUri': parentUri, 'limit': limit,
                        'revision': result['revision'], 'inventoryHash': proof['inventoryHash'],
                        'inventories': inventories, 'expires': time.monotonic()+300}
                    while len(self._family_proofs) > 64:
                        self._family_proofs.popitem(last=False)
                proof['verificationToken'] = token
            result['familyDiscovery'] = proof
            result['nativeFilesRead'] = bool(proof['metadataReads'])
            return result
        if not self._lock.acquire(blocking=False):
            issues.append('discovery-busy')
            return finish()
        try:
            if len(self.homes) > 16 or len(self.app_homes) > 16:
                raise ValueError('configured-source-limit')
            def source(row):
                if row['engineId'] != self.engine_id:
                    raise ValueError('source-engine-unconfigured')
                known = self.catalog.get(row['uri'])
                if not known:
                    raise ValueError('source-not-indexed')
                workspace = known['workingDirectory']
                if str(Path(workspace).resolve()) != workspace:
                    raise ValueError('source-workspace-noncanonical')
                location = known.get('storagePath')
                if location:
                    directory = Path(location)
                    home, project, identity = self._validate_directory(directory)
                    if identity != known['nativeSessionId'] or project != project_slug(workspace):
                        raise ValueError('source-storage-identity-mismatch')
                else:
                    candidates = [home/'projects'/project_slug(workspace)/'sessions'/known['nativeSessionId']
                                  for home in self.homes]
                    candidates = [path for path in candidates if path.is_dir() and path.resolve() == path]
                    if len(candidates) != 1:
                        raise ValueError('source-missing-or-ambiguous')
                    directory = candidates[0]
                    self._validate_directory(directory)
                if not directory.is_dir():
                    raise ValueError('source-directory-missing')
                return directory.parent
            if not family['rootKnown']:
                issues.append('root-not-indexed')
                return finish()
            while True:
                if time.monotonic() > deadline:
                    raise ValueError('reconciliation-time-limit')
                family = self.catalog.descendants(parentUri=parentUri, limit=limit)
                if family['truncated'] or set(family['issues']) & {'cycle', 'depth-limit'}:
                    issues.append('indexed-family-incomplete')
                    break
                pending = {source(row) for row in [family['root'], *family['items']]} - set(inventories)
                if not pending:
                    break
                for namespace in sorted(pending):
                    values = self._family_inventory(namespace, maxEntries-entries)
                    entries += len(values)
                    for name, directory, stamps in values:
                        if directory and not name.startswith('.'):
                            paths = [namespace/name/file for file in METADATA]
                            paths.extend(root/'sessions'/name/'lifecycle.json' for root in self.app_homes)
                            source_stamps = stamps[:len(METADATA)] + stamps[len(METADATA)+2:]
                            for path, stamp in zip(paths, source_stamps):
                                if stamp:
                                    if path.name == 'lifecycle.json' and stamp[1] > 65536:
                                        raise ValueError('lifecycle-byte-limit')
                                    admitted_files += 1
                                    byte_count += stamp[1]
                                    admitted_sources[path] = stamp
                    if byte_count > MAX_METADATA:
                        raise ValueError('metadata-byte-limit')
                    if admitted_files > proof['metadataFileLimit']:
                        raise ValueError('metadata-file-limit')
                    inventories[namespace] = values
                    def read_json(path, maximum=MAX_METADATA):
                        def admit(path, info):
                            nonlocal source_reads, source_bytes
                            if time.monotonic() > deadline:
                                raise ValueError('reconciliation-time-limit')
                            if admitted_sources.get(path) != (info.st_ino, info.st_size, info.st_mtime_ns, info.st_dev, info.st_ctime_ns):
                                raise ValueError('source-changed-before-read')
                            if source_reads >= proof['metadataFileLimit'] or source_bytes + info.st_size > MAX_METADATA:
                                raise ValueError('source-read-budget-limit')
                            source_reads += 1
                            source_bytes += info.st_size
                            self.metadata_reads += 1
                        return read_metadata_json(path, maximum, admit=admit)
                    for name, directory, _ in values:
                        if directory and not name.startswith('.'):
                            if time.monotonic() > deadline:
                                raise ValueError('reconciliation-time-limit')
                            result = self.hint(str(namespace/name), force=True, _read_json=read_json)
                            if result.get('issues') or result.get('empty') or not result.get('uri'):
                                issues.append('namespace-metadata-incomplete')
                if issues:
                    break
            for namespace, previous in inventories.items():
                if self._family_inventory(namespace, len(previous)) != previous:
                    issues.append('source-changed-during-reconciliation')
            return finish()
        except (OSError, ValueError, RuntimeError) as exc:
            issues.append(str(exc) if isinstance(exc, ValueError) else type(exc).__name__)
            return finish()
        finally:
            self._lock.release()

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
