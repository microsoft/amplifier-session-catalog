"""Scoped SQLite queries; no resident copy of the historical catalog."""
from __future__ import annotations

import base64
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import hmac
import json
import math
import os
from pathlib import Path
import secrets
import sqlite3
import stat
import time


class InvalidCursor(ValueError):
    pass


def timestamp(value, default=0.0):
    if isinstance(value, str):
        try:
            date = datetime.fromisoformat(value.replace("Z", "+00:00"))
            value = date.replace(tzinfo=date.tzinfo or timezone.utc).timestamp()
        except (ValueError, OverflowError):
            return default
    if isinstance(value, (float, int)) and not isinstance(value, bool):
        if math.isfinite(value) and 0 <= value <= 253402300799:
            return float(value)
    return default


def iso(value):
    return datetime.fromtimestamp(timestamp(value), timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def directory_status(path):
    """Unavailable mounts/permissions are not proof of deletion."""
    try:
        mode = Path(path).stat().st_mode
        return "present" if stat.S_ISDIR(mode) else "missing"
    except (FileNotFoundError, NotADirectoryError):
        return "missing"
    except (OSError, ValueError):
        return "unknown"


def checked_string(value, field, limit=8192, optional=False):
    if value is None and optional:
        return None
    if not isinstance(value, str) or not value or len(value) > limit or "\x00" in value:
        raise ValueError(f"Invalid {field}")
    return value


class Catalog:
    """A derived index. Host receipts and native history are not stored here."""

    def __init__(self, path):
        self.path = Path(path).expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.secret = secrets.token_bytes(32)
        with self.connect() as db:
            version = db.execute("PRAGMA user_version").fetchone()[0]
            if version not in (0, 1):
                raise ValueError(f"Unsupported catalog version {version}; preserve it before rebuilding")
            db.executescript("""
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS workspaces (
                    path TEXT PRIMARY KEY, availability TEXT NOT NULL, checked_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS workspace_availability ON workspaces(availability, path);
                CREATE TABLE IF NOT EXISTS sessions (
                    uri TEXT PRIMARY KEY, engine TEXT NOT NULL, native_id TEXT NOT NULL,
                    workspace TEXT NOT NULL, parent_uri TEXT, kind TEXT NOT NULL,
                    title TEXT NOT NULL, created_at REAL NOT NULL, modified_at REAL NOT NULL,
                    availability TEXT NOT NULL, source TEXT NOT NULL, storage_path TEXT,
                    native_project TEXT, fingerprint TEXT, seen_run TEXT,
                    indexed_at REAL NOT NULL, details TEXT NOT NULL, visible INTEGER NOT NULL DEFAULT 0
                );
                CREATE INDEX IF NOT EXISTS roots_order ON sessions(kind, modified_at DESC, uri);
                CREATE INDEX IF NOT EXISTS visible_roots_order ON sessions(visible, kind, modified_at DESC, uri);
                CREATE INDEX IF NOT EXISTS workspace_order ON sessions(workspace, kind, modified_at DESC, uri);
                CREATE INDEX IF NOT EXISTS parent_order ON sessions(parent_uri, modified_at DESC, uri);
                CREATE INDEX IF NOT EXISTS storage_lookup ON sessions(storage_path);
                CREATE INDEX IF NOT EXISTS native_lookup ON sessions(engine, native_id, workspace);
                CREATE INDEX IF NOT EXISTS source_sessions ON sessions(source, seen_run);
                CREATE TRIGGER IF NOT EXISTS workspace_visibility AFTER UPDATE OF availability ON workspaces
                    WHEN OLD.availability <> NEW.availability BEGIN
                    UPDATE sessions SET visible=(NEW.availability='present') WHERE workspace=NEW.path;
                END;
                CREATE TABLE IF NOT EXISTS progress (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                PRAGMA user_version=1;
            """)
        os.chmod(self.path, 0o600)

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        try:
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    def workspace(self, path, *, availability=None):
        path = checked_string(path, "workingDirectory")
        if not Path(path).is_absolute():
            raise ValueError("workingDirectory must be absolute")
        availability = availability or directory_status(path)
        if availability not in {"present", "missing", "unknown"}:
            raise ValueError("Invalid workspace availability")
        with self.connect() as db:
            db.execute("INSERT INTO workspaces VALUES(?,?,?) ON CONFLICT(path) DO UPDATE SET availability=excluded.availability, checked_at=excluded.checked_at",
                       (path, availability, time.time()))
        return {"path": path, "availability": availability}

    def upsert(self, record, *, source="host", fingerprint=None, seen_run=None):
        if not isinstance(record, dict):
            raise ValueError("record must be an object")
        uri = checked_string(record.get("uri"), "uri", 512)
        if not uri.startswith("ahp-session:/"):
            raise ValueError("uri must identify an AHP session")
        engine = checked_string(record.get("engineId"), "engineId", 128)
        native = checked_string(record.get("nativeSessionId"), "nativeSessionId", 512)
        workspace = checked_string(record.get("workingDirectory"), "workingDirectory")
        if not Path(workspace).is_absolute():
            raise ValueError("workingDirectory must be absolute")
        parent = checked_string(record.get("parentUri"), "parentUri", 512, optional=True)
        kind = record.get("kind", "child" if parent else "root")
        if kind not in {"root", "child", "internal"}:
            raise ValueError("kind must be root, child or internal")
        availability = record.get("availability", "available")
        if availability not in {"available", "owned", "read-only", "unsupported"}:
            raise ValueError("Invalid native availability")
        title = record.get("title") or f"Conversation {native[:8]}"
        title = checked_string(title, "title", 8192)
        now = time.time()
        modified = timestamp(record.get("modifiedAt"), now)
        created = timestamp(record.get("createdAt"), modified)
        details = {k: record[k] for k in ("bundle", "description", "reason", "nativeProject") if k in record}
        raw = json.dumps(details, ensure_ascii=False)
        if len(raw.encode()) > 32768:
            raise ValueError("Catalog details exceed 32 KiB")
        storage = checked_string(record.get("storagePath"), "storagePath", optional=True)
        with self.connect() as db:
            known = db.execute("SELECT path FROM workspaces WHERE path=?", (workspace,)).fetchone()
        if known is None:
            self.workspace(workspace)
        with self.connect() as db:
            visible = db.execute("SELECT availability='present' FROM workspaces WHERE path=?", (workspace,)).fetchone()[0]
            db.execute("""INSERT INTO sessions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(uri) DO UPDATE SET engine=excluded.engine,native_id=excluded.native_id,
                workspace=excluded.workspace,parent_uri=excluded.parent_uri,kind=excluded.kind,
                title=excluded.title,created_at=excluded.created_at,modified_at=excluded.modified_at,
                availability=excluded.availability,source=excluded.source,storage_path=excluded.storage_path,
                native_project=excluded.native_project,fingerprint=excluded.fingerprint,
                seen_run=excluded.seen_run,indexed_at=excluded.indexed_at,details=excluded.details,visible=excluded.visible""",
                (uri, engine, native, workspace, parent, kind, title, created, modified, availability,
                 source, storage, record.get("nativeProject"), fingerprint, seen_run, now, raw, visible))
        return self.get(uri)

    @staticmethod
    def _record(row):
        if row is None:
            return None
        result = {"uri": row["uri"], "engineId": row["engine"], "nativeSessionId": row["native_id"],
                  "workingDirectory": row["workspace"], "title": row["title"], "kind": row["kind"],
                  "createdAt": iso(row["created_at"]), "modifiedAt": iso(row["modified_at"]),
                  "availability": row["availability"], "workspaceAvailability": row["workspace_availability"],
                  "indexedAt": iso(row["indexed_at"]), "status": "idle"}
        if row["parent_uri"]:
            result["parentUri"] = row["parent_uri"]
        if row["storage_path"]:
            result["storagePath"] = row["storage_path"]
        result.update(json.loads(row["details"]))
        return result

    def get(self, uri):
        checked_string(uri, "uri", 512)
        with self.connect() as db:
            row = db.execute("SELECT s.*, w.availability AS workspace_availability FROM sessions s JOIN workspaces w ON s.workspace=w.path WHERE uri=?", (uri,)).fetchone()
        return self._record(row)

    def by_storage(self, path):
        with self.connect() as db:
            row = db.execute("SELECT uri,fingerprint FROM sessions WHERE storage_path=? LIMIT 1", (str(path),)).fetchone()
        return dict(row) if row else None

    def directory_in_use(self, *, path):
        """Indexed historical references, not a lease on external agent writers.

        The trusted host supplies a canonical absolute path and holds its own
        admission/deletion guard. Include hidden children and archived workspaces;
        browsing visibility must never grant permission to remove native history.
        """
        path = checked_string(path, "path")
        if not Path(path).is_absolute() or os.path.normpath(path) != path:
            raise ValueError("path must be canonical and absolute")
        prefix = path.rstrip("/") + "/"
        upper = prefix[:-1] + "0"
        with self.connect() as db:
            rows = db.execute("""SELECT uri,workspace,storage_path FROM sessions
                WHERE workspace=? OR (workspace>=? AND workspace<?)
                   OR storage_path=? OR (storage_path>=? AND storage_path<?)
                LIMIT 101""", (path, prefix, upper, path, prefix, upper)).fetchall()
        return {"inUse": bool(rows), "sessions": [
            {"session": row["uri"], "reason": "history-workspace" if
             row["workspace"] == path or row["workspace"].startswith(prefix) else "native-history-storage"}
            for row in rows[:100]], "truncated": len(rows) > 100,
            "coverage": "indexed-native-history", "freshness": self.progress()}

    def resolve_native(self, *, engineId, identities, allowedWorkspaceRoots):
        """Bounded migration lookup; never guess an ambiguous native identity."""
        checked_string(engineId, "engineId", 200)
        if not isinstance(identities, list) or len(identities) > 500:
            raise ValueError("identities must contain at most 500 native IDs")
        identities = list(dict.fromkeys(checked_string(value, "native ID", 512) for value in identities))
        if not isinstance(allowedWorkspaceRoots, list) or len(allowedWorkspaceRoots) > 128:
            raise ValueError("allowedWorkspaceRoots must be bounded")
        roots = [checked_string(value, "workspace root").rstrip("/") or "/" for value in allowedWorkspaceRoots]
        clauses, scope = [], []
        for path in roots:
            prefix = path.rstrip("/") + "/"
            clauses.append("(workspace=? OR substr(workspace,1,?)=?)")
            scope.extend((path, len(prefix), prefix))
        where = "(" + " OR ".join(clauses) + ")" if clauses else "0"
        result = {}
        with self.connect() as db:
            for identity in identities:
                rows = db.execute("SELECT uri FROM sessions WHERE engine=? AND native_id=? AND " + where + " LIMIT 2", (engineId, identity, *scope)).fetchall()
                result[identity] = {"status": "matched", "uri": rows[0]["uri"]} if len(rows) == 1 else {"status": "ambiguous" if rows else "missing"}
        return result

    def by_native(self, engine, identity, workspace, storage):
        """Adopt a host-created identity once its native files become discoverable.

        Different explicit native homes stay distinct. An ambiguous legacy identity
        is not guessed, and no existing native URI is silently reassigned.
        """
        with self.connect() as db:
            rows = db.execute("""SELECT uri,fingerprint FROM sessions
                WHERE engine=? AND native_id=? AND workspace=?
                AND (storage_path IS NULL OR storage_path=?) LIMIT 2""",
                (engine, identity, workspace, str(storage))).fetchall()
        return dict(rows[0]) if len(rows) == 1 else None

    def touch_seen(self, uri, seen_run):
        with self.connect() as db:
            db.execute("UPDATE sessions SET seen_run=? WHERE uri=?", (seen_run, uri))

    def _cursor(self, value):
        raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
        signed = hmac.new(self.secret, raw, hashlib.sha256).digest() + raw
        return base64.urlsafe_b64encode(signed).decode().rstrip("=")

    def _decode(self, cursor):
        try:
            if not isinstance(cursor, str) or len(cursor) > 4096:
                raise ValueError()
            raw = base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4))
            if not hmac.compare_digest(raw[:32], hmac.new(self.secret, raw[32:], hashlib.sha256).digest()):
                raise ValueError()
            value = json.loads(raw[32:])
            if value["expires"] < time.time():
                raise ValueError()
            return value
        except (ValueError, KeyError, TypeError):
            raise InvalidCursor("Unknown, expired or altered cursor; fetch a fresh first page") from None

    def list(self, *, connectionId, limit=50, cursor=None, workingDirectory=None, search=None,
             parentUri=None, includeArchive=False, allowedWorkspaceRoots=None):
        checked_string(connectionId, "connectionId", 200)
        if type(limit) is not int or not 1 <= limit <= 200:
            raise ValueError("limit must be 1..200")
        if type(includeArchive) is not bool:
            raise ValueError("includeArchive must be boolean")
        if workingDirectory is not None:
            checked_string(workingDirectory, "workingDirectory")
        if parentUri is not None:
            checked_string(parentUri, "parentUri", 512)
        if search is not None:
            checked_string(search, "search", 500)
        roots = allowedWorkspaceRoots
        if roots is not None:
            if not isinstance(roots, list) or len(roots) > 128:
                raise ValueError("allowedWorkspaceRoots must be a bounded list")
            roots = sorted({checked_string(r, "workspace root").rstrip("/") or "/" for r in roots})
        query = {"connectionId": connectionId, "workspace": workingDirectory, "search": search,
                 "parent": parentUri, "archive": includeArchive, "roots": roots}
        scope = hashlib.sha256(json.dumps(query, sort_keys=True).encode()).hexdigest()
        where, args = [], []
        if not includeArchive:
            where.append("s.visible=1")
        if parentUri:
            where.append("s.parent_uri=? AND s.kind='child'"); args.append(parentUri)
        else:
            where.append("s.kind='root'")
        if workingDirectory:
            where.append("s.workspace=?"); args.append(workingDirectory)
        if roots is not None:
            alternatives = []
            for path in roots:
                prefix = path.rstrip("/") + "/"
                alternatives.append("(s.workspace=? OR substr(s.workspace,1,?)=?)")
                args.extend((path, len(prefix), prefix))
            where.append("(" + " OR ".join(alternatives) + ")" if alternatives else "0")
        if search:
            where.append("(instr(lower(s.title),lower(?))>0 OR instr(lower(s.native_id),lower(?))>0)")
            args.extend((search, search))
        if cursor:
            data = self._decode(cursor)
            if data.get("scope") != scope:
                raise InvalidCursor("Cursor belongs to another connection or query")
            where.append("(s.modified_at < ? OR (s.modified_at=? AND s.uri>?))")
            args.extend((data["modified"], data["modified"], data["uri"]))
        sql = "SELECT s.*,w.availability AS workspace_availability FROM sessions s JOIN workspaces w ON s.workspace=w.path WHERE " + " AND ".join(where)
        sql += " ORDER BY s.modified_at DESC,s.uri LIMIT ?"
        args.append(limit + 1)
        with self.connect() as db:
            rows = db.execute(sql, args).fetchall()
        result = {"items": [self._record(row) for row in rows[:limit]], "freshness": self.progress()}
        if len(rows) > limit:
            last = rows[limit - 1]
            result["nextCursor"] = self._cursor({"scope": scope, "modified": last["modified_at"],
                                                "uri": last["uri"], "expires": time.time() + 3600})
        return result

    def progress(self, value=None):
        with self.connect() as db:
            if value is not None:
                db.execute("INSERT OR REPLACE INTO progress VALUES('discovery',?)", (json.dumps(value),))
            row = db.execute("SELECT value FROM progress WHERE key='discovery'").fetchone()
        return json.loads(row[0]) if row else {"phase": "not-started"}

    def recheck_workspaces(self, *, limit=64):
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("limit must be 1..1000")
        with self.connect() as db:
            rows = db.execute("SELECT path FROM workspaces ORDER BY checked_at,path LIMIT ?", (limit,)).fetchall()
        return [self.workspace(row[0]) for row in rows]
