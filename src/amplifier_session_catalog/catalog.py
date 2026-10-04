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
import uuid
import re


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


def managed_location(value):
    """Bounded derived metadata only; this never proves allocation ownership."""
    keys={'version','kind','allocationId','creationReceipt','allocationHash','owner'}
    if not isinstance(value,dict) or set(value)!=keys or type(value['version']) is not int or value['version']!=1 or value['kind']!='managed' or value['owner']!='amplifier-unified':
        raise ValueError('Invalid managed location projection')
    identity=checked_string(value['allocationId'],'allocation ID',36)
    if str(uuid.UUID(identity))!=identity or not re.fullmatch('[a-f0-9]{64}',value['allocationHash'] if isinstance(value['allocationHash'],str) else ''):
        raise ValueError('Invalid managed allocation identity')
    receipt=checked_string(value['creationReceipt'],'creation receipt',200)
    if any(ord(c)<32 for c in receipt):raise ValueError('Invalid managed creation receipt')
    return dict(value)


def managed_root(workspace,location):
    path=Path(workspace)
    if not path.is_absolute() or os.path.normpath(workspace)!=workspace or len(path.parts)<5 or path.parts[-3:]!=('chats',location['allocationId'],'files'):
        raise ValueError('Managed location differs from its exact allocation directory')
    return str(path.parents[2])


def managed_roots(values):
    if values is None:
        return []
    if not isinstance(values,list) or len(values)>128:
        raise ValueError('allowedManagedRoots must be a bounded list')
    for value in values:
        checked_string(value,'managed root')
        if not Path(value).is_absolute() or os.path.normpath(value)!=value:
            raise ValueError('Managed root must be a normalized absolute path')
    return sorted(set(values))


def authorized_scope(roots,managed):
    """Exact indexed managed grants are independent from workspace grants."""
    clauses,args=[],[]
    for path in roots:
        prefix=path.rstrip('/')+'/'
        clauses.append('(s.workspace=? OR substr(s.workspace,1,?)=?)')
        args.extend((path,len(prefix),prefix))
    if managed:
        clauses.append('EXISTS(SELECT 1 FROM managed_locations ml WHERE ml.uri=s.uri AND ml.workspace=s.workspace AND ml.root IN ('+','.join('?' for _ in managed)+'))')
        args.extend(managed)
    return '('+' OR '.join(clauses)+')' if clauses else '0',args


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
                CREATE TABLE IF NOT EXISTS workspace_index (
                    path TEXT PRIMARY KEY,id TEXT NOT NULL UNIQUE,name TEXT NOT NULL,sort_name TEXT NOT NULL,
                    availability TEXT NOT NULL,registered INTEGER NOT NULL DEFAULT 0,hidden INTEGER NOT NULL DEFAULT 0,
                    revision INTEGER NOT NULL DEFAULT 0,source TEXT
                );
                CREATE INDEX IF NOT EXISTS available_workspace_order ON workspace_index(availability,hidden,sort_name,path);
                CREATE TABLE IF NOT EXISTS workspace_projection (source TEXT PRIMARY KEY,revision INTEGER NOT NULL);
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
                CREATE TABLE IF NOT EXISTS native_parent_links(uri TEXT PRIMARY KEY,engine TEXT NOT NULL,parent_native_id TEXT NOT NULL,workspace TEXT NOT NULL,parent_storage TEXT NOT NULL,fallback_uri TEXT NOT NULL,state TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS native_parent_identity ON native_parent_links(engine,parent_native_id,workspace);
                CREATE TABLE IF NOT EXISTS native_claims(command_id TEXT PRIMARY KEY,signature TEXT NOT NULL,engine TEXT NOT NULL,native_id TEXT NOT NULL,workspace TEXT NOT NULL,uri TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS native_claim_identity ON native_claims(engine,native_id,workspace);
                CREATE INDEX IF NOT EXISTS source_sessions ON sessions(source, seen_run);
                CREATE TABLE IF NOT EXISTS session_archive (uri TEXT PRIMARY KEY, archived INTEGER NOT NULL CHECK(archived IN (0,1)));
                CREATE INDEX IF NOT EXISTS archived_sessions ON session_archive(archived,uri);
                CREATE TABLE IF NOT EXISTS managed_locations(uri TEXT PRIMARY KEY,workspace TEXT NOT NULL,root TEXT NOT NULL,location TEXT NOT NULL,source TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS managed_workspace ON managed_locations(workspace,uri);
                CREATE INDEX IF NOT EXISTS managed_roots ON managed_locations(root,uri);
                CREATE TABLE IF NOT EXISTS native_visibility (uri TEXT PRIMARY KEY, deleted INTEGER NOT NULL, command_id TEXT NOT NULL, evidence TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS product_visibility (
                    uri TEXT PRIMARY KEY, source TEXT NOT NULL, hidden INTEGER NOT NULL CHECK(hidden IN (0,1)), revision INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS product_visibility_projection (source TEXT PRIMARY KEY, revision INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS product_visibility_commands (
                    source TEXT NOT NULL, command_id TEXT NOT NULL, signature TEXT NOT NULL, result TEXT NOT NULL,
                    PRIMARY KEY(source,command_id)
                );
                DROP TRIGGER IF EXISTS workspace_visibility;
                CREATE TRIGGER workspace_visibility AFTER UPDATE OF availability ON workspaces
                    WHEN OLD.availability <> NEW.availability BEGIN
                    UPDATE workspace_index SET availability=NEW.availability WHERE path=NEW.path;
                    UPDATE sessions SET visible=(NEW.availability='present' AND (EXISTS(SELECT 1 FROM managed_locations ml WHERE ml.uri=sessions.uri) OR NOT EXISTS(SELECT 1 FROM workspace_index wi WHERE wi.path=NEW.path AND wi.hidden=1)) AND NOT EXISTS(SELECT 1 FROM native_visibility v WHERE v.uri=sessions.uri AND v.deleted=1) AND NOT EXISTS(SELECT 1 FROM product_visibility pv WHERE pv.uri=sessions.uri AND pv.hidden=1)) WHERE workspace=NEW.path;
                END;
                CREATE TABLE IF NOT EXISTS progress (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                PRAGMA user_version=1;
            """)
        # Repair only derived workspace identity metadata, in bounded migration batches.
        while True:
            with self.connect() as db:
                rows=db.execute("SELECT path,availability FROM workspaces WHERE NOT EXISTS(SELECT 1 FROM workspace_index wi WHERE wi.path=workspaces.path) LIMIT 100").fetchall()
                for row in rows:
                    self._workspace_index(db,row['path'],row['availability'])
            if len(rows)<100:break
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

    @staticmethod
    def workspace_id(path):
        return "workspace:"+hashlib.sha256(path.encode("utf-8")).hexdigest()

    @classmethod
    def _workspace_index(cls,db,path,availability):
        name=Path(path).name or path
        db.execute("INSERT INTO workspace_index(path,id,name,sort_name,availability) VALUES(?,?,?,?,?) ON CONFLICT(path) DO UPDATE SET availability=excluded.availability",(path,cls.workspace_id(path),name,name.casefold(),availability))

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
            self._workspace_index(db,path,availability)
        return {"path": path, "availability": availability}

    @staticmethod
    def _resolve_native_parents(db, engine, native, workspace):
        # Discovery order and host/import mapping races must not determine family
        # membership. Resolve only this indexed native identity, never all rows.
        after=''
        while True:
            links=db.execute('SELECT * FROM native_parent_links WHERE engine=? AND parent_native_id=? AND workspace=? AND uri>? ORDER BY uri LIMIT 100',(engine,native,workspace,after)).fetchall()
            for link in links:
                rows=db.execute('SELECT uri FROM sessions WHERE engine=? AND native_id=? AND workspace=? AND (storage_path=? OR storage_path IS NULL) LIMIT 2',(engine,native,workspace,link['parent_storage'])).fetchall()
                state='resolved' if len(rows)==1 else 'ambiguous' if rows else 'missing'
                parent=rows[0]['uri'] if len(rows)==1 else link['fallback_uri']
                db.execute('UPDATE sessions SET parent_uri=? WHERE uri=?',(parent,link['uri']))
                db.execute('UPDATE native_parent_links SET state=? WHERE uri=?',(state,link['uri']))
            if len(links)<100:break
            after=links[-1]['uri']

    def upsert(self, record, *, source="host", fingerprint=None, seen_run=None, confirmed_visibility=None, native_parent=None):
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
        location=managed_location(record['location']) if 'location' in record else None
        allocation_root=managed_root(workspace,location) if location else None
        raw = json.dumps(details, ensure_ascii=False)
        if len(raw.encode()) > 32768:
            raise ValueError("Catalog details exceed 32 KiB")
        archive = record.get('isArchived')
        if 'isArchived' in record and type(archive) is not bool:raise ValueError('isArchived must be boolean')
        storage = checked_string(record.get("storagePath"), "storagePath", optional=True)
        if native_parent is not None:
            if source=='host' or not isinstance(native_parent,dict) or set(native_parent)!={'nativeId','storagePath'} or not parent:
                raise ValueError('Discovery-owned exact native parent identity required')
            checked_string(native_parent['nativeId'],'native parent identity',512)
            checked_string(native_parent['storagePath'],'native parent storage')
            if native_parent['nativeId'] in {'.','..'} or any(char in native_parent['nativeId'] for char in '/\\'):
                raise ValueError('Native parent must be one exact storage identity')
            if not storage or Path(storage).parent/native_parent['nativeId']!=Path(native_parent['storagePath']):
                raise ValueError('Native parent storage differs from selected project')
        with self.connect() as db:
            known = db.execute("SELECT path FROM workspaces WHERE path=?", (workspace,)).fetchone()
        if known is None:
            self.workspace(workspace)
        if confirmed_visibility is not None:
            if not isinstance(confirmed_visibility, dict) or type(confirmed_visibility.get("deleted")) is not bool or confirmed_visibility.get("evidence") != "configured-native-marker":
                raise ValueError("Verified discovery marker required for atomic visibility")
            checked_string(confirmed_visibility.get("commandId"), "commandId", 200)
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            prior_managed=db.execute('SELECT s.engine,s.native_id,ml.workspace FROM managed_locations ml JOIN sessions s ON s.uri=ml.uri WHERE ml.uri=?',(uri,)).fetchone()
            if prior_managed and (prior_managed['engine'],prior_managed['native_id'],prior_managed['workspace'])!=(engine,native,workspace):
                raise ValueError('Managed identity projection cannot be reassigned')
            reserved=db.execute('SELECT engine,native_id,workspace FROM native_claims WHERE uri=? LIMIT 1',(uri,)).fetchone()
            if reserved and (reserved['engine'],reserved['native_id'],reserved['workspace'])!=(engine,native,workspace):raise ValueError('Claimed AHP identity cannot be overwritten')
            if source=='host':
                claimed=db.execute('SELECT DISTINCT uri FROM native_claims WHERE engine=? AND native_id=? AND workspace=? LIMIT 2',(engine,native,workspace)).fetchall()
                if claimed and (len(claimed)!=1 or claimed[0]['uri']!=uri):raise ValueError('Claimed native identity cannot have another AHP mapping')
            if source != 'host':
                # Discovery's earlier by_native lookup is only a hint. Recheck
                # under the writer transaction so a simultaneous host claim
                # cannot produce a second catalog row for the same source.
                matches = db.execute('SELECT * FROM sessions WHERE engine=? AND native_id=? AND workspace=? AND (storage_path IS NULL OR storage_path=?) LIMIT 2', (engine,native,workspace,storage)).fetchall()
                claims = db.execute('SELECT DISTINCT uri FROM native_claims WHERE engine=? AND native_id=? AND workspace=? LIMIT 2', (engine,native,workspace)).fetchall()
                if len(matches)>1 or len(claims)>1: raise ValueError('Native discovery identity is ambiguous')
                if claims and (not matches or matches[0]['uri'] != claims[0]['uri']): raise ValueError('Native discovery differs from the claimed source identity')
                if matches:
                    known_row=matches[0];uri=known_row['uri']
                    if claims:
                        title=known_row['title'];created=known_row['created_at']
            managed=db.execute('SELECT * FROM managed_locations WHERE uri=?',(uri,)).fetchone()
            if managed:
                if managed['workspace']!=workspace:
                    raise ValueError('Managed identity projection cannot be reassigned')
                previous_location=json.loads(managed['location'])
                if location and location!=previous_location:
                    if source=='host' or managed['source']!='host':raise ValueError('Managed allocation projection is immutable')
                    # A stale scanner cannot erase or replace a host-verified projection.
                    location=previous_location
                else:location=previous_location
                allocation_root=managed['root']
            if location:
                db.execute('INSERT INTO managed_locations VALUES(?,?,?,?,?) ON CONFLICT(uri) DO UPDATE SET source=CASE WHEN excluded.source=\'host\' THEN \'host\' ELSE managed_locations.source END',(uri,workspace,allocation_root,json.dumps(location),source))
                details['location']=location
            raw=json.dumps(details,ensure_ascii=False)
            if len(raw.encode())>32768:raise ValueError('Catalog details exceed 32 KiB')
            if archive is not None:
                db.execute("INSERT INTO session_archive VALUES(?,?) ON CONFLICT(uri) DO UPDATE SET archived=excluded.archived",(uri,int(archive)))
            if confirmed_visibility is not None:
                db.execute("INSERT INTO native_visibility VALUES(?,?,?,?) ON CONFLICT(uri) DO UPDATE SET deleted=excluded.deleted,command_id=excluded.command_id,evidence=excluded.evidence", (uri, int(confirmed_visibility["deleted"]), confirmed_visibility["commandId"], confirmed_visibility["evidence"]))
            visible = db.execute("SELECT availability='present' FROM workspaces WHERE path=?", (workspace,)).fetchone()[0]
            if db.execute("SELECT 1 FROM native_visibility WHERE uri=? AND deleted=1", (uri,)).fetchone() or db.execute("SELECT 1 FROM product_visibility WHERE uri=? AND hidden=1", (uri,)).fetchone() or not location and db.execute("SELECT 1 FROM workspace_index WHERE path=? AND hidden=1",(workspace,)).fetchone(): visible = 0
            db.execute("""INSERT INTO sessions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(uri) DO UPDATE SET engine=excluded.engine,native_id=excluded.native_id,
                workspace=excluded.workspace,parent_uri=excluded.parent_uri,kind=excluded.kind,
                title=excluded.title,created_at=excluded.created_at,modified_at=excluded.modified_at,
                availability=excluded.availability,source=excluded.source,storage_path=excluded.storage_path,
                native_project=excluded.native_project,fingerprint=excluded.fingerprint,
                seen_run=excluded.seen_run,indexed_at=excluded.indexed_at,details=excluded.details,visible=excluded.visible""",
                (uri, engine, native, workspace, parent, kind, title, created, modified, availability,
                 source, storage, record.get("nativeProject"), fingerprint, seen_run, now, raw, visible))
            if native_parent:
                db.execute('INSERT INTO native_parent_links VALUES(?,?,?,?,?,?,?) ON CONFLICT(uri) DO UPDATE SET engine=excluded.engine,parent_native_id=excluded.parent_native_id,workspace=excluded.workspace,parent_storage=excluded.parent_storage,fallback_uri=excluded.fallback_uri,state=excluded.state',
                           (uri,engine,native_parent['nativeId'],workspace,native_parent['storagePath'],parent,'missing'))
                self._resolve_native_parents(db,engine,native_parent['nativeId'],workspace)
            elif source!='host':
                db.execute('DELETE FROM native_parent_links WHERE uri=?',(uri,))
            self._resolve_native_parents(db,engine,native,workspace)
        return self.get(uri)

    def claim_native(self, *, record, commandId, evidence):
        """Atomic derived registration after the host verifies native authority.

        The claim is repairable metadata, never authority to create native files.
        Reuse one existing identity without overwriting title/archive/user edits.
        """
        if evidence != 'verified-native-import' or not isinstance(record,dict): raise ValueError('Verified native import claim required')
        command=checked_string(commandId,'commandId',200)
        uri=checked_string(record.get('uri'),'uri',512)
        if not uri.startswith('ahp-session:/'): raise ValueError('AHP URI required')
        engine=checked_string(record.get('engineId'),'engineId',128);native=checked_string(record.get('nativeSessionId'),'nativeSessionId',512)
        workspace=checked_string(record.get('workingDirectory'),'workingDirectory')
        if not Path(workspace).is_absolute() or os.path.normpath(workspace)!=workspace: raise ValueError('Canonical absolute workspace required')
        title=checked_string(record.get('title'),'title',8192)
        signature=json.dumps([uri,engine,native,workspace],separators=(',',':'))
        with self.connect() as db: known=db.execute('SELECT 1 FROM workspaces WHERE path=?',(workspace,)).fetchone()
        if not known:self.workspace(workspace)
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            prior=db.execute('SELECT * FROM native_claims WHERE command_id=?',(command,)).fetchone()
            if prior:
                if prior['signature']!=signature:raise ValueError('Claim command identity changed')
                result_uri=prior['uri']
                selected=db.execute('SELECT * FROM sessions WHERE uri=?',(result_uri,)).fetchone()
                if not selected or (selected['engine'],selected['native_id'],selected['workspace'])!=(engine,native,workspace):raise ValueError('Claimed native mapping is unavailable or changed')
            else:
                target=db.execute('SELECT * FROM sessions WHERE uri=?',(uri,)).fetchone()
                if target and (target['engine'],target['native_id'],target['workspace'])!=(engine,native,workspace):raise ValueError('Requested AHP URI belongs to another native identity')
                rows=db.execute('SELECT * FROM sessions WHERE engine=? AND native_id=? AND workspace=? LIMIT 2',(engine,native,workspace)).fetchall()
                if len(rows)>1:raise ValueError('Native claim identity is ambiguous')
                result_uri=rows[0]['uri'] if rows else uri
                if not rows:
                    now=time.time();modified=timestamp(record.get('modifiedAt'),now);created=timestamp(record.get('createdAt'),modified)
                    visible=db.execute("SELECT availability='present' AND NOT EXISTS(SELECT 1 FROM workspace_index WHERE path=? AND hidden=1) FROM workspaces WHERE path=?",(workspace,workspace)).fetchone()[0]
                    db.execute('INSERT INTO sessions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(uri,engine,native,workspace,None,'root',title,created,modified,'available','host',None,None,None,None,now,'{}',visible))
                db.execute('INSERT INTO native_claims VALUES(?,?,?,?,?,?)',(command,signature,engine,native,workspace,result_uri))
            self._resolve_native_parents(db,engine,native,workspace)
        return self.get(result_uri)

    def project_session_archives(self, *, items):
        """Derived flags from the trusted host; no history reads or native metadata writes."""
        if not isinstance(items,list) or len(items)>100:raise ValueError('Archive projection requires at most100 items')
        values=[]
        for item in items:
            if not isinstance(item,dict) or set(item)!={'uri','isArchived'} or type(item['isArchived']) is not bool:raise ValueError('Exact archive projection item required')
            values.append((checked_string(item['uri'],'uri',512),int(item['isArchived'])))
        with self.connect() as db:db.executemany("INSERT INTO session_archive VALUES(?,?) ON CONFLICT(uri) DO UPDATE SET archived=excluded.archived",values)
        return {'projected':len(values)}

    def session_visibility_projection_status(self, *, source):
        checked_string(source, 'source', 200)
        with self.connect() as db:
            row = db.execute('SELECT revision FROM product_visibility_projection WHERE source=?', (source,)).fetchone()
        return {'source': source, 'revision': row['revision'] if row else 0}

    def session_visibility_projection_receipt(self, *, source, commandId):
        checked_string(source, 'source', 200)
        checked_string(commandId, 'commandId', 200)
        with self.connect() as db:
            row = db.execute('SELECT result FROM product_visibility_commands WHERE source=? AND command_id=?',
                             (source, commandId)).fetchone()
        return json.loads(row['result']) if row else None

    def project_session_visibility(self, *, source, commandId, afterRevision, throughRevision, items):
        """Rebuildable product projection; the host owns reset authority and undo."""
        checked_string(source, 'source', 200)
        checked_string(commandId, 'commandId', 200)
        if (type(afterRevision) is not int or type(throughRevision) is not int
                or not 0 <= afterRevision < throughRevision <= 9223372036854775807):
            raise ValueError('Strict increasing projection revisions required')
        if not isinstance(items, list) or len(items) > 100:
            raise ValueError('Visibility projection requires at most 100 items')
        values = []
        seen = set()
        for item in items:
            if not isinstance(item, dict) or set(item) != {'uri', 'hidden', 'revision'}:
                raise ValueError('Exact visibility projection item required')
            uri = checked_string(item['uri'], 'uri', 512)
            if uri in seen or type(item['hidden']) is not bool:
                raise ValueError('Unique URI and boolean hidden flag required')
            if type(item['revision']) is not int or not afterRevision < item['revision'] <= throughRevision:
                raise ValueError('Visibility item outside batch revision')
            seen.add(uri)
            values.append((uri, source, int(item['hidden']), item['revision']))
        signature = hashlib.sha256(json.dumps([afterRevision, throughRevision, items],
                                   sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        result = {'source': source, 'commandId': commandId, 'revision': throughRevision, 'projected': len(values)}
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            prior = db.execute('SELECT signature,result FROM product_visibility_commands WHERE source=? AND command_id=?',
                               (source, commandId)).fetchone()
            if prior:
                if prior['signature'] != signature:
                    raise ValueError('Visibility command content changed')
                return json.loads(prior['result'])
            row = db.execute('SELECT revision FROM product_visibility_projection WHERE source=?', (source,)).fetchone()
            if (row['revision'] if row else 0) != afterRevision:
                raise InvalidCursor('Visibility projection changed; inspect its checkpoint and exact receipt')
            for uri, owner, hidden, revision in values:
                if not db.execute('SELECT 1 FROM sessions WHERE uri=?', (uri,)).fetchone():
                    raise ValueError('Visibility identity must already be indexed')
                prior_owner = db.execute('SELECT source FROM product_visibility WHERE uri=?', (uri,)).fetchone()
                if prior_owner and prior_owner['source'] != owner:
                    raise ValueError('Visibility projection belongs to another source')
                db.execute('INSERT INTO product_visibility VALUES(?,?,?,?) ON CONFLICT(uri) DO UPDATE SET hidden=excluded.hidden,revision=excluded.revision',
                           (uri, owner, hidden, revision))
                db.execute("""UPDATE sessions SET visible=(?=0
                    AND EXISTS(SELECT 1 FROM workspaces WHERE path=workspace AND availability='present')
                    AND (EXISTS(SELECT 1 FROM managed_locations ml WHERE ml.uri=sessions.uri)
                        OR NOT EXISTS(SELECT 1 FROM workspace_index wi WHERE wi.path=workspace AND wi.hidden=1))
                    AND NOT EXISTS(SELECT 1 FROM native_visibility v WHERE v.uri=sessions.uri AND v.deleted=1))
                    WHERE uri=?""", (hidden, uri))
            db.execute('INSERT INTO product_visibility_projection VALUES(?,?) ON CONFLICT(source) DO UPDATE SET revision=excluded.revision',
                       (source, throughRevision))
            db.execute('INSERT INTO product_visibility_commands VALUES(?,?,?,?)',
                       (source, commandId, signature, json.dumps(result)))
        return result

    def set_visibility(self, *, uri, nativeSessionId, workingDirectory, deleted, commandId, evidence):
        """Derived visibility only, after exact storage-owner confirmation. No file mutation."""
        current = self.get(checked_string(uri, "uri", 512))
        if not current or current["nativeSessionId"] != nativeSessionId or current["workingDirectory"] != workingDirectory:
            raise ValueError("Native visibility identity does not match indexed session")
        if type(deleted) is not bool:
            raise ValueError("Native deletion state must be boolean")
        checked_string(commandId, "commandId", 200)
        if evidence not in {"native-completed-receipt", "configured-native-marker"}:
            raise ValueError("Confirmed native visibility evidence required")
        with self.connect() as db:
            db.execute("INSERT INTO native_visibility VALUES(?,?,?,?) ON CONFLICT(uri) DO UPDATE SET deleted=excluded.deleted,command_id=excluded.command_id,evidence=excluded.evidence", (uri, int(deleted), commandId, evidence))
            db.execute("UPDATE sessions SET visible=(?=0 AND NOT EXISTS(SELECT 1 FROM product_visibility pv WHERE pv.uri=sessions.uri AND pv.hidden=1) AND EXISTS(SELECT 1 FROM workspaces WHERE path=workspace AND availability='present') AND (EXISTS(SELECT 1 FROM managed_locations ml WHERE ml.uri=sessions.uri) OR NOT EXISTS(SELECT 1 FROM workspace_index wi WHERE wi.path=workspace AND wi.hidden=1))) WHERE uri=?", (int(deleted), uri))
        return {"uri": uri, "deleted": deleted, "commandId": commandId}

    @staticmethod
    def _workspace_record(row):
        return {"id":row["id"],"path":row["path"],"name":row["name"],"available":row["availability"]=="present",
                "availability":row["availability"],"registered":bool(row["registered"]),"hidden":bool(row["hidden"]),
                "revision":row["revision"],"source":"registered" if row["source"] else "discovered","checkedAt":iso(row["checked_at"])}

    def get_workspace(self, *, id):
        checked_string(id,"workspace ID",128)
        with self.connect() as db:
            row=db.execute("SELECT wi.*,w.checked_at FROM workspace_index wi JOIN workspaces w ON w.path=wi.path WHERE wi.id=? AND NOT EXISTS(SELECT 1 FROM managed_locations ml WHERE ml.workspace=wi.path)",(id,)).fetchone()
        return self._workspace_record(row) if row else None

    def list_workspaces(self, *, connectionId, limit=50, cursor=None, search=None, includeHidden=False, includeUnavailable=False, allowedWorkspaceRoots=None):
        checked_string(connectionId,"connectionId",200)
        if type(limit) is not int or not 1<=limit<=200:raise ValueError("limit must be1..200")
        if type(includeHidden) is not bool or type(includeUnavailable) is not bool:raise ValueError("Workspace visibility flags must be boolean")
        if search is not None:checked_string(search,"search",500)
        roots=allowedWorkspaceRoots
        if roots is not None:
            if not isinstance(roots,list) or len(roots)>128:raise ValueError("Bounded authorized workspace roots required")
            roots=sorted({checked_string(root,"root").rstrip('/') or '/' for root in roots})
        scope=hashlib.sha256(json.dumps(["workspaces",connectionId,search,includeHidden,includeUnavailable,roots],sort_keys=True).encode()).hexdigest()
        where,args=["NOT EXISTS(SELECT 1 FROM managed_locations ml WHERE ml.workspace=wi.path)"],[]
        if not includeHidden:where.append("wi.hidden=0")
        if not includeUnavailable:where.append("wi.availability='present'")
        if search:
            where.append("(instr(lower(wi.name),lower(?))>0 OR instr(lower(wi.path),lower(?))>0)");args.extend([search,search])
        if roots is not None:
            parts=[]
            for root in roots:
                prefix=root.rstrip('/')+'/'
                parts.append("(wi.path=? OR substr(wi.path,1,?)=?)");args.extend([root,len(prefix),prefix])
            where.append('('+' OR '.join(parts)+')' if parts else '0')
        if cursor:
            decoded=self._decode(cursor)
            if decoded.get('scope')!=scope:raise InvalidCursor("Workspace cursor belongs to another connection or query")
            where.append("(wi.sort_name,wi.path)>(?,?)");args.extend([decoded['name'],decoded['path']])
        with self.connect() as db:
            rows=db.execute("SELECT wi.*,w.checked_at FROM workspace_index wi JOIN workspaces w ON w.path=wi.path WHERE "+(' AND '.join(where) or '1')+" ORDER BY wi.sort_name,wi.path LIMIT ?",[*args,limit+1]).fetchall()
        result={"items":[self._workspace_record(row) for row in rows[:limit]],"freshness":self.progress()}
        if len(rows)>limit:
            row=rows[limit-1];result['nextCursor']=self._cursor({'scope':scope,'name':row['sort_name'],'path':row['path'],'expires':time.time()+3600})
        return result

    def workspace_projection_status(self, *, source):
        checked_string(source,"workspace owner",200)
        with self.connect() as db:row=db.execute("SELECT revision FROM workspace_projection WHERE source=?",(source,)).fetchone()
        return {"source":source,"revision":row[0] if row else 0}

    def project_workspaces(self, *, source, afterRevision, throughRevision, records):
        """Trusted owner's bounded derived projection, never canonical workspace storage."""
        checked_string(source,"workspace owner",200)
        if type(afterRevision) is not int or type(throughRevision) is not int or not 0<=afterRevision<=throughRevision:raise ValueError("Monotonic projection revisions required")
        if not isinstance(records,list) or len(records)>100:raise ValueError("Workspace projection batch exceeds100")
        values=[]
        for row in records:
            if not isinstance(row,dict):raise ValueError("Workspace projection record required")
            path=checked_string(row.get('path'),'path');name=checked_string(row.get('name'),'name',200);revision=row.get('revision')
            if not Path(path).is_absolute() or os.path.normpath(path)!=path or row.get('id')!=self.workspace_id(path):raise ValueError("Canonical workspace identity required")
            if type(revision) is not int or not afterRevision<revision<=throughRevision:raise ValueError("Projection record outside batch revision")
            if type(row.get('registered')) is not bool or type(row.get('hidden')) is not bool:raise ValueError("Workspace projection flags required")
            values.append((row,path,name,revision))
        with self.connect() as db:
            old=db.execute("SELECT revision FROM workspace_projection WHERE source=?",(source,)).fetchone();current=old[0] if old else 0
            if current!=afterRevision:raise InvalidCursor("Workspace projection changed; inspect its checkpoint")
            for row,path,name,revision in values:
                if db.execute('SELECT 1 FROM managed_locations WHERE workspace=? LIMIT 1',(path,)).fetchone():raise ValueError('Managed allocation is not a project workspace')
                known=db.execute("SELECT source,hidden FROM workspace_index WHERE path=?",(path,)).fetchone()
                if known and known['source'] not in (None,source):raise ValueError("Workspace projection belongs to a different configured owner")
                # Existence is refreshed only for changed registrations, not ordinary queries.
                availability=directory_status(path)
                db.execute("INSERT INTO workspaces VALUES(?,?,?) ON CONFLICT(path) DO UPDATE SET availability=excluded.availability,checked_at=excluded.checked_at",(path,availability,time.time()))
                db.execute("INSERT INTO workspace_index VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(path) DO UPDATE SET name=excluded.name,sort_name=excluded.sort_name,availability=excluded.availability,registered=excluded.registered,hidden=excluded.hidden,revision=excluded.revision,source=excluded.source",(path,row['id'],name,name.casefold(),availability,int(row['registered']),int(row['hidden']),revision,source))
                if bool(known['hidden'] if known else False)!=row['hidden']:
                    db.execute("UPDATE sessions SET visible=(?='present' AND ?=0 AND NOT EXISTS(SELECT 1 FROM native_visibility v WHERE v.uri=sessions.uri AND v.deleted=1) AND NOT EXISTS(SELECT 1 FROM product_visibility pv WHERE pv.uri=sessions.uri AND pv.hidden=1)) WHERE workspace=?",(availability,int(row['hidden']),path))
            db.execute("INSERT INTO workspace_projection VALUES(?,?) ON CONFLICT(source) DO UPDATE SET revision=excluded.revision",(source,throughRevision))
        return {"source":source,"revision":throughRevision,"projected":len(records)}

    @staticmethod
    def _record(row):
        if row is None:
            return None
        result = {"uri": row["uri"], "engineId": row["engine"], "nativeSessionId": row["native_id"],
                  "workingDirectory": row["workspace"], "title": row["title"], "kind": row["kind"],
                  "createdAt": iso(row["created_at"]), "modifiedAt": iso(row["modified_at"]),
                  "availability": row["availability"], "workspaceAvailability": row["workspace_availability"],
                  "indexedAt": iso(row["indexed_at"]), "status": "idle", "isArchived": bool(row["is_archived"]) if "is_archived" in row.keys() else False, "nativeDeleted": bool(row["native_deleted"]) if "native_deleted" in row.keys() else False}
        result["productHidden"] = bool(row["product_hidden"]) if "product_hidden" in row.keys() else False
        if row["parent_uri"]:
            result["parentUri"] = row["parent_uri"]
        if row["storage_path"]:
            result["storagePath"] = row["storage_path"]
        result.update(json.loads(row["details"]))
        return result

    def get(self, uri):
        checked_string(uri, "uri", 512)
        with self.connect() as db:
            row = db.execute("SELECT s.*, COALESCE((SELECT archived FROM session_archive a WHERE a.uri=s.uri),0) AS is_archived, w.availability AS workspace_availability, EXISTS(SELECT 1 FROM native_visibility v WHERE v.uri=s.uri AND v.deleted=1) AS native_deleted, EXISTS(SELECT 1 FROM product_visibility pv WHERE pv.uri=s.uri AND pv.hidden=1) AS product_hidden FROM sessions s JOIN workspaces w ON s.workspace=w.path WHERE uri=?", (uri,)).fetchone()
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

    def descendants(self, *, parentUri, limit=100):
        """Bounded indexed family facts; never a lease or live-execution claim."""
        checked_string(parentUri, 'parentUri', 512)
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError('limit must be 1..100')
        with self.connect() as db:
            db.execute('BEGIN')
            root=db.execute('SELECT * FROM sessions WHERE uri=?',(parentUri,)).fetchone()
            rows=db.execute('''WITH RECURSIVE family(uri,depth,path,cycle) AS (
                SELECT uri,1,json_array(?,uri),uri=? FROM sessions WHERE parent_uri=?
                UNION ALL
                SELECT s.uri,f.depth+1,json_insert(f.path,'$[#]',s.uri),
                    EXISTS(SELECT 1 FROM json_each(f.path) WHERE value=s.uri)
                FROM sessions s JOIN family f ON s.parent_uri=f.uri
                WHERE f.depth<64 AND f.cycle=0 LIMIT ?
            ) SELECT s.*,f.depth,f.cycle,
                EXISTS(SELECT 1 FROM sessions child WHERE child.parent_uri=s.uri) AS has_children
                FROM family f JOIN sessions s ON s.uri=f.uri''',
                (parentUri,parentUri,parentUri,limit+1)).fetchall()
            progress=db.execute("SELECT value FROM progress WHERE key='discovery'").fetchone()
            identities=[root['native_id']] if root else []
            identities.extend(row['native_id'] for row in rows[:limit])
            unresolved=bool(root and db.execute('SELECT 1 FROM native_parent_links WHERE engine=? AND workspace=? AND state!=\'resolved\' AND parent_native_id IN ('+','.join('?' for _ in identities)+') LIMIT 1',(root['engine'],root['workspace'],*identities)).fetchone())
        freshness=json.loads(progress[0]) if progress else {'phase':'not-started'}
        def facts(row):
            return {'uri':row['uri'],'engineId':row['engine'],'nativeSessionId':row['native_id'],
                    'workingDirectory':row['workspace'],'parentUri':row['parent_uri'],
                    'observedParentUri':row['parent_uri'],'kind':row['kind'],'source':row['source'],
                    'modifiedAt':iso(row['modified_at']),'indexedAt':iso(row['indexed_at']),
                    'relationship':'indexed-parent'}
        issues=[]
        if root is None:issues.append('root-not-indexed')
        if unresolved:issues.append('native-parent-unresolved')
        if len(rows)>limit:issues.append('result-limit')
        if any(row['cycle'] for row in rows):issues.append('cycle')
        if any(row['depth']==64 and row['has_children'] for row in rows):issues.append('depth-limit')
        result={'parentUri':parentUri,'rootKnown':root is not None,
                'root':facts(root) if root else None,'items':[facts(row) for row in rows[:limit]],
                'completeIndexedCoverage':not issues,'truncated':bool(set(issues)&{'result-limit','depth-limit'}),
                'issues':issues,'excludedKinds':[],'includesHidden':True,
                'coverage':'indexed-native-history','freshness':freshness,
                'externalWriterCoverage':False,'nativeFilesRead':False}
        result['discoveryComplete']=freshness.get('phase')=='complete' and freshness.get('issues')==0
        # This token binds the exact observed index facts and this catalog process.
        # A stable token is still not evidence that native writers are excluded.
        result['revision']=hmac.new(self.secret,json.dumps(result,sort_keys=True).encode(),hashlib.sha256).hexdigest()
        return result

    def resolve_native(self, *, engineId, identities, allowedWorkspaceRoots, allowedManagedRoots=None):
        """Bounded migration lookup; never guess an ambiguous native identity."""
        checked_string(engineId, "engineId", 200)
        if not isinstance(identities, list) or len(identities) > 500:
            raise ValueError("identities must contain at most 500 native IDs")
        identities = list(dict.fromkeys(checked_string(value, "native ID", 512) for value in identities))
        if not isinstance(allowedWorkspaceRoots, list) or len(allowedWorkspaceRoots) > 128:
            raise ValueError("allowedWorkspaceRoots must be bounded")
        roots = [checked_string(value, "workspace root").rstrip("/") or "/" for value in allowedWorkspaceRoots]
        where,scope=authorized_scope(roots,managed_roots(allowedManagedRoots))
        result = {}
        with self.connect() as db:
            for identity in identities:
                rows = db.execute("SELECT s.uri FROM sessions s WHERE s.engine=? AND s.native_id=? AND " + where + " LIMIT 2", (engineId, identity, *scope)).fetchall()
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
             parentUri=None, includeArchive=False, allowedWorkspaceRoots=None, allowedManagedRoots=None, archive=None,
             modifiedBefore=None, includeTotal=False):
        checked_string(connectionId, "connectionId", 200)
        if type(limit) is not int or not 1 <= limit <= 200:
            raise ValueError("limit must be 1..200")
        if type(includeArchive) is not bool:
            raise ValueError("includeArchive must be boolean")
        if type(includeTotal) is not bool:
            raise ValueError("includeTotal must be boolean")
        if modifiedBefore is not None and (type(modifiedBefore) not in (int, float)
                or not math.isfinite(modifiedBefore) or not 0 <= modifiedBefore <= 253402300799):
            raise ValueError("modifiedBefore must be a finite Unix timestamp")
        if archive is not None and archive not in {'active','all','archived'}:raise ValueError('archive must be active, all or archived')
        if archive is not None and includeArchive:raise ValueError('archive cannot widen missing or hidden workspace visibility')
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
        managed=managed_roots(allowedManagedRoots)
        if allowedManagedRoots is not None and roots is None:
            raise ValueError('Managed grants require an explicit workspace root list')
        query = {"connectionId": connectionId, "workspace": workingDirectory, "search": search,
                 "parent": parentUri, "archive": includeArchive, "archiveSelector":archive, "roots": roots,
                 "modifiedBefore":modifiedBefore,"managedRoots":managed}
        scope = hashlib.sha256(json.dumps(query, sort_keys=True).encode()).hexdigest()
        where, args = [], []
        if includeArchive:
            where.append("NOT EXISTS(SELECT 1 FROM native_visibility v WHERE v.uri=s.uri AND v.deleted=1) AND NOT EXISTS(SELECT 1 FROM product_visibility pv WHERE pv.uri=s.uri AND pv.hidden=1)")
        if not includeArchive:
            where.append("s.visible=1")
        if archive in {'active','archived'}:
            where.append("s.uri "+("IN" if archive=="archived" else "NOT IN")+" (SELECT uri FROM session_archive WHERE archived=1)")
        if parentUri:
            where.append("s.parent_uri=? AND s.kind='child'"); args.append(parentUri)
        else:
            where.append("s.kind='root'")
        if workingDirectory:
            where.append("s.workspace=?"); args.append(workingDirectory)
        if roots is not None:
            authorized,scope_args=authorized_scope(roots,managed)
            where.append(authorized);args.extend(scope_args)
        if search:
            where.append("(instr(lower(s.title),lower(?))>0 OR instr(lower(s.native_id),lower(?))>0)")
            args.extend((search, search))
        if modifiedBefore is not None:
            where.append("s.modified_at < ? AND s.created_at < ?")
            args.extend((modifiedBefore, modifiedBefore))
        count_where, count_args = list(where), list(args)
        if cursor:
            data = self._decode(cursor)
            if data.get("scope") != scope:
                raise InvalidCursor("Cursor belongs to another connection or query")
        sql = "SELECT s.*,COALESCE((SELECT archived FROM session_archive a WHERE a.uri=s.uri),0) AS is_archived,w.availability AS workspace_availability FROM sessions s JOIN workspaces w ON s.workspace=w.path WHERE " + " AND ".join(where)
        if cursor:
            # The mixed descending-time/ascending-URI key cannot be expressed
            # as one row-value comparison. Separate disjoint ranges let SQLite
            # seek both parts of the ordering index, including large time ties.
            sql = (sql + " AND s.modified_at=? AND s.uri>? UNION ALL " + sql
                   + " AND s.modified_at<? ORDER BY modified_at DESC,uri LIMIT ?")
            args = [*args, data['modified'], data['uri'], *args, data['modified'], limit+1]
        else:
            sql += " ORDER BY s.modified_at DESC,s.uri LIMIT ?"
            args.append(limit + 1)
        with self.connect() as db:
            # An optional explicit count uses the same read snapshot as this page.
            # Ordinary navigation never pays for counting the whole result set.
            db.execute('BEGIN')
            rows = db.execute(sql, args).fetchall()
            total = db.execute("SELECT count(*) FROM sessions s JOIN workspaces w ON s.workspace=w.path WHERE "
                               + " AND ".join(count_where), count_args).fetchone()[0] if includeTotal else None
        result = {"items": [self._record(row) for row in rows[:limit]], "freshness": self.progress()}
        if includeTotal:
            result['total'] = total
        if len(rows) > limit:
            last = rows[limit - 1]
            result["nextCursor"] = self._cursor({"scope": scope, "modified": last["modified_at"],
                                                "uri": last["uri"], "expires": time.time() + 3600})
        return result

    def retention_candidates(self, *, connectionId, modifiedBefore, allowedWorkspaceRoots,
                             limit=50, cursor=None, workingDirectory=None, allowedManagedRoots=None):
        """Cold candidate metadata only. Age is never permission to remove work."""
        if modifiedBefore is None:
            raise ValueError('An explicit finite cutoff is required')
        if not isinstance(allowedWorkspaceRoots, list):
            raise ValueError('An explicit authorized workspace root list is required')
        result = self.list(connectionId=connectionId, modifiedBefore=modifiedBefore,
                           allowedWorkspaceRoots=allowedWorkspaceRoots, allowedManagedRoots=allowedManagedRoots, limit=limit, cursor=cursor,
                           workingDirectory=workingDirectory, archive='all', includeTotal=True)
        total=result.pop('total')
        return {**result, 'cutoff':modifiedBefore, 'candidateCount':total,
                'eligibility':'unverified', 'scope':'visible-root-metadata',
                'requires':['current-selection', 'live-and-unknown-work', 'native-descendants',
                            'dependent-owner-records', 'exact-reviewed-source'],
                'nativeFilesRead':False, 'removalAuthorized':False}

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
