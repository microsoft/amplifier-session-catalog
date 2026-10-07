"""Offline projection for a verified, stopped installation restore.

The trusted operator owns native verification and activation. This module only
rebases exact bindings in the copied index; it is deliberately not an RPC.
"""
import hashlib
import json
import os
from pathlib import Path
import re

from .catalog import checked_string


def _home(value):
    checked_string(value, 'native home')
    if not Path(value).is_absolute() or os.path.normpath(value) != value:
        raise ValueError('Canonical absolute native home required')
    return Path(value)


def rebind_restored_sources(catalog, *, command_id, restore_digest, source_home,
                            destination_home, records):
    checked_string(command_id, 'restore projection command', 200)
    if not isinstance(restore_digest, str) or not re.fullmatch('[a-f0-9]{64}', restore_digest):
        raise ValueError('Verified restore receipt digest required')
    source, destination = _home(source_home), _home(destination_home)
    if source == destination or source in destination.parents or destination in source.parents:
        raise ValueError('Restore homes must be distinct and disjoint')
    if not isinstance(records, list) or not 1 <= len(records) <= 100:
        raise ValueError('Restore projection requires 1 to 100 snapshots')
    for record in records:
        if not isinstance(record, dict) or set(record) != {'uri', 'token'}:
            raise ValueError('Exact indexed source snapshot required')
        checked_string(record['uri'], 'session URI', 512)
        if not isinstance(record['token'], str) or not re.fullmatch('[a-f0-9]{64}', record['token']):
            raise ValueError('Exact source token required')
    if len({record['uri'] for record in records}) != len(records):
        raise ValueError('Repeated source in restore projection')
    signature = hashlib.sha256(json.dumps([restore_digest, source_home, destination_home, records],
                                         sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    with catalog.connect() as db:
        db.execute('BEGIN IMMEDIATE')
        db.execute('CREATE TABLE IF NOT EXISTS restored_source_commands '
                   '(command_id TEXT PRIMARY KEY, signature TEXT NOT NULL, result TEXT NOT NULL)')
        previous = db.execute('SELECT signature,result FROM restored_source_commands WHERE command_id=?',
                              (command_id,)).fetchone()
        if previous:
            if previous['signature'] != signature:
                raise ValueError('Restore projection command has different arguments')
            return json.loads(previous['result'])
        changes = []
        for record in records:
            row = db.execute('SELECT * FROM sessions WHERE uri=?', (record['uri'],)).fetchone()
            if not row or not row['storage_path']:
                raise ValueError('Restored source is not exactly indexed')
            old = Path(row['storage_path'])
            try:
                relative = old.relative_to(source / 'projects')
            except ValueError:
                raise ValueError('Indexed source is outside reviewed restore home') from None
            if (os.path.normpath(str(old)) != str(old) or len(relative.parts) != 3
                    or relative.parts[1] != 'sessions' or relative.parts[2] != row['native_id']
                    or any(part in {'.', '..'} or part.startswith('.') for part in relative.parts)):
                raise ValueError('Indexed source is not an exact native session directory')
            snapshot = catalog._source_snapshot(str(old), db=db)
            if snapshot['token'] != record['token'] or snapshot['row']['uri'] != record['uri']:
                raise ValueError('Restored source snapshot changed')
            new = str(destination / 'projects' / relative)
            if db.execute('SELECT 1 FROM sessions WHERE storage_path=? LIMIT 1', (new,)).fetchone():
                raise ValueError('Restored destination is already indexed; inspect identities')
            link = db.execute('SELECT parent_storage FROM native_parent_links WHERE uri=?', (row['uri'],)).fetchone()
            parent = None
            if link:
                old_parent = Path(link['parent_storage'])
                if old_parent.parent != old.parent:
                    raise ValueError('Native parent source differs from restored project')
                parent = str(Path(new).parent / old_parent.name)
            changes.append((row['uri'], str(old), new, parent))
        for uri, old, new, parent in changes:
            db.execute('UPDATE sessions SET storage_path=?,fingerprint=NULL,seen_run=NULL,'
                       'source=CASE WHEN source=? THEN ? ELSE source END WHERE uri=?',
                       (new, 'native:' + source_home, 'native:' + destination_home, uri))
            db.execute('UPDATE source_presence SET storage_path=? WHERE uri=? AND storage_path=?',
                       (new, uri, old))
            if parent:
                db.execute('UPDATE native_parent_links SET parent_storage=? WHERE uri=?', (parent, uri))
        result = {'commandId': command_id, 'restoreDigest': restore_digest,
                  'rebound': len(changes), 'uris': [row[0] for row in changes], 'nativeFilesModified': False}
        db.execute('INSERT INTO restored_source_commands VALUES(?,?,?)',
                   (command_id, signature, json.dumps(result)))
        return result
