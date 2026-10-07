import json
from pathlib import Path
import shutil

import pytest

from amplifier_session_catalog import Catalog
from amplifier_session_catalog.discovery import Discovery
from amplifier_session_catalog.restore import rebind_restored_sources
from test_source_presence import fixture


def prepare(tmp_path):
    c, d, uri, child, directory, workspace, _ = fixture(tmp_path)
    source = d.homes[0]
    destination = tmp_path / 'restored-native'
    shutil.copytree(source, destination)
    records = [{'uri': u, 'token': c._source_snapshot(c.get(u)['storagePath'])['token']}
               for u in [uri, child]]
    args = dict(command_id='restore-1', restore_digest='a' * 64, source_home=str(source),
                destination_home=str(destination), records=records)
    return c, d, uri, child, source, destination, args


def test_restore_keeps_one_uri_and_family_without_native_reads_or_writes(tmp_path, monkeypatch):
    c, d, uri, child, source, destination, args = prepare(tmp_path)
    before = {str(p): p.read_bytes() for home in [source, destination] for p in home.rglob('*') if p.is_file()}
    original = Path.open
    def no_history(path, *a, **kw):
        assert path.name not in {'transcript.jsonl', 'events.jsonl'}
        return original(path, *a, **kw)
    monkeypatch.setattr(Path, 'open', no_history)
    result = rebind_restored_sources(c, **args)
    assert result['rebound'] == 2 and not result['nativeFilesModified']
    c = Catalog(c.path)
    assert rebind_restored_sources(c, **args) == result
    restored = Discovery(c, [destination])
    # Child-first rescan must retain the original parent and child links.
    for identity in [child, uri]:
        assert restored.hint(c.get(identity)['storagePath'])['uri'] == identity
    assert [row['uri'] for row in c.list(connectionId='restored')['items']] == [uri]
    assert c.descendants(parentUri=uri)['items'][0]['uri'] == child
    assert c.get(child)['parentUri'] == uri
    monkeypatch.setattr(Path, 'open', original)
    assert all(Path(path).read_bytes() == data for path, data in before.items())


def test_restore_retains_all_visibility_claim_and_unknown_state(tmp_path):
    c, d, uri, child, source, destination, args = prepare(tmp_path)
    c.claim_native(record=c.get(uri), commandId='claim', evidence='verified-native-import')
    c.project_session_archives(items=[{'uri': uri, 'isArchived': True}])
    c.project_session_visibility(source='host', commandId='hide', afterRevision=0, throughRevision=1,
                                items=[{'uri': uri, 'hidden': True, 'revision': 1}])
    c.set_visibility(uri=child, nativeSessionId='child', workingDirectory=c.get(child)['workingDirectory'],
                     deleted=True, commandId='delete', evidence='native-completed-receipt')
    unknown = {'version': 1, 'source': 'host', 'incarnation': 'i', 'revision': 3,
               'observedAt': '2026-10-06T00:00:00Z', 'effect': 'unknown', 'unknownChecked': True}
    c.upsert({**c.get(uri), 'hostObservationOnly': True, 'hostObservation': unknown})
    with c.connect() as db:
        db.execute('INSERT INTO source_presence VALUES(?,?,1)', (uri, c.get(uri)['storagePath']))
        retained = {table: [tuple(row) for row in db.execute('SELECT * FROM ' + table)]
                    for table in ['native_claims', 'session_archive', 'native_visibility', 'product_visibility']}
    for record in args['records']:
        record['token'] = c._source_snapshot(c.get(record['uri'])['storagePath'])['token']
    rebind_restored_sources(c, **args)
    c = Catalog(c.path)
    assert c.get(uri)['hostObservation'] == unknown
    with c.connect() as db:
        assert all([tuple(row) for row in db.execute('SELECT * FROM ' + table)] == rows for table, rows in retained.items())
        assert db.execute('SELECT storage_path FROM source_presence WHERE uri=?', (uri,)).fetchone()[0] == c.get(uri)['storagePath']


@pytest.mark.parametrize('failure', ['stale', 'collision', 'foreign', 'ambiguous'])
def test_refused_batch_changes_no_prior_binding(tmp_path, failure):
    c, d, uri, child, source, destination, args = prepare(tmp_path)
    if failure == 'stale':
        args['records'][1]['token'] = '0' * 64
    elif failure == 'collision':
        Discovery(c, [destination]).hint(str(destination / Path(c.get(child)['storagePath']).relative_to(source)))
    elif failure == 'foreign':
        args['source_home'] = str(tmp_path / 'other')
    else:
        c.upsert({**c.get(child), 'uri': 'ahp-session:/duplicate'})
    with c.connect() as db:
        before = [tuple(row) for row in db.execute('SELECT * FROM sessions ORDER BY uri')]
    with pytest.raises(ValueError):
        rebind_restored_sources(c, **args)
    with c.connect() as db:
        assert [tuple(row) for row in db.execute('SELECT * FROM sessions ORDER BY uri')] == before


def test_changed_command_arguments_never_rewind_a_later_binding(tmp_path):
    c, d, uri, child, source, destination, args = prepare(tmp_path)
    rebind_restored_sources(c, **args)
    with pytest.raises(ValueError, match='different arguments'):
        rebind_restored_sources(c, **{**args, 'restore_digest': 'b' * 64})
    assert c.get(uri)['storagePath'].startswith(str(destination) + '/')


@pytest.mark.parametrize('records', [[], [{}] * 101, [{'uri': 'x', 'token': 'fake'}]])
def test_restore_rejects_unbounded_or_unproven_inputs(tmp_path, records):
    c, d, uri, child, source, destination, args = prepare(tmp_path)
    with pytest.raises(ValueError):
        rebind_restored_sources(c, **{**args, 'records': records})
