from amplifier_session_catalog import Catalog
import pytest


def test_cleanup_census_includes_invisible_roots_children_and_storage_without_body_reads(tmp_path):
    catalog = Catalog(tmp_path / 'catalog.sqlite')
    for uri, kind in [('root', 'root'), ('child', 'child'), ('internal', 'internal')]:
        catalog.upsert({'uri': 'ahp-session:/' + uri, 'nativeSessionId': uri,
                        'engineId': 'native', 'workingDirectory': str(tmp_path / 'missing'),
                        'storagePath': str(tmp_path / 'history' / uri), 'kind': kind,
                        **({'parentUri': 'ahp-session:/root'} if kind == 'child' else {})})
    first = catalog.workspace_cleanup_census()
    assert len(first['items']) == 3
    assert first['coverage'] == 'indexed-metadata-only'
    assert first['transcriptBodiesRead'] is False and first['externalWriterCoverage'] is False
    assert {r['nativeSessionId'] for r in first['items']} == {'root', 'child', 'internal'}
    assert all(r['storagePath'].startswith(str(tmp_path / 'history')) for r in first['items'])
    assert catalog.workspace_cleanup_census() == first
    with catalog.connect() as db:
        db.execute("UPDATE sessions SET workspace=? WHERE uri='ahp-session:/root'", (str(tmp_path),))
    assert catalog.workspace_cleanup_census()['revision'] != first['revision']


def test_cleanup_census_refuses_overflow_instead_of_claiming_partial_coverage(tmp_path):
    catalog = Catalog(tmp_path / 'catalog.sqlite')
    catalog.upsert({'uri': 'ahp-session:/one', 'engineId': 'native', 'nativeSessionId': 'one', 'workingDirectory': str(tmp_path)})
    with catalog.connect() as db:
        columns = [r['name'] for r in db.execute('PRAGMA table_info(sessions)')]
        original = dict(db.execute('SELECT * FROM sessions LIMIT 1').fetchone())
        values = []
        for number in range(10000):
            row = {**original, 'uri': 'ahp-session:/' + str(number), 'native_id': str(number)}
            values.append(tuple(row[c] for c in columns))
        db.executemany('INSERT INTO sessions (' + ','.join(columns) + ') VALUES (' + ','.join('?' for c in columns) + ')', values)
    with pytest.raises(ValueError, match='capacity'):
        catalog.workspace_cleanup_census()
