from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
import pytest
from amplifier_session_catalog import Catalog


def row(workspace,uri='ahp-session:/requested',**extra):
    return dict(uri=uri,engineId='native',nativeSessionId='new-native',workingDirectory=str(workspace),title='Imported',**extra)

def claim(catalog,record,command='import-command'):
    return catalog.claim_native(record=record,commandId=command,evidence='verified-native-import')

def test_claim_preserves_discovery_identity_and_user_edits(tmp_path):
    catalog=Catalog(tmp_path/'index');workspace=tmp_path/'workspace';workspace.mkdir()
    discovered=row(workspace,'ahp-session:/discovered',storagePath=str(tmp_path/'home/session'))
    catalog.upsert(discovered,source='native:home')
    edited=dict(discovered,title='My edited title',isArchived=True);catalog.upsert(edited)
    first=claim(catalog,row(workspace));assert first['uri']==discovered['uri'];assert first['title']=='My edited title';assert first['isArchived'] is True
    assert claim(catalog,row(workspace))['uri']==first['uri']
    assert claim(catalog,row(workspace,'ahp-session:/new-hint'),'another-command')['uri']==first['uri']
    with pytest.raises(ValueError,match='identity changed'):claim(catalog,row(workspace,'ahp-session:/changed'))
    catalog.upsert(discovered,source='native:home');assert catalog.get(first['uri'])['title']=='My edited title'
    assert catalog.list(connectionId='a',archive='all')['items'][0]['uri']==first['uri']

def test_concurrent_scanner_claim_cannot_duplicate_and_different_homes_refuse(tmp_path):
    catalog=Catalog(tmp_path/'index');workspace=tmp_path/'workspace';workspace.mkdir();barrier=Barrier(2)
    record=row(workspace)
    def scan():
        # Discovery computed its URI before the host claim; writer recheck wins.
        barrier.wait();return catalog.upsert(row(workspace,'ahp-session:/scanner',storagePath=str(tmp_path/'home/session')),source='native:home')
    def adopt():barrier.wait();return claim(catalog,record)
    with ThreadPoolExecutor(2) as pool:
        a=pool.submit(scan);b=pool.submit(adopt);a.result();adopted=b.result()
    items=catalog.list(connectionId='one',archive='all')['items'];assert len(items)==1;assert items[0]['uri']==adopted['uri']
    with pytest.raises(ValueError,match='differs from the claimed'):catalog.upsert(row(workspace,'ahp-session:/another-home',storagePath=str(tmp_path/'other/session')),source='native:other')
    assert len(catalog.list(connectionId='two',archive='all')['items'])==1

def test_ambiguous_native_identity_or_conflicting_uri_never_overwritten(tmp_path):
    catalog=Catalog(tmp_path/'index');workspace=tmp_path/'workspace';workspace.mkdir()
    catalog.upsert(row(workspace,'ahp-session:/one'));catalog.upsert(row(workspace,'ahp-session:/two'))
    with pytest.raises(ValueError,match='ambiguous'):claim(catalog,row(workspace))
    catalog.upsert(dict(row(workspace),nativeSessionId='different'))
    with pytest.raises(ValueError,match='belongs to another'):claim(catalog,row(workspace))
    assert len(catalog.list(connectionId='one',archive='all')['items'])==3
