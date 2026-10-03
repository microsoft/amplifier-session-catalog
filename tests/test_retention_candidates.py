from pathlib import Path
from contextlib import contextmanager

import pytest

from amplifier_session_catalog import Catalog, InvalidCursor
from amplifier_session_catalog.__main__ import Server
from amplifier_session_catalog.discovery import Discovery


def test_cold_cutoff_queries_filter_before_page_and_never_claim_eligibility(tmp_path, monkeypatch):
    catalog=Catalog(tmp_path/'index.sqlite');owned=tmp_path/'owned';owned.mkdir()
    outside=tmp_path/'outside';outside.mkdir()
    def add(identity, workspace=owned, **extra):
        return catalog.upsert({'uri':'ahp-session:/'+identity,'engineId':'amplifier',
            'nativeSessionId':identity,'workingDirectory':str(workspace),
            'createdAt':500,'modifiedAt':1000,**extra})
    for identity in ['a','b','c']:add(identity)
    add('young',modifiedAt=3000);add('newly-created',createdAt=3000)
    add('child',parentUri='ahp-session:/a');add('hidden-child',kind='internal')
    add('missing',workspace=tmp_path/'absent');add('other',workspace=outside)
    add('deleted');catalog.set_visibility(uri='ahp-session:/deleted',nativeSessionId='deleted',workingDirectory=str(owned),deleted=True,commandId='hide',evidence='native-completed-receipt')
    catalog.project_session_archives(items=[{'uri':'ahp-session:/b','isArchived':True}])
    monkeypatch.setattr(Path,'open',lambda *a,**k:(_ for _ in ()).throw(AssertionError('opened native payload')))
    monkeypatch.setattr(Path,'stat',lambda *a,**k:(_ for _ in ()).throw(AssertionError('restatted workspace')))
    args={'connectionId':'review','modifiedBefore':2000,'allowedWorkspaceRoots':[str(owned)],'limit':2}
    page=catalog.retention_candidates(**args)
    assert page['candidateCount']==3 and page['eligibility']=='unverified'
    assert not page['nativeFilesRead'] and not page['removalAuthorized']
    assert len(page['items'])==2
    next_page=catalog.retention_candidates(**args,cursor=page['nextCursor'])
    assert next_page['candidateCount']==3 and len(next_page['items'])==1
    assert len({row['uri'] for row in page['items']+next_page['items']})==3
    with pytest.raises(InvalidCursor):catalog.retention_candidates(**{**args,'modifiedBefore':1500},cursor=page['nextCursor'])
    assert catalog.retention_candidates(**{**args,'allowedWorkspaceRoots':[]})['candidateCount']==0


@pytest.mark.parametrize('cutoff',[None,True,-1,float('inf'),float('nan'),'yesterday'])
def test_cutoff_and_root_authority_are_explicit(tmp_path,cutoff):
    catalog=Catalog(tmp_path/'index.sqlite')
    with pytest.raises(ValueError):catalog.retention_candidates(connectionId='c',modifiedBefore=cutoff,allowedWorkspaceRoots=[])
    with pytest.raises(ValueError):catalog.retention_candidates(connectionId='c',modifiedBefore=1,allowedWorkspaceRoots=None)


def test_large_catalog_candidate_page_and_count_are_indexed_and_stdio_read_only(tmp_path):
    catalog=Catalog(tmp_path/'index.sqlite')
    catalog.upsert({'uri':'ahp-session:/one','engineId':'a','nativeSessionId':'one',
                    'workingDirectory':str(tmp_path),'createdAt':1,'modifiedAt':2})
    with catalog.connect() as db:
        db.execute("""WITH RECURSIVE ids(n) AS (VALUES(2) UNION ALL SELECT n+1 FROM ids WHERE n<25000)
            INSERT INTO sessions SELECT printf('ahp-session:/%08d',n),engine,printf('%d',n),workspace,
            parent_uri,kind,title,created_at,modified_at,availability,source,storage_path,native_project,
            fingerprint,seen_run,indexed_at,details,visible FROM ids,sessions WHERE uri='ahp-session:/one'""")
    original=catalog.connect
    @contextmanager
    def bounded():
        with original() as db:
            steps=0
            def tick():
                nonlocal steps
                steps+=1
                return steps>2000
            db.set_progress_handler(tick,1000)
            yield db
    catalog.connect=bounded
    response=Server(catalog,Discovery(catalog,[])).handle({'jsonrpc':'2.0','id':1,'method':'retentionCandidates',
        'params':{'connectionId':'r','modifiedBefore':3,'allowedWorkspaceRoots':[str(tmp_path)],'limit':50}})
    result=response['result'];assert result['candidateCount']==25000 and len(result['items'])==50
    assert catalog.list(connectionId='ordinary',limit=1).get('total') is None
    with original() as db:assert db.execute('SELECT count(*) FROM sessions').fetchone()[0]==25000
