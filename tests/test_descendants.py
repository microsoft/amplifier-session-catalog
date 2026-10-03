from pathlib import Path

import pytest

from amplifier_session_catalog import Catalog


def add(catalog,workspace,identity,parent=None,kind=None):
    return catalog.upsert({'uri':'ahp-session:/'+identity,'engineId':'amplifier',
        'nativeSessionId':identity,'workingDirectory':str(workspace),'parentUri':parent,
        'kind':kind or ('child' if parent else 'root')})


def test_family_includes_hidden_internal_and_root_relationships_without_loading_files(tmp_path,monkeypatch):
    catalog=Catalog(tmp_path/'index.sqlite');root='ahp-session:/root'
    add(catalog,tmp_path,'root');add(catalog,tmp_path/'missing','child',root)
    add(catalog,tmp_path,'job','ahp-session:/child','internal')
    add(catalog,tmp_path,'fork',root,'root')
    catalog.set_visibility(uri='ahp-session:/job',nativeSessionId='job',workingDirectory=str(tmp_path),deleted=True,commandId='hide',evidence='native-completed-receipt')
    monkeypatch.setattr(Path,'stat',lambda *a,**k:(_ for _ in ()).throw(AssertionError('native stat')))
    monkeypatch.setattr(Path,'open',lambda *a,**k:(_ for _ in ()).throw(AssertionError('native read')))
    page=catalog.descendants(parentUri=root)
    assert page['completeIndexedCoverage'] and page['rootKnown'] and page['excludedKinds']==[]
    assert {row['nativeSessionId'] for row in page['items']}=={'child','job','fork'}
    assert all(row['parentUri']==row['observedParentUri'] for row in page['items'])
    assert not page['externalWriterCoverage'] and not page['nativeFilesRead']
    assert catalog.descendants(parentUri=root)['revision']==page['revision']
    assert not catalog.descendants(parentUri='ahp-session:/unknown')['completeIndexedCoverage']


def test_truncation_cycles_depth_and_changed_index_never_become_complete_proof(tmp_path):
    catalog=Catalog(tmp_path/'index.sqlite');root='ahp-session:/root';add(catalog,tmp_path,'root')
    before=catalog.descendants(parentUri=root)['revision']
    add(catalog,tmp_path,'one',root);add(catalog,tmp_path,'two',root)
    after=catalog.descendants(parentUri=root);assert before!=after['revision']
    assert after['revision']!=Catalog(catalog.path).descendants(parentUri=root)['revision']
    small=catalog.descendants(parentUri=root,limit=1);assert small['truncated'] and not small['completeIndexedCoverage'] and len(small['items'])==1
    add(catalog,tmp_path,'root','ahp-session:/one');cycle=catalog.descendants(parentUri=root);assert 'cycle' in cycle['issues'] and not cycle['completeIndexedCoverage']
    add(catalog,tmp_path,'deep')
    parent='ahp-session:/deep'
    for n in range(65):add(catalog,tmp_path,str(n),parent);parent='ahp-session:/'+str(n)
    deep=catalog.descendants(parentUri='ahp-session:/deep');assert 'depth-limit' in deep['issues'] and not deep['completeIndexedCoverage']
    assert len(deep['items'])==64
    with pytest.raises(ValueError):catalog.descendants(parentUri=root,limit=101)


def test_25k_unrelated_records_are_not_materialized_for_family_guard(tmp_path):
    catalog=Catalog(tmp_path/'index.sqlite');add(catalog,tmp_path,'root');add(catalog,tmp_path,'child','ahp-session:/root')
    with catalog.connect() as db:
        db.execute("""WITH RECURSIVE ids(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM ids WHERE n<25000)
          INSERT INTO sessions SELECT printf('ahp-session:/other-%d',n),engine,printf('other-%d',n),workspace,
          NULL,kind,title,created_at,modified_at,availability,source,storage_path,native_project,
          fingerprint,seen_run,indexed_at,details,visible FROM ids,sessions WHERE uri='ahp-session:/root'""")
    from contextlib import contextmanager
    original=catalog.connect
    @contextmanager
    def bounded():
        with original() as db:
            steps=0
            def tick():
                nonlocal steps
                steps+=1
                return steps>200
            db.set_progress_handler(tick,100)
            yield db
    catalog.connect=bounded
    result=catalog.descendants(parentUri='ahp-session:/root')
    assert result['completeIndexedCoverage'] and len(result['items'])==1
