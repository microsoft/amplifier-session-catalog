"""Recovery discovery relaxes only missing workspace availability."""
from pathlib import Path
import pytest
from amplifier_session_catalog import Catalog, InvalidCursor
from test_library import row, query


def test_missing_pages_are_explicit_scoped_and_do_not_read_history(tmp_path, monkeypatch):
    c=Catalog(tmp_path/'index.sqlite');root=tmp_path/'authorized';root.mkdir()
    missing=root/'gone';outside=tmp_path/'authorized-sibling'/'gone'
    for n in range(1,7):c.upsert(row(missing,n))
    c.upsert(row(missing,7,isArchived=True))
    c.upsert(row(missing,8,parentUri=row(missing,1)['uri']))
    c.upsert(row(outside,9))
    c.workspace(str(missing),availability='missing');c.workspace(str(outside),availability='missing')
    assert query(c,root)['items']==[]
    monkeypatch.setattr(Path,'stat',lambda *_: (_ for _ in ()).throw(AssertionError('filesystem read')))
    monkeypatch.setattr(Path,'open',lambda *_: (_ for _ in ()).throw(AssertionError('history read')))
    first=query(c,root,location='missing',limit=2,sort='name')
    second=query(c,root,location='missing',limit=2,sort='name',cursor=first['nextCursor'])
    assert [r['uri'] for r in first['items']+second['items']]==[row(missing,n)['uri'] for n in range(1,5)]
    assert all(r['workspaceAvailability']=='missing' for r in first['items'])
    assert [r['uri'] for r in query(c,root,location='missing',archive='archived')['items']]==[row(missing,7)['uri']]
    assert [r['uri'] for r in query(c,root,location='missing',parentUri=row(missing,1)['uri'])['items']]==[row(missing,8)['uri']]
    with pytest.raises(InvalidCursor):query(c,root,cursor=first['nextCursor'])
    c.workspace(str(missing),availability='present')
    with pytest.raises(InvalidCursor):query(c,root,location='missing',limit=2,sort='name',cursor=first['nextCursor'])
    assert query(c,root,location='missing')['items']==[]


@pytest.mark.parametrize('exclusion',['native','product','workspace','unknown','source'])
def test_missing_discovery_preserves_independent_exclusions(tmp_path,exclusion):
    from test_source_presence import fixture, disappear
    c,d,uri,_,directory,w,_=fixture(tmp_path)
    c.workspace(str(w),availability='missing')
    assert [r['uri'] for r in query(c,tmp_path,location='missing')['items']]==[uri]
    if exclusion=='native':c.set_visibility(uri=uri,nativeSessionId='root',workingDirectory=str(w),deleted=True,commandId='delete',evidence='native-completed-receipt')
    elif exclusion=='product':c.project_session_visibility(source='host',commandId='hide',afterRevision=0,throughRevision=1,items=[{'uri':uri,'hidden':True,'revision':1}])
    elif exclusion=='workspace':c.project_workspaces(source='host',afterRevision=0,throughRevision=1,records=[{'id':c.workspace_id(str(w)),'name':'Hidden','path':str(w),'hidden':True,'registered':True,'revision':1}])
    elif exclusion=='unknown':c.workspace(str(w),availability='unknown')
    else:disappear(d,directory)
    assert query(c,tmp_path,location='missing')['items']==[]


def test_missing_discovery_never_includes_managed_locations(tmp_path):
    from test_managed_locations import managed
    c=Catalog(tmp_path/'index.sqlite');root,allocation=managed(tmp_path);c.upsert(allocation)
    c.workspace(allocation['workingDirectory'],availability='missing')
    assert c.library_query(connectionId='fixture',allowedWorkspaceRoots=[str(tmp_path)],allowedManagedRoots=[str(root)],location='missing')['items']==[]
