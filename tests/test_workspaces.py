import hashlib
from pathlib import Path
import pytest
from amplifier_session_catalog import Catalog

def registration(path,revision=1,hidden=False,name='Display name'):
    return {'id':Catalog.workspace_id(str(path)),'path':str(path),'name':name,'registered':not hidden,'hidden':hidden,'revision':revision}

def test_workspace_pages_filter_existing_hidden_and_authorized_before_paging(tmp_path,monkeypatch):
    catalog=Catalog(tmp_path/'catalog.sqlite');root=tmp_path/'projects';root.mkdir()
    with catalog.connect() as db:
        db.executemany('INSERT INTO workspaces VALUES(?,?,?)',[(str(root/f'historical-{i}'),'missing',1) for i in range(4000)])
    catalog=Catalog(catalog.path) # One-time bounded derived identity backfill.
    for i in range(25):
        path=root/f'present-{i:02}';path.mkdir();catalog.workspace(str(path))
    outside=tmp_path/'outside';outside.mkdir();catalog.workspace(str(outside))
    selected=root/'present-00';catalog.project_workspaces(source='owner',afterRevision=0,throughRevision=1,records=[registration(selected,name='Unique shared label')])
    with monkeypatch.context() as patch:
        patch.setattr(Path,'stat',lambda *_a,**_k:(_ for _ in ()).throw(AssertionError('Interactive workspace query performed filesystem I/O')))
        first=catalog.list_workspaces(connectionId='a',limit=10,allowedWorkspaceRoots=[str(root)])
        second=catalog.list_workspaces(connectionId='a',limit=10,allowedWorkspaceRoots=[str(root)],cursor=first['nextCursor'])
        assert len(first['items'])==len(second['items'])==10
        assert not set(row['id'] for row in first['items'])&set(row['id'] for row in second['items'])
        assert catalog.list_workspaces(connectionId='a',search='Unique',allowedWorkspaceRoots=[str(root)])['items'][0]['path']==str(selected)
        assert catalog.get_workspace(id=registration(selected)['id'])['name']=='Unique shared label'
        with pytest.raises(ValueError,match='connection or query'):catalog.list_workspaces(connectionId='b',cursor=first['nextCursor'])

def test_removing_association_hides_normal_discovery_without_deleting_or_reviving_history(tmp_path):
    path=tmp_path/'workspace';path.mkdir();event=path/'preserve.txt';event.write_text('user files')
    catalog=Catalog(tmp_path/'catalog.sqlite');native={'uri':'ahp-session:/one','nativeSessionId':'one','engineId':'native','workingDirectory':str(path),'title':'Retained'};catalog.upsert(native)
    catalog.project_workspaces(source='owner',afterRevision=0,throughRevision=1,records=[registration(path,hidden=True)])
    assert catalog.list(connectionId='a')['items']==[] and catalog.list_workspaces(connectionId='a')['items']==[]
    assert catalog.list(connectionId='a',includeArchive=True)['items'][0]['uri']==native['uri']
    catalog.upsert(native);catalog.workspace(str(path));catalog.set_visibility(uri=native['uri'],nativeSessionId='one',workingDirectory=str(path),deleted=False,commandId='restored',evidence='native-completed-receipt')
    assert catalog.list(connectionId='a')['items']==[]
    assert catalog.get_workspace(id=registration(path)['id'])['hidden']
    assert event.read_text()=='user files'
    catalog.project_workspaces(source='owner',afterRevision=1,throughRevision=2,records=[registration(path,revision=2)])
    assert len(catalog.list(connectionId='a')['items'])==1

def test_projection_checkpoint_owner_and_revision_conflicts_are_atomic_and_repairable(tmp_path):
    path=tmp_path/'workspace';path.mkdir();catalog=Catalog(tmp_path/'one.sqlite');record=registration(path)
    assert catalog.workspace_projection_status(source='owner')['revision']==0
    catalog.project_workspaces(source='owner',afterRevision=0,throughRevision=1,records=[record])
    with pytest.raises(ValueError,match='checkpoint'):catalog.project_workspaces(source='owner',afterRevision=0,throughRevision=1,records=[record])
    with pytest.raises(ValueError,match='different configured owner'):catalog.project_workspaces(source='other',afterRevision=0,throughRevision=2,records=[registration(path,revision=2,name='forged')])
    assert catalog.get_workspace(id=record['id'])['name']==record['name']
    assert catalog.workspace_projection_status(source='other')['revision']==0
    rebuilt=Catalog(tmp_path/'rebuilt.sqlite');rebuilt.project_workspaces(source='owner',afterRevision=0,throughRevision=1,records=[record])
    assert rebuilt.get_workspace(id=record['id'])['name']==record['name']
    assert rebuilt.list_workspaces(connectionId='a')['items'][0]['available']


def test_workspace_archive_is_independent_of_history_visibility_and_cursor_scope(tmp_path):
    catalog=Catalog(tmp_path/'catalog.sqlite');root=tmp_path/'projects';root.mkdir()
    for index in range(5):
        path=root/str(index);path.mkdir()
        catalog.upsert({'uri':f'ahp-session:/{index}','nativeSessionId':str(index),'engineId':'native','workingDirectory':str(path),'title':f'Chat {index}'})
        catalog.project_workspaces(source='owner',afterRevision=index,throughRevision=index+1,records=[{**registration(path,revision=index+1,name=f'Workspace {index}'),'archived':index%2==0}])
    before=catalog.list(connectionId='history')['items']
    page=catalog.list_workspaces(connectionId='a',archive='archived',limit=2)
    assert len(page['items'])==2 and all(row['archived'] for row in page['items'])
    with pytest.raises(ValueError,match='connection or query'):catalog.list_workspaces(connectionId='a',archive='active',cursor=page['nextCursor'])
    assert len(catalog.list_workspaces(connectionId='a',archive='archived',cursor=page['nextCursor'])['items'])==1
    assert len(catalog.list_workspaces(connectionId='a',archive='all')['items'])==5
    assert len(catalog.list_workspaces(connectionId='a')['items'])==2
    # Discovery and older projections cannot erase a retained archive flag.
    path=root/'0';catalog.workspace(str(path))
    catalog.project_workspaces(source='owner',afterRevision=5,throughRevision=6,records=[registration(path,revision=6)])
    assert catalog.get_workspace(id=Catalog.workspace_id(str(path)))['archived']
    catalog=Catalog(catalog.path)
    assert catalog.list(connectionId='history')['items']==before
    assert catalog.list_workspaces(connectionId='a',archive='archived',search='Display')['items'][0]['path']==str(path)
    with pytest.raises(ValueError,match='flag'):
        catalog.project_workspaces(source='owner',afterRevision=6,throughRevision=7,records=[{**registration(path,revision=7),'archived':1}])
    assert catalog.workspace_projection_status(source='owner')['revision']==6
