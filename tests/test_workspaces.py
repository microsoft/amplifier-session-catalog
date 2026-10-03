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
