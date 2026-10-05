"""Cold metadata publication must make project browsing usable without restart."""
import json
from pathlib import Path
import pytest
from amplifier_session_catalog import Catalog
from amplifier_session_catalog.discovery import Discovery, project_slug


def source(home,workspace,identity,**fields):
    directory=home/'projects'/project_slug(workspace)/'sessions'/identity
    directory.mkdir(parents=True)
    (directory/'metadata.json').write_text(json.dumps({'working_dir':str(workspace),'turn_count':1,'title':identity,**fields}))
    (directory/'transcript.jsonl').write_bytes(b'original canonical transcript\n')
    return directory


def test_actual_cold_scan_publishes_workspace_pages_without_restart(tmp_path,monkeypatch):
    present=tmp_path/'present-with-hyphens';present.mkdir()
    missing=tmp_path/'former-workspace-with-hyphens'
    home=tmp_path/'native';catalog=Catalog(tmp_path/'index.sqlite')
    directories=[source(home,present,'root'),source(home,present,'root_child',parent_id='root'),source(home,missing,'historical')]
    original=Path.open
    def no_history(path,*args,**kwargs):
        if path.name in {'transcript.jsonl','events.jsonl'}:raise AssertionError('Cold discovery opened canonical history')
        return original(path,*args,**kwargs)
    with monkeypatch.context() as patch:
        patch.setattr(Path,'open',no_history)
        state=Discovery(catalog,[home]).scan()
    assert state['phase']=='complete' and state['issues']==0 and state['sessions']==3
    with catalog.connect() as db:
        assert db.execute('SELECT COUNT(*) FROM workspaces').fetchone()[0]==2
        assert db.execute('SELECT COUNT(*) FROM workspace_index').fetchone()[0]==2
    page=catalog.list_workspaces(connectionId='same-live-process',allowedWorkspaceRoots=[str(tmp_path)])
    assert [row['path'] for row in page['items']]==[str(present)]
    assert catalog.get_workspace(id=Catalog.workspace_id(str(missing)))['availability']=='missing'
    assert len(catalog.list(connectionId='roots')['items'])==1
    assert len(catalog.list(connectionId='children',workingDirectory=str(present),parentUri=catalog.list(connectionId='root')['items'][0]['uri'])['items'])==1
    assert all((directory/'transcript.jsonl').read_bytes()==b'original canonical transcript\n' for directory in directories)


def test_failed_cold_publication_rolls_back_workspace_and_browse_index_together(tmp_path,monkeypatch):
    workspace=tmp_path/'project';workspace.mkdir();home=tmp_path/'native';directory=source(home,workspace,'root')
    catalog=Catalog(tmp_path/'index.sqlite');original=catalog._workspace_index
    def fail_after_index(db,path,availability):
        original(db,path,availability)
        raise RuntimeError('publication interrupted before commit')
    monkeypatch.setattr(catalog,'_workspace_index',fail_after_index)
    with pytest.raises(RuntimeError,match='publication interrupted'):
        Discovery(catalog,[home]).hint(str(directory))
    with catalog.connect() as db:
        for table in ['workspaces','workspace_index','sessions']:
            assert db.execute('SELECT COUNT(*) FROM '+table).fetchone()[0]==0
    monkeypatch.setattr(catalog,'_workspace_index',original)
    assert Discovery(catalog,[home]).scan()['issues']==0
    assert catalog.list_workspaces(connectionId='recovery')['items'][0]['path']==str(workspace)


def test_later_discovery_preserves_owner_alias_and_hidden_workspace(tmp_path):
    workspace=tmp_path/'project';workspace.mkdir();home=tmp_path/'native';source(home,workspace,'root')
    catalog=Catalog(tmp_path/'index.sqlite');discovery=Discovery(catalog,[home]);assert discovery.scan()['issues']==0
    id=Catalog.workspace_id(str(workspace));catalog.project_workspaces(source='workspaces',afterRevision=0,throughRevision=1,records=[{'id':id,'path':str(workspace),'name':'Owned alias','registered':False,'hidden':True,'revision':1}])
    source(home,workspace,'later-root');assert discovery.scan()['issues']==0
    row=catalog.get_workspace(id=id);assert row['name']=='Owned alias' and row['hidden']
    assert catalog.list_workspaces(connectionId='hidden')['items']==[]
    assert catalog.list(connectionId='hidden-chats')['items']==[]
