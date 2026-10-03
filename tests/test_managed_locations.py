import json
from pathlib import Path
import uuid

import pytest

from amplifier_session_catalog import Catalog, InvalidCursor
from amplifier_session_catalog.discovery import Discovery, project_slug


def managed(tmp_path, number=1):
    identity=str(uuid.UUID(int=number))
    root=tmp_path/'private-managed'
    workspace=root/'chats'/identity/'files'
    workspace.mkdir(parents=True)
    location={'version':1,'kind':'managed','allocationId':identity,
              'creationReceipt':f'create-{number}','allocationHash':'a'*64,'owner':'amplifier-unified'}
    return root, {'uri':'ahp-session:/'+identity,'engineId':'amplifier','nativeSessionId':identity,
                  'workingDirectory':str(workspace),'title':'Managed chat','modifiedAt':1000+number,
                  'createdAt':1000,'location':location}


def test_managed_grant_is_separate_exact_and_never_a_directory_grant(tmp_path,monkeypatch):
    catalog=Catalog(tmp_path/'catalog.sqlite');root,row=managed(tmp_path)
    catalog.upsert(row)
    ordinary={**row,'uri':'ahp-session:/ordinary','nativeSessionId':'ordinary'};ordinary.pop('location')
    catalog.upsert(ordinary)
    outside=tmp_path/'project';outside.mkdir()
    catalog.upsert({**ordinary,'uri':'ahp-session:/project','workingDirectory':str(outside)})
    args={'connectionId':'client','allowedWorkspaceRoots':[],'allowedManagedRoots':[str(root)]}
    with monkeypatch.context() as patch:
        patch.setattr(Path,'stat',lambda *_a,**_k:(_ for _ in ()).throw(AssertionError('hot path stat')))
        patch.setattr(Path,'open',lambda *_a,**_k:(_ for _ in ()).throw(AssertionError('hot path native read')))
        assert [r['uri'] for r in catalog.list(**args)['items']]==[row['uri']]
        assert catalog.retention_candidates(**args,modifiedBefore=2000)['candidateCount']==1
        resolved=catalog.resolve_native(engineId='amplifier',identities=[row['nativeSessionId'],'ordinary'],allowedWorkspaceRoots=[],allowedManagedRoots=[str(root)])
        assert resolved[row['nativeSessionId']]['uri']==row['uri'] and resolved['ordinary']['status']=='missing'
        assert catalog.list(**{**args,'allowedManagedRoots':[str(root.parent)]})['items']==[]
        assert catalog.list(**{**args,'allowedManagedRoots':[]})['items']==[]
        assert [r['path'] for r in catalog.list_workspaces(connectionId='client')['items']]==[str(outside)]
        assert catalog.get_workspace(id=catalog.workspace_id(row['workingDirectory'])) is None
    with pytest.raises(ValueError,match='explicit workspace'):
        catalog.list(connectionId='client',allowedManagedRoots=[str(root)])


def test_managed_pagination_binds_scope_and_projects_only_existing_roots(tmp_path):
    catalog=Catalog(tmp_path/'catalog.sqlite')
    for n in range(1,5):
        root,row=managed(tmp_path,n);catalog.upsert(row)
    args={'connectionId':'a','allowedWorkspaceRoots':[],'allowedManagedRoots':[str(root)],'limit':2}
    first=catalog.list(**args);second=catalog.list(**args,cursor=first['nextCursor'])
    assert len({r['uri'] for r in first['items']+second['items']})==4
    with pytest.raises(InvalidCursor):catalog.list(**{**args,'allowedManagedRoots':[]},cursor=first['nextCursor'])
    catalog.workspace(row['workingDirectory'],availability='missing')
    assert len(catalog.list(**{**args,'limit':10})['items'])==3
    catalog.workspace(row['workingDirectory'],availability='present')
    assert len(catalog.list(**{**args,'limit':10})['items'])==4


def test_host_projection_survives_missing_stale_discovery_and_visibility_changes(tmp_path):
    catalog=Catalog(tmp_path/'catalog.sqlite');root,row=managed(tmp_path);catalog.upsert(row)
    catalog.project_session_archives(items=[{'uri':row['uri'],'isArchived':True}])
    stale={**row,'location':{**row['location'],'allocationHash':'b'*64}}
    assert catalog.upsert(stale,source='native:home')['location']==row['location']
    absent={**row};absent.pop('location')
    assert catalog.upsert(absent,source='native:home')['location']==row['location']
    assert catalog.get(row['uri'])['isArchived']
    args={'uri':row['uri'],'nativeSessionId':row['nativeSessionId'],'workingDirectory':row['workingDirectory'],
          'commandId':'hide','evidence':'native-completed-receipt'}
    catalog.set_visibility(**args,deleted=True)
    catalog.upsert(absent,source='native:home')
    catalog.workspace(row['workingDirectory'],availability='missing');catalog.workspace(row['workingDirectory'],availability='present')
    assert catalog.list(connectionId='a')['items']==[]
    catalog.set_visibility(**args,deleted=False)
    assert catalog.list(connectionId='a')['items'][0]['location']==row['location']
    with pytest.raises(ValueError,match='immutable'):catalog.upsert(stale)
    with pytest.raises(ValueError,match='reassigned'):catalog.upsert({**absent,'nativeSessionId':'other'})


@pytest.mark.parametrize('change',[
    {'version':True},{'allocationId':'not-uuid'},{'allocationHash':'G'*64},
    {'creationReceipt':'line\nbreak'},{'owner':'other'},{'extra':'ignored?'},
])
def test_malformed_projection_is_rejected(tmp_path,change):
    catalog=Catalog(tmp_path/'catalog.sqlite');_,row=managed(tmp_path)
    with pytest.raises(ValueError):catalog.upsert({**row,'location':{**row['location'],**change}})
    assert catalog.list(connectionId='a')['items']==[]


def test_projection_rejects_layout_changes_and_project_registration(tmp_path):
    catalog=Catalog(tmp_path/'catalog.sqlite');_,row=managed(tmp_path)
    with pytest.raises(ValueError,match='exact allocation'):catalog.upsert({**row,'workingDirectory':str(tmp_path)})
    catalog.upsert(row)
    with pytest.raises(ValueError,match='not a project'):
        catalog.project_workspaces(source='owner',afterRevision=0,throughRevision=1,records=[{
            'id':catalog.workspace_id(row['workingDirectory']),'path':row['workingDirectory'],'name':'bad',
            'registered':True,'hidden':False,'revision':1}])
    assert catalog.workspace_projection_status(source='owner')['revision']==0


def test_zero_turn_native_rebuild_and_scanner_first_race_keep_exact_managed_identity(tmp_path):
    root,row=managed(tmp_path);home=tmp_path/'native'
    directory=home/'projects'/project_slug(row['workingDirectory'])/'sessions'/row['nativeSessionId']
    directory.mkdir(parents=True)
    metadata={'cwd':row['workingDirectory'],'location':row['location'],'title':'Empty managed','turn_count':0}
    (directory/'metadata.json').write_text(json.dumps(metadata))
    (directory/'transcript.jsonl').write_text('')
    catalog=Catalog(tmp_path/'catalog.sqlite');discovery=Discovery(catalog,[home])
    assert discovery.hint(str(directory))['uri']==row['uri']
    catalog.upsert(row)
    assert len(catalog.list(connectionId='a')['items'])==1
    metadata.pop('location');(directory/'metadata.json').write_text(json.dumps(metadata))
    discovery.hint(str(directory),force=True)
    assert catalog.get(row['uri'])['location']==row['location']
    metadata['location']={'invalid':True};(directory/'metadata.json').write_text(json.dumps(metadata))
    assert discovery.hint(str(directory),force=True)['issues']
    assert catalog.get(row['uri'])['location']==row['location']
    assert catalog.list_workspaces(connectionId='a')['items']==[]


def test_noncanonical_sidecars_cannot_create_managed_identity(tmp_path):
    _,row=managed(tmp_path);home=tmp_path/'native'
    directory=home/'projects'/project_slug(row['workingDirectory'])/'sessions'/'ordinary'
    directory.mkdir(parents=True)
    (directory/'metadata.json').write_text(json.dumps({'cwd':row['workingDirectory'],'turn_count':1}))
    (directory/'naming.json').write_text(json.dumps({'location':row['location']}))
    catalog=Catalog(tmp_path/'catalog.sqlite');Discovery(catalog,[home]).hint(str(directory))
    assert 'location' not in catalog.list(connectionId='a')['items'][0]


def test_invalid_empty_native_identity_and_existing_mapping_are_not_adopted(tmp_path):
    _,row=managed(tmp_path);home=tmp_path/'native';catalog=Catalog(tmp_path/'catalog.sqlite')
    base=home/'projects'/project_slug(row['workingDirectory'])/'sessions'
    wrong=base/'wrong';wrong.mkdir(parents=True)
    metadata={'cwd':row['workingDirectory'],'location':row['location'],'turn_count':0}
    (wrong/'metadata.json').write_text(json.dumps(metadata))
    discovery=Discovery(catalog,[home])
    assert discovery.hint(str(wrong))['empty']
    assert catalog.list(connectionId='a')['items']==[]
    exact=base/row['nativeSessionId'];exact.mkdir()
    (exact/'metadata.json').write_text(json.dumps(metadata))
    other={**row,'nativeSessionId':'other'};other.pop('location');catalog.upsert(other)
    result=discovery.hint(str(exact))
    assert not result['changed'] and 'another source' in result['issues'][0]
    assert catalog.get(row['uri'])['nativeSessionId']=='other'
