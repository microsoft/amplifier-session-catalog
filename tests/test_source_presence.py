"""Reversible discovery absence is independent of every owner exclusion."""
import json
import os
from pathlib import Path
import shutil
from contextlib import contextmanager

import pytest

from amplifier_session_catalog import Catalog
from amplifier_session_catalog.discovery import Discovery, project_slug


def fixture(tmp_path):
    workspace=tmp_path/'workspace';workspace.mkdir();home=tmp_path/'native'
    sessions=home/'projects'/project_slug(workspace)/'sessions';sessions.mkdir(parents=True)
    catalog=Catalog(tmp_path/'index.sqlite');discovery=Discovery(catalog,[home])
    def native(identity,parent=None):
        directory=sessions/identity;directory.mkdir()
        (directory/'metadata.json').write_text(json.dumps({'working_dir':str(workspace),'name':identity,'parent_id':parent,'turn_count':1}))
        (directory/'transcript.jsonl').write_bytes(b'opaque preserved history')
        return directory
    directory=native('root');uri=discovery.hint(str(directory))['uri']
    child=native('child','root');child_uri=discovery.hint(str(child))['uri']
    return catalog,discovery,uri,child_uri,directory,workspace,native


def disappear(discovery,directory):
    shutil.rmtree(directory);return discovery.hint(str(directory))


def test_definite_absence_filters_before_pages_without_losing_safety_rows(tmp_path):
    c,d,uri,child,directory,workspace,_=fixture(tmp_path)
    assert disappear(d,directory)['changed']
    record=c.get(uri);assert record['availability']=='read-only' and not record['nativeDeleted'] and not record['productHidden']
    assert not c.list(connectionId='normal',limit=1)['items']
    assert c.descendants(parentUri=uri)['rootKnown'] and c.descendants(parentUri=uri)['items'][0]['uri']==child
    assert c.directory_in_use(path=str(workspace))['inUse']
    assert c.retention_candidates(connectionId='retention',modifiedBefore=253402300799,allowedWorkspaceRoots=[str(workspace)])['candidateCount']==0
    assert c.list(connectionId='historical',includeArchive=True)['items'][0]['uri']==uri


@pytest.mark.parametrize('metadata',['absent','invalid'])
def test_present_directory_bad_metadata_never_means_source_absence(tmp_path,metadata):
    c,d,uri,_,directory,_,_=fixture(tmp_path)
    if metadata=='absent':(directory/'metadata.json').unlink()
    else:(directory/'metadata.json').write_text('{')
    d.hint(str(directory))
    assert c.get(uri)['availability']=='read-only' and c.list(connectionId='still-present')['items'][0]['uri']==uri
    with c.connect() as db:assert db.execute('SELECT count(*) FROM source_presence').fetchone()[0]==0


@pytest.mark.parametrize('action',['upsert','native-visible','product-restore','workspace-recheck','workspace-restore','restart','claim'])
def test_every_projection_path_preserves_observed_absence(tmp_path,action):
    c,d,uri,_,directory,workspace,_=fixture(tmp_path);disappear(d,directory)
    if action=='upsert':c.upsert({**c.get(uri),'availability':'available'})
    elif action=='native-visible':c.set_visibility(uri=uri,nativeSessionId='root',workingDirectory=str(workspace),deleted=False,commandId='restore',evidence='native-completed-receipt')
    elif action=='product-restore':c.project_session_visibility(source='host',commandId='restore',afterRevision=0,throughRevision=1,items=[{'uri':uri,'hidden':False,'revision':1}])
    elif action=='workspace-recheck':c.workspace(str(workspace),availability='missing');c.recheck_workspaces()
    elif action=='workspace-restore':
        c.project_workspaces(source='host',afterRevision=0,throughRevision=1,records=[{'id':c.workspace_id(str(workspace)),'name':'Workspace','path':str(workspace),'hidden':True,'registered':True,'revision':1}])
        c.project_workspaces(source='host',afterRevision=1,throughRevision=2,records=[{'id':c.workspace_id(str(workspace)),'name':'Workspace','path':str(workspace),'hidden':False,'registered':True,'revision':2}])
    elif action=='restart':c=Catalog(c.path)
    else:c.claim_native(record=c.get(uri),commandId='claim',evidence='verified-native-import')
    assert not c.list(connectionId='default')['items'] and c.get(uri) and not c.get(uri)['nativeDeleted']


@pytest.mark.parametrize('exclusion',['none','native','product','workspace','workspace-missing'])
def test_actual_metadata_reappearance_restores_only_presence(tmp_path,exclusion):
    c,d,uri,_,directory,workspace,native=fixture(tmp_path);disappear(d,directory)
    if exclusion=='native':c.set_visibility(uri=uri,nativeSessionId='root',workingDirectory=str(workspace),deleted=True,commandId='delete',evidence='native-completed-receipt')
    elif exclusion=='product':c.project_session_visibility(source='host',commandId='hide',afterRevision=0,throughRevision=1,items=[{'uri':uri,'hidden':True,'revision':1}])
    elif exclusion=='workspace':c.project_workspaces(source='host',afterRevision=0,throughRevision=1,records=[{'id':c.workspace_id(str(workspace)),'name':'Workspace','path':str(workspace),'hidden':True,'registered':True,'revision':1}])
    elif exclusion=='workspace-missing':c.workspace(str(workspace),availability='missing')
    native('root');d.hint(str(directory));row=c.get(uri)
    assert row['availability']=='available' and 'unavailable' not in row.get('reason','')
    assert row['nativeDeleted']==(exclusion=='native') and row['productHidden']==(exclusion=='product')
    assert bool(c.list(connectionId='reappeared')['items'])==(exclusion=='none')
    with c.connect() as db:assert db.execute('SELECT count(*) FROM source_presence').fetchone()[0]==0


@pytest.mark.parametrize('case',['permission','symlink','foreign-root','foreign-engine'])
def test_unknown_unsafe_or_unconfigured_source_never_changes_presence(tmp_path,monkeypatch,case):
    c,d,uri,_,directory,_,_=fixture(tmp_path)
    if case=='permission':
        original=Path.stat
        def denied(path,*a,**k):
            if path==directory:raise PermissionError('fixture inaccessible source')
            return original(path,*a,**k)
        monkeypatch.setattr(Path,'stat',denied)
    elif case=='symlink':
        moved=directory.with_name('moved');directory.rename(moved);directory.symlink_to(moved)
    elif case=='foreign-root':d=Discovery(c,[])
    else:
        d=Discovery(c,d.homes,engine_id='foreign');shutil.rmtree(directory)
    if case=='foreign-engine':assert not d.hint(str(directory))['changed']
    else:
        with pytest.raises((OSError,ValueError)):d.hint(str(directory))
    assert c.list(connectionId='unknown')['items'][0]['uri']==uri and not c.get(uri)['nativeDeleted']
    with c.connect() as db:assert db.execute('SELECT count(*) FROM source_presence').fetchone()[0]==0


def test_directory_replaced_after_enoent_refuses_stale_observation(tmp_path,monkeypatch):
    c,d,uri,_,directory,_,native=fixture(tmp_path);shutil.rmtree(directory)
    original=Path.stat;once=False
    def replacing(path,*a,**k):
        nonlocal once
        try:return original(path,*a,**k)
        except FileNotFoundError:
            if path==directory and not once:once=True;native('root')
            raise
    monkeypatch.setattr(Path,'stat',replacing)
    assert not d.hint(str(directory))['changed']
    assert c.list(connectionId='replaced')['items'][0]['uri']==uri


def test_index_binding_replaced_before_absence_cas_is_not_hidden(tmp_path,monkeypatch):
    c,d,uri,_,directory,_,_=fixture(tmp_path);shutil.rmtree(directory)
    original=c._observe_source_presence
    def rebound(record,**kwargs):
        c.upsert({**record,'storagePath':str(directory.with_name('different'))},source='native:fixture')
        return original(record,**kwargs)
    monkeypatch.setattr(c,'_observe_source_presence',rebound)
    assert not d.hint(str(directory))['changed'] and c.list(connectionId='binding')['items'][0]['uri']==uri


def test_duplicate_delay_reads_current_source_facts_without_replaying_absence(tmp_path):
    c,d,uri,_,directory,_,native=fixture(tmp_path);disappear(d,directory);native('root')
    d.hint(str(directory));d.hint(str(directory))
    assert c.list(connectionId='delayed')['items'][0]['uri']==uri


def test_selected_absence_work_stays_bounded_with_unrelated_history(tmp_path,monkeypatch):
    c,d,uri,_,directory,_,_=fixture(tmp_path)
    with c.connect() as db:
        db.execute("""WITH RECURSIVE ids(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM ids WHERE n<25000)
            INSERT INTO sessions SELECT printf('ahp-session:/unrelated%08d',n),engine,printf('%d',n),workspace,
            parent_uri,kind,title,created_at,modified_at,availability,source,NULL,native_project,
            fingerprint,seen_run,indexed_at,details,visible FROM ids,sessions WHERE uri=?""",(uri,))
    original=c.connect;steps=[0]
    @contextmanager
    def bounded():
        with original() as db:
            db.set_progress_handler(lambda:(steps.__setitem__(0,steps[0]+1) or steps[0]>10000),1);yield db
    c.connect=bounded
    shutil.rmtree(directory)
    def no_open(*a,**k):raise AssertionError('Missing-source observation opened a body')
    monkeypatch.setattr(os,'open',no_open)
    assert d.hint(str(directory))['changed'] and steps[0]<10000


@pytest.mark.parametrize('metadata',['absent','invalid'])
def test_bad_metadata_after_reappearance_cannot_clear_absence(tmp_path,metadata):
    c,d,uri,_,directory,_,native=fixture(tmp_path);disappear(d,directory);native('root')
    if metadata=='absent':(directory/'metadata.json').unlink()
    else:(directory/'metadata.json').write_text('{')
    d.hint(str(directory))
    assert not c.list(connectionId='invalid-reappearance')['items']
    with c.connect() as db:assert db.execute('SELECT count(*) FROM source_presence').fetchone()[0]==1


def test_missing_child_filters_children_but_preserves_safety_relationship(tmp_path):
    c,d,uri,child,directory,_,_=fixture(tmp_path)
    disappear(d,directory.with_name('child'))
    assert not c.list(connectionId='children',parentUri=uri)['items']
    assert c.descendants(parentUri=uri)['items'][0]['uri']==child


def test_permission_change_during_absence_confirmation_rolls_back(tmp_path,monkeypatch):
    c,d,uri,_,directory,_,_=fixture(tmp_path);shutil.rmtree(directory)
    original=Path.stat;calls=0
    def denying(path,*a,**k):
        nonlocal calls
        if path==directory:
            calls+=1
            if calls>1:raise PermissionError('fixture source changed')
        return original(path,*a,**k)
    monkeypatch.setattr(Path,'stat',denying)
    with pytest.raises(PermissionError):d.hint(str(directory))
    assert c.list(connectionId='permission-race')['items'][0]['uri']==uri
    with c.connect() as db:assert db.execute('SELECT count(*) FROM source_presence').fetchone()[0]==0


def test_reappearance_replaced_before_confirmation_stays_unavailable(tmp_path,monkeypatch):
    c,d,uri,_,directory,_,native=fixture(tmp_path);disappear(d,directory);native('root')
    original=c._observe_source_presence
    def replacing(record,**kwargs):
        if not kwargs['missing']:
            directory.rename(directory.with_name('old-root'));native('root')
        return original(record,**kwargs)
    monkeypatch.setattr(c,'_observe_source_presence',replacing)
    d.hint(str(directory))
    assert not c.list(connectionId='restore-race')['items']
    with c.connect() as db:assert db.execute('SELECT count(*) FROM source_presence').fetchone()[0]==1


def host_summary(record):
    # Actual host index() summary intentionally omits storagePath.
    value={key:record[key] for key in ('uri','engineId','nativeSessionId','workingDirectory','title','createdAt','modifiedAt')}
    return {**value,'availability':'available','workspaceAvailability':'present','isArchived':False}


@pytest.mark.parametrize('path_field',['omitted','null','same','different'])
def test_actual_host_summary_cannot_discard_missing_source_binding(tmp_path,path_field):
    c,d,uri,_,directory,_,_=fixture(tmp_path);disappear(d,directory)
    record=host_summary(c.get(uri))
    if path_field=='null':record['storagePath']=None
    elif path_field=='same':record['storagePath']=str(directory)
    elif path_field=='different':record['storagePath']=str(directory.with_name('other'))
    if path_field=='different':
        with pytest.raises(ValueError):c.upsert(record)
    else:c.upsert(record)
    assert c.get(uri)['storagePath']==str(directory) and c.get(uri)['availability']=='read-only'
    assert not c.list(connectionId='summary')['items']
    assert d.hint(str(directory))['changed']


def test_late_host_summary_cannot_replay_absence_or_override_reappearance(tmp_path):
    c,d,uri,_,directory,_,native=fixture(tmp_path);disappear(d,directory)
    stale=host_summary(c.get(uri));native('root');d.hint(str(directory));c.upsert(stale)
    assert c.get(uri)['storagePath']==str(directory) and c.get(uri)['availability']=='available'
    assert c.list(connectionId='late-summary')['items'][0]['uri']==uri
    disappear(d,directory);c.upsert(stale)
    assert not c.list(connectionId='late-missing-summary')['items']


def test_host_summary_during_restoration_preserves_exact_binding(tmp_path,monkeypatch):
    c,d,uri,_,directory,_,native=fixture(tmp_path);disappear(d,directory);native('root')
    original=c._observe_source_presence
    def summary_before_confirmation(record,**kwargs):
        if not kwargs['missing']:c.upsert(host_summary(c.get(uri)))
        return original(record,**kwargs)
    monkeypatch.setattr(c,'_observe_source_presence',summary_before_confirmation)
    d.hint(str(directory))
    assert c.get(uri)['storagePath']==str(directory) and c.list(connectionId='restore-summary')['items'][0]['uri']==uri
