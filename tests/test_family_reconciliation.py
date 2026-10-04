import json
import os
from pathlib import Path
import time

import pytest

from amplifier_session_catalog import Catalog
from amplifier_session_catalog.__main__ import Server
from amplifier_session_catalog.discovery import Discovery, project_slug, iso


def fixture(tmp_path):
    workspace=tmp_path/'workspace';workspace.mkdir();home=tmp_path/'native'
    sessions=home/'projects'/project_slug(str(workspace))/'sessions';sessions.mkdir(parents=True)
    catalog=Catalog(tmp_path/'index.sqlite');uri='ahp-session:/host-retained-parent'
    catalog.upsert({'uri':uri,'engineId':'amplifier','nativeSessionId':'parent','workingDirectory':str(workspace),'title':'Retained'})
    discovery=Discovery(catalog,[home])
    def native(identity,parent=None):
        directory=sessions/identity;directory.mkdir()
        (directory/'metadata.json').write_text(json.dumps({'session_id':identity,'working_dir':str(workspace),'parent_id':parent,'turn_count':1}))
        (directory/'transcript.jsonl').write_text('opaque retained history\n')
        (directory/'events.jsonl').write_text('opaque retained events\n')
        return directory
    native('parent')
    catalog.progress({'phase':'complete','issues':0,'completedAt':iso(time.time()-20000)})
    return catalog,discovery,uri,sessions,native,workspace,home


def test_stale_global_failure_repaired_by_bounded_selected_native_metadata(tmp_path,monkeypatch):
    catalog,discovery,uri,sessions,native,workspace,home=fixture(tmp_path);native('child','parent')
    elsewhere=home/'projects'/'never-open';elsewhere.mkdir()
    before={p:p.read_bytes() for p in sessions.rglob('*') if p.is_file()}
    old=catalog.descendants(parentUri=uri)
    assert old['completeIndexedCoverage'] and old['items']==[]
    assert time.time()-__import__('datetime').datetime.fromisoformat(old['freshness']['completedAt'].replace('Z','+00:00')).timestamp()>600
    original=os.open;reads=[]
    def metadata_only(path,*args,**kwargs):
        assert Path(path).name not in {'transcript.jsonl','events.jsonl'}
        assert 'never-open' not in str(path)
        reads.append(str(path));return original(path,*args,**kwargs)
    monkeypatch.setattr(os,'open',metadata_only)
    monkeypatch.setattr(discovery,'scan',lambda:(_ for _ in ()).throw(AssertionError('global scan')))
    result=discovery.reconcile_family(parentUri=uri)
    proof=result['familyDiscovery']
    assert proof['phase']=='complete' and not proof['issues']
    assert proof['scope']=='selected-native-project-namespaces' and proof['metadataReads']==2
    assert proof['indexRevision']==result['revision']==catalog.descendants(parentUri=uri)['revision']
    assert result['freshness']==old['freshness'] and result['root']['uri']==uri
    assert len(result['items'])==1 and result['items'][0]['parentUri']==uri
    assert not proof['externalWriterCoverage'] and not proof['nativeOwnershipVerified']
    assert {p:p.read_bytes() for p in before}==before
    assert reads and all(Path(p).name=='metadata.json' for p in reads)
    assert catalog.list(connectionId='a')['items'][0]['uri']==uri


def test_new_descendant_after_repair_changes_exact_index_revision(tmp_path):
    catalog,discovery,uri,_,native,_,_=fixture(tmp_path)
    first=discovery.reconcile_family(parentUri=uri)
    child=native('late','parent');discovery.hint(str(child))
    assert catalog.descendants(parentUri=uri)['revision']!=first['familyDiscovery']['indexRevision']
    second=discovery.reconcile_family(parentUri=uri)
    assert second['familyDiscovery']['phase']=='complete' and len(second['items'])==1
    restart=Catalog(catalog.path)
    assert restart.descendants(parentUri=uri)['revision']!=second['revision']
    assert Discovery(restart,discovery.homes).reconcile_family(parentUri=uri)['familyDiscovery']['phase']=='complete'


def test_workspace_exclusion_does_not_exclude_family_safety_facts(tmp_path):
    catalog,discovery,uri,_,native,workspace,_=fixture(tmp_path);native('child','parent')
    result=discovery.reconcile_family(parentUri=uri);child=result['items'][0]['uri']
    catalog.project_session_visibility(source='host',commandId='hide',afterRevision=0,throughRevision=1,items=[{'uri':child,'hidden':True,'revision':1}])
    catalog.workspace(str(workspace),availability='unknown')
    assert not catalog.list(connectionId='a')['items']
    assert len(catalog.descendants(parentUri=uri)['items'])==1
    assert discovery.reconcile_family(parentUri=uri)['familyDiscovery']['phase']=='complete'
    assert catalog.get(child)['productHidden']


def test_inventory_entry_and_byte_bounds_refuse_before_metadata_body_io(tmp_path,monkeypatch):
    catalog,discovery,uri,sessions,native,_,_=fixture(tmp_path);native('child','parent')
    monkeypatch.setattr(discovery,'hint',lambda *_a,**_k:(_ for _ in ()).throw(AssertionError('metadata body I/O before admission')))
    limited=discovery.reconcile_family(parentUri=uri,maxEntries=1)
    assert limited['familyDiscovery']['phase']=='refused'
    assert 'namespace-entry-limit' in limited['familyDiscovery']['issues']
    with (sessions/'parent'/'metadata.json').open('wb') as stream:stream.truncate(8*1024*1024+1)
    oversized=discovery.reconcile_family(parentUri=uri)
    assert oversized['familyDiscovery']['phase']=='refused'
    assert 'metadata-byte-limit' in oversized['familyDiscovery']['issues']


def test_fifo_and_changed_namespace_remain_refused(tmp_path,monkeypatch):
    _,discovery,uri,sessions,native,_,_=fixture(tmp_path)
    (sessions/'parent'/'metadata.json').unlink();os.mkfifo(sessions/'parent'/'metadata.json')
    assert 'source-special-file' in discovery.reconcile_family(parentUri=uri)['familyDiscovery']['issues']
    (sessions/'parent'/'metadata.json').unlink();(sessions/'parent'/'metadata.json').write_text(json.dumps({'working_dir':str(sessions.parents[3]),'turn_count':1}))
    # Correct fixture bytes, then add a late child after the first inventory.
    workspace=discovery.catalog.get(uri)['workingDirectory']
    (sessions/'parent'/'metadata.json').write_text(json.dumps({'working_dir':workspace,'parent_id':None,'turn_count':1}))
    original=discovery.hint;once=[False]
    def racing(*args,**kwargs):
        result=original(*args,**kwargs)
        if not once[0]:once[0]=True;native('late','parent')
        return result
    monkeypatch.setattr(discovery,'hint',racing)
    result=discovery.reconcile_family(parentUri=uri)
    assert result['familyDiscovery']['phase']=='refused'
    assert result['familyDiscovery']['issues']


def test_missing_ambiguous_engine_busy_and_unindexed_child_refuse(tmp_path):
    catalog,discovery,uri,sessions,native,workspace,home=fixture(tmp_path)
    assert discovery.reconcile_family(parentUri='ahp-session:/missing')['familyDiscovery']['phase']=='refused'
    foreign=Discovery(catalog,[home],engine_id='other')
    assert 'source-engine-unconfigured' in foreign.reconcile_family(parentUri=uri)['familyDiscovery']['issues']
    discovery._lock.acquire()
    try:assert discovery.reconcile_family(parentUri=uri)['familyDiscovery']['issues']==['discovery-busy']
    finally:discovery._lock.release()
    other=tmp_path/'other';copy=other/'projects'/project_slug(str(workspace))/'sessions'/'parent';copy.mkdir(parents=True)
    assert 'source-missing-or-ambiguous' in Discovery(catalog,[home,other]).reconcile_family(parentUri=uri)['familyDiscovery']['issues']
    (native('empty','parent')/'metadata.json').write_text('{}')
    assert discovery.reconcile_family(parentUri=uri)['familyDiscovery']['phase']=='refused'


def test_rpc_bounds_and_family_overflow_are_explicit(tmp_path):
    catalog,discovery,uri,_,native,_,_=fixture(tmp_path)
    for n in range(3):native(f'child{n}','parent')
    result=discovery.reconcile_family(parentUri=uri,limit=1)
    assert result['truncated'] and not result['completeIndexedCoverage'] and result['familyDiscovery']['phase']=='refused'
    server=Server(catalog,discovery)
    request={'jsonrpc':'2.0','id':1,'method':'reconcileFamily','params':{'parentUri':uri}}
    assert server.handle(request)['result']['familyDiscovery']['phase']=='complete'
    for value in (0,513,True):
        request['params']['maxEntries']=value
        assert server.handle(request)['error']['code']==-32602


def test_unhinted_child_after_repair_is_refused_without_index_or_body_reads(tmp_path,monkeypatch):
    catalog,discovery,uri,_,native,_,_=fixture(tmp_path)
    proof=discovery.reconcile_family(parentUri=uri)['familyDiscovery']
    assert discovery.verify_family(parentUri=uri,verificationToken=proof['verificationToken'])['valid']
    native('unknown-new-child','parent')
    # Pure index cannot detect this physical race; public verification must do so.
    assert catalog.descendants(parentUri=uri)['revision']==proof['indexRevision']
    monkeypatch.setattr(discovery,'hint',lambda *_a,**_k:(_ for _ in ()).throw(AssertionError('verification mutated index')))
    original=os.open
    def no_source_read(path,*a,**k):
        assert Path(path).name not in {'metadata.json','transcript.jsonl','events.jsonl'}
        return original(path,*a,**k)
    monkeypatch.setattr(os,'open',no_source_read)
    result=discovery.verify_family(parentUri=uri,verificationToken=proof['verificationToken'])
    assert not result['valid'] and result['issues']
    assert catalog.descendants(parentUri=uri)['revision']==proof['indexRevision']


def test_physical_metadata_change_expiry_restart_and_token_target_refuse(tmp_path):
    catalog,discovery,uri,sessions,_,_,_=fixture(tmp_path)
    proof=discovery.reconcile_family(parentUri=uri)['familyDiscovery'];token=proof['verificationToken']
    assert not discovery.verify_family(parentUri='ahp-session:/other',verificationToken=token)['valid']
    assert not Discovery(Catalog(catalog.path),discovery.homes).verify_family(parentUri=uri,verificationToken=token)['valid']
    (sessions/'parent'/'metadata.json').write_text('{}')
    assert not discovery.verify_family(parentUri=uri,verificationToken=token)['valid']
    discovery._family_proofs[token]['expires']=time.monotonic()-1
    assert 'unknown-expired-or-mismatched-family-proof' in discovery.verify_family(parentUri=uri,verificationToken=token)['issues']
