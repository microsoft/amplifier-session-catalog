import json
import threading
import uuid
from pathlib import Path

import pytest
from amplifier_session_catalog import Catalog
from amplifier_session_catalog.discovery import Discovery, project_slug, session_uri
from test_managed_locations import managed


def fixture(tmp_path):
    _,root=managed(tmp_path);home=tmp_path/'native';project=project_slug(root['workingDirectory'])
    parent=home/'projects'/project/'sessions'/root['nativeSessionId'];parent.mkdir(parents=True)
    (parent/'metadata.json').write_text(json.dumps({'session_id':root['nativeSessionId'],'working_dir':root['workingDirectory'],'location':root['location'],'turn_count':0}))
    (parent/'transcript.jsonl').write_text('')
    child_id=str(uuid.uuid4());child=parent.parent/child_id;child.mkdir()
    (child/'metadata.json').write_text(json.dumps({'session_id':child_id,'parent_id':root['nativeSessionId'],'working_dir':root['workingDirectory'],'turn_count':1}))
    (child/'transcript.jsonl').write_text('{"role":"user","content":"retained"}\n')
    catalog=Catalog(tmp_path/'catalog.sqlite');discovery=Discovery(catalog,[home])
    return catalog,discovery,root,parent,child,session_uri(home,project,child_id)


@pytest.mark.parametrize('order',['child-first','parent-first','host-first'])
def test_managed_family_binding_is_scan_order_independent_and_preserves_user_state(tmp_path,order):
    catalog,discovery,root,parent,child,child_uri=fixture(tmp_path)
    if order=='host-first':catalog.upsert(root)
    for path in ([child,parent] if order!='parent-first' else [parent,child]):discovery.hint(str(path))
    catalog.project_session_archives(items=[{'uri':child_uri,'isArchived':True}])
    discovery.hint(str(child))
    renamed=catalog.get(child_uri);renamed['title']='Retained child title';catalog.upsert(renamed)
    row=catalog.get(root['uri']);row['title']='User rename';catalog.upsert(row)
    # Parent reindex and unchanged child fingerprint cannot detach the relation.
    discovery.hint(str(parent),force=True)
    family=catalog.descendants(parentUri=root['uri'])
    assert family['completeIndexedCoverage'] and len(family['items'])==1
    assert family['items'][0]['uri']==child_uri and family['items'][0]['parentUri']==root['uri']
    assert catalog.get(child_uri)['isArchived']
    assert catalog.get(child_uri)['title']=='Retained child title'
    # Explicitly distinguish index completeness from scanner completion.
    assert family['discoveryComplete'] is False and family['freshness']['phase']=='not-started'


def test_late_import_mapping_repairs_child_without_rewriting_its_fields(tmp_path):
    catalog,discovery,root,parent,child,child_uri=fixture(tmp_path)
    discovery.hint(str(child));before=catalog.get(child_uri)
    imported={**root,'uri':'ahp-session:/'+str(uuid.uuid4())};imported.pop('location')
    claimed=catalog.claim_native(record=imported,commandId='verified-import',evidence='verified-native-import')
    after=catalog.get(child_uri)
    assert after['parentUri']==claimed['uri']
    assert {k:v for k,v in before.items() if k!='parentUri'}=={k:v for k,v in after.items() if k!='parentUri'}
    assert catalog.descendants(parentUri=claimed['uri'])['completeIndexedCoverage']


def test_ambiguous_parent_never_claims_complete_family(tmp_path):
    catalog,discovery,root,parent,child,_=fixture(tmp_path);discovery.hint(str(child));catalog.upsert(root)
    other={**root,'uri':'ahp-session:/other'};other.pop('location');catalog.upsert(other)
    family=catalog.descendants(parentUri=root['uri'])
    assert not family['completeIndexedCoverage'] and 'native-parent-unresolved' in family['issues']


def test_concurrent_child_discovery_and_host_parent_mapping_are_atomic(tmp_path):
    catalog,discovery,root,_,child,child_uri=fixture(tmp_path);start=threading.Barrier(2);errors=[]
    def run(operation):
        try:start.wait();operation()
        except BaseException as error:errors.append(error)
    threads=[threading.Thread(target=run,args=(lambda:discovery.hint(str(child)),)),threading.Thread(target=run,args=(lambda:catalog.upsert(root),))]
    for thread in threads:thread.start()
    for thread in threads:thread.join()
    assert not errors
    assert catalog.get(child_uri)['parentUri']==root['uri']
    assert len(catalog.descendants(parentUri=root['uri'])['items'])==1


def test_upgrade_refreshes_existing_child_fingerprint_without_transcript_reads(tmp_path,monkeypatch):
    catalog,discovery,root,parent,child,child_uri=fixture(tmp_path)
    discovery.hint(str(child));discovery.hint(str(parent))
    with catalog.connect() as db:
        db.execute('DELETE FROM native_parent_links')
        db.execute('UPDATE sessions SET parent_uri=?,fingerprint=? WHERE uri=?',('ahp-session:/old-fallback','old-scanner-fingerprint',child_uri))
    original=Path.open
    def metadata_only(path,*args,**kwargs):
        assert path.name not in {'transcript.jsonl','events.jsonl'}
        return original(path,*args,**kwargs)
    monkeypatch.setattr(Path,'open',metadata_only)
    assert discovery.hint(str(child))['changed']
    assert catalog.get(child_uri)['parentUri']==root['uri']
