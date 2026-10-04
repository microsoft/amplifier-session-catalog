"""Typed private transient outcomes preserve location claims without replay."""
import json
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import threading

import pytest

from amplifier_session_catalog import Catalog
import amplifier_session_catalog.discovery as module
from amplifier_session_catalog.discovery import Discovery, MetadataReadChanged
from amplifier_session_catalog.hints import HintInbox
from test_hints import native, notice

pytestmark=pytest.mark.skipif(os.name!='posix',reason='Private hint inbox requires POSIX')


def fixture(tmp_path,*,app_homes=()):
    home,directory=native(tmp_path)
    catalog=Catalog(tmp_path/'index.sqlite');discovery=Discovery(catalog,[home],app_homes=app_homes)
    inbox=HintInbox(tmp_path/'hints',discovery);notice(inbox.directory,directory)
    return catalog,discovery,inbox,directory


@pytest.mark.parametrize('error',[PermissionError,FileNotFoundError,MetadataReadChanged])
def test_lost_notice_causal_recovery_after_typed_native_read_failure(tmp_path,monkeypatch,error):
    c,d,inbox,directory=fixture(tmp_path);original=module.read_metadata_json
    def unavailable(*args,**kwargs):raise error('fixture transient read')
    monkeypatch.setattr(module,'read_metadata_json',unavailable)
    assert inbox.drain()['failed']==1 and not c.list(connectionId='unavailable')['items']
    assert len(list(inbox.directory.iterdir()))==1
    inbox.close();monkeypatch.setattr(module,'read_metadata_json',original)
    assert inbox.drain()['processed']==1 and not list(inbox.directory.iterdir())
    assert c.list(connectionId='recovered')['items'][0]['nativeSessionId']=='one'
    inbox.close()


def test_existing_index_read_failure_remains_retryable_at_unchanged_stamps(tmp_path,monkeypatch):
    c,d,inbox,directory=fixture(tmp_path);uri=d.hint(str(directory))['uri']
    # Force a genuine new notification dependency without changing inode/mtime on recovery.
    (directory/'metadata.json').write_text(json.dumps({'working_dir':c.get(uri)['workingDirectory'],'name':'Updated','turn_count':1}))
    original=module.read_metadata_json
    monkeypatch.setattr(module,'read_metadata_json',lambda *a,**k:(_ for _ in ()).throw(PermissionError('fixture read')))
    assert inbox.drain()['failed']==1 and c.get(uri)['title']=='one';inbox.close()
    monkeypatch.setattr(module,'read_metadata_json',original)
    assert inbox.drain()['processed']==1 and c.get(uri)['title']=='Updated' and c.get(uri)['availability']=='available'
    inbox.close()


@pytest.mark.parametrize('case',['optional-naming','native-backup','malformed-native','missing-native'])
def test_accepted_partial_and_deterministic_bad_metadata_are_not_transient(tmp_path,monkeypatch,case):
    c,d,inbox,directory=fixture(tmp_path);original=module.read_metadata_json
    if case=='optional-naming':(directory/'naming.json').write_text('{}')
    elif case=='native-backup':(directory/'metadata.json.backup').write_bytes((directory/'metadata.json').read_bytes())
    elif case=='malformed-native':(directory/'metadata.json').write_text('{')
    else:(directory/'metadata.json').unlink()
    def reader(path,*args,**kwargs):
        if case=='optional-naming' and Path(path).name=='naming.json' or case=='native-backup' and Path(path).name=='metadata.json':raise PermissionError('fixture optional/primary read')
        return original(path,*args,**kwargs)
    monkeypatch.setattr(module,'read_metadata_json',reader)
    assert inbox.drain()['processed']==1 and not list(inbox.directory.iterdir());inbox.close()
    assert bool(c.list(connectionId='accepted')['items'])==(case in {'optional-naming','native-backup'})


def test_transient_authoritative_lifecycle_read_retries_even_with_valid_native(tmp_path,monkeypatch):
    app=tmp_path/'adapter';c,d,inbox,directory=fixture(tmp_path,app_homes=[app]);uri=d.hint(str(directory))['uri']
    marker=app/'sessions'/'one'/'lifecycle.json';marker.parent.mkdir(parents=True)
    marker.write_text(json.dumps({'sessionId':'one','historyCwd':c.get(uri)['workingDirectory'],'deleted':True,'commandId':'native-delete'}))
    original=module.read_metadata_json
    def reader(path,*args,**kwargs):
        if Path(path)==marker:raise PermissionError('fixture marker read')
        return original(path,*args,**kwargs)
    monkeypatch.setattr(module,'read_metadata_json',reader)
    assert inbox.drain()['failed']==1 and not c.get(uri)['nativeDeleted'];inbox.close()
    monkeypatch.setattr(module,'read_metadata_json',original)
    assert inbox.drain()['processed']==1 and c.get(uri)['nativeDeleted']
    assert not c.list(connectionId='native-deleted')['items'];inbox.close()


def test_public_hint_keeps_result_contract_and_no_private_rpc(tmp_path,monkeypatch):
    from amplifier_session_catalog.__main__ import Server
    c,d,inbox,directory=fixture(tmp_path)
    monkeypatch.setattr(module,'read_metadata_json',lambda *a,**k:(_ for _ in ()).throw(PermissionError('fixture read')))
    result=d.hint(str(directory));assert result['changed'] is False and result['issues']
    server=Server(c,d);assert '_hint_for_inbox' not in server.methods
    inbox.close()


def test_persistent_failure_remains_bounded_and_does_not_starve_other_notices(tmp_path,monkeypatch):
    c,d,inbox,directory=fixture(tmp_path);home=d.homes[0]
    for identity in ['two','three','four']:
        other=directory.with_name(identity);other.mkdir();(other/'metadata.json').write_bytes((directory/'metadata.json').read_bytes());(other/'transcript.jsonl').write_bytes(b'opaque');notice(inbox.directory,other)
    original=module.read_metadata_json;reads=[]
    def reader(path,*args,**kwargs):
        reads.append(str(path))
        if Path(path).parent==directory:raise PermissionError('fixture persistent read')
        return original(path,*args,**kwargs)
    monkeypatch.setattr(module,'read_metadata_json',reader)
    totals={'failed':0,'processed':0}
    for _ in range(12):
        before=len(reads);result=inbox.drain(limit=1)
        assert result['examined']<=1 and len(reads)-before<=1
        for key in totals:totals[key]+=result[key]
    assert totals['processed']==3 and totals['failed']>=2
    assert len(list(inbox.directory.iterdir()))==1
    assert len(c.list(connectionId='other-notices')['items'])==3
    inbox.close()


def test_competing_drainer_success_survives_an_older_failed_claim_read(tmp_path,monkeypatch):
    c,d,inbox,directory=fixture(tmp_path)
    uri=d.hint(str(directory))['uri']
    (directory/'metadata.json').write_text(json.dumps({'working_dir':c.get(uri)['workingDirectory'],'name':'Current saved title','turn_count':1}))
    path=next(inbox.directory.iterdir());claim=path.with_name(path.stem+'.processing-'+'a'*32);path.rename(claim)
    other=HintInbox(inbox.directory,d);original=module.read_metadata_json;entered=threading.Event();release=threading.Event()
    def reader(path,*args,**kwargs):
        if threading.current_thread().name.startswith('older'):
            entered.set();assert release.wait(5);raise PermissionError('fixture older read failure')
        return original(path,*args,**kwargs)
    monkeypatch.setattr(module,'read_metadata_json',reader)
    with ThreadPoolExecutor(max_workers=1,thread_name_prefix='older') as pool:
        older=pool.submit(inbox.drain);assert entered.wait(5)
        newer=other.drain();release.set();older_result=older.result(timeout=5)
    assert newer['processed']==1 and older_result['failed']==1
    assert not claim.exists() and c.list(connectionId='competing')['items'][0]['title']=='Current saved title'
    assert c.get(uri)['availability']=='available'
    inbox.close();other.close()


def test_failed_claim_preserves_new_coalesced_writer_notice(tmp_path,monkeypatch):
    c,d,inbox,directory=fixture(tmp_path);original=module.read_metadata_json;once=True
    def reader(path,*args,**kwargs):
        nonlocal once
        if once:
            once=False;notice(inbox.directory,directory);raise PermissionError('fixture read after newer notice')
        return original(path,*args,**kwargs)
    monkeypatch.setattr(module,'read_metadata_json',reader)
    assert inbox.drain(limit=1)['failed']==1 and len(list(inbox.directory.iterdir()))==2;inbox.close()
    assert inbox.drain()['processed']==2 and not list(inbox.directory.iterdir())
    assert len(c.list(connectionId='coalesced')['items'])==1;inbox.close()


def test_source_replacement_after_enoent_retains_notice_then_reads_current_source(tmp_path,monkeypatch):
    import shutil
    c,d,inbox,directory=fixture(tmp_path);uri=d.hint(str(directory))['uri'];metadata=(directory/'metadata.json').read_bytes();shutil.rmtree(directory)
    original=Path.stat;once=True
    def replacing(path,*args,**kwargs):
        nonlocal once
        try:return original(path,*args,**kwargs)
        except FileNotFoundError:
            if path==directory and once:
                once=False;directory.mkdir();(directory/'metadata.json').write_bytes(metadata);(directory/'transcript.jsonl').write_bytes(b'opaque')
            raise
    monkeypatch.setattr(Path,'stat',replacing)
    assert inbox.drain()['failed']==1 and c.get(uri)['availability']=='available';inbox.close()
    assert inbox.drain()['processed']==1 and not list(inbox.directory.iterdir());inbox.close()


def test_failed_restore_confirmation_retains_notice_until_exact_metadata_rechecks(tmp_path,monkeypatch):
    import shutil
    c,d,inbox,directory=fixture(tmp_path);uri=d.hint(str(directory))['uri'];metadata=(directory/'metadata.json').read_bytes();shutil.rmtree(directory);d.hint(str(directory))
    directory.mkdir();(directory/'metadata.json').write_bytes(metadata);(directory/'transcript.jsonl').write_bytes(b'opaque')
    original=c._observe_source_presence;once=True
    def replaced(record,**kwargs):
        nonlocal once
        if not kwargs['missing'] and once:
            once=False;directory.rename(directory.with_name('old'));directory.mkdir();(directory/'metadata.json').write_bytes(metadata);(directory/'transcript.jsonl').write_bytes(b'opaque')
        return original(record,**kwargs)
    monkeypatch.setattr(c,'_observe_source_presence',replaced)
    assert inbox.drain()['failed']==1 and not c.list(connectionId='restore-race')['items'];inbox.close()
    assert inbox.drain()['processed']==1 and c.get(uri)['availability']=='available';inbox.close()


def test_unconfigured_and_malformed_wire_poison_remain_rejected(tmp_path):
    c,d,inbox,directory=fixture(tmp_path)
    outside=tmp_path/'outside';outside.mkdir();notice(inbox.directory,outside)
    malformed=notice(inbox.directory,directory);malformed.write_text('{')
    result=inbox.drain();assert result['rejected']==2 and not result['failed'] and not list(inbox.directory.iterdir());inbox.close()


def test_partial_lifecycle_proofs_do_not_change_visibility_while_another_owner_read_retries(tmp_path,monkeypatch):
    apps=[tmp_path/'adapter-a',tmp_path/'adapter-b'];c,d,inbox,directory=fixture(tmp_path,app_homes=apps);uri=d.hint(str(directory))['uri']
    markers=[]
    for app in apps:
        marker=app/'sessions'/'one'/'lifecycle.json';marker.parent.mkdir(parents=True);marker.write_text(json.dumps({'sessionId':'one','historyCwd':c.get(uri)['workingDirectory'],'deleted':True,'commandId':'native-delete'}));markers.append(marker)
    original=module.read_metadata_json
    def reader(path,*args,**kwargs):
        if Path(path)==markers[1]:raise PermissionError('fixture second authoritative source')
        return original(path,*args,**kwargs)
    monkeypatch.setattr(module,'read_metadata_json',reader)
    assert inbox.drain()['failed']==1 and not c.get(uri)['nativeDeleted'];inbox.close()
    monkeypatch.setattr(module,'read_metadata_json',original)
    assert inbox.drain()['processed']==1 and c.get(uri)['nativeDeleted'];inbox.close()
