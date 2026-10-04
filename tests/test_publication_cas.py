"""Selected publication compares indexed state and current bounded source facts."""
from contextlib import contextmanager
import json
import os
from pathlib import Path
import shutil
import threading
from concurrent.futures import ThreadPoolExecutor
from amplifier_session_catalog.hints import HintInbox
import amplifier_session_catalog.discovery as module

import pytest

from amplifier_session_catalog.catalog import StalePublication
from amplifier_session_catalog.discovery import Discovery
from test_hint_retry import fixture


def change_title(directory,title):
    path=directory/'metadata.json';value=json.loads(path.read_text());value['name']=title;path.write_text(json.dumps(value))


def test_selected_row_cas_refuses_a_newer_host_projection(tmp_path):
    c,d,inbox,directory=fixture(tmp_path);uri=d.hint(str(directory))['uri'];snapshot=c._source_snapshot(directory);old=c.get(uri)
    c.upsert({**old,'title':'Newer host summary'})
    with pytest.raises(StalePublication):c._publish_discovery({**old,'title':'Older native view'},snapshot=snapshot,confirm=lambda:True,source='native:fixture')
    assert c.get(uri)['title']=='Newer host summary';inbox.close()


def test_ctime_readset_refuses_rewrite_even_when_inode_size_and_mtime_match(tmp_path,monkeypatch):
    c,d,inbox,directory=fixture(tmp_path);uri=d.hint(str(directory))['uri'];original=c._publish_discovery;once=True
    def rewrite(record,**kwargs):
        nonlocal once
        if once:
            once=False;path=directory/'metadata.json';before=path.stat();change_title(directory,'two');os.utime(path,ns=(before.st_atime_ns,before.st_mtime_ns));after=path.stat()
            assert (before.st_ino,before.st_size,before.st_mtime_ns)==(after.st_ino,after.st_size,after.st_mtime_ns)
            assert before.st_ctime_ns!=after.st_ctime_ns
        return original(record,**kwargs)
    monkeypatch.setattr(c,'_publish_discovery',rewrite)
    assert not d.hint(str(directory),force=True)['changed'] and c.get(uri)['title']=='one'
    assert d.hint(str(directory))['changed'] and c.get(uri)['title']=='two';inbox.close()


def test_current_lifecycle_readset_is_confirmed_before_visibility_publication(tmp_path,monkeypatch):
    app=tmp_path/'adapter';c,d,inbox,directory=fixture(tmp_path,app_homes=[app]);uri=d.hint(str(directory))['uri']
    marker=app/'sessions'/'one'/'lifecycle.json';marker.parent.mkdir(parents=True)
    value={'sessionId':'one','historyCwd':c.get(uri)['workingDirectory'],'deleted':False,'commandId':'old'};marker.write_text(json.dumps(value))
    original=c._publish_discovery;once=True
    def newer(record,**kwargs):
        nonlocal once
        if once:once=False;marker.write_text(json.dumps({**value,'deleted':True,'commandId':'new'}))
        return original(record,**kwargs)
    monkeypatch.setattr(c,'_publish_discovery',newer)
    assert not d.hint(str(directory))['changed'] and not c.get(uri)['nativeDeleted']
    assert d.hint(str(directory))['changed'] and c.get(uri)['nativeDeleted'];inbox.close()


def test_native_visibility_change_is_part_of_cas_even_when_visible_stays_zero(tmp_path,monkeypatch):
    c,d,inbox,directory=fixture(tmp_path);uri=d.hint(str(directory))['uri'];workspace=c.get(uri)['workingDirectory']
    c.project_session_visibility(source='host',commandId='hide',afterRevision=0,throughRevision=1,items=[{'uri':uri,'hidden':True,'revision':1}])
    original=c._publish_discovery
    def receipt(record,**kwargs):
        c.set_visibility(uri=uri,nativeSessionId='one',workingDirectory=workspace,deleted=True,commandId='native-receipt',evidence='native-completed-receipt')
        return original(record,**kwargs)
    monkeypatch.setattr(c,'_publish_discovery',receipt)
    assert not d.hint(str(directory),force=True)['changed']
    assert c.get(uri)['nativeDeleted'] and c.get(uri)['productHidden'];inbox.close()


def test_absence_cas_retains_claim_and_preserves_newer_summary_until_fresh_observation(tmp_path,monkeypatch):
    c,d,inbox,directory=fixture(tmp_path);uri=d.hint(str(directory))['uri'];shutil.rmtree(directory)
    original=c._observe_source_presence;once=True
    def summary(record,**kwargs):
        nonlocal once
        if once:once=False;c.upsert({**c.get(uri),'title':'Newer retained title'})
        return original(record,**kwargs)
    monkeypatch.setattr(c,'_observe_source_presence',summary)
    assert inbox.drain()['failed']==1 and c.get(uri)['title']=='Newer retained title';inbox.close()
    assert inbox.drain()['processed']==1 and not c.list(connectionId='missing')['items']
    assert c.get(uri)['title']=='Newer retained title' and not c.get(uri)['nativeDeleted'];inbox.close()


def test_retry_after_shared_claim_ack_reads_fresh_current_metadata(tmp_path,monkeypatch):
    c,d,inbox,directory=fixture(tmp_path);uri=d.hint(str(directory))['uri'];change_title(directory,'Newer current title')
    original=c._publish_discovery;once=True
    def earlier_ack(record,**kwargs):
        nonlocal once
        if once:
            once=False;c.upsert({**c.get(uri),'title':'Earlier completed title'},source='native:fixture')
            # Earlier successful drainer acknowledged the immutable shared claim.
            for path in inbox.directory.iterdir():path.unlink()
        return original(record,**kwargs)
    monkeypatch.setattr(c,'_publish_discovery',earlier_ack)
    assert inbox.drain()['processed']==1 and c.get(uri)['title']=='Newer current title'
    assert not list(inbox.directory.iterdir());inbox.close()


def test_persistent_publication_conflicts_have_two_attempts_and_keep_claim(tmp_path,monkeypatch):
    c,d,inbox,directory=fixture(tmp_path);uri=d.hint(str(directory))['uri'];change_title(directory,'Native current');d.metadata_reads=0
    original=c._publish_discovery;calls=0
    def conflict(record,**kwargs):
        nonlocal calls
        calls+=1;c.upsert({**c.get(uri),'title':'Newer host '+str(calls)})
        return original(record,**kwargs)
    monkeypatch.setattr(c,'_publish_discovery',conflict)
    assert inbox.drain(limit=1)['failed']==1 and calls==2 and d.metadata_reads==2
    assert len(list(inbox.directory.iterdir()))==1;inbox.close()


def test_atomic_restoration_never_clears_absence_before_current_publication(tmp_path,monkeypatch):
    c,d,inbox,directory=fixture(tmp_path);uri=d.hint(str(directory))['uri'];metadata=(directory/'metadata.json').read_bytes();shutil.rmtree(directory);d.hint(str(directory))
    directory.mkdir();(directory/'metadata.json').write_bytes(metadata);(directory/'transcript.jsonl').write_bytes(b'opaque')
    original=c._publish_discovery
    def conflicting(record,**kwargs):
        c.upsert({**c.get(uri),'title':'Newest retained metadata'})
        return original(record,**kwargs)
    monkeypatch.setattr(c,'_publish_discovery',conflicting)
    assert not d.hint(str(directory))['changed'] and not c.list(connectionId='restore-refusal')['items']
    with c.connect() as db:assert db.execute('SELECT count(*) FROM source_presence').fetchone()[0]==1
    monkeypatch.setattr(c,'_publish_discovery',original)
    assert d.hint(str(directory))['changed'] and c.get(uri)['availability']=='available'
    with c.connect() as db:assert db.execute('SELECT count(*) FROM source_presence').fetchone()[0]==0
    inbox.close()


def test_unchanged_visible_hint_detects_same_mtime_content_change(tmp_path):
    c,d,inbox,directory=fixture(tmp_path);uri=d.hint(str(directory))['uri'];path=directory/'metadata.json';before=path.stat();change_title(directory,'two');os.utime(path,ns=(before.st_atime_ns,before.st_mtime_ns))
    assert d.hint(str(directory))['changed'] and c.get(uri)['title']=='two';inbox.close()


def test_publication_selected_cost_is_bounded_with_25000_unrelated_rows(tmp_path,monkeypatch):
    c,d,inbox,directory=fixture(tmp_path);uri=d.hint(str(directory))['uri']
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
    c.connect=bounded;change_title(directory,'Current selected title');native_open=os.open;opens=[]
    def guarded(path,*args,**kwargs):
        assert Path(path).name not in {'transcript.jsonl','transcript.jsonl.backup','events.jsonl'}
        opens.append(str(path));return native_open(path,*args,**kwargs)
    monkeypatch.setattr(os,'open',guarded)
    assert d.hint(str(directory))['changed'] and steps[0]<10000
    assert len(opens)==1 and c.get(uri)['title']=='Current selected title';inbox.close()


def test_excess_adapter_sources_refuse_before_opening_metadata(tmp_path,monkeypatch):
    c,d,inbox,directory=fixture(tmp_path);d=Discovery(c,d.homes,app_homes=[tmp_path/str(i) for i in range(17)])
    monkeypatch.setattr(os,'open',lambda *a,**k:(_ for _ in ()).throw(AssertionError('Unbounded source opened')))
    with pytest.raises(ValueError,match='adapter limit'):d.hint(str(directory))
    inbox.close()


def test_cas_and_snapshot_are_not_new_public_wire_fields(tmp_path):
    from amplifier_session_catalog.__main__ import Server
    c,d,inbox,directory=fixture(tmp_path);server=Server(c,d)
    assert not {'_source_snapshot','_publish_discovery','_upsert'}.intersection(server.methods)
    result=server.handle({'jsonrpc':'2.0','id':1,'method':'upsert','params':{'record':{},'_publication':{}}})
    assert result['error']['code']==-32602;inbox.close()


def test_older_successful_lifecycle_cannot_rewind_newer_projection(tmp_path,monkeypatch):
    app=tmp_path/'adapter';c,d,inbox,directory=fixture(tmp_path,app_homes=[app]);uri=d.hint(str(directory))['uri']
    metadata=directory/'metadata.json'
    metadata.write_text(json.dumps({'working_dir':c.get(uri)['workingDirectory'],'name':'Older title','turn_count':1}))
    marker=app/'sessions'/'one'/'lifecycle.json';marker.parent.mkdir(parents=True)
    marker.write_text(json.dumps({'sessionId':'one','historyCwd':c.get(uri)['workingDirectory'],'deleted':False,'commandId':'native-visible'}))
    p=next(inbox.directory.iterdir());claim=p.with_name(p.stem+'.processing-'+'b'*32);p.rename(claim)
    other=HintInbox(inbox.directory,d);original=module.read_metadata_json;entered=threading.Event();release=threading.Event()
    def reader(path,*args,**kwargs):
        if Path(path)==marker and threading.current_thread().name.startswith('older'):
            entered.set();assert release.wait(5);return original(path,*args,**kwargs)
        return original(path,*args,**kwargs)
    monkeypatch.setattr(module,'read_metadata_json',reader)
    with ThreadPoolExecutor(max_workers=1,thread_name_prefix='older') as pool:
        older=pool.submit(inbox.drain);assert entered.wait(5)
        metadata.write_text(json.dumps({'working_dir':c.get(uri)['workingDirectory'],'name':'Newer successful title','turn_count':1}))
        newer=other.drain();before=c.get(uri)['title'];release.set();old_result=older.result(timeout=5)
    report={'newer':newer,'older':old_result,'beforeOlderCompletion':before,'afterOlderCompletion':c.get(uri)['title'],'remainingClaims':len(list(inbox.directory.iterdir()))}
    inbox.close();other.close()
    assert c.get(uri)['title']=='Newer successful title',report
