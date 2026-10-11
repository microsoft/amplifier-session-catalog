"""Bounded handle ownership must preserve transaction and publication safety."""
import os
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import pytest
from amplifier_session_catalog import Catalog, InvalidCursor
from amplifier_session_catalog.__main__ import Server
from amplifier_session_catalog.discovery import Discovery
from test_library import row, query
from test_cold_workspace_publication import source


def track(monkeypatch):
    opened=[];closed=[];sql=[];real=sqlite3.connect
    class Tracked(sqlite3.Connection):
        def close(self):
            closed.append((self,threading.get_ident()))
            super().close()
    def connect(*args,**kwargs):
        db=real(*args,**kwargs,factory=Tracked)
        opened.append((db,threading.get_ident()))
        db.set_trace_callback(sql.append)
        return db
    monkeypatch.setattr(sqlite3,'connect',connect)
    return opened,closed,sql


def test_scope_is_lazy_nested_and_closes_on_owner_thread_on_failure(tmp_path,monkeypatch):
    c=Catalog(tmp_path/'index.sqlite');opened,closed,_=track(monkeypatch)
    with c.connection_scope():pass
    assert not opened
    with pytest.raises(RuntimeError):
        with c.connection_scope():
            with c.connect() as first:first.execute('SELECT 1')
            with c.connection_scope():
                with c.connect() as second:assert second is first
            assert not closed
            raise RuntimeError('owner failed')
    assert closed==opened and len(opened)==1
    with pytest.raises(sqlite3.ProgrammingError):first.execute('SELECT 1')
    with c.connect() as later:assert later is not first
    assert closed==opened


def test_nested_connection_cannot_commit_outer_transaction(tmp_path,monkeypatch):
    c=Catalog(tmp_path/'index.sqlite');opened,closed,_=track(monkeypatch)
    with c.connection_scope():
        with pytest.raises(RuntimeError):
            with c.connect() as outer:
                outer.execute("INSERT INTO progress VALUES('outer','private')")
                with c.connect() as inner:
                    assert inner is not outer
                    assert inner.execute("SELECT value FROM progress WHERE key='outer'").fetchone() is None
                assert outer.in_transaction
                raise RuntimeError('rollback outer')
        with c.connect() as after:
            assert after is outer and not after.in_transaction
            assert after.execute("SELECT value FROM progress WHERE key='outer'").fetchone() is None
    assert len(opened)==2 and set(closed)==set(opened)


def test_rollback_events_never_invalidate_later_cursor_and_commit_does(tmp_path):
    w=tmp_path/'workspace';w.mkdir();c=Catalog(tmp_path/'index.sqlite')
    with c.connection_scope():
        for n in range(3):c.upsert(row(w,n))
        first=query(c,w,sort='name',limit=1)
        with pytest.raises(RuntimeError):
            with c.connect() as db:
                db.execute("UPDATE sessions SET title='rolled back' WHERE uri=?",(row(w,0)['uri'],))
                raise RuntimeError('rollback')
        # A later successful read must not publish rolled-back notifications.
        c.progress();query(c,w,sort='name',limit=1,cursor=first['nextCursor'])
        with c.connect() as db:db.execute("UPDATE sessions SET title='committed' WHERE uri=?",(row(w,0)['uri'],))
        with pytest.raises(InvalidCursor):query(c,w,sort='name',limit=1,cursor=first['nextCursor'])
        second=query(c,w,sort='name',limit=1);c.progress()
        query(c,w,sort='name',limit=1,cursor=second['nextCursor'])
        assert c.get(row(w,0)['uri'])['title']=='committed'


def test_real_scan_keeps_individual_commits_and_closes_handle(tmp_path,monkeypatch):
    c=Catalog(tmp_path/'index.sqlite');home=tmp_path/'native'
    for p in range(4):
        w=tmp_path/f'workspace-{p}';w.mkdir()
        for n in range(25):source(home,w,f'root-{n}')
    opened,closed,sql=track(monkeypatch);d=Discovery(c,[home])
    for expected_commits in [104,105]:
        opened.clear();closed.clear();sql.clear()
        assert d.scan()['issues']==0
        assert len(opened)==1 and closed==opened
        assert sum(s.strip().upper().startswith('CREATE TEMP TRIGGER') for s in sql)==12
        assert sum(s.strip().upper()=='COMMIT' for s in sql)==expected_commits
    assert len(c.list_workspaces(connectionId='same-process')['items'])==4


@pytest.mark.parametrize('failure',['body','commit'])
def test_failed_rollback_discards_handle_before_later_public_operation(tmp_path,monkeypatch,failure):
    w=tmp_path/'workspace';w.mkdir();c=Catalog(tmp_path/'index.sqlite')
    for n in range(2):c.upsert(row(w,n))
    cursor=query(c,w,sort='name',limit=1)['nextCursor']
    real=sqlite3.connect;opened=[];closed=[]
    class FailedRollback(sqlite3.Connection):
        fail_rollback=False;fail_commit=False
        def rollback(self):
            if self.fail_rollback:raise sqlite3.OperationalError('rollback failed')
            return super().rollback()
        def commit(self):
            if self.fail_commit:raise sqlite3.OperationalError('commit failed')
            return super().commit()
        def close(self):
            closed.append((self,threading.get_ident()));super().close()
    def connect(*args,**kwargs):
        db=real(*args,**kwargs,factory=FailedRollback)
        opened.append((db,threading.get_ident()));return db
    monkeypatch.setattr(sqlite3,'connect',connect)
    with c.connection_scope():
        with pytest.raises(sqlite3.OperationalError,match='rollback failed'):
            with c.connect() as failed:
                failed.execute("UPDATE sessions SET title='uncommitted' WHERE uri=?",(row(w,0)['uri'],))
                failed.fail_rollback=True
                if failure=='body':raise ValueError('operation failed')
                failed.fail_commit=True
        assert c._connection_owner.scope['connection'] is None
        assert closed==opened
        with pytest.raises(sqlite3.ProgrammingError):failed.execute('SELECT 1')
        c.progress({'phase':'failed','issues':1})
        assert c.get(row(w,0)['uri'])['title']==row(w,0)['title']
        query(c,w,sort='name',limit=1,cursor=cursor)
        with c.connect() as replacement:assert replacement is not failed
        with c.connect() as db:
            db.execute("UPDATE sessions SET title='committed' WHERE uri=?",(row(w,0)['uri'],))
        with pytest.raises(InvalidCursor):query(c,w,sort='name',limit=1,cursor=cursor)
        assert c.get(row(w,0)['uri'])['title']=='committed'
    assert len(closed)==len(opened) and set(closed)==set(opened)


def test_hint_source_reads_are_outside_writer_transaction_and_closes(tmp_path,monkeypatch):
    c=Catalog(tmp_path/'index.sqlite');w=tmp_path/'workspace';w.mkdir();home=tmp_path/'native';directory=source(home,w,'root')
    d=Discovery(c,[home]);opened,closed,_=track(monkeypatch);original=os.open;reads=[]
    def inspect(path,*args,**kwargs):
        path=Path(path)
        if path.name in {'transcript.jsonl','events.jsonl'}:raise AssertionError('history read')
        if path.name=='metadata.json':
            reads.append(path)
            assert all(not db.in_transaction for db,_ in opened)
        return original(path,*args,**kwargs)
    monkeypatch.setattr(os,'open',inspect)
    assert d.hint(str(directory))['changed']
    assert reads and len(opened)==1 and closed==opened


def test_concurrent_native_hint_and_query_own_distinct_handles(tmp_path,monkeypatch):
    c=Catalog(tmp_path/'index.sqlite');w=tmp_path/'workspace';w.mkdir();home=tmp_path/'native';directory=source(home,w,'root');d=Discovery(c,[home])
    uri=d.hint(str(directory))['uri'];before=(directory/'transcript.jsonl').read_bytes()
    opened,closed,_=track(monkeypatch);barrier=threading.Barrier(2)
    def worker(kind):
        with c.connection_scope():
            with c.connect() as db:db.execute('SELECT 1')
            barrier.wait(timeout=5)
            if kind=='hint':return d.hint(str(directory),force=True)
            return query(c,w)
    with ThreadPoolExecutor(max_workers=2) as pool:
        a=pool.submit(worker,'hint');b=pool.submit(worker,'query')
        assert a.result(timeout=10)['uri']==uri and b.result(timeout=10)['items'][0]['uri']==uri
    # Library freshness performs an existing nested read; it remains independent.
    assert len(opened)==3 and len({owner for _,owner in opened})==2
    assert set(opened)==set(closed)
    assert (directory/'transcript.jsonl').read_bytes()==before


def test_request_scope_closes_on_success_and_caught_failure(tmp_path,monkeypatch):
    c=Catalog(tmp_path/'index.sqlite');d=Discovery(c,[]);server=Server(c,d);opened,closed,_=track(monkeypatch)
    def failing():
        with c.connect() as db:db.execute("INSERT INTO progress VALUES('rollback','value')");raise RuntimeError('request failed')
    server.methods['fail']=failing
    request=lambda name:{'jsonrpc':'2.0','id':1,'method':name}
    assert 'result' in server.handle(request('status'))
    assert server.handle(request('fail'))['error']['code']==-32603
    assert opened==closed and len(opened)==2
    with c.connect() as db:assert db.execute("SELECT 1 FROM progress WHERE key='rollback'").fetchone() is None


def test_setup_failure_closes_handle_without_retaining_scope(tmp_path,monkeypatch):
    c=Catalog(tmp_path/'index.sqlite');real=sqlite3.connect;opened=[];closed=[]
    class Failed(sqlite3.Connection):
        def create_function(self,*args,**kwargs):raise RuntimeError('setup failed')
        def close(self):closed.append(self);super().close()
    def connect(*args,**kwargs):
        db=real(*args,**kwargs,factory=Failed);opened.append(db);return db
    monkeypatch.setattr(sqlite3,'connect',connect)
    with c.connection_scope():
        with pytest.raises(RuntimeError,match='setup failed'):
            with c.connect():pass
        assert c._connection_owner.scope['connection'] is None
    assert opened==closed
    assert not hasattr(c._connection_owner,'scope')


def test_background_scan_shutdown_closes_on_scanner_thread(tmp_path,monkeypatch):
    c=Catalog(tmp_path/'index.sqlite');w=tmp_path/'workspace';w.mkdir();home=tmp_path/'native';source(home,w,'root')
    d=Discovery(c,[home]);opened,closed,_=track(monkeypatch);entered=threading.Event();original=d.hint
    def paused(*args,**kwargs):
        entered.set()
        assert d.stop.wait(timeout=5)
        return original(*args,**kwargs)
    monkeypatch.setattr(d,'hint',paused)
    d.start();assert entered.wait(timeout=5)
    d.close()
    assert not d._thread.is_alive()
    assert len(opened)==1 and closed==opened
    assert opened[0][1]!=threading.get_ident()
