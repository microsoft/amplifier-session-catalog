"""Derived index continuity is a fact, never Host readiness authority."""
import json
import shutil
import sqlite3
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest
from amplifier_session_catalog import Catalog
from amplifier_session_catalog.__main__ import Server
from amplifier_session_catalog.discovery import Discovery
from test_catalog import record


def status(c,source='host'):
    return c.session_visibility_projection_status(source=source)


def test_retained_zero_during_refresh_has_identity_without_checkpoint(tmp_path):
    c=Catalog(tmp_path/'index.sqlite');first=status(c)
    assert str(uuid.UUID(first['indexIncarnation']))==first['indexIncarnation']
    assert first==dict(source='host',revision=0,indexIncarnation=first['indexIncarnation'],projectionPresent=False)
    c.progress({'phase':'running','issues':0})
    assert status(c)==first and status(Catalog(c.path))==first
    with c.connect() as db:
        assert db.execute('SELECT count(*) FROM product_visibility_projection').fetchone()[0]==0
        assert db.execute('SELECT count(*) FROM product_visibility_commands').fetchone()[0]==0
    reply=Server(c,Discovery(c,[])).handle({'jsonrpc':'2.0','id':1,'method':'sessionVisibilityProjectionStatus','params':{'source':'host'}})
    assert reply['result']==first


def test_projection_presence_is_source_specific_and_loss_does_not_reidentify(tmp_path):
    c=Catalog(tmp_path/'index.sqlite');identity=status(c)['indexIncarnation']
    row=record(tmp_path,1);c.upsert(row)
    receipt=c.project_session_visibility(source='host',commandId='hide',afterRevision=0,throughRevision=1,
        items=[{'uri':row['uri'],'hidden':True,'revision':1}])
    c.progress({'phase':'running','issues':0})
    expected=dict(source='host',revision=1,indexIncarnation=identity,projectionPresent=True)
    assert status(c)==expected and status(Catalog(c.path))==expected
    assert status(c,'foreign')==dict(source='foreign',revision=0,indexIncarnation=identity,projectionPresent=False)
    with c.connect() as db:db.execute("DELETE FROM product_visibility_projection WHERE source='host'")
    assert status(c)==dict(source='host',revision=0,indexIncarnation=identity,projectionPresent=False)
    assert c.session_visibility_projection_receipt(source='host',commandId='hide')==receipt
    assert c.get(row['uri'])['productHidden']


def test_recreation_changes_identity_and_status_reads_actual_database(tmp_path):
    c=Catalog(tmp_path/'index.sqlite');first=status(c)
    c.path.unlink()
    recreated=Catalog(c.path)
    assert status(recreated)['indexIncarnation']!=first['indexIncarnation']
    # No construction-time identity cache in the older Catalog object.
    assert status(c)==status(recreated)


def test_copy_retains_identity_and_is_not_rollback_detection(tmp_path):
    c=Catalog(tmp_path/'index.sqlite');first=status(c)
    copied=tmp_path/'copy.sqlite';shutil.copy2(c.path,copied)
    assert status(Catalog(copied))==first


def legacy(c):
    row=record(c.path.parent,1);c.upsert(row)
    c.project_session_visibility(source='host',commandId='hide',afterRevision=0,throughRevision=1,
        items=[{'uri':row['uri'],'hidden':True,'revision':1}])
    c.progress({'phase':'complete','issues':0})
    with c.connect() as db:
        db.execute("DELETE FROM progress WHERE key='index-incarnation'")
        assert db.execute('PRAGMA user_version').fetchone()[0]==1
    return row


def contents(c):
    with c.connect() as db:
        names=[r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
        return {n:sorted(tuple(r) for r in db.execute('SELECT * FROM '+n) if not (n=='progress' and r[0]=='index-incarnation')) for n in names}


def test_legacy_migration_only_adds_identity_and_preserves_authority_rows(tmp_path):
    c=Catalog(tmp_path/'index.sqlite');legacy(c);before=contents(c)
    with pytest.raises(ValueError,match='incarnation'):status(c)
    upgraded=Catalog(c.path);identity=status(upgraded)['indexIncarnation']
    assert contents(upgraded)==before
    assert status(upgraded)['projectionPresent'] and status(upgraded)['revision']==1
    assert status(Catalog(c.path))['indexIncarnation']==identity
    assert upgraded.progress()=={'phase':'complete','issues':0}
    # No Host acknowledgment or readiness fact is stored by migration.
    with upgraded.connect() as db:
        assert {r[0] for r in db.execute('SELECT key FROM progress')}=={'discovery','index-incarnation'}


def test_concurrent_initializers_acknowledge_one_persisted_identity(tmp_path):
    path=tmp_path/'index.sqlite';Catalog(path)
    with sqlite3.connect(path) as db:db.execute("DELETE FROM progress WHERE key='index-incarnation'")
    start=threading.Barrier(2)
    def initialize():
        start.wait(timeout=5)
        return status(Catalog(path))['indexIncarnation']
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures=[pool.submit(initialize) for _ in range(2)]
        identities=[f.result(timeout=15) for f in futures]
    assert identities[0]==identities[1]==status(Catalog(path))['indexIncarnation']


def test_failed_identity_initialization_rolls_back_before_retry(tmp_path,monkeypatch):
    c=Catalog(tmp_path/'index.sqlite');legacy(c);before=contents(c);real=sqlite3.connect
    class FailInsert(sqlite3.Connection):
        def execute(self,sql,*args):
            result=super().execute(sql,*args)
            if "INSERT OR IGNORE INTO progress VALUES('index-incarnation'" in sql:
                assert self.in_transaction
                raise sqlite3.OperationalError('identity insert failed')
            return result
    with monkeypatch.context() as patch:
        patch.setattr(sqlite3,'connect',lambda *a,**kw:real(*a,**kw,factory=FailInsert))
        with pytest.raises(sqlite3.OperationalError,match='identity insert failed'):Catalog(c.path)
    with real(c.path) as db:
        assert db.execute("SELECT 1 FROM progress WHERE key='index-incarnation'").fetchone() is None
    assert contents(c)==before
    assert status(Catalog(c.path))['projectionPresent']


def test_status_identity_and_checkpoint_share_a_snapshot(tmp_path,monkeypatch):
    c=Catalog(tmp_path/'index.sqlite');old=status(c)['indexIncarnation'];new=str(uuid.uuid4())
    c.project_session_visibility(source='host',commandId='first',afterRevision=0,throughRevision=1,items=[])
    real=sqlite3.connect;changed=[]
    class PauseAfterRead(sqlite3.Connection):
        def execute(self,sql,*args):
            cursor=super().execute(sql,*args)
            if 'FROM progress p' in sql and not changed:
                # Actual concurrent WAL commit after the status read starts.
                with real(c.path) as writer:
                    writer.execute('BEGIN IMMEDIATE')
                    writer.execute("UPDATE progress SET value=? WHERE key='index-incarnation'",(json.dumps(new),))
                    writer.execute("UPDATE product_visibility_projection SET revision=2 WHERE source='host'")
                changed.append(True)
            return cursor
    monkeypatch.setattr(sqlite3,'connect',lambda *a,**kw:real(*a,**kw,factory=PauseAfterRead))
    assert status(c)==dict(source='host',revision=1,indexIncarnation=old,projectionPresent=True)
    assert changed and status(c)==dict(source='host',revision=2,indexIncarnation=new,projectionPresent=True)


@pytest.mark.parametrize('value',['not-json',json.dumps('not-a-uuid'),json.dumps(123)])
def test_corrupt_identity_is_refused_not_repaired_by_status(tmp_path,value):
    c=Catalog(tmp_path/'index.sqlite')
    with c.connect() as db:db.execute("UPDATE progress SET value=? WHERE key='index-incarnation'",(value,))
    with pytest.raises(ValueError,match='incarnation'):status(c)
    with c.connect() as db:assert db.execute("SELECT value FROM progress WHERE key='index-incarnation'").fetchone()[0]==value
