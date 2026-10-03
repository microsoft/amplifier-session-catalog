"""Product presentation exclusion cannot manufacture native deletion authority."""
from contextlib import contextmanager
from pathlib import Path
import threading

import pytest

from amplifier_session_catalog import Catalog, InvalidCursor
from amplifier_session_catalog.__main__ import Server
from amplifier_session_catalog.discovery import Discovery
from test_catalog import record, native_session


def project(catalog, items, *, after=0, through=1, command='reset', source='host'):
    return catalog.project_session_visibility(source=source, commandId=command,
        afterRevision=after, throughRevision=through,
        items=[{'uri':uri,'hidden':hidden,'revision':through} for uri,hidden in items])


def test_product_hidden_is_independent_and_survives_native_reindex(tmp_path, monkeypatch):
    workspace=tmp_path/'workspace';workspace.mkdir();home=tmp_path/'native'
    native=native_session(home,workspace,'root')
    before={p:p.read_bytes() for p in native.iterdir()}
    catalog=Catalog(tmp_path/'index.sqlite');discovery=Discovery(catalog,[home])
    uri=discovery.hint(str(native))['uri'];project(catalog,[(uri,True)])
    assert catalog.get(uri)['productHidden'] and not catalog.get(uri)['nativeDeleted']
    assert not catalog.list(connectionId='archive',includeArchive=True)['items']
    with catalog.connect() as db:assert db.execute('SELECT count(*) FROM native_visibility').fetchone()[0]==0
    discovery.hint(str(native),force=True)
    catalog.workspace(str(workspace),availability='missing');catalog.workspace(str(workspace),availability='present')
    catalog.set_visibility(uri=uri,nativeSessionId='root',workingDirectory=str(workspace),deleted=False,commandId='native-restore',evidence='native-completed-receipt')
    assert not catalog.list(connectionId='a')['items']
    assert catalog.directory_in_use(path=str(workspace))['inUse']
    assert {p:p.read_bytes() for p in before}==before
    # Projection and reads do no native I/O, including filesystem probes.
    monkeypatch.setattr(Path,'stat',lambda *a,**k:(_ for _ in ()).throw(AssertionError('native stat')))
    monkeypatch.setattr(Path,'open',lambda *a,**k:(_ for _ in ()).throw(AssertionError('native read')))
    project(catalog,[(uri,False)],after=1,through=2,command='undo')
    assert catalog.list(connectionId='a')['items'][0]['uri']==uri


def test_undo_never_bypasses_native_or_workspace_exclusion(tmp_path):
    catalog=Catalog(tmp_path/'index.sqlite');row=record(tmp_path,1);catalog.upsert(row)
    uri=row['uri'];project(catalog,[(uri,True)])
    catalog.set_visibility(uri=uri,nativeSessionId='1',workingDirectory=str(tmp_path),deleted=True,commandId='delete',evidence='native-completed-receipt')
    project(catalog,[(uri,False)],after=1,through=2,command='undo')
    assert not catalog.list(connectionId='a',includeArchive=True)['items']
    catalog.set_visibility(uri=uri,nativeSessionId='1',workingDirectory=str(tmp_path),deleted=False,commandId='restore',evidence='native-completed-receipt')
    project(catalog,[(uri,True)],after=2,through=3,command='reset2')
    catalog.workspace(str(tmp_path),availability='unknown')
    project(catalog,[(uri,False)],after=3,through=4,command='undo2')
    assert not catalog.list(connectionId='a')['items']


def test_exact_command_recovers_after_restart_and_newer_projection(tmp_path):
    catalog=Catalog(tmp_path/'index.sqlite');row=record(tmp_path,1);catalog.upsert(row)
    expected=project(catalog,[(row['uri'],True)])
    project(catalog,[(row['uri'],False)],after=1,through=2,command='undo')
    catalog=Catalog(catalog.path)
    assert project(catalog,[(row['uri'],True)])==expected
    assert not catalog.get(row['uri'])['productHidden']
    assert catalog.session_visibility_projection_receipt(source='host',commandId='reset')==expected
    assert catalog.session_visibility_projection_receipt(source='host',commandId='unknown') is None
    assert catalog.session_visibility_projection_status(source='host')=={'source':'host','revision':2}
    with pytest.raises(ValueError,match='content changed'):project(catalog,[(row['uri'],False)])


def test_stale_source_and_invalid_batch_roll_back_every_row(tmp_path):
    catalog=Catalog(tmp_path/'index.sqlite');uri=record(tmp_path,1)['uri'];catalog.upsert(record(tmp_path,1))
    with pytest.raises(ValueError,match='already be indexed'):project(catalog,[(uri,True),('ahp-session:/unknown',True)])
    assert not catalog.get(uri)['productHidden']
    assert catalog.session_visibility_projection_status(source='host')['revision']==0
    assert catalog.session_visibility_projection_receipt(source='host',commandId='reset') is None
    project(catalog,[(uri,True)])
    with pytest.raises(InvalidCursor):project(catalog,[(uri,False)],command='stale')
    with pytest.raises(ValueError,match='another source'):project(catalog,[(uri,False)],source='foreign')
    assert catalog.session_visibility_projection_status(source='foreign')['revision']==0
    assert catalog.get(uri)['productHidden']


@pytest.mark.parametrize('items,after,through',[
    ([],True,2),([],0,False),([],1,1),([],0,2**63),
    ([{'uri':'u','hidden':1,'revision':1}],0,1),
    ([{'uri':'u','hidden':True,'revision':0}],0,1),
    ([{'uri':'u','hidden':True,'revision':True}],0,1),
    ([{'uri':'u','hidden':True,'revision':1,'unknown':1}],0,1),
    ([{'uri':'u','hidden':True,'revision':1}]*2,0,1),
    ([{'uri':'u','hidden':True,'revision':1}]*101,0,1),
])
def test_exact_bounded_validation(tmp_path,items,after,through):
    catalog=Catalog(tmp_path/'index.sqlite')
    with pytest.raises(ValueError):catalog.project_session_visibility(source='host',commandId='bad',afterRevision=after,throughRevision=through,items=items)
    assert catalog.session_visibility_projection_status(source='host')['revision']==0


def test_concurrent_cas_accepts_one_without_losing_receipt(tmp_path):
    catalog=Catalog(tmp_path/'index.sqlite');uri=record(tmp_path,1)['uri'];catalog.upsert(record(tmp_path,1))
    start=threading.Barrier(2);results=[];errors=[]
    def run(command,hidden):
        try:start.wait();results.append(project(catalog,[(uri,hidden)],command=command))
        except InvalidCursor:errors.append(command)
    threads=[threading.Thread(target=run,args=('a',True)),threading.Thread(target=run,args=('b',False))]
    for thread in threads:thread.start()
    for thread in threads:thread.join()
    assert len(results)==len(errors)==1
    assert catalog.session_visibility_projection_receipt(source='host',commandId=results[0]['commandId'])==results[0]
    assert catalog.session_visibility_projection_receipt(source='host',commandId=errors[0]) is None


def test_filtered_child_archive_retention_pages_and_guards(tmp_path):
    catalog=Catalog(tmp_path/'index.sqlite');root=record(tmp_path,1);catalog.upsert(root)
    child=record(tmp_path,2,parentUri=root['uri']);catalog.upsert(child)
    for i in range(3,70):catalog.upsert(record(tmp_path,i,isArchived=True))
    project(catalog,[(record(tmp_path,i)['uri'],True) for i in range(20,70)]+[(child['uri'],True)])
    for kwargs in ({},{'archive':'all'},{'includeArchive':True}):
        page=catalog.list(connectionId='a',limit=5,**kwargs)
        assert len(page['items'])==5 and page['items'][0]['nativeSessionId']=='19'
        rows=list(page['items'])
        while page.get('nextCursor'):
            page=catalog.list(connectionId='a',limit=5,cursor=page['nextCursor'],**kwargs)
            rows.extend(page['items'])
        assert [int(row['nativeSessionId']) for row in rows]==list(range(19,2,-1))+[1]
    assert not catalog.list(connectionId='a',parentUri=root['uri'])['items']
    assert catalog.descendants(parentUri=root['uri'])['items'][0]['uri']==child['uri']
    result=catalog.retention_candidates(connectionId='a',modifiedBefore=10000,allowedWorkspaceRoots=[str(tmp_path)])
    assert result['candidateCount']==18
    # Registration visibility changes cannot restore the product-hidden sessions.
    for revision,hidden in [(1,True),(2,False)]:
        catalog.project_workspaces(source='workspace-owner',afterRevision=revision-1,throughRevision=revision,records=[
            {'id':catalog.workspace_id(str(tmp_path)),'path':str(tmp_path),'name':'Work','registered':True,'hidden':hidden,'revision':revision}])
    assert len(catalog.list(connectionId='a')['items'])==18


def test_large_hidden_projection_uses_visible_index_and_scoped_updates(tmp_path):
    catalog=Catalog(tmp_path/'index.sqlite')
    for i in range(55):catalog.upsert(record(tmp_path,i))
    with catalog.connect() as db:
        db.execute("""WITH RECURSIVE ids(n) AS (VALUES(100) UNION ALL SELECT n+1 FROM ids WHERE n<25100)
            INSERT INTO sessions SELECT printf('ahp-session:/%08d',n),engine,printf('%d',n),workspace,
            parent_uri,kind,title,created_at,n+100000,availability,source,storage_path,native_project,
            fingerprint,seen_run,indexed_at,details,0 FROM ids,sessions WHERE uri='ahp-session:/00000001'""")
        db.execute("INSERT INTO product_visibility SELECT uri,'bulk-host',1,1 FROM sessions WHERE modified_at>=100000")
    original=catalog.connect
    @contextmanager
    def bounded():
        with original() as db:
            calls=[0]
            def steps():calls[0]+=1;return calls[0]>200
            db.set_progress_handler(steps,100)
            yield db
    catalog.connect=bounded
    assert len(catalog.list(connectionId='scale',limit=50)['items'])==50
    project(catalog,[(record(tmp_path,1)['uri'],True)])
    assert len(catalog.list(connectionId='scale',limit=50)['items'])==50


def test_stdio_methods_are_additive_and_passive(tmp_path):
    catalog=Catalog(tmp_path/'index.sqlite');catalog.upsert(record(tmp_path,1));server=Server(catalog,Discovery(catalog,[]))
    params={'source':'host','commandId':'reset','afterRevision':0,'throughRevision':1,'items':[{'uri':record(tmp_path,1)['uri'],'hidden':True,'revision':1}]}
    request={'jsonrpc':'2.0','id':1,'method':'projectSessionVisibility','params':params}
    expected=server.handle(request)['result']
    assert server.handle({'jsonrpc':'2.0','id':2,'method':'sessionVisibilityProjectionReceipt','params':{'source':'host','commandId':'reset'}})['result']==expected
    assert server.handle({'jsonrpc':'2.0','id':3,'method':'sessionVisibilityProjectionStatus','params':{'source':'host'}})['result']['revision']==1


def test_catalog_loss_replays_host_snapshot_in_contiguous_bounded_batches(tmp_path):
    catalog=Catalog(tmp_path/'old.sqlite')
    rows=[record(tmp_path,i) for i in range(105)]
    for row in rows:catalog.upsert(row)
    project(catalog,[(row['uri'],True) for row in rows[:100]])
    project(catalog,[(row['uri'],True) for row in rows[100:]],after=1,through=2,command='reset-page2')
    # Actual host fence is deliberately outside this component; emulate its order.
    snapshot=[(row['uri'],catalog.get(row['uri'])['productHidden']) for row in rows]
    rebuilt=Catalog(tmp_path/'rebuilt.sqlite')
    assert rebuilt.session_visibility_projection_status(source='host')['revision']==0
    for row in rows:rebuilt.upsert(row)
    for page in range(2):
        batch=snapshot[page*100:(page+1)*100]
        result=project(rebuilt,batch,after=page,through=page+1,command=f'rebuild-epoch-page{page}')
        assert result['projected']==len(batch)
        assert rebuilt.session_visibility_projection_status(source='host')['revision']==page+1
    assert rebuilt.list(connectionId='product-ready')['items']==[]
    # No-row snapshots still have an explicit verifiable completion checkpoint.
    receipt=project(rebuilt,[],after=2,through=3,command='rebuild-empty-final')
    assert receipt['projected']==0 and receipt['revision']==3
