import json
from contextlib import contextmanager
from pathlib import Path
import pytest
from amplifier_session_catalog import Catalog, InvalidCursor


def row(workspace,n,**extra):
    return dict(uri=f'ahp-session:/{n:08d}',engineId='fixture',nativeSessionId=str(n),workingDirectory=str(workspace),title=f'Chat {n:08d}',createdAt=1000+n,modifiedAt=1000+n,**extra)


def observation(effect,revision=1,incarnation='one'):
    return dict(version=1,source='fixture-host',incarnation=incarnation,revision=revision,observedAt='2026-10-04T00:00:00Z',effect=effect,unknownChecked=True)


def query(c,workspace,**extra):
    return c.library_query(connectionId='fixture',allowedWorkspaceRoots=[str(workspace)],**extra)


def estate(c,w):
    c.upsert(row(w,1))
    with c.connect() as db:
        db.execute("""WITH RECURSIVE ids(n) AS (VALUES(2) UNION ALL SELECT n+1 FROM ids WHERE n<25000)
        INSERT INTO sessions SELECT printf('ahp-session:/%08d',n),engine,printf('%d',n),workspace,parent_uri,kind,
        printf('Chat %08d',n),1000+n,1000+n,availability,source,storage_path,native_project,fingerprint,seen_run,indexed_at,details,visible FROM ids,sessions WHERE uri='ahp-session:/00000001'""")


def test_25k_cold_working_beyond_page_one_without_native_reads(tmp_path,monkeypatch):
    w=tmp_path/'existing';w.mkdir();c=Catalog(tmp_path/'index.sqlite');estate(c,w)
    assert row(w,3)['uri'] not in [r['uri'] for r in query(c,w)['items']]
    monkeypatch.setattr(Path,'stat',lambda *_: (_ for _ in ()).throw(AssertionError('filesystem read')))
    monkeypatch.setattr(Path,'open',lambda *_: (_ for _ in ()).throw(AssertionError('history read')))
    facts=dict(source='fixture-host',incarnation='one',items=[dict(uri=row(w,3)['uri'],runtime='working',effect='unverified',modifiedAt=1003)])
    result=query(c,w,activity='working',liveFacts=facts)
    assert [r['uri'] for r in result['items']]==[row(w,3)['uri']]
    assert result['items'][0]['activity']['runtime']=='working'
    assert query(c,w)['items'][0]['activity']==dict(kind='unknown',runtime='unknown',effect='unverified',evidence='indexed')
    assert result['countsAvailable'] is False


def test_cold_uncertain_effect_exceeds_residency_and_survives_native_scan_restart(tmp_path):
    w=tmp_path/'existing';w.mkdir();path=tmp_path/'index.sqlite';c=Catalog(path)
    for n in range(1,90): c.upsert(row(w,n,hostObservation=observation('unknown' if n%2 else 'error')))
    c.upsert(row(w,100,hostObservation=observation('settled')))
    c.upsert(row(w,1),source='native-fixture')
    c=Catalog(path);result=query(c,w,activity='attention')
    assert len(result['items'])==50
    assert all(r['activity']['runtime']=='unknown' and r['activity']['kind']=='attention' for r in result['items'])
    tail=query(c,w,activity='attention',cursor=result['nextCursor'])
    assert len(tail['items'])==39
    assert c.get(row(w,1)['uri'])['hostObservation']['effect']=='unknown'
    assert query(c,w,search='Chat 00000100')['items'][0]['activity']['kind']=='unknown'
    with pytest.raises(ValueError):c.upsert(row(w,1,hostObservation=observation('settled')),source='native-fixture')


@pytest.mark.parametrize('sort',['activity','created','name'])
def test_all_pages_order_ties_search_authority_and_scoped_refresh(tmp_path,sort):
    w=tmp_path/'authorized';w.mkdir();outside=tmp_path/'authorized-sibling';outside.mkdir();c=Catalog(tmp_path/'index.sqlite')
    for n in range(1,162):
        r=row(w,n);r['title']='same' if n%3 else 'other';r['modifiedAt']=1000+n//3;r['createdAt']=1000+n//5;c.upsert(r)
    for n in range(200,280):c.upsert(row(outside,n,hostObservation=observation('unknown')))
    got=[];cursor=None
    while True:
        result=query(c,w,sort=sort,archive='all',cursor=cursor);got+=result['items'];cursor=result.get('nextCursor')
        if not cursor:break
    key=(lambda r:(r['title'].lower(),r['uri'])) if sort=='name' else (lambda r:(-int(r['modifiedAt'][17:19]),r['uri']))
    expected=sorted(got,key=(lambda r:(r['title'].lower(),r['uri'])) if sort=='name' else (lambda r:(-__import__('datetime').datetime.fromisoformat(r['createdAt' if sort=='created' else 'modifiedAt'].replace('Z','+00:00')).timestamp(),r['uri'])))
    assert [r['uri'] for r in got]==[r['uri'] for r in expected] and len({r['uri'] for r in got})==161
    first=query(c,w,sort=sort);other=row(outside,299);c.upsert(other)
    query(c,w,sort=sort,cursor=first['nextCursor']) # outside scope never stales
    changed=row(w,1);changed['title']='new name';changed['modifiedAt']=1000;changed['createdAt']=1000;c.upsert(changed)
    if sort=='name':
        with pytest.raises(InvalidCursor):query(c,w,sort=sort,cursor=first['nextCursor'])
    c.set_visibility(uri=row(w,10)['uri'],nativeSessionId='10',workingDirectory=str(w),deleted=True,commandId='delete',evidence='native-completed-receipt')
    with pytest.raises(InvalidCursor):query(c,w,sort=sort,cursor=first['nextCursor'])
    assert len(query(c,w,search=str(w),sort=sort)['items'])==50
    assert query(c,w,search='ahp-session:/00000001')['items'][0]['uri']==row(w,1)['uri']
    with pytest.raises(InvalidCursor):query(Catalog(tmp_path/'index.sqlite'),w,sort=sort,cursor=first['nextCursor'])


def test_live_changes_capacity_and_metadata_delivery_freshness(tmp_path):
    w=tmp_path/'existing';w.mkdir();c=Catalog(tmp_path/'index.sqlite')
    for n in range(60):c.upsert(row(w,n))
    facts=dict(source='fixture-host',incarnation='one',items=[])
    first=query(c,w,liveFacts=facts)
    facts['items']=[dict(uri=row(w,3)['uri'],runtime='working',effect='unverified',modifiedAt=9000)]
    with pytest.raises(InvalidCursor):query(c,w,liveFacts=facts,cursor=first['nextCursor'])
    degraded=query(c,w,observationFreshness=dict(phase='degraded',issues=['failed-point-update']))
    assert degraded['observationFreshness']['phase']=='degraded'
    assert all(r['activity']['kind']=='unknown' for r in degraded['items'])
    facts['items']*=65
    with pytest.raises(ValueError):query(c,w,liveFacts=facts)

def test_observation_repair_cannot_retitle_or_promote_child_metadata(tmp_path):
    w=tmp_path/'existing';w.mkdir();c=Catalog(tmp_path/'index.sqlite')
    original=row(w,1,parentUri='ahp-session:/parent');c.upsert(original)
    stale=row(w,1,hostObservation=observation('unknown'),hostObservationOnly=True);stale['title']='stale Host title'
    c.upsert(stale)
    retained=c.get(original['uri'])
    assert retained['title']==original['title'] and retained['parentUri']==original['parentUri'] and retained['kind']=='child'
    assert query(c,w,activity='attention')['items']==[]
    assert query(c,w,activity='attention',parentUri=original['parentUri'])['items'][0]['activity']['effect']=='unknown'
    with pytest.raises(ValueError):c.upsert(row(w,2,hostObservation=observation('unknown'),hostObservationOnly=True))
    absent=tmp_path/'not-registered'
    with pytest.raises(ValueError):c.upsert(row(absent,2,hostObservation=observation('unknown'),hostObservationOnly=True))
    with c.connect() as db:assert db.execute('SELECT 1 FROM workspaces WHERE path=?',(str(absent),)).fetchone() is None

def test_25k_sparse_attention_and_working_use_indexed_candidate_work(tmp_path,monkeypatch):
    w=tmp_path/'existing';w.mkdir();c=Catalog(tmp_path/'index.sqlite');estate(c,w)
    c.upsert(row(w,3,hostObservation=observation('unknown')))
    original=c.connect;plans=[];steps=[]
    @contextmanager
    def instrumented():
        with original() as db:
            counter=[0]
            db.set_progress_handler(lambda: counter.__setitem__(0,counter[0]+1) or counter[0]>200,100)
            def trace(sql):
                if sql.startswith('SELECT s.*,COALESCE'):
                    plans.extend(tuple(r) for r in db.execute('EXPLAIN QUERY PLAN '+sql))
            db.set_trace_callback(trace)
            yield db
            steps.append(counter[0]*100)
    monkeypatch.setattr(c,'connect',instrumented)
    assert query(c,w,activity='attention')['items'][0]['uri']==row(w,3)['uri']
    facts=dict(source='fixture-host',incarnation='one',items=[dict(uri=row(w,4)['uri'],runtime='working',effect='unverified',modifiedAt=1004)])
    assert query(c,w,activity='working',liveFacts=facts)['items'][0]['uri']==row(w,4)['uri']
    assert any('library_attention_activity' in str(p) for p in plans)
    assert max(steps)<20000

def test_visibility_archive_and_managed_grants_filter_before_limit(tmp_path):
    from test_managed_locations import managed
    w=tmp_path/'visible';w.mkdir();c=Catalog(tmp_path/'index.sqlite')
    c.upsert(row(w,1));c.upsert(row(w,2,isArchived=True))
    for n in range(3,70):
        hidden=tmp_path/f'gone-{n}';c.upsert(row(hidden,n,hostObservation=observation('unknown')))
    c.upsert(row(w,70,hostObservation=observation('unknown')))
    c.set_visibility(uri=row(w,70)['uri'],nativeSessionId='70',workingDirectory=str(w),deleted=True,commandId='hide',evidence='native-completed-receipt')
    c.workspace(str(tmp_path/'gone-3'),availability='unknown')
    assert [r['uri'] for r in query(c,tmp_path,limit=1)['items']]==[row(w,1)['uri']]
    assert 'nextCursor' not in query(c,tmp_path,limit=1)
    assert [r['uri'] for r in query(c,tmp_path,archive='archived',limit=1)['items']]==[row(w,2)['uri']]
    assert query(c,tmp_path,activity='attention')['items']==[]
    root,allocation=managed(tmp_path);c.upsert(allocation)
    ordinary={**allocation,'uri':'ahp-session:/ordinary','nativeSessionId':'ordinary'};ordinary.pop('location');c.upsert(ordinary)
    args=dict(connectionId='managed',allowedWorkspaceRoots=[],allowedManagedRoots=[str(root)],location='managed')
    assert [r['uri'] for r in c.library_query(**args)['items']]==[allocation['uri']]
    assert c.library_query(**{**args,'allowedManagedRoots':[str(root.parent)]})['items']==[]
