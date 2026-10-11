import pytest
from amplifier_session_catalog import Catalog, InvalidCursor
from test_library import row, observation, query, estate


def nav(at,agent=False):
    return dict(version=1,activityAt=at,agentCreated=agent)


def observed(w,n,at,agent=False,revision=1):
    return row(w,n,hostObservation={**observation('settled',revision), 'navigation':nav(at,agent)})


def recent(c,w,**kwargs):
    return query(c,w,navigationVersion=1,sort='recent',**kwargs)


def test_agent_filter_precedes_every_page_and_user_forks_remain(tmp_path):
    w=tmp_path/'work';w.mkdir();c=Catalog(tmp_path/'index')
    for n in range(160):c.upsert(observed(w,n,2000+n,n>=60))
    c.upsert({**observed(w,200,3000), 'parentUri':row(w,0)['uri'],'kind':'root'})
    first=recent(c,w);second=recent(c,w,cursor=first['nextCursor'])
    assert len(first['items'])==50 and len(second['items'])==11
    assert first['items'][0]['uri']==row(w,200)['uri']
    assert len({r['uri'] for r in first['items']+second['items']})==61
    assert len(recent(c,w,includeAgentCreated=True)['items'])==50
    selected=row(w,159)['uri']
    selected_first=recent(c,w,selectedUri=selected)
    assert selected in [r['uri'] for r in selected_first['items']]
    assert len({r['uri'] for r in selected_first['items']+recent(c,w,selectedUri=selected,cursor=selected_first['nextCursor'])['items']})==62
    with pytest.raises(InvalidCursor):recent(c,w,selectedUri=row(w,158)['uri'],cursor=selected_first['nextCursor'])
    assert recent(c,w,selectedUri=selected,search='does not match')['items']==[]
    with pytest.raises(InvalidCursor):recent(c,w,includeAgentCreated=True,cursor=first['nextCursor'])


def test_naming_raw_progress_and_native_refresh_preserve_stable_key(tmp_path):
    w=tmp_path/'work';w.mkdir();c=Catalog(tmp_path/'index')
    for n in range(65):c.upsert(observed(w,n,2000+n))
    first=recent(c,w)
    changed=observed(w,0,2000,revision=2);changed.update(title='Automatic title',modifiedAt=99999);c.upsert(changed)
    assert recent(c,w,cursor=first['nextCursor'])['items']
    c.upsert(row(w,0),source='native-fixture')
    assert c.get(row(w,0)['uri'])['hostObservation']['navigation']==nav(2000)
    assert recent(c,w,cursor=first['nextCursor'])['items']
    c.upsert(observed(w,0,99999,revision=3))
    with pytest.raises(InvalidCursor):recent(c,w,cursor=first['nextCursor'])
    assert recent(c,w)['items'][0]['uri']==row(w,0)['uri']
    c.upsert(observed(w,0,1,revision=2))
    assert recent(c,w)['items'][0]['uri']==row(w,0)['uri']


def test_navigation_is_checked_and_not_authorable_by_native_hints(tmp_path):
    w=tmp_path/'work';w.mkdir();c=Catalog(tmp_path/'index')
    for value in [nav(float('nan')),nav(-1),nav(1,'yes'),{**nav(1),'extra':True}]:
        with pytest.raises(ValueError):c.upsert(row(w,1,hostObservation={**observation('settled'),'navigation':value}))
    with pytest.raises(ValueError):c.upsert(observed(w,1,1),source='native-fixture')
    with pytest.raises(ValueError):query(c,w,sort='recent')
    with pytest.raises(ValueError):query(c,w,includeAgentCreated=False)


def test_live_overlay_controls_both_position_and_membership(tmp_path):
    w=tmp_path/'work';w.mkdir();c=Catalog(tmp_path/'index')
    c.upsert(observed(w,1,1));c.upsert(observed(w,2,2))
    facts=dict(source='fixture-host',incarnation='one',items=[dict(uri=row(w,1)['uri'],runtime='working',effect='unverified',modifiedAt=99999,navigation=nav(3,True))])
    assert [r['uri'] for r in recent(c,w,liveFacts=facts)['items']]==[row(w,2)['uri']]
    assert recent(c,w,liveFacts=facts,includeAgentCreated=True)['items'][0]['uri']==row(w,1)['uri']
    assert recent(c,w,liveFacts=facts,selectedUri=row(w,1)['uri'])['items'][0]['uri']==row(w,1)['uri']


def test_deep_recent_pages_and_sparse_human_roots_use_indexed_seeks(tmp_path,monkeypatch):
    from contextlib import contextmanager
    w=tmp_path/'work';w.mkdir();c=Catalog(tmp_path/'index');estate(c,w)
    with c.connect() as db:
        db.execute("UPDATE sessions SET details=json_set(details,'$.hostObservation',json(?))",
                   (__import__('json').dumps({**observation('settled'),'navigation':nav(10000,True)}),))
    for n in range(1,151):c.upsert(observed(w,n,10000,revision=2))
    original=c.connect;steps=[];plans=[]
    @contextmanager
    def instrumented():
        with original() as db:
            counter=[0]
            db.set_progress_handler(lambda: counter.__setitem__(0,counter[0]+1) or counter[0]>300,100)
            def trace(sql):
                if sql.startswith('SELECT s.*,COALESCE'):plans.extend(tuple(r) for r in db.execute('EXPLAIN QUERY PLAN '+sql))
            db.set_trace_callback(trace)
            yield db
            steps.append(counter[0]*100)
    monkeypatch.setattr(c,'connect',instrumented)
    got=[];cursor=None
    for _ in range(4):
        result=recent(c,w,cursor=cursor,selectedUri=row(w,24999)['uri']);got.extend(r['uri'] for r in result['items']);cursor=result.get('nextCursor')
        if not cursor:break
    assert len(set(got))==151 and len(got)==151
    assert any('library_human_recent' in str(p) for p in plans)
    assert max(steps)<30000
