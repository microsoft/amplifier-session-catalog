from contextlib import contextmanager
import pytest
from amplifier_session_catalog import Catalog


@pytest.mark.parametrize('children',[False,True])
def test_cursor_seeks_past_large_timestamp_ties_without_scanning_skipped_history(tmp_path,children):
    catalog=Catalog(tmp_path/'index.sqlite')
    parent='ahp-session:/parent' if children else None
    for native,modified in [('z0',2),('z1',2),('older',1)]:
        catalog.upsert({'uri':'ahp-session:/'+native,'engineId':'a','nativeSessionId':native,
            'workingDirectory':str(tmp_path),'parentUri':parent,'createdAt':1,'modifiedAt':modified})
    args={'connectionId':'client','limit':1,'parentUri':parent,'allowedWorkspaceRoots':[str(tmp_path)]}
    first=catalog.list(**args)
    assert first['items'][0]['nativeSessionId']=='z0'
    # Newly discovered rows sorting before the retained cursor must not enlarge
    # continuation cost. This also exercises non-snapshot, live-page semantics.
    with catalog.connect() as db:
        db.execute("""WITH RECURSIVE ids(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM ids WHERE n<25000)
            INSERT INTO sessions SELECT printf('ahp-session:/a%08d',n),engine,printf('%d',n),workspace,
            parent_uri,kind,title,created_at,modified_at,availability,source,storage_path,native_project,
            fingerprint,seen_run,indexed_at,details,visible FROM ids,sessions WHERE uri='ahp-session:/z0'""")
    original=catalog.connect;steps=[0]
    @contextmanager
    def bounded():
        with original() as db:
            def tick():
                steps[0]+=1
                return steps[0]>5000
            db.set_progress_handler(tick,1)
            yield db
    catalog.connect=bounded
    second=catalog.list(**args,cursor=first['nextCursor'])
    assert second['items'][0]['nativeSessionId']=='z1' and steps[0]<5000
    steps[0]=0
    last=catalog.list(**args,cursor=second['nextCursor'])
    assert last['items'][0]['nativeSessionId']=='older' and 'nextCursor' not in last and steps[0]<5000


@pytest.mark.parametrize('unrelated',[2000,20950])
def test_selected_child_pages_seek_without_walking_other_visible_families(tmp_path,monkeypatch,unrelated):
    catalog=Catalog(tmp_path/'index.sqlite')
    catalog.upsert({'uri':'ahp-session:/root','engineId':'a','nativeSessionId':'root',
        'workingDirectory':str(tmp_path),'createdAt':1,'modifiedAt':2})
    # Fixed 25k estate: 4k roots and 21k children. Fifty selected children sort
    # after the unrelated families, including equal timestamps across parents.
    with catalog.connect() as db:
        db.execute("DELETE FROM sessions")
        db.executemany('INSERT INTO sessions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            ((f'ahp-session:/r{i:04d}','a',f'r{i:04d}',str(tmp_path),None,'root','Root',
              1,2,'available','fixture',None,None,None,None,1,'{}',1) for i in range(4000)))
        db.executemany('INSERT INTO sessions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            ((f'ahp-session:/c{i:05d}','a',f'c{i:05d}',str(tmp_path),
              'ahp-session:/r0000' if i>=20950 else 'ahp-session:/r0001',
              'child' if i<unrelated or i>=20950 else 'internal','Child',
              1,2 if i<20980 else 1,'available','fixture',None,None,None,None,1,'{}',1)
             for i in range(21000)))
    original=catalog.connect;steps=[0]
    @contextmanager
    def bounded():
        with original() as db:
            def tick():
                steps[0]+=1
                return steps[0]>5000
            db.set_progress_handler(tick,1)
            yield db
    catalog.connect=bounded
    def refuse(*_args,**_kwargs):raise AssertionError('List loaded native files')
    with monkeypatch.context() as patch:
        patch.setattr(type(tmp_path),'stat',refuse)
        patch.setattr(type(tmp_path),'open',refuse)
        args={'connectionId':'client','limit':25,'parentUri':'ahp-session:/r0000',
              'allowedWorkspaceRoots':[str(tmp_path)]}
        cursor=None;found=[]
        for _ in range(2):
            steps[0]=0;page=catalog.list(**args,**({'cursor':cursor} if cursor else {}))
            assert steps[0]<5000
            assert len(page['items'])==25
            found.extend(row['nativeSessionId'] for row in page['items']);cursor=page.get('nextCursor')
        assert found==[f'c{i:05d}' for i in range(20950,21000)] and cursor is None
        steps[0]=0;roots=catalog.list(connectionId='client',limit=25,allowedWorkspaceRoots=[str(tmp_path)])
        assert steps[0]<5000 and all(row['kind']=='root' for row in roots['items'])
