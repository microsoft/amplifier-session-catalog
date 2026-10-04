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
