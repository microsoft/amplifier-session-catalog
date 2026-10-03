from contextlib import contextmanager
import json
from pathlib import Path
import subprocess
import sys

import pytest

from amplifier_session_catalog import Catalog, InvalidCursor
from amplifier_session_catalog.discovery import Discovery, classify, project_slug, session_uri


@pytest.fixture
def catalog(tmp_path):
    return Catalog(tmp_path / "index.sqlite")


def record(workspace, number, **extra):
    return {"uri": f"ahp-session:/{number:08d}", "engineId": "amplifier", "nativeSessionId": str(number),
            "workingDirectory": str(workspace), "modifiedAt": 1000 + number, **extra}

def test_migration_lookup_is_indexed_scoped_and_never_guesses(catalog, tmp_path):
    existing = tmp_path / 'owned'; existing.mkdir()
    outside = tmp_path / 'outside'; outside.mkdir()
    catalog.upsert(record(existing, 1, nativeSessionId='same'))
    catalog.upsert(record(outside, 2, nativeSessionId='same'))
    catalog.upsert(record(existing, 3, nativeSessionId='duplicate'))
    catalog.upsert(record(existing, 4, nativeSessionId='duplicate'))
    result = catalog.resolve_native(engineId='amplifier', identities=['same','duplicate','missing'], allowedWorkspaceRoots=[str(existing)])
    assert result['same'] == {'status':'matched','uri':'ahp-session:/00000001'}
    assert result['duplicate'] == {'status':'ambiguous'}
    assert result['missing'] == {'status':'missing'}
    assert catalog.resolve_native(engineId='amplifier', identities=['same'], allowedWorkspaceRoots=[])['same']['status']=='missing'


def test_filter_is_before_page_and_children_are_explicit(catalog, tmp_path):
    existing = tmp_path / "existing"; existing.mkdir()
    missing = tmp_path / "gone"
    root = record(existing, 1)
    catalog.upsert(root)
    catalog.upsert(record(existing, 2, parentUri=root["uri"]))
    catalog.upsert(record(existing, 3, kind="internal"))
    for i in range(10, 30):
        catalog.upsert(record(missing, i))
    assert [r["uri"] for r in catalog.list(connectionId="a", limit=1)["items"]] == [root["uri"]]
    assert "nextCursor" not in catalog.list(connectionId="a", limit=1)
    assert len(catalog.list(connectionId="a", parentUri=root["uri"])["items"]) == 1
    assert len(catalog.list(connectionId="a", includeArchive=True)["items"]) == 21
    assert catalog.get(record(missing, 10)["uri"])["workspaceAvailability"] == "missing"


def test_directory_guard_includes_hidden_history_and_respects_path_boundaries(catalog, tmp_path, monkeypatch):
    target = tmp_path / "managed"
    catalog.upsert(record(target / "child", 1, parentUri="ahp-session:/parent"))
    catalog.upsert(record(tmp_path / "elsewhere", 2, kind="internal", storagePath=str(target / "native" / "two")))
    catalog.upsert(record(tmp_path / "managed-other", 3))
    # Removal checks must neither stat each historical directory nor open native history.
    monkeypatch.setattr(Path, "stat", lambda *a, **k: (_ for _ in ()).throw(AssertionError("guard performed stat")))
    monkeypatch.setattr(Path, "open", lambda *a, **k: (_ for _ in ()).throw(AssertionError("guard read native history")))
    result = catalog.directory_in_use(path=str(target))
    assert result["inUse"] and not result["truncated"]
    assert {row["session"]: row["reason"] for row in result["sessions"]} == {
        "ahp-session:/00000001": "history-workspace", "ahp-session:/00000002": "native-history-storage"}
    assert result["coverage"] == "indexed-native-history"
    assert result["freshness"]["phase"] == "not-started"
    assert not catalog.directory_in_use(path=str(tmp_path / "unused"))["inUse"]
    with pytest.raises(ValueError):
        catalog.directory_in_use(path="relative")


def test_directory_guard_is_bounded_on_large_cold_catalog(catalog, tmp_path, monkeypatch):
    root = tmp_path / "cold"
    catalog.upsert(record(root, 1))
    with catalog.connect() as db:
        db.execute("""WITH RECURSIVE ids(n) AS (VALUES(2) UNION ALL SELECT n+1 FROM ids WHERE n<25000)
            INSERT INTO sessions SELECT printf('ahp-session:/%08d',n),engine,printf('%d',n),workspace,
            parent_uri,kind,title,created_at,modified_at,availability,source,storage_path,native_project,
            fingerprint,seen_run,indexed_at,details,visible FROM ids,sessions WHERE uri='ahp-session:/00000001'""")
    original = catalog.connect
    @contextmanager
    def bounded_query():
        with original() as db:
            calls = 0
            def steps():
                nonlocal calls
                calls += 1
                return calls > 500
            db.set_progress_handler(steps, 100)
            yield db
    monkeypatch.setattr(catalog, "connect", bounded_query)
    assert not catalog.directory_in_use(path=str(tmp_path / "absent"))["inUse"]
    result = catalog.directory_in_use(path=str(root))
    assert result["inUse"] and result["truncated"] and len(result["sessions"]) == 100


def test_query_does_not_restat_or_read_native_files(catalog, tmp_path, monkeypatch):
    catalog.upsert(record(tmp_path, 1))
    monkeypatch.setattr(Path, "stat", lambda *a, **k: (_ for _ in ()).throw(AssertionError("query performed stat")))
    monkeypatch.setattr(Path, "open", lambda *a, **k: (_ for _ in ()).throw(AssertionError("query opened native data")))
    assert len(catalog.list(connectionId="a")["items"]) == 1
    assert catalog.get("ahp-session:/00000001")["nativeSessionId"] == "1"


def test_cursors_bind_connection_query_and_process(catalog, tmp_path):
    for i in range(7):
        catalog.upsert(record(tmp_path, i, modifiedAt=1000))
    page = catalog.list(connectionId="a", limit=2)
    cursor = page["nextCursor"]
    with pytest.raises(InvalidCursor):
        catalog.list(connectionId="b", limit=2, cursor=cursor)
    with pytest.raises(InvalidCursor):
        catalog.list(connectionId="a", limit=2, cursor=cursor, includeArchive=True)
    with pytest.raises(InvalidCursor):
        Catalog(catalog.path).list(connectionId="a", limit=2, cursor=cursor)
    ids = [r["uri"] for r in page["items"]]
    while page.get("nextCursor"):
        page = catalog.list(connectionId="a", limit=2, cursor=page["nextCursor"])
        ids.extend(r["uri"] for r in page["items"])
    assert len(ids) == len(set(ids)) == 7


def test_authorized_roots_do_not_match_sibling_prefix(catalog, tmp_path):
    a = tmp_path / "work"; a.mkdir()
    b = tmp_path / "work-other"; b.mkdir()
    catalog.upsert(record(a, 1)); catalog.upsert(record(b, 2))
    assert len(catalog.list(connectionId="a", allowedWorkspaceRoots=[str(a)])["items"]) == 1
    assert catalog.list(connectionId="a", allowedWorkspaceRoots=[])["items"] == []


def test_workspace_invalidation_updates_only_its_dependents(catalog, tmp_path):
    a = tmp_path / "a"; a.mkdir()
    b = tmp_path / "b"; b.mkdir()
    catalog.upsert(record(a, 1)); catalog.upsert(record(b, 2))
    catalog.workspace(str(a), availability="unknown")
    assert [r["nativeSessionId"] for r in catalog.list(connectionId="a")["items"]] == ["2"]
    assert catalog.get("ahp-session:/00000001")["workspaceAvailability"] == "unknown"
    catalog.workspace(str(a), availability="present")
    assert len(catalog.list(connectionId="a")["items"]) == 2


def native_session(home, workspace, identity, **metadata):
    directory = home / "projects" / project_slug(workspace) / "sessions" / identity
    directory.mkdir(parents=True)
    (directory / "metadata.json").write_text(json.dumps({"working_dir": str(workspace), "parent_id": None, **metadata}))
    (directory / "transcript.jsonl").write_text('{"role":"user","content":"hello"}\n')
    (directory / "events.jsonl").write_text("retained opaque event history\n")
    return directory


def test_discovery_hints_backups_parent_forks_and_no_native_writes(catalog, tmp_path):
    home = (tmp_path / "native").resolve()
    workspace = (tmp_path / "workspace").resolve(); workspace.mkdir()
    root = native_session(home, workspace, "root", name="Original")
    child = native_session(home, workspace, "root_child", parent_id="root")
    fork = native_session(home, workspace, "fork", parent_id="root", forked_from_turn=1, forked_at="2026-10-02")
    native_session(home, workspace, "internal", session_visibility="internal")
    (root / "metadata.json.backup").write_bytes((root / "metadata.json").read_bytes())
    before = {p: p.read_bytes() for p in home.rglob("*") if p.is_file()}
    d = Discovery(catalog, [home])
    result = d.scan()
    assert result["sessions"] == 4
    items = catalog.list(connectionId="one")["items"]
    assert {r["nativeSessionId"] for r in items} == {"root", "fork"}
    parent = session_uri(home, project_slug(workspace), "root")
    assert catalog.list(connectionId="one", parentUri=parent)["items"][0]["nativeSessionId"] == "root_child"
    assert {p: p.read_bytes() for p in before} == before
    assert d.scan()["metadataReads"] == 0
    (root / "metadata.json").write_text("{")
    d.hint(str(root))
    assert catalog.get(parent)["title"] == "Original"
    assert "backup" in catalog.get(parent)["reason"]
    assert (root / "metadata.json").read_text() == "{"


def test_hint_validates_root_and_never_reads_events(catalog, tmp_path, monkeypatch):
    home = (tmp_path / "native").resolve()
    workspace = tmp_path.resolve()
    directory = native_session(home, workspace, "root")
    import amplifier_session_catalog.discovery as module
    original = module.small_metadata
    reads = []
    def inspect(path):
        reads.append(path.name)
        assert path.name not in {"transcript.jsonl", "events.jsonl"}
        return original(path)
    monkeypatch.setattr(module, "small_metadata", inspect)
    d = Discovery(catalog, [home])
    assert d.hint(str(directory))["changed"]
    assert reads == ["metadata.json"]
    with pytest.raises(ValueError):
        d.hint(str(tmp_path / "outside"))


def test_lineage_preserves_independent_forks():
    assert classify("x_child", {"parent_id": None}) == ("root", None)
    assert classify("x_child", {"parent_id": "x"}) == ("child", "x")
    assert classify("fork", {"parent_id": "x", "forked_from_turn": 1, "forked_at": "today"}) == ("root", "x")
    assert classify("root", {"session_visibility": "internal"}) == ("internal", None)


def test_discovery_adopts_host_identity_and_child_parent(catalog, tmp_path):
    workspace = tmp_path / "work"; workspace.mkdir()
    home = tmp_path / "native"
    root = native_session(home, workspace, "native-root")
    child = native_session(home, workspace, "native-root_child", parent_id="native-root")
    catalog.upsert(record(workspace, 42, nativeSessionId="native-root", title="Host-created"))
    discovery = Discovery(catalog, [home])
    assert discovery.hint(str(root))["uri"] == "ahp-session:/00000042"
    discovery.hint(str(child))
    roots = catalog.list(connectionId="a")["items"]
    assert len(roots) == 1
    children = catalog.list(connectionId="a", parentUri=roots[0]["uri"])["items"]
    assert children[0]["nativeSessionId"] == "native-root_child"


def test_valid_notifications_never_get_responses(catalog):
    from amplifier_session_catalog.__main__ import Server
    server = Server(catalog, Discovery(catalog, []))
    assert server.handle({"jsonrpc": "2.0", "method": "unknown"}) is None
    assert server.handle({"jsonrpc": "2.0", "method": "list", "params": []}) is None
    assert server.handle({"jsonrpc": "2.0", "id": 1, "method": "unknown"})["error"]["code"] == -32601


def test_background_maintenance_is_bounded_and_stoppable():
    import threading
    from amplifier_session_catalog.__main__ import Maintenance
    scanned, checked = threading.Event(), threading.Event()
    class Indexed:
        def recheck_workspaces(self, *, limit):
            assert limit == 64
            checked.set()
    class Discovered:
        def start(self):
            scanned.set()
    maintenance = Maintenance(Indexed(), Discovered(), .01, .01)
    maintenance.start()
    try:
        assert scanned.wait(2) and checked.wait(2)
    finally:
        maintenance.close()
    assert not maintenance.thread.is_alive()


def test_rpc_subprocess_and_restart_recovery(tmp_path):
    config = [sys.executable, "-m", "amplifier_session_catalog", "serve", "--db", str(tmp_path / "index.sqlite")]
    r = record(tmp_path, 1)
    request = {"jsonrpc": "2.0", "id": 1, "method": "upsert", "params": {"record": r}}
    first = subprocess.run(config, input=json.dumps(request)+"\n", text=True, capture_output=True, check=True)
    assert json.loads(first.stdout)["result"]["uri"] == r["uri"]
    request.update(id=2, method="list", params={"connectionId": "new"})
    second = subprocess.run(config, input=json.dumps(request)+"\n", text=True, capture_output=True, check=True)
    assert json.loads(second.stdout)["result"]["items"][0]["uri"] == r["uri"]


def test_query_work_does_not_scale_with_invisible_estate(catalog, tmp_path):
    catalog.workspace(str(tmp_path), availability="present")
    missing = str(tmp_path / "gone"); catalog.workspace(missing, availability="missing")
    for i in range(55):
        catalog.upsert(record(tmp_path, i))
    original = catalog.connect
    def measure():
        steps = [0]
        @contextmanager
        def counted():
            with original() as db:
                db.set_progress_handler(lambda: (steps.__setitem__(0, steps[0]+1) or 0), 1)
                yield db
        catalog.connect = counted
        try:
            page = catalog.list(connectionId="scale", limit=50)
            assert len(page["items"]) == 50
            return steps[0]
        finally:
            catalog.connect = original
    baseline = measure()
    with original() as db:
        db.executemany("INSERT INTO sessions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [
            (f"ahp-session:/hidden-{i}", "amplifier", str(i), missing, None, "root", "Archived",
             1, 100000+i, "available", "fixture", None, None, None, None, 1, "{}", 0) for i in range(25000)])
    assert measure() <= baseline + 30


def test_native_visibility_is_filtered_before_paging_and_not_revived_by_metadata(catalog, tmp_path):
    workspace=tmp_path/'workspace';workspace.mkdir()
    for i in range(80): catalog.upsert(record(workspace,i))
    for i in range(30,80):
        catalog.set_visibility(uri=f'ahp-session:/{i:08d}',nativeSessionId=str(i),workingDirectory=str(workspace),deleted=True,commandId=f'delete-{i}',evidence='native-completed-receipt')
    first=catalog.list(connectionId='deleted',limit=20)
    assert len(first['items'])==20 and first['items'][0]['nativeSessionId']=='29'
    assert len(catalog.list(connectionId='archive',includeArchive=True)['items'])==30
    catalog.upsert(record(workspace,79));catalog.workspace(str(workspace),availability='missing');catalog.workspace(str(workspace),availability='present')
    assert catalog.list(connectionId='deleted')['items'][0]['nativeSessionId']=='29'
    with pytest.raises(ValueError):catalog.set_visibility(uri='ahp-session:/00000079',nativeSessionId='foreign',workingDirectory=str(workspace),deleted=False,commandId='restore',evidence='native-completed-receipt')
    catalog.set_visibility(uri='ahp-session:/00000079',nativeSessionId='79',workingDirectory=str(workspace),deleted=False,commandId='restore',evidence='native-completed-receipt')
    assert catalog.list(connectionId='restored')['items'][0]['nativeSessionId']=='79'


def test_configured_native_lifecycle_markers_are_bounded_and_identity_checked(catalog,tmp_path):
    workspace=tmp_path/'work';workspace.mkdir();home=tmp_path/'home';app=tmp_path/'app'
    directory=native_session(home,workspace,'root')
    discovery=Discovery(catalog,[home],known_workspaces=[workspace],app_homes=[app])
    discovery.hint(str(directory));uri=catalog.list(connectionId='one')['items'][0]['uri']
    marker=app/'sessions/root/lifecycle.json';marker.parent.mkdir(parents=True)
    marker.write_text(json.dumps({'sessionId':'root','historyCwd':str(workspace),'deleted':True,'commandId':'native-delete'}))
    discovery.hint(str(directory));assert catalog.list(connectionId='hidden')['items']==[]
    marker.write_text(json.dumps({'sessionId':'foreign','historyCwd':str(workspace),'deleted':False,'commandId':'foreign-restore'}))
    assert discovery.hint(str(directory))['issues'];assert catalog.list(connectionId='still-hidden')['items']==[]
    marker.write_text(json.dumps({'sessionId':'root','historyCwd':str(workspace),'deleted':False,'commandId':'native-restore'}))
    discovery.hint(str(directory));assert catalog.list(connectionId='restored')['items'][0]['uri']==uri
    assert (directory/'transcript.jsonl').exists()


def test_first_discovery_never_exposes_native_tombstone_between_index_writes(catalog,tmp_path,monkeypatch):
    workspace=tmp_path/'work';workspace.mkdir();home=tmp_path/'home';app=tmp_path/'app';directory=native_session(home,workspace,'removed')
    marker=app/'sessions/removed/lifecycle.json';marker.parent.mkdir(parents=True);marker.write_text(json.dumps({'sessionId':'removed','historyCwd':str(workspace),'deleted':True,'commandId':'prior-delete'}))
    original=catalog.upsert
    def observed(*args,**kwargs):
        result=original(*args,**kwargs)
        assert catalog.list(connectionId='concurrent-reader')['items']==[]
        return result
    monkeypatch.setattr(catalog,'upsert',observed)
    Discovery(catalog,[home],known_workspaces=[workspace],app_homes=[app]).hint(str(directory))
