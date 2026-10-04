from pathlib import Path
import os
import json
import subprocess
import sys
import shutil
import pytest
from amplifier_session_catalog import Catalog
from amplifier_session_catalog.discovery import Discovery


def test_first_run_without_projects_is_complete_empty_and_read_only(tmp_path):
    home=tmp_path/'home';home.mkdir()
    settings=home/'settings.yaml';settings.write_text('owned fixture')
    catalog=Catalog(tmp_path/'index.sqlite')
    state=Discovery(catalog,[home],initially_empty_homes=[home]).scan()
    assert state['phase']=='complete' and state['issues']==0
    assert state['projects']==state['sessions']==state['metadataReads']==0
    assert catalog.list(connectionId='empty')['items']==[]
    assert sorted(p.name for p in home.iterdir())==['settings.yaml']
    assert settings.read_text()=='owned fixture'
    assert Discovery(Catalog(catalog.path),[home],initially_empty_homes=[home]).scan()['issues']==0


def test_missing_previously_observed_empty_namespace_stays_issue_after_restart(tmp_path):
    home=tmp_path/'home';projects=home/'projects';projects.mkdir(parents=True)
    catalog=Catalog(tmp_path/'index.sqlite')
    assert Discovery(catalog,[home],initially_empty_homes=[home]).scan()['issues']==0
    projects.rmdir()
    assert Discovery(Catalog(catalog.path),[home],initially_empty_homes=[home]).scan()['issues']==1
    assert not projects.exists()


@pytest.mark.parametrize('source',['native','retained-host'])
def test_missing_namespace_with_preexisting_indexed_history_is_not_first_run(tmp_path,source):
    home=tmp_path/'home';home.mkdir();catalog=Catalog(tmp_path/'index.sqlite')
    storage=home/'projects'/'owned-project'/'sessions'/'native-one'
    catalog.upsert({'uri':'ahp-session:/one','engineId':'amplifier','nativeSessionId':'native-one',
        'workingDirectory':str(tmp_path),'storagePath':str(storage)},
        source='native:'+str(home) if source=='native' else 'host')
    before=catalog.get('ahp-session:/one')
    state=Discovery(Catalog(catalog.path),[home],initially_empty_homes=[home]).scan()
    assert state['issues']==1 and state['metadataReads']==0
    assert catalog.get('ahp-session:/one')==before


@pytest.mark.parametrize('fault',['missing-home','home-file','projects-file','dangling-projects-link','permission'])
def test_first_run_does_not_suppress_other_root_failures(tmp_path,monkeypatch,fault):
    home=tmp_path/'home';catalog=Catalog(tmp_path/'index.sqlite')
    if fault=='home-file':home.write_text('not a directory')
    elif fault!='missing-home':home.mkdir()
    if fault=='projects-file':(home/'projects').write_text('not a directory')
    elif fault=='dangling-projects-link':(home/'projects').symlink_to(home/'absent')
    elif fault=='permission':
        original=os.scandir
        def denied(path):
            if Path(path)==home/'projects':raise PermissionError('owned fixture permission refusal')
            return original(path)
        monkeypatch.setattr(os,'scandir',denied)
    if fault in {'missing-home','home-file'}:
        with pytest.raises(ValueError,match='configured canonical existing directory'):
            Discovery(catalog,[home],initially_empty_homes=[home])
    else:
        assert Discovery(catalog,[home],initially_empty_homes=[home]).scan()['issues']==1


def test_corrupt_metadata_remains_issue_and_no_native_file_changes(tmp_path):
    home=tmp_path/'home';directory=home/'projects'/'owned'/'sessions'/'one';directory.mkdir(parents=True)
    metadata=directory/'metadata.json';metadata.write_text('{broken')
    catalog=Catalog(tmp_path/'index.sqlite')
    assert Discovery(catalog,[home],initially_empty_homes=[home]).scan()['issues']>0
    assert metadata.read_text()=='{broken'


def test_first_run_then_native_creation_is_discovered_without_replaying_history(tmp_path,monkeypatch):
    from amplifier_session_catalog.discovery import project_slug
    home=tmp_path/'home';home.mkdir();catalog=Catalog(tmp_path/'index.sqlite');discovery=Discovery(catalog,[home],initially_empty_homes=[home])
    assert discovery.scan()['issues']==0
    directory=home/'projects'/project_slug(tmp_path)/'sessions'/'one';directory.mkdir(parents=True)
    (directory/'metadata.json').write_text('{"session_id":"one","working_dir":'+json.dumps(str(tmp_path))+',"title":"First chat"}')
    transcript=directory/'transcript.jsonl';transcript.write_text('owned canonical bytes')
    original=Path.open
    def no_bodies(path,*args,**kwargs):
        if path.name in {'transcript.jsonl','events.jsonl'}:raise AssertionError('Discovery opened canonical history')
        return original(path,*args,**kwargs)
    with monkeypatch.context() as patch:
        patch.setattr(Path,'open',no_bodies)
        state=discovery.scan()
    assert state['issues']==0 and state['sessions']==1
    assert catalog.list(connectionId='created')['items'][0]['title']=='First chat'
    assert transcript.read_text()=='owned canonical bytes'
    shutil.rmtree(home/'projects')
    assert Discovery(Catalog(catalog.path),[home],initially_empty_homes=[home]).scan()['issues']==1


def test_missing_projects_without_explicit_fresh_declaration_remains_issue(tmp_path):
    home=tmp_path/'home';home.mkdir();catalog=Catalog(tmp_path/'index.sqlite')
    assert Discovery(catalog,[home]).scan()['issues']==1


@pytest.mark.parametrize('kind',['not-configured','symlink','noncanonical','relative','missing','file'])
def test_invalid_fresh_declarations_are_rejected(tmp_path,kind):
    home=tmp_path/'home';home.mkdir();catalog=Catalog(tmp_path/'index.sqlite');declared=home
    if kind=='not-configured':declared=tmp_path/'other';declared.mkdir()
    elif kind=='symlink':declared=tmp_path/'alias';declared.symlink_to(home,target_is_directory=True)
    elif kind=='noncanonical':declared=home/'..'/'home'
    elif kind=='relative':declared=Path('relative')
    elif kind=='missing':declared=tmp_path/'missing'
    elif kind=='file':declared=tmp_path/'file';declared.write_text('owned fixture')
    with pytest.raises(ValueError,match='configured canonical existing directory'):
        Discovery(catalog,[declared if kind!='not-configured' else home],initially_empty_homes=[declared])


def test_cli_fresh_declaration_is_explicit_repeatable_and_scoped(tmp_path):
    homes=[tmp_path/'one',tmp_path/'two']
    for home in homes:home.mkdir()
    command=[sys.executable,'-B','-m','amplifier_session_catalog','scan','--db',str(tmp_path/'index.sqlite')]
    for home in homes:command+=['--home',str(home)]
    default=subprocess.run(command,text=True,capture_output=True,check=True)
    assert json.loads(default.stdout)['issues']==2
    for home in homes:command+=['--initially-empty-home',str(home)]
    opted=subprocess.run(command,text=True,capture_output=True,check=True)
    assert json.loads(opted.stdout)['issues']==0 and all(not (home/'projects').exists() for home in homes)
    bad=subprocess.run(command+['--initially-empty-home',str(tmp_path/'unconfigured')],text=True,capture_output=True)
    assert bad.returncode!=0 and 'configured canonical existing directory' in bad.stderr
