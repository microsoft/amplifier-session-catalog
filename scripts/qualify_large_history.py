"""Installed-package qualification on owned physical fixtures; never real history."""
import argparse
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import resource
import subprocess
import sys
import tempfile
import time

from amplifier_session_catalog import Catalog, InvalidCursor
from amplifier_session_catalog.discovery import Discovery, project_slug
from amplifier_session_catalog.hints import HintInbox


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--output',required=True);args=parser.parse_args()
    report={'scope':'Owned synthetic physical metadata, installed package, warm local filesystem; no live/browser/account acceptance',
            'package':__import__('amplifier_session_catalog').__file__,'projects':4000,'sessions':25000,'existingWorkspaces':100}
    with tempfile.TemporaryDirectory(prefix='catalog-physical-qualification-') as temp:
        root=Path(temp).resolve();home=root/'native';workspace_root=root/'projects';workspace_root.mkdir();app=root/'adapter'
        originals={};project_paths=[]
        start=time.monotonic()
        for project in range(4000):
            workspace=workspace_root/f'project-{project:04d}';project_paths.append(workspace)
            if project<100:workspace.mkdir()
            sessions=home/'projects'/project_slug(workspace)/'sessions';sessions.mkdir(parents=True)
            for number in range(7 if project<1000 else 6):
                identity=f's{number:02d}';directory=sessions/identity;directory.mkdir()
                (directory/'metadata.json').write_text(json.dumps({'working_dir':str(workspace),'name':f'Project {project} conversation {number}',
                    'parent_id':None if number==0 else 's00','turn_count':1,'created_at':1}))
                (directory/'transcript.jsonl').write_bytes(b'opaque canonical native history\n')
                (directory/'events.jsonl').write_bytes(b'opaque canonical native events\n')
        report['fixtureBuildSeconds']=round(time.monotonic()-start,3)
        catalog=Catalog(root/'index.sqlite');discovery=Discovery(catalog,[home],app_homes=[app]);reads=[];writes=[];writer=False
        # Python audit hooks also cover built-in open, not only os.open. They
        # reject transcript/event bodies and every catalog-origin native write.
        def audit(event,event_args):
            if event!='open' or not isinstance(event_args[0],(str,bytes,os.PathLike)):return
            path=Path(os.fsdecode(event_args[0]))
            if not path.is_relative_to(home) and not path.is_relative_to(app):return
            mode,flags=event_args[1:3];write=bool(flags&(os.O_WRONLY|os.O_RDWR|os.O_CREAT|os.O_TRUNC|os.O_APPEND))
            if write:
                assert writer,'Catalog wrote canonical fixture source'
                writes.append(str(path));return
            assert path.name not in {'transcript.jsonl','transcript.jsonl.backup','events.jsonl'},'Native history body read'
            reads.append(str(path))
        sys.addaudithook(audit)
        start=time.monotonic();scan=discovery.scan();report['initialScanSeconds']=round(time.monotonic()-start,3)
        assert scan['phase']=='complete' and scan['issues']==0 and scan['sessions']==25000 and scan['projects']==4000
        assert scan['metadataReads']==len(reads)==25000
        report['initialScan']=scan
        with catalog.connect() as db:
            assert db.execute('SELECT count(*) FROM sessions').fetchone()[0]==25000
            assert db.execute('SELECT count(*) FROM workspace_index').fetchone()[0]==4000
            assert db.execute("SELECT count(*) FROM workspace_index WHERE availability='present'").fetchone()[0]==100
        baseline_reads=len(reads);original_connect=catalog.connect;steps=[0]
        @contextmanager
        def measured():
            with original_connect() as db:
                db.set_progress_handler(lambda:(steps.__setitem__(0,steps[0]+1) or 0),1);yield db
        catalog.connect=measured
        def pages(method,**params):
            cursor=None;total=0;maximum=0;rounds=0;maxsteps=0;samples=[];first=None
            while True:
                steps[0]=0;start=time.monotonic();page=method(**params,**({'cursor':cursor} if cursor else {}));samples.append((time.monotonic()-start)*1000)
                first=first or page;maximum=max(maximum,len(json.dumps(page).encode()));total+=len(page['items']);rounds+=1;maxsteps=max(maxsteps,steps[0]);cursor=page.get('nextCursor')
                assert len(page['items'])<=params['limit']
                if not cursor:break
            return {'rows':total,'pages':rounds,'maxPageBytes':maximum,'maxSqliteVmSteps':maxsteps,'maxMs':round(max(samples),3)},first
        workspace_stats,_=pages(catalog.list_workspaces,connectionId='project-client',limit=25,allowedWorkspaceRoots=[str(workspace_root)])
        root_stats,first=pages(catalog.list,connectionId='session-client',limit=25,allowedWorkspaceRoots=[str(workspace_root)])
        assert workspace_stats['rows']==root_stats['rows']==100 and len(reads)==baseline_reads
        assert all(row['kind']=='root' and Path(row['workingDirectory']).is_dir() for row in first['items'])
        selected=first['items'][0];child_stats,_=pages(catalog.list,connectionId='child-client',parentUri=selected['uri'],limit=2)
        assert child_stats['rows']==6 and len(reads)==baseline_reads
        report['hotBrowsing']={'workspaces':workspace_stats,'roots':root_stats,'selectedChildren':child_stats,'nativeBodyOpens':0,'maxRowsHeldPerPage':26}
        assert max(item['maxPageBytes'] for item in (workspace_stats,root_stats,child_stats))<256*1024
        assert max(item['maxSqliteVmSteps'] for item in (workspace_stats,root_stats,child_stats))<25000
        # Cold client cache, changed connection scope, and fresh process secrets.
        try:catalog.list(connectionId='reconnected',limit=25,allowedWorkspaceRoots=[str(workspace_root)],cursor=first['nextCursor'])
        except InvalidCursor:pass
        else:raise AssertionError('Foreign connection cursor accepted')
        restarted=Catalog(catalog.path)
        try:restarted.list(connectionId='session-client',limit=25,allowedWorkspaceRoots=[str(workspace_root)],cursor=first['nextCursor'])
        except InvalidCursor:pass
        else:raise AssertionError('Prior-process cursor accepted')
        recovered=restarted.list(connectionId='cache-lost',limit=25,allowedWorkspaceRoots=[str(workspace_root)])
        assert [r['uri'] for r in recovered['items']]==[r['uri'] for r in first['items']] and len(reads)==baseline_reads
        report['recovery']={'clientCacheLoss':'fresh first page identical','connectionChange':'old cursor refused','processRestart':'old cursor refused; metadata index retained','nativeBodyOpens':0}
        # Actual fresh stdio owners reject all native body opens during browsing.
        bootstrap="""import sys,json,resource
from pathlib import Path
from amplifier_session_catalog import Catalog
from amplifier_session_catalog.discovery import Discovery
from amplifier_session_catalog.__main__ import Server
home=Path(sys.argv[2]);opens=[]
def audit(event,args):
 if event=='open' and isinstance(args[0],str) and Path(args[0]).is_relative_to(home):
  opens.append(args[0]);raise AssertionError('Browsing opened native source')
sys.addaudithook(audit)
catalog=Catalog(sys.argv[1]);Server(catalog,Discovery(catalog,[home])).serve()
print(json.dumps({'nativeBodyOpens':len(opens),'peakRss':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}),file=sys.stderr)
"""
        cursor=None;owners=[]
        for process_number in range(2):
            process=subprocess.Popen([sys.executable,'-I','-B','-c',bootstrap,str(catalog.path),str(home)],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
            def rpc(identity,params):
                process.stdin.write(json.dumps({'jsonrpc':'2.0','id':identity,'method':'list','params':params})+'\n');process.stdin.flush();return json.loads(process.stdout.readline())
            request={'connectionId':'stdio-client','limit':25,'allowedWorkspaceRoots':[str(workspace_root)]}
            if cursor:assert rpc(1,{**request,'cursor':cursor})['error']['code']==-32602
            response=rpc(2,request)['result'];assert [r['uri'] for r in response['items']]==[r['uri'] for r in first['items']]
            cursor=response['nextCursor'];assert rpc(3,{**request,'connectionId':'reconnect','cursor':cursor})['error']['code']==-32602
            assert len(rpc(4,{**request,'cursor':cursor})['result']['items'])==25
            process.stdin.close();process.wait(timeout=10);assert process.returncode==0
            owners.append(json.loads(process.stderr.read()))
        report['recovery']['freshStdioOwners']=owners
        # External CLI state is discovered only from explicit location-only
        # announcements and configured identity-matching tombstones.
        directory=Path(catalog.get(selected['uri'])['storagePath']);metadata=directory/'metadata.json';originals[metadata]=hashlib.sha256(metadata.read_bytes()).hexdigest()
        before=len(reads);writer=True
        value=json.loads(metadata.read_text());value['name']='External legacy CLI rename';metadata.write_text(json.dumps(value));writer=False
        before=len(reads);inbox=HintInbox(root/'hints',discovery)
        def notice():
            target=inbox.directory/(hashlib.sha256(str(directory).encode()).hexdigest()+'.json')
            target.write_text(json.dumps({'version':1,'sessionDirectory':str(directory)}));return target
        notice();assert inbox.drain(limit=1)['processed']==1;inbox.close()
        assert catalog.get(selected['uri'])['title']=='External legacy CLI rename' and len(reads)-before==1
        marker=app/'sessions'/selected['nativeSessionId']/'lifecycle.json';marker.parent.mkdir(parents=True)
        writer=True;marker.write_text(json.dumps({'sessionId':selected['nativeSessionId'],'historyCwd':selected['workingDirectory'],'deleted':True,'commandId':'cli-delete'}));writer=False
        before=len(reads);notice();assert inbox.drain(limit=1)['processed']==1;inbox.close()
        assert catalog.get(selected['uri'])['nativeDeleted'] and len(reads)-before==2
        assert not catalog.list(connectionId='deleted-root',workingDirectory=selected['workingDirectory'])['items']
        writer=True;marker.write_text(json.dumps({'sessionId':selected['nativeSessionId'],'historyCwd':selected['workingDirectory'],'deleted':False,'commandId':'cli-restore'}));writer=False
        before=len(reads);notice();assert inbox.drain(limit=1)['processed']==1;inbox.close()
        assert not catalog.get(selected['uri'])['nativeDeleted'] and len(reads)-before==2
        before=len(reads);notice();assert inbox.drain(limit=1)['processed']==1;inbox.close();assert len(reads)==before
        report['incremental']={'renameReads':1,'tombstoneReads':2,'restoreReads':2,'unchangedNoticeReads':0,'wholeScanAfterInitial':False,'requiresWriterAnnouncement':True,'nativeWritesByCatalog':0}
        # Retention count is deliberately disk-wide matching metadata, while its
        # result and Python working set remain paged and it performs no source I/O.
        before=len(reads);steps[0]=0
        retention=catalog.retention_candidates(connectionId='retention',modifiedBefore=time.time()+3600,allowedWorkspaceRoots=[str(workspace_root)],limit=25)
        assert retention['candidateCount']==100 and len(retention['items'])==25 and len(reads)==before
        assert retention['removalAuthorized'] is False and retention['nativeFilesRead'] is False
        report['retention']={'candidateCount':100,'pageRows':25,'pageBytes':len(json.dumps(retention).encode()),'sqliteVmSteps':steps[0],'nativeBodyOpens':0,'countScope':'explicit matching disk index; not constant-work','removalAuthorized':False}
        # A late unannounced child cannot hide behind the unchanged index token.
        proof=discovery.reconcile_family(parentUri=selected['uri'],maxEntries=512)['familyDiscovery'];assert proof['phase']=='complete'
        old_revision=catalog.descendants(parentUri=selected['uri'])['revision'];late=directory.parent/'late-child';late.mkdir()
        writer=True;(late/'metadata.json').write_text(json.dumps({'working_dir':selected['workingDirectory'],'parent_id':selected['nativeSessionId'],'turn_count':1}));(late/'transcript.jsonl').write_bytes(b'opaque late history');writer=False
        before=len(reads);verification=discovery.verify_family(parentUri=selected['uri'],verificationToken=proof['verificationToken'])
        assert not verification['valid'] and len(reads)==before and catalog.descendants(parentUri=selected['uri'])['revision']==old_revision
        report['familyRace']={'unannouncedNewChild':'refused','pureRevisionUnchanged':True,'verificationBodyReads':0,'nativeOwnershipVerified':False}
        catalog.connect=original_connect
        with catalog.connect() as db:db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        report['diskIndexBytes']=catalog.path.stat().st_size
        report['freshProcessPeakRssMiB']=round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/(1024**2 if sys.platform=='darwin' else 1024),2)
        assert report['freshProcessPeakRssMiB']<128
        report['limits']=['Fixture scale and warm local filesystem only','No native transcript/event body reads; no catalog-origin canonical writes','No global dormant Python history cache; paged results only','CLI must emit explicit configured location notices; no automatic watcher or CLI integration is claimed','Catalog token does not exclude noncooperating writers; hosts still require native leases','Browser, Spark, physical device and real-account acceptance not established']
    Path(args.output).write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))


if __name__=='__main__':main()
