"""Synthetic catalog scale receipt; no native data, model or user account access."""
import argparse
import json
from pathlib import Path
import resource
import statistics
import subprocess
import sys
import tempfile
import time
from amplifier_session_catalog import Catalog


def fixture(path, sessions):
    catalog = Catalog(path)
    with catalog.connect() as db:
        db.executemany('INSERT INTO workspaces VALUES(?,?,?)',
                       ((f'/fixture/project-{i}', 'present' if i < 100 else 'missing', 1) for i in range(4000)))
        db.executemany('INSERT INTO sessions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            ((f'ahp-session:/{i:08d}', 'amplifier', str(i), f'/fixture/project-{i%4000}',
              None, 'root' if i%3 == 0 else 'child', 'Synthetic conversation', 1, i + 1000,
              'available', 'fixture', None, None, None, None, 1, '{}', int(i%4000 < 100))
             for i in range(sessions)))


def probe(path):
    catalog=Catalog(path)
    samples, writes, payloads = [], [], []
    for i in range(200):
        start=time.perf_counter()
        result=catalog.list(connectionId='fixture',limit=50)
        samples.append((time.perf_counter()-start)*1000)
        payloads.append(len(json.dumps(result).encode()))
        row=result['items'][0]
        row['title']=f'Updated {i}'
        start=time.perf_counter();catalog.upsert(row);writes.append((time.perf_counter()-start)*1000)
    def summary(values):
        return {'p50Ms':round(statistics.median(values),3),'p95Ms':round(sorted(values)[189],3)}
    rss=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/(1024**2 if sys.platform=='darwin' else 1024)
    return {'list50':summary(samples),'singleSummaryUpdate':summary(writes),
            'maximumPageBytes':max(payloads),'freshProcessMaxRssMiB':round(rss,2),'rounds':len(samples)}


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--probe');args=parser.parse_args()
    if args.probe:
        print(json.dumps(probe(args.probe)));return
    rows=[]
    with tempfile.TemporaryDirectory(prefix='amplifier-catalog-benchmark-') as directory:
        for count in (25000,100000):
            path=Path(directory)/f'{count}.sqlite';fixture(path,count)
            run=subprocess.run([sys.executable,__file__,'--probe',str(path)],capture_output=True,text=True,check=True)
            rows.append({'indexedSessions':count,'indexedWorkspaces':4000,'presentWorkspaces':100,**json.loads(run.stdout)})
    print(json.dumps({'scope':'Synthetic component benchmark, fresh process, warm filesystem cache. Not UI, host, Spark, full native discovery, or model latency.','results':rows},indent=2))


if __name__=='__main__':main()
