"""Live indexed library pages, with bounded current Host facts and query guards."""
import hashlib
import json
import time
import uuid
from .catalog import LibraryRefreshRequired, InvalidCursor, authorized_scope, checked_string, managed_roots, timestamp


def library_query(catalog, *, connectionId, allowedWorkspaceRoots, limit=50, cursor=None,
                  workingDirectory=None, search=None, parentUri=None, archive='active',
                  sort='activity', activity='all', location='all', allowedManagedRoots=None,
                  liveFacts=None, observationFreshness=None):
    checked_string(connectionId,'connectionId',200)
    if type(limit) is not int or not 1<=limit<=50: raise ValueError('Library limit must be1..50')
    if sort not in {'activity','created','name'} or activity not in {'all','working','attention'} or location not in {'all','managed'} or archive not in {'active','all','archived'}: raise ValueError('Invalid library selector')
    if not isinstance(allowedWorkspaceRoots,list) or len(allowedWorkspaceRoots)>128: raise ValueError('Explicit bounded workspace grants required')
    roots=sorted(set(checked_string(r,'workspace root').rstrip('/') or '/' for r in allowedWorkspaceRoots))
    managed=managed_roots(allowedManagedRoots)
    for value,field,bound in [(workingDirectory,'workspace',8192),(parentUri,'parent',512),(search,'search',500)]:
        if value is not None: checked_string(value,field,bound)
    facts=liveFacts or {'source':'standalone','incarnation':'unverified','items':[]}
    if not isinstance(facts,dict) or set(facts)!={'source','incarnation','items'}: raise ValueError('Exact live fact envelope required')
    for field in ('source','incarnation'): checked_string(facts[field],field,200)
    if not isinstance(facts['items'],list) or len(facts['items'])>64 or len(json.dumps(facts).encode())>65536: raise ValueError('Live fact capacity exceeded; do not truncate')
    seen=set()
    for fact in facts['items']:
        if not isinstance(fact,dict) or set(fact)!={'uri','runtime','effect','modifiedAt'}: raise ValueError('Exact live fact required')
        uri=checked_string(fact['uri'],'live URI',512)
        if uri in seen or fact['runtime'] not in {'working','input-needed','idle','unknown'} or fact['effect'] not in {'unknown','error','settled','unverified'}: raise ValueError('Invalid live fact')
        seen.add(uri)
        if type(fact['modifiedAt']) not in (int,float) or timestamp(fact['modifiedAt'],-1)<0: raise ValueError('Invalid live timestamp')
    freshness=observationFreshness or {'phase':'indexed','issues':[]}
    if not isinstance(freshness,dict) or set(freshness)!={'phase','issues'} or freshness['phase'] not in {'indexed','reconciling','complete','degraded'} or not isinstance(freshness['issues'],list) or len(freshness['issues'])>16: raise ValueError('Invalid observation freshness')
    for issue in freshness['issues']: checked_string(issue,'freshness issue',200)
    query=dict(connectionId=connectionId,allowedWorkspaceRoots=roots,allowedManagedRoots=managed,workingDirectory=workingDirectory,search=search,parentUri=parentUri,archive=archive,sort=sort,activity=activity,location=location,source=facts['source'],incarnation=facts['incarnation'])
    scope=hashlib.sha256(json.dumps(query,sort_keys=True).encode()).hexdigest()
    where=['s.visible=1',"s.kind='child' AND s.parent_uri=?" if parentUri else "s.kind='root'"]
    args=[parentUri] if parentUri else []
    auth,grants=authorized_scope(roots,managed);where.append(auth);args+=grants
    if workingDirectory: where.append('s.workspace=?');args.append(workingDirectory)
    if archive!='all': where.append("s.uri "+('IN' if archive=='archived' else 'NOT IN')+" (SELECT uri FROM session_archive WHERE archived=1)")
    if location=='managed': where.append('EXISTS(SELECT 1 FROM managed_locations ml WHERE ml.uri=s.uri)')
    if search:
        where.append("("+' OR '.join('instr(lower('+field+'),lower(?))>0' for field in ['s.title','s.uri','s.native_id','s.workspace','wi.name'])+")")
        args += [search]*5
    pred=' AND '.join(where)
    effect="coalesce(json_extract(s.details,'$.hostObservation.effect'),'unverified')"
    base_kind=f"CASE WHEN {effect} IN ('error','unknown') THEN 'attention' ELSE 'unknown' END"
    live_effect="json_extract(lf.value,'$.effect')";live_runtime="json_extract(lf.value,'$.runtime')"
    live_kind=f"CASE WHEN {live_effect} IN ('error','unknown') OR {live_runtime}='input-needed' THEN 'attention' WHEN {live_runtime}='working' THEN 'working' WHEN {live_runtime}='idle' THEN 'idle' ELSE 'unknown' END"
    key={'activity':'s.modified_at','created':'s.created_at','name':'lower(s.title)'}[sort]
    live_key="json_extract(lf.value,'$.modifiedAt')" if sort=='activity' else key
    joins=' JOIN workspaces w ON w.path=s.workspace LEFT JOIN workspace_index wi ON wi.path=s.workspace '
    common="s.*,COALESCE((SELECT archived FROM session_archive a WHERE a.uri=s.uri),0) AS is_archived,w.availability AS workspace_availability,"
    base=f"SELECT {common}{key} AS library_key,{base_kind} AS library_kind,'unknown' AS library_runtime,{effect} AS library_effect,'indexed' AS library_evidence FROM sessions s {joins} WHERE {pred} AND s.uri NOT IN (SELECT json_extract(value,'$.uri') FROM json_each(?))"
    base_args=[*args,json.dumps(facts['items'])]
    if activity=='working': base+=' AND 0'
    elif activity=='attention': base+=f" AND {effect} IN ('error','unknown')"
    live=f"SELECT {common}{live_key} AS library_key,{live_kind} AS library_kind,{live_runtime} AS library_runtime,{live_effect} AS library_effect,'current-host' AS library_evidence FROM json_each(?) lf CROSS JOIN sessions s ON s.uri=json_extract(lf.value,'$.uri') {joins} WHERE {pred}"
    live_args=[json.dumps(facts['items']),*args]
    if activity!='all': live+=f' AND ({live_kind})=?';live_args.append(activity)
    ascending=sort=='name';direction='ASC' if ascending else 'DESC'
    with catalog.library_lock, catalog.connect() as db:
        db.execute('BEGIN')
        live_rows=db.execute(live,live_args).fetchall() # <=64 input facts, never the estate
        stamp=hashlib.sha256(json.dumps([(r['uri'],r['library_key']) for r in sorted(live_rows,key=lambda r:r['uri'])]).encode()).hexdigest()
        now=time.time()
        for identity in list(catalog.library_guards):
            if catalog.library_guards[identity]['expires']<=now: del catalog.library_guards[identity]
        guard_id=None;decoded=None
        if cursor:
            decoded=catalog._decode(cursor)
            if decoded.get('library')!=1 or decoded.get('scope')!=scope: raise InvalidCursor('Library cursor belongs to another query or authority')
            guard_id=decoded.get('guard');guard=catalog.library_guards.get(guard_id)
            if not guard or guard['dirty'] or guard['liveStamp']!=stamp: raise LibraryRefreshRequired('Library changed or expired; refresh the first page')
            catalog.library_guards.move_to_end(guard_id)
        parts=[];sql_args=[]
        for statement,bindings,order_key in [(base,base_args,key),(live,live_args,live_key)]:
            if decoded:
                parts += [statement+f' AND {order_key}=? AND s.uri>?',statement+f" AND {order_key}{'>' if ascending else '<'}?"]
                sql_args += [*bindings,decoded['key'],decoded['uri'],*bindings,decoded['key']]
            else: parts.append(statement);sql_args+=bindings
        sql=' UNION ALL '.join(parts)+f' ORDER BY library_key {direction},uri ASC LIMIT ?'
        rows=db.execute(sql,[*sql_args,limit+1]).fetchall()
        items=[]
        for row in rows[:limit]:
            item=catalog._record(row)
            item['status']=row['library_runtime']
            item['activity']={'kind':row['library_kind'],'runtime':row['library_runtime'],'effect':row['library_effect'],'evidence':row['library_evidence']}
            observation=item.get('hostObservation')
            if observation: item['activity']['observedAt']=observation['observedAt']
            items.append(item)
        result={'version':1,'items':items,'freshness':catalog.progress(),'observationFreshness':freshness,'paging':'live','countsAvailable':False,'externalActivityCoverage':False}
        if len(rows)>limit:
            if guard_id is None:
                guard_id=str(uuid.uuid4())
                # Tiny selector guards only; no retained members or row snapshots.
                same=[identity for identity,g in catalog.library_guards.items() if g['query']['connectionId']==connectionId]
                for identity in same[:-15]: del catalog.library_guards[identity]
                catalog.library_guards[guard_id]={'query':query,'dirty':False,'liveStamp':stamp,'expires':now+3600}
                while len(catalog.library_guards)>256: catalog.library_guards.popitem(last=False)
            last=rows[limit-1]
            result['nextCursor']=catalog._cursor({'library':1,'scope':scope,'guard':guard_id,'key':last['library_key'],'uri':last['uri'],'expires':catalog.library_guards[guard_id]['expires']})
        return result
