"""Installed causal probe: source read recovers without another notice or scan."""
import hashlib
import json
from pathlib import Path
import tempfile

import amplifier_session_catalog.discovery as module
from amplifier_session_catalog import Catalog
from amplifier_session_catalog.discovery import Discovery, project_slug
from amplifier_session_catalog.hints import HintInbox

with tempfile.TemporaryDirectory(prefix='catalog-hint-retry-qualified-') as temporary:
    root=Path(temporary).resolve();workspace=root/'workspace';workspace.mkdir()
    home=root/'native';directory=home/'projects'/project_slug(workspace)/'sessions'/'saved';directory.mkdir(parents=True)
    (directory/'metadata.json').write_text(json.dumps({'working_dir':str(workspace),'name':'Saved work','parent_id':None,'turn_count':1}))
    (directory/'transcript.jsonl').write_bytes(b'opaque fixture history')
    # A healthy independent index proves this fixture is eligible for discovery.
    control=Catalog(root/'control.sqlite');healthy=Discovery(control,[home]);healthy.hint(str(directory))
    eligible=bool(control.list(connectionId='control')['items']);assert eligible
    catalog=Catalog(root/'subject.sqlite');discovery=Discovery(catalog,[home]);inbox=HintInbox(root/'hints',discovery)
    path=inbox.directory/(hashlib.sha256(str(directory).encode()).hexdigest()+'.json')
    path.write_text(json.dumps({'version':1,'sessionDirectory':str(directory)}))
    original=module.read_metadata_json
    def unavailable(*args,**kwargs):raise PermissionError('fixture temporary metadata read')
    module.read_metadata_json=unavailable
    try:first=inbox.drain()
    finally:module.read_metadata_json=original;inbox.close()
    pending=sum(1 for _ in inbox.directory.iterdir())
    second=inbox.drain();inbox.close()
    report={'fixtureOnly':True,'package':module.__file__,'healthyControlEligible':eligible,
            'temporaryFailure':'PermissionError at bounded native reader','firstDrain':first,
            'pendingClaimsAfterFailure':pending,'secondDrainAfterSourceReadableAgain':second,
            'discoveredWithoutNewNoticeOrScan':bool(catalog.list(connectionId='recovery')['items']),
            'transcriptBytesUnchanged':(directory/'transcript.jsonl').read_bytes()==b'opaque fixture history'}
    print(json.dumps(report,indent=2))
