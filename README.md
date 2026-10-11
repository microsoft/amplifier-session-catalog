# Amplifier session catalog

Start with the [local vision](VISION.md), [catalog contract v1](contracts/catalog.v1.md)
and [contributor entry](AGENTS.md). The existing API reference below defines method
shapes; the contract maps approved promises to checks and remaining qualification.

A rebuildable SQLite index for workspace and native session discovery. It never
loads an agent, imports Amplifier's kernel, reads event/transcript contents, or
writes native history. Interactive list/get operations query the index only.

Verified inactive installation restores may use the offline
[restored source binding projection](contracts/restored-sources.v1.md) before
starting discovery. Ordinary hints cannot move an existing binding.

Default lists contain root conversations in confirmed existing workspace
directories. Child discovery is explicit; independent CLI forks remain roots.
Unknown/inaccessible directories remain distinct from confirmed missing paths.
Neither exclusion nor index loss deletes native records.

## Run

```sh
uv sync --extra test
uv run pytest -q
uv run amplifier-session-catalog serve --db /absolute/private/catalog.sqlite \
  --home /absolute/native-amplifier-home --scan-on-start
```

Multiple `--home` and `--workspace` arguments are allowed. Registered workspace
paths help resolve older metadata; project slugs are never decoded into guessed
filesystem paths. Only initial/background discovery reads metadata. `scan` runs
synchronously as a standalone CLI; the `scan` RPC schedules background work and
returns promptly. `status` reports progress. Metadata reads are capped at 8 MiB
per file; requests are capped at 1 MiB and the request queue is bounded.

The long-lived service reconciles metadata every 300 seconds and rechecks at most
64 workspace paths every 15 seconds on a background thread. Configure these with
`--scan-interval` and `--workspace-check-interval`; zero disables either schedule.
Location-only writer hints provide prompt discovery between reconciliations.
Workspace existence is an indexed observation, not a permanent claim about a mount.
Maintenance exceptions are reported on stderr without resetting the index.

A trusted fresh-deployment caller may repeat `--initially-empty-home /exact/home`
for an existing canonical directory also configured by the same exact `--home`.
The Python equivalent is `Discovery(..., initially_empty_homes=[...])`. This
explicit caller declaration permits a never-observed, absent `projects` child to
complete as empty; it does not create native paths or infer pristine history from
an empty index. Without the declaration, missing namespaces remain issues.
Previously observed namespaces, including empty ones, and retained indexed native
sources or host storage mappings keep missing-root failures even with the declaration. Permission,
non-directory, dangling-link and corrupt-metadata failures remain issues. Symlink,
noncanonical, missing, non-directory or unconfigured declared homes are rejected.
Private namespace observations are rebuildable index state, not native authority.
A successful legacy scan has no per-home observations, so its retained completion
conservatively prevents a new empty-home declaration from excusing missing roots.
Accepted first-run empty declarations are remembered separately, permitting their
repeated scans while prior namespace observations and indexed history still win.
A caller must not declare a retained or index-loss recovery home fresh: history
lost before any retained catalog observation cannot be distinguished from first
use by the index alone.

## Stdio interface v1

UTF-8 newline-delimited JSON-RPC 2.0 on stdin/stdout. Responses may arrive out of
order and must be matched by ID. Diagnostics go to stderr. This is a local trusted
component interface, not an unauthenticated network server. The caller owns user
authorization and passes its authorized workspace roots to list queries.

```json
{"jsonrpc":"2.0","id":1,"method":"list","params":{"connectionId":"ahp-connection-uuid","limit":50}}
{"jsonrpc":"2.0","id":2,"method":"get","params":{"uri":"ahp-session:/uuid"}}
{"jsonrpc":"2.0","id":3,"method":"upsert","params":{"record":{"uri":"ahp-session:/uuid","engineId":"amplifier","nativeSessionId":"native-id","workingDirectory":"/work/project","title":"Conversation","createdAt":"2026-10-02T00:00:00Z","modifiedAt":"2026-10-02T01:00:00Z"}}}
{"jsonrpc":"2.0","id":4,"method":"hint","params":{"sessionDirectory":"/native-home/projects/-work-project/sessions/native-id"}}
{"jsonrpc":"2.0","id":5,"method":"scan","params":{}}
{"jsonrpc":"2.0","id":6,"method":"status","params":{}}
{"jsonrpc":"2.0","id":7,"method":"recheck","params":{"limit":64}}
```

| Method | Result / semantics |
| --- | --- |
| `list` | `{items, nextCursor?, freshness}`; params `connectionId` required, `limit` 1–200/default50, `cursor?`, `workingDirectory?`, `search?`, `parentUri?`, `includeArchive?` false, `allowedWorkspaceRoots?`, `allowedManagedRoots?` |
| `get` | One record or null, no source reads; caller must authorize its workspace before returning it |
| `upsert` | The current indexed record; no native file mutation. A host should use the discovered URI when adopting a discovered record |
| `hint` | Read current small metadata at the allowed native location; returns `{uri?,changed,issues?}`. Hints contain locations, not trusted replacement metadata |
| `scan` / `status` | Schedule/retrieve background scan; missing hints are recovered by repeated scans. No interactive list triggers a scan |
| `recheck` | Revalidate up to `limit` oldest workspace existence facts; returns updated facts. Normally the service schedules this outside interactive request latency |
| `workspace` | Explicit `{path,availability?}` refresh; availability values present/missing/unknown. Used for known offline sources as well as normal probes |
| `resolveNative` | One-time migration lookup: `{engineId,identities,allowedWorkspaceRoots}` (at most 500 identities). Indexed, scoped results map IDs to `{status:"matched",uri}`, `ambiguous`, or `missing`. No filesystem scan, guessed path, or selection of one ambiguous match |
| `directoryInUse` | Trusted-host guard input `{path}` for a canonical absolute directory. Indexed exact/descendant workspace and native storage matches include archived, child and internal sessions. Returns `{inUse,sessions,truncated,coverage,freshness}`, with at most 100 explanations. No filesystem scan or native reads |

Directory-use coverage is explicitly `indexed-native-history`. An empty result
does not exclude an undiscovered or noncooperating external writer. The host owns
atomic create/relocate/remove admission and any native ownership checks. Discovery
freshness is `phase: not-started|running|complete|interrupted|failed`; scan reports
include `issues`, `startedAt`, and `completedAt` when settled. A destructive caller
must handle incomplete, issue-bearing and stale discovery conservatively.

Records contain `uri`, `engineId`, `nativeSessionId`, `workingDirectory`, `title`,
`createdAt`, `modifiedAt`, `indexedAt`, `kind` (root/child/internal),
`workspaceAvailability` (present/missing/unknown), and native `availability`
(available/owned/read-only/unsupported). Optional `parentUri`, `storagePath`,
`nativeProject`, `bundle`, `description`, `reason` carry bounded metadata.
`status:"idle"` is historical display information, never proof of execution
ownership. The host overlays actual runtime status and acquires native ownership
before execution. Availability `available` likewise does not certify a free lock.

Map into standard AHP `SessionSummary` in the host: `resource=uri`,
`provider=engineId`, timestamps unchanged and actual runtime status as appropriate.
Do not expose filesystem locators to unauthorized clients.

Cursors are opaque, authenticated, query/connection-bound and expire after one
hour. They also expire when this process restarts. Refetch from the first page
after AHP reconnect, catalog restart or changed query; never persist the cursor
across connections. The list is live, with deterministic modified-time/URI order;
concurrent modifications are reconciled through host catalog invalidation/refetch,
not a promise of snapshot isolation across multiple requests.
Continuation queries seek separate equal-timestamp and older-timestamp index
ranges. Already skipped roots or children, including large timestamp ties, do
not have to be walked again to fill the next page.

The visible-root index materializes only directory eligibility, updated when that
workspace's existence changes. Query cost does not walk all the newer invisible
historical entries merely to fill a page. State/cache loss rebuilds from metadata;
it does not replay work. No full-history memory cache is created.

## Verification boundary

Tests cover default filtering before paging, explicit children, independent forks,
native checksum preservation, corrupt-primary backup reads, cursor/connection
isolation, absence of source reads in queries, process restart, and SQLite work
remaining bounded after adding 25,000 newer invisible records. This is synthetic
component evidence, not Spark-1 scale or real-account execution acceptance.

`python scripts/benchmark.py` generates temporary 4,000-workspace catalogs with
25,000 and 100,000 sessions, then measures paged reads and individual summary
updates in fresh processes. It reports its evidence boundary with the results;
the fixture paths do not touch real native history.

`python scripts/qualify_large_history.py --output /absolute/receipt.json` builds
an owned physical fixture with 4,000 historical project namespaces, 25,000 native
metadata records and 100 existing workspaces. Run it with an independently
installed wheel. It checks existing-only project/root pages, lazy child pages,
fresh stdio owners and cursor rejection after reconnect/restart, explicitly
announced incremental metadata/tombstone updates, passive selected-family races
and bounded retention pages. Audit hooks reject transcript/event body reads and
catalog-origin canonical writes. Hot queries must stay below 25,000 SQLite VM
steps and 256 KiB per page, with at most 26 result rows held for a 25-row page;
the fixture process must remain below 128 MiB peak RSS. Initial reconstruction
and explicit retention counts are disk-wide metadata work, measured separately.
This qualifies synthetic announcements, not adoption by a real legacy CLI.

Native lifecycle visibility is a separate derived index. `setVisibility` accepts
an exact indexed `uri`, `nativeSessionId`, `workingDirectory`, `deleted` boolean,
`commandId`, and evidence `native-completed-receipt` (trusted host after native
confirmation) or `configured-native-marker` (discovery). It changes discovery
only; canonical files are never modified. Deleted rows are excluded before
paging, including archive browsing. Ordinary metadata updates and workspace
rechecks cannot revive them. Exact lookup reports `nativeDeleted` and retains
history references for directory-removal guards.

Configure repeated `--app-home` arguments with each explicit native ACP adapter
state home to discover out-of-band lifecycle markers at
`<appHome>/sessions/<nativeSessionId>/lifecycle.json`. Hints and periodic scanning
read only bounded, identity-matching markers, never transcript/event bodies.
There is no guessed user home. Invalid markers retain previous visibility and
produce an issue. A confirmed native restore marker or receipt re-enables the
row; an absent marker is not restore evidence.

## Optional workspace cleanup census (v1)

`libraryCapabilities.workspaceCleanupCensus.version=1` advertises the trusted
`workspaceCleanupCensus({})` metadata operation. It returns every indexed
identity and location (including hidden, child, internal, and archived records),
a revision hash, and explicit `indexed-metadata-only` coverage. More than 10,000
rows refuse the operation; no partial census is returned. It performs no source
discovery or transcript reads and makes no assertion about external writers.
The Host combines these facts with its own mappings under coordinated recovery
holds. Native filesystem deletion and product archival remain separately owned.

## Optional writer hint inbox (v1)

Writers which cannot reach the live catalog process may opt into a private local
spool. This is the same location-only trust boundary as `hint({sessionDirectory})`;
it is an efficiency hint, never a second session store or delivery receipt.

```sh
amplifier-session-catalog serve --db /private/catalog.sqlite \
  --home /configured/native-home --hint-directory /private/catalog-hints \
  --scan-on-start
```

Inbox v1 is POSIX-only; native RPC hints and ordinary scans remain portable.
The directory must be absolute, private to the same OS user (mode 0700 on POSIX),
and not a symlink. `--hint-interval` defaults to one second. Each background pass
examines at most 64 directory entries; it never runs on a catalog list/get query.
An incremental directory iterator prevents unrelated entries from forcing an
unbounded listing or starving later notices. The usual metadata reconciliation and
workspace checks remain enabled and repair missing/lost notices.

A writer emits this exact JSON object, at most 8 KiB, **after** its successful native
save: `{"version":1,"sessionDirectory":"/absolute/native/session/directory"}`.
The filename is lowercase SHA-256 of the UTF-8 absolute directory plus `.json`.
Write a mode-0600 unique temporary file and atomically replace that filename. Repeated
saves coalesce to one pending notice per session; no title, transcript, event payload,
or client state is accepted. Writers must not scan the spool, spawn a catalog, or
make successful native persistence depend on notice delivery. The spool has bounded
individual records and bounded consumer batches, not a global disk quota.

The consumer atomically claims a notice under `<hash>.processing-<32 hex chars>`
before reading it, so a later writer's notice cannot be deleted by the old consumer.
Crash claims are rediscovered. Invalid versions, filenames, roots, special files,
symlinks, and oversized records are rejected; transient index/I/O failures retain the
claim for a later batch. A private discovery adapter also retains a claim when
native metadata admission or authoritative lifecycle reads fail with typed transient
I/O/descriptor races, or a selected-source presence confirmation changes mid-check.
Public `hint` results and notice fields are unchanged. A failed native read does
not overwrite indexed metadata from another successful drainer. Transient
lifecycle failures also return before publishing native metadata, partial lifecycle
proofs or fingerprints. Prior state stays unchanged; a successful competing drainer
may consume the shared claim without being overwritten by the older failure.
Current valid native/backup metadata with optional naming/capture failures remains
accepted. Malformed or missing native metadata alone is not a transient signal;
malformed notices, unsafe paths and unconfigured roots remain rejected. Persistent
failures occupy bounded claims and bounded passes, with no scan, worker start or
history body read. Every accepted location passes through `Discovery.hint`'s
configured native-root and metadata validation. Native events/transcripts are never
opened by hint processing (transcript stat remains part of change detection).

The proposed legacy CLI opt-in is `AMPLIFIER_SESSION_CATALOG_HINT_DIRECTORY`,
pointing to the same directory. The CLI owner must implement post-save notices
and qualify its actual installed writer; an environment variable alone enables
nothing. Catalog does not watch every history directory to substitute for that
missing integration. Native ACP notifications continue using the live `hint`
RPC; neither path requires a different canonical-history format.

### Workspace metadata and registration projection

The catalog also exposes existing-directory workspace discovery without reading
native history or scanning the filesystem per query:

- `listWorkspaces({connectionId,limit<=200,cursor?,search?,includeHidden:false,includeUnavailable:false,allowedWorkspaceRoots?})`
  returns `{items,nextCursor?,freshness}`. Visibility/root/search filtering occurs
  before keyset pagination, ordered by folded display name and canonical path.
  Cursors bind the connection and exact query. DTOs include
  `{id,name,path,available,availability,registered,hidden,revision,source,checkedAt}`.
- `getWorkspace({id})` performs an exact indexed read, including unavailable or
  hidden entries. Access authorization belongs to the caller.
- `workspaceProjectionStatus({source})` returns an owner's applied revision.
- `projectWorkspaces({source,afterRevision,throughRevision,records})` applies at
  most 100 changed registration records and its checkpoint atomically. Records
  are `{id,path,name,registered,hidden,revision}`; ID is `workspace:` plus SHA256
  of the canonical path. Revision must fall inside the checkpoint interval. A
  conflicting owner or checkpoint is rejected. Only changed records refresh
  directory availability; label-only updates do not rewrite all session rows.

A separately configured registration owner retains canonical receipts and can
rebuild this projection after catalog loss. This service does not create, rename,
move, or delete workspace directories. A hidden registration suppresses default
workspace and session discovery, including after scanner/native updates. Explicit
owner attachment restores it; archive/exact reads preserve historical access.
The default scanner/recheck maintenance still repairs external directory changes.
No list or get method loads native transcript/events. Schema 1's original facts
and native records remain compatible; new workspace identity/visibility tables
are derived, rebuilt in bounded migration batches.

Archive selection is separate from workspace visibility. `list` now accepts
`archive: 'active' | 'all' | 'archived'`; every value retains ordinary existing,
non-hidden, non-deleted filtering before keyset pagination. It cannot be combined
with `includeArchive:true`. Omitting it preserves the previous API behavior,
including the historical `includeArchive` compatibility flag. Archive cursors are
bound to the selector. Records expose `isArchived`; host `upsert` may provide an
explicit boolean. Native scans that omit it never clear a host-owned archive flag.

Trusted hosts may repair derived flags through `projectSessionArchives` with
`{items:[{uri,isArchived}]}` (at most100). It changes only the derived archive
projection, not native history or directories, and may precede later native
metadata discovery. Active/archive queries use the catalog's indexed flags;
there is no transcript scan or in-memory historical session collection.

### Atomic imported native registration

`claimNative {record,commandId,evidence:'verified-native-import'}` is a trusted
host projection API. The host must independently verify the configured native
owner's creation receipt first; the catalog does not accept or manufacture native
authority. The requested URI is a hint until the transaction commits. One
unambiguous existing engine/native/workspace identity is reused, preserving title,
archive flags and other user edits. Conflicting URIs or ambiguous native homes
refuse. Exact command retries return the same derived mapping; changed identity
on a command is rejected. Receipts are repairable index metadata, not transcript
or native creation journals.

Discovery rechecks native identity under the same SQLite writer transaction, so a
scanner that calculated its URI before a host claim cannot insert a duplicate.
A claimed identity cannot later be overwritten through ordinary upsert. No native
history content is read by the claim, and missing/hidden workspace visibility stays
independent of registration. Tests include simultaneous scanner/claim transactions.

### Retention candidates and family guards

`retentionCandidates({connectionId,modifiedBefore,allowedWorkspaceRoots,limit?,cursor?,workingDirectory?})`
returns a bounded page of **candidate metadata**, never permission to remove work.
The cutoff is an explicit finite Unix timestamp; both creation and modification
must precede it. Existing, non-hidden, non-deleted root conversations are selected
before pagination, including archived roots. Authorized roots are mandatory; an
empty list returns no candidates. The maximum page size is 200. Results include
`candidateCount`, `cutoff`, `eligibility:'unverified'`,
`scope:'visible-root-metadata'`, `nativeFilesRead:false`, and
`removalAuthorized:false`. The count and page share a SQLite read snapshot. A
candidate still requires current-selection, live/unknown work, descendant,
dependent-owner and exact-reviewed-source checks by the responsible host.

`list` also accepts `modifiedBefore` and `includeTotal:false`. Counting is an
explicit operation over the matching disk index, never part of ordinary list
navigation. Cursor identity includes the cutoff; pages remain live rather than a
frozen multi-request snapshot. These queries never open or stat native files.

`descendants({parentUri,limit?})` is a trusted-host safety query, separate from
visible child navigation. It follows **all** indexed parent relationships,
including hidden, archived, deleted, internal and independent-fork records.
It returns at most 100 descendants and traverses at most 64 levels. `root` and
`items` contain exact URI, native/engine identity, working directory, parent URI,
kind, source and indexed/modified timestamps. A missing root, cycle, depth limit
or result overflow sets `completeIndexedCoverage:false` with explicit `issues`.
It does not truncate silently into a successful guard.

The returned `revision` binds the exact family facts and discovery freshness to
this catalog process. A caller can re-query under its own admission/ownership
locks and reject changed facts; the token is not a lease and is not valid across
catalog restarts. `coverage:'indexed-native-history'` and
`externalWriterCoverage:false` explicitly exclude undiscovered/noncooperating
writers. A complete indexed result does not establish complete native coverage:
callers must check scan freshness/issues and acquire and verify the actual native
family ownership. No interactive guard triggers discovery or loads history.

Tests cover missing roots, all relationship kinds, cycles, truncation, changed
facts, process-local revisions and bounded SQLite work with 25,000 unrelated
records. Retention tests separately qualify indexed counting at that scale; they
do not claim constant-time counts or authorization to delete historical logs.

### Managed chat locations

A host-verified managed chat projects a bounded `location` object with exactly
`{version:1,kind:'managed',allocationId,creationReceipt,allocationHash,owner:'amplifier-unified'}`.
The allocation ID is a canonical UUID, the hash is lowercase SHA256, and the
receipt is a bounded command ID. The working directory must be exactly
`<managed-root>/chats/<allocationId>/files`. This remains derived metadata;
the catalog never grants creation, activation, or deletion authority.

`list`, `retentionCandidates`, and `resolveNative` accept a separate trusted
`allowedManagedRoots` list (at most 128 normalized absolute paths). They authorize
only validated managed rows at those exact roots. They do not grant browsing or
access to arbitrary sessions under the same directories. Supply an explicit
`allowedWorkspaceRoots` list, possibly empty, alongside a managed grant. Cursor
scope binds both lists. Ordinary queries still use only the SQLite index.

Managed chats appear in the global root conversation list while their directories
exist; their allocation directories never appear in `listWorkspaces` or
`getWorkspace`, including requests for hidden workspaces. Registering an allocation
as a project is refused. Archive and native visibility markers still apply.

Discovery accepts location only from canonical native metadata (or its backup),
never naming or context-intelligence sidecars. Empty managed roots can therefore
be rediscovered before a first prompt. For roots, allocation and native IDs must
match; their derived AHP URI uses that same UUID, avoiding scanner-first duplicate
identities. Conflicting prior mappings require explicit owner repair. An existing
host projection survives missing, malformed, or stale scanner location metadata.
The native owner must verify its durable creation record before activation;
metadata alone is not that proof.

Native parent links retain the exact engine/native identity, canonical workspace,
and native parent storage path in a separate derived index. Child-first discovery,
later managed-root discovery, and concurrent host/import mappings repair those
links in the same writer transaction without changing child titles or archive
flags. Ambiguous parents make `descendants.completeIndexedCoverage` false.
Upgrading discovery refreshes old fingerprints once from bounded metadata; it
never reads transcript or event bodies for this repair. `discoveryComplete` is
separate from graph/index completeness and is true only for a completed scan
without issues. Callers must also check freshness; neither field excludes an
external writer or proves a current native family lease.

### Product conversation presentation visibility

`projectSessionVisibility({source,commandId,afterRevision,throughRevision,items})`
is an additive trusted-host projection seam for product presentation reset. Each
item is exactly `{uri,hidden,revision}`; a batch contains at most 100 unique,
already-indexed session URIs. Revisions are nonnegative signed 64-bit integers,
with `afterRevision < revision <= throughRevision`. `source` and `commandId` are
nonempty strings of at most 200 characters. Unknown fields, identities, owners,
changed command content and stale checkpoints refuse the entire batch.

The transaction atomically applies rows, its source checkpoint and an exact
projection receipt. The result is `{source,commandId,revision,projected}`.
`sessionVisibilityProjectionStatus({source})` returns
`{source,revision,indexIncarnation,projectionPresent}`. `source` echoes the requested
owner selector; `revision` is its persisted checkpoint or zero when absent.
`projectionPresent` is computed from actual source-row presence, not persisted as
a second state flag. The projection API does not produce a present zero checkpoint.
`indexIncarnation` is a canonical UUID inserted transactionally once under the
`index-incarnation` key in existing progress metadata. Reopen, scans and in-place
schema migration preserve it. Status reads identity and checkpoint in one SQLite
statement/snapshot from the actual handle, and refuses missing/corrupt identity
rather than repairing it. `sessionVisibilityProjectionReceipt({source,commandId})`
returns the original result or null without writing. An exact command retry
returns its original result even after restart or a newer batch; it never repeats
an older visibility change. A URI belongs to its first projection source, including
a restored row. Empty batches may advance a checkpoint. Item order is part of
command identity. Callers keep original commands until outcomes are reconciled.

Product-hidden rows are excluded before root/child/archive/retention pagination.
Exact `get` reports `productHidden`; safety family and directory-use guards retain
these references. Native visibility, workspace existence and product visibility
remain independent. Native restore, scans, ordinary metadata writes and workspace
rechecks cannot revive a product-hidden row. A fresh host-reviewed restore projects
the retained prior `hidden` flag at a newer revision; clearing it cannot bypass
native deletion or unavailable/hidden workspace filtering.

This index grants no reset, deletion, execution or maintenance authority. The host
owns eligibility checks, held maintenance, exact immutable selection, retained
before-images, reset/restore revisions and durable no-replay journals. Its reset
must preserve canonical history, receipts, files and independent artifacts. It
must restore the prior flag, rather than assume all rows were previously visible.
The catalog never forges native tombstones or reads native transcript/event bodies.

After catalog loss, the host rebuilds indexed identities and projects retained
visibility facts in bounded batches starting at checkpoint zero. **The host must
fence client listing and adoption until reconstruction completes**, so discovery
cannot temporarily reveal product-hidden histories. This indexed-only seam does
not accept markers ahead of discovery. A source identity stays stable across host
restarts; changes require an explicit migration, not a guessed new owner. Catalog
projection receipts are rebuildable index evidence, not the host's reset journals.
Source and independently installed component tests do not qualify the host reset
transaction, client controls, browser/device/account behavior or live adoption.

#### Index continuity and compatibility

Creating or recreating a database creates a new identity. Adding this metadata to
a legacy database preserves its rows, checkpoints, receipts and progress; it does
not prove initialization, reconstruction or Host readiness. No projection batch
or checkpoint is manufactured to establish identity. A future paired Host can
acknowledge the exact incarnation in its existing authority only at a verified
qualification boundary, then use that acknowledgment and settled visibility
facts for retained reads during refresh. This Catalog change does not implement
that browse admission or change full discovery, directory observations, hint
coverage, mutation eligibility or the existing rebuild fence.

Older Catalog peers without these fields use the existing qualified path; callers
must not invent an acknowledgment. Older Hosts retain their existing gating.
These fields extend only the trusted local component interface, not AHP/ACP.

A database copy or restored backup retains its stored UUID. Identity detects a
different/recreated index, not arbitrary rollback to a same-UUID backup; restore
requires owner fencing and requalification. Live file replacement while Catalog
is serving is unsupported: stop owners, replace, reopen and requalify. Identity
is read from the SQLite handle, but a leased handle may still refer to its original
file. Two surrounding status reads do not establish a page's origin under an
arbitrary replacement and reversal during the read.

#### Host snapshot/rebuild handshake

The host composes the projection with this explicit handshake:

1. Enter and persist a host-owned catalog-rebuild fence before replacing or
   reopening a lost index. While fenced, product list/adoption returns unavailable;
   native discovery and these trusted catalog methods remain usable. Do not infer
   readiness from a completed native scan or from an empty visibility checkpoint.
2. Capture an immutable, cold host-authoritative visibility snapshot with stable
   URI order, exact row count, snapshot identity and retained prior flags. Fence
   reset/restore mutations or journal and drain subsequent changes before release.
   The host owns paging this snapshot without omitting rows; each page has at most
   100 items. Catalog live-list cursors are not immutable snapshot cursors.
3. Reconstruct indexed identities, then read
   `sessionVisibilityProjectionStatus({source})`. On a fresh database it is zero.
   Project snapshot pages in contiguous checkpoint batches, always passing the
   last confirmed checkpoint as `afterRevision`. Projection revisions are the
   host's monotonic **projection batch sequence**, separate from its reset/undo
   policy revision; all rows in a page may use that page's `throughRevision`.
   Use exact original rebuild-epoch/page command IDs and retain each full batch.
4. If a response is lost, inspect that page's exact receipt. Exact retry is safe
   only with the original content; an old successful receipt never rewinds newer
   flags. Verify its result and checkpoint before moving to the next page. An
   unknown/changed checkpoint, missing indexed identity, owner conflict or partial
   host snapshot leaves the fence in place; do not skip ahead or invent success.
5. Release product listing only after every snapshot page and subsequent change
   batch has a matching receipt, the final checkpoint matches the retained replay
   manifest, and indexed reconstruction is complete. Persist host readiness under
   its own CAS so a restart cannot open listing ahead of replay. An empty snapshot
   still uses an explicit empty final checkpoint batch and readiness record.

This component does not implement that host fence or snapshot authority. The host
integration must qualify index loss, interrupted rebuild, restart, and concurrent
reset/restore admission before enabling the product reset control.

### Selected native family repair and verification (v1)

`reconcileFamily({parentUri,limit:100,maxEntries:512})` is a trusted optional
metadata-only repair for an explicitly selected family. Limits accept 1–100 and
1–512 respectively. It visits only configured native project namespaces of the
selected indexed root and its known/newly indexed descendants. It does not run
`scan`, read transcript/event bodies, start agents or change canonical files.
A stale global scan timestamp is retained honestly in `freshness`; the new
`familyDiscovery` proof is separate. Existing `descendants` remains index-only.

The result is the ordinary descendant envelope plus `familyDiscovery` with:
`version:1`, `scope:'selected-native-project-namespaces'`, `phase:'complete'|'refused'`,
`issues`, `completedAt`, `indexRevision`, `inventoryHash`, `namespaces`,
`entriesExamined`, `metadataReads`, `metadataBytesAdmitted`, `maxEntries`,
`metadataByteLimit:8388608`, `metadataFileLimit:4*maxEntries`, and
`externalWriterCoverage:false`, `transcriptBodiesRead:false`,
`nativeOwnershipVerified:false`. Complete proofs also have `verificationToken`.
The proof's `indexRevision` equals the returned pure indexed `revision`.
Reconciliation reports `nativeFilesRead:true` when it opened metadata files; this
does not mean transcript/event bodies were read. The indexed revision binds the
pure index facts, while this read report and the proof describe the repair.

All directory entries count against the aggregate inventory bound, including
lock/temp files. Every native metadata and configured adapter lifecycle marker
body shares the aggregate 8 MiB and `4*maxEntries` read limits. Admission counts
all present sources before reading a namespace; actual opened descriptor sizes
and read attempts are also checked and reported, including invalid JSON.
Lifecycle markers, including absent markers, bind both physical inventories;
creation, replacement or deletion invalidates `verifyFamily` even without an
indexed revision change. File reads may count up to 2,048 at 512 entries. At most 16 configured
native homes and 16 adapter homes are supported by this bounded operation.
Source ambiguity, noncanonical identity, missing files, unsafe/special metadata,
entry/byte limits, unfinished metadata, cycles/overflow, concurrent discovery or
changed namespace facts refuse scoped coverage. All metadata and lifecycle opens
use nonblocking descriptors, verify regular files after opening, and bind opened
file identity and size before consuming bytes. A replacement FIFO cannot block
opening, and file growth or replacement refuses scoped coverage. A five-second
budget is checked between operations; filesystem call latency is not a hard
real-time guarantee. Index repair may be partial on refusal; no complete proof
or canonical effect is claimed.

`verifyFamily({parentUri,verificationToken})` performs a bounded passive index and
physical namespace/stat recheck. It neither repairs the index nor reads native
file bodies. It returns `{version:1,scope:'selected-native-project-namespaces',
parentUri,valid,issues,completedAt,externalWriterCoverage:false,
nativeOwnershipVerified:false,transcriptBodiesRead:false,indexRevision?,inventoryHash?}`.
Found tokens echo the original revision/hash; unknown, expired or mismatched
ones refuse. Tokens expire after 300 seconds, are process-local, and occupy at
most 64 retained inventories. Restart requires a fresh explicit reconciliation.

A consumer holds its source admission, validates all ordinary family/root/parent
identity and bound checks, then accepts scoped freshness only from a complete,
issue-free v1 proof with exact `indexRevision===family.revision`. Under its
existing child/native ownership leases it must call `verifyFamily`, require
`valid:true`, exact parent/revision/hash, fresh timestamp and empty issues, then
repeat pure `descendants` revision comparison before native dispatch. A new native
child without a writer hint can leave the pure index revision unchanged; the
physical verifier must refuse that race. Do not automatically repair/retry copy
on verification refusal or replay unknown work.

This proves selected known namespaces, not all native storage or exclusion of
noncooperating external writers/cross-namespace relationships. Actual native
ownership and descendant equality remain mandatory. Namespace locators are
trusted-host details, not client UI fields. Older producers without these optional
methods retain the prior fresh-global-index path; a supported refused/failed repair
must never silently fall back to an old global timestamp.


## Reversible selected-source absence

A location hint for an already indexed exact native directory under a configured
home may observe `ENOENT`. Discovery repeats that check and verifies the indexed
URI, engine, native identity, workspace and exact storage path under the index
writer transaction. It then records internal source absence and excludes that row
from normal root/child pages and retention candidates before pagination. This is
neither `nativeDeleted` nor `productHidden`; those independent owner flags remain
unchanged. No public field, hint shape or availability enum is added.

Exact authorized `get`, safety descendants and directory-use retain the historical
row and relationships, with `read-only` availability. Explicit `includeArchive`
historical pages may still show the retained row. Present directories with missing
or invalid metadata, permission/IO errors, symlinks, foreign roots and changed
bindings do not prove physical absence. No history body is read and no worker or
model is started. Work is bounded to the selected path and its indexed row.

A host metadata summary preserves an already discovered exact native source
binding even when it omits `storagePath` or supplies null; it cannot move that
binding or change its native identity. Late summaries therefore cannot bypass
absence. Only a successful current metadata refresh at the same validated physical directory
can clear its internal absence observation. Native deletion, product exclusion and
workspace exclusion still dominate. Ordinary upsert, native/product restore,
workspace recheck and process restart cannot silently clear absence. The durable
index preserves observations across restart. A destroyed index loses this derived
state; hosts must retain their existing rebuild fence and reobserve known selected
locations before exposing reconstructed work. Global scans do not invent tombstones
for unvisited directories. This rule does not remove drafts, receipts, accepted work
or canonical history and does not authorize a native delete. A CLI delete notice
producer remains a separate integration; post-save notices require existing sources.


## Selected-source publication CAS

Discovery captures one exact indexed source row, including its native visibility
state, before reading metadata. Its private publisher compares that row token under
the existing SQLite writer transaction and confirms the selected directory identity
and current file read set before applying metadata or lifecycle proof. The file
facts include device, inode, size, mtime and ctime for four metadata locations, two
transcript stat locations and at most sixteen configured lifecycle locations. No
transcript/event body is opened. Larger adapter sets refuse before metadata opens.
A stale successful reader therefore cannot overwrite a newer completed projection.
No application-wide read lock or global filesystem scan is added.

Selected directory absence uses the same row comparison plus repeated ENOENT;
metadata restoration and clearing exact derived absence occur in the same guarded
publication transaction. Native deletion, product/workspace exclusion, archive
flags and canonical history remain independent. Indexed ambiguity, a different
native binding, unsafe files or unknown I/O do not authorize replacement.

Public `hint` retains its result shape and reports changed-false with issues when
publication races. The private inbox adapter permits at most one fresh selected
attempt after a publication conflict (two total), so a newer candidate can recover
when a successful competing drainer acknowledged their shared claim. Persistent
conflicts keep the immutable claim for a later bounded pass. Transient native or
lifecycle read failures still return before publication and wait for a later batch;
accepted usable partial metadata and malformed/unsafe/unconfigured poison treatment
remain unchanged. All publication hooks and row tokens are private, unregistered
implementation details; public upsert and notice fields do not expand.

## Live library query v1

The additive [library contract](contracts/library-query.v1.md) provides global
indexed sorting, literal search and observed activity before bounded pages.
`libraryCapabilities` negotiates it; ordinary `list` remains compatible. Cold
runtime unknown is separate from retained uncertain effects. Current Host facts
are bounded and observations carry explicit freshness. No current global totals,
frozen pages, native history reads or worker initialization are implied.

Missing ordinary workspace folders can be discovered with the explicitly negotiated
`libraryCapabilities.missingWorkspaceHistory.version: 1` and `location: "missing"`
library selector. The default library remains unchanged. This metadata-only view
retains independent hidden, deleted, source-presence, archive and access exclusions;
it does not authorize execution or relocate history. See
[the owner contract](contracts/missing-workspace-discovery.v1.md).
