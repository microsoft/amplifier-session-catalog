# Amplifier session catalog

A rebuildable SQLite index for workspace and native session discovery. It never
loads an agent, imports Amplifier's kernel, reads event/transcript contents, or
writes native history. Interactive list/get operations query the index only.

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
claim for a later batch. Every accepted location passes through `Discovery.hint`'s
configured native-root and metadata validation. Native events/transcripts are never
opened by hint processing (transcript stat remains part of change detection).

The legacy CLI opt-in is `AMPLIFIER_SESSION_CATALOG_HINT_DIRECTORY`, pointing to the
same directory. Native ACP notifications continue using the live `hint` RPC; neither
path requires or introduces a different canonical-history format.

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
`sessionVisibilityProjectionStatus({source})` returns `{source,revision}` (zero
before any batch). `sessionVisibilityProjectionReceipt({source,commandId})`
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
