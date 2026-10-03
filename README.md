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
| `list` | `{items, nextCursor?, freshness}`; params `connectionId` required, `limit` 1–200/default50, `cursor?`, `workingDirectory?`, `search?`, `parentUri?`, `includeArchive?` false, `allowedWorkspaceRoots?` |
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
