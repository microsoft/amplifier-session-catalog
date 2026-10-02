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
