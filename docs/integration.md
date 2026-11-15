# Catalog integration

The catalog is a derived index. Hosts authorize access and retain authority over
native execution, history and product visibility. This guide covers the local
stdio interface and the recovery obligations needed to embed it. The
[catalog contract](../contracts/catalog.v1.md) and
[library contract](../contracts/library-query.v1.md) cover the corresponding promises.

## Stdio interface v1

Use UTF-8 newline-delimited JSON-RPC 2.0 on stdin/stdout. Match responses by ID;
they may arrive out of order. Diagnostics go to stderr. This is a trusted local
component interface, not an unauthenticated network endpoint.

| Method | Input and result |
| --- | --- |
| `list` | Requires `connectionId`; `limit` is 1–200 (default 50). Supports `cursor`, `workingDirectory`, `search`, `parentUri`, archive selection and authorized roots. Returns `{items,nextCursor?,freshness}`. |
| `get` | Takes `uri`; returns a record or null without reading native source files. The caller must authorize its workspace. |
| `upsert` | Takes a host-supplied metadata `record`; updates the index, not native history. Reuse an already discovered URI. |
| `hint` | Takes an allowed native `sessionDirectory`; reads bounded metadata and returns `{uri?,changed,issues?}`. A location is a hint, not replacement metadata. |
| `scan` / `status` | Schedules background discovery / reads its progress. List requests never trigger a scan. |
| `recheck` | Refreshes at most `limit` indexed workspace existence facts. |
| `directoryInUse` | Takes a canonical absolute `path`; returns bounded indexed uses, coverage and freshness. An empty result cannot exclude an undiscovered writer. |

Records include `uri`, `engineId`, `nativeSessionId`, `workingDirectory`, `title`,
timestamps, kind, workspace availability and native availability. Filesystem
locators are trusted host details: do not disclose them to unauthorized clients.
`status: "idle"` and `availability: "available"` do not prove that a native lock is free.

Lists are live, not snapshot-isolated. Cursors bind the query and connection,
expire after one hour, and become invalid when the process restarts. Start a new
page sequence after reconnect or a changed query. Default lists exclude hidden,
deleted, absent and unavailable-workspace records before pagination. Explicit
archive or child selection does not grant authorization.

For optional workspace, retention, import and projection methods, consult the
[dispatcher](../src/amplifier_session_catalog/__main__.py) and
[catalog implementation](../src/amplifier_session_catalog/catalog.py) for exact
arguments. Discovery metadata is never permission to delete or replay native work.

## Discovery configuration

Repeat `--home` for explicit native homes and `--workspace` for registered workspace
paths. `--app-home` identifies native ACP adapter homes whose lifecycle markers
may affect discovery. Do not infer a path by decoding a project slug.

Initial and background scans read metadata, not transcript/event bodies. A fresh
deployment can explicitly declare an existing canonical configured home with
`--initially-empty-home`; never use that declaration to excuse missing retained
history or a lost index. Unknown or inaccessible paths are not confirmed absence.

An optional private `--hint-directory` accepts location-only post-save notices.
The inbox is POSIX-only; direct hints and normal scanning remain portable. Native
writers must opt in explicitly and must not depend on notification delivery for
successful persistence. See [the inbox implementation](../src/amplifier_session_catalog/hints.py)
before writing notices.

## Index continuity and compatibility

Creating or recreating a database creates a new `indexIncarnation`. Adding this
metadata to an older database preserves rows, checkpoints and receipts; identity
alone does not prove reconstruction or host readiness. Callers must not invent an
acknowledgement when an older peer lacks these fields.

A database copy or restored backup retains its stored UUID. Identity detects a
different/recreated index, not rollback to a same-UUID backup. Restore requires
owner fencing and requalification. Replacing the database while it is serving is
unsupported: stop owners, replace, reopen and requalify.

## Host snapshot/rebuild handshake

The host owns the rebuild fence and an immutable visibility snapshot:

1. Persist the host fence before reopening a lost or replaced index. While fenced,
   product listing and adoption stay unavailable. A completed native scan is not readiness.
2. Capture a cold, stable-URI-ordered visibility snapshot with identity, exact count
   and prior flags. Fence concurrent reset/restore changes or journal and drain them.
   Each page has at most 100 items; live catalog cursors are not snapshot cursors.
3. Reconstruct indexed identities. Read `sessionVisibilityProjectionStatus({source})`
   and replay contiguous projection batches, using the last confirmed checkpoint
   as `afterRevision`. Retain exact rebuild/page command IDs and full batch contents.
4. After a lost response, inspect the exact page receipt. Any explicit retry must
   use identical original contents. Verify the result and checkpoint before advancing;
   an unknown checkpoint, missing identity, conflict or partial snapshot keeps the fence.
5. Release listing only when all snapshot and subsequent-change receipts match,
   reconstruction is complete and the final checkpoint matches the replay manifest.
   Persist readiness under the host's own compare-and-swap. An empty snapshot still
   needs an explicit final empty batch and readiness record.

Projection receipts describe a derived index, not native reset or creation authority.
See the [product-visibility contract](../contracts/product-visibility.v1.json).

## Selected native family repair and verification v1

`reconcileFamily({parentUri,limit,maxEntries})` repairs metadata for one selected
indexed family in configured native namespaces. `limit` is 1–100 and `maxEntries`
is 1–512. It does not run a global scan, read transcript/event bodies, start an
agent or change canonical files. A refused result may contain partial index repair;
it is not a complete coverage proof.

A complete `familyDiscovery` proof binds an inventory hash and the returned index
revision, with no issues and a `verificationToken`. All directory entries count
toward the limit; metadata/lifecycle reads share an 8 MiB budget and a
`4 * maxEntries` file limit. At most 16 native and 16 adapter homes are supported.
The five-second inter-operation budget is not a hard bound on filesystem calls.

Under its existing source admission and native/child ownership leases, the consumer
must call `verifyFamily({parentUri,verificationToken})`, require `valid: true`, the
exact parent/revision/hash, fresh timestamp and no issues, then repeat the pure
`descendants` revision comparison before dispatch. Tokens expire after 300 seconds,
are process-local and retain at most 64 inventories. Restart requires a fresh
explicit reconciliation. Verification performs bounded passive stat/index checks;
it does not repair or read native bodies.

These proofs cover selected known namespaces, not noncooperating external writers
or all storage. Native ownership and descendant equality remain mandatory. A
supported refusal must not silently fall back to a stale global scan timestamp,
and must not trigger automatic copy/replay. See the
[discovery implementation](../src/amplifier_session_catalog/discovery.py) for the
complete typed proof and refusal fields.

## Reversible selected-source absence

For an exact indexed source, a location hint can confirm `ENOENT` with a repeated
filesystem check and an indexed-binding comparison. The derived absence excludes
normal listing and retention candidates; it is neither deletion nor product hiding.
Safety queries retain the historical row and relationships.

Permission/I/O errors, symlinks, invalid metadata and changed bindings do not prove
absence. Only a successful current refresh at the same validated directory clears
that observation. Ordinary upsert, restore, workspace recheck and restart do not.
Index loss also loses derived observations, so the host rebuild fence must remain
until known selected sources are reobserved. None of these operations deletes
history, drafts, receipts or accepted work.
