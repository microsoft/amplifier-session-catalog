# Candidate: restored catalog source bindings

An installation restore may put a verified copy of native history in a new home.
Ordinary discovery intentionally treats separate homes as separate sources. The
restore operator must therefore move the copied index's exact source bindings
before starting discovery, while retaining AHP URIs used by drafts and links.

Proposed owner boundary: native maintenance verifies and finalizes the history
restore; the distribution operator keeps the restored installation stopped and
supplies the verified restore receipt digest. Catalog changes only its copied,
derived index. This is an offline Python interface, not an AHP/ACP extension or a
client RPC. The catalog receipt does not authorize activation or prove that native
history was restored. Consumers must separately verify those facts.

`rebind_restored_sources(catalog, command_id, restore_digest, source_home,
destination_home, records)` accepts at most 100 exact indexed source snapshots.
Each record contains its URI and expected snapshot token. All records must still
match and belong to the supplied source home. The complete batch commits once or
does nothing. Reusing a command with identical arguments returns its prior result;
different arguments are rejected. Destination collisions or ambiguous old source
bindings are refused, never merged or deleted.

Only storage paths, discovery source tags, source-presence paths and native parent
storage references move. Native IDs, working directories, AHP URIs, parent URIs,
claims, visibility, archive flags, host observations and unknown outcomes remain.
Fingerprints are invalidated for normal metadata verification at the new home.
No transcript/event body is read and no source file is written. Caller pages the
copied index and finishes every batch before activating any reader or worker.

Workspace relocation, managed allocation relocation, index-loss recovery and
moving a live installation are outside this interface. Those require their own
owner-authoritative mappings; a history-home rebind cannot infer them.

Qualification must include same-URI discovery after a physical restore, child
relationships, exact retries, conflicts, atomic failures, retained visibility and
unknown observations, plus a signed full-installation resume with no work replay.

The offline `rebind_restored_home` wrapper records a bounded plan of at most
100,000 source snapshots and applies batches of 100 through the same interface.
After interruption it reuses the recorded snapshots and command IDs, including
already-committed batches. It does not rediscover or silently skip moved rows.
Partial failures leave the copied index inactive for inspection. An empty home
is a valid zero-row projection. The local operator command is
`python -m amplifier_session_catalog.restore --db /private/copied.sqlite --request /private/request.json`.
The private request contains only `command_id`, `restore_digest`, `source_home`
and `destination_home`. This command is deliberately absent from catalog RPC.
