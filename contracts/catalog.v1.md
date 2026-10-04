# Catalog contract — v1

This is the local entry point for inherited approved discovery and ownership
promises, implemented API obligations and their falsifiers. It does not replace
upstream wire schemas or ratify new product scope. Exact method shapes and bounds
remain in the [README](../README.md#stdio-interface-v1); product visibility uses
[its existing v1 contract](product-visibility.v1.json) and
[host rebuild handshake](../README.md#host-snapshotrebuild-handshake).

## Authority and consumers

The [family vision](../VISION.md) specializes the approved shared direction.
Shared clauses are linked at the reviewed distribution revision:

- [WS1–WS6](https://github.com/microsoft/amplifier-unified/blob/625af2f2832fafc8152f6078061d626f160362a4/contracts/working-set.v1.md): durable records, filtered discovery, lazy detail, bounds, dependency cost and incremental metadata.
- [HP1–HP3, HP6](https://github.com/microsoft/amplifier-unified/blob/625af2f2832fafc8152f6078061d626f160362a4/contracts/host-protocol.v1.md): upstream protocol authority, resource/catalog separation, recovery and host admission receipts.
- [AP3, AP6](https://github.com/microsoft/amplifier-unified/blob/625af2f2832fafc8152f6078061d626f160362a4/contracts/agent-protocol.v1.md): native execution identity and honest historical compatibility.
- [CB1–CB6](https://github.com/microsoft/amplifier-unified/blob/625af2f2832fafc8152f6078061d626f160362a4/contracts/component-boundaries.v1.md): independent consumers, one semantic owner, qualification, composable releases and isolated lanes.
- [CS2, CS4, CS7](https://github.com/microsoft/amplifier-unified/blob/625af2f2832fafc8152f6078061d626f160362a4/contracts/client-state.v1.md): client-local state, no replay and canonical ownership.
- [CX1, CX4](https://github.com/microsoft/amplifier-unified/blob/625af2f2832fafc8152f6078061d626f160362a4/contracts/client-experience.v1.md) and [CD1–CD6](https://github.com/microsoft/amplifier-unified/blob/625af2f2832fafc8152f6078061d626f160362a4/contracts/client-development.v1.md): honest orientation/ownership, shared meaning and specific acceptance. Rendering belongs to clients, not this index.

The local stdio API is a trusted component interface, not an AHP or ACP server.
Hosts implement [upstream AHP catalogs/reconciliation](https://microsoft.github.io/agent-host-protocol/guide/reconciliation.html)
and agents implement [upstream ACP](https://agentclientprotocol.com/); catalog DTOs
must not become invented standard wire fields.

## Promises and falsifiers

| Local promise / inherited basis | Broken if | Existing executable acceptance |
| --- | --- | --- |
| C1: Preserve canonical native records; never initialize execution or read transcript/event bodies for discovery (WS1, WS3, WS6; AP3/AP6). | Listing or a hint opens history bodies, changes native bytes, or starts a dormant agent. | [catalog tests](../tests/test_catalog.py): `test_query_does_not_restat_or_read_native_files`, `test_discovery_hints_backups_parent_forks_and_no_native_writes`; [hint tests](../tests/test_hints.py): `test_hints_use_public_discovery_without_opening_history`. Kernel/runtime initialization is absent from this package; actual host worker-start evidence is separate. |
| C2: Filter authorization, existing-workspace/root eligibility, confirmed selected-source absence and visibility before bounded keyset pages; expose children explicitly (WS2/WS3; HP2). | Hidden or unauthorized rows consume page slots, sibling prefixes grant access, or unrelated child history loads. | [catalog tests](../tests/test_catalog.py): filtering, scoped roots and connection/query/process cursor binding; [workspace tests](../tests/test_workspaces.py) and [managed-location tests](../tests/test_managed_locations.py): independent exact managed grants and page scopes. |
| C3: Charge changes to indexed dependencies and bound resident/query work (WS4/WS5). | A scalar update or normal visible page materializes the historical estate, or continuation walks already skipped timestamp ties. | [catalog tests](../tests/test_catalog.py): `test_workspace_invalidation_updates_only_its_dependents`, `test_query_work_does_not_scale_with_invisible_estate`; [product tests](../tests/test_product_visibility.py): `test_large_hidden_projection_uses_visible_index_and_scoped_updates`; [page seek tests](../tests/test_page_seek.py): root/child continuation with 25,000 earlier equal-timestamp rows; [benchmark](../scripts/benchmark.py): fresh-process 4,000-workspace fixtures with 25k/100k sessions; [physical qualification](../scripts/qualify_large_history.py): 4k-project/25k-session installed metadata reconstruction, existing-only pages, fresh stdio reconnect, explicit announcements and bounded retention. Counts and background rebuilds may perform proportional disk-index work; ordinary navigation does not count by default. |
| C4: Recover location-only hints and parent identities without trusting arrival order; disclose ambiguity (WS6; AP3/AP6). | Child-first/late/concurrent discovery loses a relationship, guesses an ambiguous parent, or lets a hint replace canonical metadata. | [parent mapping tests](../tests/test_native_parent_mapping.py): all ordering, late import, concurrent mapping and no-transcript refresh cases; [hint tests](../tests/test_hints.py): atomic claim, crash recovery, bounded batches and failed-write retention. |
| C5: Safety discovery includes excluded histories and refuses incomplete coverage as proof (WS1; CS7). | Hidden/internal/deleted rows disappear from a directory or family guard, or cycles/overflow produce complete proof. | [descendant tests](../tests/test_descendants.py), [retention tests](../tests/test_retention_candidates.py) and directory-use tests in [catalog tests](../tests/test_catalog.py). |
| C6: Product visibility stays distinct from native deletion; apply bounded source-exclusive CAS batches with exact recoverable receipts (CS4/CS7; CB3). | Native recheck revives product exclusion, retry rewinds a newer restore, a source takes over, or invalid batches partly apply. | [product tests](../tests/test_product_visibility.py): independent reindex/native-byte preservation, restart/newer-revision recovery, atomic rollback, source conflict, concurrent CAS, complete visible paging and contiguous snapshot replay. This is the agreed additive candidate seam, not ratification of host reset effects. |

| C7: A selected repair binds current bounded native namespace and configured lifecycle facts, with separate under-lease physical verification (WS1/WS3/WS5/WS6; AP3). | Stale global status is relabelled fresh, lifecycle reads escape shared byte/file budgets or verification, unknown children bypass leases, or a cached index revision is accepted as proof against an unhinted new child. | [family reconciliation tests](../tests/test_family_reconciliation.py): stale global reproduction, root/child preservation, unknown child after proof, passive verifier, source/entry/byte/race refusal, lifecycle overflow/FIFO/changed-marker refusal, visibility preservation and restart expiry. [Selected family protocol](../README.md#selected-native-family-repair-and-verification-v1). |

## Selected-source absence

The internal exact `(uri, storage_path)` absence observation is derived discovery
state, not native deletion or product hiding. Selected configured-path `ENOENT`
requires an indexed-binding CAS and repeated filesystem confirmation; successful
exact metadata refresh clears only presence. All visibility projections retain this
exclusion. Safety lookups retain historical rows. [Presence tests](../tests/test_source_presence.py)
cover independent exclusions, current-fact duplicate hints, unsafe/unknown sources,
stale binding and replacement races, restart, child safety and bounded 25k-row work.
Index loss also loses observations; the host rebuild fence remains required until
known selected locations are reobserved. See [the discovery rule](../README.md#reversible-selected-source-absence).

## Consumer obligations

Hosts authorize every query result and supply explicit scoped roots; exact `get`
and safety methods are not permission grants. Historical `status:idle` or
`availability:available` does not prove a free execution lock. Read freshness,
issues, truncation and external-writer coverage before relying on safety facts;
acquire native ownership independently. Retention candidates never authorize
removal. Reconnect or process replacement requires fresh catalog cursors.

Project only confirmed native facts and owner-authoritative product metadata.
Keep source IDs stable, retain exact command batches and inspect receipts after
lost replies. Catalog receipts are derived evidence, not native creation/reset or
host admission authority. For index loss, the host must keep product listing and
adoption unavailable until the retained paged visibility snapshot and subsequent
changes are replayed and verified under the documented handshake. The host owns
that fence, immutable selection and prior flags; catalog cannot authorize undo.

## Evidence and outstanding qualification

At implementation revision `ca7618684ae5cb61ee5b29a445b41104a6536298`, the owner
recorded 83 source and 83 independent installed-wheel checks, zero skips, five
Python files matching source/wheel/installed bytes, and the synthetic 4k/25k/100k
benchmark. These are component receipts, not production performance or complete
family conformance. `uv run pytest -q` and `uv run python scripts/benchmark.py`
reproduce the component checks; an installed consumer must run copied tests without
source-path injection and verify exact artifact bytes.

Host-owned maintenance/reset/retained undo, rebuild readiness and concurrent
admission require integration qualification. Actual filesystem-scale discovery,
external noncooperating writers, Linux/Windows/Python3.11, browser/device/account
behavior and live adoption were not qualified by this catalog packet. Desired
preservation and responsiveness remain promises despite these gaps. Cross-owner
semantics or new obligations require coordinated review; this document introduces
no runtime feature and does not self-ratify pending product decisions.
