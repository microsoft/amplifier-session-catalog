# Session catalog vision

This repository makes durable native work discoverable without keeping historical
work resident. A person with thousands of former workspaces should receive a useful
page of existing workspaces and root conversations, then open children and details
on demand. Discovery must never start an agent, replay accepted work, or alter the
native records it describes. Small changes should touch their indexed dependencies,
not materialize the whole installation.

This is the catalog specialization of the
[approved Unified family vision](https://github.com/microsoft/amplifier-unified/blob/625af2f2832fafc8152f6078061d626f160362a4/docs/architecture/VISION.md)
(principles 3, 5, 7–9), not a new ratification or a claim of deployed acceptance.
The catalog is independently usable; it must not depend on Unified runtime objects.

## Ownership and scope

The catalog owns rebuildable metadata indexes, workspace-existence observations,
parent mapping, bounded location-only writer hints, paged discovery and derived
visibility projections. Native adapters own canonical history and writable execution
identity. Hosts own authorization, execution/admission receipts, product reset
eligibility, retained undo and rebuild readiness. Clients own drafts and selection.

Desired outcomes are metadata-only discovery across 4,000 historical workspaces and
25,000 or more sessions, filtered pages before detail loading, honest ambiguity and
incomplete-coverage reports, and recoverable product exclusion without forged native
tombstones. These are goals to qualify, not permission to truncate required work.

Execution, AHP/ACP transport implementation, canonical history mutation/deletion,
client presentation policy and a second resume authority are outside this repository.
Upstream AHP/ACP remain authoritative where hosts map these results onto protocols.

The [catalog v1 contract](contracts/catalog.v1.md) maps promises to executable checks
and consumer obligations. The product visibility seam is an integration candidate;
its host reset/rebuild fence and browser acceptance remain separately unqualified.
No new obligation or feature is ratified by this documentation packet.
