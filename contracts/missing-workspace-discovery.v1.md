# Missing-workspace history discovery candidate

Proposed additive parity behavior for the existing-user switch: extend the
versioned library query with the explicitly selected `location: missing` filter.
Ordinary `all` and `managed` pages retain their existing present-folder contract.
The catalog advertises `missingWorkspaceHistory: {version: 1}` before a consumer
offers this filter. It reads only indexed metadata and preserves scoped roots,
root/explicit-child selection, archives, visibility exclusions, cursor scope,
page limits, and sort/search behavior. Managed allocations and unknown folder
availability are excluded. Confirmed missing native sources remain excluded.

The host independently authorizes the canonical missing path and all existing
ancestors, then negotiates exact passive history reading with Native. Native
requires matching session and working-directory metadata; neither a slug nor a
catalog row grants execution. The browser labels the result read-only and keeps
its draft. Restoring the original folder returns to ordinary validation.

Ownership review: Catalog owns discovery only; Host owns authorization and
admission; Native owns exact transcript reads; Workspace advertises only a
negotiated query; Web owns the explicit filter and read-only presentation. No
standard AHP/ACP fields are invented. No histories, configuration, runtime cwd,
or pending inputs move. A new independent recovery-copy flow is outside this
change. This candidate requires installed and browser evidence before release.
