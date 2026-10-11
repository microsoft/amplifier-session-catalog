# Proposed recent navigation extension v1

This coordinated optional extension supplements libraryQuery v1. It does not
change standard AHP/ACP, native history, input receipts, or execution authority.

Catalog advertises navigation.version=1. A trusted Host may add navigation to
its checked, source-bound, revision-ordered hostObservation and bounded live
facts: {version:1, activityAt: finite nonnegative seconds, agentCreated:boolean}.
Only positive trusted creation provenance sets agentCreated. Unknown roots and
user forks remain visible. Native discovery cannot author this projection.

Queries opt in with navigationVersion:1, sort:recent, and optional
includeAgentCreated (default false). Catalog applies filtering before paging;
optional selectedUri exempts just that root from the agent-created filter, while
retaining every workspace, archive, search and visibility restriction. A bounded
point lookup keeps the human-only index effective. The selected URI is query
scope, not an authorization grant. Explicit child queries remain unfiltered.
all selectors bind the cursor. Existing sort:activity keeps its prior meaning.
The stable key has an expression index and URI tie-breaker. Raw activity/title
changes cannot invalidate a recent cursor unless they affect its actual query.

Host retains a bounded admission chain in the additive navigation column of
session_admission. Exact input identity is reused only inside the same session;
execution receipts reject replay. Rejected inputs relinquish only their own
provisional position; accepted and uncertain inputs retain it. Completion and
attention supersede fences. The column is presentation metadata and older Host
versions may ignore it during rollback. No commands are replayed to rebuild it.

Workspace negotiates this extension separately. Clients use owner pages from
the first page, retain shown rows on stale query refusal, and offer an explicit
show-agent-created filter. Old peers do not advertise new selectors.
