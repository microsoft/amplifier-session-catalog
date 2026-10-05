# Live library query v1

Additive trusted component interface approved for this source lane on 2026-10-04.
Existing `list`, cursor semantics and its legacy status field remain unchanged.
This does not establish installed, browser or live acceptance.

`libraryCapabilities` advertises version1, live paging, page bound50, live-fact
bound64 and no global counts. `libraryQuery` requires connectionId and explicit
allowedWorkspaceRoots; accepts limit1..50, cursor, workingDirectory, search,
parentUri, archive=active|all|archived, sort=activity|created|name,
activity=all|working|attention, location=all|managed and separate managed grants.
Every authorization, visible existing-directory, root/explicit-child, archive,
location, literal search and effective activity predicate precedes LIMIT.
Search matches SQLite-lower literal substrings of title, URI, native ID, authorized
workspace path and workspace display name. It is neither glob nor Unicode casefold.

Orders are modifiedAt DESC/URI ASC, createdAt DESC/URI ASC, normalized title ASC/URI
ASC. Keyset continuation uses disjoint equal-key/URI and beyond-key ranges.
No native stat/transcript reads or execution happen in these queries. Substring
search may scan the eligible SQL index; counts are not calculated.

## Runtime evidence and uncertain effects

Trusted liveFacts contains source, incarnation and at most64 unique items
{uri,runtime:working|input-needed|idle|unknown,effect:unverified|settled|error|unknown,
modifiedAt:Unix seconds}; the envelope is bounded64KiB. It comes from the configured
Host, not client arguments. Live facts drive indexed URI seeks before pagination.
Cold rows always have runtime unknown. That alone is not Needs attention.
Known uncertain effects or retained errors are attention; input-needed is attention;
remaining working is working; proved current resident idle is idle; otherwise unknown.
The library row's status is this runtime string. `activity` gives kind/runtime/effect,
current-host versus indexed provenance and optional observation time. It is not AHP.

Host observations are derived fields within existing session details, never receipts
or a new resume authority. Checked Host upsert may supply
hostObservation={version:1,source,incarnation,revision,observedAt,effect,unknownChecked:true}.
Same-incarnation lower/equal revisions cannot overwrite newer observations. A source
cannot take over another source's observation. Native discovery preserves the field
and cannot author it. Only the trusted Host, after inspecting its retained unresolved
journals, can change unknown to settled. No ordinary idle field clears unknown.

hostObservationOnly=true updates only that observation on a retained exact identity.
It cannot create a mapping, retitle, promote children, change archive/location or
visibility. This is used for metadata reconciliation and semantic outcome updates.
Normal explicit Host metadata writes retain their existing behavior.

observationFreshness reports indexed/reconciling/complete/degraded. Complete means
this Host's metadata pass completed, not a current global runtime/operation inventory.
External tasks/questions/client delivery are not covered. Missed updates can leave
last-indexed facts stale; errors and unknown remain preserved and never become cold
idle. Consumers label observed matches appropriately, not zero current-world work.

## Live paging and refresh

Pages are live, not frozen snapshots. Tiny in-memory query guards retain selectors,
a bounded live-fact digest and dirty flag, never result members. Semantic writes
invalidate matching workspace/authority domains and relevant sort/search/activity
fields. A scope may refresh conservatively for a membership change within its
workspace, even if a search term would exclude the changed row. Unrelated workspaces,
non-name title changes without search, indexedAt and token contents do not invalidate.
Per-transaction guard-change events are capped256; overflow conservatively refreshes.
Guards are capped16 per connection/256 process-wide and expire after one hour.
Incarnation, authority or selector changes refuse old cursors. Guard loss/relevant
change returns error -32014 with data.refreshRequired=true; clients preserve displayed
pages and explicitly refresh the first page. There is no retained full estate snapshot,
spool, durable activity table or delivery outbox.
