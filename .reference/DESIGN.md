# Reuse accepted prefix facts at server finalization

This is the design argument for the focused response to
[#964 comment 6855786223](https://github.com/nashspence/riverhog/issues/964#issuecomment-6855786223).
The audited implementation is `8ce49de94c32ae39562aab5ea4178b1dc8c75e77`
on `codex/964-session-owner` / PR #979. The reference changes the selected
nonprimary-root check inside `_validate_provenance_closure`.

## Decision

Reuse the **existing confirmed, frozen Native journal result**, then compare
`JournalSummary.anchor_at(selected.through_sequence)` with the entire selected
anchor. Open that result through `open_upload_journal_result` in the current
finalizer transaction and receive its original resource owner before advancing.

Each selected attachment still has a current source/catalog admission and an
indexed exact-prefix lookup. It performs no new Native validation session,
semantic replay, source read, or whole-prefix rehash. The retained admission
result already holds the required semantic fact. There is no new durable fact
table, collection-sized in-memory map, or semantic-validation cache to evict.

**The integrity timing change is explicit.** The member cursor remains
provisional. This check proves the selected prefix against the exact admitted
source; it does not inspect the current raw catalog chunk contents. The existing
complete-history and full-corpus validation still reads current raw chunks
before `provenance_closure_validated` can become true. A same-metadata chunk
mutation between member turns can therefore be reported later, at that barrier,
instead of during the next selected-root attachment. The reference tests that
the mutation cannot produce closure success. No root publication, safe release,
or successful preservation is authorized by the provisional member cursor.

This is an alternative ordering of proof obligations, not a claim that a
metadata identity proves freshly read bytes. The integration owner should review
this timing boundary explicitly when consuming the reference. If an additional
contract requires immediate raw-byte rejection at every provisional member
turn, this alternative does not meet that extra contract; a metadata cache
cannot manufacture that guarantee either.

## Why the existing fact proves this exact check

The replaced call consumed only successful prefix validation and then closed
the summary. It did not inspect the nonprimary prefix's effective graph or
borrow member-specific semantics. The primary member path still validates its
exact historical graph and verifies delivery association, materialization,
completion requirements, and selected history authority as before.

Native entry replay publishes each accepted frame only after its semantic and
recording checks finish. The atomic publication stores that frame's reference
and SHA-256 checkpoint. A confirmed final result cannot have an unfinished
frame. The final source check additionally binds the accepted frame count,
source length, source hash, and current source identity. Thus an indexed frame
header in that admitted result is evidence for an already validated prefix.

The helper compares every coordinate, rather than accepting a journal name or
current tail as a substitute:

| Coordinate | Required equality |
| --- | --- |
| Journal | Exact `journal_id` |
| Selected frame | Exact sequence, entry ID, and JSON digest |
| Prefix | Exact byte length and SHA-256 digest |
| Catalog and policy | Current catalog checkpoint and `require_profiles=False`, as in the original selected-root call |
| Sealed source | Current source/job/capture binding and confirmed completed Native result |

The selected prefix may precede the physical journal tail. `anchor_at` supplies
the stored prefix header; it does not return the tail's effective graph as a
historical graph. The new caller witness includes such a nonterminal prefix.
The existing source-anchor tests independently compare indexed headers with
fresh validation of every prefix, including a later correction.

Relevant owning sources at the audited base:

- [Entry replay](https://github.com/nashspence/riverhog/blob/8ce49de94c32ae39562aab5ea4178b1dc8c75e77/packages/riverhog-provenance/src/riverhog_provenance/_entry_replay.py): semantic finish, recording, then atomic `_publish`.
- [Journal session](https://github.com/nashspence/riverhog/blob/8ce49de94c32ae39562aab5ea4178b1dc8c75e77/packages/riverhog-provenance/src/riverhog_provenance/journal_session.py): `_finish` checks terminal source and unfinished-frame consistency before freezing.
- [Journal prefix](https://github.com/nashspence/riverhog/blob/8ce49de94c32ae39562aab5ea4178b1dc8c75e77/packages/riverhog-provenance/src/riverhog_provenance/_journal_prefix.py): selected-anchor/exact-tail checks.
- [Journal summary](https://github.com/nashspence/riverhog/blob/8ce49de94c32ae39562aab5ea4178b1dc8c75e77/packages/riverhog-provenance/src/riverhog_provenance/journal.py): `anchor_at` validates the indexed canonical header and finishes its stored hash state.
- [Source-anchor tests](https://github.com/nashspence/riverhog/blob/8ce49de94c32ae39562aab5ea4178b1dc8c75e77/packages/riverhog-provenance/tests/test_source_anchor_growth.py): historical-prefix equivalence, corruption, reopening, independent readers, and measured indexed work.

## Identity, authority, and lifetime

The reusable fact belongs to the existing Native validation job and frozen
workspace. Its identity includes the source binding, capture attempt/directory
and version, source bytes/hash/frame count, catalog checkpoint, graph
certificate binding, workspace identity, worker epoch, completed progress, and
terminal anchor. The selected anchor is then checked within that exact result.
The new helper does not shorten this identity to a digest-only cache key.

`prepare_upload_journal_result` locks the current sealed journal and job rows,
checks their full confirmation, and checks the current catalog checkpoint.
`open_upload_journal_result` acquires the existing shared workspace lease,
opens the exact retained result, verifies current source/job authority, and
checks the terminal anchor. The caller keeps its transaction and row locks.
These facts do not replace authorization.

`ArchiveCatalogFinalizer` continues to own the current finalization claim,
construction, control gate/epoch, cursor preimage/postimage, and commit
receiving. It supplies the same `UploadJournalResultResources` owner used by
catalog projection. The private direct-advance wrapper now also requires an
explicit resource owner; it cannot create a disposable owner and lose an
uncertain opening.

| Event | Required behavior in this reference |
| --- | --- |
| Next member/bounded turn | Reopen and admit the current exact sealed result; reuse its durable facts, not a remembered permission. |
| Service restart | Reopen the existing completed job/workspace. No process-local cache is required. |
| Rolled-back member transaction | Retry the same original cursor after receiving its original resources. No provisional progress survives rollback. |
| Commit succeeded but reply was lost | Receive the captured original postimage/result; do not select the next member or repeat the successful body. |
| Changed claim, construction, epoch, or terminal selection | Existing finalizer authority rules remain in force. Original receiving is distinct from new forward work. |
| Changed source, capture identity, job, catalog, certificate, or workspace | Existing admission rejects an inconsistent or unavailable proof. It does not relabel the old result or silently start a fresh validator. |
| Selected prefix differs in any coordinate | Reject before advancing that member cursor. A correct tail cannot substitute for a different historical prefix. |
| Raw chunk bytes change while metadata stays the same | The final current-byte closure/corpus barrier rejects before closure success. Earlier member progress remains provisional. |
| Busy workspace or exhausted admission capacity | Preserve pending work and confirmed outcomes; retry through the existing owner. Capacity does not truncate histories or impose an archive-size limit. |
| Missing retained workspace | Report unavailable retained state through the existing admission policy. This helper neither repairs it nor falls back to per-member full validation. |
| Uncertain opening or close | Retain the exact original receiver and grant until closure is confirmed. A new forward turn first receives those resources. |

The underlying proof already has an application-owned retirement lifecycle and
is also required by later catalog projection. The validation job remains linked
to its upload journal; retirement selects only detached jobs and rejects linked
sources. Successful sealing closes the live validator while preserving the
completed job/workspace. The upload remains present through catalog projection's
index phase. Publication or admitted terminal cleanup later detaches it, and an
exclusive retirement lease excludes live readers. This earlier consumer adds no
retention horizon or new archive custody/reclamation authority. See
[job storage](../riverhog/src/riverhog_core/catalog_journal_validation_models.py)
and [retirement](../riverhog/src/riverhog_core/services/upload_journal_validation_retirement.py).

Owning sources:
[sealed admission](../riverhog/src/riverhog_core/services/upload_journal_result.py),
[original receiving](../riverhog/src/riverhog_core/services/upload_journal_result_receiving.py),
[catalog finalizer](../riverhog/src/riverhog_core/services/archive_catalog_finalizer.py),
and the new [selected-prefix helper](../riverhog/src/riverhog_core/services/upload_journal_prefix.py).

## Complete-history and root-last safety

The changed code still verifies each member's immutable primary, descriptor,
root/import sets, member binding, producer completion requirement, and
materialization decision. It still rejects absent or unsealed selected journals.
An exact source-prefix fact does not become member authority.

After the member cursor covers the exact artifact set,
`_validate_staged_canonical_journal_set` still resolves all retained member
histories from current raw catalog chunks, verifies documentary dependencies,
validates all physical journals, checks corpus relationships and execution
completion, and returns successfully before the closure flag is set. Archive
tree, encrypted object, root-last, and independent recovery paths are unchanged.

The complete-history resolver's `snapshots` table has a primary key derived
from the entire exact anchor and a `done` flag. Repeated direct selections are
not pending snapshots again. This deduplication survives eviction from the
resolver's small summary cache for that closure's lifetime. It is distinct
from calling `summary_at` again, whose cache-hit path intentionally rereads and
rehashes current bytes. The reference does not weaken that provider read fence.

There are deliberately separate current-byte checks at the final history
snapshot and full physical-journal levels. These are constant structural passes
for the tested repeated exact root. They must remain visible in the complete
caller witness; they are not zero-cost, and prior admission is not counted as
fresh validation at finalization.

## Work and state bounds

Let `A` be actual selected-root attachment records, and let `U` be distinct exact
selected anchors. The old selected-root loop performed a new full semantic
validation for every attachment. For one shared prefix of size `P` attached to
`F` members, that loop passed `F × P` bytes into fresh validation sessions.

The new selected-root loop performs one current sealed-result admission and
one indexed accepted-anchor lookup per attachment. The successful helper's
declared source-payload bytes and Native validation advances are both zero.
Its metadata/index work composes the existing source-owned admission and anchor
declarations, including catalog size, frame count, header/path sizes, and their
index costs. The required `A` membership checks are not relabeled copies of the
shared journal's semantic work.

The full caller still validates distinct snapshots and the physical corpus.
For a fixed set of exact direct roots, increasing attachment fanout therefore
adds membership/admission metadata work rather than multiplying shared-prefix
semantic work. The maintained witness varies fanout and shared-prefix extent
independently and counts real delegate work throughout the complete provenance-closure owner
sequence, including the terminal barrier.

No new durable rows are added. The existing Native job/workspace stores evidence
proportional to its admitted journal/index state. Each selected check retains
one original opening and receives it before proceeding; the existing owner
allows at most three simultaneous openings, including originals awaiting
receiving. The helper does not retain state proportional to collection fanout.
It adds no total archive, member, journal, or prefix-length limit.

The declaration separates admission/anchor metrics from original final-reader,
construction, and lease closure. Those lifecycle operations keep their existing
owners and grants; failure receiving is covered separately. Physical I/O,
lock waits, and CPU/wall time are observations, not numerical speed gates.

### Remaining admission cost

Reuse does not make each attachment free. The existing delegates declare the
following work for one successful helper call; these are source-owned counts
and upper bounds, not a new whole-finalizer measurement:

| Admission/header operation | Per attachment |
| --- | ---: |
| Catalog statements / returned rows | 4 / 4 |
| Read-only Native database opens | 1 |
| Native admission SQL statements, including terminal-anchor check | At most 65 |
| Selected-prefix SQL statements / returned rows | 1 / 1 |
| Lease descriptor opens / durability calls / lock calls | 4 / 3 / 5 |
| Prefix hash continuation | 1 or 2 padding blocks |
| Source payload bytes / Native validation advances | 0 / 0 |

The caller's selected-journal lookup, metadata traversal, original receiving,
primary validation, and terminal barrier are additional. For 100,000 selected
attachments, the admission component alone would request 400,000 catalog
statements and 300,000 lease durability calls. This is arithmetic on declared
operations, not a timing prediction or a large trial. The final witness records
selected-helper wall and thread CPU intervals separately from expensive fixture
admission and the rest of finalization; there is no matched baseline speed claim.

The current file batch is 1,024 members. A bounded attachment helper does not
prove that such a member turn, or a member with many roots, finishes quickly.
If measured admission/lease work later dominates, a bounded cache within the
same fenced transaction could be considered using the existing original-grant
pattern. It would require its own currentness, eviction, and uncertain-close
witness. The existing journal-ID cache is not a cross-turn permission proof.
This packet keeps that additional mechanism out of the selected recovery change.

## Scope and finite consumption

This resolves the repeated **selected nonprimary prefix semantic validation**
at the cited server finalization boundary. It is not a declaration that the
entire G13 pipeline, all imported/documentary reference patterns, overlapping
prefix selections, primary validation, per-member root traversal, terminal
transaction size, or observer per-member closure behavior now conforms to every
growth obligation.

The original G13 inventory requires distinct structural work and rejects
multiplying a shared completion document by output count. See the
[pinned inventory](https://github.com/nashspence/riverhog/blob/e35bddf8f1bb72ca3f0046b597d5966cd5a08312/.reference/v1_inventory/families-G09-G20.json).
Existing dimensions and independent evidence remain controlling; this packet
does not charge an implementation-expanded history relation as new input.

The first integration step is to reconcile the focused diff with the then-current
PR source and reproduce the maintained caller witness plus affected lifecycle
tests listed in [VALIDATION.md](VALIDATION.md). Review the provisional-cursor
versus final-byte-barrier timing explicitly. Record the finite disposition in
#964, retain the existing final barrier, and qualify the actual integration
source through the normal rail. A broader publication campaign is not needed
to decide whether this exact proof reuse is correct.
