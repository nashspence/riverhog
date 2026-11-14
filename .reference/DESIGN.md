# Imported primary history facts in the terminal closure

This is non-authoritative external reference material for [#964 comment
6863269082](https://github.com/nashspence/riverhog/issues/964#issuecomment-6863269082),
following [#903](https://github.com/nashspence/riverhog/issues/903). The audited
implementation base is `1ff0a9ce70c23f18aa94d5b52ed036376132021a`. Start with
[HANDOFF.md](HANDOFF.md) for the exact published identity and validation receipt.

## Decision and scope

Order the existing terminal obligations so that the complete, current raw
corpus is validated and indexed before imported histories consume historical
delivery facts. Reuse that one disk-backed corpus through an explicit,
provenance-owned callback interface. Each import still verifies its source proof and
descriptor, obtains current server source/job admission, selects its historical
delivery, compares the exact original State reference, and applies its own
accepted history extent.

This removes the shared-prefix read/hash term that currently multiplies with
import fanout and prevents capacity or eviction from restoring semantic replay.
The reported cache-fitting baseline already held semantic validation at one;
that fact is preserved in the comparison. It adds no process graph cache, durable proof
table, extra source index, retained Native opening, workspace lease, or archive
size limit. The generic reader's fresh provider checks remain unchanged.

This is a finite change to the import-dependent work in the existing terminal
attempt. The complete terminal pass is still synchronous. Its existing whole
attempt reconstruction after rollback or process restart remains explicit;
this branch does not establish bounded resumable terminal turns or whole-path
G13 acceptance. That distinction is necessary for an honest integration plan.

## Why a historical point proof is sufficient

`MemberHistoryClosure._accept_import` consumes the selected delivery's State
external reference. It currently obtains that fact by opening the exact
historical summary and invoking `selected_delivery_occurrence`. Even a warm
summary rereads and hashes the entire prefix, because independent providers
may enforce current access when their iterators finish. Those semantics are
correct for the default reader.

The Native corpus already implements `selected_delivery_support`. It uses the
same delivery predicate over the delivery association, State, Occurrence and
fixity Observation. Its indexed historical lookup selects the most recent birth
at or before the requested sequence and applies the assertion's half-open
retirement interval. A later correction cannot replace the selected prefix's
effective object, and a retired gap cannot silently fall back to an old object.
The returned State retains its original journal, entry, assertion and object
coordinates.

That API deliberately proves only narrow documentary support. The composition
supplies the other obligations:

1. The terminal caller admits the current sealed source and completed validation
   job, locking their rows in its existing catalog transaction.
2. It validates the complete current source bytes against that admitted exact
   terminal anchor, with exact-tail enforcement. Native validates each prefix
   before exposing its accepted entry header.
3. It adds the independently validated source to the existing corpus once. The
   corpus retains original assertion payloads, birth/retirement intervals,
   documentary dependencies and effective semantic inputs on disk.
4. After source population, the closure verifies each source proof and history
   descriptor. Its optional callback performs current server metadata admission,
   exact complete-endpoint equality, historical delivery selection, and a final
   current source/job check.
5. The closure compares the complete returned State reference with the import's
   `input_state`, then queues that binding and its requested extent. Existing
   snapshot traversal still validates the selected historical graphs and their
   documentary dependencies.
6. The terminal caller rejects unrelated staged journals, validates the whole
   corpus and execution completion, and returns before its caller records
   `provenance_closure_validated`. Publication guards remain with their current
   owners.

The early pass also validates and indexes staged sources that history
resolution may later identify as unrelated. Such a source can incur raw
validation work, or fail a source/job/byte/identity check, before the later
membership rejection. It still cannot produce successful closure. Corrupt
multi-frame sources can likewise fail a raw chain check before the historical
prefix-anchor check that ran first at the audited base.

The callback is installed only by the server terminal caller. Early input
admission, independent archive reads, recovery and other generic history uses
keep their existing fresh-reader path.

## Exact identity and lifetime

| Obligation | Owner and check |
| --- | --- |
| Exact source-proof bytes | `MemberHistoryStore.source_proof` reads the bounded canonical object and verifies its content identity. The callback cannot bypass it. |
| Source identity, collection and archive | `SourceMemberHistoryBindingProof.verify_import` compares the proof identity, source identity/collection, archive-root digest and artifact-set digest. |
| Provenance root and membership | The proof verifies the archive's provenance-root digest/length, generation, artifact set/count, and the exact member binding's inclusion. |
| Artifact and history descriptor | The exact binding and descriptor preserve artifact ID, bytes, SHA-256, history digest/length, primary and verified root/import sets. |
| Historical primary | `selected_delivery_support` checks the descriptor's exact journal anchor, delivery association, artifact ID/fixity and member role. The final effective graph is never substituted. |
| Input State | The closure compares the full original external State address, including original entry and assertion coordinates, before recording accepted imported history. |
| Import extent | The existing `(history identity, extent)` worklist and bound/retained root filtering remain authoritative. Reusing a graph fact does not reuse another member's authority. |
| Current complete source | The raw pass enforces the admitted terminal anchor and exact tail. `require_source_endpoint` compares current admission with the maximum indexed source endpoint, not merely an indexed historical prefix. |
| Current capture/job/catalog | `prepare_upload_journal_result` checks the sealed source, capture identity/version, completed job, replay/certificate binding, progress, worker epoch, terminal outcome and current catalog digest. The callback's postcheck compares all admitted source/job fields again. |
| Finalizer ownership | `ArchiveCatalogFinalizer` retains the current claim, construction, gate/epoch, cursor preimage/postimage and original commit result. Corpus and callback do not escape that catalog attempt. |
| Successful closure and root | All raw, history, corpus and completion checks precede the closure flag. The branch does not create archive authority from an index or metadata admission. |

The complete-endpoint check is essential. `corpus.select_source(anchor)` accepts
any exact indexed prefix. Used by itself, it would also accept a source
truncated to an earlier valid frame. The new neutral
`CanonicalCorpusValidator.require_source_endpoint` joins the existing
`source_extents` and `entries` indexes and checks the exact terminal coordinates.
There is no separate in-memory map proportional to the source population.

Metadata admission verifies the completed job record; it does not open or
verify the old retained workspace files. This path always obtains its semantic
evidence from fresh validation of current raw bytes, independently of those
files.

Facts may serve different independently verified source proofs that select
identical canonical bytes. Archive/member proof authority is still checked per
import; it is not inferred from graph identity or journal ID.

## Current bytes and replacement

At the audited base, the terminal full-source pass validates intrinsic journal
semantics without supplying the sealed expected anchor. Selected historical
snapshots check their own prefixes, but that does not cover a semantically valid
changed documentary tail beyond the selected prefix. This reference binds the
full pass to the exact admitted sealed anchor and requires an exact tail. A
same-length, validly encoded changed tail therefore cannot become successful
closure under unchanged sealed metadata.

Moving the pass earlier is safe only within the existing server custody
boundary. The finalizer holds the current upload/construction transaction;
source and completed job rows are admitted and locked before reading. SQLite
uses the existing immediate writer admission. PostgreSQL uses the existing
parent and source/job row locks. Ingress changes to journal bytes and history selections require the owning
parent admission. Validation and capture updates retain their existing
source-state fences; they cannot modify the sealed sources selected here. The
terminal transaction additionally locks each admitted source and completed job
row before reading its bytes. The callback repeats current metadata checks
and rejects an endpoint that differs from the validated corpus.

This is not an independent reader exception and not a claim that metadata
detects changed bytes. An external actor bypassing catalog custody to mutate
chunks after their read would violate the transaction/source immutability
premise; no metadata-only point check could detect that byte change. Each new
terminal attempt rebuilds its raw-byte proof. Generic providers retain their
existing fresh read and close fences. The witness confirms natural exhaustion
for the complete raw pass; exact-prefix readers deliver the admitted byte count
and close their original provider at that extent. It does not equate prefix
completion with consuming bytes beyond the requested prefix.

## Growth and capacity

Let `F` be import incidences sharing an exact historical primary, `P` its prefix
bytes, `J` the distinct staged journals and `S` the distinct selected snapshot
obligations. The reported old resolver reads approximately `(F + 1) * P` for
that shared prefix, excluding its separate terminal full-source pass.

After this change, the terminal raw pass remains once per distinct physical
source and the pending snapshot pass remains once per distinct selected
anchor. Imported-primary admission adds metadata work per import; it adds no
journal source read, source hash, Native validation advance or retained-result
opening. For a shared source with one selected snapshot, complete measured
source reads should be one full-source read plus one selected-prefix read,
independent of `F`. Full-source corpus indexing still performs real Native
frame/assertion work and must be counted separately from provider octets.

The maintained correction fixture deliberately retains both the old primary and
the later corrected full source. It therefore selects two snapshots and expects
three shared-source reads: the complete raw pass, the historical primary, and
the complete retained snapshot. Its measured source bytes are
`2 * full_source_bytes + primary_prefix_bytes`, independent of fanout. It is a
broader retained-history witness than the comment's one-snapshot diagnostic;
their absolute read counts must not be presented as a direct before/after pair.

The new endpoint proof uses one indexed scalar SQL result. Historical delivery
support uses existing indexed metadata projections with explicit per-value byte
admission. Source/job admission has two locked catalog selections and one
postcheck on successful return. The raw pass now also performs current source/job
admission and a postcheck for each distinct source: `3J` catalog statements,
separately from the imports' `3F`. Index seek cost, metadata widths, proof/descriptor
validation and disk I/O remain real work. The maintained witness records these
separately from elapsed-time observations; no elapsed threshold becomes a
correctness gate.

The generic summary cache remains 32 entries / 4 MiB. Imports do not consult
that cache on the optimized path, so exceeding its budget or evicting a summary
does not restore import-dependent prefix replay. Disk-backed corpus/history
state grows with actual accepted work, with bounded row/frame/projection atoms.
There is no total source/collection size cap introduced by this reference.

Other costs remain separate: repeated source-proof and descriptor validation,
distinct historical snapshots, documentary reference resolution, corpus global
validation, completion checks, ordinary member-primary validation, and the
whole terminal attempt's lock duration. Qualification-observer `2N+1` closure
reuse remains the separate #991 question. None is hidden in an import cache-hit
claim, and none is declared solved by these measurements.

## Turns, interruption and receiving

The callback retains no source iterator, Native result reader, construction or
workspace lease. Catalog cursors are closed by their current admission owner;
the copied corpus facts remain in the terminal attempt's existing disk owner.
The earlier selected-prefix helper still uses its original
`UploadJournalResultResources` receiving path unchanged.

On invalid raw bytes, proof failure, State mismatch, stale source/job metadata,
capacity refusal or failed global validation, the terminal attempt cannot set
closure success. Existing closure, corpus and raw summary close behavior
remains in place. The focused regressions cover the default reader's fresh
fences and original uncertain snapshot/Native receiving; callback admission adds
no new lease that could be silently discarded.

If the catalog transaction rolls back, the next attempt rereads and reindexes
the distinct raw sources. If the commit succeeded but its acknowledgement was
lost, `ArchiveCatalogFinalizer` receives its retained original postimage/result
without starting fresh work. A changed claim, construction, epoch or cursor
cannot be treated as the pending original operation. Process restart cannot
reuse this ephemeral corpus; it starts a fresh current-byte proof unless the
durable terminal result already committed.

The preserved synchronous barrier is a material limit. Adopting a resumable
terminal corpus would require an actual parent checkpoint, frozen source-set
identity, retained workspace, epoch/claim rebinding, bounded dependency/order
progress, cleanup and original receiving. The existing retained corpus APIs
are possible building blocks, but wrapping a synchronous loop in small helper
calls would not settle that design. This focused branch makes no such claim.

## Alternatives considered

| Alternative | Disposition |
| --- | --- |
| Reuse the existing raw corpus after ordering it before imports | Selected: complete current bytes and historical intervals are already needed; no duplicate source index or extra Native opening. |
| Lazily index retained Native results per imported source | Feasible, but adds source opening/lease/fsync admission and risks duplicating the terminal corpus. Its frame-budget loop would still leave the outer turn synchronous. |
| Remove warm `summary_at` reads or add a generic metadata cache | Rejected: would bypass authenticated provider fences and current-byte checks outside this server transaction. |
| Use the final graph for the imported primary | Rejected: later corrections can retire the selected delivery or change the effective assertion. |
| Cache full historical summaries for import fanout | Rejected: cache eviction or a prefix beyond 4 MiB would restore replay amplification. |
| Introduce durable bounded terminal turns in this reference | Deferred as a larger owning-state change; the current finite handoff reports its remaining boundary instead of pretending to implement it. |

The executable acceptance evidence and observed cost table are recorded in
[VALIDATION.md](VALIDATION.md). They must be reconciled on the integration
agent's exact source before adoption.
