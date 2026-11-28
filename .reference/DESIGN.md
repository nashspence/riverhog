# A prepared view owned by independent recovery

## Decision and avoided alternatives

Keep the change inside `a_riverhog_recovery_tool`. Its private reader already owns independent validation. Add one attempt-owned metadata seek index, adapting the existing Core index's mechanism without importing Core.

The server's [prepared reader](https://github.com/nashspence/riverhog/blob/be483fd6d567616d5fcd355202d61f1e682efe75/riverhog/src/riverhog_core/provenance_archive_read.py) is useful precedent, but wholesale extraction is not a mechanical move. Independent `iter_bindings()` verifies the complete binding-tree commitment before yielding bindings; Core's ordinary binding iteration has a different interface and relies on a separate inclusion path for that proof. Core also owns scratch-capacity handling, provider stream/cache behavior and worker handoff concerns that this single recovery attempt does not acquire.

A new neutral package would add packaging/dependency/public-interface work and coordination between two consumers before that common contract is established. The existing archive-contracts package already hosts SQLite-backed history mechanisms; extending it here would change its ownership/API and coordinate both readers. Neither extraction is needed to eliminate the reproduced multiplier. A narrow neutral extraction can be reconsidered if a second real independent consumer needs exactly this behavior; it is not a prerequisite.

A cache of raw objects also fails to fix the algorithm if each journal still walks and reparses every cached descriptor. A full Python list/dictionary is unbounded in archive size. A generalized plaintext cache would alter source-observation behavior, duplicate existing journal staging and retain more data. None is part of this proposal.

## The operation and lifetime

`CanonicalProvenanceArchiveReader.prepare(workspace=...)` creates a new context-managed `PreparedCanonicalProvenanceArchiveReader`. Creation alone exposes no prepared data. Entering the context:

1. Opens a uniquely named private descriptor index inside the attempt's existing scratch root. Registers the actual index with the reader immediately after connection creation, before fallible schema setup.
2. Reads the selected provenance root, checks its expected SHA256, generation and artifact-set identity, and retains those exact verified bytes.
3. Traverses every descriptor through the existing independent scanner. It checks canonical shape, contiguous sequence, identity authority, binding order/counts, journal identity/order/segment continuity, terminal presence and ordered root commitment.
4. Commits and seals the disposable projection only after the whole scan succeeds. Queries are read-only. A partial or failed scan never becomes ready.

The identity is the captured source callback plus the expected root digest, archive generation and artifact-set identity, within this fresh attempt. No cross-root, cross-reader or cross-attempt projection is reused. A failed retry gets a fresh private index even when the outer recovery scratch path is reused.

The view stays open through `_stage_provenance`, journal/corpus validation, payload staging and `_stage_mapping`'s selected-history work. Before publishing completion, recovery closes payload state and receives the staging context's original journal connection and prepared index. Failure before staging yields ownership now still closes the journal connection. Selection state and its member cursor are also scoped to mapping.

## What is a snapshot, and what is still fresh?

The prepared descriptors are an **authenticated immutable snapshot**, not a claim that the source directory can never change. This distinction is intentional and testable.

| Event | Result / authority |
| --- | --- |
| Wrong expected root, generation or artifact set at entry | Refuse preparation; no ready projection. |
| Truncated/malformed/missing sequence or wrong commitment at entry | Refuse; close owned resources. |
| Root/descriptors change after successful preparation | Already admitted metadata remains the selected-root snapshot for this attempt. It is not silently replaced by later descriptors. |
| Start another attempt after a root/descriptor change | Read and verify the actual source again; reject a mismatch. |
| Requested journal or binding payload changes/disappears | Fresh object access and the captured descriptor's exact size/hash reject it. |
| Selected structure changes/disappears | Fresh content-addressed access rejects it; history membership remains the existing closure's responsibility. |
| Source callback closes the owner during a read | The before-yield state guard refuses exposing buffered output. |
| A partially consumed binding iterator is resumed after close | Refuse, including already decoded bindings in the same page. |
| Description/tag head changes during recovery | Existing fresh ciphertext pins, taken before metadata staging and compared before completion, still reject the changed snapshot. |

Re-reading mutable heads is not routed through this index. `_archive_io.py`, official age/batchpass invocation, archive path checking and decryption stay unchanged.

A concrete reason to use the admitted bytes: the old scanner could authenticate a descriptor sequence and later reread descriptors for extraction. A later read is not automatically covered by the earlier ordered commitment. The new view binds extraction to the actual descriptors that passed that complete scan. It does not substitute a file mtime or a “previously validated” flag for cryptographic authority.

Root export now writes the exact verified root bytes instead of independently decrypting the root a second time. This does not add an extra root recheck at publication; none is claimed. The mutable metadata-head fence remains a separate necessary check.

## Complete work and resource bounds

Let J be selected journals, D all descriptors including the terminal, S journal-segment descriptors, B binding-page descriptors, and H the selected structure accesses. D includes S+B+1; journal payload bytes and binding/structure byte widths are separate dimensions.

- Preparation reads one root and D descriptors once, retains one decoded descriptor at a time and writes D bounded rows.
- Header enumeration visits S indexed rows once. Reading every complete selected journal then visits S rows in total, using a journal-key seek for each journal. Per-journal SQL does not compete with a kind-wide index.
- A separate complete journal-metadata enumeration visits the selected journals' segment rows, not all D rows for each journal.
- A complete binding operation retains the existing two payload passes: one verifies the full binding-tree commitment, the other yields authenticated bindings. It visits 2B selected descriptor rows, with bounded binding pages.
- Requested journal segments and selected structures remain fresh, bounded reads. Repeating the API legitimately repeats those selected reads; the projection does not convert them into cache hits.
- The actual staging caller keeps at most two metadata cursors active (header enumeration plus one journal query), one active plaintext object stream and one staged journal output stream. The descriptor index adds one SQLite connection; its configured cache is 512 KiB and temporary work is file-backed.
- Extra disk state grows with descriptor metadata and SQLite indexes, not full payload bytes. Existing journal staging remains the eventual recovery output. No complete archive is held in a Python container.

These bounds describe source traversal and selected descriptor processing. SQLite B-tree construction and seeks retain their own insertion/search costs; this reference does not label all internal database work O(D), establish a whole-recovery RAM bound, or qualify unrelated semantic-history traversal.

## Receiving, interruption and retry

Bounded source reads explicitly close the supplied iterator in a finally block, including wrong-type/oversize chunks and source exceptions. The positive probe completely consumes source streams and observes their finalization; it does not distinguish exhaustion from explicit close in that counter. Separate interruption and failed-close tests exercise explicit receiving.

The metadata index retains actual cursors until confirmed close, then closes its actual connection, then removes its private directory. A close exception marks it unavailable for reading and keeps the unresolved original on the owner. Repeated close retries that same resource; it does not create a replacement or infer success. Constructor-time registration preserves that ownership if schema setup and subsequent close both fail.

The maintained tests inject both before-effect and after-effect cursor/connection close errors, plus partial iterators and a failed constructor path. Normal staging failures explicitly close a journal connection that previously could be lost before tuple return. Existing recovery retry/no-complete-output behavior is covered by the encrypted suite.

The private metadata directory is deleted only after cursor/connection receiving. The existing outer recovery output and durable publication rules remain in force; the index is neither a durability promise nor archive authority.
