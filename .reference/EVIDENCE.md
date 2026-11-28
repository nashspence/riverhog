# Reproduced work counts and their limits

## Source and method

The final probe compares exact unmodified Store/Closure owners loaded from `git show 60612eded1e0e4e5188c28a84697435eb969fad0` with the candidate identified in [SOURCES.json](SOURCES.json). Each pair uses the same fixture instance. Other production code is unchanged. The candidate file hashes are checked before and after the probe.

The 17 small neutral cases vary members 1/2/4, root counts 2/4/129, identical root sets shared across distinct histories, private imports 1/2/4, shared ancestors 1/2/4, inherited source roots 1/129, and depth 2/4. They use real neutral journals, exact source binding proofs, the actual canonical validator, and actual SQLite connections. They verify every expected structure and selected journal anchor, original source closes, original connection closes and directory removal. These are 34 observations, not a broad size/platform campaign.

The separate encrypted arm invokes actual `recover_archive` for 1/2/4 members using the maintained physical archive fixture and pinned official age/batchpass 1.3.1. It verifies every payload byte, selected history object and journal, and the complete recovery receipt. Each original outer age process returns zero; owned recovery scratch and the experiment workspace are removed. This adds six observations, for **40 passed observations** total.

Raw final observations: [history-counts.json.gz](evidence/history-counts.json.gz). Source-change observations: [history-export-gap.json](evidence/history-export-gap.json). Maintained validation: [validation.json](evidence/validation.json).

## Actual encrypted recovery counts

| Members | All decryptions, baseline → candidate | History descriptors, baseline → candidate | History pages, baseline → candidate | All history structures, baseline → candidate |
| --- | --- | --- | --- | --- |
| 1 | 39 → 25 | 5 → 3 | 21 → 9 | 26 → 12 |
| 2 | 66 → 38 | 10 → 6 | 41 → 17 | 51 → 23 |
| 4 | 120 → 64 | 20 → 12 | 81 → 33 | 101 → 45 |

These match the earlier maintained-fixture baseline reported in [the integration account](https://github.com/nashspence/riverhog/issues/991#issuecomment-7233006671). The counts include fresh export and recovery's unchanged independent descriptor pass. Four full descriptor admissions per member become two; adding the descriptor's fresh export gives the measured five-to-three read reduction. No decryption is bypassed: fewer redundant reads invoke the unchanged decryptor fewer times.

The fixture's KDF is `log_n=1`, with tiny payloads and local filesystem storage. It is not the earlier PostgreSQL/KDF18 real publication F1 or the approved canonical 16-member pilot. Diagnostic elapsed times retained in raw evidence include instrumentation and overlapping verification activity; they establish no speedup, product maximum, marginal milliseconds per file or throughput forecast. The prior 17.093 seconds were nested cumulative attribution, not an available wall-clock saving to subtract.

## What is constant repetition, and what multiplies

For successful closure work, let H be the number of visited `(history, extent)` obligations, E the actual imported records processed, and P(h) the number of root/import pages including terminals for h. Exclude the separate caller descriptor passes and fresh export from these formulas.

- Original descriptor reads: `3H + 2E`; candidate: `H + E`. Proof reads remain E.
- Original page reads: `5 × sum(P(h)) + 2 × sum(P(source(e)))`; candidate: `sum(P(h))`.
- The first term removes repeated complete traversal within a visit. The second removes full source-set verification per incoming import before the worklist deduplicates the source history.

| Neutral resolution case | Descriptor reads, before → after | Page reads, before → after | Proof reads |
| --- | --- | --- | --- |
| One member | 3 → 1 | 15 → 3 | 0 |
| Four members sharing their root set | 12 → 4 | 60 → 12 | 0 |
| Four members, one shared ancestor with 1 root | 23 → 9 | 119 → 19 | 4 → 4 |
| Same four members/ancestor with 129 roots | 23 → 9 | 132 → 20 | 4 → 4 |
| One member, inherited depth 4 | 23 → 9 | 125 → 19 | 4 → 4 |

Increasing the shared ancestor from 1 to129 roots adds one source root page in this geometry. Original page work increases by13 reads: five in the ancestor visit and two for each of four incoming edges. Candidate work increases by one read. This is direct evidence of the removed multiplication, with actual full journal validation still performed.

The distinct-history shared-set case remains explicit: four histories still read their shared pages four times (12 versus3 page reads). Independent history count and shared-set page count can still multiply. The recursive cycle query runs once per edge, with its descendant traversal cost unchanged. Default primary journal source reads remain per edge. These observations do not establish a linear bound for the complete closure or the entire publication/recovery path.

## Added local work and retained resource cost

The candidate adds savepoint/release commands per public resolution, one indexed exact-binding check per registration, and an indexed fingerprint lookup per recorded object occurrence. It inserts fingerprint rows only on first occurrence. Processing retains one bounded current page, bounded proof/descriptor temporaries, and the existing journal snapshot/cache state; it introduces no collection-sized Python projection. Existing object inventory gains a 64-character digest and an integer extent, with ordinary SQLite overhead; it retains no object payloads. Existing membership-image limits can cause a larger image to remain uncached, without rejecting history.

For the one-member resolution, closure SELECTs rise **6 → 11**; INSERTs stay7; there is one added savepoint/release pair. The existing work transaction has **zero resolution commits in both versions**. Separate full-set heads admissions/commits fall **3 → 1**. All observed SQLite opens fall5→3; peak remains2.

For four members sharing one 129-root ancestor, closure SELECTs rise404→441, INSERTs fall303→288, and four savepoint/release pairs are added without a closure commit. Heads admissions/commits fall23→5; bound-head inserts fall727→69. All SQLite opens fall154→136; peak remains34. The **133 fresh journal reads and journal-store SQL are unchanged**. The proposal removes repeated structural work, not those required journal costs or peak capacity.

## Adverse changes and receiving

The same-authority page counterexample is reproduced against the exact old owners and final candidate. Old export accepts altered root-page bytes despite the unchanged path; final export rejects before yielding the changed object. Repeated import intentionally uses the previously admitted closure fact, so the final failure is at fresh export or fresh admission, not a fictional per-edge page reread. False terminals, a newly opened closure, repaired source retry and exact codec round trips have maintained witnesses.

Other maintained cases cover parent and late inherited failure, proof/current-read rejection, source interruption, conflicting binding, bound→retained selection, original file/SQLite cursor close uncertainty before and after physical effect, explicit cleanup retry, and closure shutdown during an active source read. Existing recovery tests preserve complete inherited/supplementary bytes and the final mutable metadata-head fence.

Retained development observations are compressed separately. An earlier count probe was interrupted during source revision; a preliminary journal-fence test was corrected to run its fence in `finally`; and the unsafe no-fingerprint intermediate was rejected. None is counted as final acceptance or hidden by selecting a fastest rerun.
