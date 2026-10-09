# NON-AUTHORITATIVE EXTERNAL REFERENCE

Issue: #960
Convention: #903
Audited base: main @ `9d15a170794fddcb66dafa863247959552f861e0`
Audited tree: `bc1bdc117f2f4aa9d845b3f9de0146db831b585d`

Produced by: OpenAI ChatGPT
Model: GPT-6 Astra Pro
Configuration: no additional exposed effort configuration recorded
Requested by: Riverhog maintainer
Published by: OpenAI ChatGPT through the connected GitHub integration

## Publication-side revision (current handoff)

This revision extends reference `515257eac3d6a2ac8bb17016fa0e052e15bb560f`
without rewriting it. The audited authoritative base remains
`9d15a170794fddcb66dafa863247959552f861e0`. Producer: OpenAI ChatGPT,
GPT-6 Astra Pro; requested by the Riverhog maintainer. The original observation
batching and exact-recipe validation changes below remain part of this branch.

**Goal:** under 600 seconds for the complete 128-WAV-plus-XMP scale lifecycle
using already prepared, verified final images. Cold image construction is
measured separately. This is an engineering target, not a measured result or
permission to weaken the proof, identity checks, history, custody, or deadlines.

### Additional root-cause fixes

- The generic incremental producer no longer re-registers all pending output
  artifacts after every output's history is attached. Upload completion refreshes
  only the payload-unsealed frontier. Members that can acquire full custody are
  then checked with read-only, bounded, fair receipt polling (at most 16 per call).
  Unknown sealing state is treated conservatively. A payload seal only releases
  the producer's internal reader; caller-owned bytes still require a validated
  complete custody receipt. Error paths keep candidates pending, and restart
  reconstructs truth from the server rather than trusting this transient queue.
- The incremental derived writer retains one history-transfer helper for its
  construction. That helper memoizes at most 128 successful receiver-prefix
  comparisons, keyed by exact receiver and selected-prefix identities. Every
  append still opens and verifies its own claim-scoped source-history closure;
  every comparison still obtains current destination status/authorization.
  Changed or deleted destination state cannot be accepted from the cache.
  Cold overlap reads stop after the selected prefix instead of consuming the
  unselected remainder of a longer journal. Eviction only causes revalidation.

No new HTTP or archive contract, target-worker concurrency, recipe, fixture,
permission policy, timeout, or gate change is included. These changes affect
reusable Riverhog client behavior, not a Stove0-specific performance shortcut.

### Reproducible counting evidence

`.reference/benchmark_publication.py` runs the real producer and transfer with
explicit deterministic API doubles. It measures request/member work, **not**
Docker, network latency, actual archive throughput, or end-to-end qualification.
The raw baseline and candidate JSON record runtime blob hashes and environment.
For the 128-audio-equivalent workload (257 output members in an open pack):

| Operation | Before | After |
| --- | ---: | ---: |
| Registered member rows, including new members | 33,411 | 258 |
| Registration API calls | 2,451 | 258 |
| Receipt GETs while payload remains unsealed | 0 | 0 |
| Successful premature custody receipts | 0 | 0 |

The 16/32/64/128-audio-equivalent experiments demonstrate linear new-member
registration work rather than repeated growing-prefix work. A separate
seal-every-member/history-delayed case also has linear registration work; it
uses bounded individual GET polling and can make more total HTTP calls than
batched writes for some workloads. Do not describe this as a universal reduction
in every request count. It removes repeated metadata registration/validation
and write-lock work; the JSON retains both kinds of calls.

For 128 comparisons of the same selected prefix against a longer receiver:
128 fresh destination status checks remain; receiver streams drop from 128 to
one, and unselected-tail chunks consumed drop from 128 to zero.

### Validation of this revision

- **58 tests passed** in 67.82 seconds under the unchanged strict warning policy:
  the 24 new publication regressions, all 17 earlier reference regressions, and
  17 existing producer/custody/provenance tests. This includes the real SQL-backed
  service-level multi-pack test, which preserves the bounded pending-source
  window and exact custody/finalization behavior.
- Eleven selected new amplification regressions **fail against untouched prior
  reference runtime**, confirming that they distinguish the old repeated work.
- The existing service-backed 25-member/multi-pack bounded-window test passed
  both old and new source (45.78 and 41.84 seconds). These individual timings are
  not a performance claim; the important evidence is continued correctness.
- `git diff --check` and Python syntax validation passed. Final publication must
  verify each blob and the reconstructed Git tree against the tested source.

These runs are source-native on Python 3.13.5 rather than the pinned 3.12.3
workspace. Docker, mise, ruff and mypy are not installed here. A dependency
installation was attempted but the environment could not resolve its package
index. Workspace modules were loaded from source; external test support provided
a transcription of upstream rfc8785 v0.1.4 and minimal riverhog-server 0.1.0
installed-version metadata. Neither workaround, exclusions nor warning-policy
changes are committed. Locked-toolchain results must still be obtained by the
integration agent. The previous observation-only validation and environment
record remains at the parent reference SHA. The larger published-operation lifecycle was also attempted
but stopped at an unavailable installed Linux provenance-provider entry point;
it is not counted as a pass. That check remains required under the locked workspace.

**Not validated:** the locked four mandatory gates, complete Linux qualification,
final-image 128-file proof, exact-SHA CI/CodeQL, long/large-media batches, or an
end-to-end sub-ten-minute result. No blanket absence-of-regressions is claimed.

### Integration order and residual cost

Reconcile with the active #948/#953/#960 candidate rather than layering duplicate
recipe caches or reverting its scratch/dispatch/local-scheduling fixes. Carry the
new tests into the maintained unit roots. Run the full source and final-image
validation rail before integration acceptance. Preserve the single 128-member
input, real complete output, exact lineage/custody, replay, and all normal gates.

Run the default scale proof from fresh disposable state and prepared exact images,
recording upload, preview, work initiation, observation/evidence preparation,
target materialization/encoding, publication and settlement separately. Report
all attempts, not just a warm successful sample. Keep native counters distinct
from complete qualification time. The intended target is <600 seconds; if it is
missed, identify remaining owner-level amplification rather than hiding it with
fixture cuts, stale evidence, skipped validation, or increased deadlines.

This patch does not cache a source-history closure or suppress fresh permission
checks. Per-output history validation and final completion-record construction
may still be expensive and need separate exact-scope measurement. Their cost is
not proven solved by the receiver-prefix cache or the counting experiment.

Reproduce native operation-count evidence from the locked workspace:

```sh
mise x -- uv run --locked --all-packages --group dev \
  python .reference/benchmark_publication.py --output /tmp/publication-counts.json
```

## Earlier fixes retained in this combined reference

The complete changes and original evidence remain in parent reference
`515257eac3d6a2ac8bb17016fa0e052e15bb560f` and are inherited here:

- Exact retained compiled-recipe/closure verification cache: every read still
  queries the database and verifies the requested identity; successful pure
  validation only, bounded admission, deep copies against mutation. The original
  native warm-load benchmark improved approximately 144-145 ms to 6.6-7.4 ms.
- Independent FFprobe streams and materialization-hint batches of sixteen:
  128/129 subjects require 8/9 rather than 128/129 physical deliveries. All
  subjects remain covered and whole-scope provenance remains unsplit.
- Per-source FFprobe cleanup keeps materialized plaintext scratch bounded within
  a larger batch, including error paths. No extra workers are added.

Those microbenchmarks and the native publication counters are not an end-to-end
speedup forecast. Provider descriptors and physical evidence legitimately change
with batching; no stale descriptor or accepted-execution evidence may be reused.

## Changed files in this revision

Runtime:
- `packages/riverhog-client/src/riverhog_client/producer.py`
- `packages/riverhog-client/src/riverhog_client/processing/history_transfer.py`
- `packages/riverhog-client/src/riverhog_client/processing/writer.py`

Tests:
- `tests/unit/test_incremental_custody_polling.py`
- `tests/unit/test_history_prefix_reuse.py`

Reference-only evidence:
- `.reference/benchmark_publication.py`
- `.reference/publication-count-baseline.json`
- `.reference/publication-count-candidate.json`

Keep `.reference/` as external handoff/evidence, not a new runtime policy
inventory on main. Code and executable tests remain the behavior authority.

## Authority, publication and preservation

This is externally produced reference material under #903. Its creation, tests,
publication, or request by the maintainer does not establish acceptance or bypass
the repository's normal integration rail. Issue #960 and subsequent maintainer
decisions control. Reconcile against then-current source and the active candidate.
No main or release branch is changed by publishing this reference.

The new published SHA recorded in #960 is the current handoff identity. The
previous reference remains in its ancestry; do not force-push it away. The branch
name is navigation. Publication reconstructs Git objects through the connected
GitHub integration; verify the resulting tree against the locally staged tree.
The API's author/committer metadata is publication metadata, not producer identity.

The interface exposes no native Development-link operation. Record that #903
limitation in the owning issue together with the exact branch and SHA; a Markdown
link does not pretend to establish the native GitHub relationship.
