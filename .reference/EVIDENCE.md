# Evidence and receiving notes

The current entrypoint is [HANDOFF.md](HANDOFF.md); the researched answer is [RESULTS.md](RESULTS.md). This file describes provenance and coverage so the evidence can be received without repeating product work.

## Files

| Material | Location | Status |
| --- | --- | --- |
| Native source reasoning and algebra | [NATIVE.md](NATIVE.md) | Static audit at the exact base; its final paragraph describes the diagnostic proposed before execution. The later result is separate. |
| Core fences, concurrency, transport | [COORDINATION.md](COORDINATION.md) | Static audit, including pinned installed Starlette/AnyIO/FastAPI sources |
| Exact phase/source reconciliation | [RECONCILIATION.md](RECONCILIATION.md) | Derived only from the completed raw observation |
| Offline reconciliation data and script | [reconciliation.json](evidence/reconciliation.json), [reconcile.py.txt](tools/reconcile.py.txt) | Stdlib-only artifact processing; no product imports or work |
| Disjoint timing and DBAPI counts | [COSTS.md](COSTS.md), [cost tables](evidence/cost-summary.json) | Instrumented observation; no matching performance control |
| Source/tree/dependency pins | [source-audit.json](evidence/source-audit.json) | Original static audit; its statements that no tests ran describe that audit, before the later diagnostic |
| Completed observation | [run.json.gz](evidence/observation/run.json.gz), [raw identity](evidence/observation/raw-identity.json) | Lossless gzip of the original unmodified 3,240,236-byte UTF-8 JSON receipt |
| Original exits, timing and stdout | [execution.json](evidence/observation/execution.json), [lane.json](evidence/observation/lane.json), [output.log](evidence/observation/output.log) | Original completed process receipts; output log is empty, not missing |
| Exact tiny inputs and ordinary expected results | `evidence/observation/{initial,checkpoint,correction}.jsonseq` and matching `*-expected.json` | Original bytes; runtime-generated identities are preserved |
| Failed preflight | [preflight/run.json](evidence/preflight/run.json), its execution/lane/log and original script | Exit 1 at the uv-version assertion before Native imports/work; preserved, not overwritten |
| Completed external probe and launcher | [probe.py.txt](tools/probe.py.txt), [run.py.txt](tools/run.py.txt) | Executed outside the repository so the diagnostic source stayed clean |
| Independent receipt review | [receipt-review.json](evidence/receipt-review.json) | Hash/result/exit/receiving/source checks; no further product execution |
| Harness syntax and style checks | [preparation-checks.json](evidence/preparation-checks.json) | AST compilation without execution, pinned Ruff check and format check |
| Request and supplied issue context | [request.md](evidence/context/request.md), [comments.json](evidence/context/comments.json) | Original retrieved text, including the original 931 publication comment; not a new historical measurement |
| Packet byte identities | [MANIFEST.json](MANIFEST.json) | SHA-256 and size of every packet file except the manifest itself |

`source-audit.json` and `receipt-review.json` retain the original absolute workspace paths as provenance. Those paths are not prerequisites for consuming the archived copies. The raw observation names the actual external scripts/output directories used for execution; the packet stores their original bytes at the locations above.

The full staged whitespace check reported only the retrieved request's original extra blank line at EOF. That raw text is preserved byte-for-byte to retain its recorded hash; no authored packet file had a whitespace finding.

## Inspect without executing product code

From a checkout of the reference, the following only reads and verifies artifacts:

```python
import gzip
import hashlib
import json
from pathlib import Path

root = Path('.reference')
manifest = json.loads((root / 'MANIFEST.json').read_text())
for row in manifest['files']:
    raw = (root / row['path']).read_bytes()
    assert len(raw) == row['bytes']
    assert hashlib.sha256(raw).hexdigest() == row['sha256']

raw = gzip.decompress((root / 'evidence/observation/run.json.gz').read_bytes())
assert hashlib.sha256(raw).hexdigest() == (
    '019236fdc3b227d49753b54e79915b62d45cf44e788283d3843411611b33c00b'
)
run = json.loads(raw)
assert run['status'] == 'complete'
assert len(run['observation']['advances']) == 479
```

Compare the final commit with **3b47cbba770c31eb1487b50f7e3b16533c10e827**: it has that sole parent, and every changed path is under `.reference/`. The source actually used for the diagnostic is the clean base, before packet files were added. The final reference contains evidence only and does not imply that a different source or a merge checkout passed product qualification.

## Original execution and cap

The complete command is in `execution.json`. The launcher invokes the maintained `scripts/ci_timing.py run` and `run_validation_child`, receives the original exit, and checks clean source at both boundaries. Each attempt used fresh external output and temporary directories. The probe retained original storage owners before work, closed result readers/Sessions/constructions, checked receiving, then removed its workspace. The launcher removed the empty outer temporary directory; it would preserve nonempty leftovers with failure status.

The 120-second overall workload cap is cooperative: checks before/after finite setup/oracle operations, every advance and terminal materialization stop further work at safe boundaries. An entered synchronous call is not forcibly interrupted. Original cleanup/join can finish after the cap, and incomplete work is not reported as success. No cap was reached in this observation. On external cancellation, the maintained process helper joins and raises; the launcher's numeric original child status can then be null, an explicit incomplete-evidence case. That path was not exercised here.

The first preflight's overly strict full-string version comparison failed on the pinned executable's platform suffix. The second script differs by one line:

```python
assert uv.stdout.split()[:2] == ["uv", "0.11.24"]
```

The full version output and uv binary SHA are still retained. The completed probe SHA-256 is **65070c82c5904a3ec7e8ff7ce289f6eb11256f03576b919f1a4cdc114f76600a**; the launcher is **e9d10e658e031e8a60608a8658cc47d25e7565aeba75816b9d02c8334ee93d33**. Python 3.12.3 and Ruff 0.16.0 match repository pins. `mise` itself was not available; the existing pinned interpreter/environment and inspected lock files were used. No dependency upgrade or re-resolution was performed.

## Measurement limits

The 479 advances are only the three instrumented retained executions. Fixture construction, journal append/correction helpers and ordinary oracle verification perform additional genuine Native work, recorded as separate stage clocks without admitted-DBAPI counters. Consequently 479 is not a census of all Native calls in the diagnostic process.

Hooks consume only actual method arguments and returned state/batch fields. They perform no extra ledger loads, source reads or validation calls to observe phases. Their bookkeeping still costs time. Inclusive method intervals nest; selected exclusive same-thread intervals are disjoint within their enclosing stage, but remain instrumented and include observer work charged to those scopes. No uninstrumented matched run isolates that overhead.

The SQL layer is the maintained admitted `_Cursor._call` and explicit `_Connection._transaction`. Execute, executemany, executescript, fetch, iterator exhaustion and explicit commit/rollback are distinct attempt types. Implicit transaction behavior through context managers and statements inside executescript are not expanded into those counters. Returned rows are not engine row visits; the regex table label is a surface classification, not a semantic query plan. These are not the historical Core catalog execution counters.

OwnedSource `read_window` delegates to `read_exact`; the nested exact call is marked by its parent. Count physical leaf delivery once. Direct frame admission uses separate exact reads of the JSON atoms. Source-currentness in this fixture is a boolean check; there is no Core catalog worker/source/certificate predicate execution. No snapshot hash or successful-currentness cache replaces a fresh production fence.

The final product files are unchanged, so this packet is not an implementation branch. No broad test suite, new archive publication, full caller census, cancellation/restart qualification, concurrency utilization experiment or complete declared-growth qualification ran. The normal receiving owner retains all product and integration obligations. No reading step here requires another diagnostic run; adopting an external harness as a maintained scenario would require reconciling it with the repository's owning `make profile` workflow.
