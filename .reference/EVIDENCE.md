# Evidence and finite verification record

Read [DESIGN.md](DESIGN.md) for the current decision. This file explains how the reference was produced and how to inspect its evidence without executing Riverhog.

## Source and method

The audited receiving source and reference commit's sole parent are **d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e**, tree **98cf298e60e4ba9c178229eae907e164265f6e48**. All tracked paths outside `.reference/` equal that source. The [source audit](evidence/source-audit.json) reconciles d4d7 against the earlier **3b47cbba770c31eb1487b50f7e3b16533c10e827**: exactly two paths changed, the prefix reader and its owning tests. The [retained patch](evidence/audited-prefix-change.patch) is that exact Git diff, not a proposed change in this reference.

Three parallel analyses inspected the source and owning tests without importing product modules: source/dependency reconciliation and receiving cases; authority/adoption/resource custody; and growth/resource accounting. They then cross-reviewed the findings. Final independent reads of DESIGN and HANDOFF checked those conclusions against d4d7 and the two supplied comments. The final lifetime review corrected a material ambiguity: missing acknowledgment cannot authorize a new worker epoch while the original operation may still hold its lease. The published design distinguishes reply loss from confirmed process exit.

The analysis used Git objects, source text, existing test bodies, TOML and installed distribution metadata. Source file hashes and Git identities are retained in the audit JSON files. Existing tests were **inspected, not collected or executed**. Agent-local reports record the clean checkout seen during their read-only audits; subsequent additions under `.reference/` are this packet's packaging, not product edits.

No product calls, project imports, fixtures, probe turns, profiling workloads, archive publication, test collection/execution, qualification campaign or benchmark ran for this handoff. No new process/worker/reader lifecycle receipts are claimed because no such product operations were started. Static file/hash/link/Git checks are recorded separately in [packet verification](evidence/packet-verification.json); their elapsed time is packaging work, not Riverhog performance.

The unfiltered staged whitespace check reports five blank context lines in the retained `.patch`: their single space is the original unified-diff context marker. Those bytes are preserved so the patch retains its audited hash. The whitespace check passes for all other packet files; the patch is checked separately for exact byte identity and reverse applicability to d4d7. These are recorded results, not an unqualified claim that the unfiltered check passed.

## Packet map

| Published file | Role |
| --- | --- |
| [HANDOFF.md](HANDOFF.md) | Short #903 provenance, source relationship, first receiving step and authority notice |
| [DESIGN.md](DESIGN.md) | Current recommendation, explicit proposed admission/composition, control boundaries and owning decisions |
| [LIFETIMES.md](LIFETIMES.md) | Detailed fresh authority map, five counterexamples, exit/receiving and recovery reasoning |
| [GROWTH.md](GROWTH.md) | Current direct multiplicities, proposed bounds, independent resource and total-work obligations |
| [VALIDATION.md](VALIDATION.md) | Exact codec reconciliation, 20 focused receiving cases and conditional small-observation outline |
| [source-audit.json](evidence/source-audit.json) | Exact source trees/files, dependencies, context hashes and attributed qualification |
| [lifetime-audit.json](evidence/lifetime-audit.json) | Source/test pins and classification of source-derived counterexamples |
| [growth-inventory.json](evidence/growth-inventory.json) | Current/proposed structural caps and explicitly classified reservations |
| [context.json](evidence/context.json) | Exact body strings and URLs for request 6887528297 and later update 6889181049 |
| [request.md](evidence/request.md) | Readable request body with one terminal newline |
| [audited-prefix-change.patch](evidence/audited-prefix-change.patch) | Exact 3b47-to-d4d7 two-file delta |
| [packet-verification.json](evidence/packet-verification.json) | Actual finite static checks of source pins, bodies, report identity and links |
| [MANIFEST.json](MANIFEST.json) | Exact byte count and SHA-256 of every packet file except the manifest itself |

The three detailed reports and their JSON audits are preserved byte-for-byte from the final analysis outputs. Their original absolute paths are provenance, not required local dependencies. Relocated names map as follows:

| Original analysis name | Published relative path |
| --- | --- |
| `core-stepping-lifetime.md` | `LIFETIMES.md` |
| `core-stepping-lifetime-evidence.json` | `evidence/lifetime-audit.json` |
| `core-stepping-growth.md` | `GROWTH.md` |
| `core-stepping-growth-inventory.json` | `evidence/growth-inventory.json` |
| `core-stepping-validation.md` | `VALIDATION.md` |
| `core-stepping-source-audit.json` | `evidence/source-audit.json` |
| `core-stepping-context.json` | `evidence/context.json` |
| `core-stepping-request.md` | `evidence/request.md` |

The report hashes in the lifetime and growth JSON audits continue to identify their exact published Markdown bytes. The source audit records both exact comment body hashes; the readable request's extra terminal newline does not alter those body strings in context.json.

## Attribution and historical evidence

The request [6887528297](https://github.com/nashspence/riverhog/issues/991#issuecomment-6887528297) names d4d7 as the audited checkpoint. The later [6889181049](https://github.com/nashspence/riverhog/issues/991#issuecomment-6889181049) reports it fully qualified and supersedes the request's pending-CI statement. Its counts, exact-input codec parity, complete-evidence digest and historical catalog-raise localization are retained as **receiving-owner reports**. We did not independently receive the complete raw CI evidence or rerun those jobs.

The prior Native phase reference remains **8b639901c4dbe157dce904acec718a23f580c6ca**. Its [cost analysis](https://github.com/nashspence/riverhog/blob/8b639901c4dbe157dce904acec718a23f580c6ca/.reference/COSTS.md), [derived cost summary](https://github.com/nashspence/riverhog/blob/8b639901c4dbe157dce904acec718a23f580c6ca/.reference/evidence/cost-summary.json), and [losslessly archived original observation](https://github.com/nashspence/riverhog/blob/8b639901c4dbe157dce904acec718a23f580c6ca/.reference/evidence/observation/run.json.gz) are the durable targets for the old local paths mentioned in GROWTH.md. The uncompressed raw observation SHA-256 is `019236fdc3b227d49753b54e79915b62d45cf44e788283d3843411611b33c00b`.

That observation ran on 3b47. The subsequent owner-reported exact-input noncodec parity is useful context; it does not convert its timings into measurements of d4d7 or of a grouped Core candidate. This packet adds a new design scope and preserves that historical reference rather than copying or superseding its evidence. The prior 479 advances, zero nontrivial adjacency traversal, six effective correction assertions and their limits remain unchanged.

## Verify without product execution

The actual published reference SHA is recorded in the owning #991 comment. In a checkout of that exact SHA, the following standard-library command verifies the packet manifest and the sole-parent/product-path relationship. It executes no project code or tests:

```bash
python - <<'PY'
from pathlib import Path
import hashlib
import json
import subprocess

def git(*args):
    return subprocess.check_output(["git", *args], text=True).strip()

base = "d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e"
assert git("show", "-s", "--format=%P", "HEAD").split() == [base]
assert not git("diff", "--name-only", base, "HEAD", "--", ".", ":(exclude).reference")
assert not git("diff", "--name-only", "HEAD", "--", ".", ":(exclude).reference")
manifest = json.loads(Path(".reference/MANIFEST.json").read_text())
expected = {record["path"] for record in manifest["files"]}
actual = {str(p) for p in Path(".reference").rglob("*")
          if p.is_file() and p.name != "MANIFEST.json"}
assert actual == expected
for record in manifest["files"]:
    payload = Path(record["path"]).read_bytes()
    assert len(payload) == record["bytes"], record["path"]
    assert hashlib.sha256(payload).hexdigest() == record["sha256"], record["path"]
print(f"Verified {len(expected)} packet files and unchanged product paths")
PY
```

This verifies packet identity, not implementation correctness or a new qualification. No candidate runtime is present to benchmark. The conditional observation outline in VALIDATION describes what a later, separately decided protocol would need to observe; it is not a request to launch another campaign.

## Limitations and authority

The static counterexamples explain why a bare inner loop and final-only adoption cannot be accepted from the present evidence. They are not newly injected failures. The unchanged sources and existing tests support the reasoning, but do not constitute qualification of a multi-step candidate that does not exist. The bounded A alternative remains a proposal with explicit scheduling, admission, continuation and finalization decisions.

Whole-path #964 acceptance and G10/G12/G13 remain open. The cause/cost of the 418 historical finalization catalog raises, Native transaction cost, transport/lifespan cost and actual ready-journal/worker occupancy are not resolved here. No timing gate, phase fusion, removed durable commit, changed concurrency, provider activation, merge or release is supplied by this reference. Owning-issue decisions and the normal integration rail control any adoption.
