# Evidence and static verification

The current reading entrypoint is [OCCUPANCY.md](OCCUPANCY.md). This file describes evidence identity and actual work; it does not add an acceptance gate or a runtime observation.

## Exact source relationship

The audited receiving source and reference commit's sole parent are **d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e**, tree **98cf298e60e4ba9c178229eae907e164265f6e48**. All product paths outside `.reference/` equal that source.

The prior reference **46ace062078d08cd70f508a3f3a7d834a92cbb40** has the same product paths and remains the durable Core-stepping handoff. Its [owning disposition](https://github.com/nashspence/riverhog/issues/991#issuecomment-6889902338) retains one-step execution and per-step catalog adoption. This is a new measurement/ownership scope, not a replacement of its observations or a continuation of the rejected grouping proposal.

The [source audit](evidence/source-audit.json) reconciles the exact d4d7 checkout, relevant subtrees, guide/lock files, installed dependency metadata and supplied comment bodies. The only 3b47-to-d4d7 tracked changes remain the Native prefix reader and its owning checkpoint tests. No runtime, test, fixture, configuration or dependency file is modified by this reference.

## Actual analysis and verification

Three parallel static analyses covered:

- caller/scheduler responsibility, fresh authority, actual worker/future and resource receiving, and narrowly selected writer-refusal paths;
- the maintained two-journal setup, conditional preparation/candidate counts, exact source/dependency reconciliation and limits of a tiny observation;
- bounded event identity, clock/admission/entry/receiving semantics, offline interval reconciliation, observer limits and growth composition.

The reports were cross-reviewed, and independent reviewers inspected OCCUPANCY and HANDOFF. Review corrected two distinctions in the final account: Native method entry is not proof that all internal admissions succeeded, and physical worker-slot release at join/collection is distinct from later semantic/resource receiving. No ready flag, scope bucket, thread count or missing acknowledgment was promoted into authority or completion.

The actual finite packaging checks are recorded in [packet-verification.json](evidence/packet-verification.json): exact source hashes/Git objects, dependency source and metadata pins, report relocation identity, context bodies, Markdown targets and source locations, JSON parsing, product-path equality and the focused static scope. Their elapsed time describes static packaging work. Publication readback additionally verifies the actual remote commit, sole parent, exact tree, manifest and clean checkout; the owning issue records that resulting SHA.

**No product/test imports, fixture construction, test collection/execution, diagnostic turns, archive publication, benchmark or qualification run occurred.** The source builder and fixture functions were read, not called. There are no new product worker/reader/lease exits to receive and no fixture scratch to remove. Runtime probe identity, runtime raw-event identity and measured queue/worker intervals are therefore not applicable to this handoff.

The proposed event/reconciliation contract is intentionally documented as a proposal. It does not fabricate a trace or supply an unexecuted reconciler result. The manifest and its offline verification are packet-identity checks, not occupancy analysis.

## Packet map and relocation

| Published file | Role |
| --- | --- |
| [HANDOFF.md](HANDOFF.md) | Short #903 provenance, exact source, first receiving action and limitations |
| [OCCUPANCY.md](OCCUPANCY.md) | Current conclusion, route map, fresh readiness/admission, measurement requirements and disposition |
| [OWNERSHIP.md](OWNERSHIP.md) | Detailed source map for callers, locks/leases, physical execution, transport/lifespan and writer-refusal dispositions |
| [FEASIBILITY.md](FEASIBILITY.md) | Legal two-key setup, conditional phase/candidate counts and why that prefix cannot answer the idle-capacity question |
| [MEASUREMENT.md](MEASUREMENT.md) | Proposed lossless finite event/reconciliation model and work/resource/observer accounting |
| [source-audit.json](evidence/source-audit.json) | Source/dependency/context pins and classified feasibility/receiving reports |
| [ownership-audit.json](evidence/ownership-audit.json) | Source and dependency implementation pins for scheduling/lifetime/refusal findings |
| [growth-inventory.json](evidence/growth-inventory.json) | Current structural bounds and proposed observation envelope, explicitly separate from measured execution |
| [context.json](evidence/context.json) | Exact supplied bodies/URLs for qualification 6889181049, decision 6889902338 and request 6890015223 |
| [request.md](evidence/request.md) | Readable exact request body |
| [packet-verification.json](evidence/packet-verification.json) | Actual finite static packet/source checks |
| [MANIFEST.json](MANIFEST.json) | Byte lengths and SHA-256 of all packet files except the manifest itself |

The detailed reports and their audit JSON are preserved byte-for-byte from the completed analyses. Absolute scratch paths in their metadata identify the original analysis locations; they are not required user dependencies. Their published names map as follows:

| Original analysis name | Published path |
| --- | --- |
| `ready-occupancy-ownership.md` | `OWNERSHIP.md` |
| `ready-occupancy-ownership.json` | `evidence/ownership-audit.json` |
| `ready-occupancy-feasibility.md` | `FEASIBILITY.md` |
| `ready-occupancy-source-audit.json` | `evidence/source-audit.json` |
| `ready-occupancy-measurement.md` | `MEASUREMENT.md` |
| `ready-occupancy-growth-inventory.json` | `evidence/growth-inventory.json` |
| `ready-occupancy-context.json` | `evidence/context.json` |
| `ready-occupancy-request.md` | `evidence/request.md` |

An audit's clean initial/final checkout record describes that agent's read-only inspection, before packet packaging. Root's later `.reference/` additions are external reference material, not changed product source. Exact source pins remain valid across that packaging.

## Supplied reports and limits

[6889181049](https://github.com/nashspence/riverhog/issues/991#issuecomment-6889181049) reports d4d7 fully qualified; this handoff does not independently receive its complete CI artifacts or rerun qualification. [6889902338](https://github.com/nashspence/riverhog/issues/991#issuecomment-6889902338) records the integration owner's one-step decision and the planned finite catalog-error diagnostic. [6890015223](https://github.com/nashspence/riverhog/issues/991#issuecomment-6890015223) reports that diagnostic and requests the present reference.

The new report's 465 `SQLITE_BUSY` / `BEGIN IMMEDIATE` failures and timing totals remain report-attributed. The exact raw/profile digests are retained, but no raw trace bytes or exact diagnostic probe were supplied with the request or located in the bounded workspace review. In particular, the scope buckets 302 under `producer.finish` and 163 without a parent scope do not identify actual source callers or dispositions. Their contents cannot be reconstructed from hashes, SQL structure or source possibilities.

The [earlier Native phase reference](https://github.com/nashspence/riverhog/tree/8b639901c4dbe157dce904acec718a23f580c6ca/.reference) retains its own 3b47 observation and source pins. Its [coordination analysis](https://github.com/nashspence/riverhog/blob/8b639901c4dbe157dce904acec718a23f580c6ca/.reference/COORDINATION.md) and [source audit](https://github.com/nashspence/riverhog/blob/8b639901c4dbe157dce904acec718a23f580c6ca/.reference/evidence/source-audit.json) already limit aggregate request/thread-lifetime counts and missing historical caller/error details. This packet uses that context without claiming a new runtime reconciliation or transferring new error metadata to the older 418 failures.

No timing comparison is made between differently conditioned observations. Whole-path #964 acceptance, G10/G12/G13, correction-wide work, transport/transaction cost and the actual ready/admissible-work/idle-authorized-executor question remain open. The reference identifies what source ownership and evidence would resolve a narrow measurement claim; it does not clear all remaining costs as constant or non-growing.

## Verify packet identity without executing the product

In a checkout of the exact published reference SHA recorded in #991, this standard-library command checks the sole parent, product-path relationship, file set and manifest:

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

Source/runtime facts remain owned by their implementation and current issue decisions. The manifest does not create authority to run an experiment, change concurrency, accept a design, merge or release.
