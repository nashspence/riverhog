# Executed reference checks and limits

Parent: 443adc3b45661401e9a87dad3641e1888e98ddbd.
Source patch base: 152d095038df0c0d0ed642cf265d47b11c65a754.
The product source remains outside this branch's .reference-only update.

## Actually run

* Applied the replacement source patch, using real `git apply --check` and
  `git apply`, to a fresh worktree of the source base. Its staged tree equals
  the prepared implementation tree: d223fd39e8477cc52670947ccc8e8258b7923494.
  Seven code/test paths are represented; no runtime source is changed on the
  reference branch or on main.
* 66 direct tests of the actual stdlib-only `validation_evidence` module passed
  with warnings as errors (standalone run: 0.07 seconds).
* 64 additional runner, aggregate CLI and maintained-profile tests passed with
  diagnostic JSON fixture I/O replacing unavailable native RFC8785 serialization.
  The final combined 130-test fixture-adapter rerun passed in 1.69 seconds. It includes
  the same 66 direct tests; do not sum the runs. These are actual module/function
  bodies, including real small subprocess controls, NOT native JCS, media or
  end-to-end qualification. No fake canonicalizer is present in the product patch.
* Tested absent goals, arbitrarily severe misses, changed/removed goals,
  noncomparable conditions, malformed observations, exact required work/source/run
  coverage, failed/absent growth outcomes, incomplete/failed subprocesses,
  evidence checker exit propagation, no repeated work and no target-driven kill.
* Parsed all seven changed Python source/test paths. Source-patch whitespace
  checks passed; rejected old target approval APIs are absent from the new source.
* Reconstructed the approved #961 guide, verifying SHA-256
  1c427d38b88c9a05fe105dea5bcc862efec52ff516233110fa0ef88b43692f44.
  Applied the new guide patch with real `git apply --check`/`git apply`; output
  exactly equals the approved new sections and handoff sentence with all unrelated
  text retained. Output SHA-256:
  d2cf10da13a9b4976911b5abd101c5ecfe609b1598a5c9c1bb57232160e7302e.

Python 3.13.5; pytest 9.0.2; warnings errors; plugin autoload disabled.
The locked RFC8785 0.1.4 package could not be fetched from the environment's
package index/network. Native dependency or locked-qualification success is not
claimed. Earlier first runs (65 policy tests before one integrity negative was
added, and repeated 64 adapter tests) are development checks, not extra coverage.

## Reproduction after preparing a separate source worktree

```
git apply --check /path/to/reference/.reference/validation-evidence.patch
git apply /path/to/reference/.reference/validation-evidence.patch
python /path/to/reference/.reference/verify_reference.py --repo "$PWD"
```

With the real locked dependencies available, use `--adapters` for native adapter
execution. To reproduce ONLY the declared diagnostic fixture experiment, explicitly
pass `--adapters --fixture-codec`; it prints its limitation and cannot masquerade
as native identity qualification. The helper never silently applies patches.
The guide patch applies to the approved #961 guide, not the runtime patch base.

## Not established

Full locked format/type/lint/unit/dist/build; native RFC8785/JCS vectors and
unchanged canonical identity behavior; complete local/CI/phase/aggregate wrapper
integration; dynamic actual operation/witness coverage and artifact verification;
#964 product growth proofs; real database/HTTP/security/recovery/media/provider or
scale qualification; independent exact-SHA CI/CodeQL. Empty growth bindings and
fixture pass strings do not establish missing operation conformance. The owner
collector must independently verify required artifacts at integration.

Performance target attainment is deliberately NOT an acceptance gate or an
unresolved approval. Required execution/evidence/contracts remain enforceable.
Historical policy patches and approval tests are reachable from the parent only;
no prior test total is relabeled as current production evidence.
