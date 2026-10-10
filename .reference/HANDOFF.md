# NON-AUTHORITATIVE EXTERNAL REFERENCE

Owning issue: #964 — Define and qualify large-collection scalability
Convention: #903
Prerequisite: #963, itself following #961; #960 remains independently finishable
Audited base: `main@8425e672c60285a549ca4d5aaba15ecbd99e5bf3`
Audited tree: `d18b813a489955b085094765f98282ea2e2276d0`

Produced by: OpenAI ChatGPT
Model: GPT-6
Configuration: no exposed effort configuration was recorded
Requested by: Riverhog maintainer
Published by: OpenAI ChatGPT through the connected GitHub integration

## Purpose

Preliminary, decision-oriented research on realistic 1,000–100,000-member
collection scaling and what measurements must exist before accepting targets
or large architectural changes. This source-controlled material is *not* an
integration branch, maintainer approval, performance guarantee or permission
to change preserved archive/permission/contract semantics.

- `RESEARCH.md`: source audit, separate confirmed findings versus conditional
  growth models, candidate workloads, architectural decision points and the
  sequence required for approval.
- `WORKLOADS.json`: candidate scenario taxonomy and required measurement
  dimensions; all numeric future runtime targets are intentionally unapproved.
- `probe_scaling.py`: stdlib source-anchored benchmark of a scanned decision
  lookup, with an explicitly hypothetical incidence model; no server imports.
- `test_probe_scaling.py`: strict AST drift checks, exact comparison counts,
  missing-decision safety and distinction between models and measurements.
- `SOURCE_PROBE.json`: retained locally observed lookup counts/timings and
  clearly labeled analytical examples. It is not a Docker qualification.

## Validation actually performed

- Source checkout pinned to the base SHA and tree above; verified Git object
  identity, changed-file scope and source locations with local git and AST.
- `python -m unittest discover -s .reference -p test_probe_scaling.py -v`:
  four checks passed on the pinned checkout.
- `python .reference/probe_scaling.py --source-root . --output ...`:
  executed actual extracted lookup method body at 128/512/1000/2048 members,
  counted each comparison and rejected unknown output. The speculative
  10,000/100,000 examples are arithmetic only.
- JSON schema/category self-consistency and syntax checks; `git diff --check`.
- No production files were changed by this reference.

## Not validated or limitations

- No container engine, locked mise/Python 3.12.3 qualification environment,
  PostgreSQL service, actual FTP/Stove0 full lifecycle, 10,000–100,000
  final-image collection run, CI/CodeQL or provider/release gate was executed.
- Runtime numbers from earlier #960 comments belong to older exact SHAs;
  the latest source cannot inherit their passing status or timing.
- Native Python microbench timings are host-specific and include an
  instrumented output-id accessor; they are *not* codec, API or cryptographic
  timings. Only the observed comparison counts are deterministic for this
  exact extracted method and the stated lookup sequence.
- Incidence examples do not prove the actual index is quadratic at 100,000.
- The entire plan and any future target must be reconciled against a precise
  post-#963 main SHA; #964 issues the future decision, not this branch.

## Repository authority / handoff

All scope and accept/reject decisions remain in #964, related owner issues,
and the normal integration rail. #963 still owns the measurement framework;
#960 still owns the original 128-file performance objective. Do not merge
`.reference` into main as a new policy registry or apply hypotheses as fixes.

Once posted, the **exact published GitHub reference SHA** in issue #964 is the
handoff identity; the branch is navigation. If the GitHub publisher cannot
establish the native Development relationship or dependency edge, record that
limitation in #964 and let a capable publisher add it without changing SHA.
