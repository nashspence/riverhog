# #968 — complete test-purpose and evidence accounting

Non-authoritative external reference under [#903](https://github.com/nashspence/riverhog/issues/903).
The [current #968 issue and decisions](https://github.com/nashspence/riverhog/issues/968) own scope and acceptance; the earlier conversation and example schemas do not.

## Start with one real slice

Read **P00 in [INTEGRATION.md](INTEGRATION.md)**, then its named source files.
Carry the existing event-cursor restart witness through actual collection,
observation and operation qualification, alongside policy and tooling controls.
Continue through P01–P04; the first slice is not issue completion.
[ACCEPTANCE.md](ACCEPTANCE.md) gives the finite completion and challenge matrix.
No new architecture study, universal proof engine or mass directory move is needed.

## Provenance and source

- Producer: OpenAI ChatGPT; model: GPT-6 Astra Pro. Hidden configuration/effort is not exposed or asserted.
- Requester: Riverhog maintainer. Publisher: OpenAI ChatGPT through connected GitHub tools.
- Audited authoritative source: `main@e43df5b5acce5d730c28b48b084e87445fbaac3b`.
- Audited source tree: `9546274bcd5918db378d1d12311e7e9cf06aa309`.
- Branch: `reference/external/968-test-evidence-roles`; exact published revision is recorded in #968.
- Supersedes the active handoff at `4916504c9441a1dcde98d9904da6e0dfe9b729e7`; that commit remains in history.
- The updated reference tree uses current audited main plus `.reference/` only. Publication retains the prior reference as first parent and audited main as second parent, permitting a non-forced branch update. This source refresh is not a production integration.

#963's shared validation foundation is now integrated; do not reapply its historical
reference patches or wait for it as an unmet prerequisite. Preserve its advisory
performance policy, exact owner ancestry, actual job-origin attempts, and failed
execution semantics. Reconcile again when authoritative source advances.

## Concrete contents

- [INTEGRATION.md](INTEGRATION.md): P00–P04, current symbols, adoption order and hand-back format.
- [ACCEPTANCE.md](ACCEPTANCE.md): oracle authority, both-direction accounting, producer/consumer parity, challenge cases and stopping rule.
- [AGENTS.section.md](AGENTS.section.md): the exact approved section, for transplantation into root AGENTS. Its relative link is intentionally root-relative.
- [onboarding.patch](onboarding.patch): the section, updated whole-guide digest and one focused guide-policy test against audited main. It is reference input, not applied production code.
- [claim_projection.py](claim_projection.py) and [test_claim_projection.py](test_claim_projection.py): the retained 22 attribution examples.
- [qualification_projection.py](qualification_projection.py) and [test_qualification_projection.py](test_qualification_projection.py): additional sufficiency, oracle and two-way accounting examples using that attribution primitive.
- [source-seams.json](source-seams.json), [manifest.json](manifest.json), and [verify_reference.py](verify_reference.py): focused source and reference integrity, not a maintained test/contract registry.
- [VALIDATION.md](VALIDATION.md): exact performed checks and limits.

## Reproduce reference-only checks

```sh
PYTHONDONTWRITEBYTECODE=1 python .reference/test_claim_projection.py
PYTHONDONTWRITEBYTECODE=1 python .reference/test_qualification_projection.py
PYTHONDONTWRITEBYTECODE=1 python .reference/verify_reference.py
python .reference/verify_reference.py --repo /path/to/authoritative/worktree --ref HEAD
```

The verifier reads committed Git objects; it does not validate uncommitted edits.
Exit 2 means source drift needs review, not a product failure. Run the actual
repository checks only through the normal integration process. Passing these
examples is not passing the first real slice or qualifying Riverhog.

## Authority and publication

Keep the reference in a separate worktree. Do not merge it into main or import
its synthetic modules into production. The examples assume already trusted,
reviewed inputs; they do not discover tests, authenticate artifacts, validate
oracle truth, inspect arbitrary assertions or authorize release. Production
representation and ordinary implementation choices remain with integration.

Creation, testing, publication, requester role or native linkage does not confer
contract or integration authority. No production branch, issue state, milestone,
native dependency, release control or provider permission changes here. Native
Development linkage, when unavailable, must remain explicitly disclosed in #968.
The actual published commit, not a local reconstructed SHA, is the handoff identity.
