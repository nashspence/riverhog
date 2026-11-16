# Reference validation

Date: November 16, 2026. Audited product source: **0f8030b0c0cd53eb33456924f070ae5979ed307e**.
All implementation files tested below are in `.reference/implementation/`.

## Executed checks

| Check | Result | Observed command elapsed seconds |
| --- | --- | ---: |
| model-tests | PASS | 0.104154 |
| ruff-lint | PASS | 0.019406 |
| ruff-format | PASS | 0.014420 |
| strict-mypy | PASS | 0.191221 |

**52 tests passed: 25 pure-policy cases and 27 accounting/composition cases.**
The raw commands, complete model-test output, exit statuses and hashes are in
[VALIDATION.json](VALIDATION.json). Tool versions match the audited repository:
Python 3.12.3 from `mise.toml`; Ruff 0.16.0 and mypy 2.3.0 from `uv.lock`.
They were provisioned as isolated tools. This was not a complete locked workspace
installation or a substitute for Riverhog's Make evidence pipeline.

The elapsed values above describe these small deterministic reference checks.
They are not product benchmarks, policy-overhead measurements or evidence of
publication/recovery speed. Mypy checks the two implementation/model modules;
it does not claim a repository-wide typecheck. Ruff covers all four Python files.

## Tested implementation identity

| File | SHA-256 |
| --- | --- |
| `accounting_model.py` | `46b6f1fea2715a4f14a7d974af40fd0ee099a91d56148d7b90aa1db9d92bbe4b` |
| `replay_policy.py` | `fc0c0b5d81c64546b36432a0f3d06ed27ce6d9eef9dab357f2032e749dd93669` |
| `test_accounting_model.py` | `a6cd522481239434c764aa77090d15f3ca43eea0e67db4f1a15eeef5d241856a` |
| `test_replay_policy.py` | `0ec3be645c67022f011e69983191c6d159c68ac6454c702678ee49b2fa929ff4` |

The published commit, audited parent and complete published-tree identity are
verified and recorded separately in the owning #995 comment. There is no
self-referential claim that this file contains its own final Git commit SHA.

## Source and policy review

Reviewed README/AGENTS at the audited integration source, workspace/dependency
and protected-boundary policies, #903, #994/#995 and the adjoining #996/#997/#1000
interfaces. Independent delegated reviews traced prepared-primary identity,
Native/Core progress, effect/custody/receiving, surviving control lifetime,
configuration and qualification seams. Product files remain identical to the base;
only `.reference/` is added. Before publication the staged diff was checked for
whitespace errors and for changes outside this directory.

## Blocked and unperformed qualification

`make lint` was attempted and **did not start**: the repository wrapper requires
`mise`, which is unavailable on PATH. Make exited 2 after the wrapper's 127. No
repository lint pass is claimed. Docker is also unavailable in this workspace.

The full locked workspace, unit/growth/runtime/backend/platform/installed/image
and offline recovery suites were not run. No Native optional generation backend
was implemented or exercised here; no measured cost coefficients, real F1
performance comparison, actual checkpoint/restart, power-loss or provider evidence
is supplied. No exact-source product CI qualification is claimed for this reference.
Reference branch workflow/check availability is reported on #995 after publication.

[QUALIFICATION.md](QUALIFICATION.md) identifies the real owner tests, installed
configuration behavior, crash/receiving cases, complete-lifecycle growth evidence,
matched measurements and exact-source checks required for integration acceptance.
Neither these model results nor historic #979 evidence can close #995.
