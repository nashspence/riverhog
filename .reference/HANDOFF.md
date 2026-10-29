# NON-AUTHORITATIVE EXTERNAL REFERENCE

Owning issue: [#983](https://github.com/nashspence/riverhog/issues/983)  
Convention: [#903](https://github.com/nashspence/riverhog/issues/903)  
Audited base: `main@209481c189f7387557a85e1c249184642816e103`  
Audited tree: `d0cdc22940e529c8b29668f3d04874a2897b851d`  
Reference branch: `reference/external/983-validation-watchdogs`

Produced by: OpenAI ChatGPT  
Model: GPT-6 Astra Pro  
Configuration/effort: no separately exposed setting asserted  
Requested by: repository maintainer, in the conversation requesting this issue and handoff  
Published by: the same assistant through the connected GitHub tools  
Prepared: October 29, 2026

## Start here

Read #983's current scope and decisions, then **P00 in [INTEGRATION.md](INTEGRATION.md)**. First reconcile the two upload/download test modules against current authoritative source. Review the four-file [first-slice.patch](first-slice.patch), then run the proposed helper and the existing concurrency/control-budget tests at factors 1 and 4 with Riverhog's pinned toolchain. Do not start by replacing every numeric timeout or by implementing the entire inventory at once.

The patch supplies one standard-library policy, sixteen focused tests, and seven incidental thread-wait replacements across five existing test functions. It does not implement the remaining repository-wide migration. [AUDIT.md](AUDIT.md) supplies pinned examples and reproducible candidate discovery; [ACCEPTANCE.md](ACCEPTANCE.md) supplies a finite challenge matrix. Read [VALIDATION.md](VALIDATION.md) before interpreting any test claim.

## Source relationship and authority

All tracked paths outside `.reference/` are identical to the audited base. Proposed implementation changes exist only inside `first-slice.patch`; no product, test, workflow, root-document or public-contract change is active in this reference tree. This is the initial #983 handoff, not a revision of any earlier reference commit. The earlier chat was intent calibration, not an independently authoritative design.

This branch is externally produced reference material. Its creation, testing, publication or request does not establish or extend an accepted design, public contract, release requirement, or permission to integrate it. Authority remains with the owning issue, subsequent explicit decisions and Riverhog's normal integration rail. Reconcile with then-current authoritative source; do not merge the reference branch or apply its patch mechanically.

The exact published reference SHA recorded in #983's publication comment is the durable handoff identity. The branch name is navigation. No native Development association or closing pull request is requested. Producer identity above is distinct from GitHub commit publication metadata.

## Validation and limitations

Executed only newly authored reference code in an isolated temporary tree: **16 tests passed at each of factors 1, 4 and 0.5**, using Python 3.13.5, not Riverhog's pinned Python 3.12.3. Checked patch application against exact base files, Python syntax and AST equivalence of the two existing tests after reversing only the seven watchdog substitutions and their imports/policy assignments. These static checks do not establish behavioral equivalence.

Did not execute repository tests, lint, builds, Docker/Compose, PostgreSQL, browser, native platform, provider or GitHub Actions qualification. No runtime images were built or changed. The reference does not implement global startup validation, shared evidence plumbing, shell/CI adapters, semantic-test repairs, or an exhaustive classified inventory. Those remain integration work, not completed acceptance.
