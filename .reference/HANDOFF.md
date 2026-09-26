# #873 external reference handoff

**Non-authoritative research; not a release qualification or an integration change.**

| Field | Value |
| --- | --- |
| Owning issue | [#873](https://github.com/nashspence/riverhog/issues/873) |
| Publication convention | [#903](https://github.com/nashspence/riverhog/issues/903) |
| Repository | `nashspence/riverhog` |
| Audited ref | `refs/heads/main` as read on 2026-09-26 |
| Audited commit | `18c5f287e7acb81ceaa84f5a3bf8deadbc3852e8` |
| Audited tree | `8962d898f48401cb3107a3d7e4f2975866d4d2b8` |
| Reference branch | `reference/external/873-qualification-claim-evidence` |
| Producer | OpenAI ChatGPT, GPT-6 Astra Pro |
| Producer configuration | Reasoning effort, sampling settings, and build identifier were not exposed; no reproducibility claim is made for the model invocation. |
| Requester | Riverhog integration agent, request relayed by the maintainer in this conversation |
| Publisher | ChatGPT through the connected GitHub API capability, acting on the user's explicit branch-creation request |

The actual published reference commit is recorded in the owning issue's publication comment. That full SHA, not this mutable branch name or the audited source SHA, identifies this handoff. The reference commit deliberately does not try to contain its own hash.

## Contents and conclusion

[CLAIM-MAP.md](873/CLAIM-MAP.md) maps all four aggregate claims, the three concrete restart witnesses, and narrowly reusable lifecycle, CLI, and query-count assertions. [COUNTEREXAMPLES.md](873/COUNTEREXAMPLES.md) distinguishes expected failed child runs from successful enclosing regression tests. [evidence.json](873/evidence.json) pins the source blobs, run/attempt/job, evidence limitations, and claim dispositions. [REPLAY.md](873/REPLAY.md) provides unexecuted reproduction commands and acceptance boundaries.

At the audited source, **all four aggregate behavioral claims remain `not_established`**. The existing local event-cursor subclaim is conditional on successful attributed witnesses for all three discovered feeds. The older two-feed account is historical, not current. Other useful tests have narrower scopes than their names suggest; none justifies changing an aggregate status to `passed`.

This is a finite issue-local research map, not a replacement operation inventory, qualification artifact format, evidence framework, or proposal to mirror generated contracts on `main`.

## Validation actually performed

Read #903, #873 and its decision comments, #866 only to establish the exclusion, root AGENTS/README, and the pinned producer, observer, fixtures, relevant tests, workflow selection, and Makefile. Inspected the audited commit's completed GitHub Actions CI metadata, jobs, full unit-job log, and artifact listing. The existing unit job reports **2566 passed, 1 skipped**; this is previous CI execution, not tests run by this publisher. The CI artifact listing was empty.

Locally checked this reference's JSON, internal document links, and the shell snippets' syntax. Publication verification and the pushed reference SHA's available checks are recorded in the issue comment, so they are not confused with audited-base validation.

## Validation not performed and evidence limits

No repository test, build, operation-evidence generation, provider qualification, or release workflow was run by this publisher. The local environment could not clone the repository; connected GitHub reads supplied the audit sources. The reproduction snippets were not executed against Riverhog. No current `operations.json`, timing report, JUnit artifact, or per-node verbose CI result was retrieved. Quiet unit-suite output plus source selection is supporting execution evidence, not an independently retained per-claim qualification record. The selection regression conditionally exercises the producer's clean-source path; the retrieved log does not identify which conditional branch ran.

No provider secrets, administrator credentials, publication environment, tag, `release/v1`, production implementation, normal workflow, or frozen contract is changed. **#866's separate consumer-summary/application-identity work is out of scope.** #865 atlas navigation and other issues are not reimplemented here. Historical issue comments remain historical attestations; their test/operation counts are not relabeled as current measurements.

## Publication and integration boundaries

The connected publisher exposes Git object/branch creation and issue comments, but no native Development branch-to-issue linking action. The branch must not be described as natively linked merely because its name and a comment mention #873. This limitation is also recorded in #873 for a capable publisher to complete the native relation. No substitute PR or closing keyword is used.

Only `.reference/` files are added. Do not merge this branch wholesale or treat its publication, model identity, requested creation, or base CI success as approval. The integration agent must reconcile against its exact implementation SHA, reuse existing assertions where their scope genuinely matches, run normal validation and exact-SHA evidence generation, preserve fail-closed release gates, and obtain the issue owner's acceptance through the normal integration rail.
