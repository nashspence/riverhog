# Issue updates for the integration agent

Owning transition: https://github.com/nashspence/riverhog/issues/1006

**The maintainer abandons the entire #994 program, including #995–#1000.** These are proposed later-decision comments and graph actions for integration. They are not claims that the reference publisher has already closed those issues. Preserve historical bodies/comments and the closed old hierarchy. [ISSUE_TRANSITION.md](ISSUE_TRANSITION.md) supplies evidence and transfer criteria.

Transfer independently required product outcomes and actual unresolved relevant defects. Retire mechanism-specific tasks, prescribed architecture and obsolete acceptance arms. Do not preserve the old program by copying its whole checklist into renamed successors.

## #991: resume under fresh operation ownership

> The maintainer-requested transition in #1006 retires all of #994, including #995–#1000. This issue resumes as the repository-wide production performance campaign with newly scoped operation outcomes and independently approved all#64 budgets. #994's precedence and inherited implementation plan are superseded. Continue from verified main; use #1001 only for explicitly justified salvage. Preserve required semantics, custody, growth and recovery, and advisory suite-runtime observations. Start with complete real publication at supported p1/p2/p4 and cold counterparts, then the finite material-operation inventory. Previously superseded no-fusion/no-batching restrictions remain superseded.

Remove the actual #994 blocker after re-reading both ends. The [GitHub dependency API](https://docs.github.com/en/rest/issues/issue-dependencies) uses the blocker's **database issue ID**, not its number:

```sh
gh api repos/nashspence/riverhog/issues/991/dependencies/blocked_by
gh api repos/nashspence/riverhog/issues/994/dependencies/blocking
gh api --method DELETE repos/nashspence/riverhog/issues/991/dependencies/blocked_by/6425185371
gh api repos/nashspence/riverhog/issues/991/dependencies/blocked_by
gh api repos/nashspence/riverhog/issues/994/dependencies/blocking
```

Confirm only the intended pair was removed. A comment does not remove a native edge. If permissions/tooling cannot do it, report the unsatisfied action rather than claiming completion.

## #994 and #995–#1000: retire the whole program

Post to #994:

> The maintainer has abandoned this entire program in favor of #1006's policy/adoption transition and the continued #991 performance campaign. #995–#1000 are superseded work packages, not active implementation prerequisites. The universal controller, replay target, checkpoint tax/retention architecture and old/restart/adaptive acceptance are retired. Independently required product outcomes and actual unresolved defects have these destinations: [named scoped owners]. Mechanism-only tasks are explicitly discarded: [bounded disposition]. #1005 retains independent ownership where its worker-exit concern applies. Close this program `not_planned`; this is not a product qualification claim.

Post an issue-specific version to each of #995–#1000:

> #1006 retires this work package with the whole #994 program. Its inherited architecture and implementation checklist will not continue in place. Independently justified surviving outcomes/evidence: [exact requirement and active destination]. Superseded mechanism-specific tasks: [explicit retirement]. New work under #991 or a scoped successor is defined by current product contracts and approved performance requirements. This issue closes `not_planned` as superseded; transfer does not require completing the abandoned work.

Preserve #994's closed parent relationship to #995–#1000 as historical organization. Each issue needs an explicit disposition before closure, but it does **not** need a successor for every old checkbox. #996–#999 must not remain as the active implementation plan.

## #1005: retain independent defect ownership, reconcile exact edges

Keep the recorded worker-exit/receiving defect accountable independently. Reconcile current source and applicability; neither abandoned accounting nor a closed #996 proves it fixed. Do not make this a global #991 blocker.

Audited relationships: #1005 (database issue ID `6558884984`) is parented by #994 and blocks #996. Inspect current state before mutation. [Official sub-issue API](https://docs.github.com/en/rest/issues/sub-issues) distinguishes singular removal from plural addition.

If #1005 remains standalone, detach it from the old program:

```sh
gh api repos/nashspence/riverhog/issues/1005/parent
gh api --method DELETE repos/nashspence/riverhog/issues/994/sub_issue -F sub_issue_id=6558884984
gh api repos/nashspence/riverhog/issues/994/sub_issues
gh api repos/nashspence/riverhog/issues/1005/parent
```

The parent read should now show no parent (including the API's documented no-parent response). If a genuinely appropriate active parent is selected instead, use `POST /repos/nashspence/riverhog/issues/NEW_PARENT/sub_issues` with JSON `{"sub_issue_id":6558884984,"replace_parent":true}`, after verifying that the existing parent is still #994. Read back old/new parent lists and #1005's parent. Do not overwrite an unrelated concurrent reparent.

If a successor actually depends on the unresolved boundary, first create and verify its blocker using `POST /repos/nashspence/riverhog/issues/SUCCESSOR/dependencies/blocked_by` with JSON `{"issue_id":6558884984}`. Then remove the obsolete blocker on retired #996:

```sh
gh api repos/nashspence/riverhog/issues/996/dependencies/blocked_by
gh api repos/nashspence/riverhog/issues/1005/dependencies/blocking
gh api --method DELETE repos/nashspence/riverhog/issues/996/dependencies/blocked_by/6558884984
gh api repos/nashspence/riverhog/issues/996/dependencies/blocked_by
gh api repos/nashspence/riverhog/issues/1005/dependencies/blocking
```

Verify the successor's reciprocal edge when one is needed. If the new implementation avoids the boundary with demonstrated equivalent safety, record that disposition and remove the obsolete edge without inventing a replacement blocker.

If no affected successor has yet been selected, remove the obsolete #1005 → #996 edge when retiring #996 and keep #1005 open with explicit standalone ownership. Any future slice using the unresolved boundary must acquire a scoped blocker before acceptance. Retirement does not require implementing or proving a replacement boundary.

## PR #1001: preserve, dispose, close

> #1006 retires the entire #994 program. Preserve this exact head/branch and evidence as a checkpoint. The bounded salvage disposition is [independently useful changes with named destinations], [evidence retained], and [architecture/code/tests retired]. A valid disposition may adopt no code. Close this superseded PR without merging the abandoned program or deleting its preserved branch. Any selected future changes must satisfy their newly scoped requirements and exact-source qualification.

Current main lacks this PR's unmerged controller/config/schema/state additions. Abandonment does not require importing or migrating them. Only selected salvage that affects genuine configuration/durable state needs coordinated schema/model/migration/test treatment. The new campaign does not wait for full old-WIP qualification or for salvage implementation to finish.

## #988/#989 and #1002–#1004: evidence-based disposition

Reconcile each recorded failure against the selected continuation source. Keep real current obligations and unexplained relevant evidence with explicit owners. A source-backed finding that a failure belongs exclusively to discarded unmerged code may justify its retirement; a passing retry or a changed issue state does not. Do not automatically port the abandoned implementation solely to reproduce its old investigation.

## #963: supersede the production timing ban

> #1006 supersedes this issue's categorical ban on timing acceptance only for independently approved, fully bound all#64 production-operation budgets. Ordinary validation/test/CI-duration targets and matched profiling comparisons remain advisory; their valid misses do not fail validation or require approval. Accurate automatic evidence and existing correctness/growth requirements remain. This later decision preserves the completed history.

## #964 and #469: keep remaining qualification accountable

On #964:

> The closed historical issue and latest remaining-scope statement do not establish whole-path/downstream acceptance. #1006 retires #994, while [linked follow-up] owns the independently required remaining G10/G12/G13 and large-collection/growth/recovery work, with current applicable #988/#989 evidence. The #979 checkpoint and small timing samples do not complete that proof. Prior history is preserved.

On #469:

> #1006 retires the #994 program, adopts the shared performance contract and redirects #991. Consume [exact accepted operation report/source/profile/budget], and retain [named scale/platform/backend/provider/independent recovery obligations] under existing or newly scoped owners. #469 retains aggregate release acceptance. A policy update, reference publication or small prepared-storage pass does not grant release authorization or unproved qualification.

## Finite handoff disposition

After evaluating this packet, post a Proffer `disposition` in #1006 against the actual current published handoff commit. Use the pinned schema and report reused/adapted/partial/deferred/superseded/rejected honestly. Include actual integration source and checks only when they exist. Deferred useful work needs a scoped next trigger and preserved location.

This reference is not native Development linkage. Do not create a dummy PR or merge the reference branch.

