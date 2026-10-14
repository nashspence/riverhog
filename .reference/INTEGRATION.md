# Ordered integration sequence

Owning issue #965 controls. This branch is not an implementation branch. Product paths match the audited main exactly; the proposed patch is test-only. Reconcile the reference and then-current main rather than importing `.reference/` into the product.

## Working protocol

Start with the issue and P00. At each checkpoint record the actual source/ref, changes, newly demonstrated behavior, scope/limitations of evidence, remaining specific findings and next phase in the issue. Keep work tracking out of main's source/tests/guide. A first vertical slice does not finish the issue. No routine phase requires another general design exercise or maintainer confirmation; raise only a concrete conflict with current authority, protected contracts or a genuinely unresolved owner decision.

The source changes should form reviewable, coherent commits through the current normal rail. Existing required lint, unit, distribution, build and applicable qualification gates remain required. Proposed command/test names here are navigation for the current source, not an alternative runner or a hard dependency graph.

## P00 — Reconcile and prove a first real omission

1. Record current main SHA/tree and inspect drift from `6e8f93b33ea09439ad7c1e94f1907e2aea3a0f43`. Read the current issue decisions and the existing workspace/release/HTTP policy tests.
2. Reproduce the scoped inventory using maintained discovery. The reference reader can corroborate the audited snapshot without importing product modules: `python .reference/audit_inventory.py --root . --output /tmp/stove0-source-observations.json`. It is not a production replacement.
3. Review `role_http_parity.patch`. It expands the actual HTTP fixture to all five observers and adds a magic-readiness runtime/OpenAPI mismatch test. The patch was syntax/applicability checked only, not executed. Adapt rather than apply mechanically if current tests changed.
4. Establish the failing readiness regression through the real app, correct the owning response declaration/behavior, and retain a discovered-versus-exercised coverage assertion so later implementations cannot be omitted. Do not mock away the HTTP binding or declare the entire parity problem solved.

Exit: exact source, accurate role inventory, one demonstrated/corrected runtime parity defect, and a clear finite remaining matrix. A specific conflict is reported with source/contract evidence; absent such a conflict continue to P01.

## P01 — Shared process configuration and startup ownership

Implement C05–C08 and the selected bootstrap table. Keep structured YAML with its current semantic owner; simple execution settings are one reusable definition bound by the actual component prefix. Connect every applicable parser/configuration through the real constructor and behavior. Converge `_STATE_ROOT`, per-component admission/capacity/retention and tool binding names; remove direct executable secret-value input and old aliases as one pre-v1 cut. Preserve programmatic credential injection.

Construct validated configuration first; then open secret files/build identity and required paths; then allocate the owned runtime under exception-safe cleanup. Help/version terminate before this path. Tests must challenge invalid/empty files, a missing native tool, invalid image identity, conflicting/retired input, and a server-start failure after runtime allocation. Confirm no leaked dispatcher, client, state lock or resource lock. Do not add daemon controls to pure schema/planning tools.

Exit: actual installed entrypoint tests and owner-level setting behavior, not just a schema diff. Coordinate with configuration connectivity work without constructing a second settings inventory.

## P02 — Transport, health, outcomes and large metadata

Use the existing HTTP model/operation/metadata machinery in every actual app and client. Replace Review0's eager body read with the bounded intake/staging path. Include all advertised metadata routes and actual error/status pairs. Separate control timeout from native execution timeout; share enclosing budgets across nested calls. Health must remain responsive during contention and cannot perform encoding or external effects.

Exercise chunked oversized transport, a valid logically large staged document, invalid schema/identity, unauthorized methods, unsupported methods/selectors, invalid remote errors and safe descriptor inspection. Preserve current owning result enums. Update clients, CLI/readouts, OpenAPI and schema/conformance tools together. Do not claim payload behavior from descriptor-only probes.

Exit: complete per-role transport coverage, bounded body/control paths and exact presentation/consumer agreement.

## P03 — Execution-owner parity before the sampler protocol cut

Connect the existing generic admission/dispatcher seam to every supplied observer and native transform/effect target through the real executable. Exercise optional immediate mode and configured mode. Verify that evidence-only observers remain evidence-only. Preserve the provenance observer's justified concurrency default while allowing its actual operational control.

Generalize reusable behavior fixtures around exact component adapters, not hard-coded private field names or one universal fake executor. Keep real component entrypoints, acceptance and payload-boundary callbacks; use safe fixtures only for native encoding/provider calls. Challenge busy admission, unrelated work, cancellation-before-grant, exact owner/attempt fencing, slot limits, queued restart, shutdown and failed startup. Retain the existing real process-group/lease tests as a separate scope.

Exit: all existing persistent execution roles have working conventional admission and lifecycle wiring. This is not sufficient to mark samplers or Review0 complete.

## P04 — One sampler/delegation hard cut

Change the sampler contract/client/support, both sampler executables, Review0 caller, conformance/schema tools and deployment state together. Selected architecture:

- A sampler owns bounded accept/refresh, status/result and cancel controls around a durable nonsecret job, preserving its exact descriptor and terminal sampling result. No old synchronous HTTP route or second runtime remains.
- Distinguish semantic request, local job/caller authority, attempt and ephemeral workspace assignment. Use current renewable caller ownership so lost Review0 processes do not leave indefinite native writers. No Riverhog claims, production callbacks or archive credentials cross into samplers.
- Shared metadata transport carries large exact requests/results without a new logical-total cap. Validate current assigned inputs after admission and before native consumption; protect against stale paths and changed bytes.
- Review0 persists the selected child binding and yields its own payload slot while waiting. Claim renewal, child-owner refresh, polling and cancellation are bounded independently due work. Do not leave an apparently asynchronous API wrapping a synchronous Review0 worker wait.
- Resume final collection publication only after validating exact child completion and checkpoint. Do not release/reuse shared workspace until the child is terminal/contained. Parent death, sampler death and transport ambiguity are separate tests.
- Review0 and child never acquire the same exclusive resource in a dependency cycle. Optional admission of Review0's local phases does not confer ownership of child scheduling.

Ordinary type/route spelling and the narrow resumable-runtime implementation are integration choices. Do not introduce a global workflow engine, resource ontology, broker or mandatory daemon for pure helpers. A generic runtime continuation must retain the existing target completion and external-effect protections; it is not permission to retry unknown effects.

Exit: two samplers and Review0 pass the complete queued/executing/canceling/restart matrix, including independent-review progress and stale caller/workspace rejection.

## P05 — Role semantics, progress, retention and identities

Recheck the five observers and seven semantic providers; direct/media transforms and their paired sampler intents; Review0 collection outcomes; Rclone verified receipts; artifact-free departure support; pure planner/compiler; all caller/tool surfaces. Use existing owning contracts/vectors as expectations, not equality of two implementations' outputs alone.

Test completed-evidence precedence, interrupted versus replayable work, role-correct empty/inapplicable outcomes, source edges/dispositions, exact destinations and publication. Test large exact values and distinguish semantic configuration from execution topology. Measure meaningful progress, complete terminal-record cleanup and retention of unresolved effects/protected completion state. Recovery traits remain different where the result kind requires them.

Exit: no unexamined role/convention cells, no arbitrary exemption for a historical implementation shape, and no forced false uniformity.

## P06 — Built deployment and original host-resource witness

Update current images, entrypoints, configuration examples, state declarations and Compose/host overlay. Give sampler operational jobs an owned durable state mount; keep shared payload scratch ephemeral and free of Riverhog authority. Preserve least privilege, network isolation, secret groups and same-image/different-role identity rules.

Run the actual maintained NVENC target and NVENC sampler with an independent host lock participant. Compare actual open descriptor device/inode. While held: no payload consumers, no resource use, responsive controls, no late launch after cancellation, independent work progress. After release: correct guarded native boundary and host reacquisition after containment. Include parent hard death and complete owned fixture cleanup. Qualify this portable boundary without claiming a real GPU encode; run hardware/native/provider qualifications only under their existing applicable authorization and evidence scope.

Exit: the original #965 defect is reproduced and prevented by built artifacts plus the shared conventional deployment.

## P07 — Close the whole issue, not the reference exercise

Reconcile discovered subjects, actual tests, result-purpose classifications and all A01–A09 acceptance items. Preserve the current evidence-accounting and qualification consumers; new policy tests remain required without automatically becoming product proof. Run complete applicable local gates and independent exact-pushed-source checks. Record final source, changed contracts/configuration, genuine role distinctions, executed evidence and remaining external qualification boundaries.

The complete selected Stove0 parity scope must pass. No closing on the reference scanner, a family prototype, a parser inventory or one image witness. No indefinite expansion into whole-product/provider qualification. After the finite issue acceptance is met, record how the reference was consumed and stop maintaining this branch as an alternate implementation.
