# Capability account, acceptance vectors and integration map

Reference input for #953, based on `802ed38225b4ecd9aacd1b7dc3de51a1b1fd1905`. Nothing in this matrix is a claim of runtime validation. The owning issue controls scope. References below are paths in the audited repository, not a hand-maintained public API inventory to land on main.

## Actual reference checks

The supplied standalone prototype generates a Draft 2020-12 source schema and performs selected static checks against **mock resource summaries**. In the producer environment, all ten example documents validated and `pytest -q test_language.py` passed **57 tests**. Python byte-compilation is an additional syntax check. Re-run from this directory using the pinned reference requirements. Published-tree verification and any GitHub check status belong in the owning issue's publication record.

Those tests cover closed grammar, local references, several port/view and role constraints, the combined observation/classification dependency graph, join/export result kinds, binding destination overlaps, obvious retirement contradictions, YAML ambiguity and selected JSON Schema reference hazards. The mocks have no verified operation/observer/interface digests. The prototype does not emit a production compiled recipe, encode JCS identity, validate real accepted evidence, execute a predicate against facts, call a service, prove restart correctness, or qualify source retirement. Its Python recursion is not evidence for an unbounded-depth production implementation. The full runtime vectors below are requirements, not tests already run.

## Current capability account

| Audited capability / old fields | Replacement or deliberately retained owner | Required witness |
|---|---|---|
| `RecipeDefinition.id`, exact `revision`, `sha256`, catalog ordering | Source ID/revision, normalized compiled payload, exact closure; author maps need not be sorted | C01–C06 |
| `RecipeCatalog.operations`, ID-only route/join operation lookup | Alias -> exact operation `{id, sha256}`; no ambient ID-only resolution in compiled recipes | C04, C07 |
| `ObserverUse.registration_id`, contract ID/hash | Distinct task ID, exact observer/interface resource, optional logical executor, separately approved deployment binding | O01–O04 |
| Observation options, `after`, `evidence_from` | Literal options, named evidence inputs, inferred dependencies and explicit ordering-only edges | O02–O07 |
| Generated partitions / evidence-slot pointers | Interface-owned named subject/evidence ports and question construction, no recipe pointer injection | O05, O08 |
| `subject_roles`, task batch size, timeouts, result byte budgets | Role-filtered ports; batches/timeouts/budgets in execution profiles, not semantic recipe identity | O04, O09–O12 |
| `ArtifactRule` first-match assignment | Ordered classification cases, explicit otherwise role/null, invariant exact member identity | P01–P04 |
| Subject-keyed `ArtifactFactBinding`, global predicates | Typed named subject/global fact views with explicit scope and coverage | P05–P09 |
| `FactPredicate` equality, inequality, contains, exists, one-of | Closed typed `test` AST; no untyped implicit coercions; `in` replaces one-of | P06–P10 |
| Nested `array_pointer` / `same_item` | Explicit nested `items` quantifier and same-row Boolean predicate | P10 |
| Ordered association sources, exact endpoints, statuses, expected options | Group preference tiers plus pinned relation interfaces over exact accepted questions; representation and completeness binding moves to interface owner | R01–R09 |
| Per-primary role selection and associated groups | Reusable named groups; candidate-scoped filtering; aggregate accepted groups in one branch call | F01–F03 |
| Whole-input routes and overlapping routes | `select: all` and all-matching fork; overlap remains legal and exact | F01, F04 |
| Exact recursive coordination routes | Typed exact recipe calls with bound member selection and explicit export capability | F05–F08 |
| Operation intent and target options | Separate `intent` / `options`, no hidden inheritance | B01–B05 |
| Work-effective-intent / evaluation projections | Explicit parameters/evaluation bindings with insert/replace/merge-object semantics | B01–B08 |
| Forward selected observer evidence | Named task forwarding, original evidence/support retained; no contract-ID-only ambiguity | O02, O13 |
| Input retrieval policy | Explicit recipe permission; execution still uses existing owning retrieval authority | L05 |
| Output collection policy (archive store, cache, copies, tags) | Collection call `output`, normalized through Riverhog's authority and approved resolved placement | B09–B11 |
| Exact named-subset join and output-role selection | One explicit collection-producing join per recipe level | J01–J07 |
| Existing child collection result inferred from join | Explicit branch/join export, including single-operation children; no heuristic | J04–J07 |
| Explicit global no-action + source-loss rule | Ordered `no_output` decision with exact per-member loss evidence; pure decision recipes valid | N01–N07 |
| Unmatched disposition, retirement mode and grace | Grouped source policy, original inventory retained, root-only exact settlement and deletion checks | S01–S09 |
| `event_input_closure` | Admission's existing single-event-root closure, NOT a global one-input recipe restriction | L01–L03 |
| Direct invocation, preview/create, evaluations, Review0 | Existing work/evaluation protocols consume compiled identity and typed parameters; no new event/loop engine | L01–L10 |
| Fencing, checkpoints, restart, retries, cancellation | Existing lifecycle owners updated for task/compiled/export/evidence-set identity | L06–L12 |
| Schema/config/API/CLI/release discovery | Regenerated from new executable authorities; no retired aliases or dual readers | I01–I08 |

This accounts for the current schema's semantics, not a promise to preserve its bugs, old names or old hashes. There are deliberate additions: independent task identity, explicit export typing, pure decision recipes, named evidence interfaces and clear binding modes. The integration agent must exercise them through the actual runtime before acceptance.

## Compiler and identity vectors

**C01** Reorder source maps and set-valued declarations, change indentation/comments/description: same normalized recipe identity. Reverse classification cases, relation tiers or decisions: changed identity when precedence differs. Literal array ordering remains significant.

**C02** Integer revision `1` and source string `"1"` normalize identically. Reject `true`, `1.0`, `"01"`, zero, negatives and `latest`. Test exact scalar boundaries using the shared authorities, including JSON booleans versus numbers and exact large integer domains.

**C03** Omitted and explicitly equivalent language defaults normalize identically. Compiler-version changes cannot reinterpret a retained compiled recipe. Adding a new optional field must be governed by the exact semantics profile, not current model defaults.

**C04** Change only the operation contract behind a source alias while preserving its semantic ID: recipe digest changes. A stale/mismatched supplied contract hash fails compilation. Two exact revisions with the same operation ID can coexist under distinct aliases without collision.

**C05** Catalog alias rename resolving to identical exact references preserves identity; unused catalog changes do not affect it. Reachable observer/interface/schema/child dependency changes do. Missing closure documents, schema reference escapes, cycles and forged digests fail before installation.

**C06** Compile -> canonical bytes -> parse -> verify -> canonical bytes is stable. Preserve local task/branch IDs. Reject source keys unknown at any closed structural boundary. Do not parse literal option keys as language directives.

**C07** Retain compiled dependency documents independently of mutable source paths/catalogs. Delete author files and change deployment aliases, then inspect existing recipe/work: exact meaning is still recoverable.

## Observation/task/evidence vectors

**O01** Two tasks use the same registration/contract with distinct options and/or logical executors. They remain separately pending, completed, forwarded and inspected. No completion by registration ID or first result.

**O02** Two tasks share a contract and subjects but have different options; a downstream consumer selecting one must not receive the other's facts. An operation forwarding both must validate their distinct questions or reject ambiguity explicitly.

**O03** Stale observer contract/interface/semantic profile or mismatched provider support fails before accepted observation. Digest match alone does not bypass schema/semantic validation.

**O04** Run the same independent-subject question with different physical batch sizes and delivery order. Logical subject coverage, accepted facts and final semantic selections/groups agree. Request/evidence hashes may legitimately differ; compare meaning and exact actual provenance, not fabricated identical envelopes.

**O05** Metadata -> classification -> role-selected stream/provenance tasks -> evidence-only filename task. All inferred edges resolve. A classification task selecting roles it is supposed to define fails with the cycle path. Unknown `after`/evidence tasks fail statically.

**O06** Empty role selection explicitly completes with empty scope where permitted. Successor ordering progresses; no empty observer request or fabricated global answer. Missing predecessor completion does not count as empty.

**O07** Restart between physical batches and between graph stages. Accepted exact batches are reused, missing batches remain pending, no duplicate/conflicting subject answers enter the set, and no later task is released early.

**O08** Generated port option pointers collide with literal options, each other or ancestor paths: reject. Wrong port kind, undeclared port, missing coverage port or modified role partitions fails validation. RFC 6901 escaped sibling keys remain distinct.

**O09** Whole-scope relation question with related members split across physical pages: relation completeness remains whole-scope; no false negative from independent page answers.

**O10** Large valid inputs exceed one page/result budget. Continue bounded progress or explicit capacity admission failure; never truncate or declare a logical size-based format error. Exercise accumulation/storage and completion accounting, not only slicing a Python list.

**O11** Change batch/time/concurrency profile after creating work. Semantic recipe/work identity remains unchanged, but an already approved execution does not silently adopt changed settings/descriptors. Reapproval or explicit existing runtime policy is used where required.

**O12** Observer inapplicable, failed, canceled, timed out, malformed and incomplete results remain distinct. None is a negative fact suitable for routing or source-loss approval.

**O13** Forward evidence from a batch containing a superset of selected members. Preserve original result identity and scope; an authorized selected view is separately identified. Verify source support and the scoped read boundary; no forged clipped result and no extra byte/provenance read capability. Reject evidence from another work/question/task even if its JSON values look identical.

## Predicate/classification/relation vectors

**P01** Two exact member instances with equal bytes stay distinct through roles/groups/selections. Same member IDs in different collection roots stay distinct. Role changes do not change immutable member identity.

**P02** First true classification case wins. Complete false falls through; indeterminate does not. `otherwise: null` leaves the original inventory member unmatched, not deleted from work accounting.

**P03** `scope: self` refers to the preclassification exact member; role filters cannot create circular self-justification. Global predicates cannot be accidentally evaluated against only the first member.

**P04** A candidate-role filter outside the group's primary/attached roles is a compile error.

**P05** Missing/partial/unaccepted evidence yields indeterminate; a malformed or forged record is an error. Neither becomes false. Verify `not`, conjunction and disjunction against the specified three-valued truth tables.

**P06** Distinguish missing field from explicit null. `exists:false` requires a complete record; `ne` on missing is not true. `true != 1`; typed numeric handling uses canonical authority.

**P07** `any`, non-vacuous `every`, and `none` over complete empty scopes/rows produce the documented results. A many-per-subject view with a member's zero records must prove that coverage separately; `every` cannot skip that member.

**P08** Global views cannot satisfy member-scoped loss or be treated as a subject index. Wrong fact/relation view kinds fail at compilation.

**P09** Boolean-looking JSON inside a comparison value or schema example remains literal data. No Python/jq/regex/expression escape hatch.

**P10** Put `name=container-format` in one nested metadata row and `value=XMP` in a different row: the conjunctive same-row test must not match. Nested array quantifiers preserve their binder at every depth.

**R01** Canonical describes relation attaches the exact sidecar before a conflicting weaker filename rule is considered.

**R02** Complete negative canonical relation permits full-leaf matching; complete negative full-leaf permits stem matching. A positive higher tier prevents weaker reinterpretation.

**R03** Unsupported, insufficient, ambiguous, partial, missing or corrupt stronger evidence prevents fallback. Unknown status is an error. Missing/duplicate status records cannot become complete.

**R04** Equal endpoints belonging to different member instances fail ambiguous resolution; equal bytes do not disambiguate them. Missing endpoint lookup evidence cannot activate filename fallback.

**R05** A relevant relation with a wrong value type or unsupported endpoint is not absence. Validate the `require` predicate separately from view selection.

**R06** Exactly one associated member mapping to several primaries is blocked; the planner exposes affected primaries. A primary with a complete negative for all attachments may still run alone.

**R07** Relation question options and primary/associated partitions must match the exact task and candidate scope; no repeated `expected_options` in a recipe that drifts from the question.

**R08** Conditions apply to one exact primary/associated group, not a bag of unrelated members. Scope filtering cannot smuggle an unattached sidecar's metadata into another primary's call.

**R09** Source-loss status complete means evidence completeness, not approval. Affirmative per-member verdicts remain separately required.

## Fork, binding, join and no-output vectors

**F01** All matching branches are selected. A whole-input branch receives all classified subjects. No branch selected and no decision matched is inapplicable, not success.

**F02** Multiple accepted input groups feed one branch call with all exact groups; do not accidentally implement one child per group/scatter.

**F03** A group failing its condition is omitted from that branch but retained in source accounting. A blocked group does not execute without uncertain sidecars.

**F04** Two branches intentionally share members, including different intents for the same operation; both are required and overlap does not double-count or invalidate original-inventory coverage.

**F05** Exact nested recipe calls preserve child identity, parent decision binding, selected member set and effective intent. No target descriptor/options are smeared onto coordination nodes.

**F06** Deep finite nesting progresses iteratively without an arbitrary protocol depth ceiling; cycles fail before execution.

**F07** Child receives only its sealed selection even when its root collections contain other members. Reconstructing child inventory from all root contents is a failure.

**F08** Parent requiring source retirement inspects no-output/permission obligations through the child closure, not only a child export Boolean.

**B01** Insert into absent object fields succeeds; insert collision fails. Replace requires a present field. Merge-object requires object source/destination and explicitly merges at one level, source keys winning.

**B02** Missing bound source is an error; explicit null is preserved. Wrong destination ancestor type, array write or invalid pointer fails.

**B03** Overlapping destination bindings fail even with escaped tokens; disjoint ones are order-independent. Whole-root replace/merge has explicit object semantics and cannot coexist with overlapping child writes.

**B04** Validate literal and final projected intent/options against exact operation/provider schemas. Do not let broad JSON dictionaries bypass schema checks, or reject a valid invocation merely because a compile-time value is provided later by a typed parameter.

**B05** Recipe calls accept only portable child intent bindings; target options/executors remain descendant-owned.

**B06** Reproduce Review0's sample plan, variant ID, portable variant intent and explicit root-options merge without accidental shallow merges elsewhere.

**B07** Parameter schema default annotations do not silently insert values. Missing required parameters fail. Parameters/evaluation bindings use the same frozen input in preview/create/restart.

**B08** Invocation outside an evaluation cannot silently fabricate evaluation fields. Explicit direct multi-root work still works without an evaluation context.

**B09** Store/cache/copy/tag output policy reaches actual collection creation, including non-default values. Primary store duplicated as a copy destination fails; unknown stores/tags obey existing authorities.

**B10** External effects cannot declare output collection placement. Output default resolution is captured in the approved plan and does not change when server defaults change.

**B11** Requested target implementation options remain exact declared choices; do not move them to mutable retry profiles or hide them from execution identity.

**J01** Exact named-subset join waits for its members' successful settlements and uses only declared output roles. Other fork branches remain required for parent completion.

**J02** One unselected/inapplicable/failed/no-output required join member never shrinks the member set. No join starts on partial evidence of successful outputs.

**J03** Effect operations cannot be join members or join calls. An operation's collection result kind is exact contract data, not target-name inference.

**J04** Single-operation child explicitly exporting its branch is a valid collection-producing child without a dummy join. Multi-output child without export cannot be consumed as an implicit collection.

**J05** Selected child no-output result makes its parent's exact consuming join inapplicable; it does not yield an empty collection.

**J06** Export names exactly one selected branch or join and its producer settlement. No flattening, implicit union, mutable latest-output pointer or export of an effect receipt.

**J07** Parent waits for every required branch, including non-join members, and optional join before terminal success. Source retirement cannot race an unfinished side effect.

**N01** Pure decision-only recipe executes no dummy target. Complete matching rule seals successful no-output before any target preflight/execution.

**N02** Ordered decisions are first-match; an earlier indeterminate rule cannot fall through into a later call.

**N03** No-output under retain leaves original sources retained. It does not prove absence of useful material or authorize deletion.

**N04** No-output under root retirement needs every required exact evidence slot to affirm every discarded member; global approval or a single positive row is insufficient.

**N05** Nested no-output is a required successful branch, separately typed and settled. Its parent can export a different branch without turning no-output into a collection.

**N06** Restart between loss approval, settlement publication, grace waiting and deletion preserves all obligations and never repeats destructive authority incorrectly.

**N07** A selected no-output decision bypasses the normal export with a no-output outcome. This is not the same as a normal plan with a missing declared export, which is inapplicable.

## Retirement and lifecycle vectors

**S01** Complete selected coverage with intentional overlap can retire only after every exact required outcome is settled and ordinary checks pass.

**S02** Unmatched, blocked or unclassified original members prevent retirement unless a separate affirmative per-member loss rule authorizes their no-successor disposition. Do not silently discard them.

**S03** A non-retirement-permitting source operation (such as the audited audio-only operation) rejects an unsafe retirement recipe/path. Result kind alone never authorizes retirement.

**S04** No-output evidence uses exact observer/interface/facts/semantic profiles and exact original member identity. Forged/missing/mismatched profiles, verdicts or scopes fail.

**S05** Grace zero and nonzero behave as declared; durable timer/restart does not allow early deletion. Retain cannot carry a retirement grace field.

**S06** Nested recipes cannot retire shared parent inputs. Their suppressed root retirement preference is visible, not silently misunderstood. Parent obligations still include descendant no-successor approvals.

**S07** External delivery requires exact verified effect receipts and applicable permissions. Child success/process exit alone is insufficient.

**S08** Riverhog's authorization, permanent-deletion eligibility and exact source-loss checks still own the final decision; compiler approval never bypasses them.

**S09** Join consumption retains branch collections. A join/export is not permission to retire original sources or a new hidden garbage-collection policy.

**L01** Direct invocation with several exact finalized roots; same opaque member ID in two roots; same bytes in distinct instances. Every scope/identity remains exact.

**L02** Admission event closure is still one finalized event root and typed policy intent, outside the recipe. Policy identity is separate from semantic work identity.

**L03** Departure effects/catalog predicates retain their independent policy owner and existing lifecycle. Do not add event selectors to recipes.

**L04** Evaluations create ordinary variant work, bind exact parameters, export one collection where required, handle partial variant success and keep canceled outputs according to existing rules.

**L05** Retrieval permission allow/available-only reaches existing costs/authority behavior in observer and operation paths. Missing available bytes is not negative content evidence.

**L06** Preview/create uses the same compiled recipe and inventory/evidence decisions. Changed recipes, operation/interface/descriptor bindings or parameters invalidate stale approval.

**L07** Fence generation changes, stale callbacks and duplicate completion never mutate an already sealed plan/evidence set/export.

**L08** Cancel/retry/restart during observation, nested planning, execution, effect receipt and retirement preserves exact state and bounded progress.

**L09** Work browsing/explain includes task identities, conditions, blocked/unmatched members, groups, exported collection, no-output evidence and retirement status without unbounded inline trees.

**L10** Checkpoint exact invocation before extension side effects. A deployment reload/provider upgrade cannot silently change queued/running work's accepted semantic parameters.

**L11** Scheduler operational fairness and paging do not become language constructs or recipe hashes. Large work remains complete rather than truncated.

**L12** Error taxonomy separates invalid definition, unresolved binding, inapplicability, indeterminate facts, extension failure, resource admission, cancellation and retirement waiting.

## Existing witnesses to re-express

Under `some-implementations/stove0/application/tests/`:

- `test_recipe_opaque_selection.py`: equal-byte member separation; complete-negative versus missing/partial relation evidence; filename tiers/statuses; nested metadata row correlation.
- `test_recipe_planning.py`: installed exact contracts; stale observer/target support; overlapping branches and joins; exact nested subrecipes; required no-output children; unmatched coverage; retirement permissions; group/evidence binding; Review0 projections; explicit retrieval/output policy; batching without omission; explicit work not consulting derivation.
- `test_staged_observation_graph.py`: role partitions and complete predecessor evidence.
- `test_preview_and_evaluation.py`: deterministic preview, bounded read cleanup, no-output preflight bypass, settlement before terminal success, grace-delay deletion, recursive preview, ordinary evaluation children, partial success/cancel/restart and forged observer outcomes.
- `test_work_state.py`, `test_coordinator.py`, `test_riverhog_adapter.py`, `test_classification_admission.py`, `test_stove0_api_parity.py`, `test_stove0_runtime_config.py`, and `test_stove0_state_schema.py`: persist/bind the new identities and remove retired configuration assumptions.

Also re-express protocol `test_fork_join.py`, observer/target support contract tests, Riverhog collection-workflow authority tests, configuration discovery/connectivity and installed qualification fixtures. Existing tests are behavioral witnesses to preserve where intended, not a reason to keep old authoring types or hashes.

## Integration map and ordering

**I01 — Authorities first.** Replace `some-implementations/stove0/packages/recipe-config/src/stove0_recipe_config/models.py` with the new source/compiler authority, not wrappers around old route models. Introduce focused observation-interface/accepted-view contracts in the appropriate protocol/support packages. Reuse shared canonical JSON, scalar, schema/profile and output-policy authorities. Do not import observer/target implementation modules into core or the compiler.

**I02 — Real interfaces and fixtures.** Publish maintained exact interfaces for metadata, streams, canonical provenance, filename sidecars, hints and sampling. Generate contract/view help and conformance vectors from those owners. Replace `qualification/fixtures/stove0/recipes.yaml` and runtime installation wiring with source + exact catalog + compiled artifacts as appropriate. Mock names/pins in this reference are not deployment defaults.

**I03 — Planner.** Replace author-model interpretation in `application/server/src/stove0_core/recipes.py`. Add the compiler boundary, task identities/completion, verified view access, group selection, explicit bindings, typed child exports and deterministic explanation. Preserve content opacity and the constrained execution algebra; do not build a generic DAG engine as a side project.

**I04 — Protocol and durable state.** Reconcile `packages/protocol/.../models.py` and `fork_join.py`, then `work_state.py`, `coordinator.py`, `coordination.py`, `preview.py`, `evaluation.py`, `riverhog.py`, callbacks and scheduler consumers. Add task/evidence-set/compiled/export identity where required, avoid digest cycles, and rebaseline current pre-v1 durable state. No legacy-row migration is requested; persistent-state ownership and exact baseline checks still apply.

**I05 — Surfaces.** Update `operator-contracts`, API app/views, API client, CLI, runtime config, admission/departure selected recipe references and Review0 planners. Expose compiled identity separately from non-semantic source presentation. Validation/compile/explain should share one implementation authority with generated schema/help, not three disagreeing validators.

**I06 — Remove old paths.** Delete retired models, aliases, source formats, JSON-pointer option injection in recipes, contract-ID-only task completion and accidental shallow projection merging. Re-express tests instead of preserving a compatibility adapter. Search independently for consumers rather than considering this list exhaustive.

**I07 — Generated closure.** Regenerate current contract/state baselines, API/config/help evidence, package dependencies, checked examples and release/installation qualification inputs. Configuration discovery must find new settings and prove connectivity under #492; do not land this reference's prose inventory as a second executable contract on main.

**I08 — Actual integration validation.** Run focused compiler/interface/planner/protocol/state tests, then repository-required `make lint`, `make unit`, `make dist-smoke`, `make build`, and required GitHub Actions on the real integrated SHA. Exercise installation/Compose and restart/scale witnesses appropriate to the changed boundaries. Provider qualification remains separately authorized; a reference branch does not authorize it. Record exact-SHA terminal evidence in the owning issue. Do not substitute these 57 standalone static checks for the integration gate.
