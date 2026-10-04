# Compiler-derived RecipeContract

External reference supplement for [#953](https://github.com/nashspence/riverhog/issues/953), under [#903](https://github.com/nashspence/riverhog/issues/903). This extends DESIGN.md and CONFORMANCE.md; it is not a separately accepted implementation or contract authority. The maintainer requested a reviewed update, explicitly treating the preceding conversation as **intent calibration**, not an authoritative field-by-field specification. The refinements below are the producer's recommendations for issue-local reconciliation and integration.

## 1. Decision and scope

Every valid `CompiledRecipe` MUST carry a compiler-derived `RecipeContract`. Authors write neither a contract nor an export-summary Boolean. There is one semantic recipe language, one compiled program, and one reproducible boundary projection of that program. `RecipeContract` is the public invocation/result signature; `ObservationInterface` remains the distinct adapter between accepted observer evidence and typed ports/views.

Keep the pre-v1 hard cut: no source `contract:` override, legacy contract reader, interface alias, compatibility checker, substitution resolver, migration or alternate runtime. Add the contract to the conceptual compiled envelope in DESIGN.md section 4, and add projection/verification to the compiler phases in section 11. All existing execution and authority constraints continue to apply.

The useful precedent is a [CWL Workflow's public inputs/outputs versus its steps](https://www.commonwl.org/v1.2/Workflow.html#Workflow), including explicit [subworkflow invocation](https://www.commonwl.org/v1.2/Workflow.html#SubworkflowFeatureRequirement). This borrows a callable-signature distinction, not CWL conformance or file-path/scatter semantics. `RecipeContract` follows the repository's `OperationContract` terminology without pretending a recipe is one leaf operation.

## 2. Exact guarantees versus conservative summaries

A contract MUST distinguish three statements:

1. **Invocation shape:** the caller supplies exact finalized roots and parameters satisfying the declared schema. This does not assert content applicability, available authority, ready storage or a successful run.
2. **Result guarantee conditional on successful normal completion:** the exported value has the stated collection shape, or there is no exported value (`completion`). A collection result is not promised for an explicit no-output decision, inapplicability, failure or cancellation.
3. **Conservative exposure/requirement summary:** declared paths may request evaluation data, materialize collections, invoke effects or request retrieval. These are upper bounds, not proof that a path is reachable, a guarantee that an effect occurs, an estimate of cost, or permission to execute it.

The initial projection profile is structural and deterministic. It examines all declared branches, join calls and observation tasks, including branches whose predicates might never hold. It does not attempt SAT solving, semantic predicate implication, arbitrary JSON Schema subsumption, or stronger whole-program optimization. Conservative false positives are documented; an omitted exposure must be sound. Future stronger analysis belongs to an explicitly specified profile, not compiler-version-dependent output.

## 3. Contract payload and ownership

Proposed closed payload, before its own `contract_sha256` is added:

```yaml
format: stove0-recipe-contract/v1
projection_profile: stove0-recipe-contract-projection/v1
invocation:
  collections:
    minimum: '1'
    maximum: null
    state: finalized
    root_scope: complete-root-inventories
    child_scope: parent-bound-exact-selection
  parameters_schema:
    type: object
    properties: {}
    additionalProperties: false
  evaluation:
    usage: unused                 # unused | path-dependent
    possible_reads: []             # canonical unique JSON Pointers
outcomes:
  normal:                         # null | completion | collection
    kind: collection
    artifacts:
      - role: example.archive/v1
        minimum: '1'
        maximum: null
  no_output_codes: []              # canonical unique local decision codes
exposure:
  may_materialize_collections: true
  may_perform_external_effects: false
  may_request_retrieval: false
source:
  unmatched: retain-in-source
  root_retirement: {mode: retain}
  parent_bound_retirement: retain
```

The exact format/profile interpretation is fixed by executable protocol authorities. A compiler build ID is provenance, not the projection profile. If exact semantic-profile documents are required by the owning protocol, bind that profile's exact document in the compiled dependency closure rather than substituting a mutable name. Production uses the repository's exact-scalar and canonical-JSON authorities; this reference projection emits **unsealed payloads only**.

### Invocation

The minimum of one finalized collection root follows audited `WorkPayload.inputs`; the maximum is unbounded. Do not invent one-root restrictions from admission events, multiply operation artifact minima into collection-root minima, or infer content/type preconditions by intersecting all conditional branch input contracts. This addition introduces no new root-cardinality authoring syntax.

Root work operates on the complete declared root inventories. A subrecipe call receives its exact parent-authorized member selection/groups, not permission to reopen all members of each root and not a caller-controlled claim to parent authority. The contract describes that distinction; invocation/selection validation still owns it. Recipe classification roles are internal. Exported artifact roles are public result properties. Output lineage from internal `derived_from_roles` is not relabeled as a public input-role requirement.

Copy the compiler-normalized parameter schema exactly. Its schema/semantic profile and offline closure remain owned by the existing compiler; this is not a second inferred parameter declaration. No defaults are inserted from JSON Schema annotations. Parameters can be structurally valid and still yield an inapplicable content decision. Per-call final parameter/intent/options checks and any semantic validators remain necessary.

`evaluation.possible_reads` is the union of evaluation JSON Pointer reads in this recipe's operation/subrecipe call bindings, join bindings and every declared child contract. Evaluation context is forwarded unchanged under the existing call model. Decode/validate pointers with RFC 6901 rules; serialize their unique set canonically. Preserve the root pointer and overlapping read paths; read overlap is legal.

An empty set means `unused`: no declared reachable-or-unproven path reads evaluation. A nonempty set means `path-dependent`, **not required at entry**. A conditional branch or an explicit no-output decision may avoid all such reads. The selected exact plan resolves which reads are actually required, validates them before the dependent effects, and never invents null/default values. Do not reject all direct invocations merely because some branch references evaluation. Requirements of unrelated branches must not be intersected. Any future `required` category requires a sound, specified must-analysis; this reference does not claim one.

### Successful results

`outcomes.normal` is null for a decision-only recipe (no ordinary fork). Without a matching explicit decision, that recipe is inapplicable, not completion-success. For a recipe with an ordinary fork and no export, normal is `{kind: completion}`. This means no returned aggregate collection, not no descendant collections or effects.

For an export, normal is `{kind: collection, artifacts: [...]}`. Derive the artifact role/cardinality boundary only from the designated producer:

- `export: {branch: x}` over an operation: project the exact `OperationContract.outputs`.
- The same export over a child recipe: copy the verified child's normal collection contract.
- `export: join`: project the exact materializing join operation's outputs.

Do not add outputs from every fork branch, multiply counts by group count (group filtering is not scatter), or propagate an operation's internal lineage role names. Minimum/maximum are guarantees of the owning producer contract for **one exported collection on normal success**, not predictions of actual counts. Unknown maxima remain null. A zero minimum stays zero: a declared role is not automatically present. Production must validate actual output counts/roles through the existing producer/settlement authority.

`no_output_codes` describes this recipe's own explicit successful no-output decisions, deduplicated and ordered. It is a conservative declared-code set, not a proof each code is attainable. Human explanations remain presentation data. Do not attach `discard-permitted`, `retained`, or deletion authority to a code. Source disposition is established by separate exact evidence/settlement.

Child no-output does **not** automatically become a parent's no-output result. A required child may settle no-output while the parent exports a different branch. Conversely, a child that selects no-output cannot satisfy a collection join/export; that consuming plan is inapplicable before dependent target execution. Parent-level no-output codes come only from parent-level decision rules. This preserves DESIGN.md section 8 rather than inventing implicit exception/return propagation.

Failure, cancellation, inapplicability, retries, leases and fencing remain universal Work lifecycle behavior, not per-recipe successful-result variants. A syntactic normal collection alternative is not proof it will ever be selected or eventually succeed.

### Exposure and source policy

`may_materialize_collections` includes every declared collection operation and join, including **unexported** outputs, plus child exposures. `may_perform_external_effects` includes declared effect operations and child exposures. `may_request_retrieval` includes `retrieve: allow` on observations and operations plus child exposures. Requests can still be denied by authority or operational policy. A workflow with no exported value is not necessarily pure; a no-output decision can still have required observations/retrieval.

Exposure also covers attempts that later fail or are canceled; no successful-result type promises atomic effects or rollback. These coarse fields do not identify approved destinations, output placement, exact operation semantics, cost, or credentials. Those stay in the compiled program and exact approved plan. The contract is not an authorization or effect manifest. Unknown/unmodeled effect kinds must fail projection or require a new specified summary rule; they must not default to false.

Copy this recipe's normalized `source.unmatched` and root retirement preference/grace. A retain preference describes this workflow's policy, not a global retention warranty against other authorized actors. Parent-bound retirement is always `retain`. Do not union descendants' root-retirement preferences into the parent's policy: a child that could request retirement when run as a root cannot retire its parent's shared inputs. Conversely, a child's `retain` preference is not evidence that its outputs settle its input obligations.

A root retirement preference is only eligibility to request the existing checks. Per-member complete settlement, exact operation permission, applicable affirmative source-loss proof, grace and ordinary Riverhog deletion authority remain mandatory. No aggregate `retirement_permitted` Boolean is sufficient. Join operations do not grant permission to discard the original source merely because they consume derived branch outputs. Equal signatures, success, exports and no-output codes authorize nothing.

## 4. Content identity without a hash cycle

Recommended sealing order:

```text
resolve and validate exact dependencies, bottom-up
normalize compiled semantic body B
C = project_boundary(B, verified dependency documents/contracts)
contract_sha256 = repository_JCS_SHA256(C)
compiled_payload = B + {contract: C + contract_sha256}
recipe_sha256 = repository_JCS_SHA256(compiled_payload)
```

Neither digest includes its own digest field. `C` contains no recipe ID, recipe revision, compiled SHA, internal branch/task/resource/provider IDs, implementation closure, compiler build ID, annotations or source map. The format/profile are domain separation for the boundary semantics. Excluding recipe ID/revision is deliberate: a new revision with the same public boundary can retain the same contract identity. Parameters' normalized schemas and public output role IDs remain included.

The enclosing recipe identity `{id, revision, sha256}` associates the signature with one exact implementation. A read/API view returns that identity together with the contract. There is no reverse recipe-SHA reference inside the contract, hence no digest cycle. A contract hash identifies the same summarized boundary, **not behavioral equivalence, compatibility, approval, safe substitution, or a runnable recipe**. Calls continue to pin the full compiled recipe identity; changing a child's implementation changes its caller's compiled dependency commitment even when their contracts are equal.

At load/install, first verify exact documents/closure and their identities, then recompute and compare the contract with the body before accepting the compiled envelope. Merely checking the supplied contract's self-hash is insufficient: an attacker could rehash a false signature. Canonical comparisons must not equate Boolean true with numeric 1. The loader's closure verification cannot trust a catalog's `exports_collection` assertion in place of the child program and derived contract. This is a cached projection with verification, not another hand-maintained authority.

Reuse [RFC 8785 JCS](https://www.rfc-editor.org/rfc/rfc8785.html) through the repository's existing implementation and exact scalar rules. Do not seal production identities with Python sorted JSON. Tests here check structural payload equality, not JCS hash vectors, authentication or complete numeric normalization.

## 5. Composition and runtime validation

Once the child has been independently verified, the parent's **boundary/type checks** consume its contract rather than opening its graph. Execution planning, effect approval and retirement proof still consume the exact program and its necessary evidence closure. A boundary signature does not eliminate those responsibilities.

At compilation, reject clearly invalid literal argument shapes, incompatible supplied port/context types, missing collection exports, undeclared output roles, effect/completion/decision-only join members and malformed identities. Use supported exact schema/type rules; do not pretend general JSON Schema implication is solved. A role with lower bound zero or data-dependent cardinality may require a plan/preflight obligation rather than unconditional compile-time approval or blanket rejection. An impossible role is a static error; actual selected roles and counts remain checked.

If a child can normally export a collection and also has explicit no-output alternatives, it is statically collection-capable, not collection-guaranteed for every successful invocation. The parent compiler preserves a dynamic collection-result obligation for a consuming join/export. Planning discharges it against the exact selected child plan before dependent work. No silent member omission, empty collection fabrication or fallback export.

The contract is independent of selected providers, batching and target availability. Actual preview/create approval binds the exact program, provider descriptors, chosen branches, evaluation reads, groups, effects and source obligations. Restart validates the same approved meaning. Do not approve effects merely from the conservative summary or accept a new implementation because its contract digest happens to match.

Expose identity plus derived contract in lightweight recipe list/show/discovery views. Expose the complete compiled program separately where already appropriate. Keep descriptions/documentation outside the hashed contract. Endpoint paths are generated from the actual API/CLI implementation; this reference does not create a handwritten route inventory. Evolve generated views/clients, configuration discovery and examples through #953's existing integration scope.

## 6. Prototype boundary and acceptance vectors

`contract_projection.py` provides a closed payload-schema generator and deterministic **unsealed structural projection** from a deliberately small boundary-input slice and mock dependency summaries. It checks types/cardinalities, export/join capability, transitive exposure/evaluation reads and supplied-versus-derived payload mismatch. It is not the source compiler, exact dependency resolver, authority verifier, producer-output validator, or JCS implementation. The test adapter creates slices from the ten existing authoring examples; that adapter is not a production compiler or second supported representation.

`test_contract_projection.py` exercises the projection and its schema, all ten example slices, conditional/no-output behavior, nested child summaries, zero/unbounded cardinalities, internal-versus-public changes, transitive effect exposure, scoped retirement, malformed outputs, forged summaries and Boolean-versus-number mismatch. Existing source tests additionally prove generated `contract`/`interface` fields are not author input. The issue publication record lists commands and actual results.

Additional issue-local integration requirements (these extend CONFORMANCE.md; execution status must be reported separately):

- RC-01: Every valid compiled recipe has exactly one derived, sealed, recomputation-verified contract; source overrides and unknown projection profiles fail.
- RC-02: Full-program/dependency hashes and contract hashes are golden-tested with the real canonical encoder; renamed aliases, comments and physical batching preserve the appropriate identities, while actual boundary changes do not.
- RC-03: Changing recipe ID/revision or internal graph/implementation with the same projected boundary preserves the contract but not full identity; public schema/output/context/exposure/source changes change the contract. Never use this as substitution authority.
- RC-04: Operation, child-export and join-output projection preserve exact output lower/upper bounds and role semantics; no branch union, scatter multiplier or fabricated lineage.
- RC-05: Decision-only, completion-only, conditional export and local/nested no-output each have their own positive/negative witnesses. An unused no-output child does not pollute parent result variants.
- RC-06: Conditional evaluation reads propagate transitively without making every invocation require evaluation. Missing selected-path reads fail before dependent effects; bypass paths still work. Parameter schemas are not unions/intersections guessed from internal calls.
- RC-07: Non-exported operations, joins and nested calls contribute exposure; omitted fields cannot hide effects. Coarse summaries never replace exact effect/destination/retrieval approval.
- RC-08: Root/child retirement projection and source-loss obligations preserve per-member authority. Rehashed false summaries, simple success codes, same-contract substitution, child-root policy leakage and false aggregate retirement permission fail closed.
- RC-09: Child contract verification is tied to the exact recipe pin; general schema entailment is not assumed. Deferred cardinality/collection-result obligations are resolved by exact planning/preflight/settlement rather than silently accepted.
- RC-10: Discovery/client/API/CLI/schema/configuration surfaces derive from the executable authority. All old manually supplied export/result flags and duplicate signature sources are removed in production.
- RC-11: Validate reference and production checks separately. Real compiler integration, Unicode/numeric JCS vectors, complete dependency verification, actual execution/restart/scale/retirement and Actions must run on the integration SHA. No migration or compatibility layer is introduced.

The prior nine-file handoff remains in Git history. This supplemental revision supersedes its absence of a RecipeContract, not its safety constraints or the owning issue's acceptance authority.
