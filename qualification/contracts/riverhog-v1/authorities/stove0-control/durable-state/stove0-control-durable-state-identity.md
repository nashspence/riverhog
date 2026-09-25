# stove0-control durable-state identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:stove0-control:stove0-control-durable-state-identity:1e7f50f47f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-control](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

| Authority fact | Value |
|---|---|
| <a id="s-6ccc939d49"></a>`distribution` | `"stove0-server"` |
| <a id="s-b41f5a4710"></a>`format` | `"state-schema/postgresql"` |
| <a id="s-20befdd158"></a>`head` | `"v1_0001"` |
| <a id="s-7f0d8ec979"></a>`id` | `"stove0-control"` |
| <a id="s-659eef24d8"></a>`dialect` | `"postgresql"` |
| <a id="s-89bd43a58e"></a>`kind` | `"relational-schema"` |
| <a id="s-dd040d58be"></a>`transition` | `"forward-migration-chain"` |

## Maintained corroboration

### Related interface records

- [ix_stove0_selection_members_continuation](stove0-control-ix-stove0-selection-members-continuation.md)
- [ix_stove0_selection_members_order](stove0-control-ix-stove0-selection-members-order.md)
- [stove0_admission_candidates](stove0-control-stove0-admission-candidates.md)
- [stove0_admission_matches](stove0-control-stove0-admission-matches.md)
- [stove0_admission_observed_revisions](stove0-control-stove0-admission-observed-revisions.md)
- [stove0_admission_policies](stove0-control-stove0-admission-policies.md)
- [stove0_artifact_selection_members](stove0-control-stove0-artifact-selection-members.md)
- [stove0_artifact_selections](stove0-control-stove0-artifact-selections.md)
- [stove0_departure_effects](stove0-control-stove0-departure-effects.md)
- [stove0_departure_policies](stove0-control-stove0-departure-policies.md)
- [stove0_departure_seen](stove0-control-stove0-departure-seen.md)
- [stove0_evaluation_children](stove0-control-stove0-evaluation-children.md)
- [stove0_evaluation_records](stove0-control-stove0-evaluation-records.md)
- [stove0_event_cursors](stove0-control-stove0-event-cursors.md)
- [stove0_lifecycle_events](stove0-control-stove0-lifecycle-events.md)
- [stove0_target_input_dispositions](stove0-control-stove0-target-input-dispositions.md)
- [stove0_target_outputs](stove0-control-stove0-target-outputs.md)
- [stove0_target_production_seals](stove0-control-stove0-target-production-seals.md)
- [stove0_target_settlement_seals](stove0-control-stove0-target-settlement-seals.md)
- [stove0_target_source_edges](stove0-control-stove0-target-source-edges.md)
- [stove0_work_evaluations](stove0-control-stove0-work-evaluations.md)
- [stove0_work_records](stove0-control-stove0-work-records.md)
- [stove0_work_relations](stove0-control-stove0-work-relations.md)
- [stove0_work_selection_references](stove0-control-stove0-work-selection-references.md)
- [uq_stove0_target_outputs_path](stove0-control-uq-stove0-target-outputs-path.md)

## Governing policies

- <a id="pa-3ef3159cfc"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:stove0-control](../../../evidence/sources/authorities.md#src-45e44b17fd) — [some-implementations/stove0/application/server/src/stove0\_core/state\_migrations/v1\_ddl.py::POSTGRESQL\_DDL](../../../../../../some-implementations/stove0/application/server/src/stove0_core/state_migrations/v1_ddl.py)

### Machine authority

- `/external_contract/durable_state/owners/5/distribution`
- `/external_contract/durable_state/owners/5/format`
- `/external_contract/durable_state/owners/5/head`
- `/external_contract/durable_state/owners/5/id`
- `/external_contract/durable_state/owners/5/structure/dialect`
- `/external_contract/durable_state/owners/5/structure/kind`
- `/external_contract/durable_state/owners/5/transition`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/durable_state/owners/5/distribution`

<!-- exact-contract-value: 8d975b7503c749fb11fdc973a2bb3d1063a032fa5498cadc746fbf109812d14c -->

```json
"stove0-server"
```

### `/external_contract/durable_state/owners/5/format`

<!-- exact-contract-value: 70741f907ba2309f4ce028b5a8db364b4c8df28634f15775f55dec8fbe1a8dce -->

```json
"state-schema/postgresql"
```

### `/external_contract/durable_state/owners/5/head`

<!-- exact-contract-value: 60903e5b6c89d52dd0ad1ac39a0226db7dccee2c2021bad5d97dce8e5e911f51 -->

```json
"v1_0001"
```

### `/external_contract/durable_state/owners/5/id`

<!-- exact-contract-value: 7913c2266cf044905994482a5e07cdf49cf3ad0fdb9b65df7d457ec4419bf184 -->

```json
"stove0-control"
```

### `/external_contract/durable_state/owners/5/structure/dialect`

<!-- exact-contract-value: 5f079352cb473c849b19a27938b2d33074d3318ca7e225ff4f9e8831cbc49fa6 -->

```json
"postgresql"
```

### `/external_contract/durable_state/owners/5/structure/kind`

<!-- exact-contract-value: 5ceb771e7cd4b2ae80216231febde0377bb5911894b739ab0612fe91ebf1e3f8 -->

```json
"relational-schema"
```

### `/external_contract/durable_state/owners/5/transition`

<!-- exact-contract-value: 4618321e15eb5529da1597f7e49b4ca6152dc599ec31740df4d4cd2b5f1d68d3 -->

```json
"forward-migration-chain"
```

</details>
