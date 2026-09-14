# stove0_core.Stove0WorkService

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0workservice:e6457b3716 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-07024b6917"></a>
- <a id="s-8386afdc42"></a>`distribution`: `stove0-server`
- <a id="s-21cbf7115b"></a>`module`: `stove0_core`
- <a id="s-def8a1d6a7"></a>`name`: `Stove0WorkService`
- <a id="s-db8d6796ee"></a>`unit`: `export`

### Declared structure

- <a id="s-a114f12d76"></a>`kind`: `"class"`
- <a id="s-2c327c1bfd"></a>`signature`: `"\"(store: 'WorkStore') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [activate_preplanned_coordination](stove0-core-stove0workservice-activate-preplanned-coordination.md)
- [activate_preplanned](stove0-core-stove0workservice-activate-preplanned.md)
- [admit_branch_set](stove0-core-stove0workservice-admit-branch-set.md)
- [admit_join](stove0-core-stove0workservice-admit-join.md)
- [begin_observations](stove0-core-stove0workservice-begin-observations.md)
- [begin_planning](stove0-core-stove0workservice-begin-planning.md)
- [begin_retirement](stove0-core-stove0workservice-begin-retirement.md)
- [bind_claim](stove0-core-stove0workservice-bind-claim.md)
- [bind_target_request](stove0-core-stove0workservice-bind-target-request.md)
- [cancel](stove0-core-stove0workservice-cancel.md)
- [complete_abandon](stove0-core-stove0workservice-complete-abandon.md)
- [create_or_resume](stove0-core-stove0workservice-create-or-resume.md)
- [fail](stove0-core-stove0workservice-fail.md)
- [mark_inapplicable](stove0-core-stove0workservice-mark-inapplicable.md)
- [rebind_claim](stove0-core-stove0workservice-rebind-claim.md)
- [record_coordination_settlement](stove0-core-stove0workservice-record-coordination-settlement.md)
- [record_observation](stove0-core-stove0workservice-record-observation.md)
- [record_retired](stove0-core-stove0workservice-record-retired.md)
- [record_target_status](stove0-core-stove0workservice-record-target-status.md)
- [request_coordination_cancel](stove0-core-stove0workservice-request-coordination-cancel.md)
- [retry_coordination](stove0-core-stove0workservice-retry-coordination.md)
- [retry_failed](stove0-core-stove0workservice-retry-failed.md)
- [seal_target_plan](stove0-core-stove0workservice-seal-target-plan.md)
- [seal_workflow_plan](stove0-core-stove0workservice-seal-workflow-plan.md)
- [verify_output](stove0-core-stove0workservice-verify-output.md)

## Governing policies

- <a id="pa-1312e7fb37"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.Stove0WorkService`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5dbaa2e5f097f5b193c7782b3c934a37daafd48312f13661581559b9c5526952 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(store: 'WorkStore') -> 'None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "Stove0WorkService",
  "unit": "export"
}
```
