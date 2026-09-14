# stove0_core.PlanningPort.workflow_plan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-planningport-workflow-plan:010e2b85f3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6c0d81f80f"></a>
- <a id="s-12cecf9b9e"></a>`distribution`: `stove0-server`
- <a id="s-19b2d01852"></a>`module`: `stove0_core`
- <a id="s-efbdfc5ac2"></a>`name`: `workflow_plan`
- <a id="s-0188fc2b9f"></a>`owner`: `stove0_core.PlanningPort`
- <a id="s-6eb1a6f053"></a>`unit`: `member`

### Declared structure

- <a id="s-661d0ce4d1"></a>`kind`: `"method"`
- <a id="s-3bf27ac641"></a>`signature`: `"\"(self, work: 'WorkIdentity', observations: 'tuple[ObservationEvidence, ...]', *, nested_observer: 'Callable[[WorkIdentity], tuple[ObservationEvidence, ...]] \| None' = None) -> 'BranchSetDecision \| WorkInapplicable'\""`

## Maintained corroboration

### Related interface records

- [PlanningPort](stove0-core-planningport.md)

## Governing policies

- <a id="pa-020192f264"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.PlanningPort.workflow_plan`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bb0636935768a4147e83d95736b284bcc6ff9ebe3cd931f609c3295ffa793960 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work: 'WorkIdentity', observations: 'tuple[ObservationEvidence, ...]', *, nested_observer: 'Callable[[WorkIdentity], tuple[ObservationEvidence, ...]] | None' = None) -> 'BranchSetDecision | WorkInapplicable'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "workflow_plan",
  "owner": "stove0_core.PlanningPort",
  "unit": "member"
}
```
