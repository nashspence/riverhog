# stove0_core.Stove0WorkService.seal_workflow_plan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0workservice-seal-workflow-plan:360b0703bd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-56a54a40ec"></a>
- <a id="s-70e395bbae"></a>`distribution`: `stove0-server`
- <a id="s-af4ee8776e"></a>`module`: `stove0_core`
- <a id="s-8d960cfffb"></a>`name`: `seal_workflow_plan`
- <a id="s-19a67687eb"></a>`owner`: `stove0_core.Stove0WorkService`
- <a id="s-bb340a3aa8"></a>`unit`: `member`

### Declared structure

- <a id="s-6223479d50"></a>`kind`: `"method"`
- <a id="s-34ebce0366"></a>`signature`: `"\"(self, work_id: 'str', plan: 'WorkflowPlan', *, expected_revision: 'int') -> 'WorkRecord'\""`

## Maintained corroboration

### Related interface records

- [Stove0WorkService](stove0-core-stove0workservice.md)

## Governing policies

- <a id="pa-da42c596d8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.Stove0WorkService.seal_workflow_plan`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3d42f8b65eb5ef099ebe3be10c2010d3c1b4f608658a75dbd89bf69cc5f67c1d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', plan: 'WorkflowPlan', *, expected_revision: 'int') -> 'WorkRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "seal_workflow_plan",
  "owner": "stove0_core.Stove0WorkService",
  "unit": "member"
}
```

</details>
