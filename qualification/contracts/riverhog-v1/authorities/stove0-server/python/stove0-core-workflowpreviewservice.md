# stove0_core.WorkflowPreviewService

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-workflowpreviewservice:98d3d6c679 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c542c38778"></a>
- <a id="s-236ea06489"></a>`distribution`: `stove0-server`
- <a id="s-7f9aef14ea"></a>`module`: `stove0_core`
- <a id="s-b806fa0498"></a>`name`: `WorkflowPreviewService`
- <a id="s-e7d9ff0ada"></a>`unit`: `export`

### Declared structure

- <a id="s-8c4b0e0bea"></a>`kind`: `"class"`
- <a id="s-7f3794ed12"></a>`signature`: `"\"(*, riverhog: 'PreviewRiverhogPort', planning: 'PlanningPort', observers: 'ObserverPort', targets: 'TargetPort') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [preview](stove0-core-workflowpreviewservice-preview.md)

## Governing policies

- <a id="pa-52c9ddda8c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.WorkflowPreviewService`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d699c6a5ff19a03cedf34e54181af4aeedfd7fca52f709042ff81081a6e79f66 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(*, riverhog: 'PreviewRiverhogPort', planning: 'PlanningPort', observers: 'ObserverPort', targets: 'TargetPort') -> 'None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "WorkflowPreviewService",
  "unit": "export"
}
```

</details>
