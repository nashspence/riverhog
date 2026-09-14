# stove0_core.WorkflowPreviewService.preview

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-workflowpreviewservice-preview:be7dc66a7c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d46019f25d"></a>
- <a id="s-eff35fc1fa"></a>`distribution`: `stove0-server`
- <a id="s-44f0cc4225"></a>`module`: `stove0_core`
- <a id="s-9a5d209e84"></a>`name`: `preview`
- <a id="s-2b7b52ab3d"></a>`owner`: `stove0_core.WorkflowPreviewService`
- <a id="s-6fdcca6b48"></a>`unit`: `member`

### Declared structure

- <a id="s-6eb7beb7d8"></a>`kind`: `"method"`
- <a id="s-79cfed358f"></a>`signature`: `"\"(self, work: 'object') -> 'WorkflowPreview'\""`

## Maintained corroboration

### Related interface records

- [WorkflowPreviewService](stove0-core-workflowpreviewservice.md)

## Governing policies

- <a id="pa-a7d4817069"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.WorkflowPreviewService.preview`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 208680d9a5d15a66480ac90de87e122b83157a87429fa182193d5b3ce1fbc1a7 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work: 'object') -> 'WorkflowPreview'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "preview",
  "owner": "stove0_core.WorkflowPreviewService",
  "unit": "member"
}
```
