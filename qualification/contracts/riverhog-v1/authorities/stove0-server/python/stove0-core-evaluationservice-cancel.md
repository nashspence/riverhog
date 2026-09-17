# stove0_core.EvaluationService.cancel

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-evaluationservice-cancel:66581adec9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f69f247e80"></a>
- <a id="s-c6a9c64ef0"></a>`distribution`: `stove0-server`
- <a id="s-2eb7e58530"></a>`module`: `stove0_core`
- <a id="s-6e6b47dfbd"></a>`name`: `cancel`
- <a id="s-6d6bf5de8e"></a>`owner`: `stove0_core.EvaluationService`
- <a id="s-4fb7894f2d"></a>`unit`: `member`

### Declared structure

- <a id="s-e0b6468600"></a>`kind`: `"method"`
- <a id="s-00ae2b78f9"></a>`signature`: `"\"(self, evaluation_id: 'str', *, controller: 'EvaluationWorkController') -> 'EvaluationRecord'\""`

## Maintained corroboration

### Related interface records

- [EvaluationService](stove0-core-evaluationservice.md)

## Governing policies

- <a id="pa-f9553eca00"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.EvaluationService.cancel`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fd6d11fe424a55be32d3ca157a469b3371b74acebe8bdeafc8be307e3dc38c50 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, evaluation_id: 'str', *, controller: 'EvaluationWorkController') -> 'EvaluationRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "cancel",
  "owner": "stove0_core.EvaluationService",
  "unit": "member"
}
```

</details>
