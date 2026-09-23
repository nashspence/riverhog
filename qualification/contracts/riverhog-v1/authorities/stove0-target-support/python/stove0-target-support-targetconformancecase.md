# stove0_target_support.TargetConformanceCase

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetconformancecase:84588d8345 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-92d09ac817"></a>
- <a id="s-b60e48b367"></a>`distribution`: `stove0-target-support`
- <a id="s-baf7bd6879"></a>`module`: `stove0_target_support`
- <a id="s-b2a9c3e737"></a>`name`: `TargetConformanceCase`
- <a id="s-932c31826e"></a>`unit`: `export`

### Declared structure

- <a id="s-a1382e9f6b"></a>`kind`: `"class"`
- <a id="s-13795ea3e8"></a>`signature`: `"\"(operation: 'OperationContract', job_request: 'TargetJobRequest', semantic_vectors: 'SemanticIntentConformanceVectors \| None' = None) -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-a7dd544fb6"></a>`operation` | `'OperationContract'` | `required` |
| <a id="s-1c95ce4eed"></a>`job_request` | `'TargetJobRequest'` | `required` |
| <a id="s-d24f19a5fe"></a>`semantic_vectors` | `'SemanticIntentConformanceVectors \| None'` | `None` |

## Governing policies

- <a id="pa-324d0fb825"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.TargetConformanceCase`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 188772e2873527006fb90f7868b512b3119401fc2d4f9a342cd653153ee52317 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "operation",
        "type": "'OperationContract'"
      },
      {
        "default": "required",
        "name": "job_request",
        "type": "'TargetJobRequest'"
      },
      {
        "default": "None",
        "name": "semantic_vectors",
        "type": "'SemanticIntentConformanceVectors | None'"
      }
    ],
    "kind": "class",
    "signature": "\"(operation: 'OperationContract', job_request: 'TargetJobRequest', semantic_vectors: 'SemanticIntentConformanceVectors | None' = None) -> None\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetConformanceCase",
  "unit": "export"
}
```

</details>
