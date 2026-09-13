# stove0_target_support.TargetConformanceCase

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetconformancecase:84588d8345 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-92d09ac817"></a>
| Field | Shape |
|---|---|
| <a id="s-6817ca728b"></a>`contract` | additional keys=`fields`, `kind`, `signature` |
| <a id="s-b60e48b367"></a>`distribution` | "stove0-target-support" |
| <a id="s-baf7bd6879"></a>`module` | "stove0_target_support" |
| <a id="s-b2a9c3e737"></a>`name` | "TargetConformanceCase" |
| <a id="s-932c31826e"></a>`unit` | "export" |

## Governing policies

- <a id="pa-324d0fb825"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetConformanceCase`

### Exact owned JSON

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
